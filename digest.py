#!/usr/bin/env python3
"""
KassenInfodienst – Wöchentlicher Überblick über die größten Krankenkassen

Recherchiert für jeden Account automatisch:
  • Personalveränderungen (Vorstandswechsel, neue CIO/CTO/CEO)
  • IT-Vorhaben & Digitalisierungsprojekte
  • Haushaltsplanung & Finanznachrichten
  • TED-Ausschreibungen (ted.europa.eu)
  • LinkedIn-Posts von Entscheidern

Ausgabe: Markdown-Datei in reports/ mit Verkaufschancen pro Kasse.

Verwendung:
    python digest.py                      # Alle Kassen
    python digest.py --kassen TK BARMER  # Nur bestimmte Kassen (Kurzname)
    python digest.py --output mein.md    # Eigener Ausgabepfad
    python digest.py --tage 7            # Recherchezeitraum in Tagen (Standard: 7)
"""

from __future__ import annotations

try:
    import openai
except ImportError:  # --demo soll auch ohne installierte Abhaengigkeiten laufen
    openai = None

import argparse
try:
    import httpx
except ImportError:
    httpx = None
import json
import os
import re
try:
    import requests as req
except ImportError:
    req = None
import smtplib
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def env_int(name: str, default: int) -> int:
    """Liest optionale Zahlen aus GitHub-Env/Vars robust, auch wenn sie leer sind."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"   ⚠️  {name}={raw!r} ist keine Zahl, nutze {default}.", file=sys.stderr)
        return default


def env_bool(name: str, default: bool = False) -> bool:
    """Liest optionale Boolean-Flags aus GitHub-Env/Vars robust."""
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "ja", "on"}:
        return True
    if raw in {"0", "false", "no", "nein", "off"}:
        return False
    print(f"   ⚠️  {name}={raw!r} ist kein Boolean, nutze {default}.", file=sys.stderr)
    return default


# Lade .env-Datei falls vorhanden (pip install python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from kassen import KASSEN, BEOBACHTETE_INSTITUTIONEN, BEOBACHTETE_ORGS, BEOBACHTETE_PERSONEN, load_target_kassen

# Zielkassen werden vorrangig aus der verbindlichen Markdown-Datei geladen.
KASSEN = load_target_kassen()

BATCH_SIZE = 5          # Mehrere Accounts pro Web-Research-Call, damit Weekly unter dem Actions-Limit bleibt
MAX_SEARCHES = 6        # Gezielte Suchen pro Batch
BATCH_PAUSE = 2         # Kurze Pause zwischen Batches
MAX_RETRIES = 0         # Scheduled Runs sollen weiterlaufen statt an einem hängenden Batch zu kleben
API_TIMEOUT = 75        # Timeout pro API-Call in Sekunden – bei Hänger schnell abbrechen
LAST_WEEK_FILE = Path("last_week.md")   # Gedächtnis: was letzte Woche berichtet wurde
REPORTS_DIR = Path("reports")
MIN_TED_VALUE_EUR = 1_000_000
MIN_RELEVANCE_SCORE = env_int("MIN_RELEVANCE_SCORE", 4)
MIN_LINKEDIN_RELEVANCE_SCORE = env_int("MIN_LINKEDIN_RELEVANCE_SCORE", 4)
MIN_WEAK_SIGNAL_SCORE = env_int("MIN_WEAK_SIGNAL_SCORE", 3)
MAX_SCORING_ITEMS = 300
MAX_NEWSLETTER_SOURCES = env_int("MAX_NEWSLETTER_SOURCES", 60)
MIN_NEWSLETTER_CHARS = env_int("MIN_NEWSLETTER_CHARS", 14000)
NEWSLETTER_TARGET_WORDS = env_int("NEWSLETTER_TARGET_WORDS", 4000)
MAX_IMAGE_FETCHES = env_int("MAX_IMAGE_FETCHES", 24)
LINKEDIN_QUERY_LIMIT = env_int("LINKEDIN_QUERY_LIMIT", 3)
LINKEDIN_RADAR_LIMIT = env_int("LINKEDIN_RADAR_LIMIT", 50)
LINKEDIN_POSTS_PER_ACCOUNT = env_int("LINKEDIN_POSTS_PER_ACCOUNT", 10)
NEWS_RSS_MARKET_LIMIT = env_int("NEWS_RSS_MARKET_LIMIT", 50)
ENABLE_LINKEDIN_VOYAGER = os.environ.get("ENABLE_LINKEDIN_VOYAGER", "").lower() in {"1", "true", "yes"}
ENABLE_SOURCE_IMAGES = os.environ.get("ENABLE_SOURCE_IMAGES", "true").lower() in {"1", "true", "yes"}
ENABLE_PERSONEN_RADAR = os.environ.get("ENABLE_PERSONEN_RADAR", "true").lower() in {"1", "true", "yes"}
ENABLE_TED_TENDERS = env_bool("ENABLE_TED_TENDERS", False)

RESEARCH_MODEL = os.environ.get("OPENAI_RESEARCH_MODEL") or "gpt-5-nano"
SCORING_MODEL = os.environ.get("OPENAI_SCORING_MODEL") or "gpt-5-nano"
NEWSLETTER_MODEL = os.environ.get("OPENAI_NEWSLETTER_MODEL") or "auto"
ENABLE_OPENAI_WEB_RESEARCH = os.environ.get("ENABLE_OPENAI_WEB_RESEARCH", "").lower() in {"1", "true", "yes"}

NEWSLETTER_MODEL_CANDIDATES = [
    "gpt-5.4-mini",
    "gpt-5.5-pro",
    "gpt-5.5",
    "gpt-5.4-pro",
    "gpt-5.4",
    "gpt-5.4-long-context",
    "gpt-5.3-chat-latest",
    "gpt-5.2",
    "gpt-5.1",
    "gpt-4.1",
]
NEWSLETTER_API = "chat"

MIN_SCORE_TOP = env_int("MIN_SCORE_TOP", 75)
MIN_SCORE_KEEP = env_int("MIN_SCORE_KEEP", 60)
MIN_SCORE_INTERNAL = env_int("MIN_SCORE_INTERNAL", 45)

FILTER_REPORT: dict[str, int] = {}

EXCLUDE_TOPIC_TERMS = {
    "prävention", "praevention", "bewegung", "jugend", "schule", "schul", "kampagne",
    "gewinnspiel", "gesundheitswoche", "aktionstag", "event", "messe", "award", "preis",
    "glückwunsch", "glueckwunsch", "sommerfest", "netzwerktreffen", "follower", "likes",
    "muttertag", "sonne", "pink", "charity run", "run & bike", "comfortable 5k",
    "podcast", "business talks", "für die ohren", "fuer die ohren",
}

LOW_VALUE_ACCOUNT_TERMS = {
    "innungsversammlung", "schornsteinfeger", "signal iduna", "infostand",
    "firmenkunden", "glücksspielsucht", "gluecksspielsucht", "suchtprävention",
    "suchtpraevention", "präventionsprojekt", "praeventionsprojekt",
}

BITMARCK_KUNDENTAG_TERMS = {
    "bitmarck kundentag",
    "bitmarck-kundentag",
    "bitmarck partnertag",
    "bitmarck-partnertag",
    "house of health",
    "bitmarck standort essen",
    "kundentag 2026",
}

STRATEGIC_GKV_EVENT_TERMS = BITMARCK_KUNDENTAG_TERMS | {
    "itsc zukunftskongress",
    "itsc-zukunftskongress",
    "zukunftskongress itsc",
    "dmea",
    "messe-rundgang",
    "messerundgang",
    "messe highlights",
    "messe-highlights",
    "meine highlights",
}

EVENT_SUBSTANCE_TERMS = {
    "highlight", "highlights", "takeaways", "kernaussagen", "impulse", "panel",
    "keynote", "roadmap", "produkt", "produkte", "plattform", "daten", "cloud",
    "ki", "künstliche intelligenz", "kuenstliche intelligenz", "automatisierung",
    "epa", "e-pa", "ti", "gematik", "interoperabilität", "interoperabilitaet",
    "prozess", "prozesse", "service", "digitalisierung", "strategie",
    "regulatorik", "security", "cyber", "nis2", "kritis",
}

LINKEDIN_ALLOWED_ROLES = {
    "vorstand", "geschäftsführung", "geschaeftsfuehrung", "cio", "cdo", "cto", "bereichsleitung",
    "leiter", "head of", "pressesprecher", "kommunikation", "politik", "regulierung"
}


GKV_CONTEXT_TERMS = {
    "gkv", "krankenkasse", "krankenkassen", "gesetzliche krankenversicherung",
    "versicherte", "versicherten", "versorgung", "leistungserbringer",
    "tk", "techniker krankenkasse", "barmer", "dak", "aok", "ikk", "bkk",
    "kkh", "sbk", "hkk", "bitmarck", "itsc", "msg", "msg systems",
    "arvato", "arvato systems", "materna", "adesso", "sopra steria",
    "davaso", "spectrumk", "gevko", "aok systems", "gesundheitswesen",
    "healthcare", "health-it", "health it", "digital health", "e-health",
    "gesundheits-it", "krankenversicherung", "sozialversicherung",
    "bmg", "gkv-spitzenverband", "gematik", "bsi", "aok-bundesverband",
    "vdek", "bkk dachverband", "ikk e.v.", "datenschutz", "regulatorik",
}

LINKEDIN_MARKET_QUERIES = [
    "BITMARCK Kundentag",
    "BITMARCK-Kundentag",
    "BITMARCK Partnertag",
    "BITMARCK-Partnertag",
    "ITSC Zukunftskongress",
    '"House of Health" BITMARCK',
    "Krankenkasse Fusion",
    "Kassenfusion GKV",
    "BKK Fusion",
    "GKV Dienstleister msg",
    "GKV Dienstleister Arvato",
    "GKV Dienstleister Materna",
    "GKV Dienstleister Sopra Steria",
    "CDO DMEA Highlights Krankenkasse",
    "CIO DMEA Highlights Krankenkasse",
    "GKV IT",
    "Krankenkasse Digitalisierung",
    "gesetzliche Krankenversicherung CIO",
    "GKV Projekt Go-live",
    "Krankenkasse KI Automatisierung",
    "Krankenkasse Servicecenter Digitalisierung",
    "Health IT KI Gesundheitswesen",
    "Digital Health Krankenkasse",
    "eHealth GKV Digitalisierung",
    "KI Gesundheitswesen Krankenkasse",
    "Healthcare IT Deutschland",
    "Krankenversicherung Transformation IT",
]

NEWS_RSS_MARKET_QUERIES = [
    '"BITMARCK Kundentag"',
    '"BITMARCK-Kundentag"',
    '"BITMARCK Partnertag"',
    '"BITMARCK-Partnertag"',
    '"ITSC Zukunftskongress"',
    '"House of Health" BITMARCK',
    '"Krankenkasse" Fusion',
    '"Kassenfusion" GKV',
    '"BKK" Fusion Krankenkasse',
    '"msg" GKV Krankenkasse',
    '"Arvato Systems" GKV Krankenkasse',
    '"Materna" GKV Krankenkasse',
    '"Sopra Steria" GKV Krankenkasse',
    '"GKV" "IT" Digitalisierung',
    '"Krankenkasse" Software Projekt',
    '"gesetzliche Krankenversicherung" KI Automatisierung',
    '"Krankenkasse" Servicecenter Digitalisierung',
    '"Krankenkasse" Cybersecurity',
    '"GKV" "Go-live"',
    '"GKV" Rollout Implementierung',
    '"Krankenkasse" Ausschreibung IT',
    '"Health IT" "Krankenkasse"',
    '"Digital Health" "GKV"',
    '"KI" "Gesundheitswesen" "Krankenkasse"',
    '"eHealth" "gesetzliche Krankenversicherung"',
]

DEDICATED_GKV_PROVIDERS = {
    "bitmarck", "itsc", "aok systems", "gkv informatik", "gevko", "davaso", "spectrumk",
    "msg", "msg systems", "arvato", "arvato systems", "materna", "adesso", "sopra steria",
}

DECISION_MAKER_TERMS = {
    "vorstand", "vorständin", "vorstandsvorsitz", "ceo", "cio", "cto", "cdo",
    "cco", "coo", "cfo", "chief", "geschäftsführer", "geschäftsführerin",
    "vorsitzender", "vorsitzende", "hauptgeschäftsführer", "praesident", "präsident",
    "geschäftsbereichsleiter", "leiter geschäftsbereich", "bereichsleiter",
    "bereichsleitung", "head of", "it-leiter", "digitalisierungsleiter",
    "director", "direktor", "direktorin", "leitung digital", "leitung it",
    "leitung versorgung", "leitung strategie", "leitung finanzen", "pressesprecher",
    "pressesprecherin", "unternehmenskommunikation", "kommunikation", "sprecher",
}

NON_DECISION_TERMS = {
    "sachbearbeiter", "kundenberater", "kundenservice", "recruiter", "recruiting",
    "talent acquisition", "praktikant", "werkstudent", "student", "azubi",
    "auszubild", "beraterin kunden", "berater kunden", "sales manager",
    "account executive", "vertrieb", "business development manager",
}

STRATEGIC_TOPIC_TERMS = {
    "ki ", "künstliche intelligenz", "automatisierung", "digitalisierung",
    "software", "cloud", "plattform", " api ", "daten", "system", "it-",
    "cyber", "security", "sicherheit", "informationssicherheit", "b3s", "nis2",
    "kritis", "c5", "portal", "app", "online", "service", "servicecenter",
    "kontaktcenter", "omnichannel", "prozess", "prozessoptimierung", "innovation",
    "strategie", "transformation", "projekt", "kooperation", "go-live", "golive",
    "rollout", "einführung", "implementierung", "migration", "zuschlag",
    "auftrag", "livegang", "release", "epa", "e-pa", "ti ", "egk", "vsdm",
    "telematik", "gematik", "datenschutz", "regulatorik", "gesetz",
    "referentenentwurf", "stellungnahme", "ausschreibung", "vergabe", "beschaffung",
    "interoperabilität", "interoperabilitaet", "diga", "e-rezept",
    "reform", "finanzierung", "finanzierungsdruck", "strukturreform",
    "versorgungsgesetz", "gesundheitsdatennutzung", "gedig",
    "kundentag", "partnertag", "zukunftskongress", "house of health",
}

LINKEDIN_NOISE_TERMS = {
    "wir stellen ein", "karriere", "job", "jobs", "bewerben", "team event",
    "sommerfest", "after work", "glückwunsch", "congratulations", "messebesuch",
    "event-selfie", "danke für den austausch", "toller austausch", "employer branding",
    "kununu", "benefits", "work-life", "recruiting", "talent", "azubi",
    "muttertag", "ganz viel pink", "charity run", "run & bike", "comfortable 5k",
    "started today's run", "podcast", "business talks", "ein bisschen was für die ohren",
    "ein bisschen was fuer die ohren", "reise", "städtetrip", "staedtetrip",
}

LINKEDIN_HARD_EXCLUDE_TERMS = {
    "zahnzusatzversicherung", "implantat geplant", "keramik statt standardfüllung",
    "standardfuellung", "standardfüllung", "eigenanteile", "finanzflüsterer",
    "finanzfluesterer", "lebenskostenoptimierer", "unabhängiger finanz",
    "unabhaengiger finanz", "wingcopter", "taking care of the people behind the technology",
    "university of kassel", "bewegungsbande", "cybermobbing", "jury unseres präventionsprojekts",
    "jury unseres praeventionsprojekts", "hochschule für angewandte wissenschaften",
    "hochschule fuer angewandte wissenschaften", "exkursion", "followers)", "follower)",
}

LINKEDIN_TRUSTED_MARKET_TERMS = {
    "stefan schellberg", "andreas strausfeld", "dieter loewe", "dieter löwe",
    "bitmarck", "itsc", "aok systems", "gkv informatik", "gematik", "bsi",
    "gkv-spitzenverband", "bundesministerium fuer gesundheit", "bundesministerium für gesundheit",
    "bmg", "vdek", "ikk e.v", "bkk dachverband", "aok-bundesverband",
    "dak-gesundheit", "techniker krankenkasse", "barmer", "ikk classic",
    "pressestelle", "unternehmenskommunikation", "cio", "cdo", "cto",
}

LINKEDIN_ACCOUNT_VALUE_TERMS = STRATEGIC_TOPIC_TERMS | {
    "fusion", "fusionen", "zusammenschluss", "kooperation", "gemeinsames projekt",
    "gemeinsame it", "plattformverbund", "verbund", "shared service", "shared services",
    "kassen-it", "dienstleistersteuerung", "versorgungspfad", "versorgungsprogramm",
    "selektivvertrag", "digitalstrategie", "kassenpolitik",
    "bitmarck kundentag", "bitmarck-kundentag", "bitmarck partnertag",
    "bitmarck-partnertag", "itsc zukunftskongress", "itsc-zukunftskongress",
    "house of health", "kundentag 2026", "messe highlights", "messe-highlights",
}

HARD_ACCOUNT_SIGNAL_TERMS = {
    "fusion", "fusionen", "zusammenschluss", "kooperation", "gemeinsames projekt",
    "go-live", "golive", "rollout", "implementierung", "migration", "plattform",
    "software", "cloud", "portal", "app", "servicecenter", "kontaktcenter",
    "automatisierung", "ki ", "daten", "interoperabilität", "interoperabilitaet",
    "epa", "e-pa", "egk", "vsdm", "ti ", "telematik", "gematik",
    "bsi", "nis2", "kritis", "b3s", "c5", "datenschutz", "informationssicherheit",
    "ausschreibung", "vergabe", "zuschlag", "auftrag", "beschaffung",
    "reform", "finanzierung", "finanzierungsdruck", "strukturreform",
    "gesundheitsdatennutzung", "gedig",
    "dienstleister", "bitmarck", "itsc", "aok systems", "gkv informatik",
    "msg", "msg systems", "arvato", "arvato systems", "materna", "adesso",
    "sopra steria", "davaso", "spectrumk", "gevko",
    "bitmarck kundentag", "bitmarck-kundentag", "bitmarck partnertag",
    "bitmarck-partnertag", "itsc zukunftskongress", "itsc-zukunftskongress",
    "house of health", "kundentag 2026", "messe highlights", "messe-highlights",
}

SOURCE_RELIABILITY_LABELS = {
    "Primärquelle": 5,
    "Pressemitteilung": 4,
    "Verbands-/Institutionsseite": 5,
    "LinkedIn-Signal": 3,
    "Medienbericht": 3,
    "Sonstiger Hinweis": 2,
}


def contains_any(value: str, terms: set[str]) -> bool:
    return any(term in value.lower() for term in terms)


def classify_source_label(text: str) -> str:
    blob = text.lower()
    if "linkedin" in blob:
        return "LinkedIn-Signal"
    if any(term in blob for term in ("bundesgesundheitsministerium", "gkv-spitzenverband", "gematik", "bsi", "vdek", "bkk dachverband", "aok-bundesverband", "ikkev")):
        return "Verbands-/Institutionsseite"
    if any(term in blob for term in ("pressemitteilung", "pressestelle", "presse")):
        return "Pressemitteilung"
    if any(term in blob for term in ("site:", "quelle", "artikel")):
        return "Medienbericht"
    return "Sonstiger Hinweis"


def evaluate_linkedin_signal(org: dict, actor_name: str, actor_title: str, text: str, reactions: int) -> tuple[bool, str]:
    """Deterministischer LinkedIn-Vorfilter fuer Entscheider- und Institutionssignale."""
    text_lower = text.lower()
    actor_blob = f"{actor_name} {actor_title}".lower()
    org_type = org.get("type", "")
    is_provider = org_type == "provider"
    is_institution = org_type == "institution"
    is_market = org_type == "market"
    is_influencer = org_type == "influencer"
    is_decision_maker = contains_any(actor_title, DECISION_MAKER_TERMS)
    is_non_decision = contains_any(actor_title, NON_DECISION_TERMS)
    is_official_or_target = (
        org.get("short", "").lower() in actor_blob
        or org.get("name", "").lower() in actor_blob
        or org.get("linkedin_search", "").lower() in actor_blob
        or org.get("short", "").lower() in text_lower
        or org.get("name", "").lower() in text_lower
    )
    is_strategic_event = _is_strategic_event_signal(text_lower)
    has_topic = contains_any(text_lower, STRATEGIC_TOPIC_TERMS) or is_strategic_event
    has_gkv_context = contains_any(text_lower, GKV_CONTEXT_TERMS) or is_official_or_target
    is_noise = contains_any(text_lower, LINKEDIN_NOISE_TERMS)

    if is_non_decision and not is_official_or_target:
        return False, "Nicht-Entscheiderrolle"
    if is_noise and not is_strategic_event:
        return False, "Karriere/Event/Marketing ohne strategischen Bezug"
    if is_strategic_event and has_gkv_context:
        return True, "strategisches GKV-IT-Event"
    if is_provider and not has_gkv_context:
        return False, "Dienstleisterpost ohne GKV-Kontext"
    if is_institution and not (has_topic or has_gkv_context):
        return False, "Institutionenpost ohne GKV-/IT-/Regulatorikbezug"
    if is_market and not (is_decision_maker or is_official_or_target or has_gkv_context):
        return False, "Marktpost ohne belastbaren GKV-Bezug"
    if not (is_decision_maker or is_official_or_target or is_influencer):
        return False, "keine Entscheider- oder offizielle Quelle"
    if not (has_topic and has_gkv_context):
        return False, "kein belastbares IT-, Digital-, Regulatorik- oder Marktsignal"
    return True, "qualifiziertes LinkedIn-Signal"


def _is_strategic_event_signal(text_blob: str) -> bool:
    blob = (text_blob or "").lower()
    if contains_any(blob, BITMARCK_KUNDENTAG_TERMS):
        return True
    if "itsc" in blob and contains_any(blob, {"zukunftskongress", "zukunfts kongress"}):
        return True
    if "dmea" in blob and contains_any(blob, EVENT_SUBSTANCE_TERMS) and (
        contains_any(blob, DECISION_MAKER_TERMS)
        or contains_any(blob, LINKEDIN_ALLOWED_ROLES)
        or contains_any(blob, GKV_CONTEXT_TERMS)
    ):
        return True
    return contains_any(blob, STRATEGIC_GKV_EVENT_TERMS) and contains_any(blob, EVENT_SUBSTANCE_TERMS)


def _is_low_value_account_signal(text_blob: str) -> bool:
    blob = (text_blob or "").lower()
    if not contains_any(blob, LOW_VALUE_ACCOUNT_TERMS):
        return False
    hard_phrase_terms = {
        "ausschreibung", "vergabe", "zuschlag", "auftrag", "plattform", "migration",
        "go-live", "rollout", "cloud", "datenplattform", "api", "schnittstelle",
        "epa", "e-pa", "telematikinfrastruktur", "gematik", "gedig", "nis2",
        "kritis", "security", "cyber", "automatisierung", "dunkelverarbeitung",
    }
    has_hard_phrase = contains_any(blob, hard_phrase_terms)
    has_hard_abbreviation = bool(re.search(r"\b(ki|ti|api)\b", blob))
    return not (has_hard_phrase or has_hard_abbreviation)

IMAGE_CACHE: dict[str, str] = {}
IMAGE_FETCH_COUNT = 0
PAGE_PREVIEW_CACHE: dict[str, dict[str, str]] = {}


def normalize_item_key(text: str) -> str:
    """Stabiler Dedupe-Key für Rohmeldungen über mehrere Suchquellen hinweg."""
    text = re.sub(r"https?://\S+", "", text.lower())
    text = re.sub(r"[_*`#>\[\]().,;:!?\"'“”„–—-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:220]


def find_url_in_obj(value, allowed_domains: tuple[str, ...] | None = None) -> str:
    """Findet die erste URL in verschachtelten API-Antworten."""
    if isinstance(value, dict):
        for key in (
            "url", "postUrl", "post_url", "activityUrl", "permalink",
            "canonicalUrl", "shareUrl", "linkedinUrl", "link",
        ):
            found = find_url_in_obj(value.get(key), allowed_domains)
            if found:
                return found
        for child in value.values():
            found = find_url_in_obj(child, allowed_domains)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_url_in_obj(child, allowed_domains)
            if found:
                return found
    elif isinstance(value, str):
        for url in re.findall(r"https?://[^\s)>\]}\"']+", value):
            if not allowed_domains or any(domain in url for domain in allowed_domains):
                return url
    return ""


def source_link(url: str, label: str = "Quelle") -> str:
    """Formatiert Quellen als kurze Markdown-Links statt langer Redirect-URLs."""
    clean = (url or "").strip()
    if not clean:
        return "Quelle nicht verlinkt"
    clean = clean.rstrip(".,;")
    return f"[{label}]({clean})"


def clean_visible_source_text(text: str) -> str:
    """Entfernt interne Scoring-/Markdown-Artefakte aus sichtbaren Newsletter-Quellen."""
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^#+\s*", "", cleaned)
    cleaned = re.sub(r"\b[QR]\d{2}\s*\|\s*", "", cleaned)
    cleaned = re.sub(r"\s*\|\s*Score=?\d+\b", "", cleaned)
    cleaned = re.sub(r"^(Top-Thema|Management|Kassenradar|Institutionen/Politik|IT/Digital/Beschaffung|LinkedIn|Marktsignal|Quelle)\s*\|\s*", "", cleaned)
    cleaned = re.sub(r"^(Primärquelle|Pressemitteilung|Verbands-/Institutionsseite|LinkedIn-Signal|Medienbericht|Sonstiger Hinweis|Marktquelle)\s*\|\s*", "", cleaned)
    cleaned = re.sub(r"\*\*\s*\((LinkedIn|News/RSS|RSS)\)", r" (\1)", cleaned)
    cleaned = cleaned.replace("**", "")
    cleaned = cleaned.replace("Vertriebsrelevanz:", "Einordnung:")
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


LOW_SIGNAL_PATTERNS = (
    "kein konkreter gkv",
    "kein konkreter it",
    "kein konkreter health-it",
    "ohne konkreten gkv",
    "ohne konkreten it",
    "ohne konkreten health-it",
    "ohne inhalt zu health it",
    "ohne inhalt zu kassen",
    "ohne kassen- oder projekt",
    "ohne kassen oder projekt",
    "nur abschieds",
    "karrierepost",
    "reiseankündigung ohne",
    "allgemeiner nachfrage",
    "allgemeiner ressourcendruck",
)


def is_low_signal_text(text: str) -> bool:
    """Erkennt Bewertungsreste, die nicht in den Newsletter gehören."""
    blob = clean_visible_source_text(text).lower()
    return any(pattern in blob for pattern in LOW_SIGNAL_PATTERNS)


def _looks_like_image_url(url: str) -> bool:
    """Erkennt Bild-URLs aus RSS, LinkedIn-CDNs und typischen Preview-Feldern."""
    if not url.startswith(("http://", "https://")):
        return False
    clean = url.split("?")[0].lower()
    return (
        clean.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))
        or "media.licdn.com" in url
        or "/dms/image/" in url
        or "image" in clean
    )


def find_image_in_obj(value) -> str:
    """Findet ein Inhaltsbild in verschachtelten API-Antworten, ohne Avatare zu bevorzugen."""
    skip_keys = {"avatar", "profile", "profilepicture", "logo", "icon", "author"}
    preferred_keys = (
        "image", "imageUrl", "image_url", "thumbnail", "thumbnailUrl",
        "thumbnail_url", "previewImage", "preview_image", "media", "mediaUrl",
        "contentImages", "images",
    )

    if isinstance(value, dict):
        for key in preferred_keys:
            if key in value:
                found = find_image_in_obj(value.get(key))
                if found:
                    return found
        for key, child in value.items():
            key_l = str(key).lower()
            if any(skip in key_l for skip in skip_keys):
                continue
            found = find_image_in_obj(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_image_in_obj(child)
            if found:
                return found
    elif isinstance(value, str):
        for url in re.findall(r"https?://[^\s)>\]}\"']+", value):
            url = url.rstrip(".,;")
            if _looks_like_image_url(url):
                return url
    return ""


def extract_page_preview(url: str) -> dict[str, str]:
    """Holt Bild und Kurzbeschreibung vom Artikel, begrenzt damit der Scheduled Run nicht festläuft."""
    global IMAGE_FETCH_COUNT

    clean = (url or "").strip()
    if not ENABLE_SOURCE_IMAGES or not clean:
        return {}
    if clean in PAGE_PREVIEW_CACHE:
        return PAGE_PREVIEW_CACHE[clean]
    if IMAGE_FETCH_COUNT >= MAX_IMAGE_FETCHES:
        return {}

    IMAGE_FETCH_COUNT += 1
    image_url = ""
    description = ""
    final_url = clean
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    }
    try:
        from bs4 import BeautifulSoup

        response = req.get(clean, headers=headers, timeout=5, allow_redirects=True)
        final_url = response.url or clean
        if response.ok and "text/html" in response.headers.get("content-type", ""):
            soup = BeautifulSoup(response.text[:250000], "html.parser")
            for attrs in (
                {"property": "og:description"},
                {"name": "description"},
                {"name": "twitter:description"},
            ):
                meta = soup.find("meta", attrs=attrs)
                if meta:
                    candidate = re.sub(r"\s+", " ", (meta.get("content") or "")).strip()
                    if len(candidate) >= 45:
                        description = candidate[:420]
                        break
            if not description:
                for selector in (
                    "article",
                    '[role="article"]',
                    "main",
                    ".article-body",
                    ".article__body",
                    ".entry-content",
                    ".post-content",
                    ".article-content",
                ):
                    node = soup.select_one(selector)
                    if not node:
                        continue
                    paragraphs = [
                        re.sub(r"\s+", " ", p.get_text(" ")).strip()
                        for p in node.find_all("p")
                    ]
                    candidate = " ".join(p for p in paragraphs if len(p) >= 35)
                    if len(candidate) >= 120:
                        description = candidate[:900]
                        break
            if not description:
                paragraphs = [
                    re.sub(r"\s+", " ", p.get_text(" ")).strip()
                    for p in soup.find_all("p")[:6]
                ]
                candidate = " ".join(p for p in paragraphs if len(p) >= 35)
                if len(candidate) >= 45:
                    description = candidate[:900]
            for attrs in (
                {"property": "og:image"},
                {"name": "twitter:image"},
                {"property": "twitter:image"},
            ):
                meta = soup.find("meta", attrs=attrs)
                if meta:
                    candidate = (meta.get("content") or "").strip()
                    if _looks_like_image_url(candidate):
                        image_url = candidate
                        break
    except Exception:
        image_url = ""
        description = ""
        final_url = clean

    preview = {"image": image_url, "description": description, "url": final_url}
    PAGE_PREVIEW_CACHE[clean] = preview
    IMAGE_CACHE[clean] = image_url
    return preview


def extract_page_image(url: str) -> str:
    """Holt ein og:image vom Artikel."""
    if url in IMAGE_CACHE:
        return IMAGE_CACHE[url]
    return extract_page_preview(url).get("image", "")


def add_image_marker(line: str, image_url: str) -> str:
    """Haengt ein Markdown-Bild als verwertbares Rohsignal an eine Quellenzeile."""
    image_url = (image_url or "").strip()
    if not image_url:
        return line
    return f"{line}\n    Bild: ![Vorschaubild]({image_url})"


def add_source_preview(line: str, url: str, image_url: str = "") -> str:
    """Ergänzt Quellenzeilen um Bild und Kurzkontext fuer echte Artikelkarten."""
    preview = extract_page_preview(url)
    final_url = preview.get("url") or url
    if final_url and final_url != url:
        line = line.replace(f"]({url})", f"]({final_url})")
    result = add_image_marker(line, image_url or preview.get("image", ""))
    description = (preview.get("description") or "").strip()
    if description:
        result += f"\n    Kontext: {description}"
    return result


def collect_visuals_from_research(all_research: str, limit: int = 6) -> list[tuple[str, str]]:
    """Extrahiert Bild-Markdown aus Rohdaten fuer einen deterministischen Fallback."""
    visuals: list[tuple[str, str]] = []
    seen: set[str] = set()
    previous_text = ""
    for raw_line in all_research.splitlines():
        line = raw_line.strip()
        for url in re.findall(r"!\[[^\]]*\]\((https?://[^)]+)\)", line):
            if url in seen:
                continue
            seen.add(url)
            label = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", previous_text or line)
            label = re.sub(r"\s+", " ", re.sub(r"[*_`>#-]+", " ", label)).strip()
            visuals.append((label[:120] or "Quelle aus dem Wochenbrief", url))
            if len(visuals) >= limit:
                return visuals
        if line and not line.startswith("Bild:"):
            previous_text = line
    return visuals


def ensure_visuals_in_summary(summary: str, all_research: str) -> str:
    """Stellt sicher, dass die Mail nicht text-only bleibt, wenn Bildquellen vorhanden sind."""
    if re.search(r"!\[[^\]]*\]\(https?://", summary or ""):
        return summary
    visuals = collect_visuals_from_research(all_research)
    if not visuals:
        return summary

    lines = [summary.rstrip(), "", "## Bildsignale aus den Quellen", ""]
    for label, url in visuals:
        lines.append(f"![{label}]({url})")
        lines.append(f"*{label}*")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def extract_markdown_link(text: str) -> tuple[str, str]:
    """Liest den ersten Markdown-Link aus einer Quellenmeldung."""
    match = re.search(r"\[([^\]]{1,80})\]\((https?://[^)]+)\)", text or "")
    if match:
        return match.group(1).strip(), match.group(2).strip()
    match = re.search(r"https?://[^\s)>\]}\"']+", text or "")
    if match:
        return "Quelle", match.group(0).rstrip(".,;")
    return "", ""


def extract_image_url(text: str) -> str:
    match = re.search(r"!\[[^\]]*\]\((https?://[^)]+)\)", text or "")
    return match.group(1).strip() if match else ""


def split_source_context(text: str) -> tuple[str, str]:
    """Trennt Meldungstext und angereicherten Artikelkontext."""
    context_match = re.search(r"\n?Kontext:\s*(.+)", text or "", flags=re.S)
    if not context_match:
        return text, ""
    context = context_match.group(1).strip()
    main = (text or "")[:context_match.start()].strip()
    return main, context


def readable_source_text(text: str) -> str:
    """Bereitet Rohmeldung fuer sichtbare Zusammenfassungen auf."""
    main, context = split_source_context(text)
    cleaned = clean_visible_source_text(main)
    cleaned = re.sub(r"Bild:\s*!\[[^\]]*\]\([^)]+\)", "", cleaned)
    cleaned = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", cleaned)
    cleaned = re.sub(r"\s*→\s*(LinkedIn|Quelle|Google News|DAZ)\b", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -*")
    context = re.sub(r"\s+", " ", context).strip()
    if context and context.lower() not in cleaned.lower():
        cleaned = f"{cleaned} {context}"
    return cleaned[:1400].strip()


def strip_source_noise(text: str) -> str:
    """Entfernt technische Quellenpräfixe, damit daraus redaktioneller Text werden kann."""
    main, _context = split_source_context(text)
    cleaned = clean_visible_source_text(main)
    cleaned = re.sub(r"Bild:\s*!\[[^\]]*\]\([^)]+\)", "", cleaned)
    cleaned = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", cleaned)
    cleaned = re.sub(r"\s*→\s*(LinkedIn|Quelle|Google News|DAZ)\b", "", cleaned)
    cleaned = cleaned.replace("**", "")
    cleaned = re.sub(
        r"^(LinkedIn|News/RSS|RSS|Personal|Automatisierung|Ausschreibung|"
        r"Branchenstimme|Sonstiges|Kuratierte Quellen)\s*\|\s*",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"^(Top-Thema|Management|Kassenradar|Institutionen/Politik|IT/Digital/Beschaffung|LinkedIn|Marktsignal|Quelle)\s*\|\s*",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"^(Primärquelle|Pressemitteilung|Verbands-/Institutionsseite|LinkedIn-Signal|Medienbericht|Sonstiger Hinweis|Marktquelle)\s*\|\s*",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(r"^[^:]{1,90}\s+\((LinkedIn|News/RSS|RSS)\)\s*:?\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^\[[0-9?.-]+\]\s*", "", cleaned)
    cleaned = re.sub(r"\bEinordnung:\s*", "", cleaned)
    cleaned = re.sub(r"\s+\b(Quelle|LinkedIn|Zum Artikel|Google News|DAZ)\b\s*$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -*")
    return cleaned


def source_kind(item: dict, label: str = "", url: str = "") -> str:
    blob = f"{item.get('section', '')} {item.get('text', '')} {label} {url}".lower()
    if "linkedin" in blob:
        return "LinkedIn"
    if "ausschreibung" in blob or "ted" in blob or "vergabe" in blob:
        return "Vergabe"
    if "rss" in blob or "news" in blob:
        return "News/RSS"
    return "Marktquelle"


def source_date_hint(text: str) -> str:
    match = re.search(r"\[([0-9]{2}\.[0-9]{2}\.[0-9]{4})\]", text or "")
    return match.group(1) if match else ""


def headline_hint_from_text(text: str, fallback: str = "Marktsignal") -> str:
    cleaned = strip_source_noise(text)
    cleaned = re.sub(r"^\w+\s+\|\s*", "", cleaned)
    if ":" in cleaned[:160]:
        candidate = cleaned.split(":", 1)[0]
    else:
        candidate = " ".join(cleaned.split()[:12])
    candidate = re.sub(r"\s+", " ", candidate).strip(" -*")
    if len(candidate) < 8:
        candidate = fallback
    if len(candidate) > 90:
        candidate = candidate[:87].rstrip() + "..."
    return candidate


def build_editorial_source_items(all_research: str, limit: int | None = None) -> list[dict]:
    """Baut aus Rohmeldungen eine saubere Quellenliste für den redaktionellen Schritt."""
    result: list[dict] = []
    seen: set[str] = set()
    max_items = limit or MAX_NEWSLETTER_SOURCES

    for item in _extract_candidate_items(all_research):
        raw_text = item.get("text", "")
        if not raw_text or is_low_signal_text(raw_text):
            continue
        label, url = extract_markdown_link(raw_text)
        cleaned = strip_source_noise(raw_text)
        if len(cleaned) < 35:
            continue
        context = split_source_context(raw_text)[1]
        quality_blob = f"{item.get('section', '')} {item.get('kasse', '')} {cleaned} {context}".lower()
        if _is_low_value_account_signal(quality_blob):
            continue
        if (
            contains_any(quality_blob, EXCLUDE_TOPIC_TERMS)
            and not _is_strategic_event_signal(quality_blob)
            and not _hard_signal_reason(quality_blob)
        ):
            continue
        key = normalize_item_key(f"{url} {cleaned}")
        if key in seen:
            continue
        seen.add(key)
        source_id = f"S{len(result) + 1:02d}"
        kind = source_kind(item, label, url)
        if kind == "LinkedIn":
            reject_reason = _linkedin_quality_reject_reason(cleaned)
            if reject_reason:
                continue
        org = clean_visible_source_text(item.get("kasse", "") or "")
        org = re.sub(r"\s+\((LinkedIn|News/RSS|RSS)\)$", "", org, flags=re.I)
        result.append({
            "id": source_id,
            "kind": kind,
            "org": org or "Markt",
            "date": source_date_hint(raw_text),
            "headline": headline_hint_from_text(raw_text, org or "Marktsignal"),
            "text": cleaned[:1200],
            "context": re.sub(r"\s+", " ", context).strip()[:900],
            "url": url,
            "link_label": label,
            "image": extract_image_url(raw_text),
        })
        if len(result) >= max_items:
            break
    return result


def build_editorial_source_pack(all_research: str, limit: int | None = None) -> tuple[str, list[dict]]:
    """Formatiert Quellen so, dass das Modell keine Rohüberschriften kopiert."""
    items = build_editorial_source_items(all_research, limit)
    blocks: list[str] = []
    for item in items:
        lines = [
            f"[{item['id']}] {item['kind']} | Organisation/Account: {item['org']}",
            f"Titelhinweis: {item['headline']}",
            f"Text: {item['text']}",
        ]
        if item["date"]:
            lines.insert(1, f"Datum: {item['date']}")
        if item["context"]:
            lines.append(f"Artikelkontext: {item['context']}")
        if item["url"]:
            lines.append(f"URL: {item['url']}")
        if item["image"]:
            lines.append(f"Bild: {item['image']}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks), items


def _flat_source_link(item: dict) -> str:
    url = (item.get("url") or "").strip().rstrip(".,;")
    return f"[Quelle]({url})" if url else ""


def _flat_clean_phrase(text: str) -> str:
    cleaned = clean_visible_source_text(text or "")
    cleaned = strip_source_noise(cleaned)
    cleaned = re.sub(r"\b(Signal|Einordnung|Quelle)\s*:\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(
        r"^(Management|Top[- ]?Thema|Kassenradar|Institutionen/Politik|IT/Digital/Beschaffung|LinkedIn|Marktsignal|Marktquelle|News/RSS|Quelle)\s+",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"^(Management|Top[- ]?Thema|Kassenradar|Institutionen/Politik|IT/Digital/Beschaffung|LinkedIn|Marktsignal|Marktquelle|News/RSS|Quelle)\s+",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"\bKonkretes Digital-/IT-/Prozesssignal mit moeglicher Folge fuer Betrieb, Service, Daten oder Plattformen\.?",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"\bRegulatorik- oder Betriebsdruck: Kassen muessen Fristen, Schnittstellen, Kommunikation, Sicherheit und Dienstleistersteuerung operativ zusammenbringen\.?",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(r"\b(Event|Messe|Netzwerkevent)\.?", "", cleaned, flags=re.I)
    return re.sub(r"\s+", " ", cleaned).strip(" -*:")


def _flat_title(item: dict) -> str:
    title = _flat_clean_phrase(item.get("headline", "") or item.get("org", "") or "Fundstueck")
    title = re.sub(r"^(Markt|Quelle|News/RSS|LinkedIn)\s*:\s*", "", title, flags=re.I)
    title = re.sub(r"\s+", " ", title).strip(" -*:")
    return title[:120].rstrip() or "Fundstueck"


def _flat_relevance(item: dict) -> str:
    blob = f"{item.get('org', '')} {item.get('headline', '')} {item.get('text', '')}".lower()
    if contains_any(blob, BITMARCK_KUNDENTAG_TERMS):
        return "Relevant, weil der BITMARCK-Kundentag ein zentraler Treffpunkt der GKV-IT ist und Hinweise auf Produktroadmap, Plattformstrategie, Daten-, Cloud-, KI-, Service- und Betriebsprioritaeten der BITMARCK-Kassen liefern kann."
    if "itsc" in blob and contains_any(blob, {"zukunftskongress", "zukunfts kongress"}):
        return "Relevant, weil der ITSC-Zukunftskongress als Plattformsignal fuer ITSC-nahe Kassen Hinweise auf Betriebs-, Plattform-, Service-, Daten- und Transformationsprioritaeten liefern kann."
    if "dmea" in blob and contains_any(blob, EVENT_SUBSTANCE_TERMS):
        return "Relevant, wenn ein Entscheider seine DMEA-Highlights fachlich einordnet, weil daraus Prioritaeten zu Digital Health, Daten, KI, Interoperabilitaet, ePA/TI oder Versorgungsprozessen sichtbar werden."
    if contains_any(blob, {"reform", "finanzierung", "finanzierungsdruck", "strukturreform"}):
        return "Relevant, weil Reform- und Finanzierungsdruck die Budget-, Priorisierungs- und Umsetzungslogik der Krankenkassen beeinflusst und damit auch IT-, Prozess- und Dienstleisterentscheidungen rahmt."
    if item.get("kind") == "Vergabe":
        return "Relevant, weil Leistungsbild, Zuständigkeiten oder Budgetfenster sichtbar werden und daraus Prozess-, Integrations-, Betriebs- oder Beratungsbedarf entstehen kann."
    if contains_any(blob, {"fusion", "zusammenschluss"}):
        return "Relevant, weil Konsolidierung typischerweise Datenmigration, Systemharmonisierung, Versichertenkommunikation, Serviceprozesse und Dienstleistersteuerung berührt."
    if contains_any(blob, DEDICATED_GKV_PROVIDERS):
        return "Relevant, weil ein GKV-naher Dienstleister sichtbar wird; daraus koennen Hinweise auf Plattformstrategie, Betriebsmodell, Projektbewegung, Abhaengigkeiten oder Wettbewerbsdynamik entstehen."
    if contains_any(blob, {"gematik", "epa", "e-pa", "ti ", "egk", "vsdm", "bsi", "nis2", "kritis", "gedig", "gesundheitsdatennutzung"}):
        return "Relevant, weil daraus Umsetzungsdruck bei Fristen, Schnittstellen, Kommunikation, Datenschutz, Sicherheit und Dienstleistersteuerung entstehen kann."
    if contains_any(blob, STRATEGIC_TOPIC_TERMS):
        return "Relevant, weil das Fundstueck eine konkrete Verschiebung bei Digitalisierung, Daten, Service, Prozessen, Betrieb oder Kassenstrategie anzeigen kann."
    return ""


def _flat_summary_text(item: dict, limit: int = 520) -> str:
    title = _flat_title(item)
    raw = item.get("context") or item.get("text") or ""
    text = _flat_clean_phrase(readable_source_text(raw))
    text = re.sub(r"^(Markt|Quelle|News/RSS|LinkedIn)\s*:\s*", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -*:")
    if text.lower().startswith(title.lower()):
        text = text[len(title):].strip(" :-")
    relevance = _flat_relevance(item)
    combined = f"{title}: {text}" if text else title
    if relevance and relevance.lower() not in combined.lower():
        combined = f"{combined} {relevance}"
    combined = re.sub(r"\s+", " ", combined).strip()
    if len(combined) > limit:
        combined = combined[: limit - 3].rstrip() + "..."
    return combined


def _dedupe_flat_items(items: list[dict], limit: int | None = None) -> list[dict]:
    deduped: list[dict] = []
    seen_urls: set[str] = set()
    seen_titles: set[str] = set()
    seen_texts: set[str] = set()
    for item in items:
        url = (item.get("url") or "").strip().rstrip(".,;")
        title_key = normalize_item_key(_flat_title(item))
        text_key = normalize_item_key(_flat_summary_text(item, limit=320))
        if url and url in seen_urls:
            continue
        if title_key and title_key in seen_titles:
            continue
        if text_key and text_key in seen_texts:
            continue
        if url:
            seen_urls.add(url)
        seen_titles.add(title_key)
        seen_texts.add(text_key)
        deduped.append(item)
        if limit and len(deduped) >= limit:
            break
    return deduped


def newsletter_needs_repair(text: str, source_count: int = 0) -> bool:
    """Erkennt Outputs, die noch wie Rohdaten statt Branchenbrief wirken."""
    cleaned = text or ""
    if len(cleaned.strip()) < 500:
        return True
    raw_markers = (
        "LinkedIn |",
        "News/RSS",
        "Einordnung:**",
        "Kuratierte Quellen",
        "Rohsignal",
        "Quellenradar",
        "GKV-Markt**",
        "TK**",
        "DAK**",
        "Warm-up fuer Account-Recherche",
        "## Management Summary",
        "## Account-Intelligence",
        "## Beobachtungsliste",
        "## Quellenbasis",
        "Zahnzusatzversicherung",
        "WINGCOPTER",
        "University of Kassel",
        "## Management Summary",
        "## Top-Themen",
        "## Kassenradar",
        "## Institutionen",
        "## IT-, Digital",
        "## LinkedIn",
        "## Marktsignale",
        "## Relevanz fuer mich",
        "## Quellenuebersicht",
        "**Signal:**",
        "**Einordnung:**",
        "**Quelle:**",
    )
    if any(marker in cleaned for marker in raw_markers):
        return True
    heading_count = len(re.findall(r"^###\s+", cleaned, flags=re.M))
    long_paragraphs = len([
        line for line in cleaned.splitlines()
        if len(line.strip()) > 180 and not line.lstrip().startswith(("#", "-", "*", "!["))
    ])
    if source_count >= 12 and heading_count >= 8 and long_paragraphs < heading_count * 2:
        return True
    return False


# ---------------------------------------------------------------------------
# System-Prompt (einmalig, gecacht)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Du bist Redakteur des persoenlichen Wochenbriefs "KassenInfodienst".
Der Dienst richtet sich an Christian Galler in seiner Rolle als Account Manager
im IT-Vertrieb fuer gesetzliche Krankenkassen. Leitfrage jeder Ausgabe: Was muss
Christian diese Woche wissen, um Markt, Kunden, relevante Personen und
Bewegungen in der GKV-IT-Landschaft besser einzuordnen?

Er soll kein allgemeiner Pressespiegel und keine Linksammlung sein, sondern ein
entscheidungsorientiertes Branchenbriefing: Was ist passiert, warum ist es relevant,
welcher IT-/Digital-/Regulatorik-/Beschaffungsdruck oder welche Marktbewegung
entsteht und was ist daraus fuer Account Management, Business Development und
Networking abzuleiten?

Der bestehende KassenInfodienst bleibt erkennbar: Markdown-Rubriken, pointierte
redaktionelle Sprache, Quellenlinks, keine leeren Platzhalter. Der Stil ist
professionell, praezise, meinungsstark, aber nicht boulevardesk.

RELEVANZKERN:
- Harte Fakten aus Politik, Regulierung und Institutionen: Gesetzesvorhaben,
  Stellungnahmen, Fristen, gematik-/TI-/ePA-Vorgaben, BSI/KRITIS/NIS2,
  GKV-Spitzenverband, BMG, Verbandspositionen und daraus folgende Umsetzungslast
- Breite IT-Themen in der GKV-Welt: IT-Vorhaben, Plattformen, Apps, Portale,
  Service- und Prozessmodernisierung, Daten, KI, Automatisierung, Cloud, Betrieb
- Fusionen, Kassenkooperationen, gemeinsame IT-Projekte, Plattformverbünde,
  Dienstleisterwechsel, Konsolidierung, Shared Services und Verbundvorhaben
- Kassen-eigene RSS-/News-Signale auch dann aufnehmen, wenn sie weicher sind,
  sofern sie Positionierung, Versorgungsstrategie, Servicefokus, Prävention,
  Mitgliederkommunikation, Kooperationen oder Themenverschiebungen einer Kasse zeigen
- Vorstands-, CIO-, CDO-, CTO-, CEO-, Bereichsleitungs- und Pressestellen-Aussagen
- Projektbedarf, Modernisierungsdruck, Ausschreibungsnaehe, Dienstleisterwechsel,
  Personalaufbau in IT-/Digitalrollen oder strategische Bewegungen
- Marktsignale von Kassen, BMG, GKV-Spitzenverband, gematik, BSI, AOK-BV, vdek,
  IKK e.V., BKK Dachverband, BITMARCK, ITSC und vergleichbaren Akteuren

LINKEDIN ALS QUALIFIZIERTE TOP-VOICE-QUELLE:
Bevorzugt aufnehmen: Vorstand, Geschaeftsfuehrung, CEO, CIO, CDO, CTO,
Bereichsleitung IT/Digitalisierung/Versorgung/Strategie, offizielle
Kommunikation, Pressestellen, relevante Verbands- und Institutionsvertreter
sowie praegende Stimmen aus der Kassen- und GKV-IT-Landschaft. Beispiele: Chef
der DAK-Pressestelle, IKK-classic-CDO Stefan Schellberg, BITMARCK-CEO Andreas
Strausfeld, ITSC-CEO Dieter Loewe. Ignorieren: Sachbearbeiter, Recruiter,
generische Vertriebsrollen, Event-Selfies ohne fachliche Aussage,
Karrieremeldungen ohne Marktbezug, Employer Branding, reine Glueckwuensche,
Likes/Reposts ohne eigene Einordnung oder generisches Sales-/Partner-Marketing.

REDAKTIONELLE REGELN:
- Qualitaet vor Menge. Schwache Wochen nicht kuenstlich aufblasen.
- Keine KI-Floskeln, keine Allgemeinplaetze, keine Debug-/Score-Artefakte.
- Weiche Beobachtungen ausdruecklich als Signal, Hinweis oder Interpretation markieren.
- Jede relevante Meldung mit Quelle, Datum/Zeitraum und belastbarer Einordnung verbinden.
- Wenn nichts Relevantes vorliegt: lieber weglassen als Platzhalter schreiben.
- Sprache: Deutsch, professionell, persoenlich verwertbar, entscheidungsorientiert.
"""

# ---------------------------------------------------------------------------
# TED-Ausschreibungen (EU-Vergabeplattform, kostenlos, kein API-Key nötig)
# ---------------------------------------------------------------------------

TED_API = "https://api.ted.europa.eu/v3/notices/search"
TED_FIELDS = [
    "publication-number", "notice-title", "buyer-name",
    "classification-cpv", "total-value", "estimated-value-proc",
    "publication-date", "notice-type",
]


def search_ted_tenders(kassen: list[dict], tage: int) -> str:
    """Sucht TED-Ausschreibungen aller Wertgrenzen für alle Kassen in einem API-Call.

    Gibt einen formatierten Markdown-Block zurück, der direkt in den Newsletter
    als Kontext für den 'Ausschreibungen'-Abschnitt einfließt.
    Hinweis: TED enthält nur EU-schwellenwertüberschreitende Ausschreibungen
    (Liefer-/DL ab ~143k€, Bau ab ~5,5 Mio€). UVgO-Ausschreibungen (national)
    werden auf DTVP/subreport veröffentlicht, nicht auf TED.
    """
    today = date.today()
    start_date = (today - timedelta(days=tage)).strftime("%Y%m%d")

    # Minimale Query: nur Land + Datum (Klammern/Sonderzeichen in Kassennamen
    # können den TED-Query-Parser brechen → alle Textfilter in Python)
    query = f"buyer-country = DEU AND publication-date >= {start_date}"

    payload = {
        "query": query,
        "fields": TED_FIELDS,
        "limit": 250,
        "scope": "ALL",
        "paginationMode": "PAGE_NUMBER",
        "page": 1,
    }

    try:
        resp = req.post(TED_API, json=payload, timeout=20)
        if not resp.ok:
            print(f"   ⚠️  TED-API Fehler {resp.status_code}: {resp.text[:500]}", file=sys.stderr)
            return ""
        data = resp.json()
    except Exception as e:
        print(f"   ⚠️  TED-API nicht erreichbar: {e}", file=sys.stderr)
        return ""

    # Alle Filter in Python: Kassennamen + Mindestwert
    kassen_namen = {k["name"].lower() for k in kassen}
    kassen_shorts = {k["short"].lower() for k in kassen}

    def _buyer_str(buyer_field) -> str:
        """buyer-name ist ein mehrsprachiges Objekt {"de": "...", "en": "..."} oder string."""
        if isinstance(buyer_field, dict):
            return " ".join(str(v) for v in buyer_field.values()).lower()
        return str(buyer_field or "").lower()

    def is_relevant_kasse(buyer_field) -> bool:
        b = _buyer_str(buyer_field)
        return (
            any(name in b or b in name for name in kassen_namen)
            or any(short in b.split() for short in kassen_shorts)
        )

    def _notice_value(n: dict) -> float:
        """Liest den Wert aus total-value oder estimated-value-proc (beide können vorkommen)."""
        tv = n.get("total-value")
        if isinstance(tv, dict):
            return float(tv.get("amount") or tv.get("value") or 0)
        if isinstance(tv, (int, float)):
            return float(tv)
        ev = n.get("estimated-value-proc")
        if isinstance(ev, dict):
            return float(ev.get("amount") or ev.get("value") or 0)
        if isinstance(ev, (int, float)):
            return float(ev)
        return 0.0

    def _cpv_codes(n: dict) -> list[str]:
        raw = n.get("classification-cpv") or []
        if isinstance(raw, (str, int)):
            raw = [raw]
        codes: list[str] = []
        for item in raw:
            if isinstance(item, dict):
                item = item.get("code") or item.get("id") or item.get("value") or ""
            match = re.search(r"\d{2,8}", str(item))
            if match:
                codes.append(match.group(0))
        return codes

    def _is_relevant_tender(n: dict) -> bool:
        value = _notice_value(n)
        if value < MIN_TED_VALUE_EUR:
            return False

        title = str(n.get("notice-title") or "").lower()
        cpvs = _cpv_codes(n)
        it_cpv_prefixes = (
            "30",   # computer equipment
            "32",   # telecom equipment
            "48",   # software package
            "64",   # telecom services
            "72",   # IT services
            "793",  # market/economic research
            "794",  # business/management consulting
            "795",  # office support/call centre
        )
        title_include = {
            "software", "it-", " it ", "cloud", "daten", "plattform", "portal",
            "dms", "ecm", "crm", "ki", "künstliche intelligenz", "automatisierung",
            "digital", "cyber", "security", "informationssicherheit", "servicecenter",
            "callcenter", "prozess", "beratung", "rechenzentrum", "hosting",
        }
        title_exclude = {
            "reinigung", "bürobedarf", "mobiliar", "möbel", "papier",
            "postdienst", "catering", "gebäudereinigung", "strom", "gas",
        }
        if any(word in title for word in title_exclude):
            return False
        return (
            any(code.startswith(it_cpv_prefixes) for code in cpvs)
            or any(word in f" {title} " for word in title_include)
        )

    notices = [
        n for n in data.get("notices", [])
        if is_relevant_kasse(n.get("buyer-name")) and _is_relevant_tender(n)
    ]
    if not notices:
        return ""

    # Ergebnisse formatieren (nach Kasse gruppiert)
    lines = ["## 💎 TED-Ausschreibungen (via TED API)\n"]
    for n in notices:
        pub_num = n.get("publication-number", "")
        title   = (n.get("notice-title") or "Ohne Titel")
        buyer   = _buyer_str(n.get("buyer-name")) or "Unbekannt"
        value   = _notice_value(n)
        pub_dt  = n.get("publication-date", "")
        cpv_raw = n.get("classification-cpv") or []
        cpvs    = ", ".join(str(c) for c in cpv_raw) if cpv_raw else ""
        url     = f"https://ted.europa.eu/en/notice/{pub_num}"

        value_str = f"ca. {value/1_000_000:.1f} Mio €" if value else "Volumen unbekannt"
        lines.append(
            f"- **{buyer}** | {title} | {value_str} | CPV: {cpvs} | {pub_dt}"
            f"\n  🔗 [{pub_num}]({url})\n"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LinkedIn via LinkdAPI (100 Free Credits, dann kostenpflichtig)
# ---------------------------------------------------------------------------

def scrape_linkedin_linkdapi(kassen: list[dict], tage: int) -> str:
    """Sucht LinkedIn-Posts via LinkdAPI (linkdapi.com).

    Findet Posts von Personen UND Unternehmen die den Kassennamen erwähnen.
    Benötigt: LINKDAPI_KEY Umgebungsvariable.
    Kostenmodell: 1 Credit pro Suche. 100 Free Credits beim Start.
    """
    api_key = os.environ.get("LINKDAPI_KEY", "").strip()
    if not api_key:
        return ""

    try:
        from linkdapi import LinkdAPI
    except ImportError:
        print("   ⚠️  linkdapi nicht installiert – pip install linkdapi", file=sys.stderr)
        return ""

    today = date.today()
    cutoff = today - timedelta(days=tage)
    cutoff_ts_ms = int(datetime(cutoff.year, cutoff.month, cutoff.day).timestamp()) * 1000

    client = LinkdAPI(api_key)
    all_findings: list[str] = []
    post_count = 0
    raw_post_count = 0
    dropped_non_decision = 0
    dropped_no_context = 0
    dropped_no_topic = 0
    dropped_duplicate = 0
    seen_global_posts: set[str] = set()
    linkedin_audit: list[dict] = []

    market_target = {
        "name": "GKV & IT Markt",
        "short": "GKV-Markt",
        "type": "market",
        "linkedin_search": "GKV IT",
        "linkedin_queries": LINKEDIN_MARKET_QUERIES,
    }

    for kasse in [market_target] + kassen:
        # LinkedIn-Queries: Kassen direkt, Dienstleister mit GKV/Krankenkassen-Kontext.
        raw_posts: list[dict] = []
        if kasse.get("type") == "provider":
            search_terms = kasse.get("linkedin_queries") or [
                f"{kasse['linkedin_search']} GKV",
                f"{kasse['linkedin_search']} Krankenkasse",
            ]
        elif kasse.get("type") == "influencer":
            search_terms = kasse.get("linkedin_queries") or [
                kasse["linkedin_search"],
                f"{kasse['linkedin_search']} KI Gesundheit",
                f"{kasse['linkedin_search']} Health IT",
                f"{kasse['linkedin_search']} Krankenkasse GKV",
            ]
        elif kasse.get("type") == "market":
            search_terms = kasse.get("linkedin_queries") or LINKEDIN_MARKET_QUERIES
        else:
            company = kasse["linkedin_search"]
            search_terms = kasse.get("linkedin_queries") or [
                company,
                f"{company} Vorstand CIO CDO",
                f"{company} IT Digitalisierung Service",
                f"{kasse['short']} Krankenkasse Projekt",
            ]

        # Reihenfolge beibehalten, Duplikate entfernen.
        search_terms = list(dict.fromkeys(search_terms))
        query_limit = (
            len(search_terms)
            if kasse.get("type") in {"market", "influencer"}
            else int(kasse.get("linkedin_query_limit") or LINKEDIN_QUERY_LIMIT)
        )
        search_requests = [
            {"keyword": term, "date_posted": "past-month", "sort_by": "date_posted"}
            for term in search_terms[:query_limit]
        ]
        if kasse.get("type") not in {"market", "influencer"}:
            search_requests.append(
                {"author_company": kasse["linkedin_search"], "date_posted": "past-month", "sort_by": "date_posted"}
            )

        for search_kwargs in search_requests:
            search_type = list(search_kwargs.keys())[0]
            # Retry-Loop mit Exponential-Backoff bei 429
            for attempt in range(3):
                try:
                    result = client.search_posts(**search_kwargs)
                    if isinstance(result, dict) and result.get("success"):
                        posts = result.get("data", {})
                        if isinstance(posts, dict):
                            posts = posts.get("posts") or posts.get("elements") or posts.get("items") or []
                        for post in posts:
                            if isinstance(post, dict):
                                post["_audit_query"] = search_kwargs.get("keyword") or search_kwargs.get("author_company") or ""
                                raw_posts.append(post)
                    break  # Erfolg
                except Exception as e:
                    err_str = str(e)
                    if "429" in err_str and attempt < 2:
                        wait = 15 * (attempt + 1)  # 15s, 30s
                        print(f"   ⏳ LinkdAPI 429 – warte {wait}s ({kasse['short']} {search_type}) ...", file=sys.stderr)
                        time.sleep(wait)
                    else:
                        print(f"   ⚠️  LinkdAPI {kasse['short']} ({search_type}): {e}", file=sys.stderr)
                        break
            time.sleep(1.2)

        # Duplikate entfernen (gleicher Post-Text)
        seen_texts: set[str] = set()
        findings: list[str] = []
        raw_post_count += len(raw_posts)

        for post in raw_posts:

            # Zeitstempel prüfen (kann int, str oder dict sein)
            ts_raw = post.get("postedAt") or post.get("createdAt") or post.get("timestamp") or 0
            if isinstance(ts_raw, dict):
                ts = int(ts_raw.get("time") or ts_raw.get("timestamp") or ts_raw.get("value") or 0)
            elif isinstance(ts_raw, str):
                try:
                    ts = int(ts_raw)
                except ValueError:
                    ts = 0
            else:
                ts = int(ts_raw or 0)
            if ts and ts < cutoff_ts_ms:
                continue

            # Text
            text = (
                post.get("text") or post.get("content") or
                post.get("commentary") or post.get("description") or ""
            ).strip()
            if not text or len(text) < 20:
                continue

            # Autor + Titel (für Entscheider-Filter)
            author = post.get("author") or post.get("actor") or {}
            if isinstance(author, dict):
                actor_name = author.get("name") or author.get("fullName") or kasse["short"]
                actor_title = (author.get("headline") or author.get("title") or "").lower()
            else:
                actor_name = str(author) or kasse["short"]
                actor_title = ""

            # Reaktionen zuerst lesen (brauchen wir für den Filter)
            likes = post.get("numLikes") or post.get("likes") or 0
            comments = post.get("numComments") or post.get("comments") or 0
            reactions = int(likes) + int(comments)

            post_url = find_url_in_obj(post, ("linkedin.com",))
            audit_base = {
                "organization": kasse.get("short") or kasse.get("name") or "",
                "actor": actor_name,
                "role": actor_title,
                "query": post.get("_audit_query", ""),
                "text": text[:1200],
                "url": post_url,
                "date": datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d") if ts else "",
            }
            text_key = normalize_item_key(f"{actor_name} {text}")
            post_key = post_url or text_key
            if text_key in seen_texts or post_key in seen_global_posts:
                dropped_duplicate += 1
                linkedin_audit.append({**audit_base, "status": "verworfen", "reason": "Duplikat"})
                continue
            seen_texts.add(text_key)
            seen_global_posts.add(post_key)

            # Relevanz-Filter:
            #   Person: nur Entscheider/Fuehrung, keine Sachbearbeiter
            #   Dienstleister: nur mit GKV-Kontext
            ENTSCHEIDER = {
                "vorstand", "vorständin", "vorstandsvorsitz", "ceo", "cio", "cto", "cdo",
                "cco", "coo", "cfo", "chief", "geschäftsführer", "geschäftsführerin",
                "vorsitzender", "vorsitzende", "hauptgeschäftsführer",
                "geschäftsbereichsleiter", "leiter geschäftsbereich",
                "bereichsleiter", "head of", "it-leiter", "digitalisierungsleiter",
                "director", "direktor", "direktorin", "leitung digital", "leitung it",
                "leitung versorgung", "leitung finanzen", "pressesprecher",
            }
            NICHT_ENTSCHEIDER = {
                "sachbearbeiter", "kundenberater", "kundenservice", "recruiter",
                "recruiting", "talent acquisition", "praktikant", "werkstudent",
                "student", "azubi", "auszubild", "beraterin kunden", "berater kunden",
            }
            THEMEN_IT = {
                "ki ", "künstliche intelligenz", "automatisierung", "digitalisierung",
                "software", "cloud", "plattform", " api ", "daten", "system",
                "it-", "cyber", "sicherheit", "technologie", "agil", "scrum",
                "portal", "app", "online", "service", "kundenservice",
                "servicecenter", "kontaktcenter", "omnichannel", "prozess",
                "prozessoptimierung", "innovation",
                "strategie", "transformation", "projekt", "kooperation", "go-live",
                "golive", "rollout", "einführung", "implementierung", "migration",
                "kunde", "kundin", "zuschlag", "auftrag", "livegang", "release",
                "telematik", "gematik", "ti ", "e-rezept", "diga",
                "versichertenservice", "digital health", "data", "analytics",
                "versorgung", "versorgungsmanagement", "selektivvertrag",
                "gesundheitswesen", "healthcare", "health-it", "health it",
                "digital health", "e-health", "krankenhaus", "medizin",
                "patient", "patienten", "telemedizin", "interoperabilität",
                "interoperabilitaet", "epa", "khzg",
                "gesundheitsdaten", "datennutzung", "plattformökonomie",
            }
            THEMEN_BRANCHE = {
                "gkv", "gesundheitspolitik", "krankenversicherung", "krankenkasse",
                "versorgung", "pflege", "finanzierung", "finanzen", "reform",
                "beitrag", "beitragssatz", "zusatzbeitrag", "bundestag",
                "ministerium", "verwaltung", "verwaltungsrat", "vorstand",
                "strategie", "markt", "kunden", "versicherte", "service",
                "qualitaet", "qualität", "innovation",
                "gesundheitswesen", "healthcare", "digital health", "e-health",
                "ki", "künstliche intelligenz", "daten", "regulierung",
            }
            OFFICIAL_ACTORS = {
                "krankenkasse", "aok", "barmer", "dak", "techniker krankenkasse",
                "tk", "ikk", "bkk", "kkh", "sbk", "hkk", "bitmarck", "itsc",
                "aok systems", "gkv informatik", "gevko", "davaso", "spectrumk",
                "adesso", "msg", "materna", "arvato", "sopra steria",
            }
            text_lower = text.lower()
            actor_blob = f"{actor_name} {actor_title}".lower()
            is_provider = kasse.get("type") == "provider"
            is_market = kasse.get("type") == "market"
            is_influencer = kasse.get("type") == "influencer"
            is_entscheider = any(k in actor_title for k in ENTSCHEIDER)
            is_non_decision = any(k in actor_title for k in NICHT_ENTSCHEIDER)
            is_official_market_actor = is_market and any(k in actor_blob for k in OFFICIAL_ACTORS)
            is_named_influencer = is_influencer and (
                kasse["short"].lower() in actor_blob
                or kasse["name"].lower() in actor_blob
                or any(part in actor_blob for part in kasse["short"].lower().split())
            )
            is_company_or_kasse = (
                kasse["short"].lower() in actor_blob
                or kasse["name"].lower() in actor_blob
                or kasse["linkedin_search"].lower() in actor_blob
                or kasse["short"].lower() in text_lower
                or kasse["name"].lower() in text_lower
                or is_official_market_actor
            )
            is_it_thema = any(k in text_lower for k in THEMEN_IT)
            is_dedicated_gkv_provider = (
                is_provider
                and any(provider in f"{kasse['short']} {kasse['name']}".lower() for provider in DEDICATED_GKV_PROVIDERS)
            )
            has_gkv_context = (
                any(term in text_lower for term in GKV_CONTEXT_TERMS)
                or is_dedicated_gkv_provider
                or is_company_or_kasse
            )
            is_branchenthema = any(k in text_lower for k in THEMEN_BRANCHE)

            qualified, drop_reason = evaluate_linkedin_signal(kasse, actor_name, actor_title, text, reactions)
            if not qualified and not (is_influencer and is_branchenthema):
                if "Kontext" in drop_reason or "GKV" in drop_reason:
                    dropped_no_context += 1
                elif "Thema" in drop_reason or "Signal" in drop_reason:
                    dropped_no_topic += 1
                else:
                    dropped_non_decision += 1
                linkedin_audit.append({**audit_base, "status": "verworfen", "reason": drop_reason})
                continue

            if is_non_decision:
                dropped_non_decision += 1
                linkedin_audit.append({**audit_base, "status": "verworfen", "reason": "Nicht-Entscheiderrolle"})
                continue
            if is_provider and not has_gkv_context:
                dropped_no_context += 1
                linkedin_audit.append({**audit_base, "status": "verworfen", "reason": "Dienstleisterpost ohne GKV-Kontext"})
                continue
            if not (is_entscheider or is_company_or_kasse or is_named_influencer):
                dropped_non_decision += 1
                linkedin_audit.append({**audit_base, "status": "verworfen", "reason": "keine Entscheider- oder offizielle Quelle"})
                continue
            if not (
                (is_it_thema and has_gkv_context)
                or (is_influencer and is_branchenthema)
                or (is_entscheider and has_gkv_context)
                or (is_company_or_kasse and has_gkv_context and is_branchenthema)
                or _is_strategic_event_signal(text_lower)
            ):
                dropped_no_topic += 1
                linkedin_audit.append({**audit_base, "status": "verworfen", "reason": "kein belastbares IT-, Digital-, Regulatorik- oder Marktsignal"})
                continue

            post_date = datetime.fromtimestamp(ts / 1000).strftime("%d.%m.%Y") if ts else "?"
            line = f"  - [{post_date}] **{actor_name}**"
            if actor_title:
                line += f" ({actor_title[:60]})"
            line += f": {text[:900].strip()}"
            if reactions:
                line += f" _(👍 {likes} · 💬 {comments})_"
            if post_url:
                line += f" → {source_link(post_url, 'LinkedIn')}"
            else:
                line += " _(Quelle: LinkedIn via LinkdAPI, keine Post-URL geliefert)_"
            findings.append(add_image_marker(line, find_image_in_obj(post)))
            linkedin_audit.append({**audit_base, "status": "an_relevanzfilter_uebergeben", "reason": "LinkdAPI-Vorfilter bestanden"})

        if findings:
            all_findings.append(f"**{kasse['short']}** (LinkedIn):")
            all_findings.extend(findings[:LINKEDIN_POSTS_PER_ACCOUNT])
            all_findings.append("")
            post_count += len(findings[:LINKEDIN_POSTS_PER_ACCOUNT])

    if not all_findings:
        _write_linkedin_audit(linkedin_audit, today)
        print(
            "   LinkedIn LinkdAPI: "
            f"{raw_post_count} Rohposts, 0 behalten "
            f"(Duplikate: {dropped_duplicate}, Nicht-Entscheider: {dropped_non_decision}, ohne GKV-Kontext: {dropped_no_context}, ohne IT/Projekt-Thema: {dropped_no_topic})."
        )
        return ""

    lines = [f"## 📣 LinkedIn-Posts ({post_count} Treffer via LinkdAPI)\n"]
    lines.extend(all_findings)
    print(
        "   LinkedIn LinkdAPI: "
        f"{raw_post_count} Rohposts, {post_count} behalten "
        f"(Duplikate: {dropped_duplicate}, Nicht-Entscheider: {dropped_non_decision}, ohne GKV-Kontext: {dropped_no_context}, ohne IT/Projekt-Thema: {dropped_no_topic})."
    )
    _write_linkedin_audit(linkedin_audit, today)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LinkedIn & RSS-Feed Scraping
# ---------------------------------------------------------------------------

def _parse_rss_xml(xml_text: str, cutoff: date) -> list[tuple[str, str]]:
    """Parst RSS-XML und gibt Liste von (title, link) Tupeln zurück, gefiltert nach cutoff."""
    import xml.etree.ElementTree as ET
    from email.utils import parsedate

    results = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return results

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    # RSS 2.0 items
    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate")
        if title_el is None or link_el is None:
            continue
        title = (title_el.text or "").strip()
        link = (link_el.text or "").strip()
        if not title or not link:
            continue
        if pub_el is not None and pub_el.text:
            parsed = parsedate(pub_el.text)
            if parsed:
                pub_date = date(parsed[0], parsed[1], parsed[2])
                if pub_date < cutoff:
                    continue
        results.append((title, link))
    return results


def _rss_publisher_from_title(title: str) -> str:
    """Google-News-RSS-Titel enthalten den Publisher meist nach dem letzten ' - '."""
    parts = [part.strip() for part in (title or "").rsplit(" - ", 1)]
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return ""


def _write_news_rss_audit(entries: list[dict], today: date) -> None:
    """Speichert die angenommenen News-RSS-Treffer transparent ab."""
    if not entries:
        return
    REPORTS_DIR.mkdir(exist_ok=True)
    json_path = REPORTS_DIR / f"news_rss_sources_{today.isoformat()}.json"
    md_path = REPORTS_DIR / f"news_rss_sources_{today.isoformat()}.md"
    json_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# News-RSS-Quellen {today.strftime('%d.%m.%Y')}",
        "",
        "| Bereich | Organisation | Publisher | Titel | Link |",
        "|---|---|---|---|---|",
    ]
    for entry in entries:
        title = str(entry.get("title", "")).replace("|", "\\|")
        publisher = str(entry.get("publisher", "")).replace("|", "\\|")
        group = str(entry.get("group", "")).replace("|", "\\|")
        org = str(entry.get("organization", "")).replace("|", "\\|")
        link = str(entry.get("link", "")).strip()
        link_md = f"[Quelle]({link})" if link else ""
        lines.append(f"| {group} | {org} | {publisher} | {title} | {link_md} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_linkedin_audit(entries: list[dict], today: date) -> None:
    """Speichert gesehene LinkedIn-Treffer samt Filterentscheidung."""
    if not entries:
        return
    REPORTS_DIR.mkdir(exist_ok=True)
    json_path = REPORTS_DIR / f"linkedin_audit_{today.isoformat()}.json"
    md_path = REPORTS_DIR / f"linkedin_audit_{today.isoformat()}.md"
    json_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# LinkedIn-Audit {today.strftime('%d.%m.%Y')}",
        "",
        "| Status | Organisation | Autor | Rolle | Grund | Query | Text | Link |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for entry in entries:
        status = str(entry.get("status", "")).replace("|", "\\|")
        org = str(entry.get("organization", "")).replace("|", "\\|")
        actor = str(entry.get("actor", "")).replace("|", "\\|")
        role = str(entry.get("role", "")).replace("|", "\\|")[:100]
        reason = str(entry.get("reason", "")).replace("|", "\\|")
        query = str(entry.get("query", "")).replace("|", "\\|")[:120]
        text = str(entry.get("text", "")).replace("|", "\\|").replace("\n", " ")[:260]
        link = str(entry.get("url", "")).strip()
        link_md = f"[LinkedIn]({link})" if link else ""
        lines.append(f"| {status} | {org} | {actor} | {role} | {reason} | {query} | {text} | {link_md} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_curation_audit(entries: list[dict], today: date) -> None:
    """Speichert die zweite Filterstufe: was ging in den Newsletter, was nicht."""
    if not entries:
        return
    REPORTS_DIR.mkdir(exist_ok=True)
    json_path = REPORTS_DIR / f"curation_audit_{today.isoformat()}.json"
    md_path = REPORTS_DIR / f"curation_audit_{today.isoformat()}.md"
    json_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# Kurations-Audit {today.strftime('%d.%m.%Y')}",
        "",
        "| Status | Quelle | Organisation | Score | Grund | Text | Link |",
        "|---|---|---|---:|---|---|---|",
    ]
    for entry in entries:
        status = str(entry.get("status", "")).replace("|", "\\|")
        kind = str(entry.get("kind", "")).replace("|", "\\|")
        org = str(entry.get("organization", "")).replace("|", "\\|")
        score = entry.get("score", "")
        reason = str(entry.get("reason", "")).replace("|", "\\|")
        text = str(entry.get("text", "")).replace("|", "\\|").replace("\n", " ")[:280]
        link = str(entry.get("url", "")).strip()
        link_md = f"[Quelle]({link})" if link else ""
        lines.append(f"| {status} | {kind} | {org} | {score} | {reason} | {text} | {link_md} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def scrape_linkedin_voyager(kassen: list[dict], tage: int) -> str:
    """Scraped LinkedIn-Unternehmensseiten direkt via li_at-Session-Cookie.

    Nutzt LinkedIn's interne Voyager-API – kein externer Dienst, keine Kosten.
    Benötigt: LINKEDIN_LI_AT Umgebungsvariable (Session-Cookie aus Browser-DevTools).

    Cookie-Lebensdauer: ~1 Jahr. Bei Ablauf: li_at in GitHub Secrets erneuern.
    """
    li_at = os.environ.get("LINKEDIN_LI_AT", "").strip()
    if not li_at:
        return ""

    today = date.today()
    cutoff = today - timedelta(days=tage)
    cutoff_ts_ms = int(datetime(cutoff.year, cutoff.month, cutoff.day).timestamp()) * 1000

    session = req.Session()
    BASE_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    # JSESSIONID: direkt aus Env (bevorzugt) oder via Session-Init holen
    jsessionid = os.environ.get("LINKEDIN_JSESSIONID", "").strip()

    if jsessionid:
        print(f"   ✅ JSESSIONID aus Secret gesetzt.")
    else:
        # Fallback: JSESSIONID via LinkedIn-Request holen (kann in CI geblockt werden)
        session.cookies.set("li_at", li_at, domain=".linkedin.com", path="/")
        try:
            resp = session.get(
                "https://www.linkedin.com/feed/",
                headers=BASE_HEADERS,
                timeout=20,
                allow_redirects=True,
            )
            print(f"   LinkedIn Session-Init: HTTP {resp.status_code}, URL={resp.url}")
            jsessionid = session.cookies.get("JSESSIONID") or ""
            print(f"   JSESSIONID erhalten: {'ja' if jsessionid else 'nein'}")
        except Exception as e:
            print(f"   ⚠️  LinkedIn Session-Init fehlgeschlagen: {e}", file=sys.stderr)
            return ""

    if not jsessionid:
        print(
            "   ⚠️  LinkedIn: kein JSESSIONID – bitte LINKEDIN_JSESSIONID als Secret setzen.",
            file=sys.stderr,
        )
        return ""

    # Cookies für alle Requests setzen
    session.cookies.set("li_at", li_at, domain=".linkedin.com", path="/")
    session.cookies.set("JSESSIONID", jsessionid, domain=".linkedin.com", path="/")

    # csrf-token = JSESSIONID ohne umgebende Anführungszeichen
    csrf_token = jsessionid.strip('"')

    API_HEADERS = {
        **BASE_HEADERS,
        "Accept": "application/vnd.linkedin.normalized+json+2.1",
        "x-li-lang": "de_DE",
        "x-li-track": (
            '{"clientVersion":"1.13.12","osName":"web","timezoneOffset":1,'
            '"timezone":"Europe/Berlin","deviceFormFactor":"DESKTOP"}'
        ),
        "x-restli-protocol-version": "2.0.0",
        "csrf-token": csrf_token,
        "Referer": "https://www.linkedin.com/",
    }

    all_findings: list[str] = []
    post_count = 0

    import re as _re

    for kasse in kassen:
        keywords = urllib.parse.quote(kasse["linkedin_search"])

        # Mehrere Endpoint-Varianten probieren (LinkedIn ändert API-Pfade)
        candidate_urls = [
            # Variante 1: blended mit Content-Filter
            (
                "https://www.linkedin.com/voyager/api/search/blended"
                f"?keywords={keywords}&q=all"
                "&filters=List(resultType-%3ECONTENT)"
                "&start=0&count=10"
            ),
            # Variante 2: blended ohne Filter (gibt mixed results, aber Posts sind dabei)
            (
                "https://www.linkedin.com/voyager/api/search/blended"
                f"?keywords={keywords}&q=all&start=0&count=10"
            ),
            # Variante 3: hits endpoint
            (
                "https://www.linkedin.com/voyager/api/search/hits"
                f"?keywords={keywords}&q=all&type=CONTENT&count=10"
            ),
            # Variante 4: feed keyword search
            (
                "https://www.linkedin.com/voyager/api/feed/updates"
                f"?q=keywords&keywords={keywords}&count=10"
            ),
        ]

        raw_elements = []
        for url in candidate_urls:
            try:
                r = session.get(url, headers=API_HEADERS, timeout=15)
                print(f"   🔍 {kasse['short']} → {r.status_code} ({url.split('voyager/api/')[1][:40]})")
                if r.status_code == 200:
                    data = r.json()
                    elements = (
                        data.get("elements")
                        or data.get("data", {}).get("elements", [])
                        or []
                    )
                    # blended liefert Gruppen; Content-Elemente extrahieren
                    if elements and isinstance(elements[0], dict) and "elements" in elements[0]:
                        flat = []
                        for group in elements:
                            flat.extend(group.get("elements", []))
                        elements = flat
                    if elements:
                        print(f"      ✅ {len(elements)} Elemente")
                        print(f"      Erstes Element (keys): {list(elements[0].keys())[:8]}")
                        raw_elements = elements
                        break
                    else:
                        # Keine Elemente – zeige rohe Antwort für Diagnose
                        print(f"      Rohantwort (500 Zeichen): {r.text[:500]}")
                time.sleep(0.3)
            except Exception as e:
                print(f"      ⚠️  {e}")

        findings: list[str] = []
        for el in raw_elements:
                    # Zeitstempel
                    created = el.get("created", {})
                    ts = created.get("time", 0) if isinstance(created, dict) else int(created or 0)
                    if ts and ts < cutoff_ts_ms:
                        continue

                    # Post-Text aus verschiedenen möglichen Strukturen
                    text = ""
                    for path in [
                        ["commentary", "text", "text"],
                        ["text", "text"],
                        ["description", "text"],
                        ["headline", "text"],
                    ]:
                        node = el
                        for key in path:
                            node = node.get(key, {}) if isinstance(node, dict) else {}
                        if isinstance(node, str) and len(node) > 20:
                            text = node
                            break
                    # Auch in "image"-freien Update-Strukturen suchen
                    if not text:
                        raw = str(el)
                        match = _re.search(r'"text"\s*:\s*"([^"]{30,})"', raw)
                        if match:
                            text = match.group(1)
                    if not text:
                        continue

                    # Autor
                    actor_name = ""
                    for path in [["actor", "name", "text"], ["authorV2", "name"], ["title", "text"]]:
                        node = el
                        for key in path:
                            node = node.get(key, {}) if isinstance(node, dict) else {}
                        if isinstance(node, str) and node:
                            actor_name = node
                            break
                    if not actor_name:
                        actor_name = kasse["short"]

                    # Reaktionen
                    counts = el.get("socialDetail", {}).get("totalSocialActivityCounts", {}) or {}
                    likes = counts.get("numLikes", 0)
                    comments = counts.get("numComments", 0)

                    post_date = datetime.fromtimestamp(ts / 1000).strftime("%d.%m.%Y") if ts else "?"
                    line = f"  - [{post_date}] **{actor_name}**: {text[:300].strip()}"
                    if likes or comments:
                        line += f" _(👍 {likes} · 💬 {comments})_"
                    findings.append(line)

        time.sleep(0.5)

        if findings:
            all_findings.append(f"**{kasse['short']}** (LinkedIn):")
            all_findings.extend(findings[:5])
            all_findings.append("")
            post_count += len(findings[:5])

    if not all_findings:
        return ""

    lines = [f"## 📣 LinkedIn-Direktdaten ({post_count} Posts via li_at-Session)\n"]
    lines.extend(all_findings)
    return "\n".join(lines)


def scrape_linkedin_rss(kassen: list[dict], tage: int) -> str:
    """Fallback: LinkedIn via Google News RSS (kein li_at erforderlich, aber weniger Daten)."""
    today = date.today()
    cutoff = today - timedelta(days=tage)

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    }

    all_findings: list[str] = []
    for kasse in kassen:
        company = kasse["linkedin_search"]
        company_findings: list[str] = []
        for query in [
            f'site:linkedin.com/posts "{company}"',
            f'"{company}" linkedin.com Vorstand CIO Digitalisierung',
        ]:
            rss_url = (
                "https://news.google.com/rss/search?q="
                + urllib.parse.quote(query)
                + "&hl=de&gl=DE&ceid=DE:de"
            )
            try:
                resp = req.get(rss_url, headers=HEADERS, timeout=10)
                if resp.status_code == 200:
                    for title, link in _parse_rss_xml(resp.text, cutoff)[:5]:
                        line = f"  - {title} → {source_link(link)}"
                        company_findings.append(add_source_preview(line, link))
            except Exception:
                pass

        if company_findings:
            all_findings.append(f"**{kasse['short']}** (LinkedIn/RSS):")
            all_findings.extend(company_findings[:4])
            all_findings.append("")

    if not all_findings:
        return ""
    lines = ["## 📣 LinkedIn-RSS-Findings (Fallback ohne li_at)\n"]
    lines.extend(all_findings)
    return "\n".join(lines)


def scrape_news_rss(kassen: list[dict], tage: int) -> str:
    """Schneller News-Fallback via Google News RSS, ohne OpenAI Web Search."""
    today = date.today()
    cutoff = today - timedelta(days=tage)
    after_date = cutoff.strftime("%Y-%m-%d")

    include_terms = {
        "ki", "chatbot", "automatisierung", "software", "cloud", "dms", "portal",
        "cyber", "security", "ausschreibung", "vergabe", "fusion", "zusammenschluss",
        "verwaltungsrat", "bafin", "stellenabbau", "cio", "cdo", "digital",
        "it-", "vorstand", "go-live", "rollout", "implementierung", "migration",
        "auftrag", "zuschlag", "projekt", "kooperation", "kunde",
        "gesundheitswesen", "healthcare", "health-it", "health it", "digital health",
        "ehealth", "e-health", "daten", "plattform", "interoperabilität",
        "interoperabilitaet", "telemedizin", "patienten", "epa", "e-pa", "ti",
        "gematik", "bsi", "nis2", "kritis", "datenschutz",
        "kundentag", "partnertag", "zukunftskongress", "house of health",
        "dienstleister", "bitmarck", "itsc", "msg", "arvato", "materna",
        "sopra steria", "adesso", "davaso", "spectrumk", "gevko",
    }
    exclude_terms = {
        "prävention", "ratgeber", "bonus", "gesundheitstag", "gewinnspiel",
        "podcast", "rezept", "sport", "ernährung",
        "beitragssatz", "zusatzbeitrag",
    }

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    }

    all_findings: list[str] = []
    seen_links: set[str] = set()
    rss_audit: list[dict] = []

    market_findings: list[str] = []
    for query in NEWS_RSS_MARKET_QUERIES:
        rss_url = (
            "https://news.google.com/rss/search?q="
            + urllib.parse.quote(f"{query} after:{after_date}")
            + "&hl=de&gl=DE&ceid=DE:de"
        )
        try:
            resp = req.get(rss_url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                continue
            for title, link in _parse_rss_xml(resp.text, cutoff)[:6]:
                title_lower = title.lower()
                if link in seen_links:
                    continue
                if _is_low_value_account_signal(title_lower):
                    continue
                if any(term in title_lower for term in exclude_terms) and not _is_strategic_event_signal(title_lower):
                    continue
                if not any(term in title_lower for term in include_terms):
                    continue
                if not any(term in title_lower for term in GKV_CONTEXT_TERMS):
                    continue
                seen_links.add(link)
                rss_audit.append({
                    "group": "GKV & IT Markt",
                    "organization": "Markt",
                    "publisher": _rss_publisher_from_title(title),
                    "title": title,
                    "link": link,
                    "query": query,
                })
                line = f"  - {title} → {source_link(link)}"
                market_findings.append(add_source_preview(line, link))
                if len(market_findings) >= NEWS_RSS_MARKET_LIMIT:
                    break
        except Exception as e:
            print(f"   ⚠️  News-RSS Marktquery: {e}", file=sys.stderr)
        if len(market_findings) >= NEWS_RSS_MARKET_LIMIT:
            break

    if market_findings:
        all_findings.append("**GKV & IT Markt** (News/RSS):")
        all_findings.extend(market_findings)
        all_findings.append("")

    for kasse in kassen:
        company = kasse["name"]
        if kasse.get("type") == "provider":
            queries = list(kasse.get("news_queries") or [])
            queries.extend([
                (
                    f'"{company}" '
                    '(GKV OR Krankenkasse OR Krankenkassen OR AOK OR BKK OR TK OR BARMER OR DAK) '
                    '(Projekt OR Go-live OR Rollout OR Implementierung OR Migration OR Zuschlag OR Auftrag OR Kunde OR Plattform OR Cloud OR Daten OR KI) '
                    f'after:{after_date}'
                ),
                (
                    f'"{kasse.get("short", company)}" '
                    '(GKV OR Krankenkasse OR Krankenkassen) '
                    '(Projekt OR Plattform OR Cloud OR Daten OR KI OR Migration OR Betrieb OR Service) '
                    f'after:{after_date}'
                ),
            ])
            queries = [f"{query} after:{after_date}" if "after:" not in query else query for query in queries]
        elif kasse.get("type") == "institution":
            queries = kasse.get("news_queries") or [
                f'"{company}" (GKV OR Krankenkasse OR ePA OR TI OR gematik OR Datenschutz OR NIS2 OR KRITIS OR Digitalisierung OR Gesetz OR Stellungnahme) after:{after_date}'
            ]
            queries = [f"{query} after:{after_date}" if "after:" not in query else query for query in queries]
        elif kasse.get("type") == "influencer":
            queries = kasse.get("news_queries") or [
                f'"{company}" (KI OR Digitalisierung OR "Health IT" OR "Digital Health" OR Gesundheitswesen) after:{after_date}'
            ]
            queries = [f"{query} after:{after_date}" if "after:" not in query else query for query in queries]
        else:
            queries = [(
                f'"{company}" '
                '(KI OR Chatbot OR Automatisierung OR Software OR Cloud OR DMS OR Portal OR '
                'Cybersecurity OR Ausschreibung OR Vergabe OR Fusion OR BaFin OR CIO OR CDO OR Stellenabbau) '
                f'after:{after_date}'
            )]

        findings: list[str] = []
        for query in queries[:4]:
            rss_url = (
                "https://news.google.com/rss/search?q="
                + urllib.parse.quote(query)
                + "&hl=de&gl=DE&ceid=DE:de"
            )
            try:
                resp = req.get(rss_url, headers=HEADERS, timeout=10)
                if resp.status_code != 200:
                    continue
                for title, link in _parse_rss_xml(resp.text, cutoff)[:8]:
                    title_lower = title.lower()
                    if link in seen_links:
                        continue
                    if _is_low_value_account_signal(title_lower):
                        continue
                    if any(term in title_lower for term in exclude_terms) and not _is_strategic_event_signal(title_lower):
                        continue
                    if kasse.get("type") in {"provider", "institution"} and not any(term in title_lower for term in GKV_CONTEXT_TERMS | include_terms):
                        continue
                    if not any(term in title_lower for term in include_terms):
                        continue
                    seen_links.add(link)
                    rss_audit.append({
                        "group": "Account/Organisation",
                        "organization": kasse.get("short") or company,
                        "publisher": _rss_publisher_from_title(title),
                        "title": title,
                        "link": link,
                        "query": query,
                    })
                    line = f"  - {title} → {source_link(link)}"
                    findings.append(add_source_preview(line, link))
                    if len(findings) >= 4:
                        break
            except Exception as e:
                print(f"   ⚠️  News-RSS {kasse['short']}: {e}", file=sys.stderr)
            if len(findings) >= 4:
                break

        if findings:
            all_findings.append(f"**{kasse['short']}** (News/RSS):")
            all_findings.extend(findings)
            all_findings.append("")

    if not all_findings:
        return ""
    _write_news_rss_audit(rss_audit, today)
    lines = ["## 📰 News-RSS-Findings (schneller Scheduled-Run)\n"]
    lines.extend(all_findings)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Kern-Funktion: Einen Batch Kassen recherchieren
# ---------------------------------------------------------------------------

def research_batch(client: openai.OpenAI, batch: list[dict], tage: int) -> str:
    """Recherchiert eine einzelne Krankenkasse mittels OpenAI Web Search."""

    today = date.today()
    cutoff = today - timedelta(days=tage)
    period_start = cutoff.strftime("%d.%m.%Y")
    period_end = today.strftime("%d.%m.%Y")
    after_date = cutoff.strftime("%Y-%m-%d")   # für Google after:-Operator

    # Für Einzelkasse optimierter Prompt (BATCH_SIZE=1)
    k = batch[0] if len(batch) == 1 else None

    if k:
        user_prompt = f"""Recherchiere News-Highlights für: **{k['name']}** | {k['url']}
Suchfenster: {period_start} – {period_end}

Führe genau 3 Web-Suchen durch:

SUCHE 1 – Konkrete IT-/Automatisierungs-News:
"{k['name']}" (KI OR Chatbot OR Automatisierung OR Software OR Cloud OR DMS OR Portal OR Cybersecurity OR Ausschreibung) after:{after_date}
→ Nur konkrete Projekte, Go-lives, Vergaben, Anbieterwechsel oder messbare Vorhaben.

SUCHE 2 – Branchenmedien & Fachpresse:
"{k['name']}" (CIO OR CDO OR IT-Leiter OR Vorstand Digital OR Geschäftsbereich Digitalisierung OR Stellenabbau) after:{after_date}
→ Personal- und Organisationssignale nur berichten, wenn sie Vertriebsrelevanz für IT/BPO/Automatisierung haben.

SUCHE 3 – Flurfunk & Branchengerüchte:
"{k['name']}" (Fusion OR Zusammenschluss OR Verwaltungsrat OR BaFin OR Konflikt OR Streit OR Gerücht OR Insolvenz) after:{after_date}
→ Gossip, politische Konflikte, Fusionsgerüchte, Verwaltungsratszwist, BaFin-Beobachtungen.
  Datum prüfen – nur Inhalte aus {period_start}–{period_end}.

ZEITREGEL: Nur Inhalte aus {period_start}–{period_end} berichten.
Bekannte Vorstandsänderungen (2025, 1.1.2026, 1.4.2026): IGNORIEREN.
NICHT berichten: allgemeine Beitragssatzmeldungen, ePA-Pflicht, Gesundheitsratgeber, Kampagnen,
Awards, Prävention, Selbstlob-Pressemitteilungen ohne konkretes IT-/Strategie-Ereignis.

OUTPUT: Maximal 5 Rohmeldungen mit Datum, Quelle/URL, Kategorie und Vertriebsrelevanz.
Wenn nichts Relevantes: nur "KEINE_HIGHLIGHTS"."""
    else:
        kassen_liste = "\n".join(
            f"- **{ki['name']}** | {ki['url']}"
            for ki in batch
        )
        user_prompt = f"""Recherchiere Highlights (Zeitraum: {period_start} – {period_end}) für:

{kassen_liste}

Maximal {MAX_SEARCHES} gezielte Web-Suchen für den gesamten Batch.
Priorisiere nur harte Signale:
- konkrete IT-/Automatisierungsprojekte
- Ausschreibungen/Vergaben mit IT-, BPO- oder Strategiebezug
- relevante Personalwechsel im Digital-/IT-/Vorstandsbereich
- Fusions-, BaFin-, Verwaltungsrats- oder Stellenabbau-Signale

Keine allgemeinen Beitragssatzmeldungen, Prävention, Awards, Ratgeber, ePA-Pflicht oder Selbstlob-Pressemitteilungen.
Maximal 8 Rohmeldungen für den gesamten Batch, jeweils mit Datum und Quelle/URL.
Wenn nichts Relevantes: "KEINE_HIGHLIGHTS"."""

    response = client.responses.create(
        model=RESEARCH_MODEL,
        instructions=SYSTEM_PROMPT,
        tools=[{"type": "web_search_preview"}],
        input=user_prompt,
    )
    full_text = response.output_text or ""
    print(full_text)

    return full_text


def _extract_candidate_items(all_research: str) -> list[dict]:
    """Zerlegt Rohdaten in bewertbare Einheiten, ohne lange Quellenblöcke zu verlieren."""
    items: list[dict] = []
    current_section = "Rohdaten"
    current_kasse = ""

    for raw_line in all_research.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):
            current_section = line.lstrip("# ").strip()
            continue
        if line.startswith("**") and line.endswith(":"):
            current_kasse = line.strip("*: ")
            continue
        if (line.startswith("Bild:") or line.startswith("Kontext:")) and items:
            items[-1]["text"] = f"{items[-1]['text']}\n{line}"
            continue
        if line.startswith(("- ", "* ", "  - ")) or re.match(r"^\d+\.\s+", line):
            text = re.sub(r"^\d+\.\s+", "", line).lstrip("-* ").strip()
            if len(text) >= 25:
                items.append({
                    "id": f"item_{len(items) + 1}",
                    "section": current_section,
                    "kasse": current_kasse,
                    "text": text[:1200],
                })

    if items:
        return items[:MAX_SCORING_ITEMS]

    blocks = [b.strip() for b in re.split(r"\n(?=###? |\*\*.+\*\*:)", all_research) if b.strip()]
    for block in blocks[:MAX_SCORING_ITEMS]:
        if len(block) >= 40 and "KEINE_HIGHLIGHTS" not in block:
            items.append({
                "id": f"item_{len(items) + 1}",
                "section": current_section,
                "kasse": "",
                "text": block[:1200],
            })
    return items


def _prefilter_reason(text_blob: str, is_linkedin: bool) -> str:
    is_news_rss = "news/rss" in text_blob or " rss" in text_blob
    if _is_strategic_event_signal(text_blob):
        return ""
    if _is_low_value_account_signal(text_blob):
        return "Ausschluss: regionaler Vertriebs-/Praeventionshinweis ohne harten GKV-IT- oder Marktwert"
    if is_news_rss and not is_linkedin and contains_any(text_blob, GKV_CONTEXT_TERMS):
        return ""
    if is_linkedin and contains_any(text_blob, LINKEDIN_NOISE_TERMS | EXCLUDE_TOPIC_TERMS):
        return "Ausschluss: LinkedIn-Rauschen ohne belastbares Account-Signal"
    if contains_any(text_blob, EXCLUDE_TOPIC_TERMS) and not contains_any(text_blob, STRATEGIC_TOPIC_TERMS):
        return "Ausschluss: Praevention/Event/Kampagne ohne strategischen IT-Bezug"
    if "landesvertretung" in text_blob and not contains_any(text_blob, STRATEGIC_TOPIC_TERMS):
        return "Ausschluss: lokale Landesvertretung ohne bundesweite strategische Relevanz"
    if is_linkedin and "follower" in text_blob:
        return "Ausschluss: Follower-Zahl ist kein Relevanzkriterium"
    return ""


def _linkedin_role_ok(text_blob: str) -> bool:
    return contains_any(text_blob, LINKEDIN_ALLOWED_ROLES) or contains_any(text_blob, DECISION_MAKER_TERMS)


def _linkedin_quality_reject_reason(text_blob: str) -> str:
    """Harte Nachfilterung, damit fachfremde LinkedIn-Treffer nicht ins Briefing gelangen."""
    blob = text_blob.lower()
    if _is_strategic_event_signal(blob):
        return ""
    if _is_low_value_account_signal(blob):
        return "LinkedIn regionaler Vertriebs-/Praeventionshinweis ohne harten GKV-IT-Wert"
    if contains_any(blob, LINKEDIN_HARD_EXCLUDE_TERMS):
        return "LinkedIn fachfremd oder nur Consumer-/Event-/Follower-Signal"
    if contains_any(blob, LINKEDIN_NOISE_TERMS | EXCLUDE_TOPIC_TERMS):
        return "LinkedIn HR/Event/Marketing ohne fachlichen GKV-IT-Wert"

    trusted = contains_any(blob, LINKEDIN_TRUSTED_MARKET_TERMS)
    has_account_value = contains_any(blob, LINKEDIN_ACCOUNT_VALUE_TERMS)
    has_gkv_context = contains_any(blob, GKV_CONTEXT_TERMS)
    role_ok = _linkedin_role_ok(blob)

    if trusted and has_account_value and has_gkv_context:
        return ""
    if role_ok and has_account_value and has_gkv_context:
        return ""
    return "LinkedIn ohne qualifizierte Top-Voice-/Kassen-/GKV-IT-Relevanz"


def _hard_signal_reason(text_blob: str) -> str:
    """Deterministische Rettung fuer harte Nicht-Rausch-Signale, wenn der KI-Scorer zu streng ist."""
    blob = text_blob.lower()
    if _is_strategic_event_signal(blob):
        return "Strategisches GKV-IT-Event mit Relevanz fuer Plattform-, Produkt-, Daten-, Cloud-, KI-, Service-, Betriebs- oder Dienstleisterthemen."
    if _is_low_value_account_signal(blob):
        return ""
    if contains_any(blob, LINKEDIN_NOISE_TERMS | LINKEDIN_HARD_EXCLUDE_TERMS):
        return ""
    has_hard_term = contains_any(blob, HARD_ACCOUNT_SIGNAL_TERMS) or bool(re.search(r"\b(ki|ti|api)\b", blob))
    if not has_hard_term:
        return ""
    if not (
        contains_any(blob, GKV_CONTEXT_TERMS)
        or contains_any(blob, {"krankenkasse", "krankenkassen", "gkv", "versicherung", "versicherte"})
    ):
        return ""
    if contains_any(blob, {"fusion", "zusammenschluss"}):
        return "Fusions-/Konsolidierungssignal mit moeglichen Folgen fuer IT-Migration, Plattformen, Prozesse und Versichertenkommunikation."
    if contains_any(blob, {"epa", "e-pa", "egk", "vsdm", "ti ", "telematik", "gematik"}) or re.search(r"\bti\b", blob):
        return "Regulatorisches TI-/ePA-/gematik-Signal mit Umsetzungsdruck fuer Schnittstellen, Betrieb und Kommunikation."
    if contains_any(blob, {"bsi", "nis2", "kritis", "b3s", "c5", "datenschutz", "informationssicherheit", "security", "cyber"}):
        return "Security-/Compliance-Signal mit Relevanz fuer Betrieb, Dienstleistersteuerung und Verantwortlichkeiten."
    if contains_any(blob, {"ausschreibung", "vergabe", "zuschlag", "auftrag", "beschaffung"}):
        return "Beschaffungsnahes Signal mit moeglichem Bedarf an Prozess-, Integrations-, Betriebs- oder Beratungsleistung."
    if contains_any(blob, {"go-live", "golive", "rollout", "implementierung", "migration", "plattform", "cloud", "portal", "app", "servicecenter", "automatisierung"}) or re.search(r"\b(ki|api)\b", blob):
        return "Konkretes IT-/Digital-/Betriebssignal mit moeglichen Folgen fuer Umsetzung, Integration oder Serviceprozesse."
    return "Belastbares GKV-/Health-IT-Signal mit fachlichem Account-Bezug."


def _deterministic_keep_reason(text_blob: str, is_linkedin: bool) -> str:
    """Harte Markt-/IT-Signale duerfen nicht nur wegen eines schwachen Scorer-Urteils verschwinden."""
    blob = text_blob.lower()
    hard_reason = _hard_signal_reason(blob)
    if hard_reason:
        return hard_reason
    if not is_linkedin and contains_any(blob, GKV_CONTEXT_TERMS) and contains_any(blob, HARD_ACCOUNT_SIGNAL_TERMS):
        return "Oeffentliches GKV-/Health-IT-Signal mit moeglicher Relevanz fuer Regulierung, Prozesse, Daten, Service, Betrieb oder Dienstleister."
    return ""


def build_hard_signal_rescue_research(all_research: str, limit: int = 12) -> str:
    """Rettet harte Signale aus Rohquellen, ohne LinkedIn-/RSS-Rauschen wieder einzuschleusen."""
    rescued: list[str] = []
    seen: set[str] = set()
    for item in _extract_candidate_items(all_research):
        raw_text = item.get("text", "")
        kind = source_kind(item)
        cleaned = strip_source_noise(raw_text)
        if len(cleaned) < 35:
            continue
        if kind == "LinkedIn" and _linkedin_quality_reject_reason(cleaned):
            continue
        reason = _hard_signal_reason(f"{item.get('section', '')} {item.get('kasse', '')} {cleaned}")
        if not reason:
            continue
        label, _url = extract_markdown_link(raw_text)
        key = normalize_item_key(cleaned)
        if key in seen:
            continue
        seen.add(key)
        org = clean_visible_source_text(item.get("kasse", "") or "") or "Markt"
        org = re.sub(r"\s+\((LinkedIn|News/RSS|RSS)\)$", "", org, flags=re.I)
        rescued.append(
            f"**{org}**:\n"
            f"  - {kind} | {clean_visible_source_text(raw_text)}\n"
            f"    Kontext: {reason}"
        )
        if len(rescued) >= limit:
            break
    if not rescued:
        return ""
    return "## Kuratierte Quellen\n\n" + "\n\n".join(rescued)


def score_research_items(client: openai.OpenAI, all_research: str) -> str:
    """Filtert Rohmeldungen per strukturierter Relevanzbewertung vor dem Newsletter."""
    global FILTER_REPORT
    items = _extract_candidate_items(all_research)
    if not items:
        return ""

    scoring_prompt = f"""Bewerte Rohmeldungen für den KassenInfodienst.
Ziel: persoenliches woechentliches GKV-/Health-IT-Briefing fuer Christian Galler
in seiner Rolle als Account Manager im IT-Vertrieb fuer gesetzliche Krankenkassen.
Leitfrage: Was sollte Christian diese Woche ueber Markt, Kunden, Kassen-IT,
Politik, Dienstleister, Top-Stimmen, Fusionen, Kooperationen und gemeinsame
IT-Projekte wissen? Kein Pressespiegel, keine Linkliste, kein Rauschen.

Bewerte jede Meldung intern nach sechs Kriterien mit 1-5 Punkten:
1. Strategische Relevanz fuer GKV, IT, Digitalisierung, Regulatorik, Fusionen, Kooperationen, gemeinsame IT-Projekte oder Marktbewegung
2. Entscheidungsebene der Quelle
3. Belastbarkeit der Quelle
4. Handlungswert fuer Account Management / Business Development
5. Neuigkeitswert im Recherchezeitraum
6. Bezug zu Zielkonten, Institutionen oder GKV-nahen Dienstleistern

Gesamtscore:
Score 5 = starkes strategisches Signal oder unmittelbarer Account-/Opportunity-Anlass.
Score 4 = klar relevant, belegt, aktuell, mit IT-/Digital-/Regulatorik-/Beschaffungs-, Fusions-, Kooperations- oder Projektbezug.
Score 3 = plausibles Marktsignal; behalten, wenn die Quelle fachlich relevant ist, aus einer beobachteten Kasse/Institution/Dienstleisterlandschaft kommt oder eine klare Entscheider-/Top-Voice-Quelle ist.
Score 1-2 = Rauschen.

Themenbreite:
- Breite GKV-IT-Themen behalten: Fusionen, gemeinsame IT-Projekte, Kooperationen, Plattformverbünde, Dienstleisterwechsel, Shared Services, Daten-/KI-/Automatisierungsvorhaben, App-/Portal-/Serviceprozesse, Cloud/Betrieb/Security und Versorgungsprogramme mit Prozess- oder Datenfolge.

RSS-/Kassenfeed-Regel:
- Kassen-eigene RSS-/News-Signale nur behalten, wenn sie einen konkreten strategischen oder operativen Account-Wert haben: IT/Digitalisierung, ePA/TI, Daten/KI, Service-/Prozessmodernisierung, Beschaffung, Fusion/Konsolidierung, Dienstleister, Regulatorik, Plattform/Betrieb oder belastbare Versorgungskooperation mit Prozess-/Datenfolge.
- Reine Präventions-, Gesundheitsratgeber-, Regional-, Sponsoring-, Messe- oder Firmenkunden-PR ist raus. Nicht aus einem weichen Thema nachträglich eine IT-Chance konstruieren.

Politik-/Regulatorik-Regel:
- Harte Fakten aus BMG, Bundestag/Bundesrat, GKV-Spitzenverband, gematik, BSI, Datenschutzaufsicht, Verbänden und Fachmedien hoch priorisieren, wenn sie Fristen, Pflichten, Finanzierungsfragen, TI/ePA/eGK, Versorgung, Datenschutz, Sicherheit oder Kassenorganisation betreffen.

LinkedIn-Regel:
- Qualifizierte Top-Voice-Quelle, kein beliebiger Pressespiegel.
- CEO/Geschaeftsfuehrung allein reicht nicht. Die Person oder Organisation muss aus Kasse, Verband, Politik, Institution, GKV-IT-Dienstleister oder klarer Health-IT-Landschaft kommen.
- Bevorzugt behalten: Vorstand, Geschaeftsfuehrung, CEO, CIO, CDO, CTO, Bereichsleitung IT/Digitalisierung/Versorgung/Strategie, Pressestelle, offizielle Unternehmenskommunikation, relevante Verbandsspitzen und Institutionen.
- Beispiele fuer besonders relevante Stimmen: Chef der DAK-Pressestelle, IKK-classic-CDO Stefan Schellberg, BITMARCK-CEO Andreas Strausfeld, ITSC-CEO Dieter Loewe.
- LinkedIn behalten, wenn konkrete Aussage zu IT, Digitalisierung, Service-/Prozessmodernisierung, TI/ePA/eGK/gematik, Datenschutz, Informationssicherheit, Gesetzgebung, Beschaffung, Plattform/App/Portal, Versorgung, Kassenpolitik, Dienstleistersteuerung oder strategischer Marktbewegung vorliegt.
- Hart ignorieren: Zahnzusatzversicherung, Implantat-/Keramikfüllungswerbung, Finanzberater-/Maklerposts, Consumer-Insurance-Vertrieb, Follower-Zahlen, Hochschul-/Präventionsprojekte ohne IT-/Kassenstrategie, Event-/Exkursionsposts, Wingcopter-/Benefits-/HR-Posts.
- Ignorieren: Karrieremeldungen ohne Marktbezug, Event-Selfies ohne fachliche Aussage, Kultur-/Employer-Branding, generisches Sales-/Partner-Marketing, Recruiter, Sachbearbeiter, Teamleiter ohne strategische Aussage, Glueckwuensche, Likes, Reposts ohne eigene Einordnung.

Dienstleister-/Institutionen-Regel:
- Behalte Hinweise auf GKV-Projekte, gemeinsame IT-Vorhaben, Kooperationen, Fusionen, Go-lives, Rollouts, Implementierungen, Zuschlaege, neue Kassenkunden, Betriebs-/Service-Erfolge, regulatorische Fristen und offiziellen Umsetzungsdruck.
- BMG, GKV-Spitzenverband, gematik, BSI, AOK-Bundesverband, vdek, IKK e.V., BKK Dachverband, BITMARCK und ITSC sind wichtig, wenn daraus Handlungsdruck fuer Kassen, Dienstleister oder IT-Landschaften entsteht.

Streng ausschließen:
- allgemeine Beitragssatzmeldungen ohne IT-/Strategiewinkel
- ePA-/TI-Pflichtthemen ohne konkreten Umsetzungs-, Anbieter-, Prozess- oder Kassenwinkel
- generische Gesundheitsratgeber, Awards, Kampagnen, Präventions-/Bewegungsprojekte oder Selbstlob ohne erkennbare Kassenpositionierung, IT-/Servicefolge oder Marktbezug
- fachfremde LinkedIn-Posts, die nur das Wort GKV nutzen, z.B. Zahnzusatzversicherung, Finanzvertrieb, Implantate, private Zusatzversicherung oder allgemeine Lebenshaltungskostenoptimierung
- Pressemitteilungen ohne konkretes Projekt, Namen, Frist, Entscheidung, neues Ereignis oder verwertbare Kassenagenda
- Ausschreibungen unter 1 Mio EUR oder ohne IT-/Strategie-/BPO-Bezug
- alte oder undatierte Meldungen, wenn keine aktuelle Entwicklung erkennbar ist

Antworte als JSON-Objekt:
{{
  "items": [
    {{
      "id": "item_1",
      "score": 1,
      "category": "Management|Top-Thema|Kassenradar|Institutionen/Politik|IT/Digital/Beschaffung|LinkedIn|Marktsignal|Quelle",
      "keep": false,
      "criteria": {{
        "strategic_relevance": 1,
        "decision_level": 1,
        "source_reliability": 1,
        "account_value": 1,
        "novelty": 1,
        "target_account_fit": 1
      }},
      "source_type": "Primärquelle|Pressemitteilung|Verbands-/Institutionsseite|LinkedIn-Signal|Medienbericht|Sonstiger Hinweis",
      "sales_relevance": "kurz und konkret",
      "exclude_reason": "kurz, leer wenn keep=true"
    }}
  ]
}}

Rohmeldungen:
{json.dumps(items, ensure_ascii=False)}"""

    try:
        completion = client.chat.completions.create(
            model=SCORING_MODEL,
            max_completion_tokens=6000,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Du bist ein strenger GKV-Relevanzfilter. Du bewertest nüchtern, nicht journalistisch."},
                {"role": "user", "content": scoring_prompt},
            ],
        )
        raw = completion.choices[0].message.content or "{}"
        data = json.loads(raw)
    except Exception as e:
        print(f"   ⚠️  Scoring fehlgeschlagen, nutze harte Signalrettung: {e}", file=sys.stderr)
        return build_hard_signal_rescue_research(all_research)

    decisions = {
        str(item.get("id")): item
        for item in data.get("items", [])
        if isinstance(item, dict)
    }

    kept: list[str] = []
    fallback: list[str] = []
    dropped = 0
    seen_items: set[str] = set()
    curation_audit: list[dict] = []
    filter_stats = {"geprueft": len(items), "dedupliziert": 0, "verworfen": 0, "linkedin_verworfen": 0, "behalten": 0}

    def audit_item(item: dict, status: str, reason: str, score: int | str = "") -> None:
        label, url = extract_markdown_link(item.get("text", ""))
        curation_audit.append({
            "status": status,
            "kind": source_kind(item, label, url),
            "organization": clean_visible_source_text(item.get("kasse", "") or ""),
            "score": score,
            "reason": reason,
            "text": clean_visible_source_text(item.get("text", ""))[:1200],
            "url": url,
        })

    for item in items:
        dedupe_key = normalize_item_key(item.get("text", ""))
        if dedupe_key in seen_items:
            dropped += 1
            filter_stats["dedupliziert"] += 1
            audit_item(item, "verworfen", "Duplikat")
            continue
        seen_items.add(dedupe_key)

        decision = decisions.get(item["id"], {})
        category = str(decision.get("category") or item["section"])
        text_blob = f"{item.get('section', '')} {category} {item.get('text', '')}".lower()
        is_linkedin = "linkedin" in text_blob
        relevance_blob = item.get("text", "").lower() if is_linkedin else text_blob
        deterministic_reason = _deterministic_keep_reason(text_blob, is_linkedin)

        pre_reason = _prefilter_reason(relevance_blob, is_linkedin)
        if pre_reason:
            dropped += 1
            filter_stats["verworfen"] += 1
            if is_linkedin:
                filter_stats["linkedin_verworfen"] += 1
            audit_item(item, "verworfen", pre_reason)
            continue

        if is_linkedin:
            linkedin_reject_reason = _linkedin_quality_reject_reason(item.get("text", ""))
            if linkedin_reject_reason:
                dropped += 1
                filter_stats["verworfen"] += 1
                filter_stats["linkedin_verworfen"] += 1
                audit_item(item, "verworfen", linkedin_reject_reason)
                continue

        score_5 = int(decision.get("score") or 0)
        final_score = max(0, min(100, score_5 * 20))
        keep = bool(decision.get("keep")) and final_score >= MIN_SCORE_KEEP
        if deterministic_reason and not keep:
            keep = True
            final_score = max(final_score, MIN_SCORE_KEEP)
            category = category or "Marktsignal"
        if final_score >= MIN_SCORE_TOP:
            category = "Top-Thema"
        elif final_score >= MIN_SCORE_KEEP:
            category = category or "Kassenradar"
        elif final_score >= MIN_SCORE_INTERNAL:
            dropped += 1
            filter_stats["verworfen"] += 1
            audit_item(item, "verworfen", str(decision.get("exclude_reason") or "Score unter Auslieferungsschwelle"), final_score)
            continue
        else:
            keep = False

        if not keep:
            dropped += 1
            filter_stats["verworfen"] += 1
            audit_item(item, "verworfen", str(decision.get("exclude_reason") or "Scorer keep=false oder Score zu niedrig"), final_score)
            continue

        relevance = str(decision.get("sales_relevance") or deterministic_reason or "").strip()
        source_type = str(decision.get("source_type") or classify_source_label(item.get("text", ""))).strip()
        header_bits = [category, source_type, f"Score={final_score}"]
        if item.get("kasse"):
            header_bits.append(item["kasse"])
        source_id = f"Q{len(kept) + len(fallback) + 1:02d}"
        title = " | ".join(header_bits)
        visible_text = clean_visible_source_text(item["text"]).replace("Follower", "")
        block = (
            f"### {source_id} | {title}\n"
            + visible_text
            + (f"\nEinordnung: {relevance}" if relevance else "")
        )
        kept.append(block)
        filter_stats["behalten"] += 1
        audit_item(item, "ausgeliefert_an_redaktion", relevance or deterministic_reason or "behalten", final_score)

    FILTER_REPORT = filter_stats
    _write_curation_audit(curation_audit, date.today())
    print(f"   🧹 Relevanzfilter: {len(kept) + len(fallback)} behalten, {dropped} verworfen.")
    if not kept and not fallback:
        return ""
    if len(kept) + len(fallback) > MAX_NEWSLETTER_SOURCES:
        print(
            f"   ✂️  Quellenradar auf {MAX_NEWSLETTER_SOURCES} eindeutige Meldungen gekürzt "
            f"(vorher {len(kept) + len(fallback)})."
        )
    kept = kept[:MAX_NEWSLETTER_SOURCES]
    remaining = max(0, MAX_NEWSLETTER_SOURCES - len(kept))
    fallback = fallback[:remaining]
    parts: list[str] = []
    if kept:
        parts.append("## Kuratierte Quellen\n\n" + "\n\n".join(kept))
    if fallback:
        parts.append("## LinkedIn-Radar\n\n" + "\n\n".join(fallback[:LINKEDIN_RADAR_LIMIT]))
    return "\n\n".join(parts)


def build_observation_radar(all_research: str) -> str:
    """Fail-closed: schwache Rohquellen werden nicht als Beobachtungsradar ausgegeben."""
    return ""


def build_source_based_newsletter(all_research: str, today: date) -> str:
    """Kompakter Fallback: eine deduplizierte Fundstueckliste ohne Rubriken."""
    items = build_editorial_source_items(all_research)
    if not items:
        return build_empty_summary(0, today)

    relevant_items = [item for item in items if _flat_relevance(item)]
    if not relevant_items:
        return build_empty_summary(len(items), today)
    relevant_items = _dedupe_flat_items(relevant_items, limit=min(MAX_NEWSLETTER_SOURCES, 12))

    lines: list[str] = []
    for item in relevant_items:
        link = _flat_source_link(item)
        line = f"- {_flat_summary_text(item)}"
        if link:
            line = f"{line}\n  {link}"
        lines.append(line)

    return "\n\n".join(lines).strip() + "\n"


def generate_compact_editorial_newsletter(client: openai.OpenAI, all_research: str, today: date) -> str:
    """Schreibt die finale Ausgabe als redaktionelle Fundstueckliste ohne Rubriken."""
    items = build_editorial_source_items(all_research)
    relevant_items = [item for item in items if _flat_relevance(item)]
    relevant_items = _dedupe_flat_items(relevant_items, limit=min(MAX_NEWSLETTER_SOURCES, 24))
    if not relevant_items:
        return build_empty_summary(len(items), today)

    source_lines: list[str] = []
    for idx, item in enumerate(relevant_items, start=1):
        source_lines.append(
            "\n".join(
                [
                    f"ID: F{idx:02d}",
                    f"Titel: {_flat_title(item)}",
                    f"Quelle: {(item.get('url') or '').strip()}",
                    f"Art: {item.get('kind', '')}",
                    f"Organisation: {item.get('org', '')}",
                    f"Text: {_flat_clean_phrase(item.get('text', ''))[:1200]}",
                    f"Kontext: {_flat_clean_phrase(item.get('context', ''))[:900]}",
                    f"Warum relevant: {_flat_relevance(item)}",
                ]
            )
        )

    prompt = f"""Du schreibst den persoenlichen KassenInfodienst fuer einen Account Manager im GKV-IT-Vertrieb.

Schreibe aus den Fundstuecken unten eine knappe, redaktionelle Liste. Kein Rubriken-Newsletter, keine Management Summary, keine Quellenuebersicht, keine Meta-Saetze.

Pflichtformat pro Fundstueck:
- **Kurzer Titel:** 2 bis 5 Saetze. Fasse den Inhalt des Fundstuecks wirklich zusammen. Erklaere konkret, was die Aussage ist und warum sie fuer GKV-IT, Kassen, Dienstleister, Daten, KI, ePA/TI, Prozesse, Service, Regulierung, Betrieb oder Account Management relevant ist. Schreibe nicht generisch.
  [Quelle](URL)

Wenn das Fundstueck konkrete Aussagen enthaelt, nutze gern kurze Unterpunkte im selben Bullet:
• Aussage: ...
• Bedeutung: ...

Regeln:
- Keine Ueberschriften.
- Keine getrennten Labels "Signal", "Einordnung" oder "Quelle".
- Quelle nur als klickbarer Markdown-Link `[Quelle](URL)`, ohne Beschreibung.
- Keine URLs erfinden; wenn keine URL vorhanden ist, keine Quellenzeile.
- Keine Scorer-Floskeln wie "Relevant als Hinweis auf Digital-/IT-/Prozessbedarf".
- Keine Dopplungen. Wenn mehrere Fundstuecke dasselbe Thema betreffen, fasse sie in einem Punkt zusammen und nutze die beste Quelle.
- Mehr Substanz statt mehr Laenge. Bei {len(relevant_items)} Fundstuecken sollten etwa {min(18, max(8, len(relevant_items)))} gute Punkte entstehen, sofern die Quellen genug hergeben.
- Deutsch, direkt, fachlich, ohne Werbesprache.

Gute Zielqualitaet:
- **BKK Dachverband stuetzt die GeDIG-Richtung und rahmt die ePA als Versorgungszentrum:** Der Verband positioniert die ePA nicht nur als Ablage, sondern als Einstieg in Versorgung, Ersteinschaetzung, Terminbuchung und Ueberweisungen. Zusaetzlich befuerwortet er, dass Kassen mit Einwilligung der Versicherten bestimmte ePA-Daten zur fruehzeitigen Risikoerkennung nutzen koennen. Fuer Kassen-IT heisst das: App-Fuehrung, Consent, Datenzugriff, Auswertungslogik und Versorgungsprozesse werden zu einem zusammenhaengenden Umsetzungsfeld.
  [Quelle](https://example.invalid)

Fundstuecke:
{chr(10).join(source_lines)[:26000]}
"""

    try:
        completion = client.chat.completions.create(
            model=NEWSLETTER_MODEL,
            max_completion_tokens=5000,
            messages=[
                {"role": "system", "content": "Du bist ein sehr guter deutscher Fachredakteur fuer GKV, Health IT und Kassen-IT. Du verdichtest Quellen zu nuetzlichem Account-Text."},
                {"role": "user", "content": prompt},
            ],
        )
        result = completion.choices[0].message.content or ""
    except Exception as exc:
        print(f"   ⚠️  Redaktioneller Kurzschritt fehlgeschlagen, nutze Fallback: {exc}", file=sys.stderr)
        return build_source_based_newsletter(all_research, today)

    if len(result.strip()) < 400 or newsletter_needs_repair(result, len(relevant_items)):
        return build_source_based_newsletter(all_research, today)
    return result.strip() + "\n"



# ---------------------------------------------------------------------------
# Newsletter-Erstellung
# ---------------------------------------------------------------------------

def load_last_week() -> str:
    """Lädt den Newsletter der letzten Woche als Kontext, bereinigt von Markdown-Headern."""
    if not LAST_WEEK_FILE.exists():
        return ""
    raw = LAST_WEEK_FILE.read_text(encoding="utf-8")
    # Markdown-Header entfernen, damit alte Überschriften nicht in den neuen Newsletter wandern
    lines = [l for l in raw.splitlines() if not l.startswith("#")]
    return "\n".join(lines).strip()


def save_last_week(newsletter: str, today: date) -> None:
    """Speichert den heutigen Newsletter als Gedächtnis für nächste Woche."""
    # Header-Zeile ohne Markdown-Syntax damit load_last_week sie sauber verarbeitet
    memory = f"Bereits berichtet KW {today.isocalendar()[1]} ({today.strftime('%d.%m.%Y')}):\n\n{newsletter[:4000]}\n"
    LAST_WEEK_FILE.write_text(memory, encoding="utf-8")


def generate_executive_summary(client: openai.OpenAI, all_research: str, today: date) -> str:
    """Erstellt den kuratierten Newsletter, filtert Wiederholungen aus der letzten Woche heraus."""
    return generate_compact_editorial_newsletter(client, all_research, today)

    last_week = load_last_week()
    last_week_block = ""
    if last_week:
        last_week_block = f"""
BEREITS LETZTE WOCHE BERICHTET (NICHT WIEDERHOLEN):
{last_week[:3000]}

→ Meldungen die dort stehen: nur erwähnen wenn sich etwas WESENTLICH geändert hat
  (z.B. Personalwechsel war angekündigt → jetzt vollzogen; Ausschreibung war offen → jetzt vergeben).
  Sonst weglassen.
"""

    source_pack, editorial_items = build_editorial_source_pack(all_research)
    source_count = len(editorial_items)
    if not source_pack:
        source_pack = all_research[:50000]

    prompt = f"""Du bist Chefredakteur des GKV-Branchenbriefs "KassenInfodienst".
Erstelle einen woechentlichen Branchenueberblick "GKV & IT" aus den Rohdaten unten.
Ziel: ungefaehr {NEWSLETTER_TARGET_WORDS} Woerter, wenn die Rohdaten genug Stoff liefern; schwache Wochen nicht kuenstlich aufblasen.
Der Leser moechte LinkedIn nicht haendisch durchklicken. Verdichte qualifizierte
LinkedIn-Signale zu einem lesbaren Wochenbericht mit eingebetteten Quellenlinks.
LinkedIn ist aber nur Signalquelle: keine irrelevanten Einzelposts, keine Event-/HR-/Sales-Beitraege.

SAUBERES QUELLENPAKET DIESER WOCHE:
{source_pack[:55000]}
{last_week_block}
FORMAT:

Keine Rubriken. Keine Management Summary. Keine Quellenuebersicht. Kein optisches Aufplustern.

Gib nur eine flache, deduplizierte Liste relevanter Fundstuecke aus.
Pro Fundstueck genau dieses Muster:

- **Kurzer Titel:** Zwei bis vier Saetze als Zusammenfassung des Fundstuecks. Signal und Einordnung sind ein gemeinsamer Text: Was steht drin, warum ist es fuer GKV, Kassen-IT, Service, Regulierung, Daten, KI, Prozesse, Dienstleister oder Account Management relevant? Keine getrennten Labels.
  [Quelle](URL)

Wenn keine echte URL vorhanden ist, lasse die Quellenzeile weg. Keine Beschreibung der Quelle, nur den Markdown-Link.
Zielumfang: kompakt. Schwache Wochen nicht aufblaehen.

REGELN:
- KEINEN Titel ausgeben – Header kommt automatisch
- Keine Ueberschriften ausgeben.
- Keine Rubriken ausgeben.
- Keine Management Summary ausgeben.
- Keine separate Quellenuebersicht ausgeben.
- "KEINE_HIGHLIGHTS"-Einträge ignorieren
- Nur Meldungen aus den kuratierten Rohdaten verwenden, keine neuen Fakten ergänzen
- Keine sichtbaren internen Scores, keine Roh-IDs, keine Formulierungen wie "Quellenradar", "Rohsignal", "kuratierte Rohmeldung" oder "Nullmeldung".
- Wenn eine Rohmeldung bereits als "ohne konkreten Kontext", "kein konkreter GKV-/IT-Bezug",
  "nur Karrieremeldung" oder "nur Reiseankündigung" eingeordnet ist: weglassen.
- Keine Platzhalterlinks wie "(LinkedIn)", "(Quelle)", "(LinkedIn Quelle)" oder "(DAZ Quelle)".
- Keine rohen Volltext-URLs im sichtbaren Text. Quelle immer nur als [Quelle](URL), ohne Beschreibung.
- Interne Rohdaten-IDs wie Q01, Q02, item_17 nicht sichtbar ausgeben.
- Auch die sauberen Quellen-IDs S01, S02 usw. nicht sichtbar ausgeben.
- Keine Rohüberschriften wie "LinkedIn | BITMARCK ..." übernehmen.
- Keine Labels "Signal:", "Kurzfassung:", "Einordnung:" oder "Quelle:" ausgeben.
- Nur echte URLs aus den Rohdaten als Link nutzen. Wenn keine URL vorhanden ist:
  "LinkedIn via LinkdAPI, keine URL geliefert" schreiben.
- Keine Meldung als harte Tatsache aufnehmen, wenn Datum, Quelle oder konkreter Anlass unklar bleibt;
  weiche LinkedIn-Signale duerfen als "Signal" oder "Gespraechsanlass" eingeordnet werden
- Keine Vorstandsänderungen vor dem Recherchezeitraum
- Keine Wiederholungen aus letzter Woche (außer bei Entwicklung)
- Tonalität: persoenliche Wochenzeitung fuer GKV & IT – praezise, pointiert, mit Namen
- Kein LinkedIn von Sachbearbeitern, Recruitern, Praktikanten oder reinem HR-Marketing
- LinkedIn-Rohsignale nicht wegwerfen: kompakt clustern, wenn sie als Gespraechsanlass taugen
- Thought-Leadership-Signale nicht wegwerfen: Wenn eine anerkannte Branchenstimme
  Health IT, KI, Digital Health oder Versorgung einordnet, als Markttrend aufnehmen.
  Einzelne Personen duerfen aber maximal 1-2 Meldungen bekommen; nicht eine Person dominieren lassen.
- Dienstleister-Projektsignale sind wichtig, auch wenn sie nicht direkt von einer Kasse kommen
- Bei schwacher Quellenlage lieber sehr kompakt bleiben.

Schreibe auf Deutsch."""

    if NEWSLETTER_API == "responses":
        response = client.responses.create(
            model=NEWSLETTER_MODEL,
            instructions=SYSTEM_PROMPT,
            max_output_tokens=9000,
            input=prompt,
        )
        result = response.output_text or ""
    else:
        completion = client.chat.completions.create(
            model=NEWSLETTER_MODEL,
            max_completion_tokens=9000,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        result = completion.choices[0].message.content or ""
    if newsletter_needs_repair(result, source_count):
        print("   ↪️  Newsletter enthaelt noch Rubriken/Labels – nutze kompakte Fundstueckliste.")
        result = build_source_based_newsletter(all_research, today)
    print(result)
    return result




def build_filter_report_section() -> str:
    if not FILTER_REPORT:
        return ""
    gepr = FILTER_REPORT.get("geprueft", 0)
    verw = FILTER_REPORT.get("verworfen", 0) + FILTER_REPORT.get("dedupliziert", 0)
    beh = FILTER_REPORT.get("behalten", 0)
    signal = "hoch" if beh >= 10 else "normal" if beh >= 5 else "niedrig"
    return (
        "## Relevanz-Hinweis der Woche\n\n"
        f"Diese Woche wurden {gepr} Quellen geprüft. {verw} wurden verworfen oder dedupliziert. "
        f"{beh} Themen wurden aufgenommen. Signalstärke: {signal}.\n"
    )

def build_empty_summary(raw_highlights_count: int, today: date) -> str:
    """Erzeugt eine sichtbare, nicht-leere Mail, wenn der Relevanzfilter alles verwirft."""
    return f"""## Keine belastbaren Account-Signale

Diese Woche gibt es keine belastbaren Account-Signale aus den beobachteten Quellen.

**Quellenlage**
- Rohquellen mit Treffern vor dem KI-Filter: {raw_highlights_count}
- Nach Relevanzfilter: 0 kuratierte Meldungen
- Stichtag: {today.strftime('%d.%m.%Y')}

Das ist kein technischer Fehler. Der Lauf wurde bewusst nicht mit LinkedIn-/RSS-Rauschen aufgefuellt.
"""


# ---------------------------------------------------------------------------
# HTML-E-Mail
# ---------------------------------------------------------------------------

def build_html_email(report_content: str, today: date) -> str:
    """Konvertiert den Markdown-Bericht in eine schöne HTML-E-Mail."""
    import html as html_lib
    import markdown as md_module

    html_body = md_module.markdown(
        report_content,
        extensions=["tables", "fenced_code", "sane_lists"],
    )

    MONATE = ["Januar","Februar","März","April","Mai","Juni",
              "Juli","August","September","Oktober","November","Dezember"]
    date_str = f"{today.day}. {MONATE[today.month - 1]} {today.year}"

    kw = today.isocalendar()[1]
    escaped_date = html_lib.escape(date_str)

    # Zeitungsoptik: ruhiger Masthead, klare Ressortlinien und Artikel statt Debug-Karten.
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>KassenInfodienst KW {kw}</title>
  <style>
    body {{
      margin: 0;
      padding: 0;
      background: #F2F0EA;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
      color: #111111;
      font-size: 15px;
      line-height: 1.68;
    }}
    .outer {{
      width: 100%;
      background: #F2F0EA;
    }}
    .wrapper {{
      max-width: 760px;
      width: 100%;
      margin: 0 auto;
    }}

    .header {{
      background: #FFFFFF;
      padding: 28px 40px 24px;
      border-radius: 4px 4px 0 0;
      border-top: 5px solid #111111;
      border-bottom: 1px solid #D9D6CC;
      margin: 28px 0 0;
      text-align: center;
    }}
    .header-eyebrow {{
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 3px;
      text-transform: uppercase;
      color: #555555;
      margin: 0 0 2px;
    }}
    .header h1 {{
      font-family: Georgia, 'Times New Roman', serif;
      font-size: 48px;
      font-weight: 900;
      color: #111111;
      letter-spacing: -1.2px;
      line-height: 1.05;
      margin: 0 0 18px;
    }}
    .spectrum {{
      border-collapse: collapse;
      margin: 0 auto 8px;
    }}
    .spectrum td {{
      height: 4px;
      width: 56px;
      padding: 0;
      line-height: 0;
      font-size: 0;
    }}
    .header-sub {{
      font-size: 10px;
      color: #666666;
      letter-spacing: 0.3px;
      margin: 0 0 22px;
    }}
    .header-meta {{
      border-collapse: collapse;
      width: 100%;
    }}
    .header-meta td {{
      font-size: 13px;
      color: #555555;
      padding: 0;
    }}
    .header-meta .right {{
      text-align: right;
      font-size: 12px;
      color: #555555;
    }}

    .card {{
      background: #ffffff;
      border-radius: 0 0 4px 4px;
      overflow: hidden;
      margin: 0 0 18px;
      text-align: left;
      border-bottom: 1px solid #D9D6CC;
    }}

    h2 {{
      margin: 30px 28px 8px;
      padding: 9px 0 8px;
      background: transparent;
      color: #111111;
      border-top: 3px double #111111;
      border-bottom: 1px solid #D9D6CC;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 1.7px;
      text-transform: uppercase;
      font-family: 'Helvetica Neue', Arial, sans-serif;
      text-align: left;
    }}
    h3 {{
      margin: 0;
      padding: 22px 28px 0;
      font-family: Georgia, 'Times New Roman', serif;
      font-size: 21px;
      font-weight: 700;
      color: #111111;
      line-height: 1.24;
      text-align: left;
    }}

    p {{
      margin: 0;
      padding: 8px 28px 13px;
      color: #333333;
      font-size: 15px;
      line-height: 1.78;
      text-align: left;
    }}
    h2 + p {{
      padding-top: 20px;
    }}

    .card img {{
      display: block;
      width: calc(100% - 56px);
      max-width: calc(100% - 56px);
      max-height: 220px;
      object-fit: cover;
      border: 0;
      margin: 12px 28px 18px;
      background: #F5F5F5;
    }}

    .card p em {{
      display: block;
      color: #777777;
      font-size: 12px;
      line-height: 1.45;
      margin-top: -4px;
    }}

    ul {{
      list-style: none;
      padding: 0;
      margin: 0;
    }}
    li {{
      padding: 13px 28px 15px;
      border-bottom: 1px solid #EEEAE0;
      color: #333333;
      background: #FFFFFF;
      font-size: 15px;
      line-height: 1.72;
      text-align: left;
    }}
    li:last-child {{
      border-bottom: none;
    }}
    li img {{
      margin: 12px 0 8px;
      max-height: 190px;
    }}

    a {{
      color: #1A5276;
      text-decoration: none;
      font-weight: 700;
    }}
    .more {{
      padding-top: 2px;
      padding-bottom: 24px;
    }}
    .more a {{
      display: inline-block;
      padding: 8px 13px;
      background: #111111;
      color: #FFFFFF;
      border-radius: 3px;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.8px;
    }}
    strong {{ color: #111111; font-weight: 800; }}
    em     {{ color: #777777; font-style: normal; font-size: 13px; }}

    blockquote {{
      margin: 0;
      padding: 18px 28px;
      background: #FFF8E5;
      border-left: 0;
      color: #4A3411;
      font-size: 15px;
      line-height: 1.7;
    }}

    hr {{
      border: none;
      border-top: 1px solid #EFEFEF;
      margin: 0;
    }}

    table.content-table {{
      border-collapse: collapse;
      width: calc(100% - 56px);
      margin: 20px 28px;
      font-size: 13px;
    }}
    table.content-table th {{
      background: #0D0D0D;
      color: #FFFFFF;
      padding: 9px 13px;
      text-align: left;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 1px;
      text-transform: uppercase;
    }}
    table.content-table td {{
      padding: 9px 13px;
      border-bottom: 1px solid #EFEFEF;
      color: #444444;
    }}
    table.content-table tr:nth-child(even) td {{ background: #FAFAFA; }}

    .footer {{
      text-align: center;
      padding: 4px 0 40px;
      color: #AAAAAA;
      font-size: 12px;
      line-height: 2;
    }}
    .footer a {{ color: #888888; }}
  </style>
</head>
<body style="margin:0;padding:0;background:#F2F0EA;">
  <table class="outer" width="100%" cellpadding="0" cellspacing="0" border="0">
    <tr><td align="center" style="padding:28px 12px 40px;">
      <div class="wrapper">

        <div class="header">
          <p class="header-eyebrow">Wöchentlicher</p>
          <h1>Kassen&shy;Infodienst</h1>
          <table class="spectrum" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background:#C0392B;border-radius:4px 0 0 4px;">&nbsp;</td>
              <td style="background:#D35400;">&nbsp;</td>
              <td style="background:#B7950B;">&nbsp;</td>
              <td style="background:#1E8449;">&nbsp;</td>
              <td style="background:#1A5276;border-radius:0 4px 4px 0;">&nbsp;</td>
            </tr>
          </table>
          <p class="header-sub">GKV &middot; Health IT &middot; KI &middot; Kassen &middot; Dienstleister</p>
          <table class="header-meta" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td>{escaped_date}</td>
              <td class="right">KW&nbsp;{kw} &nbsp;&middot;&nbsp; Branchenbrief</td>
            </tr>
          </table>
        </div>

        <div class="card">
          {html_body}
        </div>

        <div class="footer">
          Zusammengestellt mit OpenAI &nbsp;&middot;&nbsp; Automatischer KassenInfodienst<br>
          <a href="https://github.com/cgallerhh/KassenInfodienst">github.com/cgallerhh/KassenInfodienst</a>
        </div>

      </div>
    </td></tr>
  </table>
</body>
</html>"""


# ---------------------------------------------------------------------------
# E-Mail-Versand
# ---------------------------------------------------------------------------

def send_email(report_path: Path, summary: str, today: date) -> None:
    """Sendet den Digest-Bericht als moderne HTML-E-Mail via Gmail SMTP."""
    gmail_user = (os.environ.get("GMAIL_USER") or "").strip().replace("\xa0", "")
    gmail_password = (os.environ.get("GMAIL_APP_PASSWORD") or "").strip().replace("\xa0", "")
    recipient = (os.environ.get("RECIPIENT_EMAIL") or gmail_user).strip().replace("\xa0", "")

    if not gmail_user or not gmail_password or len(gmail_password) < 8:
        print(
            "⚠️  E-Mail übersprungen: GMAIL_USER oder GMAIL_APP_PASSWORD fehlt in .env",
            file=sys.stderr,
        )
        return

    report_content = report_path.read_text(encoding="utf-8")
    subject = f"🏥 KassenInfodienst – {today.strftime('%d.%m.%Y')}"

    # Multipart: HTML + Plain-Text-Fallback
    msg = MIMEMultipart("alternative")
    msg["From"] = gmail_user
    msg["To"] = recipient
    msg["Subject"] = subject

    # Plain-Text-Fallback (kurze Zusammenfassung)
    plain = (
        f"KassenInfodienst – Wöchentlicher Überblick\n"
        f"{today.strftime('%d. %B %Y')}\n"
        f"{'=' * 50}\n\n"
        f"{summary or 'Bericht siehe HTML-Ansicht.'}\n"
    )
    msg.attach(MIMEText(plain, "plain", "utf-8"))

    # HTML-E-Mail (nur summary, ohne Markdown-Header – vermeidet doppelten Titel)
    html = build_html_email(summary or report_content, today)
    msg.attach(MIMEText(html, "html", "utf-8"))

    print(f"📧 Sende HTML-E-Mail an {recipient} (von: {gmail_user}) ...")
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.set_debuglevel(0)
            server.starttls()
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        print("   ✅ E-Mail gesendet.")
    except smtplib.SMTPAuthenticationError as e:
        print(f"   ❌ SMTP Login fehlgeschlagen (App-Passwort prüfen): {e}", file=sys.stderr)
        raise
    except smtplib.SMTPException as e:
        print(f"   ❌ SMTP-Fehler beim Senden: {e}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"   ❌ Unerwarteter Fehler beim E-Mail-Versand: {e}", file=sys.stderr)
        raise


# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="KassenInfodienst – Wöchentlicher Krankenkassen-Digest"
    )
    parser.add_argument(
        "--kassen",
        nargs="*",
        metavar="KURZNAME",
        help=(
            "Nur bestimmte Kassen verarbeiten (Kurzname, z.B. TK BARMER DAK). "
            "Standard: alle Kassen."
        ),
    )
    parser.add_argument(
        "--output",
        metavar="DATEI",
        help="Ausgabedatei (Standard: reports/digest_YYYY-MM-DD.md)",
    )
    parser.add_argument(
        "--tage",
        type=int,
        default=7,
        help="Recherchezeitraum in Tagen (Standard: 7)",
    )
    parser.add_argument(
        "--kein-summary",
        action="store_true",
        help="Newsletter-Erstellung überspringen",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Beispielausgabe mit Dummy-Daten erzeugen, ohne API-Keys oder E-Mail-Versand.",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Bericht nach Fertigstellung per E-Mail senden (Gmail SMTP)",
    )
    return parser.parse_args()


def normalize_kassen_filter(raw_values: list[str] | None) -> list[str]:
    """Normalisiert --kassen aus Leerzeichen-, Komma- oder UI-Eingaben."""
    if not raw_values:
        return []

    normalized: list[str] = []
    for raw in raw_values:
        for part in re.split(r"[,;\n]+", raw):
            value = part.strip().strip("\"'")
            if value:
                normalized.append(value)
    return normalized


def filter_kassen(args: argparse.Namespace) -> list[dict]:
    """Filtert Kassen nach --kassen-Argument."""
    requested = normalize_kassen_filter(args.kassen)
    if not requested:
        return KASSEN

    filter_set = {k.upper() for k in requested}
    result = [
        k for k in KASSEN
        if k["short"].upper() in filter_set
        or k["name"].upper() in filter_set
        # Mehrteilige Kurznamen (z.B. "BKK firmus"): alle Wörter im Filter vorhanden?
        or all(w.upper() in filter_set for w in k["short"].split())
    ]

    if not result:
        print(
            f"Fehler: Keine Kasse mit Kurzname {requested!r} gefunden.\n"
            f"Verfügbare Kurznamen: {[k['short'] for k in KASSEN]}",
            file=sys.stderr,
        )
        sys.exit(1)

    matched: set[str] = set()
    for k in result:
        short = k["short"].upper()
        name = k["name"].upper()
        if short in filter_set:
            matched.add(short)
        if name in filter_set:
            matched.add(name)
        short_words = {w.upper() for w in k["short"].split()}
        if short_words and short_words.issubset(filter_set):
            matched.update(short_words)
    unknown = [
        value for value in requested
        if value.upper() not in matched
    ]
    observed = {
        item["short"].upper()
        for item in BEOBACHTETE_INSTITUTIONEN + BEOBACHTETE_ORGS + BEOBACHTETE_PERSONEN
    }
    ignored = [value for value in unknown if value.upper() not in observed]
    observed_only = [value for value in unknown if value.upper() in observed]
    if observed_only:
        print(
            "   ℹ️  Nicht als Kassenfilter genutzt, aber im Radar ohnehin enthalten: "
            + ", ".join(observed_only),
            file=sys.stderr,
        )
    if ignored:
        print(
            "   ⚠️  Unbekannte Kassenfilter ignoriert: " + ", ".join(ignored),
            file=sys.stderr,
        )

    return result


def normalize_openai_api_key(raw: str | None) -> str:
    """Bereinigt typische Secret-Copy-Paste-Artefakte, ohne den Key zu verändern."""
    key = (raw or "").strip().replace("\xa0", "")
    if not key:
        return ""

    if key.startswith(("'", '"')) and key.endswith(("'", '"')) and len(key) > 2:
        key = key[1:-1].strip()
    if key.lower().startswith("bearer "):
        key = key[7:].strip()
    if key.startswith("OPENAI_API_KEY="):
        key = key.split("=", 1)[1].strip()
    return key


def describe_api_key(key: str) -> str:
    """Gibt nur harmlose Metadaten aus, nie den API-Key selbst."""
    if key.startswith("sk-proj-"):
        kind = "Project-Key"
    elif key.startswith("sk-"):
        kind = "Secret-Key"
    else:
        kind = "ungewöhnliches Format"

    has_space = any(ch.isspace() for ch in key)
    flags = []
    if has_space:
        flags.append("enthält Leerzeichen/Zeilenumbruch")
    if len(key) < 20:
        flags.append("auffällig kurz")

    suffix = f"; Hinweis: {', '.join(flags)}" if flags else ""
    return f"{kind}, Länge {len(key)}{suffix}"


def list_available_models(client: openai.OpenAI) -> list[str]:
    """Liest die per API sichtbaren Modelle. Die Limits-Seite allein reicht nicht."""
    try:
        return sorted(m.id for m in client.models.list().data)
    except Exception as e:
        print(f"⚠️  Konnte Modellzugriff nicht vorab prüfen: {e}", file=sys.stderr)
        return []


def choose_available_worker_model(available: list[str], configured: str, label: str) -> str:
    """Wählt ein nutzbares Arbeitsmodell, ohne den Digest wegen Secret-Drift abzubrechen."""
    configured = (configured or "").strip()
    if not available:
        print(f"⚠️  {label}-Modellzugriff nicht vorab prüfbar – nutze {configured or 'gpt-5-nano'}.", file=sys.stderr)
        return configured or "gpt-5-nano"

    available_set = set(available)
    if configured and configured != "auto" and configured in available_set:
        print(f"🤖 {label}-Modell verfügbar: {configured}")
        return configured

    candidates = [
        "gpt-5-nano",
        "gpt-5-mini",
        "gpt-5",
        "gpt-5.2",
        "gpt-5.1",
        "gpt-4.1",
    ]
    chosen = next((model for model in candidates if model in available_set), "")
    if not chosen:
        chosen = next((model for model in available if model.startswith("gpt-5")), configured or "gpt-5-nano")

    if configured and configured != "auto" and configured != chosen:
        gpt5_models = [model for model in available if model.startswith("gpt-5")]
        print(
            f"⚠️  {label}-Modell {configured} ist für diesen API-Key nicht per /v1/models sichtbar.\n"
            f"   Nutze stattdessen: {chosen}\n"
            f"   Sichtbare gpt-5-Modelle: {', '.join(gpt5_models) or 'keine'}",
            file=sys.stderr,
        )
    else:
        print(f"🤖 {label}-Modell: {chosen}")
    return chosen


def choose_newsletter_model(client: openai.OpenAI, available: list[str], configured: str) -> tuple[str, str]:
    """Wählt das beste tatsächlich nutzbare Newsletter-Modell samt API-Endpunkt."""
    if configured and configured != "auto":
        candidates = [configured, "gpt-4.1", "gpt-5-nano"]
    else:
        candidates = NEWSLETTER_MODEL_CANDIDATES
    candidates = list(dict.fromkeys(candidates))

    available_set = set(available)
    for model in candidates:
        if available and model not in available_set:
            print(f"   ℹ️  {model} nicht in /v1/models sichtbar – teste direkten Zugriff trotzdem kurz.")
        try:
            probe = client.chat.completions.create(
                model=model,
                max_completion_tokens=8,
                messages=[{"role": "user", "content": "Antworte nur mit OK."}],
            )
            content = (probe.choices[0].message.content or "").strip()
            if content:
                print(f"🤖 Newsletter-Modell: {model} via chat.completions")
                return model, "chat"
        except Exception as e:
            print(f"   ⚠️  Newsletter-Modell {model} via chat.completions nicht nutzbar: {e}", file=sys.stderr)

        try:
            probe = client.responses.create(
                model=model,
                instructions="Antworte nur mit OK.",
                max_output_tokens=8,
                input="OK?",
            )
            content = (probe.output_text or "").strip()
            if content:
                print(f"🤖 Newsletter-Modell: {model} via responses")
                return model, "responses"
        except Exception as e:
            print(f"   ⚠️  Newsletter-Modell {model} via responses nicht nutzbar: {e}", file=sys.stderr)

    print("⚠️  Kein höheres Newsletter-Modell nutzbar; falle auf gpt-5-nano zurück.", file=sys.stderr)
    return "gpt-5-nano", "chat"


def make_report_header(today: date, tage: int, kassen: list[dict]) -> str:
    period_start = (today - timedelta(days=tage)).strftime("%d.%m.%Y")
    period_end = today.strftime("%d.%m.%Y")
    monate = [
        "Januar", "Februar", "Maerz", "April", "Mai", "Juni",
        "Juli", "August", "September", "Oktober", "November", "Dezember",
    ]
    date_label = f"{today.day}. {monate[today.month - 1]} {today.year}"

    kw = today.isocalendar()[1]
    return f"""# KassenInfodienst | KW {kw}

*{date_label} · {len(kassen)} Kassen · Recherchezeitraum {period_start} – {period_end}*

---

"""


def build_demo_summary(today: date) -> str:
    """Erzeugt eine Beispielausgabe fuer Layout- und Redaktionsprüfung ohne externe Quellen."""
    return """- **ePA-Umsetzung als Integrations- und Service-Thema:** Ein gematik- oder BMG-Hinweis waere nur dann relevant, wenn daraus konkrete Folgefragen fuer Portal, App, Authentifizierung, Kontaktcenter, Wissensmanagement, Schnittstellen oder Dienstleistersteuerung entstehen. Fuer Account Management zaehlt nicht die Pflicht an sich, sondern wo die Umsetzung operativ klemmt.
  [Quelle](https://www.gematik.de)

- **Service-Modernisierung bei einer Kasse:** Eine Kassenmeldung zu digitalen Kontaktstrecken waere ein Fundstueck, wenn sie auf echte Prozessarbeit hinweist und nicht nur Imagekommunikation ist. Relevant waere dann die Frage, welche Servicefaelle noch manuelle Nacharbeit erzeugen.
  [Quelle](https://www.gkv-spitzenverband.de)
"""


def main() -> None:
    global RESEARCH_MODEL, SCORING_MODEL, NEWSLETTER_MODEL, NEWSLETTER_API

    args = parse_args()

    if args.demo:
        today = date.today()
        kassen = filter_kassen(args)
        REPORTS_DIR.mkdir(exist_ok=True)
        output_path = Path(args.output) if args.output else REPORTS_DIR / "demo_personal_briefing.md"
        output_path.write_text(make_report_header(today, args.tage, kassen) + build_demo_summary(today), encoding="utf-8")
        print(f"✅ Demo-Briefing gespeichert: {output_path}")
        return

    missing_packages = []
    if openai is None:
        missing_packages.append("openai")
    if httpx is None:
        missing_packages.append("httpx")
    if req is None:
        missing_packages.append("requests")
    if missing_packages:
        print(
            "Fehler: Python-Paket(e) fehlen: " + ", ".join(missing_packages) + ". Tipp: pip install -r requirements.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    api_key = normalize_openai_api_key(os.environ.get("OPENAI_API_KEY"))
    if not api_key:
        print(
            "Fehler: Umgebungsvariable OPENAI_API_KEY nicht gesetzt.\n"
            "Tipp: export OPENAI_API_KEY=sk-...",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"🔐 OpenAI API-Key erkannt: {describe_api_key(api_key)}")

    client = openai.OpenAI(api_key=api_key, timeout=API_TIMEOUT)
    available_models = list_available_models(client)
    RESEARCH_MODEL = choose_available_worker_model(available_models, RESEARCH_MODEL, "Recherche")
    SCORING_MODEL = choose_available_worker_model(available_models, SCORING_MODEL, "Scoring")
    NEWSLETTER_MODEL, NEWSLETTER_API = choose_newsletter_model(client, available_models, NEWSLETTER_MODEL)

    today = date.today()
    kassen = filter_kassen(args)

    # Ausgabedatei bestimmen
    REPORTS_DIR.mkdir(exist_ok=True)
    output_path = Path(args.output) if args.output else REPORTS_DIR / f"gkv-briefing-{today.strftime('%Y-%m-%d')}.md"

    print(f"🏥 KassenInfodienst – Starte Recherche")
    print(f"   Kassen:    {len(kassen)}")
    print(f"   Zeitraum:  letzte {args.tage} Tage")
    print(f"   Ausgabe:   {output_path}")
    print()

    institution_targets = BEOBACHTETE_INSTITUTIONEN
    personen_targets = BEOBACHTETE_PERSONEN if ENABLE_PERSONEN_RADAR else []
    if not ENABLE_PERSONEN_RADAR and BEOBACHTETE_PERSONEN:
        print("👥 Personen-Radar übersprungen (ENABLE_PERSONEN_RADAR nicht gesetzt; vermeidet Personen-Übergewicht).")
        print()

    ted_section = ""
    if ENABLE_TED_TENDERS:
        print("📋 TED-Ausschreibungen abrufen ...")
        # TED: mindestens 30 Tage Fenster (GKV-Ausschreibungen kommen selten)
        ted_section = search_ted_tenders(kassen, max(args.tage, 30))
        if ted_section:
            count = ted_section.count("\n- ")
            print(f"   ✅ {count} Ausschreibung(en) gefunden.")
        else:
            print("   ℹ️  Keine TED-Ausschreibungen im Zeitraum gefunden.")
        print()
    else:
        print("📋 TED-Ausschreibungen übersprungen (ENABLE_TED_TENDERS nicht gesetzt).")
        print()

    # Schneller Scheduled-Standard: deterministische News-RSS-Suche statt OpenAI Web Search.
    print("📰 News-RSS abrufen ...")
    news_data = scrape_news_rss(kassen + institution_targets + BEOBACHTETE_ORGS + personen_targets, args.tage)
    if news_data:
        news_count = news_data.count("\n  - ")
        print(f"   ✅ {news_count} News-RSS-Treffer gesammelt.")
        all_research_parts: list[str] = [news_data]
    else:
        print("   ℹ️  Keine News-RSS-Treffer.")
        all_research_parts = []
    print()

    # Optional: OpenAI Web Search. Für Cron standardmäßig aus, weil es bei allen Kassen
    # zu langsam/fragil ist. Bei Bedarf ENABLE_OPENAI_WEB_RESEARCH=true setzen.
    research_targets = kassen + institution_targets + BEOBACHTETE_ORGS + personen_targets
    if ENABLE_OPENAI_WEB_RESEARCH:
        batches = [research_targets[i : i + BATCH_SIZE] for i in range(0, len(research_targets), BATCH_SIZE)]

        for idx, batch in enumerate(batches, 1):
            batch_names = " | ".join(k["short"] for k in batch)
            print(f"📡 OpenAI Web-Research Batch {idx}/{len(batches)}: {batch_names} ...")

            research = ""
            for attempt in range(1, MAX_RETRIES + 2):
                try:
                    research = research_batch(client, batch, args.tage)
                    break
                except ((httpx.TimeoutException, httpx.ReadTimeout) if httpx else TimeoutError) as e:
                    print(f"   ⏰ Timeout (Versuch {attempt}) – überspringe", file=sys.stderr)
                    research = ""
                    break
                except Exception as e:
                    err_str = str(e)
                    is_overload = "overloaded" in err_str.lower() or "529" in err_str
                    wait = 45 if is_overload else 15
                    print(f"   ⚠️  Fehler (Versuch {attempt}): {e}", file=sys.stderr)
                    if attempt <= MAX_RETRIES:
                        print(f"   ⏳ Warte {wait}s vor Retry ...", file=sys.stderr)
                        time.sleep(wait)
                    else:
                        research = ""

            # Nur echte Highlights sammeln (KEINE_HIGHLIGHTS ignorieren)
            if research.strip() and "KEINE_HIGHLIGHTS" not in research:
                all_research_parts.append(research)

            print(f"   ✅ Fertig.")

            # Pause zwischen Batches (Search-Rate-Limit vermeiden)
            if idx < len(batches):
                print(f"   ⏳ Pause {BATCH_PAUSE}s ...")
                time.sleep(BATCH_PAUSE)
    else:
        print("📡 OpenAI Web-Research übersprungen (ENABLE_OPENAI_WEB_RESEARCH nicht gesetzt).")

    # LinkedIn-Posts scrapen: LinkdAPI > Voyager-API > RSS-Fallback
    # Neben Kassen und Dienstleistern werden auch Branchenstimmen beobachtet.
    linkedin_targets = kassen + institution_targets + BEOBACHTETE_ORGS + personen_targets
    linkedin_data = ""
    if os.environ.get("LINKDAPI_KEY"):
        print("🔗 LinkedIn via LinkdAPI (inkl. BITMARCK + ITSC) ...")
        linkedin_data = scrape_linkedin_linkdapi(linkedin_targets, args.tage)
        if linkedin_data:
            post_count = linkedin_data.count("\n  - [")
            print(f"   ✅ {post_count} LinkedIn-Posts via LinkdAPI.")
        else:
            print("   ℹ️  Keine LinkedIn-Posts via LinkdAPI – versuche Fallback.")
    if not linkedin_data and os.environ.get("LINKEDIN_LI_AT") and ENABLE_LINKEDIN_VOYAGER:
        print("🔗 LinkedIn Voyager-API (li_at-Session, inkl. BITMARCK + ITSC) ...")
        linkedin_data = scrape_linkedin_voyager(linkedin_targets, args.tage)
        if linkedin_data:
            post_count = linkedin_data.count("\n  - [")
            print(f"   ✅ {post_count} LinkedIn-Posts via Voyager-API.")
        else:
            print("   ℹ️  Keine LinkedIn-Posts via Voyager – versuche RSS-Fallback.")
    elif not linkedin_data and os.environ.get("LINKEDIN_LI_AT"):
        print("🔗 LinkedIn Voyager-API übersprungen (ENABLE_LINKEDIN_VOYAGER nicht gesetzt; vermeidet 429/Redirect-Loops).")
    if not linkedin_data:
        print("🔗 LinkedIn RSS-Fallback ...")
        linkedin_data = scrape_linkedin_rss(linkedin_targets, args.tage)
        if linkedin_data:
            print("   ✅ LinkedIn-RSS-Findings gesammelt.")
        else:
            print("   ℹ️  Keine LinkedIn-RSS-Findings.")
    if linkedin_data:
        all_research_parts.insert(0, linkedin_data)
    print()

    # Newsletter zusammensetzen – TED-Daten voranstellen
    if ted_section:
        all_research_parts.insert(0, ted_section)
    raw_research = "\n\n".join(all_research_parts)
    all_research = raw_research
    raw_highlights_count = len(all_research_parts)

    if raw_research.strip():
        print("🧹 Bewerte Relevanz und filtere Rauschen ...")
        filtered_research = score_research_items(client, raw_research)
        if filtered_research.strip():
            all_research = filtered_research.strip()
        else:
            rescued_research = build_hard_signal_rescue_research(raw_research)
            if rescued_research.strip():
                print("   ✅ KI-Filter ergab 0, harte Signalrettung hat belastbare Treffer gefunden.")
                all_research = rescued_research.strip()
            else:
                print("   ℹ️  Filter ergab 0 belastbare Treffer – erstelle kurze Nullmeldung.")
                all_research = ""

    highlights_count = len(_extract_candidate_items(all_research)) if all_research.strip() else 0
    print(f"\n📊 {highlights_count} kuratierte Meldung(en) aus {raw_highlights_count} Rohquellen.")

    newsletter = ""
    if not args.kein_summary and highlights_count > 0:
        print("📰 Erstelle Newsletter ...")
        if LAST_WEEK_FILE.exists():
            print("   📖 Letzte Woche geladen – filtere Wiederholungen ...")

        try:
            newsletter = generate_executive_summary(client, all_research, today)
        except openai.OpenAIError as e:
            print(f"   ⚠️  Newsletter-Fehler: {e}", file=sys.stderr)
            newsletter = f"> ⚠️ Newsletter konnte nicht erstellt werden: {e}\n"

        if len(newsletter.strip()) < 500:
            print(
                f"   ⚠️  Newsletter-Modell lieferte nur {len(newsletter.strip())} Zeichen – nutze quellenbasierten Fallback.",
                file=sys.stderr,
            )
            newsletter = build_source_based_newsletter(all_research, today)
        elif newsletter_needs_repair(newsletter, highlights_count):
            print(
                "   ⚠️  Newsletter wirkt noch wie Rohdatenliste – nutze redaktionell bereinigten Fallback.",
                file=sys.stderr,
            )
            newsletter = build_source_based_newsletter(all_research, today)

        # Gedächtnis für nächste Woche speichern
        if newsletter and "konnte nicht erstellt" not in newsletter:
            save_last_week(newsletter, today)
            print("   💾 Newsletter als nächste-Woche-Kontext gespeichert.")

        print("   ✅ Fertig.")
    elif highlights_count == 0:
        newsletter = build_empty_summary(raw_highlights_count, today)

    # Finalen Newsletter schreiben (Header + kuratierter Inhalt)
    header = make_report_header(today, args.tage, kassen)
    output_path.write_text(header + newsletter, encoding="utf-8")

    print(f"\n✅ Newsletter gespeichert: {output_path}")

    # E-Mail versenden
    if args.email:
        print()
        send_email(output_path, newsletter, today)

    print()


if __name__ == "__main__":
    main()
