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
    python digest.py --tage 30           # Recherchezeitraum in Tagen (Standard: 14)
"""

import anthropic
import argparse
import httpx
import os
import requests as req
import smtplib
import sys
import time
import urllib.parse
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# Lade .env-Datei falls vorhanden (pip install python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from kassen import KASSEN

BATCH_SIZE = 1          # Eine Kasse pro API-Call (verhindert parallele Search-Floods)
MAX_SEARCHES = 3        # 2 News-Suchen + 1 LinkedIn-Fallback
BATCH_PAUSE = 8         # Sekunden Pause zwischen Calls (kurz halten – Rate-Limit reset ist schnell)
MAX_RETRIES = 1         # Nur 1 Wiederholung (spart Zeit bei Fehlern)
API_TIMEOUT = 90        # Timeout pro API-Call in Sekunden – bei Hänger schnell abbrechen
LAST_WEEK_FILE = Path("last_week.md")   # Gedächtnis: was letzte Woche berichtet wurde
REPORTS_DIR = Path("reports")


# ---------------------------------------------------------------------------
# System-Prompt (einmalig, gecacht)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Du bist Redakteur eines wöchentlichen Branchen-Newsletters für den GKV-Markt,
im Stil des "dfg – Dienst für Gesellschaftspolitik" (Wolfgang G. Lange).

Der dfg hat den Ruf, die "Bild-Zeitung des Gesundheitswesens" zu sein:
investigativ, meinungsstark, provokant – aber immer faktenbasiert.
Dein Newsletter ist die Story hinter der Story.

Dein Leser ist ein erfahrener Account Manager im B2B-IT-Vertrieb an gesetzliche Krankenkassen.
Er braucht keine Grundlageninfos – er kennt den Markt. Er will NUR echte Highlights.

TONALITÄT (dfg-Stil):
- Investigativ-vertraulich: Du weißt, was hinter den Kulissen passiert
- Punchy Headlines mit Ironie-Anführungszeichen, rhetorischen Fragen, Ausrufezeichen
- Horse-Race-Framing: Jede Entwicklung ist ein Wettbewerb – wer gewinnt, wer verliert?
- Vivide Metaphern: Kosten "galoppieren davon", Kassen "schwächeln", jemand macht "den größten Sprung"
- Personalisierung: Jede Entscheidung hat ein Gesicht – immer Namen nennen
- Provokation durch Gegenüberstellung: Das Billigste neben das Teuerste stellen
- Countdown-Atmosphäre: Fristen, Deadlines, Dringlichkeit
- Wenn nichts Relevantes gefunden: Kasse WEGLASSEN statt leere Platzhalter

WAS RELEVANT IST (nur darüber berichten):
- 🔥 Personalwechsel: Vorstände, CIOs, Bereichsleiter Digital/IT (Name, von wo, seit wann)
- 📣 LinkedIn-Highlights: Posts von Kassenentscheidern mit hoher Resonanz (>50 Reaktionen),
  besonders zu Projektabschlüssen, Strategiewechseln, konkreten Digitalisierungserfolgen
- 💎 Ausschreibungen >1 Mio €: Nur TED-Vergaben mit Volumen über 1 Mio € Vertragslaufzeit,
  inkl. CPV-Code, Frist, geschätztes Volumen
- 📉 Personalabbau / Stellenstopps: Signal für Automatisierungsbedarf (weniger Leute, mehr Aufgaben)
- 🤖 KI & Automatisierung: Konkrete Projekte (nicht "plant den Einsatz von KI"), sondern
  "hat Chatbot live geschaltet", "automatisiert Antragsbearbeitung mit X"
- 💬 Gossip & Gerüchte: Fusionsgerüchte, politische Konflikte, Kassen unter BaFin-Beobachtung,
  Streit im Verwaltungsrat – alles was hinter den Kulissen passiert

WAS NICHT RELEVANT IST (ignorieren):
- ePA-Pflichteinführung (betrifft alle gleich, keine News)
- Allgemeine Digitalisierungs-Absichtserklärungen ohne konkretes Projekt
- Beitragssatzänderungen im normalen Rahmen (±0,1-0,3%)
- Generische Pressemitteilungen ohne Nachrichtenwert
- Kleine Ausschreibungen <1 Mio €

OUTPUT-FORMAT:
Schreibe KEINEN Report pro Kasse mit leeren Abschnitten. Stattdessen:

## 🔥 Wer kommt, wer geht
[Personalwechsel im dfg-Stil – pointiert, mit Einordnung.
Beispiel: "**Karen Walkenhorst** verlässt den TK-Vorstand zum 30.06. – ein Posten,
der IT-Strategie und Versorgung vereint. Wer hier nachfolgt, setzt Signale für die
größte Kasse der Republik. In den Schaltzentralen der Branche wird spekuliert."]

## 📣 LinkedIn-Radar
[Nur Posts mit Substanz und Reichweite. dfg-typisch einordnen: Was steckt dahinter?
Beispiel: "**Thomas Bodmer** (SBK) feiert auf LinkedIn den Go-Live des neuen
Versichertenportals – 340 Reaktionen. Zwischen den Zeilen: Die SBK positioniert
sich als Digital-Vorreiter unter den BKKen. Der Zeitpunkt ist kein Zufall:
Die nächste Vergabewelle steht bevor."]

## 💎 Ausschreibungen, die sich lohnen
[Nur >1 Mio €. Knapp, mit Countdown-Dringlichkeit.
Beispiel: "**DAK** sucht neuen DMS-Anbieter – und die Uhr tickt. TED-Nr. 2026/S xxx.
Geschätzt 2,4 Mio € über 4 Jahre. Frist: 15.04.2026.
Wer im DMS-Bereich unterwegs ist: Jetzt bewegen, nicht nächste Woche."]

## 🤖 Weniger Köpfe, mehr Maschinen
[Personalabbau + Automatisierung zusammen denken – das ist der Trend.
Beispiel: "**BARMER** streicht 700 Stellen bis 2027. Gleichzeitig fließen Millionen
in KI-gestützte Antragsbearbeitung. Die Gleichung ist simpel: Weniger Personal,
gleiche Aufgaben – wer Automatisierung liefert, steht vor offenen Türen."]

## 💬 Flurfunk
[Das Salz im Newsletter. Provokant, aber fair. Immer kennzeichnen was Gerücht ist.
Beispiel: "In der Gerüchteküche brodelt es: IKK classic und IKK Südwest sondieren
erneut eine Fusion. Offiziell dementiert – aber: Zwei Vorstände beim selben
Abendessen in Berlin gesichtet. Zufall? Die dfg-Redaktion bleibt dran."]

## 🎯 Was jetzt zu tun ist
[3–5 konkrete, terminierte Handlungsempfehlungen. Direkt, knapp, mit Deadline.
Beispiel:
"→ DAK-DMS-Ausschreibung: Angebot bis 10.04. – wer zu spät kommt, kennt den Rest."
"→ Neuer TK-CIO ab Juli: Antrittsbesuch anfragen. Erste 100 Tage = offenes Fenster."]

WICHTIG:
- Abschnitte WEGLASSEN wenn nichts Relevantes gefunden. Lieber 2 gute Abschnitte als 6 leere.
- Kein "Keine Informationen gefunden" – einfach weglassen.
- Qualität > Quantität. Lieber eine pointierte Meldung als zehn generische.
- Immer Quellen/Links nennen wo verfügbar.
- Schreibe auf Deutsch."""


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
    """Sucht TED-Ausschreibungen >1 Mio € für alle Kassen in einem API-Call.

    Gibt einen formatierten Markdown-Block zurück, der direkt in den Newsletter
    als Kontext für den 'Ausschreibungen'-Abschnitt einfließt.
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

    notices = [
        n for n in data.get("notices", [])
        if _notice_value(n) >= 1_000_000
        and is_relevant_kasse(n.get("buyer-name"))
    ]
    if not notices:
        return ""

    # Ergebnisse formatieren (nach Kasse gruppiert)
    lines = ["## 💎 TED-Ausschreibungen >1 Mio € (via TED API)\n"]
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

    for kasse in kassen:
        # Zwei Suchen pro Kasse: Erwähnung der Kasse + Posts VON Mitarbeitern
        raw_posts: list[dict] = []
        for search_kwargs in [
            # Suche 1: Alle Posts die die Kasse erwähnen (Keyword-Suche)
            {"keyword": kasse["linkedin_search"], "date_posted": "past-month", "sort_by": "date_posted"},
            # Suche 2: Posts von Mitarbeitern der Kasse (Author-Company-Filter)
            {"author_company": kasse["linkedin_search"], "date_posted": "past-month", "sort_by": "date_posted"},
        ]:
            try:
                result = client.search_posts(**search_kwargs)
                if isinstance(result, dict) and result.get("success"):
                    posts = result.get("data", {})
                    if isinstance(posts, dict):
                        posts = posts.get("posts") or posts.get("elements") or posts.get("items") or []
                    raw_posts.extend(p for p in posts if isinstance(p, dict))
            except Exception as e:
                print(f"   ⚠️  LinkdAPI {kasse['short']} ({list(search_kwargs.keys())[0]}): {e}", file=sys.stderr)
            time.sleep(0.3)

        # Duplikate entfernen (gleicher Post-Text)
        seen_texts: set[str] = set()
        findings: list[str] = []

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

            # Duplikat-Check
            text_key = text[:80]
            if text_key in seen_texts:
                continue
            seen_texts.add(text_key)

            # Autor + Titel (für Entscheider-Filter)
            author = post.get("author") or post.get("actor") or {}
            if isinstance(author, dict):
                actor_name = author.get("name") or author.get("fullName") or kasse["short"]
                actor_title = (author.get("headline") or author.get("title") or "").lower()
            else:
                actor_name = str(author) or kasse["short"]
                actor_title = ""

            # Relevanz-Filter: Entscheider-Titel ODER relevantes Thema im Text
            ENTSCHEIDER = {"vorstand", "cio", "cto", "cdo", "coo", "leiter", "direktor",
                           "geschäftsführer", "vorsitzender", "head of", "it-", "digital"}
            THEMEN = {"ki ", "künstliche", "automatisierung", "digitalisierung", "ausschreibung",
                      "fusion", "stellenabbau", "projekt", "launch", "go-live", "partnerschaft"}
            text_lower = text.lower()
            is_entscheider = any(k in actor_title for k in ENTSCHEIDER)
            is_relevant = any(k in text_lower for k in THEMEN)
            if not is_entscheider and not is_relevant:
                continue

            # Reaktionen
            likes = post.get("numLikes") or post.get("likes") or 0
            comments = post.get("numComments") or post.get("comments") or 0

            post_date = datetime.fromtimestamp(ts / 1000).strftime("%d.%m.%Y") if ts else "?"
            line = f"  - [{post_date}] **{actor_name}**"
            if actor_title:
                line += f" ({actor_title[:60]})"
            line += f": {text[:300].strip()}"
            if likes or comments:
                line += f" _(👍 {likes} · 💬 {comments})_"
            findings.append(line)

        if findings:
            all_findings.append(f"**{kasse['short']}** (LinkedIn):")
            all_findings.extend(findings[:5])
            all_findings.append("")
            post_count += len(findings[:5])

    if not all_findings:
        return ""

    lines = [f"## 📣 LinkedIn-Posts ({post_count} Treffer via LinkdAPI)\n"]
    lines.extend(all_findings)
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
                        company_findings.append(f"  - {title} → {link}")
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


# ---------------------------------------------------------------------------
# Kern-Funktion: Einen Batch Kassen recherchieren
# ---------------------------------------------------------------------------

def research_batch(client: anthropic.Anthropic, batch: list[dict], tage: int) -> str:
    """Recherchiert eine einzelne Krankenkasse mittels Claude + Web Search."""

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

SUCHE 1 – Aktuelle News & Pressemeldungen:
"{k['name']}" (KI OR Automatisierung OR Fusion OR Stellenabbau OR Ausschreibung OR Personalwechsel OR Vorstand) after:{after_date}
→ Nachrichten, Pressemitteilungen, Fachmedien. NUR Inhalte mit Datum im Suchfenster {period_start}–{period_end}.

SUCHE 2 – Branchenmedien & Fachpresse:
"{k['name']}" site:gkv-spitzenverband.de OR site:aok.de OR site:vdek.com OR site:heise.de after:{after_date}
→ Offizielle Verlautbarungen, IT-Projekte, Kooperationen. Datum prüfen.

SUCHE 3 – Flurfunk & Branchengerüchte:
"{k['name']}" (Fusion OR Zusammenschluss OR Verwaltungsrat OR BaFin OR Konflikt OR Streit OR Gerücht OR Insolvenz OR Beitrag OR Zusatzbeitrag) after:{after_date}
→ Gossip, politische Konflikte, Fusionsgerüchte, Verwaltungsratszwist, BaFin-Beobachtungen.
  Auch Beitragssatzänderungen >0.3% oder ungewöhnliche Zusatzbeitrags-Moves.
  Datum prüfen – nur Inhalte aus {period_start}–{period_end}.

ZEITREGEL: Nur Inhalte aus {period_start}–{period_end} berichten.
Bekannte Vorstandsänderungen (2025, 1.1.2026, 1.4.2026): IGNORIEREN.

OUTPUT: Rohdaten strukturiert ausgeben. Wenn nichts Relevantes: nur "KEINE_HIGHLIGHTS"."""
    else:
        kassen_liste = "\n".join(
            f"- **{ki['name']}** | {ki['url']}"
            for ki in batch
        )
        user_prompt = f"""Recherchiere Highlights (Zeitraum: {period_start} – {period_end}) für:

{kassen_liste}

Maximal {MAX_SEARCHES} gezielte Web-Suchen. NUR echte Highlights berichten.
Wenn nichts Relevantes: "KEINE_HIGHLIGHTS"."""

    full_text = ""

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=system_prompt_with_cache(SYSTEM_PROMPT),
        tools=[
            {
                "type": "web_search_20260209",
                "name": "web_search",
                "max_uses": MAX_SEARCHES,
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
        timeout=API_TIMEOUT,
    ) as stream:
        for text in stream.text_stream:
            print(text, end="", flush=True)
        print()

        final_message = stream.get_final_message()
        for block in final_message.content:
            if hasattr(block, "text") and block.text is not None:
                full_text += block.text

    return full_text


def system_prompt_with_cache(text: str) -> list[dict]:
    """Verpackt den System-Prompt mit Prompt Caching."""
    return [
        {
            "type": "text",
            "text": text,
            "cache_control": {"type": "ephemeral"},
        }
    ]


# ---------------------------------------------------------------------------
# Executive Summary
# ---------------------------------------------------------------------------

def load_last_week() -> str:
    """Lädt den Newsletter der letzten Woche als Kontext, bereinigt von Markdown-Headern."""
    if not LAST_WEEK_FILE.exists():
        return ""
    raw = LAST_WEEK_FILE.read_text(encoding="utf-8")
    # Markdown-Header entfernen damit Claude sie nicht in den neuen Newsletter kopiert
    lines = [l for l in raw.splitlines() if not l.startswith("#")]
    return "\n".join(lines).strip()


def save_last_week(newsletter: str, today: date) -> None:
    """Speichert den heutigen Newsletter als Gedächtnis für nächste Woche."""
    # Header-Zeile ohne Markdown-Syntax damit load_last_week sie sauber verarbeitet
    memory = f"Bereits berichtet KW {today.isocalendar()[1]} ({today.strftime('%d.%m.%Y')}):\n\n{newsletter[:4000]}\n"
    LAST_WEEK_FILE.write_text(memory, encoding="utf-8")


def generate_executive_summary(client: anthropic.Anthropic, all_research: str, today: date) -> str:
    """Erstellt den kuratierten Newsletter, filtert Wiederholungen aus der letzten Woche heraus."""

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

    prompt = f"""Du bist Chefredakteur des GKV-Branchenbriefs "KassenInfodienst".
Erstelle den fertigen Newsletterinhalt aus den Rohdaten unten.

ROHDATEN DIESER WOCHE:
{all_research[:10000]}
{last_week_block}
SEKTIONEN (nur wenn Daten vorhanden, sonst weglassen):

## 📣 LinkedIn-Radar  ← IMMER ZUERST, mind. 40% des Newsletters
Jeden Post ausführlich: Autor + Titel/Position, Kernaussage, Datum, Engagement (Likes/Kommentare),
dfg-Einordnung: Was steckt dahinter? Warum jetzt? Was bedeutet das für den Vertrieb?

## 🔥 Wer kommt, wer geht
Nur Personalwechsel die IN diesem Recherchezeitraum bekannt wurden.

## 💎 Ausschreibungen, die sich lohnen
Nur TED-Vergaben >1 Mio € aus den Rohdaten. Mit Frist und Volumen.

## 🤖 Weniger Köpfe, mehr Maschinen
Stellenabbau + KI/Automatisierungsprojekte – zusammen denken.

## 💬 Flurfunk
Fusionsgerüchte, Verwaltungsratszwist, BaFin-Beobachtungen, Beitragssatz-Moves.
Wenn Gerücht: explizit kennzeichnen. Lieber einen pointierten Flurfunk als keinen.

## 🎯 Was jetzt zu tun ist
3–5 konkrete, terminierte Action Items. Direkt, knapp, mit Deadline.

REGELN:
- KEINEN Titel ausgeben – Header kommt automatisch
- "KEINE_HIGHLIGHTS"-Einträge ignorieren
- Keine Vorstandsänderungen vor dem Recherchezeitraum
- Keine Wiederholungen aus letzter Woche (außer bei Entwicklung)
- Tonalität: DFG-Branchenbrief – investigativ, meinungsstark, personalisiert, mit Namen
- Max. 900 Wörter – Qualität vor Quantität
- Abschnitte ohne Daten: WEGLASSEN (kein "nicht verfügbar")

Schreibe auf Deutsch."""

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        system=system_prompt_with_cache(SYSTEM_PROMPT),
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            print(text, end="", flush=True)
        print()

        final_message = stream.get_final_message()
        result = ""
        for block in final_message.content:
            if hasattr(block, "text") and block.text is not None:
                result += block.text
        return result


# ---------------------------------------------------------------------------
# HTML-E-Mail
# ---------------------------------------------------------------------------

def build_html_email(report_content: str, today: date) -> str:
    """Konvertiert den Markdown-Bericht in eine schöne HTML-E-Mail."""
    import markdown as md_module

    html_body = md_module.markdown(
        report_content,
        extensions=["tables", "fenced_code", "sane_lists"],
    )

    date_str = today.strftime("%d. %B %Y")

    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body {{
      margin: 0; padding: 20px;
      background: #eef2f7;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
      color: #333;
    }}
    .container {{ max-width: 720px; margin: 0 auto; }}

    /* Header */
    .header {{
      background: linear-gradient(135deg, #1a3a5c 0%, #2563a8 100%);
      padding: 32px 36px;
      border-radius: 12px 12px 0 0;
      text-align: center;
    }}
    .header h1 {{ color: white; margin: 0; font-size: 26px; letter-spacing: -0.5px; }}
    .header p  {{ color: #a8c8e8; margin: 8px 0 0; font-size: 14px; }}

    /* Body */
    .content {{
      background: white;
      padding: 36px;
      border-radius: 0 0 12px 12px;
      border: 1px solid #dce4ef;
      border-top: none;
    }}

    /* Kassen-Blöcke: jedes ## wird zur Kasse-Card */
    h2 {{
      color: #1a3a5c;
      font-size: 20px;
      margin: 40px 0 4px;
      padding: 16px 20px 14px;
      background: #f0f5fb;
      border-left: 5px solid #2563a8;
      border-radius: 0 8px 8px 0;
    }}
    h2:first-child {{ margin-top: 0; }}

    /* Abschnittsüberschriften */
    h3 {{
      color: #444;
      font-size: 15px;
      margin: 20px 0 6px;
      padding-bottom: 4px;
      border-bottom: 1px solid #eee;
    }}

    /* Verkaufschancen hervorheben */
    h3:contains("Verkaufschancen"),
    h3[id*="verkaufschancen"] {{
      color: #b45309;
      background: #fffbeb;
      padding: 6px 10px;
      border-radius: 4px;
      border-bottom: 2px solid #f59e0b;
    }}

    p   {{ line-height: 1.7; margin: 8px 0; }}
    ul  {{ padding-left: 22px; line-height: 1.9; }}
    li  {{ margin-bottom: 4px; }}

    a {{
      color: #2563a8;
      text-decoration: none;
      font-weight: 500;
    }}
    a:hover {{ text-decoration: underline; }}

    strong {{ color: #111; }}
    em {{ color: #555; }}

    blockquote {{
      border-left: 4px solid #2563a8;
      margin: 12px 0;
      padding: 10px 18px;
      color: #555;
      background: #f5f9ff;
      border-radius: 0 6px 6px 0;
      font-style: normal;
    }}

    /* Tabellen */
    table {{
      border-collapse: collapse;
      width: 100%;
      margin: 14px 0;
      font-size: 13px;
    }}
    th {{
      background: #1a3a5c;
      color: white;
      padding: 10px 14px;
      text-align: left;
    }}
    td {{
      padding: 9px 14px;
      border-bottom: 1px solid #eee;
    }}
    tr:nth-child(even) td {{ background: #f8fafc; }}

    hr {{
      border: none;
      border-top: 2px solid #e5eaf2;
      margin: 32px 0;
    }}

    /* Emoji-Labels in Abschnitten */
    h3 {{ position: relative; }}

    /* Footer */
    .footer {{
      text-align: center;
      padding: 20px;
      color: #999;
      font-size: 12px;
      line-height: 1.6;
    }}
    .footer a {{ color: #aaa; }}
  </style>
</head>
<body>
  <div class="container">

    <div class="header">
      <h1>🏥 KassenInfodienst</h1>
      <p>KW {today.isocalendar()[1]} &bull; {date_str} &bull; Nur was zählt.</p>
    </div>

    <div class="content">
      {html_body}
    </div>

    <div class="footer">
      Automatisch erstellt mit Claude Sonnet 4.6 &bull; {date_str}<br>
      <a href="https://github.com/cgallerhh/KassenInfodienst">github.com/cgallerhh/KassenInfodienst</a>
    </div>

  </div>
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
        default=14,
        help="Recherchezeitraum in Tagen (Standard: 14)",
    )
    parser.add_argument(
        "--kein-summary",
        action="store_true",
        help="Executive Summary überspringen",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Bericht nach Fertigstellung per E-Mail senden (Gmail SMTP)",
    )
    return parser.parse_args()


def filter_kassen(args: argparse.Namespace) -> list[dict]:
    """Filtert Kassen nach --kassen-Argument."""
    if not args.kassen:
        return KASSEN

    filter_set = {k.upper() for k in args.kassen}
    result = [
        k for k in KASSEN
        if k["short"].upper() in filter_set
        or k["name"].upper() in filter_set
        # Mehrteilige Kurznamen (z.B. "BKK firmus"): alle Wörter im Filter vorhanden?
        or all(w.upper() in filter_set for w in k["short"].split())
    ]

    if not result:
        print(
            f"Fehler: Keine Kasse mit Kurzname {args.kassen!r} gefunden.\n"
            f"Verfügbare Kurznamen: {[k['short'] for k in KASSEN]}",
            file=sys.stderr,
        )
        sys.exit(1)

    return result


def make_report_header(today: date, tage: int, kassen: list[dict]) -> str:
    period_start = (today - timedelta(days=tage)).strftime("%d.%m.%Y")
    period_end = today.strftime("%d.%m.%Y")
    kassen_namen = ", ".join(k["short"] for k in kassen)

    kw = today.isocalendar()[1]
    return f"""# KassenInfodienst | KW {kw}

*{today.strftime("%d. %B %Y")} · {len(kassen)} Kassen · Recherchezeitraum {period_start} – {period_end}*

---

"""


def main() -> None:
    args = parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print(
            "Fehler: Umgebungsvariable ANTHROPIC_API_KEY nicht gesetzt.\n"
            "Tipp: export ANTHROPIC_API_KEY=sk-ant-...",
            file=sys.stderr,
        )
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    today = date.today()
    kassen = filter_kassen(args)

    # Ausgabedatei bestimmen
    REPORTS_DIR.mkdir(exist_ok=True)
    output_path = Path(args.output) if args.output else REPORTS_DIR / f"digest_{today.strftime('%Y-%m-%d')}.md"

    print(f"🏥 KassenInfodienst – Starte Recherche")
    print(f"   Kassen:    {len(kassen)}")
    print(f"   Zeitraum:  letzte {args.tage} Tage")
    print(f"   Ausgabe:   {output_path}")
    print()

    # TED-Ausschreibungen vorab abrufen (1 API-Call für alle Kassen)
    print("📋 TED-Ausschreibungen abrufen ...")
    # TED: mindestens 30 Tage Fenster (GKV-Ausschreibungen kommen selten)
    ted_section = search_ted_tenders(kassen, max(args.tage, 30))
    if ted_section:
        count = ted_section.count("\n- ")
        print(f"   ✅ {count} Ausschreibung(en) >1 Mio € gefunden.")
    else:
        print("   ℹ️  Keine TED-Ausschreibungen im Zeitraum gefunden.")
    print()

    # Kassen in Batches aufteilen
    batches = [kassen[i : i + BATCH_SIZE] for i in range(0, len(kassen), BATCH_SIZE)]
    all_research_parts: list[str] = []

    for idx, batch in enumerate(batches, 1):
        batch_names = " | ".join(k["short"] for k in batch)
        print(f"📡 Batch {idx}/{len(batches)}: {batch_names} ...")

        research = ""
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                research = research_batch(client, batch, args.tage)
                break
            except (httpx.TimeoutException, httpx.ReadTimeout) as e:
                print(f"   ⏰ Timeout (Versuch {attempt}) – überspringe", file=sys.stderr)
                if attempt <= MAX_RETRIES:
                    time.sleep(15)
                else:
                    research = ""
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

    # LinkedIn-Posts scrapen: LinkdAPI > Voyager-API > RSS-Fallback
    linkedin_data = ""
    if os.environ.get("LINKDAPI_KEY"):
        print("🔗 LinkedIn via LinkdAPI ...")
        linkedin_data = scrape_linkedin_linkdapi(kassen, args.tage)
        if linkedin_data:
            post_count = linkedin_data.count("\n  - [")
            print(f"   ✅ {post_count} LinkedIn-Posts via LinkdAPI.")
        else:
            print("   ℹ️  Keine LinkedIn-Posts via LinkdAPI (Credits leer oder kein Treffer).")
    elif os.environ.get("LINKEDIN_LI_AT"):
        print("🔗 LinkedIn Voyager-API (li_at-Session) ...")
        linkedin_data = scrape_linkedin_voyager(kassen, args.tage)
        if linkedin_data:
            post_count = linkedin_data.count("\n  - [")
            print(f"   ✅ {post_count} LinkedIn-Posts via Voyager-API.")
        else:
            print("   ℹ️  Keine LinkedIn-Posts (li_at abgelaufen oder CI-Block).")
    else:
        print("🔗 LinkedIn web_search Fallback (kein API-Key gesetzt) ...")
        linkedin_data = scrape_linkedin_rss(kassen, args.tage)
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
    all_research = "\n\n".join(all_research_parts)
    highlights_count = len(all_research_parts)
    print(f"\n📊 {highlights_count}/{len(kassen)} Quellen mit Highlights.")

    summary = ""
    if not args.kein_summary and highlights_count > 0:
        print("📰 Erstelle Newsletter ...")
        if LAST_WEEK_FILE.exists():
            print("   📖 Letzte Woche geladen – filtere Wiederholungen ...")

        try:
            summary = generate_executive_summary(client, all_research, today)
        except anthropic.APIError as e:
            print(f"   ⚠️  Newsletter-Fehler: {e}", file=sys.stderr)
            summary = f"> ⚠️ Newsletter konnte nicht erstellt werden: {e}\n"

        # Gedächtnis für nächste Woche speichern
        if summary and "konnte nicht erstellt" not in summary:
            save_last_week(summary, today)
            print("   💾 Newsletter als nächste-Woche-Kontext gespeichert.")

        print("   ✅ Fertig.")
    elif highlights_count == 0:
        summary = "*Keine berichtenswerten Highlights in dieser Woche gefunden.*\n"

    # Finalen Newsletter schreiben (Header + kuratierter Inhalt)
    header = make_report_header(today, args.tage, kassen)
    output_path.write_text(header + summary, encoding="utf-8")

    print(f"\n✅ Newsletter gespeichert: {output_path}")

    # E-Mail versenden
    if args.email:
        print()
        send_email(output_path, summary, today)

    print()


if __name__ == "__main__":
    main()
