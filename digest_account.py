#!/usr/bin/env python3
"""Account-Intelligence wrapper for KassenInfodienst.

This module keeps the existing collection, filtering and mail delivery pipeline from
``digest.py``. It only replaces the editorial layer so weekly runs read like a
GKV account-intelligence briefing instead of a newsletter or source digest.
"""

from __future__ import annotations

from datetime import date
import re

import digest


_BASE_BUILD_SOURCE_BASED_NEWSLETTER = digest.build_source_based_newsletter


ACCOUNT_INTELLIGENCE_SYSTEM = """
Du erstellst einen woechentlichen persoenlichen GKV-/Health-IT-Branchenbrief
fuer Christian Galler in seiner Rolle als
Account Manager im IT-Vertrieb fuer gesetzliche Krankenkassen.

Leitfrage: Was sollte Christian diese Woche wissen, um Markt, Kunden, Personen,
Politik, Dienstleister und breite IT-Themen in der GKV-Welt einzuordnen?
Dazu zaehlen auch Fusionen, gemeinsame IT-Projekte, Kassenkooperationen,
Plattform-/Betriebsthemen, Daten/KI/Automatisierung, Serviceprozesse und
Versorgungsprogramme mit operativer Folge.

Der KassenInfodienst ist kein Rubriken-Newsletter mehr. Keine Management
Summary, keine Top-Themen, kein Kassenradar, keine Quellenuebersicht, kein
optisches Aufplustern. Ausgabe ist eine kurze, deduplizierte Liste relevanter
Fundstuecke.

Jedes relevante Fundstueck muss in Account-Logik uebersetzt werden:
Was steht drin und warum ist es fuer GKV-IT, Service, Prozesse, Daten, KI,
Regulierung, Dienstleister oder Account Management relevant?

Schreibe nicht: "Die gematik hat X veroeffentlicht."
Schreibe: "Das erhoeht den Umsetzungsdruck bei Kassen mit heterogener
IT-Landschaft. Fuer BITMARCK-/ITSC-nahe Kassen kann daraus Beratungsbedarf bei
Prozess-, Integrations- und Betriebsmodellen entstehen."

Keine Rohdatenoptik, keine Artikelkarten, keine generischen Quellenresuemes,
keine Debug-Begriffe, keine Erklaerbaer-Passagen. Quelle nur als Markdown-Link
`[Quelle](URL)`, ohne Beschreibung.

Der Leser ist Fachspezialist mit gutem Markteinblick. Streng gegen Rauschen,
aber nicht blind gegen Substanz: Fusionen, ePA/TI/gematik, BSI/KRITIS/NIS2,
Vergaben, IT-Projekte, Migrationen, Plattform- und Dienstleistersignale muessen
bei belastbarer Quelle redaktionell ausgeschoepft werden. Schwache Wochen nicht
kuenstlich aufblasen.
"""


def _clean_sentence(text: str, limit: int = 520) -> str:
    cleaned = digest.readable_source_text(text or "")
    cleaned = re.sub(r"\bEinordnung:\s*", "", cleaned)
    cleaned = re.sub(r"\s+\b(Quelle|LinkedIn|Zum Artikel)\b\s*$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -*")
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 3].rstrip() + "..."
    return cleaned


def _item_link(item: dict, label: str = "Quelle") -> str:
    url = (item.get("url") or "").strip()
    if not url:
        return "LinkedIn via LinkdAPI, keine URL geliefert" if item.get("kind") == "LinkedIn" else "Quelle nicht verlinkt"
    if item.get("kind") == "LinkedIn":
        label = "LinkedIn"
    return f"[{label}]({url})"


def _account_hint(item: dict) -> str:
    blob = f"{item.get('org', '')} {item.get('headline', '')} {item.get('text', '')}".lower()
    if any(term in blob for term in ("gematik", "epa", "e-pa", "ti ", "egk", "vsdm", "telematik")):
        return (
            "Relevanz entsteht weniger aus der Nachricht selbst als aus der Umsetzung: "
            "Kassen muessen Fristen, Schnittstellen, Versichertenkommunikation, Betrieb "
            "und Dienstleistersteuerung zusammenbekommen. Das ist besonders wichtig, wenn "
            "die IT-Landschaft ueber mehrere Plattformen, Dienstleister oder Fachbereiche verteilt ist."
        )
    if any(term in blob for term in ("bitmarck", "itsc", "aok systems", "gkv informatik")):
        return (
            "Das Signal liegt in der Dienstleisterlandschaft. Fuer angeschlossene oder fachlich nahe "
            "Kassen kann daraus Druck bei Betriebsmodell, Integration, Prozessdesign, Releasefaehigkeit "
            "oder Dienstleistersteuerung entstehen."
        )
    if any(term in blob for term in ("cyber", "security", "bsi", "nis2", "kritis", "datenschutz", "informationssicherheit")):
        return (
            "Das ist ein Governance- und Betriebsrisiko, nicht nur ein IT-Thema. Fuer Kassen zaehlt, "
            "ob Sicherheitsanforderungen bereits in Betrieb, Dienstleistervertraege, Cloud-Modelle "
            "und interne Verantwortlichkeiten uebersetzt sind."
        )
    if any(term in blob for term in ("ausschreibung", "vergabe", "zuschlag", "auftrag", "beschaffung")):
        return (
            "Hier liegt die fachliche Bedeutung in der Beschaffungsnaehe: Budget, Zuständigkeit, "
            "Leistungsbild und Zeitfenster koennen sichtbar werden. Wichtig ist, frueh zu klaeren, "
            "ob es nur um Technik oder um Prozess-, Betriebs- und Integrationsleistung geht."
        )
    if any(term in blob for term in ("fusion", "kooperation", "go-live", "rollout", "implementierung", "migration")):
        return (
            "Das ist ein belastbares Markt- oder Projektbewegungssignal. Account-seitig zaehlt, "
            "welche Abhaengigkeiten bei Plattform, Betrieb, Dienstleistersteuerung oder Prozessumsetzung entstehen."
        )
    if any(term in blob for term in ("portal", "app", "servicecenter", "kontaktcenter", "automatisierung", "ki ", "daten", "cloud")):
        return (
            "Das Signal verweist auf konkrete Modernisierung in Service, Daten, Automatisierung oder Betrieb. "
            "Relevant ist es nur, wenn daraus ein fachlicher Umsetzungs- oder Integrationsbedarf ableitbar ist."
        )
    return ""


def _account_meaning(item: dict) -> str:
    blob = f"{item.get('org', '')} {item.get('headline', '')} {item.get('text', '')}".lower()
    org = item.get("org") or "den Account"
    if any(term in blob for term in ("gematik", "epa", "e-pa", "ti ", "egk", "vsdm", "telematik")):
        return f"Bei {org} auf Umsetzungsstand, Schnittstellen, Betriebsmodell und Dienstleisterabhaengigkeiten schauen."
    if any(term in blob for term in ("bitmarck", "itsc", "aok systems", "gkv informatik")):
        return "Pruefen, ob das Signal Rueckschluesse auf Roadmap, Betriebsmodell oder Plattformabhaengigkeiten zulaesst."
    if any(term in blob for term in ("cyber", "security", "bsi", "nis2", "kritis", "datenschutz", "informationssicherheit")):
        return "Security-/Compliance-Anforderungen gegen Betrieb, Dienstleistervertraege und Verantwortlichkeiten spiegeln."
    if any(term in blob for term in ("ausschreibung", "vergabe", "zuschlag", "auftrag", "beschaffung")):
        return "Leistungsbild, Budgetnaehe und moeglichen Bedarf an Prozess-, Integrations- oder Betriebsleistung klaeren."
    return "Nur als Account-Signal nutzen, wenn sich daraus konkrete operative Folgen oder Projektbewegung ableiten lassen."


def _dedupe_account_items(items: list[dict], limit: int = 8) -> list[dict]:
    """Entfernt inhaltliche Dubletten fuer ein kompakteres Briefing."""
    deduped: list[dict] = []
    seen: set[str] = set()
    for item in items:
        key = digest.normalize_item_key(
            f"{item.get('org','')} {item.get('headline','')} {item.get('text','')[:280]}"
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def build_account_intelligence_fallback(all_research: str, today: date) -> str:
    """Use the compact deterministic item list."""
    return _BASE_BUILD_SOURCE_BASED_NEWSLETTER(all_research, today)


def generate_account_intelligence_summary(client: digest.openai.OpenAI, all_research: str, today: date) -> str:
    """Write a compact editorial item list instead of the old long-form newsletter."""
    return digest.generate_compact_editorial_newsletter(client, all_research, today)


# Patch the editorial layer before digest.main() runs.
digest.SYSTEM_PROMPT = digest.SYSTEM_PROMPT + "\n" + ACCOUNT_INTELLIGENCE_SYSTEM
digest.build_source_based_newsletter = build_account_intelligence_fallback
digest.generate_executive_summary = generate_account_intelligence_summary


if __name__ == "__main__":
    digest.main()
