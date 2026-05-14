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


ACCOUNT_INTELLIGENCE_SYSTEM = """
Du erstellst kein Newsletter-Format, sondern ein woechentliches
Account-Intelligence-Briefing GKV fuer Christian Galler in seiner Rolle als
Account Manager im IT-Vertrieb fuer gesetzliche Krankenkassen.

Leitfrage: Was sollte Christian diese Woche wissen, um Markt, Kunden, Personen,
Politik, Dienstleister und breite IT-Themen in der GKV-Welt einzuordnen?
Dazu zaehlen auch Fusionen, gemeinsame IT-Projekte, Kassenkooperationen,
Plattform-/Betriebsthemen, Daten/KI/Automatisierung, Serviceprozesse und
Versorgungsprogramme mit operativer Folge.

Ein Newsletter berichtet. Dieses Briefing bewertet.

Jede relevante Quelle muss in Account-Logik uebersetzt werden:
- Was ist das Signal?
- Warum erzeugt es Druck, Risiko oder Bewegung im GKV-/Health-IT-Markt?
- Welche Kassen, Dienstleister, IT-Landschaften oder Rollen koennten betroffen sein?
- Welche fachliche Account-Bedeutung ergibt sich daraus?

Schreibe nicht: "Die gematik hat X veroeffentlicht."
Schreibe: "Das erhoeht den Umsetzungsdruck bei Kassen mit heterogener
IT-Landschaft. Fuer BITMARCK-/ITSC-nahe Kassen kann daraus Beratungsbedarf bei
Prozess-, Integrations- und Betriebsmodellen entstehen."

Keine Rohdatenoptik, keine Artikelkarten, keine generischen Quellenresuemes,
keine Debug-Begriffe, keine Erklaerbaer-Passagen. Quellen stehen nur dort, wo
sie die Bewertung stuetzen.

Der Leser ist Fachspezialist mit gutem Markteinblick. Keine separaten
Gespraechsanlaesse, keine ausformulierten Anschlussfragen, keine redaktionellen
Restlauf- oder Nullsignal-Kommentare. Account-Bedeutung wird knapp in die
Bewertung integriert und nicht als eigene Rubrik wiederholt.
"""


def _clean_sentence(text: str, limit: int = 520) -> str:
    cleaned = digest.readable_source_text(text or "")
    cleaned = re.sub(r"\bEinordnung:\s*", "", cleaned)
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
    if item.get("kind") == "LinkedIn":
        return (
            "Relevant ist das nur bei qualifizierten Stimmen aus Kasse, Verband, Politik oder GKV-IT. "
            "Der Wert liegt dann in sichtbarer Prioritaetensetzung: Strategie, IT-/Serviceagenda, "
            "Regulatorik, Dienstleistersteuerung, Plattform- oder Versorgungslogik."
        )
    return (
        "Das Signal sollte als Account-Hypothese genutzt werden: Welche operativen Folgen entstehen "
        "fuer Service, Portal, Automatisierung, Daten, Versorgung, Betrieb oder Dienstleistersteuerung?"
    )


def _source_overview(items: list[dict]) -> list[str]:
    grouped: dict[str, list[dict]] = {}
    for item in items:
        grouped.setdefault(item.get("kind") or "Quelle", []).append(item)

    lines = ["## Quellenbasis", ""]
    for kind in ("LinkedIn", "News/RSS", "Vergabe", "Marktquelle"):
        if kind not in grouped:
            continue
        lines.append(f"**{kind}**")
        for item in grouped[kind][:8]:
            org = item.get("org") or "Markt"
            headline = item.get("headline") or "Signal"
            lines.append(f"- **{org}:** {headline} - {_item_link(item)}")
        lines.append("")
    return lines


def build_account_intelligence_fallback(all_research: str, today: date) -> str:
    """Deterministic fallback that writes briefing blocks instead of article cards."""
    items = digest.build_editorial_source_items(all_research)
    if not items:
        return digest.build_empty_summary(0, today)

    top_items = items[: min(8, len(items))]
    account_orgs = sorted({item.get("org", "Markt") for item in top_items if item.get("org")})

    lines: list[str] = [
        "## Management Summary",
        "",
    ]
    for item in top_items[:5]:
        signal = _clean_sentence(item.get("text", ""), 260)
        lines.append(
            f"- **{item.get('org') or 'Markt'}:** {signal} "
            f"Bewertung: {_account_hint(item)}"
        )

    lines.extend([
        "",
        "## Account-Intelligence",
        "",
    ])

    for item in top_items:
        headline = item.get("headline") or item.get("org") or "Account-Signal"
        org = item.get("org") or "Markt"
        lines.extend([
            f"### {org}: {headline}",
            "",
            f"**Signal**  ",
            f"{_clean_sentence(item.get('text', ''), 620)} {_item_link(item)}",
            "",
            f"**Bewertung**  ",
            _account_hint(item),
            "",
        ])

    lines.extend([
        "## Beobachtungsliste",
        "",
        f"- Accounts/Organisationen aus diesem Lauf: {', '.join(account_orgs[:12]) or 'keine eindeutigen Accounts' }.",
        "- Wiederkehrende Entscheider, offizielle Accounts und Dienstleistersignale im naechsten Lauf hoeher gewichten.",
        "",
    ])
    lines.extend(_source_overview(items))
    return "\n".join(lines).strip() + "\n"


def generate_account_intelligence_summary(client: digest.openai.OpenAI, all_research: str, today: date) -> str:
    """Model-based account-intelligence briefing with deterministic fallback."""
    last_week = digest.load_last_week()
    source_pack, editorial_items = digest.build_editorial_source_pack(all_research)
    if not source_pack:
        return build_account_intelligence_fallback(all_research, today)

    prompt = f"""Erstelle aus den Quellen ein Account-Intelligence-Briefing GKV fuer Christian Galler als Account Manager im IT-Vertrieb fuer gesetzliche Krankenkassen.

Leitfrage: Was sollte Christian diese Woche ueber Markt, Kunden, Politik, Kassen-IT, Dienstleister, Top-Stimmen, Fusionen, Kooperationen und gemeinsame IT-Projekte wissen?

Zielaccount-Kontext: Beruecksichtige alle relevanten Kassen, Institutionen und Dienstleister aus dem Quellenpaket. Ordne Signale nur dort einem Account zu, wo die Quelle oder eine belastbare Marktlogik das hergibt. Keine kuenstlichen Fokuslisten, keine DAK-Fixierung, keine Restlauf-Kommentare.

Quellenpaket:
{source_pack[:52000]}

Bereits letzte Woche berichtet, nicht ohne neue Entwicklung wiederholen:
{last_week[:2500]}

Pflichtstruktur:

## Management Summary
3 bis 6 bewertete Punkte. Jeder Punkt muss Signal, Bedeutung und fachliche Account-Bedeutung in einem fachlichen Absatz oder Bullet zusammenfassen.

## Account-Intelligence
Pro wichtigem Signal ein Abschnitt mit:
**Signal**
**Bewertung**

## Dienstleister- und IT-Landschaft
BITMARCK, ITSC, AOK Systems, gematik, BSI und andere Institutionen aufnehmen, wenn daraus ein positives, belegbares Markt-, Projekt-, Umsetzungs-, Integrations-, Betriebs- oder Beratungsdrucksignal folgt. Keine Abschnitte, die nur sagen, dass kein belastbares Signal vorliegt.

## Kassen-, Politik- und Top-Voice-Radar
Harte politische/regulatorische Fakten, weichere Kassen-RSS-Signale und relevante LinkedIn-Top-Stimmen aus Kassen- und IT-Landschaft fachlich zusammenfuehren. Beispiele fuer relevante Stimmen: DAK-Pressestelle, IKK-classic-CDO Stefan Schellberg, BITMARCK-CEO Andreas Strausfeld, ITSC-CEO Dieter Loewe.

## Quellenbasis
Kurze gruppierte Quellenliste.

Regeln:
- Nicht als Newsletter schreiben.
- Nicht nur berichten, sondern bewerten.
- Keine Artikelkarten, keine grossen Bildstrecken, keine generischen Einordnungen.
- Keine Roh-IDs, keine Score-Artefakte, keine Labels wie Rohsignal oder Quellenradar.
- Keine Rubrik und keine Formulierung "Gespraechsanlass".
- Keine separate Rubrik "Account-Relevanz"; fachliche Account-Bedeutung in die Bewertung integrieren.
- Keine Rubrik "Zielkassen-spezifische Relevanz".
- Keine redaktionellen Negativbloecke wie "Rest des Laufs", "nicht kuenstlich aufblasen",
  "keine belastbaren Signale", "nur nachrangig relevant" oder Zusammenfassungen nicht relevanter Kassen.
- Quellen ohne verwertbares Signal einfach weglassen.
- Keine neuen Fakten erfinden; weiche Schluesse als Hypothese oder Signal markieren.
- Quellenlinks kurz als [Quelle](URL), [LinkedIn](URL) oder [Zum Artikel](URL).
- Deutsch, praezise, account-orientiert.
"""

    try:
        if digest.NEWSLETTER_API == "responses":
            response = client.responses.create(
                model=digest.NEWSLETTER_MODEL,
                instructions=digest.SYSTEM_PROMPT + "\n" + ACCOUNT_INTELLIGENCE_SYSTEM,
                max_output_tokens=9000,
                input=prompt,
            )
            result = response.output_text or ""
        else:
            completion = client.chat.completions.create(
                model=digest.NEWSLETTER_MODEL,
                max_completion_tokens=9000,
                messages=[
                    {"role": "system", "content": digest.SYSTEM_PROMPT + "\n" + ACCOUNT_INTELLIGENCE_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
            )
            result = completion.choices[0].message.content or ""
    except Exception as exc:
        print(f"   Hinweis: Account-Intelligence-Modellfassung fehlgeschlagen, nutze Fallback: {exc}", file=digest.sys.stderr)
        return build_account_intelligence_fallback(all_research, today)

    if len(result.strip()) < 700 or digest.newsletter_needs_repair(result, len(editorial_items)):
        return build_account_intelligence_fallback(all_research, today)
    return result.strip() + "\n"


# Patch the editorial layer before digest.main() runs.
digest.SYSTEM_PROMPT = digest.SYSTEM_PROMPT + "\n" + ACCOUNT_INTELLIGENCE_SYSTEM
digest.build_source_based_newsletter = build_account_intelligence_fallback
digest.generate_executive_summary = generate_account_intelligence_summary


if __name__ == "__main__":
    digest.main()
