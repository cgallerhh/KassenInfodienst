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
import os
import smtplib
import sys
import time
from datetime import date, timedelta
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

BATCH_SIZE = 3          # Kassen pro Claude-Aufruf
MAX_SEARCHES = 4        # Web-Suchen pro Batch (weniger = weniger Rate-Limits)
BATCH_PAUSE = 10        # Sekunden Pause zwischen Batches
MAX_RETRIES = 2         # Wiederholungsversuche bei Fehler
REPORTS_DIR = Path("reports")


# ---------------------------------------------------------------------------
# System-Prompt (einmalig, gecacht)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Du bist ein Research-Assistent für einen erfahrenen Account Manager im B2B-Vertrieb.
Seine Kunden sind gesetzliche Krankenkassen in Deutschland.
Er möchte einen wöchentlichen Überblick, um Verkaufschancen zu identifizieren.

Recherchiere für jede Krankenkasse im Batch die relevanten Neuigkeiten und leite daraus
konkrete Handlungsempfehlungen für den Account Manager ab.

OUTPUT-FORMAT (für jede Kasse im Batch genau dieses Format, auf Deutsch):

---
## [Name der Kasse] | [Domain]

### 👤 Personal & Vorstand
[Findings zu Vorstandswechsel, neuen Führungskräften, CIO/CTO/CEO-Änderungen.
Nenne immer Namen, Datum und Quelle falls verfügbar.
Falls nichts gefunden: "Keine aktuellen Personalveränderungen gefunden."]

### 💻 IT-Vorhaben & Digitalisierung
[Findings zu IT-Projekten, Ausschreibungen, Cloud-Migration, neue Software/Systeme,
eHealth-Projekte, Plattformprojekte, Digitalisierungsvorhaben.
Konkrete Projekte mit Budget und Zeitplan falls verfügbar.
Falls nichts gefunden: "Keine aktuellen IT-Meldungen gefunden."]

### 💰 Haushaltsplanung & Finanzen
[Findings zu Beitragssatzänderungen, Finanzberichten, Haushaltsplänen,
Sparmaßnahmen oder Investitionsankündigungen.
Falls nichts gefunden: "Keine aktuellen Finanzmeldungen gefunden."]

### 📋 TED-Ausschreibungen
[Öffentliche Ausschreibungen dieser Kasse auf ted.europa.eu.
Suche nach: "[Kassenname] site:ted.europa.eu" und "[Kassenname] Ausschreibung".
Falls nichts gefunden: "Keine aktuellen TED-Ausschreibungen gefunden."]

### 🔗 LinkedIn (Entscheider-Posts)
[Posts von Vorständen, CIOs, CTO, Leitungen Digitalisierung dieser Kasse auf LinkedIn.
Suche nach: "[Kassenname] Vorstand LinkedIn" und "[Kassenname] site:linkedin.com".
Falls nichts gefunden: "Keine relevanten LinkedIn-Posts gefunden."]

### 💡 Verkaufschancen für den Account Manager
[2–4 konkrete, handlungsorientierte Empfehlungen. Beispiele:
- "Vorstandswechsel → Antrittsbesuche anfragen, neue Prioritäten erfragen"
- "IT-Ausschreibung für CRM läuft → Angebot vorbereiten bis [Datum]"
- "Neuer CIO seit [Datum] → Kennenlerngespräch initiieren"
- "Beitragssatzerhöhung → Effizienzlösungen proaktiv platzieren"]

Schreibe auf Deutsch. Sei präzise und faktenbasiert. Vermeide allgemeine Floskeln."""


# ---------------------------------------------------------------------------
# Kern-Funktion: Einen Batch Kassen recherchieren
# ---------------------------------------------------------------------------

def research_batch(client: anthropic.Anthropic, batch: list[dict], tage: int) -> str:
    """Recherchiert einen Batch von Krankenkassen mittels Claude + Web Search."""

    today = date.today()
    period_start = (today - timedelta(days=tage)).strftime("%d.%m.%Y")
    period_end = today.strftime("%d.%m.%Y")

    kassen_liste = "\n".join(
        f"- **{k['name']}** | Website: {k['url']} | LinkedIn-Suche: \"{k['linkedin_search']}\""
        for k in batch
    )

    user_prompt = f"""Recherchiere aktuelle Informationen (Zeitraum: {period_start} – {period_end}) für folgende Krankenkassen:

{kassen_liste}

Suche für JEDE dieser Kassen nach:

1. **Personal**: Vorstandswechsel, neue Führungskräfte, CEO/CIO/CTO-Änderungen
   → Suche: "[Kassenname] Vorstand", "[Kassenname] Geschäftsführung 2025 2026"

2. **IT-Vorhaben**: Digitalisierungsprojekte, neue IT-Systeme, Cloud-Projekte, eHealth
   → Suche: "[Kassenname] IT Digitalisierung", "[Kassenname] Ausschreibung Software"

3. **Haushaltsplanung**: Beitragssatz, Finanznachrichten, Haushalt, Investitionen
   → Suche: "[Kassenname] Beitragssatz 2025 2026", "[Kassenname] Finanzen"

4. **TED-Ausschreibungen**: Öffentliche Vergaben auf ted.europa.eu
   → Suche: "[Kassenname] ted.europa.eu", "[Kassenname] Vergabe Ausschreibung"

5. **LinkedIn**: Posts von Entscheidern (Vorstände, CIO, Bereichsleiter Digital)
   → Suche: "[Kassenname] site:linkedin.com", "[LinkedIn-Suche aus Liste]"

Erstelle für JEDE Kasse eine vollständige Analyse im vorgegebenen Format.
Wenn du für eine Kasse keine Informationen findest, vermerke das klar."""

    full_text = ""

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=5000,
        system=system_prompt_with_cache(SYSTEM_PROMPT),
        tools=[
            {
                "type": "web_search_20260209",
                "name": "web_search",
                "max_uses": MAX_SEARCHES,
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        # Zeige Fortschritt während Claude recherchiert
        for text in stream.text_stream:
            print(text, end="", flush=True)
        print()  # Zeilenumbruch nach dem Stream

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

def generate_executive_summary(client: anthropic.Anthropic, all_research: str, today: date) -> str:
    """Erstellt eine kompakte Executive Summary der wichtigsten Findings."""

    prompt = f"""Basierend auf dem folgenden Recherche-Bericht über Krankenkassen:

{all_research[:8000]}

Erstelle eine kurze **Executive Summary** (max. 500 Wörter) für den Account Manager mit:

1. **Top 5 Sofortmaßnahmen** – Was muss diese Woche unbedingt getan werden?
2. **Größte Verkaufschancen** – Welche 3 Kassen haben das höchste Potenzial gerade?
3. **Wichtigste Personalveränderungen** – Wer ist neu im Markt?
4. **Relevanteste Ausschreibungen** – Welche TED-Vergaben laufen aktuell?

Schreibe auf Deutsch, prägnant und handlungsorientiert."""

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=2000,
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
      <p>Wöchentlicher Überblick &bull; {date_str}</p>
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

    # HTML-E-Mail (vollständiger gerenderter Bericht)
    html = build_html_email(report_content, today)
    msg.attach(MIMEText(html, "html", "utf-8"))

    print(f"📧 Sende HTML-E-Mail an {recipient} ...")
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(gmail_user, gmail_password)
        server.send_message(msg)
    print("   ✅ E-Mail gesendet.")


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

    return f"""# KassenInfodienst – Wöchentlicher Überblick

**Erstellt am:** {today.strftime("%d. %B %Y")}
**Recherchezeitraum:** {period_start} – {period_end}
**Kassen:** {len(kassen)} ({kassen_namen})

> Dieser Bericht wurde automatisch mit Claude (Anthropic) und Web-Recherche erstellt.
> Alle Angaben ohne Gewähr – bitte Quellen prüfen.

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

    header = make_report_header(today, args.tage, kassen)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header)

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
            except Exception as e:
                if attempt <= MAX_RETRIES:
                    wait = 30 * attempt
                    print(f"   ⚠️  Fehler (Versuch {attempt}): {e}", file=sys.stderr)
                    print(f"   ⏳ Warte {wait}s vor Wiederholung ...", file=sys.stderr)
                    time.sleep(wait)
                else:
                    print(f"   ❌ Batch {idx} nach {MAX_RETRIES + 1} Versuchen fehlgeschlagen: {e}", file=sys.stderr)
                    research = f"\n> ⚠️ Batch {idx} ({batch_names}) konnte nicht abgerufen werden.\n"

        all_research_parts.append(research)

        with open(output_path, "a", encoding="utf-8") as f:
            f.write(research)
            f.write("\n")

        print(f"   ✅ Fertig.")

        # Pause zwischen Batches (Search-Rate-Limit vermeiden)
        if idx < len(batches):
            print(f"   ⏳ Pause {BATCH_PAUSE}s ...")
            time.sleep(BATCH_PAUSE)

    # Executive Summary
    summary = ""
    if not args.kein_summary:
        print()
        print("📊 Erstelle Executive Summary ...")
        all_research = "\n".join(all_research_parts)

        try:
            summary = generate_executive_summary(client, all_research, today)
        except anthropic.APIError as e:
            print(f"   ⚠️  Summary-Fehler: {e}", file=sys.stderr)
            summary = f"> ⚠️ Executive Summary konnte nicht erstellt werden: {e}\n"

        # Summary an den Anfang einfügen (nach Header)
        existing_content = output_path.read_text(encoding="utf-8")
        summary_block = f"## 📊 Executive Summary\n\n{summary}\n\n---\n\n"
        new_content = existing_content.replace("---\n\n", "---\n\n" + summary_block, 1)
        output_path.write_text(new_content, encoding="utf-8")

        print("   ✅ Fertig.")

    print()
    print(f"✅ Bericht gespeichert: {output_path}")

    # E-Mail versenden
    if args.email:
        print()
        send_email(output_path, summary, today)

    print()
    print("Tipp: Wöchentliche Automatisierung → python setup_schedule.py")


if __name__ == "__main__":
    main()
