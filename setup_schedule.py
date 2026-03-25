#!/usr/bin/env python3
"""
Richtet den wöchentlichen KassenInfodienst auf dem Mac ein.
Erstellt einen LaunchAgent, der jeden Freitag um 07:00 Uhr den Digest startet
und per E-Mail verschickt.

Ausführen (einmalig):
    python3 setup_schedule.py
"""

import os
import subprocess
import sys
from pathlib import Path

PLIST_LABEL = "com.kasseninfodienst.weekly"
PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / f"{PLIST_LABEL}.plist"


def main() -> None:
    # Pfade ermitteln
    project_dir = Path(__file__).parent.resolve()
    python_bin = Path(sys.executable).resolve()
    digest_script = project_dir / "digest.py"
    log_dir = project_dir / "logs"
    log_dir.mkdir(exist_ok=True)

    print(f"📁 Projektverzeichnis : {project_dir}")
    print(f"🐍 Python             : {python_bin}")
    print(f"📄 Skript             : {digest_script}")
    print()

    # Uhrzeit abfragen
    hour_input = input("⏰ Uhrzeit (Stunde, 0–23, Standard 7): ").strip()
    hour = int(hour_input) if hour_input.isdigit() else 7

    minute_input = input("⏰ Minute (0–59, Standard 0): ").strip()
    minute = int(minute_input) if minute_input.isdigit() else 0

    print()
    print(f"Der Digest wird jeden Freitag um {hour:02d}:{minute:02d} Uhr ausgeführt.")
    print("E-Mail wird automatisch versendet (--email Flag).")
    print()

    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_LABEL}</string>

    <key>ProgramArguments</key>
    <array>
        <string>{python_bin}</string>
        <string>{digest_script}</string>
        <string>--email</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{project_dir}</string>

    <!-- Jeden Freitag (Weekday=5) um {hour:02d}:{minute:02d} Uhr -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key>
        <integer>5</integer>
        <key>Hour</key>
        <integer>{hour}</integer>
        <key>Minute</key>
        <integer>{minute}</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>{log_dir}/weekly.log</string>

    <key>StandardErrorPath</key>
    <string>{log_dir}/weekly.err</string>

    <!-- Nicht nachholen wenn Mac ausgeschaltet war -->
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
"""

    # Vorhandenen Job ggf. entladen
    if PLIST_PATH.exists():
        print(f"🔄 Vorhandener LaunchAgent wird neu geladen ...")
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)], capture_output=True)

    # Plist schreiben
    PLIST_PATH.write_text(plist_content, encoding="utf-8")
    print(f"✅ LaunchAgent erstellt: {PLIST_PATH}")

    # Laden
    result = subprocess.run(
        ["launchctl", "load", str(PLIST_PATH)],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"✅ LaunchAgent geladen – läuft ab sofort jeden Freitag um {hour:02d}:{minute:02d} Uhr.")
    else:
        print(f"⚠️  launchctl load Fehler: {result.stderr}", file=sys.stderr)
        print(f"   Manuell laden: launchctl load {PLIST_PATH}")

    print()
    print("📋 Nächste Schritte:")
    print("   1. Gmail App-Passwort in .env eintragen (GMAIL_USER, GMAIL_APP_PASSWORD)")
    print("   2. Empfänger in .env eintragen (RECIPIENT_EMAIL)")
    print("   3. Jetzt testen: python3 digest.py --kassen TK --email")
    print()
    print("   Logs:")
    print(f"   tail -f {log_dir}/weekly.log")
    print(f"   tail -f {log_dir}/weekly.err")
    print()
    print("   Job deaktivieren:")
    print(f"   launchctl unload {PLIST_PATH}")


if __name__ == "__main__":
    main()
