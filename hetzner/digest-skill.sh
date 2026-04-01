#!/bin/bash
# OpenClaw-Skill: KassenInfodienst Digest ausfuehren
#
# Dieses Skript wird von OpenClaw aufgerufen, z.B. per Telegram-Nachricht:
#   "Erstelle den wöchentlichen Digest"
# oder per Cron-Skill in OpenClaw (freitags 09:00 Uhr).
#
# Einrichten in OpenClaw:
#   openclaw skill add --name digest --command /workspace/kasseninfodienst/hetzner/digest-skill.sh

set -euo pipefail

WORKSPACE="/workspace/kasseninfodienst"
PYTHON_BIN="python3"

# Umgebungsvariablen aus der OpenClaw .env laden
export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"
export GMAIL_USER="${GMAIL_USER:-}"
export GMAIL_APP_PASSWORD="${GMAIL_APP_PASSWORD:-}"
export RECIPIENT_EMAIL="${RECIPIENT_EMAIL:-}"
export LINKDAPI_KEY="${LINKDAPI_KEY:-}"
export LINKEDIN_LI_AT="${LINKEDIN_LI_AT:-}"
export LINKEDIN_JSESSIONID="${LINKEDIN_JSESSIONID:-}"

cd "$WORKSPACE"

# Abhängigkeiten sicherstellen
$PYTHON_BIN -m pip install -q -r requirements.txt

# Digest ausführen (mit E-Mail-Versand wenn konfiguriert)
if [ -n "$GMAIL_USER" ] && [ -n "$GMAIL_APP_PASSWORD" ]; then
    echo "Starte Digest mit E-Mail-Versand..."
    $PYTHON_BIN digest.py --email
else
    echo "Starte Digest ohne E-Mail-Versand (GMAIL_USER/GMAIL_APP_PASSWORD nicht gesetzt)..."
    $PYTHON_BIN digest.py
fi
