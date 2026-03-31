#!/usr/bin/env bash
# KassenInfodienst – n8n Setup Script
# Importiert den Workflow und setzt Variablen via n8n REST API
#
# Voraussetzungen:
#   - curl installiert
#   - n8n läuft auf N8N_URL
#   - API-Key aus n8n (Settings → API → Create API Key)
#
# Verwendung:
#   N8N_URL=http://89.167.14.159:5678 \
#   N8N_API_KEY=<dein-api-key> \
#   ANTHROPIC_API_KEY=<claude-api-key> \
#   RECIPIENT_EMAIL=<deine@email.de> \
#   TELEGRAM_CHAT_ID=<chat-id> \
#   ./n8n/setup.sh

set -euo pipefail

N8N_URL="${N8N_URL:-http://89.167.14.159:5678}"
N8N_API_KEY="${N8N_API_KEY:?Bitte N8N_API_KEY setzen (n8n Settings → API → Create API Key)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOW_FILE="$SCRIPT_DIR/kasseninfodienst_workflow.json"

echo "=== KassenInfodienst n8n Setup ==="
echo "Instance: $N8N_URL"
echo ""

# 1. Verbindung testen
echo "→ Verbindung testen..."
if ! curl -sf "$N8N_URL/healthz" > /dev/null 2>&1; then
  echo "✗ n8n nicht erreichbar unter $N8N_URL"
  exit 1
fi
echo "✓ n8n erreichbar"

# 2. Workflow importieren
echo "→ Workflow importieren..."
IMPORT_RESPONSE=$(curl -sf \
  -X POST "$N8N_URL/api/v1/workflows" \
  -H "X-N8N-API-KEY: $N8N_API_KEY" \
  -H "Content-Type: application/json" \
  -d "@$WORKFLOW_FILE")

WORKFLOW_ID=$(echo "$IMPORT_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "✓ Workflow importiert (ID: $WORKFLOW_ID)"

# 3. Variablen setzen (falls angegeben)
set_variable() {
  local key="$1" value="$2"
  curl -sf \
    -X POST "$N8N_URL/api/v1/variables" \
    -H "X-N8N-API-KEY: $N8N_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"$key\",\"value\":\"$value\",\"type\":\"string\"}" > /dev/null
  echo "✓ Variable gesetzt: $key"
}

echo "→ Variablen setzen..."
[[ -n "${ANTHROPIC_API_KEY:-}" ]] && set_variable "anthropicApiKey" "$ANTHROPIC_API_KEY"
[[ -n "${RECIPIENT_EMAIL:-}" ]]   && set_variable "RECIPIENT_EMAIL"  "$RECIPIENT_EMAIL"
[[ -n "${TELEGRAM_CHAT_ID:-}" ]]  && set_variable "TELEGRAM_CHAT_ID" "$TELEGRAM_CHAT_ID"

# 4. Workflow aktivieren
if [[ -n "${WORKFLOW_ID:-}" ]]; then
  echo "→ Workflow aktivieren..."
  curl -sf \
    -X PATCH "$N8N_URL/api/v1/workflows/$WORKFLOW_ID" \
    -H "X-N8N-API-KEY: $N8N_API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"active":true}' > /dev/null
  echo "✓ Workflow aktiviert"
fi

echo ""
echo "=== Setup abgeschlossen ==="
echo ""
echo "Nächste Schritte in n8n (http://89.167.14.159:5678):"
echo "  1. Settings → Credentials → New:"
echo "     • Gmail OAuth2 (für Gmail versenden)"
echo "     • HTTP Header Auth: Name=LinkdAPI, Header=X-API-Key, Value=<LINKDAPI_KEY>"
echo "     • Telegram API: Bot-Token aus @BotFather"
echo "  2. Credentials in den jeweiligen Nodes zuweisen"
echo "  3. Workflow einmal manuell testen (Execute Workflow)"
echo ""
echo "Öffne: $N8N_URL/workflow/$WORKFLOW_ID"
