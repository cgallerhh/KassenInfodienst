#!/bin/bash
# OpenClaw Installation Script fuer Hetzner Server
# Ubuntu 22.04 / 24.04 (auch Debian 12)
#
# Verwendung:
#   chmod +x setup.sh
#   sudo bash setup.sh

set -euo pipefail

OPENCLAW_DIR="/opt/openclaw"
OPENCLAW_USER="openclaw"
ENV_FILE="$OPENCLAW_DIR/.env"

echo "=== OpenClaw Setup fuer Hetzner ==="

# 1. System aktualisieren
echo "[1/7] System aktualisieren..."
apt-get update -qq && apt-get upgrade -y -qq

# 2. Abhängigkeiten installieren
echo "[2/7] Abhängigkeiten installieren..."
apt-get install -y -qq \
    curl \
    git \
    ca-certificates \
    gnupg \
    ufw

# 3. Node.js 22 installieren
echo "[3/7] Node.js 22 installieren..."
if ! command -v node &>/dev/null || [[ $(node -v | cut -d. -f1 | tr -d 'v') -lt 22 ]]; then
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
    apt-get install -y nodejs
fi
echo "Node.js Version: $(node -v)"

# 4. OpenClaw global installieren
echo "[4/7] OpenClaw installieren..."
npm install -g openclaw@latest
echo "OpenClaw Version: $(openclaw --version)"

# 5. Benutzer und Verzeichnis anlegen
echo "[5/7] Systembenutzer und Verzeichnis anlegen..."
if ! id "$OPENCLAW_USER" &>/dev/null; then
    useradd --system --create-home --home-dir "$OPENCLAW_DIR" \
            --shell /bin/bash "$OPENCLAW_USER"
fi
mkdir -p "$OPENCLAW_DIR"
chown -R "$OPENCLAW_USER:$OPENCLAW_USER" "$OPENCLAW_DIR"

# 6. Umgebungsvariablen einrichten (falls noch nicht vorhanden)
if [ ! -f "$ENV_FILE" ]; then
    echo "[6/7] Beispiel-.env anlegen (bitte befüllen!)..."
    cp "$(dirname "$0")/.env.openclaw.example" "$ENV_FILE"
    chown "$OPENCLAW_USER:$OPENCLAW_USER" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "WARNUNG: Bitte $ENV_FILE mit echten Werten befüllen, dann 'systemctl start openclaw' ausführen."
else
    echo "[6/7] .env bereits vorhanden – wird nicht überschrieben."
fi

# 7. systemd-Dienst installieren und starten
echo "[7/7] systemd-Dienst einrichten..."
openclaw service install --systemd
systemctl daemon-reload
systemctl enable openclaw

echo ""
echo "=== Installation abgeschlossen ==="
echo ""
echo "Nächste Schritte:"
echo "  1. $ENV_FILE mit API-Keys befüllen"
echo "  2. sudo systemctl start openclaw"
echo "  3. openclaw doctor       # System prüfen"
echo "  4. openclaw gateway status"
echo ""
echo "Firewall-Empfehlung (kein öffentlicher Port):"
echo "  ufw default deny incoming"
echo "  ufw allow OpenSSH"
echo "  ufw enable"
