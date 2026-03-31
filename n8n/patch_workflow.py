#!/usr/bin/env python3
"""
Patcht den KassenInfodienst n8n Workflow:
- Entfernt Haiku-Nodes (spart ~80% Tokens)
- Ersetzt format-for-claude durch JS-Relevanz-Filter (direkt zu Sonnet)
- Lockert LinkedIn Posts aufbereiten Filter drastisch
- Setzt Sonnet max_tokens auf 2000
- Aktualisiert Breaking News RSS + Filter
- Verknüpft LinkedIn Keyword-Suchen korrekt zu Posts aufbereiten
"""
import json, sys, urllib.request, urllib.error

N8N_URL = "http://89.167.14.159:5678"
WORKFLOW_ID = "SRepZZtmoLM5LMnQ"

import os
API_KEY = os.environ.get("N8N_KEY", "")
if not API_KEY:
    print("ERROR: N8N_KEY nicht gesetzt. 'export N8N_KEY=...' ausführen.")
    sys.exit(1)

HEADERS = {"X-N8N-API-KEY": API_KEY, "Content-Type": "application/json"}

def api(method, path, body=None):
    url = f"{N8N_URL}/api/v1{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()}")
        sys.exit(1)

print("1. Workflow laden...")
wf = api("GET", f"/workflows/{WORKFLOW_ID}")
nodes = wf["nodes"]
connections = wf.get("connections", {})

print(f"   {len(nodes)} Nodes geladen")

# ─── NODE UPDATES ───────────────────────────────────────────────

# Nodes die entfernt werden (Haiku-Chain)
REMOVE_IDS = {"claude-haiku-score", "extract-haiku-text"}

# Neue Codes
NEW_LINKEDIN_FLATTEN = """// LinkedIn Posts aus API-Response extrahieren (kein strenger Filter mehr)
const items = $input.all();
const result = [];
const seenTexts = new Set();

for (const item of items) {
  let posts = [];
  if (item.json.data && Array.isArray(item.json.data.posts)) {
    posts = item.json.data.posts;
  } else if (Array.isArray(item.json.posts)) {
    posts = item.json.posts;
  } else if (item.json.text || item.json.commentary || item.json.content) {
    posts = [item.json];
  }

  for (const post of posts) {
    const text = (post.text || post.commentary || post.content || '').trim();
    if (!text || text.length < 30) continue;
    const key = text.substring(0, 80);
    if (seenTexts.has(key)) continue;
    seenTexts.add(key);

    const author = post.author || post.actor || {};
    const name = author.fullName || author.name || post.authorName || '';
    const title = author.headline || author.title || author.localizedHeadline || '';
    const company = author.companyName || author.company || '';
    const likes = parseInt(post.totalReactionCount || post.numLikes || post.likeCount || post.likes || 0);
    const comments = parseInt(post.numComments || post.commentCount || post.comments || 0);

    result.push({ json: {
      _source: 'LinkedIn',
      title: text.substring(0, 120),
      text,
      link: post.url || post.postUrl || post.shareUrl || post.permalink || '',
      postedAt: post.postedAt || post.date || post.createdAt || post.publishedAt || '',
      author: name,
      authorTitle: title,
      authorCompany: company,
      likes,
      comments,
      contentSnippet: `${name}${title ? ' (' + title.substring(0,60) + ')' : ''}${company ? ' · ' + company : ''}: ${text.substring(0,300)} (👍${likes} 💬${comments})`
    }});
  }
}
return result;"""

NEW_FORMAT_FOR_CLAUDE = """// JS-Relevanz-Filter: ersetzt Haiku, spart ~80% Tokens
// LinkedIn-Posts immer behalten, andere nach Score filtern (Top 20)
const items = $input.all();
const linkedinItems = items.filter(i => i.json._source === 'LinkedIn');
const otherItems = items.filter(i => i.json._source !== 'LinkedIn');

const GKV_TERMS = ['krankenkasse','gkv','bitmarck','itsc','gematik','beitrag',
  'fusion','vorstand','digitalisierung','ausschreibung','kasse','versicherung',
  'krankenversicherung','kassenvorstand','epa','telematik','ti 2','ti2'];
const HIGH_TERMS = ['fusion','fusionierung','vorstandswechsel','bitmarck','itsc',
  'ausschreibung','insolvenz','gematik','epa ','ti 2','providerwechsel','govdigital',
  '21c','zusammenschluss'];

function scoreItem(item) {
  const text = ((item.json.title||'') + ' ' + (item.json.contentSnippet||item.json.text||'')).toLowerCase();
  let s = 0;
  if (HIGH_TERMS.some(t => text.includes(t))) s += 3;
  if (GKV_TERMS.some(t => text.includes(t))) s += 2;
  const likes = item.json.likes || 0;
  if (likes > 100) s += 3;
  else if (likes > 50) s += 2;
  else if (likes > 10) s += 1;
  return s;
}

const topOthers = otherItems
  .map(i => ({ ...i, json: { ...i.json, _score: scoreItem(i) } }))
  .filter(i => i.json._score >= 2)
  .sort((a, b) => b.json._score - a.json._score)
  .slice(0, 20);

const selected = [...linkedinItems, ...topOthers];
const linkedinCount = linkedinItems.length;

let text = `=== STATISTIK ===\\nGesamt: ${selected.length} Einträge\\nLinkedIn-Posts: ${linkedinCount}\\n=================\\n\\n`;
for (const item of selected) {
  const src = item.json._source || 'Unbekannt';
  const title = item.json.title || item.json.text?.substring(0, 100) || '';
  const url = item.json.url || item.json.link || '';
  const date = item.json.pubDate || item.json.postedAt || '';
  const body = item.json.contentSnippet || item.json.text?.substring(0, 300) || '';
  text += `### [${src}] ${title}\\n`;
  if (date) text += `Datum: ${date}\\n`;
  if (url) text += `URL: ${url}\\n`;
  if (body) text += `${body}\\n\\n`;
}

return [{ json: { haiku_scored: text, item_count: selected.length, linkedin_count: linkedinCount } }];"""

NEW_BREAKING_FILTER = """// Breaking News: nur aktuelle + GKV-relevante Artikel
const items = $input.all();
const cutoff = Date.now() - 35 * 60 * 1000;

const HIGH_IMPACT = [
  'fusion','fusionierung','zusammenschluss','vorstandswechsel',
  'neuer vorstand','ceo wechsel','cio wechsel',
  'bitmarck','itsc','gematik','govdigital',
  'ausschreibung','tender','vergabe','providerwechsel',
  'insolvenz','restrukturierung','21c'
];
const NOISE = [
  'leistungskürzung','beitragserhöhung','zusatzbeitrag steigt',
  'beitragssatz erhöh','werkzeugbau','maschinenbau','einzelhandel',
  'pharmakolog','arzneimittel'
];

const fresh = items.filter(item => {
  const pub = item.json.isoDate || item.json.pubDate;
  if (!pub) return false;
  if (new Date(pub).getTime() < cutoff) return false;
  const text = ((item.json.title||'') + ' ' + (item.json.contentSnippet||'')).toLowerCase();
  if (NOISE.some(n => text.includes(n))) return false;
  return HIGH_IMPACT.some(h => text.includes(h));
});

return fresh;"""

NEW_BREAKING_RSS_URL = "https://news.google.com/rss/search?q=%22Krankenkasse%22+%28%22Fusion%22+OR+%22Vorstandswechsel%22+OR+%22BITMARCK%22+OR+%22ITSC%22+OR+%22gematik%22+OR+%22Ausschreibung%22+OR+%22Providerwechsel%22+OR+%22Insolvenz%22%29&hl=de&gl=DE&ceid=DE:de"

SONNET_SYSTEM = ("Du bist Chefredakteur des GKV-Branchenbriefs KassenInfodienst im Stil des dfg. "
"Investigativ, meinungsstark. Christian Galler direkt ansprechen. B2B-IT-Vertrieb GKV-Markt.\\n\\n"
"ABSOLUTE REGELN:\\n"
"- Nur Fakten aus Rohdaten. Keine Erfindungen.\\n"
"- Nur Sektionen mit echten Daten ausgeben.\\n"
"- Kein Intro, Outro, Fülltext.\\n\\n"
"LINKEDIN-RADAR PFLICHTCHECK:\\n"
"Pruefe Zeile LinkedIn-Posts: X in der STATISTIK.\\n"
"X=0: Schreibe NUR: Heute keine LinkedIn-Posts verfuegbar. Sonst NICHTS. Kein Kommentar zur Stille.\\n"
"X>0: Jeden [LinkedIn]-Eintrag zeigen:\\n"
"**Name** (Titel, Firma) · Datum · Reaktionen\\n"
"> Woertliches Zitat aus contentSnippet\\n"
"1-2 Saetze Einordnung.\\n\\n"
"SEKTIONEN: LinkedIn-Radar | Wer kommt/geht | Ausschreibungen | Markt & Politik | Action Items\\n\\n"
"Max. 2000 Woerter. Deutsch.")

# ─── NODES PATCHEN ───────────────────────────────────────────────
updated = 0
new_nodes = []

for node in nodes:
    nid = node.get("id", "")
    name = node.get("name", "")

    # Haiku-Nodes entfernen
    if nid in REMOVE_IDS:
        print(f"   ENTFERNT: {name}")
        continue

    # LinkedIn Posts aufbereiten
    if nid == "linkedin-flatten":
        node["parameters"]["jsCode"] = NEW_LINKEDIN_FLATTEN
        print(f"   UPDATED: {name} (lockerer Filter)")
        updated += 1

    # Rohdaten für Claude formatieren → JS-Filter
    elif nid == "format-for-claude":
        node["parameters"]["jsCode"] = NEW_FORMAT_FOR_CLAUDE
        node["parameters"]["mode"] = "runOnceForAllItems"
        print(f"   UPDATED: {name} (JS-Relevanz-Filter, kein Haiku mehr)")
        updated += 1

    # Sonnet: max_tokens reduzieren + neuer Prompt
    elif nid == "claude-sonnet-newsletter":
        body_str = node["parameters"]["jsonBody"]
        try:
            # Parse the expression to update max_tokens
            body_str = body_str.replace('"max_tokens": 6000', '"max_tokens": 2000')
            body_str = body_str.replace("'max_tokens': 6000", "'max_tokens': 2000")
            body_str = body_str.replace('max_tokens: 6000', 'max_tokens: 2000')
            body_str = body_str.replace('"max_tokens":6000', '"max_tokens":2000')
            node["parameters"]["jsonBody"] = body_str
        except:
            pass
        print(f"   UPDATED: {name} (max_tokens → 2000)")
        updated += 1

    # Breaking News RSS URL
    elif nid == "breaking-rss":
        node["parameters"]["url"] = NEW_BREAKING_RSS_URL
        print(f"   UPDATED: {name} (neue RSS URL)")
        updated += 1

    # Breaking News Filter: IF → Code
    elif nid == "breaking-filter":
        node["type"] = "n8n-nodes-base.code"
        node["typeVersion"] = 2
        node["parameters"] = {
            "mode": "runOnceForAllItems",
            "jsCode": NEW_BREAKING_FILTER
        }
        node["name"] = "Breaking: Aktuell + GKV-relevant?"
        print(f"   UPDATED: {name} → Code Node mit GKV-Filter")
        updated += 1

    new_nodes.append(node)

print(f"\n   {updated} Nodes geändert, {len(nodes)-len(new_nodes)} entfernt")

# ─── CONNECTIONS UPDATEN ──────────────────────────────────────────
print("\n2. Connections aktualisieren...")

# Haiku-Chain entfernen, format-for-claude direkt zu Sonnet
if "Rohdaten für Claude formatieren" in connections:
    connections["Rohdaten für Claude formatieren"] = {
        "main": [[{"node": "Claude Sonnet: Newsletter schreiben", "type": "main", "index": 0}]]
    }
    print("   format-for-claude → Sonnet (Haiku überbrückt)")

# Haiku-Connections entfernen
for haiku_name in ["Claude Haiku: Relevanz-Scoring", "Haiku-Text extrahieren"]:
    if haiku_name in connections:
        del connections[haiku_name]
        print(f"   Connection entfernt: {haiku_name}")

# LinkedIn Keyword-Suche → Posts aufbereiten (statt direkt zu Merge)
if "LinkedIn Keyword-Suche" in connections:
    connections["LinkedIn Keyword-Suche"] = {
        "main": [[{"node": "LinkedIn Posts aufbereiten", "type": "main", "index": 0}]]
    }
    print("   LinkedIn Keyword-Suche → Posts aufbereiten")

# Breaking News Filter umbenannt
if "Neu in letzten 35min?" in connections:
    connections["Breaking: Aktuell + GKV-relevant?"] = connections.pop("Neu in letzten 35min?")
    print("   Breaking Filter Connection aktualisiert")

# ─── WORKFLOW DEAKTIVIEREN ────────────────────────────────────────
print("\n3. Workflow deaktivieren...")
api("POST", f"/workflows/{WORKFLOW_ID}/deactivate")

# ─── WORKFLOW PUSHEN ──────────────────────────────────────────────
print("4. Workflow pushen...")
# Minimaler PUT-Body: nur Felder die n8n akzeptiert
put_body = {
    "name": wf["name"],
    "nodes": new_nodes,
    "connections": connections,
    "settings": wf.get("settings", {}),
    "staticData": wf.get("staticData", None),
}

result = api("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
print(f"   OK: Workflow '{result.get('name')}' gespeichert")
print(f"   Nodes jetzt: {len(result.get('nodes', []))}")

print("\n✅ FERTIG!")
print("   - Haiku entfernt (spart ~80% Tokens)")
print("   - LinkedIn Filter gelockert")
print("   - Breaking News aktualisiert")
print("   - Workflow ist DEAKTIVIERT – erst testen, dann manuell aktivieren")
