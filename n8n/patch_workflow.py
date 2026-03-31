#!/usr/bin/env python3
"""
Patcht den KassenInfodienst n8n Workflow:
- Entfernt Haiku-Nodes (spart ~80% Tokens)
- Ersetzt format-for-claude durch JS-Relevanz-Filter (direkt zu Sonnet)
- Lockert LinkedIn Posts aufbereiten Filter drastisch
- Setzt Sonnet max_tokens auf 2000
- Aktualisiert Breaking News RSS + Filter
- NEU: Ersetzt alle LinkedIn-Keyword-Nodes durch Query-Generator + API-Call
  (gezielte Suche: keyword + authorCompany + authorJobTitle statt boolean)
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

# ─── CODES ──────────────────────────────────────────────────────────

NEW_LINKEDIN_FLATTEN = """// LinkedIn Posts aus API-Response extrahieren (permissiver Filter)
const items = $input.all();
const result = [];
const seenTexts = new Set();

for (const item of items) {
  // Error-Antworten überspringen
  if (item.json.error) continue;

  let posts = [];
  if (item.json.data && item.json.data.posts && Array.isArray(item.json.data.posts)) {
    posts = item.json.data.posts;
  } else if (item.json.data && item.json.data.posts && item.json.data.posts.items) {
    posts = item.json.data.posts.items;
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

NEW_LINKEDIN_QUERY_GEN = """// LinkedIn Query Generator – gezielte Suchen statt boolean keywords
// Company IDs aus linkdAPI /search/companies aufgelöst
const BASE = "https://linkdapi.com/api/v1/search/posts";
const C = {
  bitmarck: "955964",
  itsc:     "8195481",
  gematik:  "125822",
  dak:      "42928985",
  tk:       "41317",
  barmer:   "18012268",
  hkk:      "10699871",
  hek:      "36667598",
  kkh:      "39196",
  ikk:      "1412986",
  viactiv:  "12414104"
};

const queries = [
  // IT-Dienstleister
  { keyword: "ePA",           authorCompany: C.gematik },
  { keyword: "Telematik",     authorCompany: C.gematik },
  { keyword: "Cloud",         authorCompany: C.bitmarck, authorJobTitle: "CIO" },
  { keyword: "Atlassian",     authorCompany: C.bitmarck },
  { keyword: "SAP",           authorCompany: C.bitmarck },
  { keyword: "Digitalisierung", authorCompany: C.bitmarck },
  { keyword: "IT-Strategie",  authorCompany: C.itsc },
  { keyword: "Cloud",         authorCompany: C.itsc },
  // GKV Kassen – Entscheider
  { keyword: "ePA",           authorCompany: C.dak,    authorJobTitle: "Vorstand" },
  { keyword: "Digitalisierung", authorCompany: C.tk,   authorJobTitle: "CIO" },
  { keyword: "NIS2",          authorCompany: C.barmer },
  { keyword: "ePA",           authorCompany: C.tk },
  // GKV-weit nach Rolle
  { keyword: "Digitalisierung", authorJobTitle: "CIO" },
  { keyword: "NIS2",            authorJobTitle: "Vorstand" },
  { keyword: "ePA",             authorJobTitle: "CIO" },
];

return queries.map(q => {
  const p = new URLSearchParams({
    keyword:    q.keyword,
    sortBy:     "relevance",
    datePosted: "past-week",
    count:      "10"
  });
  if (q.authorCompany)  p.append("authorCompany",  q.authorCompany);
  if (q.authorJobTitle) p.append("authorJobTitle", q.authorJobTitle);
  return { json: { url: `${BASE}?${p.toString()}`, ...q } };
});"""

NEW_FORMAT_FOR_CLAUDE = """// JS-Relevanz-Filter: ersetzt Haiku, spart ~80% Tokens
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

# ─── IDs / Namen der zu entfernenden LinkedIn-Nodes ─────────────────
# Alle HTTP-Request-Nodes die linkdapi /search/posts aufrufen werden entfernt
# Plus die alte Suchbegriffe-Code-Node
REMOVE_IDS_FIXED = {"claude-haiku-score", "extract-haiku-text", "linkedin-suchbegriffe"}

# ─── NEUE NODES ─────────────────────────────────────────────────────
# Position: neben den alten LinkedIn-Nodes (wird ggf. in n8n manuell verschoben)
LINKEDIN_QUERY_GEN_NODE = {
    "id": "linkedin-query-gen",
    "name": "LinkedIn Query Generator",
    "type": "n8n-nodes-base.code",
    "typeVersion": 2,
    "position": [460, 540],
    "parameters": {
        "mode": "runOnceForAllItems",
        "jsCode": NEW_LINKEDIN_QUERY_GEN
    }
}

LINKEDIN_API_CALL_NODE = {
    "id": "linkedin-api-call",
    "name": "LinkedIn API Call",
    "type": "n8n-nodes-base.httpRequest",
    "typeVersion": 4.2,
    "position": [680, 540],
    "parameters": {
        "method": "GET",
        "url": "={{ $json.url }}",
        "authentication": "genericCredentialType",
        "genericAuthType": "httpHeaderAuth",
        "options": {
            "timeout": 30000,
            "response": {
                "response": {
                    "neverError": True
                }
            }
        }
    },
    "credentials": {
        "httpHeaderAuth": {
            "id": "2AZJHqeS0X9NUG2A",
            "name": "linkdAPI Key"
        }
    },
    "onError": "continueRegularOutput"
}

# ─── NODES PATCHEN ───────────────────────────────────────────────────
updated = 0
new_nodes = []
removed_names = []   # Namen entfernter Nodes für Connection-Cleanup
trigger_node_name = None

def is_linkedin_search_node(node):
    """True wenn HTTP-Request-Node der linkdapi /search/posts aufruft."""
    if node.get("type") != "n8n-nodes-base.httpRequest":
        return False
    params = node.get("parameters", {})
    url = params.get("url", "")
    if isinstance(url, str):
        return ("linkdapi.com/api/v1/search/posts" in url or
                "linkd.app/api/v1/search/posts" in url)
    return False

linkedin_api_added = False

for node in nodes:
    nid  = node.get("id", "")
    name = node.get("name", "")
    ntype = node.get("type", "")

    # Trigger-Node merken (für Connection-Update)
    if ntype == "n8n-nodes-base.scheduleTrigger" or "06:00" in name or "trigger" in name.lower():
        trigger_node_name = name

    # Feste IDs entfernen (Haiku + alte Suchbegriffe)
    if nid in REMOVE_IDS_FIXED:
        print(f"   ENTFERNT: {name}")
        removed_names.append(name)
        continue

    # Alle linkdapi-Search-Nodes entfernen (inkl. manuell hinzugefügte)
    if is_linkedin_search_node(node):
        print(f"   ENTFERNT (linkdAPI Search): {name}")
        removed_names.append(name)
        # Neue Nodes einmal an dieser Stelle einfügen
        if not linkedin_api_added:
            new_nodes.append(LINKEDIN_QUERY_GEN_NODE)
            new_nodes.append(LINKEDIN_API_CALL_NODE)
            linkedin_api_added = True
            print("   HINZUGEFÜGT: LinkedIn Query Generator")
            print("   HINZUGEFÜGT: LinkedIn API Call")
        continue

    # LinkedIn Posts aufbereiten (linkedin-flatten)
    if nid == "linkedin-flatten":
        node["parameters"]["jsCode"] = NEW_LINKEDIN_FLATTEN
        print(f"   UPDATED: {name} (permissiver Filter)")
        updated += 1

    # format-for-claude → JS-Filter
    elif nid == "format-for-claude":
        node["parameters"]["jsCode"] = NEW_FORMAT_FOR_CLAUDE
        node["parameters"]["mode"] = "runOnceForAllItems"
        print(f"   UPDATED: {name} (JS-Relevanz-Filter)")
        updated += 1

    # Sonnet: max_tokens
    elif nid == "claude-sonnet-newsletter":
        body_str = node["parameters"].get("jsonBody", "")
        body_str = body_str.replace('"max_tokens": 6000', '"max_tokens": 2000')
        body_str = body_str.replace('"max_tokens":6000',  '"max_tokens":2000')
        node["parameters"]["jsonBody"] = body_str
        print(f"   UPDATED: {name} (max_tokens → 2000)")
        updated += 1

    # Breaking News RSS URL
    elif nid == "breaking-rss":
        node["parameters"]["url"] = NEW_BREAKING_RSS_URL
        print(f"   UPDATED: {name} (neue RSS URL)")
        updated += 1

    # Breaking News Filter → Code Node
    elif nid == "breaking-filter":
        node["type"]        = "n8n-nodes-base.code"
        node["typeVersion"] = 2
        node["parameters"]  = {"mode": "runOnceForAllItems", "jsCode": NEW_BREAKING_FILTER}
        node["name"]        = "Breaking: Aktuell + GKV-relevant?"
        print(f"   UPDATED: {name} → Code Node")
        updated += 1

    new_nodes.append(node)

# Falls kein linkdapi-Node gefunden (erster Run oder abweichende IDs)
if not linkedin_api_added:
    new_nodes.append(LINKEDIN_QUERY_GEN_NODE)
    new_nodes.append(LINKEDIN_API_CALL_NODE)
    print("   HINZUGEFÜGT: LinkedIn Query Generator (am Ende)")
    print("   HINZUGEFÜGT: LinkedIn API Call (am Ende)")

print(f"\n   {updated} Nodes geändert, {len(nodes)-len(new_nodes)+2} entfernt")

# ─── CONNECTIONS UPDATEN ─────────────────────────────────────────────
print("\n2. Connections aktualisieren...")

# Entfernte Nodes aus Connections löschen
for removed in removed_names:
    if removed in connections:
        del connections[removed]
        print(f"   Connection entfernt: {removed}")

# Haiku-Chain entfernen, format-for-claude direkt zu Sonnet
for name_key in list(connections.keys()):
    if "Claude formatieren" in name_key or name_key == "format-for-claude":
        connections[name_key] = {
            "main": [[{"node": "Claude Sonnet: Newsletter schreiben", "type": "main", "index": 0}]]
        }
        print(f"   {name_key} → Sonnet (Haiku überbrückt)")

# Haiku-eigene Connections entfernen
for haiku_name in ["Claude Haiku: Relevanz-Scoring", "Haiku-Text extrahieren"]:
    if haiku_name in connections:
        del connections[haiku_name]
        print(f"   Connection entfernt: {haiku_name}")

# Breaking News Filter Connection umbenennen
if "Neu in letzten 35min?" in connections:
    connections["Breaking: Aktuell + GKV-relevant?"] = connections.pop("Neu in letzten 35min?")
    print("   Breaking Filter Connection aktualisiert")

# Neue LinkedIn Chain: Query Generator → API Call → Posts aufbereiten
connections["LinkedIn Query Generator"] = {
    "main": [[{"node": "LinkedIn API Call", "type": "main", "index": 0}]]
}
connections["LinkedIn API Call"] = {
    "main": [[{"node": "LinkedIn Posts aufbereiten", "type": "main", "index": 0}]]
}
print("   LinkedIn Query Generator → API Call → Posts aufbereiten")

# Trigger: entfernte LinkedIn-Nodes raus, Query Generator rein
if trigger_node_name and trigger_node_name in connections:
    trigger_conn = connections[trigger_node_name]
    for branch in trigger_conn.get("main", []):
        # Entfernte Nodes aus Trigger-Connections entfernen
        branch[:] = [c for c in branch if c.get("node") not in removed_names]
        # Query Generator hinzufügen falls noch nicht vorhanden
        if not any(c.get("node") == "LinkedIn Query Generator" for c in branch):
            branch.append({"node": "LinkedIn Query Generator", "type": "main", "index": 0})
    print(f"   Trigger ({trigger_node_name}) → LinkedIn Query Generator")

# ─── WORKFLOW DEAKTIVIEREN ───────────────────────────────────────────
print("\n3. Workflow deaktivieren...")
api("POST", f"/workflows/{WORKFLOW_ID}/deactivate")

# ─── WORKFLOW PUSHEN ─────────────────────────────────────────────────
print("4. Workflow pushen...")
put_body = {
    "name": wf["name"],
    "nodes": new_nodes,
    "connections": connections,
    "settings": {k: v for k, v in wf.get("settings", {}).items()
                 if k in ("executionOrder", "saveManualExecutions", "callerPolicy",
                          "errorWorkflow", "timezone", "saveDataSuccessExecution",
                          "saveDataErrorExecution", "saveExecutionProgress")},
    "staticData": wf.get("staticData", None),
}

result = api("PUT", f"/workflows/{WORKFLOW_ID}", put_body)
print(f"   OK: Workflow '{result.get('name')}' gespeichert")
print(f"   Nodes jetzt: {len(result.get('nodes', []))}")

print("\n✅ FERTIG!")
print("   - Haiku entfernt (spart ~80% Tokens)")
print("   - LinkedIn: 15 gezielte Queries (keyword + authorCompany + authorJobTitle)")
print("   - linkdapi.com als Base-URL (kein Timeout mehr)")
print("   - Breaking News aktualisiert")
print("   - Workflow ist DEAKTIVIERT – erst testen, dann manuell aktivieren")
