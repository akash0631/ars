# CLAUDE.md — Instructions for Claude Code

## DO NOT clone or set up locally. Work directly in this repo and deploy to Azure.

## PROJECT OVERVIEW
V2 Retail Auto Replenishment System. 320+ stores, 242 MAJCATs, replaces 20-machine Excel process.
Owner: Akash Agarwal, Director V2 Retail.

## HOW TO DEPLOY (every code change)
```bash
# 1. Get Azure token
TOKEN=$(curl -s -X POST "https://login.microsoftonline.com/3eb968d0-bf19-40f9-b191-f3186ac38f02/oauth2/v2.0/token" \
  -d "client_id=8f54a771-3b04-4458-bef3-f1fa98dc38a0" \
  -d "client_secret=[AZURE_SECRET_IN_ENV]" \
  -d "scope=https://management.azure.com/.default" -d "grant_type=client_credentials" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

# 2. Zip and deploy backend
cd backend
zip -r /tmp/ars-deploy.zip . -x "__pycache__/*" "venv/*" "logs/*" "*.pyc" ".env"
curl -X POST "https://ars-v2retail-api.scm.azurewebsites.net/api/zipdeploy?isAsync=true" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/zip" --data-binary @/tmp/ars-deploy.zip

# 3. Restart
curl -X POST "https://management.azure.com/subscriptions/7c2e7784-61b3-4aa7-9967-f41b381406dd/resourceGroups/rg-ars-prod/providers/Microsoft.Web/sites/ars-v2retail-api/restart?api-version=2022-09-01" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Length: 0"

# 4. Wait 120s, then test
sleep 120
curl -s "https://ars-v2retail-api.azurewebsites.net/health"
```

## HOW TO LOGIN TO THE API
```bash
APP_TOKEN=$(curl -s -X POST "https://ars-v2retail-api.azurewebsites.net/api/v1/auth/login" \
  -H "Content-Type: application/json" -d '{"username":"superadmin","password":"Admin@12345"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
```

## HOW TO TEST SNOWFLAKE (from any machine with Python)
```python
import snowflake.connector
conn = snowflake.connector.connect(
    account='iafphkw-hh80816', user='akashv2kart', password='[SF_PASS_IN_ENV]',
    database='V2_ALLOCATION', schema='RESULTS', warehouse='ALLOC_WH',
)
# 246M scored pairs, 4.97M store stock rows available
```

## CREDENTIALS
- **Azure API:** https://ars-v2retail-api.azurewebsites.net | superadmin / Admin@12345
- **Azure SQL:** ars-v2retail-sql.database.windows.net | arsadmin / [SQL_PASS_IN_ENV]
- **Azure Tenant:** 3eb968d0-bf19-40f9-b191-f3186ac38f02
- **Azure Client:** 8f54a771-3b04-4458-bef3-f1fa98dc38a0 | Secret: [AZURE_SECRET_IN_ENV]
- **Snowflake:** iafphkw-hh80816 | akashv2kart / [SF_PASS_IN_ENV]
- **Supabase:** https://pymdqnnwwxrgeolvgvgv.supabase.co
  - Service Role: [SUPABASE_SERVICE_KEY_IN_ENV]
- **Dashboard:** replen.v2retail.net (Cloudflare Worker)

## ARCHITECTURE (Unified — what we're building)
```
Supabase (budget) → Snowflake (246M scores + 5M store stock) → Azure ARS (fill → size → DO)
                                                                      ↓
                                                              replen.v2retail.net
```
- **Snowflake owns:** Engines 1+2 (budget cascade + article scoring)
- **Azure ARS owns:** Engines 3+4+5 (L-ART waterfall filler + size allocator + delivery orders)
- **DO NOT rebuild scoring on Azure** — read Snowflake's 246M pre-computed scores

## WHAT NEEDS TO BE DONE (priority order)

### 1. CRITICAL: Make snowflake-connector-python work on Azure
File: `backend/requirements.txt` — add `snowflake-connector-python==3.12.3`
Problem: Azure App Service B2 crashes on cold start when this package is installed (SQL login timeout).
Options:
  a) Increase Azure startup timeout + connection pool settings
  b) Lazy-import snowflake connector (don't import at startup)
  c) Use Azure Functions as a Snowflake proxy
  d) Cache Snowflake data in Azure SQL on a schedule

### 2. CRITICAL: Wire Snowflake scores into the engine
File: `backend/app/services/allocation/engine.py`
File: `backend/app/services/allocation/snowflake_loader.py` (already created)
The engine currently falls back to a duplicate local scorer. Make it ONLY use Snowflake.
Key query: `SELECT ... FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES WHERE MAJCAT = 'M_JEANS' QUALIFY ROW_NUMBER() OVER (PARTITION BY ST_CD ORDER BY TOTAL_SCORE DESC) <= 200`
Returns: 91K rows in 8s

### 3. CRITICAL: Wire Snowflake store stock for L-ART waterfall
File: `backend/app/services/allocation/option_filler.py` (waterfall code already exists)
Query: `SELECT STORE_CODE, GENCOLOR_KEY, STK_QTY FROM V2RETAIL.GOLD.FACT_STOCK_GENCOLOR WHERE GENCOLOR_KEY IN (...scored articles...) AND STK_QTY > 0`
Returns: 35K rows in 4s
This makes fill rate jump from 23% to ~100%

### 4. Remove duplicate code
Delete: `backend/app/services/allocation/article_scorer.py` (duplicate of Snowflake)
Delete: `backend/app/services/allocation/budget_cascade.py` (duplicate of Snowflake)
Clean: Remove all local scoring paths from `engine.py`

### 5. Run all 242 MAJCATs
Add endpoint: `POST /api/v1/allocation-engine/run-all`
Loop through Snowflake MAJCATs, run Engine 3→4→5 for each
Target: < 1 hour for all 242 categories

### 6. Daily automation
Cloudflare Worker cron at replen.v2retail.net:
  21:00 UTC → Snowflake COMPUTE_SCORES → Azure run-all → results back to Snowflake

## KEY DATA (verified, in Snowflake right now)
- ARTICLE_SCORES: 246,203,071 rows (242 MAJCATs × 455 stores × 75K articles)
- FACT_STOCK_GENCOLOR: 4,969,008 rows (326 stores × 100K articles) — L-ART data
- ALLOC_BUDGET_CASCADE: 14,999 rows (stores × MAJCATs with MBQ)
- MSA_ARTICLES: 560,367 rows (DC articles)
- Score range: 20-120 (no HERO/FOCUS data yet)

## THE ALLOCATION WATERFALL (the core algorithm)
For each store × MAJCAT:
1. Get MBQ (e.g., 55 option slots for store HA10, M_JEANS)
2. Phase 1 L-ART: Articles already in store fill slots first (from FACT_STOCK_GENCOLOR)
3. Phase 2 Continuation: L-ART with DC stock get replenishment
4. Phase 3 MIX: New DC articles fill remaining empty slots (from ARTICLE_SCORES)
5. Fill Rate = (L-ART + MIX) / MBQ (should be ~100%)
6. Size allocation: Break each option into sizes using Supabase size contribution %

## FILE STRUCTURE
```
backend/
├── app/services/allocation/
│   ├── engine.py              # Main orchestrator (1005 lines)
│   ├── snowflake_loader.py    # Reads Snowflake scores + store stock
│   ├── option_filler.py       # L-ART → MIX waterfall (397 lines)
│   ├── size_allocator.py      # Size distribution
│   ├── article_scorer.py      # ❌ DELETE (duplicate of Snowflake)
│   └── budget_cascade.py      # ❌ DELETE (duplicate of Snowflake)
├── app/api/v1/endpoints/
│   └── allocation_engine.py   # API endpoints (738 lines)
├── static/allocation.html     # Dashboard UI
└── requirements.txt           # NEEDS snowflake-connector-python
```


## CURRENT BUILD STATUS (as of April 8, 2026)

### ✅ DONE
- snowflake_loader.py: Direct Snowflake connection with LAZY IMPORT (line 32: `import snowflake.connector` inside `_get_connection()`, NOT at module level — this prevents Azure cold-start crash)
- option_filler.py: L-ART waterfall implemented as GlobalGreedyFiller (392 lines). Has Phase 1 (L-ART, status='L'/'L_ONLY'), Phase 2 (Continuation), Phase 3 (MIX). Status labels: L, L_ONLY, ST_SPEC, HERO, FOCUS, MIX
- allocation_engine.py: 8 API endpoints, all read from Snowflake directly (no Azure SQL dependency for scoring)
- M_JEANS test PASSED: 91K scored pairs, 35K store stock, 428 stores, 10,912 assignments (80.8% L-ART + 19.2% MIX), 100% fill rate
- Budget cascade dedup: 6x duplicates removed, synthetic seg column added
- Size contribution from Supabase: sz32=26%, sz34=24%, sz30=21%, sz36=12%, sz28=11%
- Pushed to akash0631/ars (commit bbe41ea)
- MCP server updated with full allocation engine context (70 lines)

### ⚠️ IMPORTANT: DO NOT USE THE OLD SYSTEM
- article_scorer.py is a DUPLICATE of Snowflake scoring — DO NOT use it, DELETE it
- budget_cascade.py is a DUPLICATE of Snowflake ALLOC_BUDGET_CASCADE — DO NOT use it, DELETE it
- engine.py still has old Azure SQL fallback paths — the API (allocation_engine.py) bypasses these entirely
- The CORRECT flow is: allocation_engine.py → snowflake_loader.py → option_filler.py → size_allocator.py
- NEVER re-score articles locally — Snowflake has 246M pre-computed scores

### ⬜ PENDING
- Azure deployment verification (zip deploy accepted, needs restart and health check)
- Test FW_M_SLIPPER, L_KURTI_HS, M_TEES_HS (same pipeline, different MAJCATs)
- Run all 242 MAJCATs endpoint (POST /allocation-engine/run-all)
- Daily cron: Snowflake rescore → Azure allocate → results back to Snowflake
- Populate DC_ARTICLE_PRIORITY (HERO/FOCUS article lists from V2 planning team)
- Populate STORE_SPECIFIC_LISTING (store-specific mandates)
- Replace RNG_SEG MRP-quartile approximation with real SAP mapping from product_master (range_segment column, 3M rows on Supabase)

### SUPABASE ARTICLE MASTER (discovered April 8)
- product_master: 3M rows with range_segment (E/P/V/SP), division, vendor, article_status
- variant_article_product_master: 1M rows with 87 columns including rng_seg, fabric, macro_mvgr
- All variants of a generic article share attributes (rng_seg, div, fabric) — only color+size differ
- M_JEANS has 22,508 variants with range segments E, P, SP, V

### Universal MCP Server (context for any Claude session)
- URL: https://universal-mcp.akash-bab.workers.dev
- Key: ArsV2Mcp@22cab54c1bee24fa6893906c
- Has 38 tools covering all V2 Retail systems + full allocation engine context
- Any Claude with MCP access gets: credentials, architecture, Snowflake data inventory, L-ART waterfall, test results

## FULL HANDOVER DOCUMENT
See: V2_RETAIL_HANDOVER.md in this repo (465 lines, complete data inventory, all credentials, 7 agent tasks)


---

## V2 RETAIL — ABAP AI STUDIO & HHT PLATFORM

### ABAP AI Studio
- **URL:** https://abap.v2retail.net (Cloudflare Worker: abap-ai-studio)
- **Login:** akash/admin2026 (admin), bhavesh/developer
- **GitHub:** akash0631/abap-ai-studio (main=prod, dev=dev)
- **Features:** AI Chat, 8-Stage Agent Pipeline, Smart Debug, RFC Developer, HHT Studio, Code Search
- **D1 Database:** 43487dc8-c72c-42fc-a901-efafab7b5dd9

### SAP RFC Proxy
- **URL:** https://sap-api.v2retail.net/api/rfc/proxy
- **Header:** X-RFC-Key: v2-rfc-proxy-2026
- **Env routing:** ?env=prod (PROD SAP), ?env=qa (QA SAP), default=DEV
- **SAP Systems:** DEV=192.168.144.174/210, PROD=192.168.144.170/600, QA=192.168.144.179/600

### HHT Android App
- **APK:** v12.103 at apk.v2retail.net/download (R2 v2retail bucket)
- **GitHub:** akash0631/v2-android-hht (main branch)
- **Server dropdown:** V2 Cloud (PROD) | Dev Cloud (hht-api.v2retail.net/dev) | QA Cloud (/qa)
- **CRITICAL:** Old Tomcat URLs (192.168.151.40:16080/xmwgw) incompatible with v12 JSON format. ALWAYS use cloud proxy URLs.
- **Azure Middleware:** v2-hht-api.azurewebsites.net/api/hht (PROD only)

### Pipeline Rules (from incidents)
1. ALWAYS read PROD source FIRST via RPY_PROGRAM_READ before generating code
2. ALWAYS test FM after deploying (call with blank params, check SYNTAX_ERROR)
3. If SYNTAX_ERROR detected, auto-restore PROD code immediately
4. V2 naming: IM_ (import), EX_ (export). NEVER IV_/EV_
5. NEVER rewrite more than 50% of an FM. Optimize FROM existing code
6. NEVER remove global variables (GT_*, GS_*)
7. NEVER change error message text
8. FM name != FG name. Check TFDIR.PNAME for include name

### Verified SAP Z-Tables
ZWM_USR02 (user-plant), ZWM_DC_MASTER (DC config), ZWM_CRATE (crate-bin), ZWM_DCSTK1/2/3 (stock take), ZWM_GRT_PUTWAY (GRT putaway), ZSDC_FLRMSTR (floor master), ZSDC_ART_STATUS (article status), ZDISC_ARTL (discount articles)

### Incident History
1. AI invented IV_CRATE_NUMBER param and ZWM_CRATES table -> SYNTAX_ERROR dump
2. AI rewrote ZSDC_DIRECT_ART_VAL_BARCOD_RFC 148/167 lines, removed GT_DATA2 -> SYNTAX_ERROR (x2)
3. HHT IM_STOCK_TAKE_ID copy-paste bug sent USER instead of stock_take_id
4. v12 JSON to old Tomcat middleware -> parse error -> fixed with cloud proxy

### All GitHub Repos (akash0631)
| Repo | Purpose |
|------|---------|
| abap-ai-studio | ABAP AI Development Studio (CF Worker) |
| rfc-api | IIS .NET RFC API (148 controllers) |
| v2-android-hht | HHT Android App (Zebra devices) |
| v2-hht-middleware | Azure HHT Middleware (NCo tunnel to SAP) |
| ars | Auto Replenishment System |

### Build Rules (CRITICAL)
1. NEVER use regex for HTML_B64 replacement in ABAP Studio build — use string find/replace
2. Frontend SYS constant MUST use backticks. After SYS must come "const TEMPLATES=["
3. CF Worker deploy filename must be index.js
4. HHT APK uploads to R2 v2retail bucket (not nubo, not eatnubo)
5. SAP FM name != FG name — ALWAYS check TFDIR.PNAME



## LOVABLE APPS (React + TypeScript + Supabase)
All repos under akash0631, built on Lovable platform:

| App | Repo | Purpose | Lovable URL |
|-----|------|---------|-------------|
| HubWise | hubwise-route-orchestrator | DC route planner (DCPlanner, Allocation, Maps) | lovable.dev/projects/d5f2c890 |
| ReachAttest | reach-attest | HR/attendance mgmt (HRAdmin, Masters, bulk ops) | lovable.dev/projects/d8a4ed9b |
| ApexKarma | apex-karma | Project/task mgmt (Projects, Teams, Reports) | lovable.dev/projects/247fd162 |
| V2RetailOps | v2retailoperation | Store ops (Dashboard, Karma Checklist, Gondola) | — |
| Nubo Pulse OS | nubo-os-your-store-s-brain | Restaurant brain (Bills, Inventory, AI, Feedback) | — |

## N8N WORKFLOW AUTOMATION
- **Instance:** n8n.happyocean-047968bd.centralindia.azurecontainerapps.io (Azure Container App)
- **Proxy:** nubo-n8n-proxy CF Worker → Cloudflare Tunnel → n8n
- **Used for:** Nubo automation (ads scheduling, social posting, operations alerts)
- **Related workers:** nubo-ads-bot, nubo-ads-cron, nubo-ads-snapshot, nubo-ga-bot, nubo-ig-fetch, nubo-social

### Universal MCP Server
- **URL:** https://universal-mcp.akash-bab.workers.dev
- **Key:** ArsV2Mcp@22cab54c1bee24fa6893906c
- **Tools:** 36 total covering ARS, HHT, SQL, RFC, ABAP, Azure, Cloudflare, Nubo, GitHub
- **ABAP tools:** abap_read_source, abap_read_interface, abap_test_fm, abap_studio_status
```json
Claude Desktop: %APPDATA%\Claude\claude_desktop_config.json
{
  "mcpServers": {
    "v2": {
      "url": "https://universal-mcp.akash-bab.workers.dev",
      "headers": { "X-API-Key": "ArsV2Mcp@22cab54c1bee24fa6893906c" }
    }
  }
}

Claude Code:
claude mcp add v2 --transport http --header "X-API-Key: ArsV2Mcp@22cab54c1bee24fa6893906c" https://universal-mcp.akash-bab.workers.dev
```

### Developer Claude.ai Setup
1. Settings -> Enable "Code Execution and File Creation"
2. Settings -> Integrations -> Connect Cloudflare Developer Platform
3. Projects -> Create "V2 Retail" -> Upload V2_COMPLETE_HANDOVER.md + DEVELOPER_CLAUDE_SETUP.md
4. Set Project Instructions with API endpoints + rules (see DEVELOPER_CLAUDE_SETUP.md)
