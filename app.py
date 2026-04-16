"""
Hunting Machine — 4-Stage Account Intelligence Pipeline
Claude Sonnet 4.6 (scoring/intelligence) + Claude Haiku 4.5 (pre-filter/name resolution) + Apollo.io
"""

import streamlit as st
import pandas as pd
import json
import io
import time
import requests
import anthropic
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── Configuration ─────────────────────────────────────────────────────────────
CLAUDE_MODEL        = "claude-sonnet-4-6"
CLAUDE_HAIKU_MODEL  = "claude-haiku-4-5-20251001"
BATCH_SIZE          = 10
APOLLO_MIN_SCORE    = 28
APOLLO_DELAY        = 0.6
S3B_CREDIT_CAP      = 50
CACHE_TTL_DAYS      = 7
CACHE_FILE          = os.path.join(os.path.dirname(__file__), "stage1_cache.json")
CACHE_FILE_S2       = os.path.join(os.path.dirname(__file__), "stage2_cache.json")
CACHE_FILE_S3A      = os.path.join(os.path.dirname(__file__), "stage3a_cache.json")
CACHE_FILE_PF       = os.path.join(os.path.dirname(__file__), "prefilter_cache.json")

SALESFORCE_KW   = {"salesforce","sales cloud","service cloud","marketing cloud",
                   "commerce cloud","data cloud","pardot","tableau","mulesoft",
                   "heroku","slack","exacttarget"}
COMPETITOR_KW   = {"dynamics","microsoft dynamics","hubspot","sap crm","zoho","freshsales"}
CRM_HIRING_KW   = {"salesforce","crm","sales cloud","service cloud",
                   "marketing cloud","data cloud","agentforce"}
TIER_ORDER      = {"A Strategic":0,"B Prime":1,"C Monitor":2,"Low Priority":3,"Remove":4}

FALLBACK_HOT_TITLES  = ["CEO", "CFO", "CIO", "CMO", "CDO", "COO", "CTO",
                         "Director General", "Directora General", "Group CEO",
                         "Chief Executive", "Chief Financial", "Chief Information",
                         "Chief Digital", "Chief Data", "Chief Marketing",
                         "Chief Commercial", "Chief Operating", "Chief Technology",
                         "Co-Founder", "Founder", "Owner", "Partner"]
FALLBACK_WARM_TITLES = ["Director", "Directora", "VP", "Vice President",
                         "Head of", "Responsable de", "Digital Director",
                         "Sales Director", "Marketing Director", "IT Director",
                         "eCommerce Director", "CRM Director", "Operations Director",
                         "Logistics Director", "Financial Director", "Engineering Manager",
                         "Product Manager", "Managing Director"]

# ── Anthropic client singleton (avoids re-instantiating the HTTP pool per call) ─
_anthropic_clients: dict = {}

def _get_client(key: str) -> anthropic.Anthropic:
    if key not in _anthropic_clients:
        _anthropic_clients[key] = anthropic.Anthropic(api_key=key)
    return _anthropic_clients[key]


# ── Stage 1 cache ─────────────────────────────────────────────────────────────
def _cache_key(company: str) -> str:
    return company.strip().lower()

def load_stage1_cache() -> dict:
    """Load cache file, drop expired entries, return valid entries keyed by company."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS)
        valid = {}
        for k, v in raw.items():
            try:
                cached_at = datetime.fromisoformat(v["cached_at"])
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=timezone.utc)
                if cached_at >= cutoff:
                    valid[k] = v
            except Exception:
                pass
        return valid
    except Exception:
        return {}

def save_stage1_cache(cache: dict):
    """Write cache dict to disk."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def cache_days_remaining(entry: dict) -> int:
    try:
        cached_at = datetime.fromisoformat(entry["cached_at"])
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        expires = cached_at + timedelta(days=CACHE_TTL_DAYS)
        remaining = (expires - datetime.now(timezone.utc)).days
        return max(remaining, 0)
    except Exception:
        return 0

# ── Stage 2 cache ─────────────────────────────────────────────────────────────
def load_stage2_cache() -> dict:
    if not os.path.exists(CACHE_FILE_S2):
        return {}
    try:
        with open(CACHE_FILE_S2, "r", encoding="utf-8") as f:
            raw = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS)
        valid = {}
        for k, v in raw.items():
            try:
                cached_at = datetime.fromisoformat(v["cached_at"])
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=timezone.utc)
                if cached_at >= cutoff:
                    valid[k] = v
            except Exception:
                pass
        return valid
    except Exception:
        return {}

def save_stage2_cache(cache: dict):
    try:
        with open(CACHE_FILE_S2, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ── Stage 3a cache ─────────────────────────────────────────────────────────────
def load_stage3a_cache() -> dict:
    if not os.path.exists(CACHE_FILE_S3A):
        return {}
    try:
        with open(CACHE_FILE_S3A, "r", encoding="utf-8") as f:
            raw = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS)
        valid = {}
        for k, v in raw.items():
            try:
                cached_at = datetime.fromisoformat(v["cached_at"])
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=timezone.utc)
                if cached_at >= cutoff:
                    valid[k] = v
            except Exception:
                pass
        return valid
    except Exception:
        return {}

def save_stage3a_cache(cache: dict):
    try:
        with open(CACHE_FILE_S3A, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ── Pre-filter cache ──────────────────────────────────────────────────────────
def load_prefilter_cache() -> dict:
    if not os.path.exists(CACHE_FILE_PF):
        return {}
    try:
        with open(CACHE_FILE_PF, "r", encoding="utf-8") as f:
            raw = json.load(f)
        cutoff = datetime.now(timezone.utc) - timedelta(days=CACHE_TTL_DAYS)
        valid = {}
        for k, v in raw.items():
            try:
                cached_at = datetime.fromisoformat(v["cached_at"])
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=timezone.utc)
                if cached_at >= cutoff:
                    valid[k] = v
            except Exception:
                pass
        return valid
    except Exception:
        return {}

def save_prefilter_cache(cache: dict):
    try:
        with open(CACHE_FILE_PF, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Hunting Machine", page_icon="🎯", layout="wide")
st.title("🎯 Hunting Machine")
st.markdown("4-stage account intelligence · **Claude Sonnet 4.6** (scoring) + **Claude Haiku 4.5** (pre-filter/names) + **Apollo.io**")

# ── Sidebar ───────────────────────────────────────────────────────────────────
anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
apollo_key    = os.getenv("APOLLO_API_KEY", "")

st.sidebar.title("⚙️ Configuration")
ae_name        = st.sidebar.text_input("AE Name (source tag)", placeholder="e.g. Alvaro")
target_country = st.sidebar.text_input("🌍 Country/region priority", placeholder="e.g. Spain", help="Filters Apollo company search to this country — use when looking for a local entity (e.g. Axactor Spain vs Axactor Group)")
single_account = st.sidebar.text_input("🎯 Run only for this account", placeholder="e.g. Axactor Spain", help="Runs the full pipeline for this single account only, bypassing the uploaded list")
st.sidebar.markdown("---")
st.sidebar.markdown("""
**Pipeline**
1. 📂 Upload lists
2. 🔍 Pre-filter (binary qualification)
3. 🧠 Claude first tiering
4. ✏️ Manual review gate
5. 🔍 Apollo enrichment (score ≥ 28)
6. 👥 Lead intelligence
7. ✉️ Outreach generation
8. 📥 Download
""")

# ── Session state ─────────────────────────────────────────────────────────────
for k, default in [
    ("stage",                  0),
    ("prefilter_done",         False),
    ("prefilter_results",      []),
    ("company_sources",        {}),
    ("stage1_results",         []),
    ("stage2_results",         []),
    ("stage3a_results",        {}),
    ("stage3_results",         []),
    ("stage4_results",         []),
    ("s2_run_credits",         0),
    ("s3_run_credits",         0),
    ("single_account_last_run",""),
]:
    if k not in st.session_state:
        st.session_state[k] = default


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS — File loading
# ════════════════════════════════════════════════════════════════════════════════
def load_companies(f, source):
    companies = []
    name = f.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(f)
        col = df.columns[0]
        for k1, k2 in [("company","name"), ("account","name")]:
            m = next((c for c in df.columns if k1 in c.lower() and k2 in c.lower()), None)
            if m:
                col = m
                break
        companies = df[col].dropna().tolist()
    elif name.endswith(".json"):
        data = json.load(f)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    companies.append(item)
                elif isinstance(item, dict):
                    for key in ["company_name","Company","Account Name","name"]:
                        if key in item and item[key]:
                            companies.append(item[key])
                            break
        elif isinstance(data, dict):
            for key in ["accounts","companies","data"]:
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        c = item.get("company_name") or item.get("Company") or item.get("name","")
                        if c:
                            companies.append(c)
                    break
    return [(c.strip(), source) for c in companies if isinstance(c, str) and c.strip()]


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS — Apollo
# ════════════════════════════════════════════════════════════════════════════════
def apollo_post(endpoint, payload, key):
    url = f"https://api.apollo.io/v1/{endpoint}"
    r = requests.post(url, json=payload,
                      headers={"Content-Type":"application/json","X-Api-Key":key},
                      timeout=20)
    r.raise_for_status()
    time.sleep(APOLLO_DELAY)
    return r.json()

def claude_resolve_names(company_list, key):
    """Use Claude's semantic knowledge to resolve company names to their most likely Apollo-indexed form."""
    block = "\n".join(f"- {c}" for c in company_list)
    prompt = f"COMPANIES:\n{block}"
    try:
        return call_claude(prompt, _RESOLVE_NAMES_SYSTEM, key,
                           max_tokens=1024, cache_system=True)
    except Exception:
        return [{"company": c, "canonical_name": c, "domain": "", "alt_names": []}
                for c in company_list]


def enrich_org(resolved, key, country=""):
    """Try original name first, then Claude's canonical, then alt names — stop at first Apollo hit.
    On hit: fetch full org intelligence via organizations/enrich using Apollo's own primary_domain.
    If country is provided, filters the company search to that geography."""
    original  = resolved.get("company", "")
    canonical = resolved.get("canonical_name") or original
    alt_names = resolved.get("alt_names") or []

    candidates = []
    if original:
        candidates.append(original)
    if canonical and canonical not in candidates:
        candidates.append(canonical)
    for alt in alt_names:
        if alt and alt not in candidates:
            candidates.append(alt)

    for name in candidates:
        try:
            payload = {"q_organization_name": name, "page": 1, "per_page": 1}
            if country:
                payload["organization_locations"] = [country]
            data = apollo_post("mixed_companies/search", payload, key)
            orgs = data.get("accounts", []) or data.get("organizations", [])
            if not orgs:
                continue
            account = orgs[0]
            account["_resolved_name"] = name

            # Domain cross-validation — reject wrong entity before spending enrichment credit
            apollo_domain = account.get("primary_domain") or account.get("domain", "")
            claude_domain = resolved.get("domain", "")
            if claude_domain and apollo_domain:
                claude_root = claude_domain.split(".")[0].lower()
                apollo_root = apollo_domain.split(".")[0].lower()
                if claude_root not in apollo_root and apollo_root not in claude_root:
                    continue  # domain mismatch — wrong entity, try next candidate

            # Fetch full intelligence record using Apollo's own domain
            domain = apollo_domain
            if domain:
                try:
                    enrich_data = apollo_post("organizations/enrich", {"domain": domain}, key)
                    org_intel   = enrich_data.get("organization") or {}
                    # Merge intelligence fields into account object
                    for field in ["technology_names", "funding_events", "job_postings",
                                  "estimated_num_employees", "total_funding",
                                  "short_description", "keywords"]:
                        if org_intel.get(field):
                            account[field] = org_intel[field]
                    account["_enrich_credits"] = 1
                except Exception:
                    account["_enrich_credits"] = 0
            else:
                account["_enrich_credits"] = 0

            return account
        except Exception:
            continue
    return None

SENIORITY_BY_PRIORITY = {
    "Hot":  ["c_suite", "vp"],
    "Warm": ["director", "manager"],
    "Cold": ["manager", "senior"],
}

def unlock_person(person_id, key):
    """Reveal name/email for a locked Apollo contact. Returns enriched person dict or None."""
    try:
        data = apollo_post("people/match", {"id": person_id, "reveal_personal_emails": False}, key)
        return data.get("person")
    except Exception:
        return None

def _email_domain_matches(email, company_domain):
    """Return True if email domain matches company domain, or if either is unknown."""
    if not email or not company_domain:
        return True
    email_domain = email.split("@")[-1].lower().strip()
    return email_domain == company_domain.lower().strip()

def search_people(company_name, domain, titles, key, max_results=2, priority="Warm"):
    contacts, seen = [], set()

    def _run_search(payload):
        """Try q_organization_name first, fall back to domain."""
        payload_name = {**payload, "q_organization_name": company_name}
        try:
            data = apollo_post("mixed_people/api_search", payload_name, key)
            results = data.get("people", [])
            if results:
                return results
        except Exception:
            pass
        if domain:
            payload_domain = {**payload, "organization_domains": [domain]}
            try:
                data = apollo_post("mixed_people/api_search", payload_domain, key)
                return data.get("people", [])
            except Exception:
                pass
        return []

    # Pass 1 — title-based search, only candidates with a known email (FIX 1)
    for p in _run_search({"page":1, "per_page": max_results + 4, "person_titles": titles[:10]}):
        if p.get("id") not in seen and p.get("has_email"):
            contacts.append(p)
            seen.add(p["id"])

    # Pass 2 — seniority fallback, same has_email filter (FIX 1)
    if not contacts:
        seniority = SENIORITY_BY_PRIORITY.get(priority, ["director"])
        for p in _run_search({"page":1, "per_page": max_results + 4, "person_seniority": seniority}):
            if p.get("id") not in seen and p.get("has_email"):
                contacts.append(p)
                seen.add(p["id"])

    # Unlock + validate
    verified, seen_names = [], set()
    unlock_credits = 0
    for p in contacts:
        if len(verified) >= max_results:
            break
        unlocked = unlock_person(p.get("id"), key)
        unlock_credits += 1
        if not unlocked:
            continue
        name = unlocked.get("name") or ""
        if not name or name in seen_names:
            continue

        # FIX 2: discard stale ex-employees
        emp_history = unlocked.get("employment_history") or [{}]
        if emp_history and emp_history[0].get("current") is False:
            continue

        email = unlocked.get("email") or ""
        if domain and email and not _email_domain_matches(email, domain):
            continue  # domain known, email present, domain mismatch → discard
        if not domain or not email:
            # Domain unknown OR no email — fall back to employer name check
            current_employer = emp_history[0].get("organization_name", "")
            if current_employer and company_name.lower() not in current_employer.lower() \
               and current_employer.lower() not in company_name.lower():
                continue

        # FIX 4: discard company-page LinkedIn URLs (slug matches company name)
        linkedin = unlocked.get("linkedin_url") or ""
        if linkedin:
            slug = linkedin.rstrip("/").split("/")[-1].lower()
            company_slug = company_name.lower().replace(" ", "-")
            if slug == company_slug:
                continue

        unlocked["_original"] = p
        verified.append(unlocked)
        seen_names.add(name)

    return verified, unlock_credits

def score_apollo(stage1_score, apollo_org):
    if not apollo_org:
        return {"bonus":0, "final_score":stage1_score, "signals":{},
                "account_type":"Unknown", "technologies":[],
                "employees":None, "funding_total":None, "funding_date":None,
                "domain":None, "linkedin":None, "crm_job_titles":[]}

    bonus, signals = 0, {}
    raw_tech  = apollo_org.get("technology_names") or []
    tech_low  = {t.lower() for t in raw_tech}

    sf = bool(tech_low & SALESFORCE_KW)
    comp_crm = bool(tech_low & COMPETITOR_KW)

    if sf:
        bonus += 8
        signals["salesforce_in_stack"] = True
        account_type = "Existing Business"
    elif comp_crm:
        bonus += 3
        signals["competitor_crm"] = True
        account_type = "Green Field — Displacement"
    else:
        signals["salesforce_in_stack"] = False
        account_type = "Green Field — Transformation"

    # Funding
    events = apollo_org.get("funding_events") or []
    latest_date, total_funding = None, None
    signals["recent_funding"] = False
    if events:
        dated = sorted([e for e in events if e.get("date")],
                       key=lambda e: e["date"], reverse=True)
        if dated:
            latest_date = dated[0]["date"]
            try:
                dt = datetime.fromisoformat(latest_date.replace("Z","+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc)-dt).days/30.44 <= 18:
                    bonus += 5
                    signals["recent_funding"] = latest_date
            except Exception:
                pass
        raw_total = apollo_org.get("total_funding")
        total_funding = int(raw_total) if raw_total else None

    # CRM hiring — from real job postings
    job_postings = apollo_org.get("job_postings") or []
    crm_jobs = [j.get("title","") for j in job_postings
                if any(k in j.get("title","").lower() for k in CRM_HIRING_KW)]
    if crm_jobs:
        bonus += 3
    signals["crm_hiring"]    = bool(crm_jobs)
    signals["crm_job_titles"] = crm_jobs

    # Headcount
    emp = apollo_org.get("estimated_num_employees")
    if emp and emp > 0:
        bonus += 2
        signals["headcount_confirmed"] = True

    # Domain
    domain = (apollo_org.get("primary_domain")
              or (apollo_org.get("website_url") or "").replace("http://","")
                 .replace("https://","").split("/")[0])

    return {
        "bonus": bonus,
        "final_score": stage1_score + bonus,
        "signals": signals,
        "account_type": account_type,
        "technologies": raw_tech[:10],
        "employees": emp,
        "funding_total": total_funding,
        "funding_date": latest_date,
        "domain": domain,
        "linkedin": apollo_org.get("linkedin_url"),
        "crm_job_titles": crm_jobs,
    }

def score_to_tier(score):
    if score >= 35: return "A Strategic"
    if score >= 25: return "B Prime"
    if score >= 15: return "C Monitor"
    return "Low Priority"

def claude_prefilter(company_list, key):
    block = "\n".join(f"- {c}" for c in company_list)
    prompt = f"COMPANIES:\n{block}"
    try:
        return call_claude(prompt, _PREFILTER_SYSTEM, key,
                           max_tokens=1024, cache_system=True, model=CLAUDE_HAIKU_MODEL)
    except Exception as e:
        return [{"company": c, "decision": "keep", "reason": "", "detail": str(e)}
                for c in company_list]


def is_new_hire(person, months=6):
    """Return (bool, date_str) — True if current role started within `months` months."""
    history = person.get("employment_history") or []
    current = next((e for e in history if e.get("current") or not e.get("end_date")), None)
    if not current:
        return False, None
    start = current.get("start_date", "")
    if not start:
        return False, None
    try:
        parts = start.split("-")
        dt = datetime(int(parts[0]), int(parts[1]) if len(parts) > 1 else 1,
                      int(parts[2]) if len(parts) > 2 else 1, tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - dt).days / 30.44 <= months:
            return True, start
    except Exception:
        pass
    return False, None


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS — Claude
# ════════════════════════════════════════════════════════════════════════════════

# Static system prompts — defined once at module level so cache_control is effective
# across all calls within the same session (Anthropic caches for 5 min of activity).

_PREFILTER_SYSTEM = """You only respond with structured JSON arrays. Never wrap in markdown code blocks.
You are screening companies for a Salesforce enterprise sales team targeting Iberian (Spain and Portugal) accounts.

DISCARD if ANY of the following apply:
- Fewer than 600 employees (estimated)
- No decision-making presence in Spain or Portugal
- Industry with no CRM/SaaS transformation potential (e.g. public administration, micro-retail, NGO, agriculture)
- Company name unrecognisable or clearly not an enterprise

KEEP all others.

Return ONLY a JSON array, one object per company in the same order:
[{"company":"","decision":"keep","reason":"","detail":""}]

For kept companies: reason and detail are empty strings.
For discarded companies: reason = exact discard criteria phrase above, detail = brief factual context (one sentence)."""

_RESOLVE_NAMES_SYSTEM = """You only respond with structured JSON arrays. Never wrap in markdown code blocks.
You are helping match company names to their Apollo.io database entries.

For each company return:
- canonical_name: the most likely name Apollo.io uses to index this company. This may be identical to the original name, or a known variant (e.g. without legal suffix, parent company name, common trade name). Do NOT translate to English — use whatever name Apollo most likely uses.
- domain: primary web domain if you know it (e.g. lupa.es, mango.com) — empty string if unsure
- alt_names: up to 2 additional name variants to try as fallbacks (e.g. with/without legal suffix, abbreviated name, parent brand)

Examples of correct resolution:
- "Lupa Supermercados" → canonical: "Lupa Supermercados", domain: "lupa.es", alt_names: ["Lupa"]
- "Grupo Mahou San Miguel" → canonical: "Mahou San Miguel", domain: "mahou.es", alt_names: ["Mahou-San Miguel", "Grupo Mahou"]
- "El Corte Inglés" → canonical: "El Corte Ingles", domain: "elcorteingles.es", alt_names: ["El Corte Inglés"]
- "Clínica Baviera" → canonical: "Clinica Baviera", domain: "clinicabaviera.com", alt_names: ["Baviera"]

Return ONLY a JSON array, one object per company in the same order:
[{"company":"","canonical_name":"","domain":"","alt_names":[]}]"""

_STAGE1_SYSTEM = """You only respond with structured JSON arrays. Never wrap in markdown code blocks.

SCORING RULES (Score 1-50):
1. Size: 600-1500 (+8), 1500-5000 (+12), 5000+ (+15)
2. Industry: Consumer Goods (+15), Retail/Omnichannel (+15), Financial Services (+10),
   Telco (+10), Pharma (+8), Manufacturing (+8), Other (+5)
3. Spain layer: HQ in Spain (+12), Spain main revenue (+8), Spanish exec committee (+8), Intl HQ (+0)
4. CRM signals: Salesforce confirmed (+8), Dynamics/SAP CRM (+6), CRM hiring (+8), Commerce Cloud (+6)

TRIGGER EVENTS (note if detected — do NOT add extra points):
- New CIO/CDO/CCO hired recently
- Recent funding round
- Active CRM/digital job postings
- Competitor CRM confirmed

TIERING: 35+ = A Strategic | 25-34 = B Prime | 15-24 = C Monitor | <15 = Low Priority

Return ONLY a JSON array, one object per company in the same order:
[{"company":"","score":0,"tier":"","industry":"","account_type_hint":"Green Field or Existing Business","trigger_events":[],"narrative":""}]"""

_STAGE3A_SYSTEM = """You only respond with structured JSON arrays. Never wrap in markdown code blocks.

BUYING COMMITTEE RULES:
- Hot Lead: C-Level with P&L or transformation ownership, or confirmed CRM decision maker
- Warm Lead: Director/Manager owning CRM, Digital, Sales Ops, or Ecommerce
- Cold Lead: Manager/analyst level — intel gathering only

OUTREACH ANGLES by account type:
- Green Field Transformation: Customer 360 unification, replace spreadsheets/point solutions
- Green Field Displacement: competitor weaknesses, migration path, better ROI
- Existing Business: expansion (more clouds, Agentforce, Data Cloud, AI)

SEARCH TITLES RULES:
- Provide 8-10 title variants per persona covering both English and Spanish versions
- Include abbreviations (CIO, CDO, CTO), full titles, and common Iberian variants
- Example for CIO persona: ["CIO", "Chief Information Officer", "Director de Sistemas", "Director de Tecnologia", "Director TI", "Director de Informatica", "IT Director", "Head of IT", "Chief Technology Officer", "CTO"]
- Example for CRM persona: ["CRM Director", "Director CRM", "Sales Operations Director", "Director de Operaciones Comerciales", "Head of CRM", "CRM Manager", "Director de Ventas", "Sales Director", "Commercial Director", "Director Comercial"]
- Broader is always better — Apollo will filter by relevance

Return ONLY a JSON array, one object per company in the same order:
[{"company":"","buying_committee":[{"persona":"","role_type":"Power Lead or Sponsor Lead","priority":"Hot or Warm or Cold","search_titles":[],"why":""}],"outreach_angle":"","why_now":"","value_pillar":""}]"""


def call_claude(prompt, system, key, max_tokens=4096, cache_system=False, model=None):
    client = _get_client(key)
    _model = model or CLAUDE_MODEL
    system_param = (
        [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
        if cache_system else system
    )
    msg = client.messages.create(
        model=_model, max_tokens=max_tokens, temperature=0,
        system=system_param,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)

def claude_stage1(batch, key):
    block = "\n".join(f"- {c}" for c in batch)
    prompt = f"Analyze these Spanish companies and score each one.\n\nCOMPANIES:\n{block}"
    try:
        return call_claude(prompt, _STAGE1_SYSTEM, key,
                           cache_system=True)
    except Exception as e:
        return [{"company":c,"score":0,"tier":"Low Priority","industry":"Unknown",
                 "account_type_hint":"Unknown","trigger_events":[],"narrative":str(e)}
                for c in batch]

def claude_stage3a(accounts, key):
    block = ""
    for a in accounts:
        block += f"""
Company: {a['company']}
Industry: {a['industry']}
Account Type: {a['account_type']}
Final Score: {a['final_score']}
Technologies: {', '.join(a.get('technologies',[])[:5]) or 'Unknown'}
Active CRM Job Postings: {', '.join(a.get('crm_job_titles',[])) or 'None detected'}
Trigger Events: {', '.join(a.get('trigger_events',[])) or 'None detected'}
"""
    prompt = f"For each account define the ideal buying committee and outreach strategy.\n{block}"
    try:
        return call_claude(prompt, _STAGE3A_SYSTEM, key,
                           max_tokens=8000, cache_system=True)
    except Exception as e:
        return [{"company":a['company'],"buying_committee":[],"outreach_angle":"",
                 "why_now":str(e),"value_pillar":""}
                for a in accounts]

def claude_stage4(leads_data, key):
    block = ""
    for item in leads_data:
        for lead in item.get("hot_leads", []):
            new_hire_line = f"New Hire: YES — joined {lead['hire_date']} (use 'new broom' angle)" if lead.get("new_hire") else "New Hire: No"
            block += f"""
Company: {item['company']} ({item['industry']} · {item['account_type']})
Lead: {lead.get('name','Unknown')} — {lead.get('title','')}
{new_hire_line}
Angle: {item.get('outreach_angle','')}
Why Now: {item.get('why_now','')}
Value Pillar: {item.get('value_pillar','')}
---"""
    if not block.strip():
        return []
    prompt = f"""Generate personalized outreach for each lead.

{block}

For each lead return:
- email_opener: 2-3 lines personalized to their role and company context (in Spanish or English based on company)
- why_now_hook: one sentence based on trigger events / account type
- sequence: LinkedIn connect → email → call cadence suggestion

Return ONLY a JSON array:
[{{"company":"","lead_name":"","lead_title":"","email_opener":"","why_now_hook":"","sequence":""}}]"""
    try:
        return call_claude(prompt,
                           "You only respond with structured JSON arrays. Never wrap in markdown code blocks.",
                           key, max_tokens=8000)
    except Exception:
        return []


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS — HTML export
# ════════════════════════════════════════════════════════════════════════════════
def generate_html(s1, s2, s3, s4, today):
    tier_colors = {"A Strategic":"#16a34a","B Prime":"#2563eb",
                   "C Monitor":"#d97706","Low Priority":"#6b7280"}
    type_colors = {"Existing Business":"#7c3aed",
                   "Green Field — Displacement":"#dc2626",
                   "Green Field — Transformation":"#0891b2",
                   "Unknown":"#6b7280"}

    results = s2 if s2 else s1
    results = sorted(results, key=lambda x: x.get("final_score", x.get("score",0)), reverse=True)

    leads_by_company  = {r["company"]: r for r in s3}
    outreach_by_lead  = {}
    for r in s4:
        outreach_by_lead[f"{r.get('company','')}-{r.get('lead_name','')}"] = r

    rows = ""
    for r in results:
        tier   = r.get("final_tier", r.get("tier",""))
        atype  = r.get("account_type", r.get("account_type_hint",""))
        score  = r.get("final_score", r.get("score",0))
        tc     = tier_colors.get(tier,"#6b7280")
        ac     = type_colors.get(atype,"#6b7280")

        leads_html = ""
        for lead in leads_by_company.get(r.get("company",""),{}).get("leads",[]):
            pc = "#dc2626" if lead.get("priority")=="Hot" else "#d97706" if lead.get("priority")=="Warm" else "#6b7280"
            ln = f' <a href="{lead["linkedin"]}" target="_blank" style="color:#2563eb">LI</a>' if lead.get("linkedin") else ""
            leads_html += (f'<div style="margin-bottom:3px">'
                           f'<span style="color:{pc};font-weight:600">{lead.get("priority","")}</span> '
                           f'{lead.get("name","—")} · '
                           f'<span style="color:#6b7280;font-size:11px">{lead.get("title","")}</span>'
                           f'{ln}</div>')

        rows += f"""<tr>
          <td style="font-weight:600">{r.get('company','')}</td>
          <td style="text-align:center;color:#6b7280">{r.get('ae_source','')}</td>
          <td style="text-align:center;font-weight:700;font-size:18px">{score}</td>
          <td style="text-align:center">
            <span style="background:{tc};color:white;padding:2px 8px;border-radius:10px;font-size:12px">{tier}</span>
          </td>
          <td>
            <span style="background:{ac};color:white;padding:2px 6px;border-radius:8px;font-size:11px">{atype}</span>
          </td>
          <td style="font-size:12px">{r.get('industry','')}</td>
          <td style="font-size:12px">{leads_html or '—'}</td>
          <td style="font-size:11px;color:#4b5563">{r.get('narrative','')}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Hunting Machine — {today}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f9fafb;padding:32px;color:#111827}}
h1{{font-size:24px;font-weight:700;color:#1e293b}}
.meta{{color:#6b7280;font-size:13px;margin:6px 0 24px}}
table{{width:100%;border-collapse:collapse;background:white;border-radius:10px;
       overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
th{{background:#f8fafc;padding:11px 12px;text-align:left;font-size:11px;color:#6b7280;
    text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #e5e7eb}}
td{{padding:12px;border-bottom:1px solid #f1f5f9;vertical-align:top}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8fafc}}
</style></head><body>
<h1>🎯 Hunting Machine</h1>
<div class="meta">Generated {today} · {len(results)} accounts · Claude Sonnet 4.6 + Apollo.io</div>
<table>
<thead><tr>
  <th>Company</th><th>AE</th><th>Score</th><th>Tier</th>
  <th>Type</th><th>Industry</th><th>Leads</th><th>Narrative</th>
</tr></thead>
<tbody>{rows}</tbody>
</table></body></html>"""


# ════════════════════════════════════════════════════════════════════════════════
# STAGE 0 — Upload + Pre-Filter
# ════════════════════════════════════════════════════════════════════════════════
st.markdown("---")
st.subheader("📂 Step 0 — Upload Account Lists")

uploaded_files = st.file_uploader(
    "Upload one or more account files (CSV or JSON)",
    type=["csv","json"],
    accept_multiple_files=True
)

_single = single_account.strip()

# Reset pipeline state when the single-account target changes (or when switching
# from batch mode to single-account mode mid-session).
_PIPELINE_DEFAULTS = {
    "stage": 0, "prefilter_done": False, "prefilter_results": [],
    "company_sources": {}, "stage1_results": [], "stage2_results": [],
    "stage3a_results": {}, "stage3_results": [], "stage4_results": [],
    "s2_run_credits": 0, "s3_run_credits": 0,
}
if _single and _single != st.session_state.single_account_last_run:
    for k, v in _PIPELINE_DEFAULTS.items():
        st.session_state[k] = v
    st.session_state.single_account_last_run = _single

if _single and not st.session_state.prefilter_done:
    # Single-account mode: bypass file upload entirely
    company_sources = {_single: ae_name.strip() or "manual"}
    all_companies   = [_single]
    st.caption(f"🎯 Single-account mode: running full pipeline for **{_single}**")
    if not (anthropic_key and anthropic_key.startswith("sk-ant-")):
        st.warning("Anthropic API key not found in .env file.")
    elif st.button("🚀 Run Full Pipeline", type="primary"):
        st.session_state.prefilter_results = [{"company": _single, "decision": "keep", "detail": "Single-account mode — pre-filter bypassed."}]
        st.session_state.company_sources   = company_sources
        st.session_state.prefilter_done    = True
        st.rerun()

elif uploaded_files and not st.session_state.prefilter_done:
    company_sources = {}
    for f in uploaded_files:
        src = ae_name.strip() or f.name.rsplit(".",1)[0]
        pairs = load_companies(f, src)
        added = sum(1 for c,s in pairs if c not in company_sources
                    and not company_sources.update({c:s}))  # type: ignore
        st.caption(f"📄 {f.name} → {added} companies · source: **{src}**")

    all_companies = list(company_sources.keys())

    # ── Pre-filter cache preview ───────────────────────────────────────────────
    pf_cache     = load_prefilter_cache()
    cached_pf    = [c for c in all_companies if _cache_key(c) in pf_cache]
    new_pf       = [c for c in all_companies if _cache_key(c) not in pf_cache]

    col_pfc, col_pfn, col_pft = st.columns(3)
    col_pfc.metric("⚡ From cache", len(cached_pf))
    col_pfn.metric("✨ New to Claude", len(new_pf))
    col_pft.metric("Total", len(all_companies))
    if cached_pf:
        st.caption(f"✅ {len(cached_pf)} account(s) pre-filtered in the last {CACHE_TTL_DAYS} days will be loaded instantly.")

    btn_pf_label = (
        f"🔍 Run Pre-Filter ({len(new_pf)} new accounts)"
        if new_pf else
        f"⚡ Load Pre-Filter from Cache ({len(cached_pf)} accounts)"
    )

    if not (anthropic_key and anthropic_key.startswith("sk-ant-")):
        st.warning("Anthropic API key not found in .env file.")
    elif st.button(btn_pf_label, type="primary"):
        pf_cache = load_prefilter_cache()
        pf = []

        # Serve cached decisions
        for company in cached_pf:
            entry = dict(pf_cache[_cache_key(company)])
            pf.append(entry)

        # Run Claude only on new companies
        if new_pf:
            with st.spinner(f"Screening {len(new_pf)} new companies in one Claude call..."):
                new_results = claude_prefilter(new_pf, anthropic_key)
            now = datetime.now(timezone.utc).isoformat()
            for r in new_results:
                r["cached_at"] = now
                pf_cache[_cache_key(r["company"])] = r
                pf.append(r)
            save_prefilter_cache(pf_cache)

        # Preserve original upload order
        order = {c: i for i, c in enumerate(all_companies)}
        pf.sort(key=lambda r: order.get(r.get("company",""), 9999))

        st.session_state.prefilter_results = pf
        st.session_state.company_sources   = company_sources
        st.session_state.prefilter_done    = True
        st.rerun()

# ── Pre-filter results ────────────────────────────────────────────────────────
if st.session_state.prefilter_done and st.session_state.stage == 0:
    st.markdown("---")
    st.subheader("🔍 Pre-Filter Results")
    st.caption("Review below. Restore any accounts incorrectly discarded before running Stage 1.")

    pf  = st.session_state.prefilter_results
    cs  = st.session_state.company_sources
    kept      = [r for r in pf if r.get("decision") == "keep"]
    discarded = [r for r in pf if r.get("decision") == "discard"]
    saving    = f"~{int(len(discarded)/len(pf)*100)}%" if pf else "0%"

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Uploaded",     len(pf))
    mc2.metric("Proceeding",   len(kept))
    mc3.metric("Discarded",    len(discarded))
    mc4.metric("Tokens saved", saving)

    st.markdown(f"**✅ Proceeding to Stage 1 — {len(kept)} accounts**")
    keep_df = pd.DataFrame([{
        "Company":   r["company"],
        "AE":        cs.get(r["company"], ""),
        "Reasoning": r.get("detail", ""),
    } for r in kept])
    st.dataframe(keep_df, width='stretch', hide_index=True)

    restored_companies = []
    if discarded:
        with st.expander(f"❌ Discarded accounts ({len(discarded)}) — click to review and restore"):
            st.caption("Check ↩ Restore to add an account back. AE relationship context may override the pre-filter.")
            if "restore_all" not in st.session_state:
                st.session_state.restore_all = False
            if st.button("☑️ Select All", key="select_all_discarded"):
                st.session_state.restore_all = True
            if st.button("Clear All", key="clear_all_discarded"):
                st.session_state.restore_all = False
            discard_df = pd.DataFrame([{
                "Company":     r["company"],
                "AE":          cs.get(r["company"], ""),
                "Reason":      r.get("reason", ""),
                "Detail":      r.get("detail", ""),
                "↩ Restore":   st.session_state.restore_all,
            } for r in discarded])
            edited_discard = st.data_editor(
                discard_df,
                column_config={"↩ Restore": st.column_config.CheckboxColumn("↩ Restore", default=False)},
                width='stretch',
                hide_index=True,
                key="restore_editor",
                disabled=["Company","AE","Reason","Detail"],
            )
            restored_companies = edited_discard[edited_discard["↩ Restore"] == True]["Company"].tolist()
            if restored_companies:
                st.caption(f"↩ {len(restored_companies)} account(s) will be restored to Stage 1.")

    total_proceeding = len(kept) + len(restored_companies)
    final_companies  = [r["company"] for r in kept] + restored_companies

    # ── Cache preview (shown immediately after pre-filter) ────────────────────
    cache        = load_stage1_cache()
    cached_cos   = [c for c in final_companies if _cache_key(c) in cache]
    new_cos      = [c for c in final_companies if _cache_key(c) not in cache]

    st.markdown("---")
    st.markdown("**⚡ Stage 1 Cache Status**")
    col_c, col_n, col_t = st.columns(3)
    col_c.metric("⚡ From cache", len(cached_cos))
    col_n.metric("✨ New to Claude", len(new_cos))
    col_t.metric("Total", total_proceeding)
    if cached_cos:
        st.caption(f"✅ {len(cached_cos)} account(s) scored in the last {CACHE_TTL_DAYS} days will be loaded instantly — only {len(new_cos)} new accounts will go to Claude.")

    btn_label = (
        f"🧠 Confirm & Run Stage 1 — Claude Full Scoring ({len(new_cos)} new accounts)"
        if new_cos else
        f"⚡ Load Stage 1 from Cache ({len(cached_cos)} accounts)"
    )
    if st.button(btn_label, type="primary"):
        s1      = []
        cache   = load_stage1_cache()
        progress = st.progress(0)
        status   = st.empty()
        total    = len(final_companies)

        # Serve cached accounts first
        for company in cached_cos:
            entry = cache[_cache_key(company)]
            entry["ae_source"]   = st.session_state.company_sources.get(company, "")
            entry["from_cache"]  = True
            s1.append(entry)

        # Score new accounts via Claude
        if new_cos:
            batches = [new_cos[i:i+BATCH_SIZE] for i in range(0, len(new_cos), BATCH_SIZE)]
            for i, batch in enumerate(batches):
                status.text(f"🧠 Batch {i+1}/{len(batches)}: {', '.join(batch[:3])}...")
                results = claude_stage1(batch, anthropic_key)
                for r in results:
                    r["ae_source"]  = st.session_state.company_sources.get(r.get("company",""), "")
                    r["from_cache"] = False
                    r["cached_at"]  = datetime.now(timezone.utc).isoformat()
                    cache[_cache_key(r["company"])] = r
                s1.extend(results)
                progress.progress((len(cached_cos) + sum(len(batches[j]) for j in range(i+1))) / total)

            save_stage1_cache(cache)

        progress.progress(1.0)
        s1.sort(key=lambda x: x.get("score", 0), reverse=True)
        st.session_state.stage1_results = s1
        st.session_state.stage          = 1
        status.success(f"✅ Stage 1 complete — {len(cached_cos)} from cache, {len(new_cos)} freshly scored.")
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# STAGE 1 — Review Gate (only when stage == 1)
# ════════════════════════════════════════════════════════════════════════════════
if st.session_state.stage == 1 and st.session_state.stage1_results:
    st.markdown("---")
    st.subheader("✏️ Step 1 — Manual Review Gate")
    st.caption(f"Override tiers or mark accounts as 'Remove' before Apollo spend. Recommended: score ≥ {APOLLO_MIN_SCORE} for Apollo enrichment. Accounts scoring 25–27 may return limited data for Iberian companies.")

    s1 = st.session_state.stage1_results
    cache_now = load_stage1_cache()
    _in_single_mode = bool(st.session_state.single_account_last_run)
    if _in_single_mode:
        st.caption("🎯 Single-account mode — Override Tier defaulted to A Strategic so the full pipeline runs regardless of score.")
    review_df = pd.DataFrame([{
        "Company":        r.get("company",""),
        "AE":             r.get("ae_source",""),
        "Score":          int(r.get("score",0)),
        "Claude Tier":    r.get("tier",""),
        "Override Tier":  "A Strategic" if _in_single_mode else (r.get("tier","") if r.get("score",0) >= APOLLO_MIN_SCORE else "C Monitor"),
        "Industry":       r.get("industry",""),
        "Account Type":   r.get("account_type_hint",""),
        "Triggers":       ", ".join(r.get("trigger_events",[])),
        "Narrative":      r.get("narrative",""),
        "Source":         "⚡ cached" if r.get("from_cache") else "✨ new",
        "Cache expires":  f"{cache_days_remaining(cache_now[_cache_key(r.get('company',''))])}d" if _cache_key(r.get("company","")) in cache_now else "—",
    } for r in s1])

    show_all = st.checkbox("Show Low Priority accounts", value=False)
    display_df = review_df if show_all else review_df[review_df["Claude Tier"] != "Low Priority"]

    c1, c2 = st.columns([3,1])
    with c2:
        eligible = len(display_df[~display_df["Override Tier"].isin(["Low Priority","Remove"])])
        st.metric("Proceeding to Apollo", eligible)

    edited_df = st.data_editor(
        display_df,
        column_config={
            "Override Tier": st.column_config.SelectboxColumn(
                "Override Tier",
                options=["A Strategic","B Prime","C Monitor","Low Priority","Remove"],
                required=True,
            ),
            "Score":     st.column_config.NumberColumn("Score", format="%d"),
            "Narrative": st.column_config.TextColumn("Narrative", width="large"),
            "Triggers":  st.column_config.TextColumn("Triggers", width="medium"),
        },
        width='stretch',
        hide_index=True,
        key="review_editor",
    )

    # Accounts promoted to A Strategic or B Prime go to Apollo regardless of raw score.
    # Accounts scoring 25-27 default to C Monitor so they are excluded unless AE promotes them.
    confirmed = edited_df[edited_df["Override Tier"].isin(["A Strategic", "B Prime"])]
    below_threshold = edited_df[
        (edited_df["Score"] >= 25) & (edited_df["Score"] < APOLLO_MIN_SCORE) &
        ~edited_df["Override Tier"].isin(["A Strategic","B Prime","Low Priority","Remove"])
    ]
    st.caption(f"**{len(confirmed)} accounts** will be sent to Apollo.")
    if len(below_threshold) > 0:
        st.caption(f"⚠️ {len(below_threshold)} account(s) scoring 25–27 are excluded by default — change their Override Tier to B Prime or A Strategic above to include them.")

    # ── Stage 2 cache preview ─────────────────────────────────────────────────
    s2_cache     = load_stage2_cache()
    all_rows     = confirmed.to_dict("records")
    cached_s2    = [r for r in all_rows if _cache_key(r["Company"]) in s2_cache]
    new_s2       = [r for r in all_rows if _cache_key(r["Company"]) not in s2_cache]

    col_c2, col_n2, col_t2 = st.columns(3)
    col_c2.metric("⚡ From cache", len(cached_s2))
    col_n2.metric("🔍 New to Apollo", len(new_s2))
    col_t2.metric("Total", len(all_rows))
    if cached_s2:
        st.caption(f"✅ {len(cached_s2)} account(s) enriched in the last {CACHE_TTL_DAYS} days will be loaded instantly.")

    btn_s2_label = (
        f"🔍 Confirm & Run Apollo — Stage 2 ({len(new_s2)} new accounts)"
        if new_s2 else
        f"⚡ Load Stage 2 from Cache ({len(cached_s2)} accounts)"
    )

    if not apollo_key:
        st.warning("Apollo API key not found in .env file.")
    elif st.button(btn_s2_label, type="primary"):
        rows     = all_rows
        progress = st.progress(0)
        status   = st.empty()
        s2       = []
        s2_cache = load_stage2_cache()
        triggers_by_company = {r.get("company",""): r.get("trigger_events",[])
                                for r in st.session_state.stage1_results}

        run_credits = 0

        # Serve cached accounts
        for row in cached_s2:
            entry = dict(s2_cache[_cache_key(row["Company"])])
            # Update mutable fields from current run
            entry["stage1_score"]  = row["Score"]
            entry["stage1_tier"]   = row["Claude Tier"]
            entry["override_tier"] = row["Override Tier"]
            entry["from_s2_cache"] = True
            entry["apollo_credits"] = 0  # no credits spent — served from cache
            s2.append(entry)

        # Resolve + enrich only new accounts
        if new_s2:
            status.text("🧠 Claude resolving company names for Apollo search...")
            company_names = [row["Company"] for row in new_s2]
            resolved_list = claude_resolve_names(company_names, anthropic_key)
            resolved_map  = {r.get("company",""): r for r in resolved_list}

            for i, row in enumerate(new_s2):
                status.text(f"🔍 Apollo enriching {i+1}/{len(new_s2)}: {row['Company']}...")
                resolved   = resolved_map.get(row["Company"],
                                 {"company": row["Company"], "canonical_name": row["Company"],
                                  "domain": "", "alt_names": []})
                apollo_org = enrich_org(resolved, apollo_key, country=target_country.strip())
                scoring    = score_apollo(row["Score"], apollo_org)

                final_tier = score_to_tier(scoring["final_score"])
                override   = row["Override Tier"]
                if TIER_ORDER.get(override,99) > TIER_ORDER.get(final_tier,99):
                    final_tier = override

                credits = apollo_org.get("_enrich_credits", 0) if apollo_org else 0
                run_credits += credits
                entry = {
                    "company":          row["Company"],
                    "ae_source":        row["AE"],
                    "industry":         row["Industry"],
                    "narrative":        row["Narrative"],
                    "stage1_score":     row["Score"],
                    "stage1_tier":      row["Claude Tier"],
                    "override_tier":    override,
                    "apollo_bonus":     scoring["bonus"],
                    "final_score":      scoring["final_score"],
                    "final_tier":       final_tier,
                    "account_type":     scoring["account_type"],
                    "sig_salesforce":   scoring["signals"].get("salesforce_in_stack", False),
                    "sig_funding":      bool(scoring["signals"].get("recent_funding", False)),
                    "sig_crm_hiring":   scoring["signals"].get("crm_hiring", False),
                    "crm_job_titles":   scoring["crm_job_titles"],
                    "technologies":     scoring["technologies"],
                    "employees":        scoring["employees"],
                    "funding_total":    scoring["funding_total"],
                    "domain":           scoring["domain"] or resolved.get("domain",""),
                    "linkedin":         scoring["linkedin"],
                    "trigger_events":   triggers_by_company.get(row["Company"], []),
                    "apollo_name_used": apollo_org.get("_resolved_name","") if apollo_org else "",
                    "apollo_canonical": resolved.get("canonical_name",""),
                    "apollo_credits":   credits,
                    "from_s2_cache":    False,
                    "cached_at":        datetime.now(timezone.utc).isoformat(),
                }
                s2.append(entry)
                s2_cache[_cache_key(row["Company"])] = entry
                progress.progress((len(cached_s2) + i + 1) / len(rows))

            save_stage2_cache(s2_cache)

        progress.progress(1.0)
        s2.sort(key=lambda x: x.get("final_score",0), reverse=True)
        st.session_state.stage2_results  = s2
        st.session_state.stage           = 2
        st.session_state.s2_run_credits  = run_credits
        status.success(f"✅ Stage 2 complete — {len(cached_s2)} from cache, {len(new_s2)} freshly enriched · 🔋 {run_credits} Apollo credits used.")
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# STAGE 2 — Apollo Results
# ════════════════════════════════════════════════════════════════════════════════
if st.session_state.stage >= 2 and st.session_state.stage2_results:
    st.markdown("---")
    st.subheader("🔍 Stage 2 — Apollo Enrichment")
    st.caption(
        "**S1 Score** = Claude first tiering · "
        "**Apollo +** = bonus from real Apollo signals · "
        "**Final Score** = S1 + Apollo · Sorted by final score descending"
    )

    s2 = st.session_state.stage2_results
    _single_mode = bool(st.session_state.single_account_last_run)

    # ── Metrics ───────────────────────────────────────────────────────────────
    # In single-account mode all accounts proceed to 3a/3b regardless of tier.
    tier_a    = s2 if _single_mode else [r for r in s2 if r["final_tier"] == "A Strategic"]
    tier_b    = [r for r in s2 if r["final_tier"] == "B Prime"]
    promoted  = [r for r in s2 if r["final_tier"] != r["stage1_tier"]]
    eb        = [r for r in s2 if "Existing" in r.get("account_type","")]
    gf        = [r for r in s2 if "Green Field" in r.get("account_type","")]
    no_apollo = [r for r in s2 if r.get("apollo_bonus",0) == 0 and not r.get("sig_salesforce")]
    resolved_hit = [r for r in s2 if r.get("apollo_name_used") and
                    r["apollo_name_used"] != r["company"]]
    hit_rate  = int((len(s2) - len(no_apollo)) / len(s2) * 100) if s2 else 0
    avg_bonus = round(sum(r.get("apollo_bonus",0) for r in s2) / len(s2), 1) if s2 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Enriched",       len(s2))
    c2.metric("A Strategic",    len(tier_a), delta=f"+{len(promoted)} promoted by Apollo" if promoted else None)
    c3.metric("B Prime",        len(tier_b))
    c4.metric("Apollo Hit Rate",f"{hit_rate}%",
              delta=f"↑ name resolution: {len(resolved_hit)} via canonical/alt" if resolved_hit else None)

    total_s2_credits = sum(r.get("apollo_credits", 0) for r in s2)
    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Existing Business", len(eb))
    c6.metric("Green Field",       len(gf))
    c7.metric("No Apollo Data",    len(no_apollo))
    c8.metric("Avg Apollo Bonus",  f"+{avg_bonus}")

    cr1, cr2 = st.columns(2)
    cr1.metric("🔋 Apollo Credits This Run", st.session_state.s2_run_credits)
    cr2.metric("🔋 Apollo Credits Total (cached incl.)", total_s2_credits)

    # ── Name resolution summary ────────────────────────────────────────────────
    canon_fixed = [r for r in s2 if r.get("apollo_canonical") and
                   r.get("apollo_name_used") == r.get("apollo_canonical") and
                   r["apollo_canonical"] != r["company"]]
    alt_fixed   = [r for r in s2 if r.get("apollo_name_used") and
                   r["apollo_name_used"] != r.get("apollo_canonical","") and
                   r["apollo_name_used"] != r["company"]]

    if resolved_hit:
        parts = []
        if canon_fixed:
            parts.append(f"{len(canon_fixed)} found via canonical name "
                         f"({', '.join(r['company'] for r in canon_fixed[:2])})")
        if alt_fixed:
            parts.append(f"{len(alt_fixed)} found via alt name "
                         f"({', '.join(r['company'] for r in alt_fixed[:2])})")
        st.success(f"🧠 **Name resolution:** Claude resolved all company names in 1 API call. "
                   + " · ".join(parts) + f" · Apollo hit rate: **{hit_rate}%**")

    # ── Name resolution expander ───────────────────────────────────────────────
    resolved_rows = [r for r in s2 if r.get("apollo_canonical")]
    if resolved_rows:
        with st.expander(f"🔤 Claude Name Resolution — {len(resolved_rows)} companies"):
            res_html = """
<style>
.rt { width:100%; border-collapse:collapse; font-size:12px; font-family:-apple-system,sans-serif; }
.rt th { color:#9ca3af; font-weight:600; text-transform:uppercase; font-size:10px;
         letter-spacing:.05em; padding:0 12px 8px 0; text-align:left;
         border-bottom:1px solid #f1f5f9; }
.rt td { padding:7px 12px 7px 0; border-bottom:1px solid #f8fafc; vertical-align:middle; }
.rt tr:last-child td { border-bottom:none; }
.ae   { color:#6b7280; }
.cn   { font-weight:600; color:#111827; }
.dtag { background:#f1f5f9; color:#6b7280; font-size:10px; padding:1px 7px;
        border-radius:6px; margin-left:4px; }
.atag { background:#fef9c3; color:#92400e; font-size:10px; padding:1px 7px;
        border-radius:6px; margin-left:4px; }
.mb   { font-size:10px; font-weight:600; padding:2px 8px; border-radius:10px; }
.mc   { background:#dcfce7; color:#16a34a; }
.ma   { background:#fef9c3; color:#92400e; }
.mo   { background:#f1f5f9; color:#6b7280; }
.mn   { background:#fee2e2; color:#dc2626; }
</style>
<table class="rt">
<thead><tr>
  <th>AE Upload Name</th><th>Canonical (Apollo)</th>
  <th>Domain</th><th>Alt Names</th><th>Matched On</th>
</tr></thead><tbody>"""
            for r in resolved_rows:
                ae_name_val = r["company"]
                canon       = r.get("apollo_canonical","")
                domain      = r.get("domain","")
                matched_on  = r.get("apollo_name_used","")
                if matched_on == canon and matched_on != ae_name_val:
                    m_cls, m_txt = "mc", "canonical"
                elif matched_on and matched_on != canon and matched_on != ae_name_val:
                    m_cls, m_txt = "ma", "alt name"
                elif matched_on == ae_name_val:
                    m_cls, m_txt = "mo", "original"
                else:
                    m_cls, m_txt = "mn", "no match"
                domain_tag = f'<span class="dtag">{domain}</span>' if domain else "—"
                row_bg = ' style="background:#f0fdf4"' if m_cls=="mc" and canon!=ae_name_val else \
                         ' style="background:#fffbeb"' if m_cls=="ma" else ""
                res_html += f"""<tr{row_bg}>
  <td class="ae">{ae_name_val}</td>
  <td class="cn">{canon}</td>
  <td>{domain_tag}</td>
  <td>—</td>
  <td><span class="mb {m_cls}">{m_txt}</span></td>
</tr>"""
            res_html += "</tbody></table>"
            st.markdown(res_html, unsafe_allow_html=True)

    # ── Main results table ─────────────────────────────────────────────────────
    def _tier_badge(tier):
        cls = "ta" if tier == "A Strategic" else "tb" if tier == "B Prime" else "tc"
        return f'<span class="tbadge {cls}">{tier}</span>'

    def _type_badge(atype):
        if "Existing" in atype:   cls = "teb"
        elif "Displacement" in atype: cls = "tgfd"
        else:                     cls = "tgft"
        return f'<span class="typbadge {cls}">{atype}</span>'

    def _signals_html(r):
        pills = []
        if r.get("sig_salesforce"):     pills.append('<span class="sig sig-sf">✓ Salesforce +8</span>')
        if r.get("account_type","").count("Displacement"): pills.append('<span class="sig sig-comp">⚡ Competitor CRM +3</span>')
        if r.get("sig_funding"):        pills.append('<span class="sig sig-fund">💰 Funding +5</span>')
        if r.get("sig_crm_hiring"):     pills.append('<span class="sig sig-jobs">📋 CRM Jobs +3</span>')
        if r.get("employees"):          pills.append('<span class="sig sig-head">✓ Headcount +2</span>')
        return " ".join(pills) if pills else '<span class="sig sig-none">No Apollo signals</span>'

    tbl_html = """
<style>
.s2t { width:100%; border-collapse:collapse; background:white; border-radius:8px;
       overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,.07); font-family:-apple-system,sans-serif; }
.s2t th { background:#f8fafc; padding:10px 12px; text-align:left; font-size:10px;
          color:#6b7280; text-transform:uppercase; letter-spacing:.04em;
          border-bottom:1px solid #e5e7eb; white-space:nowrap; }
.s2t td { padding:10px 12px; border-bottom:1px solid #f1f5f9; font-size:13px; vertical-align:middle; }
.s2t tr:last-child td { border-bottom:none; }
.s2t tr.promoted td { background:#fffbeb; }
.s2t tr.resolved td { background:#f0fdf4; }
.s1sc { font-size:15px; font-weight:700; text-align:center; color:#9ca3af; }
.apcol { text-align:center; }
.bpos { font-size:15px; font-weight:800; color:#16a34a; }
.bzero { font-size:15px; font-weight:700; color:#d1d5db; }
.bsub { font-size:10px; color:#9ca3af; margin-top:2px; }
.fsc { font-size:17px; font-weight:800; text-align:center; }
.fsa { color:#16a34a; } .fsb { color:#2563eb; } .fsc2 { color:#d97706; }
.tbadge { display:inline-block; padding:2px 9px; border-radius:10px;
          font-size:11px; font-weight:600; white-space:nowrap; }
.ta { background:#dcfce7; color:#16a34a; } .tb { background:#dbeafe; color:#2563eb; }
.tc { background:#fef3c7; color:#92400e; }
.ptag { display:inline-block; background:#fef3c7; color:#92400e; border:1px solid #fcd34d;
        border-radius:4px; font-size:9px; font-weight:700; padding:1px 5px; margin-left:4px; }
.typbadge { display:inline-block; padding:2px 7px; border-radius:7px;
            font-size:10px; font-weight:600; white-space:nowrap; }
.teb { background:#f3e8ff; color:#7c3aed; }
.tgfd { background:#fee2e2; color:#dc2626; }
.tgft { background:#e0f2fe; color:#0369a1; }
.sig { display:inline-flex; align-items:center; padding:2px 7px; border-radius:6px;
       font-size:10px; font-weight:600; margin:1px; white-space:nowrap; }
.sig-sf   { background:#f0fdf4; color:#16a34a; border:1px solid #86efac; }
.sig-fund { background:#eff6ff; color:#2563eb; border:1px solid #93c5fd; }
.sig-jobs { background:#fff7ed; color:#c2410c; border:1px solid #fed7aa; }
.sig-head { background:#f8fafc; color:#6b7280; border:1px solid #e2e8f0; }
.sig-comp { background:#fee2e2; color:#dc2626; border:1px solid #fca5a5; }
.sig-none { background:#f8fafc; color:#9ca3af; border:1px solid #e5e7eb; font-style:italic; }
.filetag  { font-size:11px; background:#f1f5f9; color:#6b7280; padding:1px 7px; border-radius:8px; }
.techlist { font-size:10px; color:#6b7280; line-height:1.7; }
.rhint    { font-size:10px; color:#16a34a; margin-top:2px; }
.rhinta   { font-size:10px; color:#d97706; margin-top:2px; }
</style>
<table class="s2t">
<thead><tr>
  <th>Company</th><th>AE</th>
  <th style="text-align:center">S1 Score<br><span style="font-size:9px;font-weight:400;text-transform:none">(Claude)</span></th>
  <th style="text-align:center">Apollo +<br><span style="font-size:9px;font-weight:400;text-transform:none">(bonus)</span></th>
  <th style="text-align:center">Final Score</th>
  <th>Final Tier</th><th>Account Type</th>
  <th>Apollo Signals</th><th>Employees</th><th>Top Technologies</th>
  <th style="text-align:center">🔋 Credits</th>
</tr></thead><tbody>"""

    for r in s2:
        was_promoted = r["final_tier"] != r["stage1_tier"]
        resolved_via_canon = (r.get("apollo_canonical") and
                              r.get("apollo_name_used") == r.get("apollo_canonical") and
                              r["apollo_canonical"] != r["company"])
        resolved_via_alt   = (r.get("apollo_name_used") and
                              r["apollo_name_used"] != r.get("apollo_canonical","") and
                              r["apollo_name_used"] != r["company"])

        if resolved_via_canon:
            row_cls = "resolved"
        elif was_promoted:
            row_cls = "promoted"
        else:
            row_cls = ""

        # Company cell
        resolve_hint = ""
        if resolved_via_canon:
            resolve_hint = f'<div class="rhint">✓ Resolved → {r["apollo_canonical"]}</div>'
        elif resolved_via_alt:
            resolve_hint = f'<div class="rhinta">Resolved via alt → {r["apollo_name_used"]}</div>'

        # Scores
        s1    = r["stage1_score"]
        bonus = r.get("apollo_bonus", 0)
        final = r["final_score"]
        tier  = r["final_tier"]

        fs_cls = "fsa" if tier == "A Strategic" else "fsb" if tier == "B Prime" else "fsc2"

        # Bonus breakdown
        bonus_parts = []
        if r.get("sig_salesforce"):     bonus_parts.append("SF +8")
        if "Displacement" in r.get("account_type","") and not r.get("sig_salesforce"):
                                        bonus_parts.append("Comp +3")
        if r.get("sig_funding"):        bonus_parts.append("Fund +5")
        if r.get("sig_crm_hiring"):     bonus_parts.append("Jobs +3")
        if r.get("employees"):          bonus_parts.append("Head +2")
        bonus_sub = " · ".join(bonus_parts)

        promoted_tag = '<span class="ptag">⬆ Promoted</span>' if was_promoted else ""
        emp_str = f"~{r['employees']:,}" if r.get("employees") else "—"
        tech_str = " · ".join(r.get("technologies",[])[:5]) or "—"

        tbl_html += f"""<tr class="{row_cls}">
  <td><strong>{r['company']}</strong>{resolve_hint}</td>
  <td><span class="filetag">{r['ae_source']}</span></td>
  <td class="s1sc">{s1}</td>
  <td class="apcol">
    <div class="{'bpos' if bonus > 0 else 'bzero'}">{'+' if bonus > 0 else ''}{bonus}</div>
    <div class="bsub">{bonus_sub}</div>
  </td>
  <td class="fsc {fs_cls}">{final}</td>
  <td>{_tier_badge(tier)}{promoted_tag}</td>
  <td>{_type_badge(r.get('account_type',''))}</td>
  <td>{_signals_html(r)}</td>
  <td style="text-align:center;color:#6b7280;font-size:12px">{emp_str}</td>
  <td class="techlist">{tech_str}</td>
  <td style="text-align:center;font-size:12px;color:{'#16a34a' if r.get('apollo_credits',0)==0 else '#b45309'}">
    {'⚡ 0' if r.get('from_s2_cache') else f"🔋 {r.get('apollo_credits',0)}"}</td>
</tr>"""

    tbl_html += "</tbody></table>"
    st.markdown(tbl_html, unsafe_allow_html=True)
    st.markdown("")  # spacing

    # ── Summary callouts ───────────────────────────────────────────────────────
    if promoted:
        names = " · ".join(r["company"] for r in promoted[:3])
        st.warning(f"⬆ **{len(promoted)} account(s) promoted by Apollo:** {names}")

    if _single_mode:
        st.info(f"👥 **Ready for Lead Intelligence** — single-account mode: all {len(tier_a)} account(s) proceed to Stage 3a + 3b regardless of tier.")
    else:
        st.info(f"👥 **Ready for Lead Intelligence** — "
                f"**{len(tier_a)} Tier A Strategic accounts** proceed to Stage 3a + 3b.")

    if st.session_state.stage == 2:
        if not tier_a:
            st.error("No accounts available for Stage 3a. Review overrides above." if _single_mode else "No Tier A Strategic accounts found. Review overrides above.")

        # ── Step 1: Define Buying Committees (3a) ─────────────────────────────
        elif not st.session_state.stage3a_results:
            st.markdown("")
            s3a_cache   = load_stage3a_cache()
            cached_s3a  = [a for a in tier_a if _cache_key(a["company"]) in s3a_cache]
            new_s3a     = [a for a in tier_a if _cache_key(a["company"]) not in s3a_cache]
            col_s3a_c, col_s3a_n, col_s3a_t = st.columns(3)
            col_s3a_c.metric("⚡ From cache", len(cached_s3a))
            col_s3a_n.metric("✨ New to Claude", len(new_s3a))
            col_s3a_t.metric("Total", len(tier_a))
            if cached_s3a:
                st.caption(f"✅ {len(cached_s3a)} account(s) have buying committees cached in the last {CACHE_TTL_DAYS} days — only {len(new_s3a)} new accounts will go to Claude.")
            if not anthropic_key:
                st.warning("Enter your Anthropic API key in the sidebar to proceed.")
            elif st.button("🧠 Define Buying Committees — Stage 3a", type="primary"):
                progress = st.progress(0)
                status   = st.empty()
                personas = []

                # Serve cached accounts instantly
                for a in cached_s3a:
                    personas.append(s3a_cache[_cache_key(a["company"])])

                # Run Claude only for new accounts
                if new_s3a:
                    status.text("🧠 Claude defining buying committees...")
                    batches = [new_s3a[i:i+BATCH_SIZE] for i in range(0, len(new_s3a), BATCH_SIZE)]
                    for i, batch in enumerate(batches):
                        results = claude_stage3a(batch, anthropic_key)
                        for r in results:
                            r["cached_at"] = datetime.now(timezone.utc).isoformat()
                            s3a_cache[_cache_key(r["company"])] = r
                        personas.extend(results)
                        progress.progress((len(cached_s3a) + sum(len(batches[j]) for j in range(i+1))) / len(tier_a))
                    save_stage3a_cache(s3a_cache)

                progress.progress(1.0)
                st.session_state.stage3a_results = {p["company"]: p for p in personas}
                status.success(f"✅ Buying committees defined — {len(cached_s3a)} from cache, {len(new_s3a)} freshly scored.")
                st.rerun()

        # ── Step 2: Review gate + Run People Search (3b) ──────────────────────
        else:
            personas_map = st.session_state.stage3a_results

            st.markdown("---")
            st.subheader("🧠 Stage 3a — Buying Committee Review")
            st.caption("Review the buying committees Claude defined before Apollo people search runs. "
                       "Expand any account to see personas, outreach angle, and why-now context.")

            # Metrics
            all_personas   = [p for pd in personas_map.values()
                               for p in pd.get("buying_committee", [])]
            hot_personas   = [p for p in all_personas if p.get("priority") == "Hot"]
            nh_accounts    = [c for c, pd in personas_map.items()
                              if any(te for te in st.session_state.stage3a_results.get(c,{})
                                     .get("buying_committee",[]))]
            total_personas = len(all_personas)
            apollo_est     = total_personas  # 1 call per persona

            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("Accounts",        len(tier_a))
            mc2.metric("Hot Personas",    len(hot_personas))
            mc3.metric("Total Personas",  total_personas)
            mc4.metric("Apollo Calls Est.", apollo_est)

            # Per-account expanders
            priority_icon = {"Hot": "🔴", "Warm": "🟡", "Cold": "🟢"}
            role_colors   = {"Power Lead": "#b91c1c", "Sponsor Lead": "#1d4ed8"}

            for account in tier_a:
                p_data    = personas_map.get(account["company"], {})
                committee = p_data.get("buying_committee", [])
                hot_count = sum(1 for p in committee if p.get("priority") == "Hot")
                nh_flag   = any(te for te in account.get("trigger_events", [])
                                if "hire" in te.lower() or "cio" in te.lower()
                                or "cdo" in te.lower() or "cco" in te.lower())

                header = (f"{'🟢' if account['final_tier']=='A Strategic' else '🔵'} "
                          f"**{account['company']}** · Score {account['final_score']} · "
                          f"{len(committee)} personas"
                          + (f" · 🔴 {hot_count} Hot" if hot_count else "")
                          + (" · 🆕 trigger" if nh_flag else ""))

                with st.expander(header):
                    col_intel, col_personas = st.columns([1, 2])

                    with col_intel:
                        st.markdown(f"""
**Tier:** {account.get('final_tier','')}
**Industry:** {account.get('industry','')}
**Account Type:** {account.get('account_type','')}
**Angle:** {p_data.get('outreach_angle','')}
**Why Now:** {p_data.get('why_now','')}
**Value Pillar:** {p_data.get('value_pillar','')}
""")
                    with col_personas:
                        if committee:
                            cards_html = ""
                            for p in committee:
                                pri   = p.get("priority", "Warm")
                                icon  = priority_icon.get(pri, "🟡")
                                role  = p.get("role_type", "")
                                rc    = role_colors.get(role, "#6b7280")
                                titles_html = "".join(
                                    f'<span style="font-size:10px;background:#f1f5f9;color:#6b7280;'
                                    f'padding:1px 6px;border-radius:4px;margin:1px;'
                                    f'display:inline-block">{t}</span>'
                                    for t in p.get("search_titles", [])
                                )
                                bg = "#fff9f9" if pri=="Hot" else "#fffdf5" if pri=="Warm" else "#f9fafb"
                                bc = "#fecaca" if pri=="Hot" else "#fde68a" if pri=="Warm" else "#f1f5f9"
                                cards_html += f"""
<div style="border:1px solid {bc};background:{bg};border-radius:8px;
            padding:9px 12px;margin-bottom:7px">
  <div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">
    <span style="font-size:14px">{icon}</span>
    <span style="font-weight:700;font-size:13px;color:#111827">{p.get('persona','')}</span>
    <span style="font-size:9px;font-weight:700;padding:1px 6px;border-radius:6px;
                 background:{rc}22;color:{rc}">{role}</span>
  </div>
  <div style="font-size:12px;color:#374151;margin-bottom:5px">{p.get('why','')}</div>
  <div style="display:flex;flex-wrap:wrap;gap:3px">{titles_html}</div>
</div>"""
                            st.markdown(cards_html, unsafe_allow_html=True)
                        else:
                            st.caption("No committee defined.")

            # Run 3b button
            st.markdown("")
            if not apollo_key:
                st.warning("Enter your Apollo API key in the sidebar to run people search.")
            elif st.button("👥 Run People Search — Stage 3b", type="primary"):
                progress = st.progress(0)
                status   = st.empty()
                s3             = []
                s3_run_credits = 0

                for i, account in enumerate(tier_a):
                    status.text(f"👥 Apollo finding leads {i+1}/{len(tier_a)}: {account['company']}...")
                    p_data    = personas_map.get(account["company"], {})
                    committee = p_data.get("buying_committee", [])
                    all_leads, seen_names = [], set()
                    acct_unlock_credits = 0

                    # If Stage 3a returned no committee, use fallback title search
                    search_personas = committee if committee else [
                        {"priority": "Hot",  "role_type": "Power Lead",   "search_titles": FALLBACK_HOT_TITLES},
                        {"priority": "Warm", "role_type": "Sponsor Lead", "search_titles": FALLBACK_WARM_TITLES},
                    ]

                    # FIX 3: use Apollo-matched name for people search, not AE-uploaded name
                    search_name = account.get("apollo_name_used") or account["company"]

                    # FIX 5: check credit cap before starting this account
                    if s3_run_credits >= S3B_CREDIT_CAP:
                        status.warning(f"⚠️ Credit cap of {S3B_CREDIT_CAP} reached — stopping Stage 3b. {len(tier_a) - i} account(s) skipped.")
                        break

                    # Pre-check: does Apollo have any people for this account? (free call)
                    try:
                        probe = apollo_post("mixed_people/api_search", {
                            "q_organization_name": search_name, "page": 1, "per_page": 1
                        }, apollo_key)
                        if not probe.get("people"):
                            st.caption(f"⚠️ {account['company']} — no people indexed in Apollo, skipping.")
                            s3.append({**account, "buying_committee": committee,
                                        "outreach_angle": p_data.get("outreach_angle",""),
                                        "why_now": p_data.get("why_now",""),
                                        "value_pillar": p_data.get("value_pillar",""),
                                        "leads": [], "unlock_credits": 0})
                            progress.progress((i+1)/len(tier_a))
                            continue
                    except Exception:
                        pass  # probe failed — proceed with persona searches anyway

                    empty_streak = 0
                    for persona in search_personas:
                        titles = persona.get("search_titles", [])
                        if not titles:
                            continue
                        # Consecutive-empty guard: 3 empty personas in a row → stop this account
                        if empty_streak >= 3:
                            break
                        # Intra-account credit cap: stop persona loop if cap reached mid-account
                        if s3_run_credits + acct_unlock_credits >= S3B_CREDIT_CAP:
                            break
                        people, unlock_credits = search_people(
                            search_name, account.get("domain",""),
                            titles, apollo_key, max_results=2,
                            priority=persona.get("priority","Warm")
                        )
                        acct_unlock_credits += unlock_credits
                        empty_streak = 0 if people else empty_streak + 1
                        for person in people:
                            name = person.get("name","")
                            if name and name not in seen_names:
                                new_hire, hire_date = is_new_hire(person)
                                all_leads.append({
                                    "name":         name,
                                    "title":        person.get("title",""),
                                    "email":        person.get("email",""),
                                    "email_status": person.get("email_status",""),
                                    "linkedin":     person.get("linkedin_url",""),
                                    "seniority":    person.get("seniority",""),
                                    "priority":     persona.get("priority","Warm"),
                                    "role_type":    persona.get("role_type",""),
                                    "new_hire":     new_hire,
                                    "hire_date":    hire_date or "",
                                })
                                seen_names.add(name)

                    all_leads.sort(key=lambda l: {"Hot":0,"Warm":1,"Cold":2}.get(l.get("priority","Cold"),2))
                    s3_run_credits += acct_unlock_credits

                    s3.append({
                        **account,
                        "buying_committee":  committee,
                        "outreach_angle":    p_data.get("outreach_angle",""),
                        "why_now":           p_data.get("why_now",""),
                        "value_pillar":      p_data.get("value_pillar",""),
                        "leads":             all_leads,
                        "unlock_credits":    acct_unlock_credits,
                    })
                    progress.progress((i+1)/len(tier_a))

                s3.sort(key=lambda x: x.get("final_score",0), reverse=True)
                st.session_state.stage3_results  = s3
                st.session_state.stage           = 3
                st.session_state.s3_run_credits  = s3_run_credits
                total_leads = sum(len(r["leads"]) for r in s3)
                status.success(f"✅ Lead intelligence complete — {total_leads} leads found across {len(s3)} accounts · 🔋 {s3_run_credits} Apollo unlock credits used.")
                st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# STAGE 3 — Lead Intelligence Results
# ════════════════════════════════════════════════════════════════════════════════
if st.session_state.stage >= 3 and st.session_state.stage3_results:
    st.markdown("---")
    st.subheader("👥 Stage 3 — Lead Intelligence")

    s3 = st.session_state.stage3_results
    tier_icon = {"A Strategic":"🟢","B Prime":"🔵","C Monitor":"🟡"}

    total_leads   = sum(len(r.get("leads",[])) for r in s3)
    total_credits = sum(r.get("unlock_credits",0) for r in s3)
    s3c1, s3c2, s3c3 = st.columns(3)
    s3c1.metric("Accounts",       len(s3))
    s3c2.metric("Total Leads",    total_leads)
    s3c3.metric("🔋 Unlock Credits", total_credits)

    for r in s3:
        icon    = tier_icon.get(r.get("final_tier",""),"⚪")
        credits = r.get("unlock_credits", 0)
        with st.expander(f"{icon} **{r['company']}** · Score {r['final_score']} · {r['account_type']} · 🔋 {credits} credits"):
            c1, c2 = st.columns([1,2])
            with c1:
                st.markdown(f"""
**Tier:** {r.get('final_tier','')}
**Industry:** {r.get('industry','')}
**Angle:** {r.get('outreach_angle','')}
**Why Now:** {r.get('why_now','')}
**Value Pillar:** {r.get('value_pillar','')}
""")
            with c2:
                leads = r.get("leads",[])
                if leads:
                    for lead in leads:
                        p_icon = "🔴" if lead.get("priority")=="Hot" else "🟡" if lead.get("priority")=="Warm" else "🟢"
                        ln = f" [LinkedIn]({lead['linkedin']})" if lead.get("linkedin") else ""
                        em = f" `{lead['email']}`" if lead.get("email") else ""
                        new_badge = f" 🆕 *hired {lead['hire_date']}*" if lead.get("new_hire") else ""
                        st.markdown(f"{p_icon} **{lead.get('name','—')}** · {lead.get('title','')}{new_badge}{em}{ln}")
                else:
                    st.caption("No leads found via Apollo.")

    if st.session_state.stage == 3:
        if st.button("✉️ Generate Outreach Intelligence — Stage 4", type="primary"):
            progress  = st.progress(0)
            status    = st.empty()
            leads_data = [{
                "company":        r["company"],
                "industry":       r["industry"],
                "account_type":   r["account_type"],
                "outreach_angle": r.get("outreach_angle",""),
                "why_now":        r.get("why_now",""),
                "value_pillar":   r.get("value_pillar",""),
                "hot_leads":      [l for l in r.get("leads",[]) if l.get("priority")=="Hot"],
            } for r in s3]

            status.text("✉️ Claude generating personalized outreach...")
            batches = [leads_data[i:i+BATCH_SIZE] for i in range(0,len(leads_data),BATCH_SIZE)]
            s4 = []
            for i, batch in enumerate(batches):
                s4.extend(claude_stage4(batch, anthropic_key))
                progress.progress((i+1)/len(batches))

            st.session_state.stage4_results = s4
            st.session_state.stage          = 4
            status.success(f"✅ Outreach intelligence complete — {len(s4)} sequences generated.")
            st.rerun()


# ════════════════════════════════════════════════════════════════════════════════
# STAGE 4 — Outreach + Downloads
# ════════════════════════════════════════════════════════════════════════════════
if st.session_state.stage >= 4:
    st.markdown("---")
    st.subheader("✉️ Stage 4 — Outreach Intelligence")

    s4 = st.session_state.stage4_results
    if s4:
        for r in s4:
            with st.expander(f"✉️ **{r.get('lead_name','?')}** at {r.get('company','?')} — {r.get('lead_title','')}"):
                st.markdown(f"**Opening:**\n\n{r.get('email_opener','')}")
                st.markdown(f"**Why Now:** {r.get('why_now_hook','')}")
                st.markdown(f"**Sequence:** {r.get('sequence','')}")
    else:
        st.info("No Hot leads found — outreach sequences require at least one Hot lead per account.")

    # ── Downloads ──────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📥 Download Results")
    today = datetime.now().strftime("%Y-%m-%d")

    s1 = st.session_state.stage1_results
    s2 = st.session_state.stage2_results
    s3 = st.session_state.stage3_results

    col_xl, col_json, col_html = st.columns(3)

    with col_xl:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            # Accounts — sorted by final score
            acc = sorted(s2 if s2 else s1,
                         key=lambda x: x.get("final_score", x.get("score",0)), reverse=True)
            pd.DataFrame(acc).to_excel(writer, sheet_name="Accounts", index=False)
            # Leads
            if s3:
                lead_rows = []
                for r in sorted(s3, key=lambda x: x.get("final_score",0), reverse=True):
                    for lead in r.get("leads",[]):
                        lead_rows.append({"Company":r["company"],
                                          "Final Score":r.get("final_score",0), **lead})
                pd.DataFrame(lead_rows).to_excel(writer, sheet_name="Leads", index=False)
            # Outreach
            if s4:
                pd.DataFrame(s4).to_excel(writer, sheet_name="Outreach", index=False)
        st.download_button("📥 Excel (3 sheets)", data=buf.getvalue(),
                           file_name=f"hunting_machine_{today}.xlsx")

    with col_json:
        export = {
            "generated": today,
            "accounts": sorted(s2 if s2 else s1,
                               key=lambda x: x.get("final_score", x.get("score",0)), reverse=True),
            "leads":    sorted(s3, key=lambda x: x.get("final_score",0), reverse=True) if s3 else [],
            "outreach": s4,
        }
        st.download_button("📥 JSON",
                           data=json.dumps(export, ensure_ascii=False, indent=2).encode(),
                           file_name=f"hunting_machine_{today}.json",
                           mime="application/json")

    with col_html:
        html = generate_html(s1, s2, s3, s4, today)
        st.download_button("📥 HTML",
                           data=html.encode(),
                           file_name=f"hunting_machine_{today}.html",
                           mime="text/html")

    st.markdown("---")
    if st.button("🔄 Start New Run", type="secondary"):
        for k, default in [
            ("stage",0), ("prefilter_done",False), ("prefilter_results",[]),
            ("company_sources",{}), ("stage1_results",[]), ("stage2_results",[]),
            ("stage3a_results",{}), ("stage3_results",[]), ("stage4_results",[]),
        ]:
            st.session_state[k] = default
        st.rerun()
