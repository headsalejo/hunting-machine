"""
Run Stage 3a + 3b for specific accounts from the command line.
Usage: python3 run_s3b.py
"""
import sys, types, json

# ── Stub out streamlit so app.py can be imported ──────────────────────────────
st_stub = types.ModuleType("streamlit")
for attr in ["session_state","secrets","cache_data","cache_resource",
             "set_page_config","title","subheader","caption","write","info",
             "warning","error","success","button","expander","columns",
             "sidebar","text_input","selectbox","file_uploader","progress",
             "spinner","status","metric","markdown","divider","stop","rerun",
             "dataframe","empty","container"]:
    setattr(st_stub, attr, lambda *a, **kw: None)
# session_state needs attribute-style access (st.session_state.foo)
class _State(dict):
    def __getattr__(self, name):
        return self.get(name)
    def __setattr__(self, name, val):
        self[name] = val
st_stub.session_state = _State()

# sidebar and other objects need method-style access
class _Stub:
    def __getattr__(self, name):
        return lambda *a, **kw: None
    def __call__(self, *a, **kw):
        return self
    def __enter__(self):
        return self
    def __exit__(self, *a):
        pass
class _SidebarStub(_Stub):
    def text_input(self, *a, **kw):
        return ""  # blank sidebar fields — no filter, no single-account override
st_stub.sidebar = _SidebarStub()
st_stub.status  = _Stub

sys.modules["streamlit"] = st_stub

# pandas stub (only needed for display, not logic)
try:
    import pandas
except ImportError:
    pd_stub = types.ModuleType("pandas")
    pd_stub.DataFrame = list
    sys.modules["pandas"] = pd_stub

import os
sys.path.insert(0, os.path.dirname(__file__))
from app import (claude_stage3a, is_new_hire, apollo_post, unlock_person,
                 _email_domain_matches, FALLBACK_HOT_TITLES, FALLBACK_WARM_TITLES,
                 APOLLO_DELAY, SENIORITY_BY_PRIORITY)
import time

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
APOLLO_KEY    = os.environ["APOLLO_API_KEY"]

# ── Patched search_people with 4 new guards ───────────────────────────────────
def _is_personal_linkedin(linkedin_url, company_name):
    """Return False if the LinkedIn slug looks like a company page rather than a person."""
    if not linkedin_url:
        return True
    slug = linkedin_url.rstrip('/').split('/')[-1].lower()
    company_slug = company_name.lower().replace(' ', '-')
    return slug != company_slug

def search_people_v2(company_name, domain, titles, key, max_results=2, priority="Warm"):
    contacts, seen = [], set()

    def _run_search(payload):
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

    # Pass 1 — title search, only candidates with has_email=True (FIX 1)
    for p in _run_search({"page": 1, "per_page": max_results + 4, "person_titles": titles[:10]}):
        if p.get("id") not in seen and p.get("has_email"):
            contacts.append(p)
            seen.add(p["id"])

    # Pass 2 — seniority fallback, same has_email filter (FIX 1)
    if not contacts:
        seniority = SENIORITY_BY_PRIORITY.get(priority, ["director"])
        for p in _run_search({"page": 1, "per_page": max_results + 4, "person_seniority": seniority}):
            if p.get("id") not in seen and p.get("has_email"):
                contacts.append(p)
                seen.add(p["id"])

    # Unlock + validate
    verified, seen_names, unlock_credits = [], set(), 0
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
            print(f"    [skip] {name} — no longer at company (current=False)")
            continue

        email = unlocked.get("email") or ""
        if not _email_domain_matches(email, domain):
            continue
        if not email:
            current_employer = emp_history[0].get("organization_name", "")
            if current_employer and company_name.lower() not in current_employer.lower() \
               and current_employer.lower() not in company_name.lower():
                continue

        # FIX 3: discard company-page LinkedIn URLs
        linkedin = unlocked.get("linkedin_url", "")
        if not _is_personal_linkedin(linkedin, company_name):
            print(f"    [skip] {name} — LinkedIn URL looks like company page ({linkedin})")
            continue

        verified.append(unlocked)
        seen_names.add(name)

    return verified, unlock_credits

# ── Full pipeline for a single company ───────────────────────────────────────
def run_full_pipeline(company_name):
    from app import (claude_prefilter, claude_stage1, claude_resolve_names,
                     enrich_org, score_apollo, claude_stage3a,
                     is_new_hire, FALLBACK_HOT_TITLES, FALLBACK_WARM_TITLES,
                     score_to_tier, APOLLO_DELAY)

    print(f"\n{'='*60}")
    print(f"  {company_name} — Full Pipeline")
    print(f"{'='*60}")

    # Pre-filter (skipped)
    print("\n[Pre-filter] Skipped — running full pipeline regardless.")

    # Stage 1
    print("\n[Stage 1] Scoring with Claude...")
    s1 = claude_stage1([company_name], ANTHROPIC_KEY)
    s1r = s1[0] if s1 else {}
    stage1_score = s1r.get('score', 0)
    print(f"  Industry  : {s1r.get('industry','?')}")
    print(f"  Score     : {stage1_score} → {s1r.get('tier','?')}")
    print(f"  Narrative : {s1r.get('narrative','')}")
    if s1r.get('trigger_events'):
        print(f"  Triggers  : {', '.join(s1r['trigger_events'])}")

    # Stage 2 — name resolution + Apollo enrichment
    print("\n[Stage 2] Resolving name + Apollo enrichment...")
    resolved_list = claude_resolve_names([company_name], ANTHROPIC_KEY)
    resolved = resolved_list[0] if resolved_list else {"company": company_name, "canonical_name": company_name, "domain": "", "alt_names": []}
    print(f"  Canonical : {resolved.get('canonical_name')} | Domain: {resolved.get('domain')} | Alts: {resolved.get('alt_names')}")

    apollo_org = enrich_org(resolved, APOLLO_KEY)
    if apollo_org:
        print(f"  Apollo hit: {apollo_org.get('_resolved_name')} | Domain: {apollo_org.get('primary_domain','')} | Credits: {apollo_org.get('_enrich_credits',0)}")
    else:
        print("  Apollo    : No confident match found (domain mismatch or no results) — Stage 1 score stands")

    s2 = score_apollo(stage1_score, apollo_org)
    domain       = s2.get('domain') or resolved.get('domain','')
    account_type = s2.get('account_type','Unknown')
    final_score  = s2.get('final_score', stage1_score)
    final_tier   = score_to_tier(final_score)
    technologies = s2.get('technologies', [])
    crm_job_titles = s2.get('crm_job_titles', [])
    trigger_events = s1r.get('trigger_events', [])

    print(f"  Apollo bonus  : +{s2.get('bonus',0)} → Final score: {final_score} ({final_tier})")
    print(f"  Account type  : {account_type}")
    print(f"  Domain        : {domain}")
    print(f"  Technologies  : {', '.join(technologies[:5]) or 'None'}")

    # Stage 3a
    print("\n[Stage 3a] Defining buying committee with Claude...")
    account = {
        "company": company_name, "industry": s1r.get('industry','Unknown'),
        "account_type": account_type, "final_score": final_score,
        "technologies": technologies, "crm_job_titles": crm_job_titles,
        "trigger_events": trigger_events, "domain": domain,
    }
    s3a = claude_stage3a([account], ANTHROPIC_KEY)
    result = s3a[0] if s3a else {}
    committee = result.get('buying_committee', [])
    print(f"  Outreach angle : {result.get('outreach_angle','—')}")
    print(f"  Why now        : {result.get('why_now','—')}")
    print(f"  Value pillar   : {result.get('value_pillar','—')}")
    print(f"  Committee      : {len(committee)} persona(s)")
    for p in committee:
        print(f"    [{p.get('priority')}] {p.get('persona')} — {p.get('why','')}")

    # Stage 3b
    search_personas = committee if committee else [
        {"priority": "Hot",  "role_type": "Power Lead",   "search_titles": FALLBACK_HOT_TITLES},
        {"priority": "Warm", "role_type": "Sponsor Lead", "search_titles": FALLBACK_WARM_TITLES},
    ]
    if not committee:
        print("\n  [!] No buying committee — using fallback title search")

    search_name = resolved.get('canonical_name') or company_name

    # Pre-check: does Apollo have any people indexed for this account? (free call)
    try:
        probe = apollo_post("mixed_people/api_search",
                            {"q_organization_name": search_name, "page": 1, "per_page": 1},
                            APOLLO_KEY)
        if not probe.get("people"):
            print(f"\n  [!] No people indexed in Apollo for '{search_name}' — skipping Stage 3b, 0 credits spent.")
            return
    except Exception:
        pass  # probe failed — proceed anyway

    all_leads, total_credits = [], 0
    seen_names = set()
    empty_streak = 0
    for persona in search_personas:
        titles = persona.get('search_titles', [])
        if not titles:
            continue
        if empty_streak >= 3:
            print(f"  [!] 3 consecutive empty personas — stopping early.")
            break
        priority = persona.get('priority', 'Warm')
        print(f"\n[Stage 3b] Searching {priority} persona: {persona.get('persona','Fallback')}...")
        people, credits = search_people_v2(
            search_name, domain, titles, APOLLO_KEY, max_results=2, priority=priority
        )
        empty_streak = 0 if people else empty_streak + 1
        total_credits += credits
        for person in people:
            name = person.get('name','')
            if name and name not in seen_names:
                seen_names.add(name)
                new_hire, hire_date = is_new_hire(person)
                lead = {
                    "name": name, "title": person.get('title',''),
                    "email": person.get('email',''), "email_status": person.get('email_status',''),
                    "linkedin": person.get('linkedin_url',''),
                    "priority": priority, "new_hire": new_hire, "hire_date": hire_date,
                }
                all_leads.append(lead)
                flag = " 🆕 NEW HIRE" if new_hire else ""
                print(f"  ✓ [{priority}] {lead['name']} — {lead['title']}{flag}")
                print(f"      Email   : {lead['email']} ({lead['email_status']})")
                print(f"      LinkedIn: {lead['linkedin']}")

    print(f"\n  Total leads: {len(all_leads)} | Unlock credits: {total_credits}")

# ── Account definitions ───────────────────────────────────────────────────────
ACCOUNTS = [
    {
        "company":       "PIKOLINOS",
        "industry":      "Retail/Omnichannel",
        "account_type":  "Existing Business",
        "final_score":   48,
        "technologies":  ["Demandware", "CloudFlare", "Facebook", "Apple Pay"],
        "crm_job_titles":[],
        "trigger_events":["Active international e-commerce expansion",
                          "Digital transformation and DTC strategy investment"],
        "domain":        "pikolinos.com",
    },
    {
        "company":       "Galderma",
        "industry":      "Unknown",
        "account_type":  "Unknown",
        "final_score":   0,
        "technologies":  [],
        "crm_job_titles":[],
        "trigger_events":[],
        "domain":        "",
    },
    {
        "company":       "Dex Tools",
        "industry":      "Unknown",
        "account_type":  "Unknown",
        "final_score":   0,
        "technologies":  [],
        "crm_job_titles":[],
        "trigger_events":[],
        "domain":        "",
    },
]

def run(account):
    name = account["company"]
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    # Stage 3a — buying committee
    print("\n[Stage 3a] Defining buying committee with Claude...")
    s3a = claude_stage3a([account], ANTHROPIC_KEY)
    result = s3a[0] if s3a else {}
    committee = result.get("buying_committee", [])

    print(f"  Outreach angle : {result.get('outreach_angle','—')}")
    print(f"  Why now        : {result.get('why_now','—')}")
    print(f"  Value pillar   : {result.get('value_pillar','—')}")
    print(f"  Committee      : {len(committee)} persona(s)")
    for p in committee:
        print(f"    [{p.get('priority')}] {p.get('persona')} — {p.get('why','')}")

    # Stage 3b — people search
    search_personas = committee if committee else [
        {"priority": "Hot",  "role_type": "Power Lead",   "search_titles": FALLBACK_HOT_TITLES},
        {"priority": "Warm", "role_type": "Sponsor Lead", "search_titles": FALLBACK_WARM_TITLES},
    ]
    if not committee:
        print("\n  [!] No buying committee — using fallback title search")

    all_leads, total_credits = [], 0
    for persona in search_personas:
        titles = persona.get("search_titles", [])
        if not titles:
            continue
        priority = persona.get("priority", "Warm")
        print(f"\n[Stage 3b] Searching {priority} persona: {persona.get('persona', 'Fallback')}...")
        people, credits = search_people(
            account["company"], account.get("domain", ""),
            titles, APOLLO_KEY, max_results=2, priority=priority
        )
        total_credits += credits
        for person in people:
            new_hire, hire_date = is_new_hire(person)
            lead = {
                "name":         person.get("name", ""),
                "title":        person.get("title", ""),
                "email":        person.get("email", ""),
                "email_status": person.get("email_status", ""),
                "linkedin":     person.get("linkedin_url", ""),
                "priority":     priority,
                "new_hire":     new_hire,
                "hire_date":    hire_date,
            }
            all_leads.append(lead)
            flag = " 🆕 NEW HIRE" if new_hire else ""
            print(f"  ✓ [{priority}] {lead['name']} — {lead['title']}{flag}")
            print(f"      Email   : {lead['email']} ({lead['email_status']})")
            print(f"      LinkedIn: {lead['linkedin']}")
        time.sleep(APOLLO_DELAY)

    print(f"\n  Total leads: {len(all_leads)} | Unlock credits used: {total_credits}")
    return {"company": name, "leads": all_leads, "unlock_credits": total_credits,
            "outreach_angle": result.get("outreach_angle",""),
            "why_now": result.get("why_now",""),
            "value_pillar": result.get("value_pillar","")}

if __name__ == "__main__":
    for company in ["Pikolinos", "Qualits Energy"]:
        run_full_pipeline(company)
