# Hunting Machine — Rules & Playbook

---

## PRE-FILTER — Binary Qualification (before Stage 1)

### Purpose
Eliminate obvious low-potential accounts before any scoring tokens are spent.
One single Claude call processes the entire uploaded list.

### Discard Criteria (any one is enough to discard)
- Fewer than 600 employees (estimated)
- No decision-making presence in Spain or Portugal
- Industry with no CRM/SaaS transformation potential (e.g. public administration, micro-retail, NGO, agriculture)
- Company name unrecognisable or clearly not an enterprise

### Output per account
- `keep` — proceeds to Stage 1 full scoring
- `discard` — removed from pipeline, shown in a collapsible log with restore option

### Design principles
- One API call for the full list (not batched) — cheapest possible
- No scoring, no narrative, binary only
- AE can review and restore any discarded account before proceeding

---

## STAGE 1 — Claude First Tiering

### 🎯 Target Profile
- 600+ employees
- Decision-making power in Spain or Portugal
- CRM / SaaS transformation potential
- Strong bias toward Consumer Goods & Retail

### Batch size: 10 companies per Claude call

### 🏢 Account Scoring (Max 50 Points)

**1. Company Size**
| Criteria | Score |
|----------|-------|
| 600–1500 employees | +8 |
| 1500–5000 employees | +12 |
| 5000+ employees | +15 |

**2. Industry Fit**
| Industry | Score |
|----------|-------|
| Consumer Goods | +15 |
| Retail (Omnichannel, eCommerce heavy) | +15 |
| Financial Services | +10 |
| Telco | +10 |
| Pharma | +8 |
| Manufacturing | +8 |
| Other | +5 |

**3. Spain / Portugal Sovereignty Layer**
| Criteria | Score |
|----------|-------|
| HQ in Spain or Portugal | +12 |
| Iberia is main revenue center | +8 |
| Spanish or Portuguese executive committee | +8 |
| International HQ outside Iberia | +0 |

**4. Tech Stack / CRM Signals (from training knowledge)**
| Signal | Score |
|--------|-------|
| Using Salesforce | +8 |
| Using Dynamics / SAP CRM | +6 |
| Hiring CRM-related roles | +8 |
| Commerce Cloud presence | +6 |

### 🚦 Account Tiering
| Score | Tier |
|-------|------|
| 35+ | A Strategic |
| 25–34 | B Prime |
| 15–24 | C Monitor |
| <15 | Low Priority |

### ⚡ Trigger Events (note if detected — do NOT add points in Stage 1)
| Trigger | Note |
|---------|------|
| New CIO / CDO / CCO hired recently | Flag — confirmed in Stage 3b via Apollo hire date |
| Recent funding round | Flag — confirmed in Stage 2 via Apollo funding_events[] |
| Active CRM job postings | Flag — confirmed in Stage 2 via Apollo job_postings[] |
| Competitor CRM confirmed | Flag as displacement priority |

---

## STAGE 1 → STAGE 2 GATE — Manual Review

Before Apollo credits are spent, accounts must be reviewed:
- AE confirms or overrides Claude tier via the Override Tier dropdown
- Manager promotes/demotes accounts based on relationship context
- Accounts flagged as "Remove" are excluded
- **Only accounts with Override Tier = A Strategic or B Prime proceed to Apollo**
- Accounts scoring ≥ 28 default to their Claude tier in the Override column
- Accounts scoring 25–27 default to C Monitor — AE must explicitly promote to B Prime or A Strategic to include them in Apollo

---

## STAGE 2 — Apollo Name Resolution (before enrichment)

### Problem
AEs upload Spanish company names. Apollo indexes international/English legal names.
Direct string matching fails for a significant portion of Iberian accounts.

| AE Upload | Apollo Record |
|-----------|--------------|
| Grupo Mahou San Miguel | Mahou-San Miguel Group |
| El Corte Inglés | El Corte Ingles |
| Clínica Baviera | Baviera Group |
| Laboratorios Rovi | Rovi Pharmaceuticals |

### Solution — Claude Semantic Name Resolution
Before any Apollo search, send all company names to Claude in a single batch call.
Claude resolves each company to its most likely Apollo-indexed form:
- `canonical_name` — international/English legal name
- `domain` — primary web domain (e.g. mahou.es, mango.com)
- `alt_names` — up to 2 fallback variants

### Search Strategy (per account)
1. Try `canonical_name` first
2. Try each `alt_name` in order
3. Try original AE-uploaded name last
4. Stop at first Apollo hit — record which name matched
5. If all fail → no Apollo data, Stage 1 score stands

### Design Principles
- One Claude batch call for all accounts — minimal token cost (~$0.01–0.02 per run)
- Semantic resolution: Claude understands the entity, not just the string
- Every resolved name and matched name stored for auditability
- Graceful fallback — a failed resolution never blocks the pipeline

---

## STAGE 2 — Apollo Enrichment (opportunistic layer)

### Apollo coverage note
Apollo data quality for the Iberian market is variable. If Apollo returns no
data for an account, the Stage 1 score stands unchanged. Apollo is a bonus
layer, not a dependency.

### Apollo Signals Used
| Signal | Apollo Field | Bonus |
|--------|-------------|-------|
| Salesforce confirmed in tech stack | `technology_names` | +8 → flag Existing Business |
| Competitor CRM in tech stack | `technology_names` | +3 → flag Displacement |
| Recent funding (last 18 months) | `funding_events[]` | +5 |
| Active CRM job postings | `job_postings[]` | +3 |
| Headcount confirmed | `estimated_num_employees` | +2 |

### Account Type Classification (from Apollo)
| Signal | Classification |
|--------|---------------|
| Salesforce in tech stack confirmed | Existing Business → Expansion angle |
| Competitor CRM in tech stack | Green Field → Displacement angle |
| No CRM detected / Apollo no data | Green Field → Transformation angle (default) |

---

## STAGE 3a — Claude Lead Intelligence

For each Tier A Strategic account, Claude defines the ideal buying committee
using the full Apollo context (tech stack, job postings, funding, employees).

### Buying Committee Structure
| Role | Type | Priority |
|------|------|----------|
| Economic Buyer (CEO / CFO / CCO) | Power Lead | 🔴 Hot |
| Technical Evaluator (CIO / CTO / IT Director) | Power Lead | 🔴 Hot |
| Internal Champion (CRM / Digital / Sales Ops Director) | Sponsor Lead | 🟡 Warm |
| End User Influencer (Sales Manager / Marketing Manager) | Nurture Lead | 🟢 Cold |

### Lead Classification Rules
**🔴 Hot Lead** — Target for direct outreach
- C-Level with P&L or transformation ownership
- Active CRM or digital mandate visible on LinkedIn
- Recently hired into a transformation role (last 12 months)

**🟡 Warm Lead** — Target as internal champion / coach
- Director or senior manager level
- Owns CRM, digital, ecommerce, or sales operations
- Build relationship before engaging the economic buyer

**🟢 Cold / Nurture Lead** — Add to sequence, do not cold call
- Manager level or below
- Operational role, not a decision maker

### Outreach Angle by Account Type
| Account Type | Angle | Opening Hook |
|-------------|-------|-------------|
| Green Field — no CRM | Transformation | "You're managing [X] customers across [Y] channels without a unified CRM — here's what that's costing you" |
| Green Field — competitor CRM | Displacement | "Companies your size that moved from [Dynamics/HubSpot] to Salesforce saw X% improvement in [metric]" |
| Existing Business — basic usage | Expansion | "You're using [product] — here's what the next layer unlocks for your team" |
| Existing Business — growth signals | Agentforce / Data | "With your headcount growth, Agentforce could automate X hours of rep time per week" |

---

## STAGE 3b — Apollo Lead Nurturing

Apollo people search runs against the buying committee personas defined in Stage 3a.

### Search Priorities
1. Search for **Hot Leads** first (C-Level, economic buyer, technical evaluator)
2. Search for **Warm Leads** second (sponsor level, CRM/digital owners)
3. Only search for **Cold Leads** if Hot and Warm are insufficient

### Data Fields to Capture per Lead
- Full name, exact title
- LinkedIn URL
- Email + email status (verified / unverified)
- Seniority level
- Employment start date → detect new hire (within 6 months = 🆕 flag)

### Lead Quality Filter
- Prefer email_status = "verified" or "likely to engage"
- Deprioritise leads with no LinkedIn URL
- Flag leads hired in last 6 months as **high priority** — new decision-maker window

---

## STAGE 4 — Outreach Intelligence (Claude)

For each confirmed Hot Lead, Claude generates:
- A personalized 3-line opening for email / LinkedIn
- A "why now" hook based on trigger events from Stages 1–2 and new hire flag from Stage 3b
- A Salesforce value pillar mapped to their role and industry
- A suggested sequence: LinkedIn connect → email → call

### Why Now Hooks (priority order)
1. 🆕 New CxO hire at the account (highest urgency — "new broom" window)
2. Recent funding round
3. Active CRM / digital job postings confirmed by Apollo
4. Competitor CRM confirmed (displacement urgency)
5. Industry peer case study (social proof)
6. Event / conference recently attended

---

## World Class Additions (Roadmap)

- **Partner warm path**: flag accounts where a Salesforce SI partner has an existing relationship
- **Fiscal timing**: Iberian companies' budget cycles vary by sector; flag accounts in planning season vs freeze
- **Mutual connections**: LinkedIn 1st/2nd degree connections between AE and target lead
- **Bombora / G2 intent data**: layer in 3rd-party intent signals for accounts actively researching CRM
- **Agentforce signal**: companies with large sales/service teams + AI hiring = prime Agentforce target
- **Competitive win/loss patterns**: flag accounts where Salesforce won deals with similar companies
