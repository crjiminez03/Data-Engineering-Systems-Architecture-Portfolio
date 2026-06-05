# Client Resource Agent — Architecture & Design Document
# Portfolio Reference: Copilot Studio Agent Projects
# Project Type: Microsoft Copilot Studio — Dual-Agent System
# Platform: Microsoft Teams, SharePoint, Microsoft App Store
# Licensing Model: Pay-As-You-Go (no Premium end-user licenses required)

---

## Project Overview

A dual-agent system built in Microsoft Copilot Studio to assist healthcare
and social service providers in connecting clients to community resources
during appointments. Prior to this solution, providers spent significant
appointment time manually searching through Excel spreadsheets, PDFs,
and printed flyers to locate relevant contact information, demographics,
and eligibility requirements for facilities.

**Problem Solved:**
- Providers lost billable appointment time searching static documents
- Resource lists were frequently outdated — phone numbers, hours, and
  eligibility criteria changed without the document being updated
- No consistent categorization made it hard to quickly filter by need type
- Resources were siloed in Excel files not accessible from a mobile/Teams context

**Solution:**
Two cooperating Copilot Studio agents:
1. **Client Resource Agent** — Provider-facing. Answers "what resources are
   available for this client?" in real time during or before appointments.
2. **Resource Verification Agent** — Web-facing. Scrubs the knowledge source
   document to detect outdated contact info, demographics, and hours —
   reports what is incorrect, what it should be, and where it verified from.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     PROVIDER INTERACTION LAYER                       │
│         Microsoft Teams Channel │ SharePoint Embedded │ MS App Store │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │   CLIENT RESOURCE AGENT  │  (Agent 1 — Internal)
          │   Copilot Studio         │
          │                          │
          │  Topics:                 │
          │  • Greeting / Triage     │
          │  • Resource Search       │
          │  • Category Filter       │
          │  • Contact Lookup        │
          │  • Eligibility Check     │
          │  • Export / Share        │
          └────────────┬────────────┘
                       │ Knowledge Source Query
                       ▼
          ┌────────────────────────────┐
          │  KNOWLEDGE SOURCE          │
          │  SharePoint-hosted Excel   │
          │  • Facility name           │
          │  • Service category tags   │
          │  • Contact info            │
          │  • Address / demographics  │
          │  • Eligibility criteria    │
          │  • Flyer links             │
          │  • Last verified date      │
          └────────────┬───────────────┘
                       │
          ┌────────────▼────────────┐
          │  RESOURCE VERIFICATION   │  (Agent 2 — Web-Facing)
          │  AGENT                   │
          │  Copilot Studio          │
          │                          │
          │  Capabilities:           │
          │  • Bing / web search     │
          │  • Knowledge doc scrub   │
          │  • Discrepancy report    │
          │  • Verification source   │
          └──────────────────────────┘
```

---

## Agent 1: Client Resource Agent

### Purpose
Real-time resource lookup assistant for providers during client appointments.
Answers questions like:
- "What food assistance is available near zip code [00000]?"
- "Find DV shelters that accept children"
- "Show me all housing resources with walk-in hours"

### Knowledge Source Structure
The Excel knowledge base is hosted in SharePoint and connected as a
Copilot Studio knowledge source. It contains one row per facility with
the following columns:

| Column | Type | Description |
|--------|------|-------------|
| FacilityID | Text | Unique identifier |
| FacilityName | Text | Name of the organization |
| PrimaryCategory | Text | Top-level category (see Rule Engine) |
| SubCategories | Text | Pipe-delimited sub-tags e.g. "Food\|Walk-In\|Children" |
| Phone | Text | Primary contact phone |
| AlternatePhone | Text | Secondary contact |
| Email | Text | Contact email |
| Website | Text | Organization website |
| Address | Text | Physical address |
| City | Text | City |
| ZipCode | Text | Zip code |
| County | Text | County served |
| ServiceArea | Text | Geographic service area description |
| HoursOfOperation | Text | Operating hours |
| WalkInAvailable | Yes/No | Accepts walk-ins |
| AppointmentRequired | Yes/No | Requires appointment |
| EligibilityCriteria | Text | Who qualifies |
| AcceptsChildren | Yes/No | Services available for children |
| LanguagesSupported | Text | Languages served |
| DocumentsRequired | Text | What to bring |
| FlyerLink | URL | Link to printable flyer |
| LastVerifiedDate | Date | When info was last confirmed accurate |
| LastVerifiedBy | Text | Who or what verified it |
| Notes | Text | Additional provider notes |

---

## Rule Engine — Resource Categorization

The rule engine uses a combination of three mechanisms to categorize
and surface resources accurately:

### Mechanism 1: PrimaryCategory Tag (Explicit Classification)
Each facility is assigned one PrimaryCategory from a controlled vocabulary:

| Category | Description |
|----------|-------------|
| HOUSING | Emergency shelter, transitional housing, rental assistance |
| FOOD | Food pantries, meal programs, SNAP assistance |
| DOMESTIC_VIOLENCE | DV shelters, legal advocacy, safety planning |
| HOMELESSNESS | Street outreach, day shelters, drop-in centers |
| MENTAL_HEALTH | Counseling, crisis lines, psychiatric services |
| SUBSTANCE_USE | Detox, treatment, recovery support |
| LEGAL | Legal aid, immigration, tenant rights |
| EMPLOYMENT | Job training, resume help, placement services |
| TRANSPORTATION | Bus passes, medical transport, rideshare programs |
| CHILDCARE | Daycare subsidy, Head Start, after-school |
| HEALTHCARE | Free clinics, FQHC, dental, vision |
| FINANCIAL | Emergency funds, utility assistance, benefits navigation |
| EDUCATION | GED, ESL, vocational training |
| LGBTQ | LGBTQ-affirming services across categories |

### Mechanism 2: SubCategory Keyword Tags (Multi-Label)
Each facility can have multiple SubCategory tags enabling cross-category
filtering. Tags are stored pipe-delimited and matched against user queries:

```
Keywords → SubCategory Tags mapped at design time:

"walk in"        → Walk-In
"children"       → Children | Family
"spanish"        → Spanish | Bilingual
"24 hour"        → 24-Hour
"emergency"      → Emergency
"veterans"       → Veterans
"disabled"       → Disability
"seniors"        → Seniors
"no id required" → No-ID-Required
"transgender"    → LGBTQ-Affirming
"rental"         → Rental-Assistance
"utility"        → Utility-Assistance
"eviction"       → Eviction-Prevention
"domestic"       → DV
"shelter"        → Shelter
"food bank"      → Food-Bank
"meal"           → Hot-Meals
"snap"           → SNAP-Enrollment
"mental health"  → Mental-Health
"counseling"     → Counseling
"crisis"         → Crisis-Line
"detox"          → Detox
"sober"          → Recovery
```

### Mechanism 3: Agent Topic Routing Logic
The Copilot Studio agent uses topic trigger phrases and entity extraction
to map user intent to the correct category filter before querying the
knowledge source. This acts as the final routing layer:

```
User says:          → Agent extracts:      → Filters knowledge source by:
"food near me"      → FOOD + location      → PrimaryCategory=FOOD + ZipCode/County
"DV shelter"        → DOMESTIC_VIOLENCE    → PrimaryCategory=DOMESTIC_VIOLENCE
"housing help"      → HOUSING              → PrimaryCategory=HOUSING
"walk-in clinic"    → HEALTHCARE + Walk-In → PrimaryCategory=HEALTHCARE + WalkIn=Yes
"kids daycare"      → CHILDCARE + Children → PrimaryCategory=CHILDCARE + Children=Yes
"emergency shelter" → HOUSING/HOMELESSNESS → SubCategory=Emergency + Shelter
```

---

## Agent 1: Topic Flow Design

### Topic: Greeting & Triage
```
TRIGGER: Conversation start / "hello" / "help"

Bot: "Hi! I'm the Client Resource Assistant. I can help you find
      community resources for your client. What does your client need
      help with today?"

      [Quick reply buttons]:
      • Housing & Shelter
      • Food Assistance
      • Domestic Violence Resources
      • Homelessness Services
      • Mental Health Support
      • Financial Assistance
      • All Other Services

→ Routes to: Resource Search topic with category pre-set
```

### Topic: Resource Search
```
TRIGGER: Category selected OR free-text need description

ENTITIES EXTRACTED:
  - {Category}    — mapped via rule engine
  - {Location}    — zip code, city, or county
  - {SpecialNeeds} — children, walk-in, Spanish, 24hr etc.

KNOWLEDGE SOURCE QUERY:
  "Find {Category} resources in {Location} that offer {SpecialNeeds}"

Bot returns top 3-5 matches with:
  • Facility name
  • Phone number
  • Address
  • Hours
  • Walk-in availability
  • Eligibility summary

FOLLOW-UP:
  "Would you like more details, directions, or the printable flyer
   for any of these?"
```

### Topic: Contact Lookup
```
TRIGGER: "phone number for [facility]" / "how do I contact [name]"

Queries knowledge source by FacilityName
Returns: Phone, AlternatePhone, Email, Website, Address
```

### Topic: Eligibility Check
```
TRIGGER: "does [facility] accept [criteria]" / "do they take kids"

Extracts: FacilityName + eligibility question type
Returns: EligibilityCriteria, AcceptsChildren, DocumentsRequired,
         LanguagesSupported for the matched facility
```

### Topic: Export & Share
```
TRIGGER: "send this to me" / "share these results" / "print"

Offers:
  • Copy to clipboard (Teams message)
  • Open flyer link (FlyerLink URL)
  • Email summary (Power Automate flow trigger)
```

---

## Agent 2: Resource Verification Agent

### Purpose
Runs on a scheduled or on-demand basis to verify that the knowledge
source document is current and accurate. Uses web search capabilities
to cross-reference each facility's contact info and demographics against
live web sources. Produces a discrepancy report showing:
- What field is incorrect
- What the correct value appears to be
- Where the verification source came from (URL + date)

### Verification Scope
For each facility row in the knowledge source:
1. Search for the facility by name + city
2. Compare Phone, Email, Address, Hours against found web data
3. Check if the facility still exists (detect permanently closed resources)
4. Flag any field where the knowledge source value differs from web data
5. Record the source URL and retrieval date as evidence

### Discrepancy Report Output Format

```
RESOURCE VERIFICATION REPORT
Generated: [Date]
Agent: Resource Verification Agent
Knowledge Source: Client_Resources_Master.xlsx

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FACILITY: [Example Shelter Organization A]          ← fictional example
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ⚠ PHONE — POSSIBLY OUTDATED
    Current in doc:  (555) 555-0101
    Found online:    (555) 555-0199
    Source:          https://example-shelter-org-a.example.com/contact
    Retrieved:       2024-11-14
    Confidence:      High (official website match)

  ⚠ HOURS — POSSIBLY OUTDATED
    Current in doc:  Mon-Fri 8am-5pm
    Found online:    Mon-Sun 7am-9pm
    Source:          Google Business Profile
    Retrieved:       2024-11-14
    Confidence:      Medium (third-party listing)

  ✔ ADDRESS — VERIFIED
    Matches:         100 Example Blvd, Anytown, ST 00000
    Source:          https://example-shelter-org-a.example.com
    Retrieved:       2024-11-14

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FACILITY: [Example Food Assistance Organization B]   ← fictional example
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✖ FACILITY STATUS — PERMANENTLY CLOSED
    Note: Multiple sources indicate this location closed March 2024.
    Source:          https://example-nonprofit-directory.example.com/updates
    Action Required: Remove from knowledge source.

[Summary]
  Total facilities reviewed:  47
  Verified accurate:          31 (66%)
  Flagged for review:         12 (26%)
  Confirmed closed:            4 (8%)
```

### Verification Agent Topic: Run Verification
```
TRIGGER: "verify resources" / "check for outdated info" / scheduled run

FOR EACH facility in knowledge source:
  1. Web search: "{FacilityName} {City} {State} contact hours"
  2. Extract: phone, address, hours from top results
  3. Compare against knowledge source values
  4. Classify discrepancy confidence: High / Medium / Low
  5. Append to report

OUTPUT:
  • Discrepancy report posted to SharePoint
  • Summary card posted in Teams admin channel
  • Flagged rows highlighted for knowledge source editor to review
```

---

## Licensing Strategy — Pay-As-You-Go

### The Challenge
The organization had a mixed license environment:
- Most end users: Microsoft 365 Basic (no Copilot Studio access)
- Small subset: Microsoft 365 Premium

Upgrading all end users to Premium solely for agent access was not
cost-justified given usage patterns and budget constraints.

### Solution: Pay-As-You-Go (Copilot Studio Metered Billing)
Configured the agent to use Copilot Studio's Pay-As-You-Go metered
billing model connected to an Azure subscription, rather than requiring
per-user Copilot Studio Premium licenses.

**How it works:**
- Agent is published without requiring end users to hold Copilot Studio licenses
- Usage is billed per message/session to the Azure subscription
- Organization pays only for actual consumption
- End users on Basic licenses can interact with the agent via Teams and
  SharePoint without any license upgrade

**Cost model:**
| Billing Unit | Rate (approx.) |
|-------------|----------------|
| Per message (classic) | ~$0.01 per message |
| Per session (generative) | Varies by model tier |

**Why this was the right call:**
- Avoided ~$22/user/month Premium license cost for infrequent users
- Kept the agent accessible to all providers regardless of license tier
- Gave finance a predictable consumption-based cost model tied to actual use
- No license procurement delay — deployed immediately on Azure billing

---

## Deployment Configuration

### Channels Deployed
| Channel | Configuration |
|---------|--------------|
| Microsoft Teams | Published as a Teams app via Copilot Studio Teams channel |
| SharePoint | Embedded via SharePoint web part on the provider intranet page |
| Microsoft App Store (org) | Published to the organizational app store so any staff member could find and install via Teams Apps |

### Teams Deployment Steps (Reference)
1. In Copilot Studio → Publish → Teams channel → Enable
2. Download the Teams app manifest (.zip)
3. Submit to Teams Admin Center → Manage Apps → Upload custom app
4. Set availability: All users in org (or specific group)
5. Users find it under Apps → Built for your org → Install

### SharePoint Deployment Steps (Reference)
1. In Copilot Studio → Publish → SharePoint → Copy embed code
2. In SharePoint → Edit page → Add web part → Embed
3. Paste embed code → Publish page
4. Set page permissions to match provider group

### App Store (Org) Publication
1. Package the agent manifest from Teams Admin Center
2. Submit to Microsoft AppSource internal catalog
3. Approved apps appear in Teams → Apps → Built for [Org Name]
4. Any staff member can download without IT ticket

---

## Impact Summary

| Metric | Before | After |
|--------|--------|-------|
| Time to find a resource | 5-15 min manual search | < 30 seconds |
| Resource accuracy | Unverified, often stale | Verified on schedule |
| Provider adoption | N/A | All-staff accessible via Teams |
| License cost per user | Would have been $22/mo Premium | Pay-per-use (~$0.01/msg) |
| Knowledge source updates | Manual, ad-hoc | Verification agent flags automatically |
