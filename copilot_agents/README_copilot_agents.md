# Microsoft Copilot Studio Agents — Portfolio Overview
# Portfolio Reference: Copilot Studio Agent Projects
# Author: Christopher J.
# Platform: Microsoft Copilot Studio, Teams, SharePoint, MS Forms, Power Automate

---

## Overview

Two production Copilot Studio agents built to solve real operational
problems in a healthcare and social services organization. Both agents
were deployed to Microsoft Teams, SharePoint, and the organizational
Microsoft App Store — accessible by all staff without Premium license
upgrades through Pay-As-You-Go metered billing.

---

## Agent 1: Client Resource Agent

**Type:** Dual-agent system (internal + web-verification)
**Users:** Healthcare and social service providers
**Problem:** Providers spent 5–15 minutes per appointment manually
searching Excel files and printed flyers for client resource information.
Resources were frequently outdated with no systematic verification process.

### What It Does
- Answers provider questions about community resources in real time
  during client appointments
- Categorizes resources using a three-mechanism rule engine
  (explicit tags + keyword matching + topic routing)
- Returns contact info, eligibility, walk-in availability, and
  printable flyer links
- Flags entries not verified within 90 days
- A second verification agent uses web search to scrub the knowledge
  source and report what is outdated, what the correct value should be,
  and where the verification came from

### Key Design Decisions
| Decision | Why |
|----------|-----|
| Pay-As-You-Go billing | Avoided $22/user/month Premium license for infrequent users — metered billing on Azure subscription instead |
| Dual-agent architecture | Separated real-time lookup (speed) from verification (thoroughness) — different latency requirements |
| Rule engine (3 layers) | Category tags alone weren't specific enough; keyword matching caught modifiers; topic routing handled ambiguous queries |
| SharePoint-hosted knowledge source | Allowed knowledge editors to update Excel without touching the agent configuration |
| 90-day verification flag | Built-in data quality signal visible to providers without waiting for the verification agent to run |

### Files
| File | Description |
|------|-------------|
| `01_client_resource_agent_architecture.md` | Full system architecture, knowledge source schema, topic flow design, verification report format, licensing strategy, deployment steps |
| `02_system_prompts_topic_flows.py` | Complete system prompts for both agents + all topic flow pseudocode (Greeting, Search, Detail, Eligibility, Fallback, Verification) |
| `03_rule_engine_reference.py` | Python reference implementation of the categorization rule engine — category lookup table, subcategory tag matrix, conflict resolution, runnable test cases |

---

## Agent 2: HR Recognition Agent

**Type:** Single agent with MS Forms + Power Automate integration
**Users:** All staff (nominators), HR team, Recognition council
**Problem:** Employee recognition nominations were scored subjectively by
council members who naturally advocated for employees they knew personally,
creating familiarity bias regardless of demonstrated performance.

### What It Does
- Ingests Microsoft Forms nomination submissions automatically via
  Power Automate trigger
- Scores each nomination across 6 weighted competency dimensions
  (Impact 25%, Consistency 20%, Collaboration 20%, Initiative 15%,
  Values Alignment 10%, Client Impact 10%)
- Produces a structured score card with rubric level, rationale,
  and suggested council focus questions per nominee
- Provides an objective evidence-based score (0–100) before council
  members score independently — anchors discussion without overriding
  human judgment
- Sends confirmation to nominators and posts notifications to HR Teams channel

### Key Design Decisions
| Decision | Why |
|----------|-----|
| Weighted rubric (not LLM gut feel) | Defined scoring signals give the council confidence the agent isn't making arbitrary decisions |
| Agent scores evidence, not person | Nominee department, tenure, seniority never passed to the agent — scored solely on written nomination text |
| Council scores independently | Agent score is an anchor, not the verdict — preserves human judgment and council authority |
| Anonymous nominations supported | Nominator name is optional and not passed to the scoring agent — protects nominator and removes nominator bias |
| Suggested council focus areas | Turns gap dimensions into conversation starters — gives the council productive questions rather than just a number |
| MS Forms as input | No new system to learn for staff — widely understood tool with Power Automate integration built in |

### Files
| File | Description |
|------|-------------|
| `01_hr_recognition_agent_architecture.md` | Full architecture, MS Forms question design, scoring rubric table, scoring signals per dimension, system prompt, example score card output, Power Automate flow design, deployment + access control, non-bias safeguards |
| `02_scoring_engine_reference.py` | Python reference implementation of the weighted scoring engine — Nomination and ScoreCard dataclasses, dimension scoring logic, rubric level mapping, formatted score card output, runnable example |

---

## Shared Deployment Pattern

Both agents share the same deployment approach:

```
Copilot Studio
    │
    ├── Teams Channel → Published as Teams app → Submitted to Teams Admin
    │                   → Approved → Available in "Built for [Org]" apps
    │
    ├── SharePoint   → Embed web part → Published on intranet page
    │
    └── MS App Store → Org catalog → Any staff member can install
                       without IT ticket or license change
```

**Licensing:** Pay-As-You-Go (Azure metered billing)
- End users on Microsoft 365 Basic can access both agents
- No Copilot Studio Premium per-user license required for end users
- Billed per message/session to org Azure subscription
- Cost scales with actual usage — zero cost when not in use

---

## Skills Demonstrated

| Skill | Evidence |
|-------|----------|
| Copilot Studio agent design | Two production agents with multi-topic flows, knowledge sources, and web capabilities |
| Rule engine design | Three-layer categorization combining lookup tables, regex keyword matching, and intent routing |
| AI prompt engineering | System prompts with explicit behavioral guardrails, scoring rubrics, and output format requirements |
| Bias mitigation in AI systems | Structured scoring design that separates evidence from identity |
| Power Automate integration | Forms → Agent pipeline with confirmation emails and Teams notifications |
| Microsoft 365 licensing strategy | Pay-As-You-Go architecture enabling org-wide access without Premium license procurement |
| Multi-channel deployment | Teams + SharePoint + MS App Store with appropriate access scoping |
| Business process analysis | Translated provider pain points and council process into concrete agent behaviors |
