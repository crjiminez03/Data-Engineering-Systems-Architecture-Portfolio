# HR Recognition Agent — Architecture & Design Document
# Portfolio Reference: Copilot Studio Agent Projects
# Project Type: Microsoft Copilot Studio + MS Forms Integration
# Platform: Microsoft Teams, SharePoint, Microsoft App Store
# Purpose: Non-bias employee recognition scoring for council nomination process

---

## Project Overview

An AI-powered recognition agent built in Microsoft Copilot Studio that
processes employee nomination submissions from Microsoft Forms, applies a
weighted scoring rubric, and produces structured scoring reports for the
recognition council's nomination review sessions.

**Problem Solved:**
- Council nomination sessions were subject to personal familiarity bias —
  well-known employees received more advocacy regardless of demonstrated impact
- Scoring was inconsistent across council members with no shared rubric
- Manual tallying of nomination responses was time-consuming and error-prone
- Nominators had no confirmation their submission was received and processed

**Solution:**
A Copilot Studio agent that:
1. Ingests MS Forms nomination responses automatically via Power Automate
2. Applies a weighted scoring model across 6 competency dimensions
3. Produces a normalized score (0–100) per nominee
4. Presents the scored report to the council alongside the raw responses
5. Allows council members to apply their own score independently — the
   agent score serves as an objective anchor, not the final decision

**Design Principle — Non-Bias by Design:**
The agent scores the written content of the nomination, not the person.
It does not know or consider: tenure, department, seniority, gender,
or previous recognition history. Each nomination is scored on its
documented evidence alone.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    NOMINATION SUBMISSION LAYER                       │
│              Microsoft Forms — Employee Nomination Form              │
│         (Nominator fills out form describing nominee's impact)       │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Form submitted
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   POWER AUTOMATE FLOW                                │
│  Trigger: New MS Forms response                                      │
│  Actions:                                                            │
│    1. Get response details from Forms                                │
│    2. Parse all question/answer fields                               │
│    3. Post structured payload to HR Recognition Agent               │
│    4. Send confirmation email to nominator                           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Structured nomination payload
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│               HR RECOGNITION AGENT (Copilot Studio)                  │
│                                                                      │
│  Topics:                                                             │
│    • Process Nomination (ingests payload, runs scoring)              │
│    • Score Report (council-facing scored output)                     │
│    • Nomination Summary (all nominees ranked)                        │
│    • Council Query (ad-hoc questions about nominees)                 │
│    • Score Explanation (why a nominee scored X)                      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Scored results
                           ▼
┌────────────────────────────────┐  ┌──────────────────────────────┐
│  SharePoint List               │  │  Teams Channel               │
│  (scored nominations stored)   │  │  (council notification card) │
└────────────────────────────────┘  └──────────────────────────────┘
```

---

## Microsoft Forms — Nomination Form Design

The nomination form captures structured evidence across 6 scoring dimensions.
Each dimension has 1–2 questions. Answers are free-text (paragraph) to allow
the nominator to describe specific examples rather than selecting from a scale.

### Form Questions

**Section 1: Nominee Information**
- Q1: Nominee's full name
- Q2: Nominee's department / team
- Q3: Nominator's name (optional — nominations can be anonymous)
- Q4: Nomination period (month/quarter being recognized)

**Section 2: Impact (Weight: 25%)**
- Q5: Describe a specific situation where this person's actions had a
      measurable or meaningful impact on the team, clients, or organization.
      Please include what happened, what they did, and what the outcome was.

- Q6: How did their contribution go beyond what is typically expected of
      their role?

**Section 3: Consistency (Weight: 20%)**
- Q7: Describe how this person consistently demonstrates excellence.
      How often and over what period have you observed this behavior?

**Section 4: Collaboration & Teamwork (Weight: 20%)**
- Q8: Give an example of how this person supported, uplifted, or
      collaborated with colleagues. How did they contribute to
      team success rather than individual achievement?

**Section 5: Initiative & Innovation (Weight: 15%)**
- Q9: Describe a time this person took initiative, proposed an
      improvement, or found a creative solution to a challenge
      without being asked.

**Section 6: Values Alignment (Weight: 10%)**
- Q10: How does this person demonstrate the organization's core values
       in their day-to-day work? Provide a specific example.

**Section 7: Client / Community Impact (Weight: 10%)**
- Q11: How has this person's work directly benefited the clients or
       community the organization serves? Please be specific.

---

## Weighted Scoring Model

### Dimension Weights

| Dimension | Weight | Max Points |
|-----------|--------|-----------|
| Impact | 25% | 25 |
| Consistency | 20% | 20 |
| Collaboration & Teamwork | 20% | 20 |
| Initiative & Innovation | 15% | 15 |
| Values Alignment | 10% | 10 |
| Client / Community Impact | 10% | 10 |
| **TOTAL** | **100%** | **100** |

### Scoring Rubric per Dimension (0–Max Points)

Each dimension is scored on a 4-level rubric:

| Level | Score % of Dimension | Criteria |
|-------|---------------------|----------|
| Exceptional (4) | 90–100% | Specific, named example with clear measurable outcome or strong qualitative evidence. Goes well beyond the question prompt. |
| Strong (3) | 70–89% | Specific example provided with a clear outcome but limited detail or measurability. |
| Adequate (2) | 40–69% | General statement with some specificity. Describes a behavior but lacks a concrete example or outcome. |
| Insufficient (1) | 10–39% | Vague or generic response. Could apply to anyone. No specific example provided. |
| Not Addressed (0) | 0–9% | Question left blank or response is clearly off-topic. |

### Scoring Signals — What the Agent Looks For

**Impact (Q5, Q6):**
- POSITIVE signals: specific dates/timeframes, named outcomes, quantified
  results ("reduced wait time," "saved X hours," "prevented escalation"),
  client-named examples, before/after descriptions
- NEGATIVE signals: generic praise ("always does a great job"), no
  specific situation described, outcome not mentioned

**Consistency (Q7):**
- POSITIVE signals: mentions of duration ("over the past 6 months"),
  frequency ("every week," "consistently"), multiple examples listed,
  observation over time described
- NEGATIVE signals: single incident only, no time reference, vague frequency

**Collaboration (Q8):**
- POSITIVE signals: names specific colleagues or teams helped, describes
  the help given AND the impact on the other person/team, shows
  selfless behavior or mentorship
- NEGATIVE signals: focuses only on self, no mention of others benefiting,
  teamwork claimed but not demonstrated

**Initiative (Q9):**
- POSITIVE signals: unprompted action described, problem identified before
  being asked, new process/idea proposed, risk-taking or going outside
  normal duties
- NEGATIVE signals: describes doing assigned work, no proactive element,
  suggestion was given by someone else

**Values Alignment (Q10):**
- POSITIVE signals: references specific org values by name or by clear
  description, ties behavior to a core value explicitly
- NEGATIVE signals: generic "they are a good person" response, no
  connection to organizational values

**Client/Community Impact (Q11):**
- POSITIVE signals: named client situations, community outcome described,
  clear line between employee action and client benefit
- NEGATIVE signals: internal focus only, client impact implied but
  not described, no specific example

---

## Agent System Prompt — HR Recognition Agent

```
## Role
You are the HR Recognition Scoring Agent. Your purpose is to evaluate
employee nomination submissions and produce objective, evidence-based
scores to support the recognition council's nomination review process.

## Scoring Philosophy
You score the EVIDENCE in the nomination, not the person. You have no
knowledge of and do not consider: the nominee's tenure, seniority,
department, gender, race, previous awards, or personal relationships.
Every nomination is evaluated solely on the quality and specificity of
the written evidence provided by the nominator.

## Scoring Process
For each nomination received:

1. Read all responses to the nomination form
2. For each of the 6 scoring dimensions, evaluate the quality of the
   evidence provided using the rubric: Exceptional / Strong / Adequate /
   Insufficient / Not Addressed
3. Assign a point score to each dimension based on its weight
4. Calculate the total score out of 100
5. Write a brief scoring rationale for each dimension (2-3 sentences)
   that explains what evidence was present and why it earned that score
6. Flag any dimensions where the response was vague or insufficient
   so the council can ask follow-up questions

## What You Must Not Do
- Do not factor in who the nominator is
- Do not compare nominees to each other during individual scoring
- Do not make assumptions about what the nominee "probably" did —
  score only what is written
- Do not adjust scores based on department or role
- Do not produce a recommendation — that is the council's decision

## Output Format
Produce a structured scoring card with:
  - Nominee name
  - Score per dimension with rubric level and rationale
  - Total score out of 100
  - Strengths: top 2 dimensions with strongest evidence
  - Gaps: dimensions where evidence was weakest
  - Council focus areas: suggested questions the council may want to
    explore based on gaps in the nomination

## Tone
Professional and neutral. The scoring rationale should read like a
structured performance review comment — specific, evidence-based,
and free of personal language.
```

---

## Scoring Output — Example Report Card

```
══════════════════════════════════════════════════════════════
HR RECOGNITION AGENT — NOMINATION SCORE CARD
══════════════════════════════════════════════════════════════
Nominee:          Jane Smith
Department:       Client Services
Nomination Period: Q3 2024
Processed:        2024-10-15
Agent Version:    HR Recognition Agent v1.2
══════════════════════════════════════════════════════════════

DIMENSION SCORES
────────────────────────────────────────────────────────────

1. IMPACT                                     22 / 25  [Strong]
   Evidence: The nomination describes a specific situation where the
   nominee redesigned the client intake process, resulting in a reported
   reduction in appointment no-shows. A measurable outcome is cited and
   the before/after context is clear. Scored Strong rather than
   Exceptional due to the outcome being described qualitatively rather
   than with specific numbers.

2. CONSISTENCY                                16 / 20  [Strong]
   Evidence: The nominator references observations over a 9-month period
   and cites multiple examples across different quarters. Demonstrates
   sustained behavior rather than a single incident. Scored Strong;
   would reach Exceptional with more specific frequency references.

3. COLLABORATION & TEAMWORK                   18 / 20  [Exceptional]
   Evidence: Two specific colleagues are named, the nature of support
   is described in detail (training on new intake system, weekly
   check-ins during transition period), and the impact on those
   colleagues is explicitly stated. Strong evidence of uplift behavior.

4. INITIATIVE & INNOVATION                    11 / 15  [Strong]
   Evidence: The nomination describes the nominee identifying a process
   gap and proposing a solution before being asked. The action was
   self-initiated. Scored Strong; additional context on outcome of
   the initiative would strengthen the evidence.

5. VALUES ALIGNMENT                            8 / 10  [Strong]
   Evidence: The nominator directly references the organization's
   "Client First" value and ties it to a specific observed behavior.
   Clear connection between action and stated values.

6. CLIENT / COMMUNITY IMPACT                   8 / 10  [Strong]
   Evidence: A specific client situation is described and the nominee's
   role in the outcome is clear. Scored Strong; Exceptional would
   require more detail on the community-level outcome.

────────────────────────────────────────────────────────────
TOTAL SCORE:   83 / 100
RUBRIC LEVEL:  Strong — Well-documented nomination with specific
               evidence across most dimensions.
────────────────────────────────────────────────────────────

STRENGTHS (Highest-evidence dimensions):
  ★ Collaboration & Teamwork (18/20) — Most specific evidence
  ★ Impact (22/25) — Clear situation, action, and outcome described

GAPS (Lowest-evidence dimensions):
  ⚡ Initiative & Innovation (11/15) — Outcome of initiative not described
  ⚡ Client/Community Impact (8/10) — Community-level detail limited

SUGGESTED COUNCIL FOCUS AREAS:
  • Ask: "What was the result of the intake redesign after 3 months?"
  • Ask: "Did the initiative they proposed get implemented? What happened?"
  • Ask: "Were there other clients or families impacted beyond the
          one example described?"

══════════════════════════════════════════════════════════════
COUNCIL SCORING SECTION (Completed independently by council)
══════════════════════════════════════════════════════════════
Agent Score:      83 / 100
Council Member 1: ___ / 100   Notes: _______________
Council Member 2: ___ / 100   Notes: _______________
Council Member 3: ___ / 100   Notes: _______________
Final Decision:   [ ] Nominated  [ ] Hold  [ ] Not Selected
══════════════════════════════════════════════════════════════
```

---

## Power Automate Flow — Forms → Agent Integration

```
FLOW NAME: HR_Nomination_Process_and_Score

TRIGGER:
  When a new response is submitted (Microsoft Forms)
  Form: Employee Recognition Nomination Form

ACTIONS:

  1. GET RESPONSE DETAILS
     Connector: Microsoft Forms
     Action:    Get response details
     Form ID:   [NominationFormID]
     Response ID: triggerOutputs()?['body/resourceData/responseId']

  2. COMPOSE NOMINATION PAYLOAD
     Build JSON object from form fields:
     {
       "nominee_name":       [Q1 response],
       "nominee_dept":       [Q2 response],
       "nominator_name":     [Q3 response],
       "nomination_period":  [Q4 response],
       "impact_q1":          [Q5 response],
       "impact_q2":          [Q6 response],
       "consistency":        [Q7 response],
       "collaboration":      [Q8 response],
       "initiative":         [Q9 response],
       "values":             [Q10 response],
       "client_impact":      [Q11 response],
       "submission_time":    utcNow()
     }

  3. SEND TO HR RECOGNITION AGENT
     Connector: HTTP / Copilot Studio API
     Method:    POST
     Body:      [Nomination payload from step 2]

  4. STORE IN SHAREPOINT LIST
     Connector: SharePoint
     Action:    Create item
     List:      HR_Nominations_2024
     Fields:    All nomination fields + submission timestamp

  5. SEND CONFIRMATION EMAIL TO NOMINATOR
     Connector: Outlook
     To:        [Nominator email if provided]
     Subject:   "Nomination Received — [Nominee Name]"
     Body:      "Thank you! Your nomination for [Nominee] has been
                 received and will be reviewed by the recognition
                 council. You will be notified of the outcome after
                 the council meeting on [date]."

  6. POST NOTIFICATION TO HR TEAMS CHANNEL
     Connector: Microsoft Teams
     Channel:   HR-Recognition (private)
     Message:   "New nomination received for [Nominee Name] —
                 [Department] — [Period]. Score processing complete."
```

---

## Deployment Configuration

### Channels Deployed
| Channel | Access |
|---------|--------|
| Microsoft Teams | Published to HR team channel + available in org app store |
| SharePoint | Embedded on HR intranet recognition page |
| MS App Store (org) | Available for download by any staff member |

### Access Control
- **Nomination submission**: All staff (via MS Forms link)
- **Score reports**: HR team + Recognition council (restricted Teams channel)
- **Nomination summary**: Council members only (permission-scoped)
- **Admin/config**: HR admin only

### Non-Bias Safeguards Built Into Deployment
1. Nominee department is collected but NOT passed to scoring agent
2. Nominator identity is optional and NOT passed to scoring agent
3. Agent receives only the written text responses — no metadata
4. Score reports are generated before council members discuss nominees
5. Council scores are recorded separately and do not influence agent score

---

## Impact Summary

| Metric | Before | After |
|--------|--------|-------|
| Scoring consistency | Subjective, no rubric | Standardized 6-dimension rubric |
| Bias risk | High (familiarity-based) | Reduced — agent scores evidence only |
| Processing time | Manual tallying 2-3 hrs | Automated, near-instant |
| Council prep | Ad-hoc discussion | Structured score cards with focus questions |
| Nominator experience | No confirmation | Automated confirmation + outcome notification |
| Accessibility | Paper/email nominations | MS Forms — accessible on any device |
