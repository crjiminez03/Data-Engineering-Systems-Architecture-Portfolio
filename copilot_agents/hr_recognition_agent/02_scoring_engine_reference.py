# HR Recognition Agent — Scoring Engine & Topic Flow Reference
# Portfolio Reference: Copilot Studio Agent Projects
# Description: Python reference implementation of the weighted scoring
#              engine and full topic flow pseudocode for the HR
#              Recognition Agent. The actual scoring logic runs inside
#              the Copilot Studio agent via its generative AI instructions.
#              This file documents the design for portfolio reference.

# ══════════════════════════════════════════════════════════════
# SCORING ENGINE — PYTHON REFERENCE IMPLEMENTATION
# ══════════════════════════════════════════════════════════════

from dataclasses import dataclass, field
from typing import Optional
import json
from datetime import datetime


# ── Scoring Constants ──────────────────────────────────────────
DIMENSION_WEIGHTS = {
    "impact":            0.25,
    "consistency":       0.20,
    "collaboration":     0.20,
    "initiative":        0.15,
    "values_alignment":  0.10,
    "client_impact":     0.10,
}

RUBRIC_LEVELS = {
    "exceptional":    (0.90, 1.00),
    "strong":         (0.70, 0.89),
    "adequate":       (0.40, 0.69),
    "insufficient":   (0.10, 0.39),
    "not_addressed":  (0.00, 0.09),
}

MAX_TOTAL_SCORE = 100


# ── Scoring Signal Definitions ────────────────────────────────
# Positive and negative signals the agent looks for per dimension
# These are the rubric guidelines passed to the generative AI

SCORING_SIGNALS = {
    "impact": {
        "weight":    0.25,
        "max_pts":   25,
        "questions": ["impact_q1", "impact_q2"],
        "positive":  [
            "specific dates or timeframes mentioned",
            "named measurable outcome (reduced, increased, saved, prevented)",
            "before and after comparison described",
            "client or situation named specifically",
            "goes beyond what was asked / expected",
            "quantified result (numbers, percentages, time saved)",
        ],
        "negative":  [
            "generic praise with no specific situation",
            "no outcome described",
            "could apply to anyone in the role",
            "single adjective description only",
        ],
        "exceptional_threshold": "Specific named situation with measurable or "
                                  "strongly qualitative outcome described in detail",
        "not_addressed_threshold": "Blank response or fewer than 2 sentences",
    },

    "consistency": {
        "weight":    0.20,
        "max_pts":   20,
        "questions": ["consistency"],
        "positive":  [
            "duration mentioned (months, quarters, since joining)",
            "frequency described (daily, weekly, every time)",
            "multiple examples across different time periods",
            "pattern of behavior described not just one incident",
            "observed over extended period by nominator",
        ],
        "negative":  [
            "single incident described",
            "no time reference",
            "vague frequency (sometimes, often without context)",
        ],
        "exceptional_threshold": "Sustained pattern with specific timeframe and "
                                  "multiple distinct examples cited",
        "not_addressed_threshold": "Blank or single vague sentence",
    },

    "collaboration": {
        "weight":    0.20,
        "max_pts":   20,
        "questions": ["collaboration"],
        "positive":  [
            "specific colleague or team named",
            "nature of support described in detail",
            "impact on the other person/team explicitly stated",
            "mentorship, training, or uplift behavior shown",
            "team success prioritized over individual recognition",
        ],
        "negative":  [
            "focuses on own achievement only",
            "no mention of others benefiting",
            "teamwork claimed but not demonstrated with example",
            "vague 'works well with others' without specifics",
        ],
        "exceptional_threshold": "Named colleagues, specific support described, "
                                  "and impact on those individuals/team stated",
        "not_addressed_threshold": "Blank or self-focused response only",
    },

    "initiative": {
        "weight":    0.15,
        "max_pts":   15,
        "questions": ["initiative"],
        "positive":  [
            "action was self-initiated without being asked",
            "problem identified before being assigned",
            "new process, tool, or approach proposed",
            "went outside normal duties to solve a problem",
            "risk-taking or stepping up described",
        ],
        "negative":  [
            "describes completing assigned work only",
            "suggestion was prompted by someone else",
            "no proactive element described",
            "reactive rather than proactive framing",
        ],
        "exceptional_threshold": "Unprompted identification of problem + proposed "
                                  "solution + evidence of follow-through",
        "not_addressed_threshold": "Blank or describes normal job duties only",
    },

    "values_alignment": {
        "weight":    0.10,
        "max_pts":   10,
        "questions": ["values"],
        "positive":  [
            "specific organizational value referenced by name or clear description",
            "behavior tied explicitly to a stated value",
            "example shows value in action not just in words",
        ],
        "negative":  [
            "generic 'good person' response",
            "no connection to organizational values",
            "lists values without showing them in behavior",
        ],
        "exceptional_threshold": "Named value + specific behavior example + "
                                  "clear connection between the two",
        "not_addressed_threshold": "Blank or values not mentioned",
    },

    "client_impact": {
        "weight":    0.10,
        "max_pts":   10,
        "questions": ["client_impact"],
        "positive":  [
            "named client situation described",
            "clear line between employee action and client benefit",
            "community-level outcome referenced",
            "client experience improved or protected",
        ],
        "negative":  [
            "internal/operational focus only",
            "client impact implied but not described",
            "no specific example involving a client or community member",
        ],
        "exceptional_threshold": "Specific client situation + employee action + "
                                  "client/community outcome clearly described",
        "not_addressed_threshold": "Blank or no client reference at all",
    },
}


# ── Nomination Data Class ──────────────────────────────────────
@dataclass
class Nomination:
    nominee_name:       str
    nominee_dept:       str
    nomination_period:  str
    nominator_name:     Optional[str]  # Optional — can be anonymous
    impact_q1:          str            # Specific situation + outcome
    impact_q2:          str            # Beyond what is expected
    consistency:        str
    collaboration:      str
    initiative:         str
    values:             str
    client_impact:      str
    submission_time:    str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ── Dimension Score Result ─────────────────────────────────────
@dataclass
class DimensionScore:
    dimension:        str
    raw_score:        float    # 0.0 to 1.0
    weighted_score:   float    # raw_score * weight * 100
    max_points:       int
    rubric_level:     str      # exceptional / strong / adequate / insufficient / not_addressed
    rationale:        str
    positive_signals: list
    negative_signals: list
    flagged:          bool     # True if insufficient/not_addressed


# ── Full Score Card ────────────────────────────────────────────
@dataclass
class ScoreCard:
    nomination:       Nomination
    dimension_scores: list
    total_score:      float
    overall_level:    str
    strengths:        list   # Top 2 dimensions
    gaps:             list   # Bottom 2 dimensions
    council_focus:    list   # Suggested questions for council
    processed_at:     str = field(default_factory=lambda: datetime.utcnow().isoformat())


def get_rubric_level(raw_score: float) -> str:
    """Map a 0.0–1.0 score to a rubric level label."""
    for level, (low, high) in RUBRIC_LEVELS.items():
        if low <= raw_score <= high:
            return level
    return "not_addressed"


def score_nomination(nomination: Nomination,
                     dimension_scores_input: dict) -> ScoreCard:
    """
    Calculates the full scored ScoreCard from a nomination.

    Args:
        nomination: Nomination dataclass
        dimension_scores_input: dict of {dimension: raw_score (0.0-1.0)}
            These raw scores are provided by the agent's generative AI
            evaluation of each response against the rubric signals.

    Returns:
        ScoreCard with full breakdown, total score, strengths, gaps,
        and suggested council focus areas.
    """
    dimension_results = []
    total_weighted    = 0.0

    for dim, config in SCORING_SIGNALS.items():
        raw_score = dimension_scores_input.get(dim, 0.0)
        raw_score = max(0.0, min(1.0, raw_score))  # clamp 0-1

        weight         = config["weight"]
        max_pts        = config["max_pts"]
        weighted_score = round(raw_score * max_pts, 1)
        rubric_level   = get_rubric_level(raw_score)
        flagged        = rubric_level in ("insufficient", "not_addressed")

        total_weighted += weighted_score

        dimension_results.append(DimensionScore(
            dimension=        dim,
            raw_score=        raw_score,
            weighted_score=   weighted_score,
            max_points=       max_pts,
            rubric_level=     rubric_level,
            rationale=        "",   # Filled by agent generative output
            positive_signals= [],
            negative_signals= [],
            flagged=          flagged,
        ))

    total_score    = round(total_weighted, 1)
    overall_level  = get_rubric_level(total_score / 100)

    # Identify strengths (top 2 by weighted_score / max_points ratio)
    sorted_by_pct  = sorted(
        dimension_results,
        key=lambda x: x.weighted_score / x.max_points,
        reverse=True
    )
    strengths = [d.dimension for d in sorted_by_pct[:2]]
    gaps      = [d.dimension for d in sorted_by_pct[-2:]]

    # Generate council focus areas for flagged/gap dimensions
    council_focus = []
    for d in dimension_results:
        if d.flagged or d.dimension in gaps:
            focus_map = {
                "impact":           "Ask: Can you describe the specific outcome in more detail? Were there measurable results?",
                "consistency":      "Ask: Over what period have you observed this behavior? Can you share another example?",
                "collaboration":    "Ask: Can you name a specific colleague this person supported and describe how?",
                "initiative":       "Ask: Was this action self-initiated? What was the outcome of the initiative?",
                "values_alignment": "Ask: Which specific organizational value does this behavior reflect and how?",
                "client_impact":    "Ask: Can you describe the direct impact on a specific client or community member?",
            }
            if d.dimension in focus_map:
                council_focus.append(focus_map[d.dimension])

    return ScoreCard(
        nomination=       nomination,
        dimension_scores= dimension_results,
        total_score=      total_score,
        overall_level=    overall_level,
        strengths=        strengths,
        gaps=             gaps,
        council_focus=    council_focus,
    )


def format_score_card(sc: ScoreCard) -> str:
    """Formats a ScoreCard as a readable report string."""
    dim_labels = {
        "impact":           "Impact",
        "consistency":      "Consistency",
        "collaboration":    "Collaboration & Teamwork",
        "initiative":       "Initiative & Innovation",
        "values_alignment": "Values Alignment",
        "client_impact":    "Client / Community Impact",
    }
    rubric_display = {
        "exceptional":   "Exceptional",
        "strong":        "Strong",
        "adequate":      "Adequate",
        "insufficient":  "Insufficient ⚡",
        "not_addressed": "Not Addressed ⚠",
    }

    lines = [
        "═" * 62,
        "HR RECOGNITION AGENT — NOMINATION SCORE CARD",
        "═" * 62,
        f"Nominee:          {sc.nomination.nominee_name}",
        f"Department:       {sc.nomination.nominee_dept}",
        f"Period:           {sc.nomination.nomination_period}",
        f"Processed:        {sc.processed_at[:10]}",
        "─" * 62,
        "DIMENSION SCORES",
        "─" * 62,
    ]

    for ds in sc.dimension_scores:
        label = dim_labels.get(ds.dimension, ds.dimension)
        level = rubric_display.get(ds.rubric_level, ds.rubric_level)
        lines.append(
            f"\n{label:<32} {ds.weighted_score:>5.1f} / {ds.max_points}  [{level}]"
        )
        if ds.rationale:
            lines.append(f"   {ds.rationale}")

    lines += [
        "─" * 62,
        f"TOTAL SCORE:   {sc.total_score} / 100",
        f"OVERALL LEVEL: {rubric_display.get(sc.overall_level, sc.overall_level)}",
        "─" * 62,
        f"\nSTRENGTHS: {', '.join(dim_labels.get(s,s) for s in sc.strengths)}",
        f"GAPS:      {', '.join(dim_labels.get(g,g) for g in sc.gaps)}",
        "\nSUGGESTED COUNCIL FOCUS AREAS:",
    ]
    for q in sc.council_focus:
        lines.append(f"  • {q}")
    lines.append("═" * 62)

    return "\n".join(lines)


# ── Example Run ───────────────────────────────────────────────
if __name__ == "__main__":
    sample = Nomination(
        nominee_name      = "Jane Smith",
        nominee_dept      = "Client Services",
        nomination_period = "Q3 2024",
        nominator_name    = None,   # Anonymous
        impact_q1         = "Jane redesigned the client intake process after noticing clients were waiting 45+ minutes. She mapped the current process, identified 3 bottlenecks, and proposed a triage system that reduced average wait time to 12 minutes within 6 weeks.",
        impact_q2         = "This was entirely outside her job description. She took it on voluntarily during her lunch breaks and presented it to leadership unprompted.",
        consistency       = "I've observed this behavior for 9 months. Every week she goes above and beyond. In Q1 she reorganized the resource library. In Q2 she trained two new hires. In Q3 the intake redesign. It's not a one-time thing.",
        collaboration     = "She personally mentored Marcus and Deja during their first 90 days, met with them weekly, answered questions on her own time, and both said she was the reason they felt confident in the role.",
        initiative        = "She identified the intake bottleneck before anyone asked her to. She researched triage models used at other agencies, built a proposal, and presented it to her supervisor — it was approved and implemented.",
        values            = "Our core value is 'Client First' and Jane embodies it. She stayed late to help a client in crisis connect with a DV shelter when her shift ended because she didn't want to hand the client off mid-conversation.",
        client_impact     = "Directly because of her intake redesign, 3 clients who previously walked out due to wait times stayed and were connected to housing resources they urgently needed.",
        submission_time   = "2024-10-14T14:32:00Z"
    )

    # Agent provides these raw scores (0.0-1.0) after evaluating each dimension
    # In production the agent generates these via its generative AI rubric evaluation
    agent_raw_scores = {
        "impact":           0.88,
        "consistency":      0.82,
        "collaboration":    0.95,
        "initiative":       0.85,
        "values_alignment": 0.80,
        "client_impact":    0.80,
    }

    scorecard = score_nomination(sample, agent_raw_scores)
    print(format_score_card(scorecard))
