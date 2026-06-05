# Client Resource Agent — Rule Engine & Knowledge Source Schema Reference
# Portfolio Reference: Copilot Studio Agent Projects
# Description: Complete rule engine logic combining service type tags,
#              keyword matching, and category lookup table. Also defines
#              the full knowledge source schema with field-level notes.

# ══════════════════════════════════════════════════════════════
# RULE ENGINE — CATEGORY LOOKUP TABLE
# Primary mapping: user intent keywords → PrimaryCategory
# ══════════════════════════════════════════════════════════════

CATEGORY_LOOKUP = {
    # HOUSING
    "housing":              "HOUSING",
    "shelter":              "HOUSING",
    "rent":                 "HOUSING",
    "eviction":             "HOUSING",
    "transitional housing": "HOUSING",
    "rental assistance":    "HOUSING",
    "emergency housing":    "HOUSING",
    "motel voucher":        "HOUSING",
    "affordable housing":   "HOUSING",

    # FOOD
    "food":                 "FOOD",
    "hungry":               "FOOD",
    "groceries":            "FOOD",
    "food bank":            "FOOD",
    "food pantry":          "FOOD",
    "meals":                "FOOD",
    "snap":                 "FOOD",
    "wic":                  "FOOD",
    "nutrition":            "FOOD",
    "hot meals":            "FOOD",

    # DOMESTIC VIOLENCE
    "domestic violence":    "DOMESTIC_VIOLENCE",
    "dv":                   "DOMESTIC_VIOLENCE",
    "abuse":                "DOMESTIC_VIOLENCE",
    "safe house":           "DOMESTIC_VIOLENCE",
    "safety plan":          "DOMESTIC_VIOLENCE",
    "restraining order":    "DOMESTIC_VIOLENCE",
    "intimate partner":     "DOMESTIC_VIOLENCE",

    # HOMELESSNESS
    "homeless":             "HOMELESSNESS",
    "unhoused":             "HOMELESSNESS",
    "sleeping outside":     "HOMELESSNESS",
    "day shelter":          "HOMELESSNESS",
    "drop in":              "HOMELESSNESS",
    "outreach":             "HOMELESSNESS",
    "encampment":           "HOMELESSNESS",

    # MENTAL HEALTH
    "mental health":        "MENTAL_HEALTH",
    "counseling":           "MENTAL_HEALTH",
    "therapy":              "MENTAL_HEALTH",
    "psychiatry":           "MENTAL_HEALTH",
    "crisis":               "MENTAL_HEALTH",
    "depression":           "MENTAL_HEALTH",
    "anxiety":              "MENTAL_HEALTH",
    "suicide":              "MENTAL_HEALTH",
    "behavioral health":    "MENTAL_HEALTH",

    # SUBSTANCE USE
    "substance":            "SUBSTANCE_USE",
    "addiction":            "SUBSTANCE_USE",
    "detox":                "SUBSTANCE_USE",
    "rehab":                "SUBSTANCE_USE",
    "treatment":            "SUBSTANCE_USE",
    "recovery":             "SUBSTANCE_USE",
    "alcohol":              "SUBSTANCE_USE",
    "drugs":                "SUBSTANCE_USE",
    "sober":                "SUBSTANCE_USE",

    # LEGAL
    "legal":                "LEGAL",
    "lawyer":               "LEGAL",
    "attorney":             "LEGAL",
    "court":                "LEGAL",
    "immigration":          "LEGAL",
    "tenant rights":        "LEGAL",
    "legal aid":            "LEGAL",
    "custody":              "LEGAL",

    # EMPLOYMENT
    "job":                  "EMPLOYMENT",
    "employment":           "EMPLOYMENT",
    "work":                 "EMPLOYMENT",
    "resume":               "EMPLOYMENT",
    "job training":         "EMPLOYMENT",
    "unemployment":         "EMPLOYMENT",
    "career":               "EMPLOYMENT",
    "hire":                 "EMPLOYMENT",

    # TRANSPORTATION
    "transportation":       "TRANSPORTATION",
    "bus":                  "TRANSPORTATION",
    "ride":                 "TRANSPORTATION",
    "gas":                  "TRANSPORTATION",
    "medical transport":    "TRANSPORTATION",
    "bus pass":             "TRANSPORTATION",

    # CHILDCARE
    "childcare":            "CHILDCARE",
    "daycare":              "CHILDCARE",
    "child care":           "CHILDCARE",
    "head start":           "CHILDCARE",
    "after school":         "CHILDCARE",
    "babysitting":          "CHILDCARE",

    # HEALTHCARE
    "healthcare":           "HEALTHCARE",
    "medical":              "HEALTHCARE",
    "doctor":               "HEALTHCARE",
    "clinic":               "HEALTHCARE",
    "dental":               "HEALTHCARE",
    "vision":               "HEALTHCARE",
    "prescription":         "HEALTHCARE",
    "medicaid":             "HEALTHCARE",
    "fqhc":                 "HEALTHCARE",

    # FINANCIAL
    "financial":            "FINANCIAL",
    "money":                "FINANCIAL",
    "utility":              "FINANCIAL",
    "electric":             "FINANCIAL",
    "gas bill":             "FINANCIAL",
    "emergency funds":      "FINANCIAL",
    "benefits":             "FINANCIAL",
    "assistance":           "FINANCIAL",

    # EDUCATION
    "education":            "EDUCATION",
    "ged":                  "EDUCATION",
    "esl":                  "EDUCATION",
    "english":              "EDUCATION",
    "vocational":           "EDUCATION",
    "school":               "EDUCATION",
    "literacy":             "EDUCATION",

    # LGBTQ
    "lgbtq":                "LGBTQ",
    "gay":                  "LGBTQ",
    "lesbian":              "LGBTQ",
    "transgender":          "LGBTQ",
    "queer":                "LGBTQ",
    "pride":                "LGBTQ",
    "gender affirming":     "LGBTQ",
}


# ══════════════════════════════════════════════════════════════
# RULE ENGINE — SUBCATEGORY TAG MATRIX
# Multi-label tags applied in addition to PrimaryCategory
# ══════════════════════════════════════════════════════════════

SUBCATEGORY_TAG_RULES = {
    # Modifier tags — applied when these keywords appear in user query
    # or in facility description during knowledge source indexing

    "walk.?in|walk in":         "Walk-In",
    "no appointment":           "Walk-In",
    "24.?hour|24 hour|overnight":"24-Hour",
    "emergency":                "Emergency",
    "children|kids|family":     "Children",
    "spanish|español|bilingual": "Bilingual-Spanish",
    "veteran|military":         "Veterans",
    "disab":                    "Disability",
    "senior|elderly|older":     "Seniors",
    "no id|without id":         "No-ID-Required",
    "lgbtq|transgender|queer":  "LGBTQ-Affirming",
    "rental":                   "Rental-Assistance",
    "utility":                  "Utility-Assistance",
    "eviction":                 "Eviction-Prevention",
    "food bank|pantry":         "Food-Bank",
    "hot meal|meal program":    "Hot-Meals",
    "snap|ebt":                 "SNAP-Enrollment",
    "crisis line|hotline":      "Crisis-Line",
    "detox|withdrawal":         "Detox",
    "sober living|recovery":    "Recovery",
    "free|no cost|no charge":   "Free-Service",
    "sliding scale|income":     "Sliding-Scale",
    "referral only":            "Referral-Required",
    "women only|women.s":       "Women-Only",
    "men only|men.s":           "Men-Only",
    "single":                   "Single-Adults",
    "couple":                   "Couples",
}


# ══════════════════════════════════════════════════════════════
# RULE ENGINE — CATEGORY CONFLICT RESOLUTION
# When a query maps to multiple categories, resolve priority
# ══════════════════════════════════════════════════════════════

CONFLICT_RESOLUTION = {
    # (Category A, Category B) → Preferred primary, other becomes sub-filter
    ("HOUSING", "DOMESTIC_VIOLENCE"):  "DOMESTIC_VIOLENCE",  # DV takes priority — safety first
    ("HOUSING", "HOMELESSNESS"):       "HOMELESSNESS",        # More specific
    ("MENTAL_HEALTH", "SUBSTANCE_USE"):"MENTAL_HEALTH",       # Co-occurring — show both
    ("HEALTHCARE", "MENTAL_HEALTH"):   "MENTAL_HEALTH",       # Intent is usually MH
    ("FINANCIAL", "HOUSING"):          "HOUSING",             # Rental/utility = housing need
    ("LGBTQ", "ANY"):                  "LGBTQ",               # Always add as sub-filter not primary
}

# When LGBTQ is in the query, always add LGBTQ-Affirming sub-filter
# regardless of the primary category selected


# ══════════════════════════════════════════════════════════════
# RULE ENGINE — PYTHON IMPLEMENTATION REFERENCE
# Mirrors the logic built into the Copilot Studio topic flows
# This version is for documentation/testing purposes
# ══════════════════════════════════════════════════════════════
import re

def categorize_query(user_query: str) -> dict:
    """
    Maps a free-text user query to PrimaryCategory + SubCategory tags.
    Mirrors the entity extraction and routing logic in Copilot Studio topics.

    Args:
        user_query: Raw text from provider e.g. "my client needs DV shelter,
                    has 2 kids, speaks Spanish"

    Returns:
        dict with:
            primary_category: str
            sub_tags: list[str]
            matched_keywords: list[str]
            confidence: str (HIGH / MEDIUM / LOW)
    """
    query_lower = user_query.lower()

    # Step 1: Match primary category keywords
    matched_categories = {}
    matched_kws = []

    for keyword, category in CATEGORY_LOOKUP.items():
        if keyword in query_lower:
            matched_categories[category] = matched_categories.get(category, 0) + 1
            matched_kws.append(keyword)

    # Step 2: Resolve primary category (highest hit count wins)
    if not matched_categories:
        primary = "GENERAL"
        confidence = "LOW"
    elif len(matched_categories) == 1:
        primary = list(matched_categories.keys())[0]
        confidence = "HIGH"
    else:
        # Multiple categories — apply conflict resolution
        sorted_cats = sorted(matched_categories.items(), key=lambda x: x[1], reverse=True)
        top_two = (sorted_cats[0][0], sorted_cats[1][0])

        if top_two in CONFLICT_RESOLUTION:
            primary = CONFLICT_RESOLUTION[top_two]
        elif (top_two[1], top_two[0]) in CONFLICT_RESOLUTION:
            primary = CONFLICT_RESOLUTION[(top_two[1], top_two[0])]
        else:
            primary = sorted_cats[0][0]

        confidence = "MEDIUM"

    # Step 3: Extract sub-category modifier tags
    sub_tags = []
    for pattern, tag in SUBCATEGORY_TAG_RULES.items():
        if re.search(pattern, query_lower):
            sub_tags.append(tag)

    # Step 4: LGBTQ always adds as sub-filter even if not primary
    if "LGBTQ" in matched_categories and primary != "LGBTQ":
        if "LGBTQ-Affirming" not in sub_tags:
            sub_tags.append("LGBTQ-Affirming")

    return {
        "primary_category":  primary,
        "sub_tags":          list(set(sub_tags)),
        "matched_keywords":  matched_kws,
        "confidence":        confidence,
    }


# ── Test the Rule Engine ──────────────────────────────────────
if __name__ == "__main__":
    test_queries = [
        "my client needs DV shelter, has 2 kids, speaks Spanish",
        "looking for emergency housing walk-in near 00000",
        "food bank that's open on weekends no ID required",
        "client is homeless veteran needs mental health support",
        "rental assistance for single mom facing eviction",
        "transgender youth needing counseling",
        "detox program that accepts medicaid",
        "client needs a job, has GED, speaks English and Spanish",
    ]

    print("Rule Engine — Category Resolution Test")
    print("=" * 65)
    for q in test_queries:
        result = categorize_query(q)
        print(f"\nQuery: '{q}'")
        print(f"  Primary:    {result['primary_category']}  [{result['confidence']}]")
        print(f"  Sub-tags:   {', '.join(result['sub_tags']) if result['sub_tags'] else 'None'}")
        print(f"  Keywords:   {', '.join(result['matched_keywords'])}")
