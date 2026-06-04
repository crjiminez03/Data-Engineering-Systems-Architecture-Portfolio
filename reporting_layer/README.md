# /reporting_layer — Power BI Reporting Layer Overview

## Overview

The reporting layer sits on top of the Gold Delta tables in the Fabric
Lakehouse, accessed via the Power BI semantic model (.pbix). This document
describes the dashboard and report designs built on the Gold layer data,
completing the end-to-end story from on-prem source systems through to
business-facing analytics.

---

## Semantic Model Design

The semantic model connects directly to Gold Lakehouse tables via the
Fabric-native DirectLake connection mode where possible, falling back to
Import mode for tables requiring complex pre-aggregation.

### Tables in the Semantic Model

| Semantic Model Table | Source Gold Table | Mode |
|---------------------|-------------------|------|
| RefreshSummary | `gold_semantic_model_refresh_summary` | Import |
| DAXPerformance | `gold_semantic_model_dax_performance` | Import |
| UserActivity | `gold_semantic_model_user_activity` | Import |
| ModelHealthScorecard | `gold_model_health_scorecard` | Import |
| AuditEvents | `gold_audit_user_activity_summary` | Import |
| ReportVisualUsage | `gold_report_visual_usage` | Import |
| UserReportActivity | `gold_user_report_activity` | Import |
| ModelUsageSummary | `gold_model_usage_summary` | Import |
| Date | Date dimension (DAX-generated) | Calculated |

### Key DAX Measures

```dax
-- Refresh Success Rate (%)
Refresh Success Rate =
DIVIDE(
    CALCULATE(SUM(RefreshSummary[success_count])),
    CALCULATE(SUM(RefreshSummary[total_refresh_ops])),
    0
) * 100

-- Average DAX Duration (formatted)
Avg DAX Duration (s) =
DIVIDE(
    AVERAGE(DAXPerformance[avg_duration_ms]),
    1000,
    0
)

-- SLA Breach Count
SLA Breach Days =
CALCULATE(
    COUNTROWS(RefreshSummary),
    RefreshSummary[sla_status] = "SLA_BREACH"
)

-- Model Health Grade
Health Grade =
SWITCH(
    TRUE(),
    AVERAGE(ModelHealthScorecard[health_score]) >= 90, "A",
    AVERAGE(ModelHealthScorecard[health_score]) >= 75, "B",
    AVERAGE(ModelHealthScorecard[health_score]) >= 60, "C",
    "D"
)

-- Slow Query % (DAX execution in Slow or Critical tier)
Slow Query Pct =
DIVIDE(
    CALCULATE(
        SUM(DAXPerformance[dax_execution_count]),
        DAXPerformance[performance_tier] IN {"Slow (2-10s)", "Critical (>10s)"}
    ),
    SUM(DAXPerformance[dax_execution_count]),
    0
) * 100

-- Unique Active Users (last 30 days)
Active Users (30d) =
CALCULATE(
    DISTINCTCOUNT(UserReportActivity[user_upn]),
    DATESINPERIOD('Date'[Date], MAX('Date'[Date]), -30, DAY)
)
```

---

## Report Pages

### Report 1: Workspace Operations Dashboard

**Audience:** Data Engineering team, IT Operations
**Refresh:** Daily (after Gold pipeline completes at ~7:30 AM)

**Page 1 — Executive Summary**
- KPI cards: Total operations today, Success rate %, Avg duration, Active users
- Line chart: Daily operation volume trend (last 30 days)
- Donut: Operations by status (Success / Failure / Started / Completed)
- Table: Top 5 models by operation count with success rate

**Page 2 — Refresh Health**
- Matrix: Model × Day with conditional formatting (green/amber/red by success rate)
- Bar chart: Average refresh duration by model (sorted longest first)
- SLA breach indicator: Models with avg duration > 30 mins flagged red
- Trend line: Daily success rate over last 90 days

**Page 3 — DAX Performance**
- Scatter plot: Avg duration vs execution count per model (bubble = CPU time)
- Table: Slowest queries by p95 duration with performance tier badge
- Bar chart: CPU-bound vs IO-bound vs Mixed operations by model
- User filter: Filter all visuals by executing user UPN

**Page 4 — User Activity**
- Bar chart: Top 20 users by total operations
- Engagement tier donut: Power / Active / Regular / Casual user split
- Table: User × Model × engagement tier with success rate column
- Heatmap: User activity by hour of day (when are peak usage times?)

---

### Report 2: Semantic Model Usage Intelligence

**Audience:** BI developers, Report owners, Business stakeholders
**Refresh:** Daily

**Page 1 — Report Usage Overview**
- KPI cards: Total report renders, Distinct users, Distinct reports, Avg render time
- Bar chart: Top 10 most-used reports by render count
- Table: Report × Page × Visual with render count and performance tier

**Page 2 — Visual Performance**
- Table: Slowest visuals by p95 render time with performance tier badge
  - Conditional formatting: red = Critical (>10s), amber = Slow (2-10s)
- Bar chart: Performance tier distribution across all visuals
- Drill-through: Click any visual to see its DAX query execution history

**Page 3 — Report Health**
- Matrix: Report × Week with avg render time (trend over time)
- Failure rate table: Reports/visuals with highest failure rates
- Filter panel: By workspace, model, client type (PBIClient, Excel, etc.)

---

### Report 3: Audit Log Activity Monitor

**Audience:** Security team, HR, Compliance
**Refresh:** Daily (after audit log Silver load at ~6:30 AM)
**Access:** Restricted — HR and Security roles only

**Page 1 — Daily Activity Summary**
- KPI cards: Total audit events, Distinct users, Operations performed, Failures
- Line chart: Daily event volume (last 30 days)
- Bar chart: Top operations by count (ViewReport, ExportReport, ShareReport etc.)

**Page 2 — User Behaviour**
- Table: User × Operation × Count with success rate
- Engagement tier breakdown
- Filter: Date range, operation type, workspace

**Page 3 — Export & Sharing Activity**
- Focused view on ExportReport, ShareReport, DownloadReport operations
- Useful for data governance: who is exporting what and how often

---

### Report 4: Pipeline Operations Health (Internal)

**Audience:** Data Engineering team only
**Refresh:** After each pipeline run
**Source:** `pipeline_audit_log` table

**Page 1 — Run History**
- Table: Recent pipeline runs with rows read/written, duration, status
- Status badge: SUCCESS (green) / FAILURE (red) / PARTIAL_FAILURE (amber)
- Trend: Daily row volume by destination table

**Page 2 — Failure Analysis**
- Table: All failures with error step, error message, destination table
- Bar chart: Failures by pipeline name and destination table
- Alert: Any table with >1 failure in last 7 days flagged

**Page 3 — Validation Results**
- Table: `ingestion_validation_log` results — source vs destination counts
- Conditional formatting: CRITICAL (red), WARN (amber), PASS (green)
- Silent failure flags highlighted separately

---

## Deployment Notes

- Semantic model (.pbix) published to Fabric workspace
- Reports built in Power BI Desktop, published to same workspace
- Scheduled refresh tied to Gold pipeline completion via pipeline trigger
- Row-level security (RLS) applied to Audit Log reports:
  - Security role: HR_Audit_Readers — sees all data
  - Security role: Workspace_Owners — sees own workspace only
- Reports shared via Power BI App (not direct workspace access)
  - App deployed to: Data Engineering team, BI Developers, Report Stakeholders
  - Audit Log App deployed separately with restricted membership
