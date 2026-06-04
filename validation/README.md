# /validation — Ingestion Integrity Validation

## Overview

Row count validation framework that compares source vs destination record
counts per batch date across the medallion pipeline. Designed specifically
to catch **silent load failures** — scenarios where a pipeline reports
SUCCESS but data is missing or partially loaded due to behind-the-scenes
issues that do not surface as explicit errors.

## The Silent Failure Problem

A Fabric pipeline activity can return a green checkmark (SUCCESS) while:
- Delta transaction log compaction causes partial reads
- A MERGE operation silently deduplicates more rows than expected
- A KQL query returns fewer rows than the actual source due to timeout
- A Dataflow Gen2 Replace lands fewer rows than the staging source

Without explicit row count comparison, these issues only surface when
a report consumer notices missing data — often days later.

## Validation Status Classifications

| Status | Meaning | Action |
|--------|---------|--------|
| `PASS` | Destination row count within 5% of source | No action |
| `WARN_SHORT_LOAD` | Destination loaded < 95% of source rows | Investigate — possible data loss |
| `WARN_OVER_LOADED` | Destination has > 105% of source rows | Investigate — possible duplication |
| `CRITICAL_MISSING` | Destination has 0 rows, source has rows | Pipeline failed silently |
| `DEST_ONLY` | Destination has rows, source has 0 | Orphan data — investigate |
| `NO_DATA` | Both source and destination have 0 rows | Expected for empty windows |

## Silent Failure Detection Logic

Beyond row counts, the notebook cross-references operation status from
`silver_semantic_model_logs_parsed`:

```
IF   reported_success_rate >= 95%    (pipeline said SUCCESS)
AND  pct_loaded < 95%                (but actual rows loaded < 95% of source)
THEN flag as SILENT_FAILURE
```

This catches the specific scenario where every individual operation log
entry shows SUCCESS but the aggregate data volume is wrong.

## Files

| File | Purpose |
|------|---------|
| `09_rowcount_validation_pre_post.py` | Runs validation across all configured source→destination table pairs. Writes results to `ingestion_validation_log`. Performs silent failure cross-reference check. Designed to run as a final step in both master pipelines. |

## Validation Targets Covered

| Pipeline | Layer | Source → Destination |
|----------|-------|---------------------|
| SemanticModelLogs | Bronze→Silver | `bronze_semantic_model_logs` → `silver_semantic_model_logs_parsed` |
| SemanticModelLogs | Silver→Gold | `silver_semantic_model_logs_parsed` → `gold_semantic_model_refresh_summary` |
| SemanticModelLogs | Silver→Gold | `silver_semantic_model_logs_parsed` → `gold_semantic_model_dax_performance` |
| AuditLogs | Bronze→Silver | `bronze_audit_logs_powerbi` → `silver_audit_logs_powerbi` |

## Recommended Schedule

Run at the end of each master pipeline as a final validation gate.
Results in `ingestion_validation_log` can feed a Power BI monitoring
dashboard or trigger a Teams alert on CRITICAL status.
