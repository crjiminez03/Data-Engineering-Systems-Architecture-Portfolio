# /kql — Eventhouse KQL Query Reference

KQL queries for Microsoft Fabric Eventhouse (Real-Time Analytics).
These queries are the source layer for the SemanticModelLogs medallion pipeline
and serve as standalone ad-hoc analysis tools for workspace observability.

## Files

| File | Queries | Purpose |
|------|---------|---------|
| `workspace_logs_query.kql` | 8 | General workspace activity — failures, user summaries, slow operations, refresh trends, item-specific filtering |
| `semantic_model_logs_query.kql` | 12 | SemanticModel-specific — DAX performance, CPU vs IO profiling, identity parsing, ApplicationContext parsing, duplicate detection, watermark check, real-time feed |

## How to Use

Open in Fabric Real-Time Analytics → KQL Database → Query editor.
Or use as the `kustoQuery` parameter in the Kusto Spark connector
inside a PySpark notebook (see `/semantic_model/01_gold_semantic_model_logs_parsed.py`).

## Key Queries to Note

- **Query 8 (semantic_model):** CPU/IO ratio classification — identifies whether
  slow operations are CPU-bound or storage-bound, which drives different optimization approaches.
- **Query 7 (semantic_model):** Duplicate OperationId detection — data quality
  check run before silver MERGE to catch upstream issues.
- **Query 11 (semantic_model):** Incremental watermark check — used to validate
  the row count the bronze notebook should expect before writing.
