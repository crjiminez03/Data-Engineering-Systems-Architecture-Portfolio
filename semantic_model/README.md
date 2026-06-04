# /semantic_model — SemanticModelLogs Gold Layer + System Tables

## Architecture Note

SemanticModelLogs does **not** have Bronze or Silver layers.
The Power BI semantic model sits on top of Gold Delta tables.
These logs track report/visual usage activity *against* those Gold tables —
they are themselves Gold-layer output, not a source to be cleansed upstream.

```
Gold Delta Tables  ←── Semantic Model (.pbix)
                              │
                              │ generates
                              ▼
                   SemanticModelLogs (KQL Eventhouse)
                              │
                              │ ingested by
                              ▼
                   gold_semantic_model_logs
                   gold_semantic_model_logs_parsed
```

## Files

| File | Purpose |
|------|---------|
| `01_gold_semantic_model_logs_parsed.py` | Reads SemanticModelLogs from KQL Eventhouse. Creates two Gold tables: `gold_semantic_model_logs` (cleansed base) and `gold_semantic_model_logs_parsed` (fully parsed). Extracts UPN, ReportId, VisualId, PageId, VisualName, PageName from `ApplicationContext.Sources[0]` JSON array and DAX query text from `EventText`. DDL + MERGE upsert. |
| `02_semantic_model_system_tables_live.py` | Connects live to the .pbix semantic model via `sempy.fabric.evaluate_dax()` (XMLA endpoint). Pulls `$SYSTEM.TMSCHEMA_TABLES`, `TMSCHEMA_COLUMNS`, `TMSCHEMA_MEASURES`, `TMSCHEMA_PARTITIONS`, `DISCOVER_STORAGE_TABLE_COLUMNS`. No CSV exports — fully automated. Builds `metadata_master` Delta table. |
| `03_gold_semantic_model_usage_analytics.py` | Builds 4 Gold analytics tables from `gold_semantic_model_logs_parsed`: report/visual render usage with performance tiers, DAX query performance with CPU-bound profiling, user engagement tiers (Power/Active/Regular/Casual), daily model usage summary. |

## Key Parsed Fields (from ApplicationContext JSON)

The `ApplicationContext` column contains a nested JSON structure when an
operation is triggered by a Power BI visual render:

```json
{
  "Sources": [{
    "ReportId":   "guid",
    "ReportName": "My Report",
    "VisualId":   "guid",
    "VisualName": "Sales Bar Chart",
    "PageId":     "guid",
    "PageName":   "Overview"
  }],
  "ActivityId": "guid",
  "ClientType": "PBIClient"
}
```

Parsing path used: `$.Sources[0].ReportId` with `coalesce` fallback to `$.ReportId`
for older log format compatibility.

## Execution Order in Pipeline

This folder's notebooks run as part of the Gold + Semantic Model Master Pipeline:

1. Gold DDL notebook (locks precision schema)
2. Dataflow Gen2 items (populate staging tables)
3. Gold DML notebook (MERGE staging → gold)
4. **`01_gold_semantic_model_logs_parsed.py`** ← runs here
5. **`02_semantic_model_system_tables_live.py`** ← runs here
6. **`03_gold_semantic_model_usage_analytics.py`** ← runs here
7. Semantic model refresh triggered
