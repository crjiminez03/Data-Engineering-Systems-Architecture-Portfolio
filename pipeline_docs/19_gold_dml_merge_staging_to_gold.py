# Fabric Notebook — Gold Layer DML: MERGE Staging → Gold Tables
# Portfolio Reference: Fabric Workspace Observability Project
# Description: After Dataflow Gen2 items load data into _staging tables
#              using the Replace method, this DML notebook runs MERGE
#              operations to move data from each staging table into the
#              corresponding gold table — adding new records and updating
#              existing ones. This preserves the precision schema defined
#              in the DDL notebook and maintains full change history.
#
#              Execution order in Gold + Semantic Model pipeline:
#                1. Gold DDL notebook  (18_gold_ddl_precision_tables.py)
#                2. Dataflow Gen2 items run     → populates _staging tables
#                3. THIS notebook runs          → MERGE staging → gold
#                4. Semantic model refresh triggered
#
#              Why MERGE instead of letting Dataflow write directly?
#                - Dataflow Gen2 Replace method drops and recreates the target
#                  which would lose the precision DDL on every run.
#                - Staging → MERGE pattern preserves schema, enables
#                  SCD Type 1 updates, and allows row-level audit tracking.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws, lit, count,
    when
)
from delta.tables import DeltaTable
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

STAGING_SUFFIX = "_staging"

# ── Merge config: (gold_table, merge_keys, staging_table) ─────
# merge_keys: columns that uniquely identify a record in the gold table
MERGE_CONFIG = [
    {
        "gold_table":    "gold_model_health_scorecard",
        "staging_table": "gold_model_health_scorecard_staging",
        "merge_keys":    ["ItemName", "WorkspaceName"],
        "hash_cols":     ["health_score", "avg_refresh_success_rate",
                          "avg_dax_duration_ms", "health_grade"],
    },
    {
        "gold_table":    "gold_semantic_model_refresh_summary",
        "staging_table": "gold_semantic_model_refresh_summary_staging",
        "merge_keys":    ["log_date", "ItemName", "WorkspaceName"],
        "hash_cols":     ["success_count", "failure_count", "avg_duration_ms",
                          "success_rate_pct", "sla_status"],
    },
    {
        "gold_table":    "gold_semantic_model_dax_performance",
        "staging_table": "gold_semantic_model_dax_performance_staging",
        "merge_keys":    ["log_date", "ItemName", "WorkspaceName",
                          "executing_user", "app_name"],
        "hash_cols":     ["dax_execution_count", "avg_duration_ms",
                          "p95_duration_ms", "performance_tier"],
    },
    {
        "gold_table":    "gold_audit_user_activity_summary",
        "staging_table": "gold_audit_user_activity_summary_staging",
        "merge_keys":    ["log_date", "UserId", "Operation", "WorkSpaceName"],
        "hash_cols":     ["total_events", "success_events", "failure_events",
                          "success_rate_pct", "user_tier"],
    },
]

print(f"[Gold DML] Merge operations to run: {len(MERGE_CONFIG)}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Helper: Validate Staging Table is Populated
# ──────────────────────────────────────────────────────────────
def validate_staging(staging_table: str, gold_table: str) -> dict:
    """
    Checks staging table exists and has rows.
    Returns stats dict for logging.
    """
    try:
        df_stg    = spark.read.format("delta").table(staging_table)
        stg_rows  = df_stg.count()

        # Check gold table current state
        try:
            df_gold   = spark.read.format("delta").table(gold_table)
            gold_rows = df_gold.count()
        except Exception:
            gold_rows = 0

        return {
            "staging_rows":   stg_rows,
            "gold_rows_pre":  gold_rows,
            "is_valid":       stg_rows > 0,
            "warning":        stg_rows == 0,
        }
    except Exception as e:
        return {
            "staging_rows":   0,
            "gold_rows_pre":  0,
            "is_valid":       False,
            "error":          str(e),
        }


# ──────────────────────────────────────────────────────────────
# Cell 3 — Helper: Build MERGE Condition String
# ──────────────────────────────────────────────────────────────
def build_merge_condition(merge_keys: list) -> str:
    """Builds the ON clause for MERGE from the list of key columns."""
    return " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])


# ──────────────────────────────────────────────────────────────
# Cell 4 — Helper: Add Audit + Hash Columns to Staging DataFrame
# ──────────────────────────────────────────────────────────────
def enrich_staging(df, hash_cols: list):
    """
    Adds _row_hash, _dw_created_at, _dw_updated_at to staging data.
    _dw_created_at is set only on insert (preserved on update via MERGE).
    """
    df = df.withColumn(
        "_row_hash",
        sha2(concat_ws("|", *[col(c).cast("string") for c in hash_cols]), 256)
    )
    df = df.withColumn("_dw_updated_at", current_timestamp())
    # _dw_created_at set on first insert only — MERGE whenMatchedUpdate won't touch it
    df = df.withColumn("_dw_created_at", current_timestamp())
    return df


# ──────────────────────────────────────────────────────────────
# Cell 5 — Execute All MERGE Operations
# ──────────────────────────────────────────────────────────────
merge_results = []

for config in MERGE_CONFIG:
    gold_tbl    = config["gold_table"]
    staging_tbl = config["staging_table"]
    merge_keys  = config["merge_keys"]
    hash_cols   = config["hash_cols"]

    print(f"\n[Gold DML] MERGE: {staging_tbl} → {gold_tbl}")

    # ── Validate staging ──────────────────────────────────────
    validation = validate_staging(staging_tbl, gold_tbl)

    if not validation["is_valid"]:
        print(f"  ✖ Staging table invalid or empty — skipping MERGE")
        print(f"    Error: {validation.get('error', 'No rows in staging')}")
        merge_results.append({
            "gold_table": gold_tbl, "status": "SKIPPED",
            "staging_rows": 0, "rows_inserted": 0, "rows_updated": 0
        })
        continue

    stg_rows = validation["staging_rows"]
    print(f"  Staging rows: {stg_rows:,}  |  Gold rows (pre): {validation['gold_rows_pre']:,}")

    try:
        # ── Read and enrich staging ───────────────────────────
        df_staging = spark.read.format("delta").table(staging_tbl)
        df_staging = enrich_staging(df_staging, hash_cols)

        # ── Check if gold table exists as Delta ───────────────
        if DeltaTable.isDeltaTable(spark, f"Tables/{gold_tbl}"):
            gold_dt = DeltaTable.forName(spark, gold_tbl)

            merge_condition = build_merge_condition(merge_keys)

            (
                gold_dt.alias("tgt")
                .merge(
                    df_staging.alias("src"),
                    merge_condition
                )
                # Update existing rows only when data has changed (hash comparison)
                .whenMatchedUpdate(
                    condition="tgt._row_hash <> src._row_hash",
                    set={
                        c: f"src.{c}"
                        for c in df_staging.columns
                        if c not in ["_dw_created_at"]  # Preserve original created timestamp
                    }
                )
                # Insert new rows — sets _dw_created_at for the first time
                .whenNotMatchedInsertAll()
                .execute()
            )

            # ── Capture merge metrics ──────────────────────────
            gold_rows_post  = spark.read.format("delta").table(gold_tbl).count()
            rows_inserted   = gold_rows_post - validation["gold_rows_pre"]
            rows_updated    = stg_rows - max(rows_inserted, 0)  # approximate

            print(f"  ✔ MERGE complete | "
                  f"inserted={rows_inserted:,} | "
                  f"~updated={max(rows_updated,0):,} | "
                  f"gold_total={gold_rows_post:,}")

            merge_results.append({
                "gold_table":    gold_tbl,
                "status":        "SUCCESS",
                "staging_rows":  stg_rows,
                "rows_inserted": rows_inserted,
                "rows_updated":  max(rows_updated, 0),
                "gold_rows_post":gold_rows_post,
            })

        else:
            # Gold table doesn't exist yet — initial load
            df_staging.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "false") \
                .saveAsTable(gold_tbl)
            gold_rows_post = df_staging.count()
            print(f"  ✔ Initial load | rows={gold_rows_post:,}")

            merge_results.append({
                "gold_table":    gold_tbl,
                "status":        "INITIAL_LOAD",
                "staging_rows":  stg_rows,
                "rows_inserted": gold_rows_post,
                "rows_updated":  0,
                "gold_rows_post":gold_rows_post,
            })

    except Exception as e:
        print(f"  ✖ MERGE FAILED: {e}")
        merge_results.append({
            "gold_table": gold_tbl, "status": "FAILURE",
            "staging_rows": stg_rows, "rows_inserted": 0,
            "rows_updated": 0, "error": str(e)
        })


# ──────────────────────────────────────────────────────────────
# Cell 6 — Post-Merge Validation: Staging vs Gold Row Counts
# ──────────────────────────────────────────────────────────────
print("\n── Post-Merge Staging vs Gold Row Count Validation ──")
print(f"{'Gold Table':<50} {'Stg Rows':>10} {'Gold Post':>10} {'Status':>12}")
print("─" * 85)

for r in merge_results:
    stg  = r.get("staging_rows", 0)
    post = r.get("gold_rows_post", 0)

    # Gold can have MORE rows than staging (it accumulates history)
    # Flag only if staging > gold (would mean rows were lost)
    flag = "⚠ CHECK" if stg > post else "✔"
    print(f"  {r['gold_table']:<48} {stg:>10,} {post:>10,} {flag:>12}")


# ──────────────────────────────────────────────────────────────
# Cell 7 — Final Summary
# ──────────────────────────────────────────────────────────────
print("\n═══════════════════════════════════════════════════════════")
print("               GOLD DML MERGE SUMMARY                     ")
print("═══════════════════════════════════════════════════════════")
for r in merge_results:
    icon = "✔" if r["status"] in ("SUCCESS","INITIAL_LOAD") else \
           "⚠" if r["status"] == "SKIPPED" else "✖"
    print(f"  {icon} [{r['status']:12s}] {r['gold_table']:48s} "
          f"INS:{r.get('rows_inserted',0):>8,}  "
          f"UPD:{r.get('rows_updated',0):>8,}")
print("═══════════════════════════════════════════════════════════")

fail_count = sum(1 for r in merge_results if r["status"] == "FAILURE")
if fail_count > 0:
    mssparkutils.notebook.exit(f"PARTIAL_FAILURE: {fail_count} tables failed")
else:
    mssparkutils.notebook.exit("SUCCESS")
