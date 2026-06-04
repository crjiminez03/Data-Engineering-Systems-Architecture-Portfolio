# Fabric Notebook — Row Count Validation: Pre vs Post Ingestion
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Validates ingestion integrity for Gold layer tables AND
#              SemanticModelLog tables. Catches silent load failures where
#              a pipeline reports SUCCESS but data is missing or partially loaded.
#              Compares source row counts vs destination row counts per batch,
#              flags discrepancies, and writes results to a validation log table.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, current_timestamp, abs as spark_abs,
    round as spark_round, when, to_date, max as spark_max
)
from datetime import datetime, timedelta, timezone

spark = SparkSession.builder.getOrCreate()

VALIDATION_LOG_TABLE = "ingestion_validation_log"

# ── Validation targets: (layer, source_table, destination_table, join_date_col) ──
VALIDATION_TARGETS = [
    # SemanticModelLogs pipeline
    {
        "pipeline":      "SemanticModelLogs",
        "layer":         "BRONZE→SILVER",
        "source_table":  "bronze_semantic_model_logs",
        "dest_table":    "silver_semantic_model_logs_parsed",
        "date_col_src":  "Timestamp",
        "date_col_dst":  "Timestamp",
        "key_col":       "OperationId",
    },
    {
        "pipeline":      "SemanticModelLogs",
        "layer":         "SILVER→GOLD_REFRESH",
        "source_table":  "silver_semantic_model_logs_parsed",
        "dest_table":    "gold_semantic_model_refresh_summary",
        "date_col_src":  "Timestamp",
        "date_col_dst":  "log_date",
        "key_col":       None,   # Aggregated table: compare day totals
    },
    {
        "pipeline":      "SemanticModelLogs",
        "layer":         "SILVER→GOLD_DAX",
        "source_table":  "silver_semantic_model_logs_parsed",
        "dest_table":    "gold_semantic_model_dax_performance",
        "date_col_src":  "Timestamp",
        "date_col_dst":  "log_date",
        "key_col":       None,
    },
    # AuditLogs pipeline
    {
        "pipeline":      "AuditLogs",
        "layer":         "BRONZE→SILVER",
        "source_table":  "bronze_audit_logs_powerbi",
        "dest_table":    "silver_audit_logs_powerbi",
        "date_col_src":  "CreationTime",
        "date_col_dst":  "CreationTime",
        "key_col":       "Id",
    },
]

# Window: how many days back to validate
VALIDATION_DAYS = 7

utc_now   = datetime.now(timezone.utc)
start_dt  = utc_now - timedelta(days=VALIDATION_DAYS)
start_str = start_dt.strftime("%Y-%m-%d")

print(f"[Validation] Running for past {VALIDATION_DAYS} days (from {start_str})")
print(f"[Validation] Targets: {len(VALIDATION_TARGETS)}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Core Validation Functions
# ──────────────────────────────────────────────────────────────
def get_daily_counts(table: str, date_col: str, start: str) -> dict:
    """
    Returns {date_str: row_count} for all days >= start.
    """
    try:
        df = (
            spark.read.format("delta").table(table)
            .withColumn("_date", to_date(col(date_col)))
            .filter(col("_date") >= start)
            .groupBy("_date")
            .agg(count("*").alias("row_count"))
        )
        return {
            row["_date"].strftime("%Y-%m-%d"): row["row_count"]
            for row in df.collect()
        }
    except Exception as e:
        print(f"  [ERROR] Could not read {table}: {e}")
        return {}


def get_distinct_key_counts(table: str, date_col: str, key_col: str, start: str) -> dict:
    """
    Returns {date_str: distinct_key_count} — more precise dedup check.
    """
    try:
        from pyspark.sql.functions import countDistinct
        df = (
            spark.read.format("delta").table(table)
            .withColumn("_date", to_date(col(date_col)))
            .filter(col("_date") >= start)
            .groupBy("_date")
            .agg(countDistinct(col(key_col)).alias("distinct_count"))
        )
        return {
            row["_date"].strftime("%Y-%m-%d"): row["distinct_count"]
            for row in df.collect()
        }
    except Exception as e:
        print(f"  [ERROR] Distinct key count failed for {table}: {e}")
        return {}


# ──────────────────────────────────────────────────────────────
# Cell 3 — Run Validation per Target
# ──────────────────────────────────────────────────────────────
results = []

for target in VALIDATION_TARGETS:
    pipeline    = target["pipeline"]
    layer       = target["layer"]
    src_tbl     = target["source_table"]
    dst_tbl     = target["dest_table"]
    src_date    = target["date_col_src"]
    dst_date    = target["date_col_dst"]
    key_col     = target["key_col"]

    print(f"\n[Validation] {pipeline} | {layer}")
    print(f"  Source: {src_tbl}  →  Dest: {dst_tbl}")

    src_counts = get_daily_counts(src_tbl, src_date, start_str)
    dst_counts = get_daily_counts(dst_tbl, dst_date, start_str)

    # If key column available: also check distinct key counts
    src_keys = get_distinct_key_counts(src_tbl, src_date, key_col, start_str) \
               if key_col else {}
    dst_keys = get_distinct_key_counts(dst_tbl, dst_date, key_col, start_str) \
               if key_col else {}

    # All dates that appear in either
    all_dates = sorted(set(list(src_counts.keys()) + list(dst_counts.keys())), reverse=True)

    for dt in all_dates:
        src_rc  = src_counts.get(dt, 0)
        dst_rc  = dst_counts.get(dt, 0)
        src_key = src_keys.get(dt, None)
        dst_key = dst_keys.get(dt, None)

        diff        = dst_rc - src_rc
        pct_loaded  = round(dst_rc / src_rc * 100, 2) if src_rc > 0 else None

        # ── Status classification ──────────────────────────────
        if src_rc == 0 and dst_rc == 0:
            status = "NO_DATA"
        elif src_rc == 0 and dst_rc > 0:
            status = "DEST_ONLY"          # Orphan rows in dest
        elif dst_rc == 0 and src_rc > 0:
            status = "CRITICAL_MISSING"   # Nothing loaded
        elif pct_loaded and pct_loaded < 95:
            status = "WARN_SHORT_LOAD"    # > 5% missing
        elif pct_loaded and pct_loaded > 105:
            status = "WARN_OVER_LOADED"   # Duplication possible
        else:
            status = "PASS"

        key_diff = (dst_key - src_key) if (src_key is not None and dst_key is not None) else None

        results.append({
            "validated_at":         datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pipeline":             pipeline,
            "layer":                layer,
            "batch_date":           dt,
            "source_table":         src_tbl,
            "destination_table":    dst_tbl,
            "source_row_count":     src_rc,
            "destination_row_count":dst_rc,
            "row_diff":             diff,
            "pct_loaded":           pct_loaded,
            "source_distinct_keys": src_key,
            "dest_distinct_keys":   dst_key,
            "key_diff":             key_diff,
            "validation_status":    status,
        })

        icon = "✔" if status == "PASS" else "⚠" if "WARN" in status else "✖"
        print(f"  {icon} {dt}  src={src_rc:>8,}  dst={dst_rc:>8,}  "
              f"pct={str(pct_loaded)+'%':>8}  [{status}]")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Write Validation Results to Delta Log Table
# ──────────────────────────────────────────────────────────────
df_results = spark.createDataFrame(results)

df_results.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(VALIDATION_LOG_TABLE)

print(f"\n[Validation] Results written to {VALIDATION_LOG_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Highlight Failures & Warnings
# ──────────────────────────────────────────────────────────────
df_val = spark.read.format("delta").table(VALIDATION_LOG_TABLE)

print("\n══════════════════════════════════════════════════════════")
print("              VALIDATION RESULTS SUMMARY                 ")
print("══════════════════════════════════════════════════════════")

print("\n── CRITICAL / WARNING Rows ──")
display(
    df_val
    .filter(col("validation_status").isin(
        "CRITICAL_MISSING", "WARN_SHORT_LOAD", "WARN_OVER_LOADED", "DEST_ONLY"
    ))
    .select("batch_date", "pipeline", "layer",
            "source_row_count", "destination_row_count",
            "pct_loaded", "validation_status")
    .orderBy("batch_date", ascending=False)
)

print("\n── All Results (last 7 days) ──")
display(
    df_val
    .filter(col("batch_date") >= start_str)
    .select("batch_date", "pipeline", "layer",
            "source_row_count", "destination_row_count",
            "row_diff", "pct_loaded",
            "source_distinct_keys", "dest_distinct_keys",
            "key_diff", "validation_status")
    .orderBy("batch_date", ascending=False, secondary="pipeline")
)

print("\n── Status Count Summary ──")
display(df_val.groupBy("validation_status", "pipeline").count().orderBy("pipeline"))


# ──────────────────────────────────────────────────────────────
# Cell 6 — Silent Failure Detection (Success status but data missing)
# ──────────────────────────────────────────────────────────────
# Cross-reference: Check if silver logs show "SUCCESS" for a day
# but row counts don't match — the hidden failure scenario.
print("\n── Silent Failure Check: SUCCESS status vs actual loaded rows ──")

try:
    df_silver_ops = spark.read.format("delta").table("silver_semantic_model_logs_parsed")
    df_silver_date = (
        df_silver_ops
        .withColumn("log_date", to_date(col("Timestamp")))
        .filter(col("log_date") >= start_str)
        .groupBy("log_date", "ItemName", "OperationName")
        .agg(
            count("*").alias("total_ops"),
            count(when(col("Status") == "SUCCESS", 1)).alias("success_ops"),
            count(when(col("Status") == "FAILURE", 1)).alias("failed_ops"),
        )
        .withColumn("reported_success_rate",
            spark_round(col("success_ops") / col("total_ops") * 100, 2)
        )
    )

    # Join with validation results to flag days that look "green" but had low loads
    df_silent_check = (
        df_silver_date.alias("ops")
        .join(
            df_val.filter(col("pipeline") == "SemanticModelLogs")
                  .alias("val"),
            df_silver_date["log_date"].cast("string") == df_val["batch_date"],
            "left"
        )
        .filter(
            (col("ops.reported_success_rate") >= 95) &
            (col("val.pct_loaded") < 95)
        )
        .select(
            "ops.log_date", "ops.ItemName", "ops.OperationName",
            "ops.total_ops", "ops.reported_success_rate",
            "val.source_row_count", "val.destination_row_count",
            "val.pct_loaded", "val.validation_status"
        )
    )

    silent_count = df_silent_check.count()
    if silent_count > 0:
        print(f"  ⚠ SILENT FAILURES DETECTED: {silent_count} cases where status=SUCCESS but data missing!")
        display(df_silent_check)
    else:
        print("  ✔ No silent failures detected in this window.")

except Exception as e:
    print(f"  [Silent Check] Skipped: {e}")
