# Fabric Notebook — Metadata Health Checks: Bronze / Silver / Gold
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Runs null-rate, column usage, measure usage, and top-value
#              profiling checks across all three medallion layers using
#              the metadata_master as a reference catalog.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when, isnan, isnull,
    round as spark_round, lit, current_timestamp
)
from pyspark.sql.types import DoubleType, LongType

spark = SparkSession.builder.getOrCreate()

# ── Tables to Health-Check per Layer ──────────────────────────
LAYER_CONFIG = {
    "BRONZE": [
        "bronze_semantic_model_logs",
    ],
    "SILVER": [
        "silver_semantic_model_logs_parsed",
    ],
    "GOLD": [
        "gold_semantic_model_refresh_summary",
        "gold_semantic_model_dax_performance",
        "gold_semantic_model_user_activity",
    ],
}

METADATA_TABLE     = "metadata_master"
HEALTH_RESULTS_TBL = "metadata_health_results"

print("[Health] Metadata health check starting across all layers")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Helper: Null / Missing Rate per Column
# ──────────────────────────────────────────────────────────────
def null_profile(df, table_name: str, layer: str):
    total = df.count()
    rows = []
    for c in df.columns:
        null_cnt = df.filter(isnull(col(c)) | (col(c).cast("string") == "")).count()
        null_rate = round(null_cnt / total * 100, 2) if total > 0 else 0.0
        rows.append((layer, table_name, c, total, null_cnt, null_rate))

    schema = ["layer", "table_name", "column_name", "total_rows",
              "null_count", "null_rate_pct"]
    return spark.createDataFrame(rows, schema)


# ──────────────────────────────────────────────────────────────
# Cell 3 — Run Null Profiling per Layer
# ──────────────────────────────────────────────────────────────
all_null_profiles = []

for layer, tables in LAYER_CONFIG.items():
    for tbl in tables:
        try:
            df = spark.read.format("delta").table(tbl)
            df_profile = null_profile(df, tbl, layer)
            all_null_profiles.append(df_profile)
            print(f"  ✔ [{layer}] {tbl} profiled")
        except Exception as e:
            print(f"  ✖ [{layer}] {tbl} failed: {e}")

from functools import reduce
from pyspark.sql import DataFrame

df_null_health = reduce(DataFrame.union, all_null_profiles)
df_null_health.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(HEALTH_RESULTS_TBL)

print(f"\n[Health] Null profile rows: {df_null_health.count():,}")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Columns with High Null Rate (> 30%)
# ──────────────────────────────────────────────────────────────
print("\n── Columns with Null Rate > 30% ──")
display(
    df_null_health
    .filter(col("null_rate_pct") > 30)
    .orderBy("null_rate_pct", ascending=False)
)


# ──────────────────────────────────────────────────────────────
# Cell 5 — Most Referenced Columns in Silver/Gold (from metadata_master)
# ──────────────────────────────────────────────────────────────
df_meta = spark.read.format("delta").table(METADATA_TABLE)

print("\n── Object Type Distribution ──")
display(df_meta.groupBy("object_type", "table_name").count().orderBy("table_name"))

print("\n── All Measures by Table ──")
display(
    df_meta.filter(col("object_type") == "MEASURE")
    .select("model_name", "table_name", "object_name")
    .orderBy("table_name", "object_name")
)

print("\n── Hidden Columns/Measures ──")
display(
    df_meta.filter(col("is_hidden") == True)
    .select("model_name", "table_name", "object_name", "object_type")
    .orderBy("table_name")
)


# ──────────────────────────────────────────────────────────────
# Cell 6 — Silver Log Column Usage in DAX Queries
# ──────────────────────────────────────────────────────────────
df_silver = spark.read.format("delta").table("silver_semantic_model_logs_parsed")

print("\n── Top Operation Names ──")
display(
    df_silver.groupBy("OperationName")
    .agg(count("*").alias("count"))
    .orderBy("count", ascending=False)
    .limit(20)
)

print("\n── Top ItemNames by Log Volume ──")
display(
    df_silver.groupBy("ItemName", "ModelMode")
    .agg(count("*").alias("log_count"))
    .orderBy("log_count", ascending=False)
    .limit(20)
)

print("\n── Average Duration by Operation ──")
display(
    df_silver.groupBy("OperationName")
    .agg(
        spark_round(col("DurationMs").cast(DoubleType()).alias("avg_ms"), 2),
        count("*").alias("count")
    )
    .orderBy("avg_ms", ascending=False)
    .limit(20)
)


# ──────────────────────────────────────────────────────────────
# Cell 7 — Bronze/Silver/Gold Row Counts Summary
# ──────────────────────────────────────────────────────────────
print("\n═══════════════════════════════════════════")
print("        MEDALLION LAYER ROW COUNTS         ")
print("═══════════════════════════════════════════")
for layer, tables in LAYER_CONFIG.items():
    for tbl in tables:
        try:
            rc = spark.read.format("delta").table(tbl).count()
            print(f"  [{layer:6s}] {tbl:55s} {rc:>10,}")
        except Exception as e:
            print(f"  [{layer:6s}] {tbl:55s}  ERROR: {e}")
print("═══════════════════════════════════════════")
