# Fabric Notebook — Column Profiling: Health Impact Analysis
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Deep statistical profiling of columns across bronze/silver/gold
#              layers. Identifies cardinality, skew, outliers, and data quality
#              issues that could impact downstream reporting or performance.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, isnull, isnan, avg, stddev,
    min as spark_min, max as spark_max, percentile_approx,
    round as spark_round, lit, current_timestamp, when,
    length, trim
)
from pyspark.sql.types import (
    NumericType, StringType, TimestampType, LongType, DoubleType, IntegerType
)

spark = SparkSession.builder.getOrCreate()

# Tables to profile
PROFILE_TARGETS = {
    "BRONZE": "bronze_semantic_model_logs",
    "SILVER": "silver_semantic_model_logs_parsed",
    "GOLD_REFRESH": "gold_semantic_model_refresh_summary",
    "GOLD_DAX":     "gold_semantic_model_dax_performance",
    "GOLD_USERS":   "gold_semantic_model_user_activity",
}

PROFILING_OUTPUT = "column_profiling_results"
print("[Profile] Column profiling notebook initialised")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Helper: Full Column Profile
# ──────────────────────────────────────────────────────────────
def profile_dataframe(df, table_name: str, layer: str):
    """
    Returns a list of dicts with per-column profile stats.
    Numeric columns: min/max/avg/stddev/p25/p50/p75
    String columns:  min_len/max_len/avg_len/top cardinality
    All columns:     null_count, null_pct, distinct_count
    """
    total = df.count()
    results = []

    for c in df.columns:
        dtype = df.schema[c].dataType

        # ── Null stats (all types) ─────────────────────────────
        null_cnt      = df.filter(isnull(col(c))).count()
        blank_cnt     = df.filter(col(c).cast("string") == "").count() \
                          if not isinstance(dtype, (LongType, DoubleType, IntegerType)) else 0
        distinct_cnt  = df.select(countDistinct(col(c))).collect()[0][0]
        null_pct      = round(null_cnt / total * 100, 2) if total > 0 else 0.0

        row = {
            "layer":          layer,
            "table_name":     table_name,
            "column_name":    c,
            "data_type":      str(dtype),
            "total_rows":     total,
            "null_count":     null_cnt,
            "blank_count":    blank_cnt,
            "null_rate_pct":  null_pct,
            "distinct_count": distinct_cnt,
            "cardinality_pct": round(distinct_cnt / total * 100, 2) if total > 0 else 0.0,
            # Numeric stats (populated below if numeric)
            "min_val":    None,
            "max_val":    None,
            "avg_val":    None,
            "stddev_val": None,
            "p25":        None,
            "p50_median": None,
            "p75":        None,
            # String stats
            "min_length": None,
            "max_length": None,
            "avg_length": None,
            # Health flags
            "health_flag": "OK",
            "health_notes": "",
        }

        # ── Numeric Profiling ──────────────────────────────────
        if isinstance(dtype, (LongType, DoubleType, IntegerType)):
            df_non_null = df.filter(col(c).isNotNull())
            if df_non_null.count() > 0:
                stats = df_non_null.select(
                    spark_round(spark_min(col(c)).cast(DoubleType()), 4).alias("mn"),
                    spark_round(spark_max(col(c)).cast(DoubleType()), 4).alias("mx"),
                    spark_round(avg(col(c).cast(DoubleType())),        4).alias("av"),
                    spark_round(stddev(col(c).cast(DoubleType())),      4).alias("sd"),
                    percentile_approx(col(c).cast(DoubleType()), 0.25).alias("p25"),
                    percentile_approx(col(c).cast(DoubleType()), 0.50).alias("p50"),
                    percentile_approx(col(c).cast(DoubleType()), 0.75).alias("p75"),
                ).collect()[0]
                row.update({
                    "min_val": stats["mn"], "max_val": stats["mx"],
                    "avg_val": stats["av"], "stddev_val": stats["sd"],
                    "p25": stats["p25"], "p50_median": stats["p50"], "p75": stats["p75"]
                })
                # Flag extreme skew: stddev >> avg
                if stats["av"] and stats["sd"] and stats["av"] != 0:
                    cv = abs(stats["sd"] / stats["av"])
                    if cv > 3:
                        row["health_flag"] = "WARN"
                        row["health_notes"] += "High variance (CV>3). "

        # ── String Profiling ───────────────────────────────────
        elif isinstance(dtype, StringType):
            df_s = df.filter(col(c).isNotNull() & (trim(col(c)) != ""))
            if df_s.count() > 0:
                str_stats = df_s.select(
                    spark_min(length(col(c))).alias("mn_len"),
                    spark_max(length(col(c))).alias("mx_len"),
                    spark_round(avg(length(col(c)).cast(DoubleType())), 2).alias("av_len"),
                ).collect()[0]
                row.update({
                    "min_length": str_stats["mn_len"],
                    "max_length": str_stats["mx_len"],
                    "avg_length": str_stats["av_len"],
                })

        # ── Health Flags ───────────────────────────────────────
        if null_pct > 80:
            row["health_flag"] = "CRITICAL"
            row["health_notes"] += f"Null rate {null_pct}% is very high. "
        elif null_pct > 30:
            row["health_flag"] = "WARN" if row["health_flag"] == "OK" else row["health_flag"]
            row["health_notes"] += f"Null rate {null_pct}% is elevated. "

        if distinct_cnt == 1:
            row["health_flag"] = "WARN" if row["health_flag"] == "OK" else row["health_flag"]
            row["health_notes"] += "Only 1 distinct value (possible constant). "

        if distinct_cnt == total and total > 1000:
            row["health_notes"] += "100% cardinality (likely ID/key column). "

        results.append(row)

    return results


# ──────────────────────────────────────────────────────────────
# Cell 3 — Run Profiling Across All Targets
# ──────────────────────────────────────────────────────────────
all_results = []

for layer, tbl in PROFILE_TARGETS.items():
    try:
        df = spark.read.format("delta").table(tbl)
        print(f"[Profile] Profiling [{layer}] {tbl} ({df.count():,} rows, {len(df.columns)} cols)...")
        rows = profile_dataframe(df, tbl, layer)
        all_results.extend(rows)
        print(f"  ✔ Done — {len(rows)} column profiles generated")
    except Exception as e:
        print(f"  ✖ [{layer}] {tbl} failed: {e}")

# Write to Delta
df_profiling = spark.createDataFrame(all_results)
df_profiling = df_profiling.withColumn("_profiled_at", current_timestamp())
df_profiling.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(PROFILING_OUTPUT)

print(f"\n[Profile] {PROFILING_OUTPUT} written → {df_profiling.count():,} column profiles")


# ──────────────────────────────────────────────────────────────
# Cell 4 — View Critical & Warning Flags
# ──────────────────────────────────────────────────────────────
print("\n── CRITICAL Health Flags ──")
display(
    df_profiling.filter(col("health_flag") == "CRITICAL")
    .select("layer", "table_name", "column_name", "null_rate_pct", "health_notes")
    .orderBy("null_rate_pct", ascending=False)
)

print("\n── WARNING Health Flags ──")
display(
    df_profiling.filter(col("health_flag") == "WARN")
    .select("layer", "table_name", "column_name", "null_rate_pct",
            "distinct_count", "health_notes")
    .orderBy("null_rate_pct", ascending=False)
)


# ──────────────────────────────────────────────────────────────
# Cell 5 — Numeric Column Distribution (Silver)
# ──────────────────────────────────────────────────────────────
print("\n── Numeric Column Stats (Silver Layer) ──")
display(
    df_profiling
    .filter((col("layer") == "SILVER") & col("avg_val").isNotNull())
    .select("column_name", "min_val", "p25", "p50_median", "avg_val",
            "p75", "max_val", "stddev_val", "null_rate_pct")
    .orderBy("column_name")
)


# ──────────────────────────────────────────────────────────────
# Cell 6 — Top Cardinality Columns (useful for indexing/partitioning)
# ──────────────────────────────────────────────────────────────
print("\n── Top 10 Highest Cardinality Columns ──")
display(
    df_profiling
    .select("layer", "table_name", "column_name",
            "distinct_count", "cardinality_pct", "total_rows")
    .orderBy("distinct_count", ascending=False)
    .limit(10)
)
