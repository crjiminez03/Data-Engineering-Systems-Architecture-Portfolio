# Fabric Notebook — Column Profiling v2: Metadata-Driven
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Upgraded column profiling that uses metadata_master_full as
#              the catalog of record. Instead of profiling arbitrary tables,
#              it iterates the master metadata to profile every column that
#              has a confirmed lakehouse mapping, then writes results back
#              to column_profiling_results with full semantic model lineage
#              attached — so you can see exactly which measures/reports
#              depend on a column with data quality issues.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, isnull, avg, stddev,
    min as spark_min, max as spark_max, percentile_approx,
    round as spark_round, lit, current_timestamp, when, length, trim
)
from pyspark.sql.types import LongType, DoubleType, IntegerType, StringType
from functools import reduce

spark = SparkSession.builder.getOrCreate()

METADATA_TABLE    = "metadata_master_full"
PROFILING_OUTPUT  = "column_profiling_results_v2"

print("[Profile v2] Metadata-driven column profiling starting")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Load Metadata Master to Determine What to Profile
# ──────────────────────────────────────────────────────────────
df_meta = spark.read.format("delta").table(METADATA_TABLE)

# Only profile columns that have a confirmed physical lakehouse mapping
# and are not pure DAX measures (no physical column to profile)
df_profile_targets = (
    df_meta
    .filter(
        (col("has_lakehouse_mapping") == True) &
        (col("object_type") == "COLUMN") &
        col("lh_table_name").isNotNull() &
        col("lh_column_name").isNotNull()
    )
    .select(
        "lh_layer", "lakehouse_name", "lh_table_name",
        "lh_column_name", "lh_data_type", "lh_is_nullable",
        "is_partition_col", "sem_table_name", "sem_column_name",
        "sem_data_type", "sem_is_hidden", "sem_is_key",
        "model_name", "data_type_alignment", "lh_table_row_count"
    )
    .dropDuplicates(["lh_table_name", "lh_column_name", "lh_layer"])
)

target_count = df_profile_targets.count()
print(f"[Profile v2] Columns to profile: {target_count:,}")

# Group by table to avoid re-reading the same table multiple times
from collections import defaultdict
table_col_map = defaultdict(list)

for row in df_profile_targets.collect():
    key = (row["lh_layer"], row["lh_table_name"])
    table_col_map[key].append(row.asDict())

print(f"[Profile v2] Distinct tables to scan: {len(table_col_map)}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Profile Function (per column, with metadata context)
# ──────────────────────────────────────────────────────────────
def profile_column(df, meta: dict, total_rows: int) -> dict:
    """Profile a single column and return a result dict with metadata context."""
    c         = meta["lh_column_name"]
    dtype_str = meta["lh_data_type"].lower() if meta["lh_data_type"] else ""

    # ── Null / blank stats ──────────────────────────────────────
    null_cnt  = df.filter(isnull(col(c))).count()
    blank_cnt = df.filter(col(c).cast("string") == "").count() \
                  if "string" in dtype_str or "varchar" in dtype_str else 0
    dist_cnt  = df.select(countDistinct(col(c))).collect()[0][0]
    null_pct  = round(null_cnt / total_rows * 100, 2) if total_rows > 0 else 0.0

    result = {
        # ── Lineage context ──────────────────────────────────────
        "model_name":          meta["model_name"],
        "sem_table_name":      meta["sem_table_name"],
        "sem_column_name":     meta["sem_column_name"],
        "sem_is_hidden":       meta["sem_is_hidden"],
        "sem_is_key":          meta["sem_is_key"],
        "data_type_alignment": meta["data_type_alignment"],
        # ── Physical location ────────────────────────────────────
        "lh_layer":            meta["lh_layer"],
        "lh_table_name":       meta["lh_table_name"],
        "lh_column_name":      c,
        "lh_data_type":        meta["lh_data_type"],
        "is_partition_col":    meta["is_partition_col"],
        # ── Core stats ───────────────────────────────────────────
        "total_rows":          total_rows,
        "null_count":          null_cnt,
        "blank_count":         blank_cnt,
        "null_rate_pct":       null_pct,
        "distinct_count":      dist_cnt,
        "cardinality_pct":     round(dist_cnt / total_rows * 100, 2) if total_rows > 0 else 0.0,
        # Numeric stats
        "min_val": None, "max_val": None, "avg_val": None,
        "stddev_val": None, "p25": None, "p50_median": None, "p75": None,
        # String stats
        "min_length": None, "max_length": None, "avg_length": None,
        # Health
        "health_flag": "OK",
        "health_notes": "",
    }

    # ── Numeric profiling ────────────────────────────────────────
    if any(t in dtype_str for t in ["int", "long", "double", "float", "decimal", "numeric"]):
        df_nn = df.filter(col(c).isNotNull())
        if df_nn.count() > 0:
            try:
                stats = df_nn.select(
                    spark_round(spark_min(col(c).cast(DoubleType())), 4).alias("mn"),
                    spark_round(spark_max(col(c).cast(DoubleType())), 4).alias("mx"),
                    spark_round(avg(col(c).cast(DoubleType())),        4).alias("av"),
                    spark_round(stddev(col(c).cast(DoubleType())),      4).alias("sd"),
                    percentile_approx(col(c).cast(DoubleType()), 0.25).alias("p25"),
                    percentile_approx(col(c).cast(DoubleType()), 0.50).alias("p50"),
                    percentile_approx(col(c).cast(DoubleType()), 0.75).alias("p75"),
                ).collect()[0]
                result.update({
                    "min_val": stats["mn"], "max_val": stats["mx"],
                    "avg_val": stats["av"], "stddev_val": stats["sd"],
                    "p25": stats["p25"], "p50_median": stats["p50"], "p75": stats["p75"]
                })
                if stats["av"] and stats["sd"] and stats["av"] != 0:
                    if abs(stats["sd"] / stats["av"]) > 3:
                        result["health_flag"]  = "WARN"
                        result["health_notes"] += "High variance (CV>3). "
            except Exception as e:
                result["health_notes"] += f"Numeric profile error: {e}. "

    # ── String profiling ─────────────────────────────────────────
    elif "string" in dtype_str or "varchar" in dtype_str or "char" in dtype_str:
        df_s = df.filter(col(c).isNotNull() & (trim(col(c)) != ""))
        if df_s.count() > 0:
            try:
                ss = df_s.select(
                    spark_min(length(col(c))).alias("mn"),
                    spark_max(length(col(c))).alias("mx"),
                    spark_round(avg(length(col(c)).cast(DoubleType())), 2).alias("av"),
                ).collect()[0]
                result.update({
                    "min_length": ss["mn"],
                    "max_length": ss["mx"],
                    "avg_length": ss["av"],
                })
            except Exception as e:
                result["health_notes"] += f"String profile error: {e}. "

    # ── Health flags ─────────────────────────────────────────────
    if null_pct > 80:
        result["health_flag"]  = "CRITICAL"
        result["health_notes"] += f"Null rate {null_pct}% very high. "
    elif null_pct > 30:
        result["health_flag"]  = "WARN" if result["health_flag"] == "OK" else result["health_flag"]
        result["health_notes"] += f"Null rate {null_pct}% elevated. "

    if dist_cnt == 1:
        result["health_flag"]  = "WARN" if result["health_flag"] == "OK" else result["health_flag"]
        result["health_notes"] += "Only 1 distinct value. "

    # Flag key columns with nulls — these are data integrity issues
    if meta["sem_is_key"] and null_cnt > 0:
        result["health_flag"]  = "CRITICAL"
        result["health_notes"] += f"KEY COLUMN has {null_cnt:,} nulls — integrity risk. "

    # Type mismatch between semantic model and lakehouse
    if meta["data_type_alignment"] == "TYPE_MISMATCH":
        result["health_flag"]  = "WARN" if result["health_flag"] == "OK" else result["health_flag"]
        result["health_notes"] += f"Type mismatch: sem={meta['sem_data_type']} vs lh={meta['lh_data_type']}. "

    return result


# ──────────────────────────────────────────────────────────────
# Cell 4 — Run Profiling
# ──────────────────────────────────────────────────────────────
all_results = []

for (layer, table_name), col_metas in table_col_map.items():
    try:
        full_ref = f"{col_metas[0]['lakehouse_name']}.{table_name}"
        df       = spark.read.format("delta").table(full_ref)
        total    = df.count()
        print(f"\n[Profile v2] [{layer}] {table_name} — {total:,} rows, {len(col_metas)} cols to profile")

        for meta in col_metas:
            c = meta["lh_column_name"]
            if c not in df.columns:
                print(f"  ⚠ Column '{c}' not in DataFrame — skipping")
                continue
            result = profile_column(df, meta, total)
            all_results.append(result)

            icon = {"OK": "✔", "WARN": "⚠", "CRITICAL": "✖"}.get(result["health_flag"], "?")
            print(f"  {icon} [{result['health_flag']:8s}] {c}  "
                  f"null={result['null_rate_pct']}%  "
                  f"distinct={result['distinct_count']:,}")

    except Exception as e:
        print(f"  ✖ [{layer}] {table_name}: {e}")

print(f"\n[Profile v2] Total column profiles generated: {len(all_results):,}")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Write Results
# ──────────────────────────────────────────────────────────────
df_profile_out = spark.createDataFrame(all_results)
df_profile_out = df_profile_out.withColumn("_profiled_at", current_timestamp())

df_profile_out.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(PROFILING_OUTPUT)

print(f"[Profile v2] {PROFILING_OUTPUT} written → {df_profile_out.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Summary Dashboard
# ──────────────────────────────────────────────────────────────
df_out = spark.read.format("delta").table(PROFILING_OUTPUT)

print("\n── Health Flag Distribution ──")
display(df_out.groupBy("health_flag", "lh_layer").count().orderBy("health_flag", "lh_layer"))

print("\n── CRITICAL Issues (action required) ──")
display(
    df_out.filter(col("health_flag") == "CRITICAL")
    .select("lh_layer", "lh_table_name", "lh_column_name",
            "sem_table_name", "sem_is_key",
            "null_rate_pct", "data_type_alignment", "health_notes")
    .orderBy("null_rate_pct", ascending=False)
)

print("\n── Type Mismatches (semantic model vs physical) ──")
display(
    df_out.filter(col("data_type_alignment") == "TYPE_MISMATCH")
    .select("sem_table_name", "sem_column_name", "sem_data_type",
            "lh_table_name", "lh_column_name", "lh_data_type", "lh_layer")
    .orderBy("sem_table_name")
)

print("\n── Key Columns with Nulls (integrity risks) ──")
display(
    df_out.filter((col("sem_is_key") == True) & (col("null_count") > 0))
    .select("model_name", "sem_table_name", "sem_column_name",
            "lh_table_name", "null_count", "null_rate_pct", "health_notes")
    .orderBy("null_rate_pct", ascending=False)
)

print("\n── Numeric Column Distribution (Silver layer) ──")
display(
    df_out.filter((col("lh_layer") == "SILVER") & col("avg_val").isNotNull())
    .select("lh_table_name", "lh_column_name",
            "min_val", "p25", "p50_median", "avg_val", "p75", "max_val",
            "stddev_val", "null_rate_pct")
    .orderBy("lh_table_name", "lh_column_name")
)
