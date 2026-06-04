# Fabric Notebook — Semantic Model System Tables (DAX Studio Export)
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Reads system tables exported from DAX Studio against the .pbix
#              semantic model (Partitions, Tables, Columns, Measures, StorageTables)
#              and builds a unified Metadata Master Delta table.
#
# Prerequisites:
#   - Export system tables from DAX Studio: $SYSTEM.TMSCHEMA_* and $SYSTEM.DISCOVER_STORAGE_*
#     File → Advanced → Export System Tables → saves as CSV per table
#   - Upload CSVs to Fabric Lakehouse Files/system_tables/

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, lit, current_timestamp,
    when, concat_ws, sha2
)
from pyspark.sql.types import StringType, LongType, BooleanType, IntegerType

spark = SparkSession.builder.getOrCreate()

# ── Configuration ──────────────────────────────────────────────
SRC_PATH       = "Files/system_tables"          # Lakehouse Files path
MODEL_NAME     = "YourModelName"                # <-- Replace with your model name
METADATA_TABLE = "metadata_master"

# ── Source CSV file names (as exported by DAX Studio) ─────────
SRC_TABLES       = f"{SRC_PATH}/TMSCHEMA_TABLES.csv"
SRC_COLUMNS      = f"{SRC_PATH}/TMSCHEMA_COLUMNS.csv"
SRC_MEASURES     = f"{SRC_PATH}/TMSCHEMA_MEASURES.csv"
SRC_PARTITIONS   = f"{SRC_PATH}/TMSCHEMA_PARTITIONS.csv"
SRC_STORAGE_TBLS = f"{SRC_PATH}/DISCOVER_STORAGE_TABLE_COLUMNS.csv"

print(f"[Metadata] Reading system tables for model: {MODEL_NAME}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Read System Tables
# ──────────────────────────────────────────────────────────────
def read_csv(path: str, label: str):
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("multiLine", "true")
          .option("escape", '"')
          .load(path))
    print(f"  [{label}] rows: {df.count():,}  cols: {len(df.columns)}")
    return df

df_tables       = read_csv(SRC_TABLES,       "TABLES")
df_columns      = read_csv(SRC_COLUMNS,      "COLUMNS")
df_measures     = read_csv(SRC_MEASURES,     "MEASURES")
df_partitions   = read_csv(SRC_PARTITIONS,   "PARTITIONS")
df_storage_tbls = read_csv(SRC_STORAGE_TBLS, "STORAGE_TABLES")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Standardise Column Names (strip spaces → snake_case)
# ──────────────────────────────────────────────────────────────
import re

def clean_col_names(df):
    for c in df.columns:
        cleaned = re.sub(r'\s+', '_', c.strip()).lower()
        df = df.withColumnRenamed(c, cleaned)
    return df

df_tables       = clean_col_names(df_tables)
df_columns      = clean_col_names(df_columns)
df_measures     = clean_col_names(df_measures)
df_partitions   = clean_col_names(df_partitions)
df_storage_tbls = clean_col_names(df_storage_tbls)

print("[Metadata] Column names normalised.")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Build Metadata Master (Columns + Tables joined)
# ──────────────────────────────────────────────────────────────

# Join columns to their parent table
df_col_meta = (
    df_columns
    .join(df_tables.select("id", "name").withColumnRenamed("name", "table_name")
                                         .withColumnRenamed("id",   "table_id"),
          df_columns["tableid"] == col("table_id"), "left")
    .withColumn("model_name",      lit(MODEL_NAME))
    .withColumn("object_type",     lit("COLUMN"))
    .withColumn("_metadata_date",  current_timestamp())
    .select(
        lit(MODEL_NAME).alias("model_name"),
        col("table_name"),
        col("explicitname").alias("object_name"),
        col("datatype").alias("data_type"),
        col("isavailableinmdx").alias("is_available_mdx"),
        col("ishidden").alias("is_hidden"),
        col("iskey").alias("is_key"),
        col("columntype").alias("column_type"),
        col("encodingtype").alias("encoding_type"),
        lit("COLUMN").alias("object_type"),
        col("_metadata_date")
    )
)

# Measures
df_meas_meta = (
    df_measures
    .join(df_tables.select("id", "name").withColumnRenamed("name", "table_name")
                                         .withColumnRenamed("id",   "table_id"),
          df_measures["tableid"] == col("table_id"), "left")
    .withColumn("_metadata_date", current_timestamp())
    .select(
        lit(MODEL_NAME).alias("model_name"),
        col("table_name"),
        col("name").alias("object_name"),
        lit("DECIMAL").alias("data_type"),
        lit(None).cast(StringType()).alias("is_available_mdx"),
        col("ishidden").alias("is_hidden"),
        lit(None).cast(StringType()).alias("is_key"),
        lit(None).cast(StringType()).alias("column_type"),
        lit(None).cast(StringType()).alias("encoding_type"),
        lit("MEASURE").alias("object_type"),
        col("_metadata_date")
    )
)

# Union all metadata
df_meta_master = df_col_meta.union(df_meas_meta)
df_meta_master.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(METADATA_TABLE)
print(f"[Metadata] {METADATA_TABLE} written → {df_meta_master.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Save Individual System Tables as Delta
# ──────────────────────────────────────────────────────────────
system_table_map = {
    "sys_tables":       df_tables.withColumn("model_name", lit(MODEL_NAME)),
    "sys_columns":      df_columns.withColumn("model_name", lit(MODEL_NAME)),
    "sys_measures":     df_measures.withColumn("model_name", lit(MODEL_NAME)),
    "sys_partitions":   df_partitions.withColumn("model_name", lit(MODEL_NAME)),
    "sys_storage_tbls": df_storage_tbls.withColumn("model_name", lit(MODEL_NAME)),
}

for tbl_name, df in system_table_map.items():
    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(tbl_name)
    print(f"  ✔ {tbl_name} → {df.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Quick Explore
# ──────────────────────────────────────────────────────────────
print("\n── Tables in Model ──")
display(df_tables.select("name", "description", "ishidden", "ispartitionable").orderBy("name"))

print("\n── Columns per Table ──")
display(
    df_col_meta.groupBy("table_name", "object_type").count()
    .orderBy("table_name")
)

print("\n── All Measures ──")
display(df_meas_meta.select("table_name", "object_name").orderBy("table_name", "object_name"))
