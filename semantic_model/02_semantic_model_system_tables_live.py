# Fabric Notebook — Semantic Model System Tables via Live PBIX Connection
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Connects directly to the semantic model (.pbix) via its
#              XMLA endpoint using semantic-link (sempy) — no CSV exports,
#              no manual DAX Studio steps. Pulls all system tables live:
#                - $SYSTEM.TMSCHEMA_TABLES
#                - $SYSTEM.TMSCHEMA_COLUMNS
#                - $SYSTEM.TMSCHEMA_MEASURES
#                - $SYSTEM.TMSCHEMA_PARTITIONS
#                - $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS (storage/size stats)
#              Writes each as a Delta table and builds the metadata_master
#              so it refreshes automatically as part of the Gold pipeline.
#
# Library: semantic-link (sempy) — Microsoft's official Fabric library
#          for connecting to Power BI semantic models from notebooks.
#          Install: %pip install semantic-link
#
# Connection: Uses the workspace + dataset (semantic model) name.
#             No credentials needed — inherits notebook identity via
#             Fabric managed identity / service principal.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Install & Import
# ──────────────────────────────────────────────────────────────
# %pip install semantic-link --quiet   # Uncomment on first run

import sempy.fabric as fabric
from sempy.fabric import FabricDataFrame
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, current_timestamp,
    coalesce, when, sha2, concat_ws
)
from pyspark.sql.types import StringType
import re

spark = SparkSession.builder.getOrCreate()

# ── Configuration ──────────────────────────────────────────────
WORKSPACE_NAME  = "YourFabricWorkspaceName"      # <-- Replace
DATASET_NAME    = "YourSemanticModelName"         # <-- Replace (.pbix name without extension)

# Output Delta tables
OUT_TABLES      = "sys_sem_tables"
OUT_COLUMNS     = "sys_sem_columns"
OUT_MEASURES    = "sys_sem_measures"
OUT_PARTITIONS  = "sys_sem_partitions"
OUT_STORAGE     = "sys_sem_storage_columns"
OUT_META_MASTER = "metadata_master"

print(f"[SysTable] Connecting to semantic model: '{DATASET_NAME}'")
print(f"[SysTable] Workspace: '{WORKSPACE_NAME}'")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Helper: Execute DMV / System Query via sempy
# ──────────────────────────────────────────────────────────────
def run_dax_system_query(workspace: str, dataset: str, query: str,
                          label: str) -> pd.DataFrame:
    """
    Executes a DAX or DMV query against the semantic model
    using sempy's evaluate_dax() for DAX or the XMLA DMV path.
    Returns a pandas DataFrame.
    """
    try:
        # sempy.fabric.evaluate_dax sends DAX to the XMLA endpoint
        df_pd = fabric.evaluate_dax(
            workspace=workspace,
            dataset=dataset,
            dax_string=query
        )
        print(f"  [{label}] rows: {len(df_pd):,}  cols: {len(df_pd.columns)}")
        return df_pd
    except Exception as e:
        print(f"  [{label}] ERROR: {e}")
        return pd.DataFrame()


def clean_col_names(df_pd: pd.DataFrame) -> pd.DataFrame:
    """Normalise column names to snake_case."""
    df_pd.columns = [
        re.sub(r'\W+', '_', c.strip()).lower().strip('_')
        for c in df_pd.columns
    ]
    return df_pd


def pd_to_delta(df_pd: pd.DataFrame, table_name: str, model_name: str):
    """Convert pandas → Spark → Delta table."""
    if df_pd.empty:
        print(f"  [Skip] {table_name}: empty result")
        return
    df_pd = clean_col_names(df_pd)
    df_spark = spark.createDataFrame(df_pd.astype(str))  # cast all to str for safety
    df_spark = (
        df_spark
        .withColumn("model_name",    lit(model_name))
        .withColumn("_extracted_at", current_timestamp())
    )
    df_spark.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    print(f"  ✔ {table_name} written → {df_spark.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 3 — TMSCHEMA_TABLES
#           All tables in the semantic model
# ──────────────────────────────────────────────────────────────
q_tables = """
SELECT
    [ID],
    [Name],
    [Description],
    [IsHidden],
    [IsPrivate],
    [DataCategory],
    [ShowAsVariationsOnly],
    [IsPartitionable]
FROM $SYSTEM.TMSCHEMA_TABLES
"""
df_tables = run_dax_system_query(WORKSPACE_NAME, DATASET_NAME, q_tables, "TABLES")
pd_to_delta(df_tables, OUT_TABLES, DATASET_NAME)


# ──────────────────────────────────────────────────────────────
# Cell 4 — TMSCHEMA_COLUMNS
#           All columns including hidden, key, and encoding type
# ──────────────────────────────────────────────────────────────
q_columns = """
SELECT
    [ID],
    [TableID],
    [ExplicitName],
    [InferredDataType],
    [DataType],
    [ExplicitDataType],
    [DataCategory],
    [IsHidden],
    [IsKey],
    [IsUnique],
    [IsNullable],
    [IsAvailableInMdx],
    [ColumnType],
    [EncodingType],
    [SummarizateBy],
    [FormatString],
    [DisplayOrdinal],
    [SourceColumn],
    [Description]
FROM $SYSTEM.TMSCHEMA_COLUMNS
"""
df_columns = run_dax_system_query(WORKSPACE_NAME, DATASET_NAME, q_columns, "COLUMNS")
pd_to_delta(df_columns, OUT_COLUMNS, DATASET_NAME)


# ──────────────────────────────────────────────────────────────
# Cell 5 — TMSCHEMA_MEASURES
#           All DAX measures with expressions
# ──────────────────────────────────────────────────────────────
q_measures = """
SELECT
    [ID],
    [TableID],
    [Name],
    [Expression],
    [FormatString],
    [IsHidden],
    [Description],
    [DataType],
    [DisplayFolder]
FROM $SYSTEM.TMSCHEMA_MEASURES
"""
df_measures = run_dax_system_query(WORKSPACE_NAME, DATASET_NAME, q_measures, "MEASURES")
pd_to_delta(df_measures, OUT_MEASURES, DATASET_NAME)


# ──────────────────────────────────────────────────────────────
# Cell 6 — TMSCHEMA_PARTITIONS
#           Partition details — source query, type, refresh mode
# ──────────────────────────────────────────────────────────────
q_partitions = """
SELECT
    [ID],
    [TableID],
    [Name],
    [Description],
    [Type],
    [Mode],
    [DataView],
    [QueryDefinition],
    [State],
    [RefreshedTime],
    [ModifiedTime]
FROM $SYSTEM.TMSCHEMA_PARTITIONS
"""
df_partitions = run_dax_system_query(WORKSPACE_NAME, DATASET_NAME, q_partitions, "PARTITIONS")
pd_to_delta(df_partitions, OUT_PARTITIONS, DATASET_NAME)


# ──────────────────────────────────────────────────────────────
# Cell 7 — DISCOVER_STORAGE_TABLE_COLUMNS
#           Physical storage stats: compressed size, row counts
# ──────────────────────────────────────────────────────────────
q_storage = """
SELECT
    [TABLE_ID],
    [COLUMN_ID],
    [DIMENSION_NAME],
    [COLUMN_TYPE],
    [USED_SIZE],
    [DATA_SIZE],
    [DICTIONARY_SIZE],
    [ROWS_COUNT],
    [ORIGINAL_ROWS_COUNT],
    [SEGMENT_COUNT],
    [ENCODING]
FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS
WHERE [COLUMN_TYPE] = 'BASIC_DATA'
"""
df_storage = run_dax_system_query(WORKSPACE_NAME, DATASET_NAME, q_storage, "STORAGE")
pd_to_delta(df_storage, OUT_STORAGE, DATASET_NAME)


# ──────────────────────────────────────────────────────────────
# Cell 8 — Build metadata_master from Live System Tables
#           Joins columns + tables + measures into one catalog
# ──────────────────────────────────────────────────────────────
df_s_cols = spark.read.format("delta").table(OUT_COLUMNS)
df_s_tbls = spark.read.format("delta").table(OUT_TABLES)
df_s_meas = spark.read.format("delta").table(OUT_MEASURES)
df_s_stor = spark.read.format("delta").table(OUT_STORAGE)

# ── Columns ─────────────────────────────────────────────────────
df_col_meta = (
    df_s_cols.alias("c")
    .join(
        df_s_tbls.select("id", "name", "description", "ishidden")
                 .withColumnRenamed("id",          "tbl_id")
                 .withColumnRenamed("name",         "table_name")
                 .withColumnRenamed("description",  "table_description")
                 .withColumnRenamed("ishidden",     "table_is_hidden")
                 .alias("t"),
        col("c.tableid") == col("t.tbl_id"), "left"
    )
    # Join storage for row count and size
    .join(
        df_s_stor.select(
            col("dimension_name").alias("stor_col_name"),
            col("used_size").alias("used_size_bytes"),
            col("rows_count").alias("storage_row_count"),
            col("encoding").alias("stor_encoding"),
        ).alias("st"),
        upper(trim(col("c.explicitname"))) == upper(trim(col("st.stor_col_name"))),
        "left"
    )
    .select(
        lit(DATASET_NAME).alias("model_name"),
        col("t.table_name"),
        col("c.explicitname").alias("object_name"),
        col("c.datatype").alias("data_type"),
        col("c.columntype").alias("column_type"),
        col("c.encodingtype").alias("encoding_type"),
        col("c.ishidden").alias("is_hidden"),
        col("c.iskey").alias("is_key"),
        col("c.isunique").alias("is_unique"),
        col("c.isnullable").alias("is_nullable"),
        col("c.isavailableinmdx").alias("is_available_mdx"),
        col("c.formatstring").alias("format_string"),
        col("c.description").alias("column_description"),
        col("t.table_description"),
        col("t.table_is_hidden"),
        col("st.used_size_bytes"),
        col("st.storage_row_count"),
        col("st.stor_encoding").alias("storage_encoding"),
        lit("COLUMN").alias("object_type"),
        current_timestamp().alias("_extracted_at"),
    )
)

# ── Measures ──────────────────────────────────────────────────
df_meas_meta = (
    df_s_meas.alias("m")
    .join(
        df_s_tbls.select("id", "name")
                 .withColumnRenamed("id",   "tbl_id")
                 .withColumnRenamed("name",  "table_name")
                 .alias("t"),
        col("m.tableid") == col("t.tbl_id"), "left"
    )
    .select(
        lit(DATASET_NAME).alias("model_name"),
        col("t.table_name"),
        col("m.name").alias("object_name"),
        col("m.datatype").alias("data_type"),
        lit("Measure").alias("column_type"),
        lit(None).cast(StringType()).alias("encoding_type"),
        col("m.ishidden").alias("is_hidden"),
        lit(None).cast(StringType()).alias("is_key"),
        lit(None).cast(StringType()).alias("is_unique"),
        lit(None).cast(StringType()).alias("is_nullable"),
        lit(None).cast(StringType()).alias("is_available_mdx"),
        col("m.formatstring").alias("format_string"),
        col("m.description").alias("column_description"),
        lit(None).cast(StringType()).alias("table_description"),
        lit(None).cast(StringType()).alias("table_is_hidden"),
        lit(None).cast(StringType()).alias("used_size_bytes"),
        lit(None).cast(StringType()).alias("storage_row_count"),
        lit(None).cast(StringType()).alias("storage_encoding"),
        lit("MEASURE").alias("object_type"),
        current_timestamp().alias("_extracted_at"),
    )
)

df_meta_master = df_col_meta.union(df_meas_meta)
df_meta_master.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_META_MASTER)

print(f"\n[SysTable] {OUT_META_MASTER} written → {df_meta_master.count():,} objects")


# ──────────────────────────────────────────────────────────────
# Cell 9 — Explore the Live Metadata
# ──────────────────────────────────────────────────────────────
df_meta = spark.read.format("delta").table(OUT_META_MASTER)

print("\n── Table Summary ──")
display(
    df_meta.groupBy("table_name", "object_type")
    .count()
    .orderBy("table_name", "object_type")
)

print("\n── All Measures (with DAX Expressions) ──")
df_meas_view = spark.read.format("delta").table(OUT_MEASURES)
display(
    df_meas_view.select(
        "table_id", "name", "expression", "formatstring",
        "ishidden", "description", "displayfolder"
    ).orderBy("table_id", "name")
)

print("\n── Largest Columns by Storage ──")
display(
    df_meta.filter(col("object_type") == "COLUMN")
    .select("table_name", "object_name", "data_type",
            "storage_row_count", "used_size_bytes", "storage_encoding")
    .orderBy(col("used_size_bytes").cast("long").desc_nulls_last())
    .limit(20)
)

print("\n── Hidden Objects (not visible in reports) ──")
display(
    df_meta.filter(col("is_hidden") == "True")
    .select("table_name", "object_name", "object_type", "data_type")
    .orderBy("table_name")
)

print("\n── Partition Details (refresh mode per table) ──")
df_parts = spark.read.format("delta").table(OUT_PARTITIONS)
display(
    df_parts.select(
        "tableid", "name", "type", "mode",
        "state", "refreshedtime", "querydefinition"
    ).orderBy("tableid")
)
