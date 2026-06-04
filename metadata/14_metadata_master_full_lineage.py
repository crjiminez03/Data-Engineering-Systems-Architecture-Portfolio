# Fabric Notebook — Master Metadata Table: Semantic Model + Lakehouse Lineage
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Joins the two metadata sources into one master catalog:
#
#   SOURCE 1 (Semantic Model objects from DAX Studio):
#     sys_tables, sys_columns, sys_measures, sys_partitions, sys_storage_tbls
#     → What the Power BI model exposes: tables, columns, measures, data types,
#       hidden flags, keys, encoding, storage tables
#
#   SOURCE 2 (Lakehouse physical columns from system tables):
#     lakehouse_column_catalog (Bronze/Silver/Gold layers)
#     → What physically exists in Delta: actual row counts, data types,
#       nullable flags, partition columns, last modified, last operation
#
#   OUTPUT: metadata_master_full
#     One row per object with full lineage from physical storage → semantic model
#     Used as the foundation for column profiling, health checks, and impact analysis.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, upper, lower, coalesce, when, current_timestamp,
    concat_ws, sha2, count, countDistinct, isnull
)
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# ── Source Tables ──────────────────────────────────────────────
SRC_METADATA_MASTER  = "metadata_master"           # From 04_semantic_model_system_tables.py
SRC_LH_CATALOG       = "lakehouse_column_catalog"  # From 13_lakehouse_system_tables_catalog.py
SRC_SYS_TABLES       = "sys_tables"
SRC_SYS_COLUMNS      = "sys_columns"
SRC_SYS_MEASURES     = "sys_measures"
SRC_SYS_PARTITIONS   = "sys_partitions"
SRC_STORAGE_TBLS     = "sys_storage_tbls"

OUTPUT_TABLE         = "metadata_master_full"
OUTPUT_LINEAGE       = "metadata_lineage_map"

print("[MetaMaster] Building unified metadata master with full lineage")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Load Source 1: Semantic Model Objects
# ──────────────────────────────────────────────────────────────
df_sem_meta = spark.read.format("delta").table(SRC_METADATA_MASTER)
df_sys_cols  = spark.read.format("delta").table(SRC_SYS_COLUMNS)
df_sys_meas  = spark.read.format("delta").table(SRC_SYS_MEASURES)
df_sys_parts = spark.read.format("delta").table(SRC_SYS_PARTITIONS)
df_sys_tbls  = spark.read.format("delta").table(SRC_SYS_TABLES)
df_stor_tbls = spark.read.format("delta").table(SRC_STORAGE_TBLS)

print(f"  Semantic model columns: {df_sys_cols.count():,}")
print(f"  Semantic model measures: {df_sys_meas.count():,}")
print(f"  Semantic model tables: {df_sys_tbls.count():,}")
print(f"  Partitions: {df_sys_parts.count():,}")
print(f"  Storage tables: {df_stor_tbls.count():,}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Load Source 2: Lakehouse Physical Catalog
# ──────────────────────────────────────────────────────────────
df_lh = spark.read.format("delta").table(SRC_LH_CATALOG)

print(f"\n  Lakehouse column records (all layers): {df_lh.count():,}")
display(
    df_lh.groupBy("layer", "table_name")
    .count()
    .withColumnRenamed("count", "col_count")
    .orderBy("layer", "table_name")
)


# ──────────────────────────────────────────────────────────────
# Cell 4 — Enrich Semantic Model Columns
#           Join sys_columns → sys_partitions → sys_storage_tbls
#           to get physical storage row counts per model column
# ──────────────────────────────────────────────────────────────

# Join partitions to columns via TableID
df_col_enriched = (
    df_sys_cols.alias("c")
    .join(
        df_sys_tbls.select("id", "name", "description", "ishidden", "dataCategory")
                   .withColumnRenamed("id",          "tbl_id")
                   .withColumnRenamed("name",         "sem_table_name")
                   .withColumnRenamed("description",  "table_description")
                   .withColumnRenamed("ishidden",     "table_is_hidden")
                   .alias("t"),
        col("c.tableid") == col("t.tbl_id"), "left"
    )
    # Join storage tables to get compressed size / row counts
    .join(
        df_stor_tbls.select(
            col("DIMENSION_NAME").alias("stor_table_name"),
            col("USED_SIZE").alias("used_size_bytes"),
            col("ROWS_COUNT").alias("storage_row_count"),
        ).alias("st"),
        upper(trim(col("c.explicitname"))) == upper(trim(col("st.stor_table_name"))),
        "left"
    )
    .select(
        lit("SemanticModel").alias("source_system"),
        col("c.model_name"),
        col("t.sem_table_name").alias("sem_table_name"),
        col("c.explicitname").alias("sem_column_name"),
        col("c.datatype").alias("sem_data_type"),
        col("c.columntype").alias("sem_column_type"),
        col("c.encodingtype").alias("sem_encoding_type"),
        col("c.ishidden").alias("sem_is_hidden"),
        col("c.iskey").alias("sem_is_key"),
        col("c.isavailableinmdx").alias("sem_is_mdx"),
        col("t.table_description"),
        col("t.table_is_hidden"),
        col("st.used_size_bytes"),
        col("st.storage_row_count"),
        lit("COLUMN").alias("object_type"),
    )
)

# Measures (no physical storage link — they are calculated)
df_meas_enriched = (
    df_sys_meas.alias("m")
    .join(
        df_sys_tbls.select("id", "name")
                   .withColumnRenamed("id",   "tbl_id")
                   .withColumnRenamed("name",  "sem_table_name")
                   .alias("t"),
        col("m.tableid") == col("t.tbl_id"), "left"
    )
    .select(
        lit("SemanticModel").alias("source_system"),
        col("m.model_name"),
        col("t.sem_table_name").alias("sem_table_name"),
        col("m.name").alias("sem_column_name"),
        lit("MEASURE_EXPRESSION").alias("sem_data_type"),
        lit("Calculated").alias("sem_column_type"),
        lit(None).cast(StringType()).alias("sem_encoding_type"),
        col("m.ishidden").alias("sem_is_hidden"),
        lit(None).cast(StringType()).alias("sem_is_key"),
        lit(None).cast(StringType()).alias("sem_is_mdx"),
        lit(None).cast(StringType()).alias("table_description"),
        lit(None).cast(StringType()).alias("table_is_hidden"),
        lit(None).cast(StringType()).alias("used_size_bytes"),
        lit(None).cast(StringType()).alias("storage_row_count"),
        lit("MEASURE").alias("object_type"),
    )
)

df_sem_objects = df_col_enriched.union(df_meas_enriched)
print(f"\n[MetaMaster] Semantic model objects (cols + measures): {df_sem_objects.count():,}")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Build Lineage Map
#           Semantic model table → Silver/Gold destination table
#           via naming convention matching (configurable mapping)
# ──────────────────────────────────────────────────────────────
# This mapping connects semantic model table names to the
# lakehouse Delta tables that feed them through the pipeline.
# Adjust to match your actual naming conventions.

from pyspark.sql import Row

lineage_rows = [
    # (sem_table_name, lh_layer, lh_table_name, relationship)
    Row(sem_table_name="Customers",   lh_layer="GOLD",   lh_table_name="gold_customers",                  relationship="DirectFeed"),
    Row(sem_table_name="Orders",      lh_layer="GOLD",   lh_table_name="gold_orders",                     relationship="DirectFeed"),
    Row(sem_table_name="Products",    lh_layer="GOLD",   lh_table_name="gold_products",                   relationship="DirectFeed"),
    Row(sem_table_name="AuditEvents", lh_layer="GOLD",   lh_table_name="gold_audit_user_activity_summary",relationship="Aggregated"),
    Row(sem_table_name="ModelHealth", lh_layer="GOLD",   lh_table_name="gold_model_health_scorecard",     relationship="Aggregated"),
    Row(sem_table_name="RefreshLog",  lh_layer="SILVER", lh_table_name="silver_semantic_model_logs_parsed",relationship="DirectFeed"),
    # Add all your model tables → lakehouse mappings here
]

df_lineage_map = spark.createDataFrame(lineage_rows)
df_lineage_map = df_lineage_map.withColumn("_created_at", current_timestamp())

df_lineage_map.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUTPUT_LINEAGE)

print(f"[MetaMaster] Lineage map written: {df_lineage_map.count()} entries")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Build Master Metadata Table
#           Joins: Semantic Objects + Lakehouse Catalog + Lineage Map
# ──────────────────────────────────────────────────────────────
# The join key between semantic model and lakehouse:
#   sem_table_name → lineage_map → lh_table_name + lh_layer
#   sem_column_name → lakehouse column_name (name normalisation applied)

df_master = (
    df_sem_objects.alias("sem")

    # Step 1: join to lineage map on semantic table name
    .join(
        df_lineage_map.alias("lm"),
        upper(trim(col("sem.sem_table_name"))) == upper(trim(col("lm.sem_table_name"))),
        "left"
    )

    # Step 2: join to lakehouse catalog on (lh_table_name + column_name)
    .join(
        df_lh.select(
            "layer", "table_name", "column_name",
            "data_type", "is_nullable", "is_partition_col",
            "table_row_count", "last_modified", "last_operation",
            "last_modified_by", "lakehouse_name"
        ).alias("lh"),
        (upper(trim(col("lm.lh_table_name"))) == upper(trim(col("lh.table_name")))) &
        (upper(trim(col("sem.sem_column_name"))) == upper(trim(col("lh.column_name")))) &
        (col("lm.lh_layer") == col("lh.layer")),
        "left"
    )

    .select(
        # ── Identity ─────────────────────────────────────────────
        col("sem.model_name"),
        col("sem.source_system"),
        col("sem.object_type"),

        # ── Semantic Model Object ────────────────────────────────
        col("sem.sem_table_name"),
        col("sem.sem_column_name"),
        col("sem.sem_data_type"),
        col("sem.sem_column_type"),
        col("sem.sem_encoding_type"),
        col("sem.sem_is_hidden"),
        col("sem.sem_is_key"),
        col("sem.sem_is_mdx"),
        col("sem.table_description"),
        col("sem.used_size_bytes").alias("sem_storage_bytes"),
        col("sem.storage_row_count").alias("sem_storage_rows"),

        # ── Lineage ──────────────────────────────────────────────
        col("lm.lh_layer"),
        col("lm.lh_table_name"),
        col("lm.relationship"),

        # ── Physical Lakehouse Column ─────────────────────────────
        col("lh.lakehouse_name"),
        col("lh.column_name").alias("lh_column_name"),
        col("lh.data_type").alias("lh_data_type"),
        col("lh.is_nullable").alias("lh_is_nullable"),
        col("lh.is_partition_col"),
        col("lh.table_row_count").alias("lh_table_row_count"),
        col("lh.last_modified"),
        col("lh.last_operation"),
        col("lh.last_modified_by"),

        # ── Type Match Flag (semantic vs physical) ────────────────
        when(
            col("lh.column_name").isNull(),
            "NOT_IN_LAKEHOUSE"
        ).when(
            upper(col("sem.sem_data_type")) == upper(col("lh.data_type")),
            "TYPE_MATCH"
        ).otherwise(
            "TYPE_MISMATCH"
        ).alias("data_type_alignment"),

        # ── Coverage flags ────────────────────────────────────────
        when(col("lh.column_name").isNull(), False).otherwise(True)
            .alias("has_lakehouse_mapping"),
        when(col("lm.lh_table_name").isNull(), False).otherwise(True)
            .alias("has_lineage_mapping"),

        # ── Audit ─────────────────────────────────────────────────
        current_timestamp().alias("_master_built_at"),
        sha2(concat_ws("|",
            col("sem.model_name"),
            col("sem.sem_table_name"),
            col("sem.sem_column_name"),
            col("lm.lh_table_name")
        ), 256).alias("_row_hash")
    )
)

df_master.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUTPUT_TABLE)

print(f"\n[MetaMaster] {OUTPUT_TABLE} written → {df_master.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 7 — Health Insights from Master Metadata
# ──────────────────────────────────────────────────────────────
df_out = spark.read.format("delta").table(OUTPUT_TABLE)

print("\n── Objects with NO Lakehouse Mapping (orphaned in semantic model) ──")
display(
    df_out.filter(col("has_lineage_mapping") == False)
    .select("model_name", "sem_table_name", "sem_column_name", "object_type")
    .orderBy("sem_table_name", "sem_column_name")
)

print("\n── Data Type Mismatches: Semantic vs Physical ──")
display(
    df_out.filter(col("data_type_alignment") == "TYPE_MISMATCH")
    .select("sem_table_name", "sem_column_name",
            "sem_data_type", "lh_data_type",
            "lh_layer", "lh_table_name")
    .orderBy("sem_table_name")
)

print("\n── Column Coverage Summary ──")
display(
    df_out.groupBy("object_type", "lh_layer")
    .agg(
        count("*").alias("total_objects"),
        count(when(col("has_lakehouse_mapping") == True, 1)).alias("mapped"),
        count(when(col("has_lakehouse_mapping") == False, 1)).alias("unmapped"),
    )
    .orderBy("object_type", "lh_layer")
)

print("\n── Hidden Semantic Objects (not visible in reports) ──")
display(
    df_out.filter(col("sem_is_hidden") == True)
    .select("model_name", "sem_table_name", "sem_column_name",
            "object_type", "sem_data_type")
    .orderBy("sem_table_name")
)

print("\n── Largest Tables by Storage (bytes) ──")
display(
    df_out.filter(col("sem_storage_bytes").isNotNull())
    .groupBy("sem_table_name", "lh_layer", "lh_table_name")
    .agg(spark_sum("sem_storage_bytes").alias("total_bytes"),
         spark_sum("sem_storage_rows").alias("total_rows"))
    .orderBy("total_bytes", ascending=False)
    .limit(15)
)
