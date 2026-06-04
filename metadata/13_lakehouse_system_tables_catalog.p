# Fabric Notebook — Lakehouse System Tables: Metadata Extraction
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Queries Fabric Lakehouse system/information_schema tables to extract
#              physical column-level metadata for ALL Delta tables across
#              Bronze, Silver, and Gold layers. This is SOURCE 2 in the metadata
#              master join (SOURCE 1 = DAX Studio system tables from semantic model).
#              Output: lakehouse_column_catalog — one row per column per table per layer.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, upper, trim, when,
    count, sum as spark_sum, coalesce
)
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# ── Layer → Lakehouse catalog/schema mapping ──────────────────
# In Fabric, each Lakehouse registers as a database in the Spark catalog
LAYER_CATALOG_MAP = {
    "BRONZE": "BronzeLakehouse",     # <-- Replace with your actual Lakehouse names
    "SILVER": "SilverLakehouse",
    "GOLD":   "GoldLakehouse",
}

OUTPUT_TABLE = "lakehouse_column_catalog"
print(f"[LH Catalog] Extracting metadata from {len(LAYER_CATALOG_MAP)} Lakehouse layers")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Helper: Get All Tables in a Lakehouse
# ──────────────────────────────────────────────────────────────
def get_lakehouse_tables(lakehouse_name: str, layer: str) -> list:
    """
    Returns list of (lakehouse, table_name, layer) tuples.
    Uses spark.catalog.listTables() which reflects the Hive metastore.
    """
    try:
        tables = spark.catalog.listTables(lakehouse_name)
        result = [(lakehouse_name, t.name, t.tableType, layer) for t in tables]
        print(f"  [{layer}] {lakehouse_name}: {len(result)} tables found")
        return result
    except Exception as e:
        print(f"  [{layer}] ERROR reading {lakehouse_name}: {e}")
        return []


all_tables = []
for layer, lh_name in LAYER_CATALOG_MAP.items():
    all_tables.extend(get_lakehouse_tables(lh_name, layer))

print(f"\n[LH Catalog] Total tables across all layers: {len(all_tables)}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Extract Column Metadata via INFORMATION_SCHEMA
#           and spark.catalog.listColumns()
# ──────────────────────────────────────────────────────────────
column_rows = []

for lakehouse, table_name, table_type, layer in all_tables:
    full_table = f"{lakehouse}.{table_name}"
    try:
        # Use spark.catalog.listColumns for schema info
        cols = spark.catalog.listColumns(table_name, lakehouse)

        # Get row count and Delta stats
        df_tbl  = spark.read.format("delta").table(full_table)
        row_cnt = df_tbl.count()

        # Get Delta table history (last 1 entry for last modified)
        try:
            from delta.tables import DeltaTable
            dt     = DeltaTable.forName(spark, full_table)
            hist   = dt.history(1).collect()
            last_mod        = str(hist[0]["timestamp"]) if hist else None
            last_operation  = str(hist[0]["operation"]) if hist else None
            last_user       = str(hist[0].get("userName", "")) if hist else None
        except Exception:
            last_mod = last_operation = last_user = None

        for c in cols:
            column_rows.append({
                "layer":              layer,
                "lakehouse_name":     lakehouse,
                "table_name":         table_name,
                "full_table_ref":     full_table,
                "table_type":         table_type,
                "column_name":        c.name,
                "data_type":          c.dataType,
                "is_nullable":        c.nullable,
                "column_description": c.description or "",
                "is_partition_col":   c.isPartition,
                "is_bucket_col":      c.isBucket,
                "table_row_count":    row_cnt,
                "last_modified":      last_mod,
                "last_operation":     last_operation,
                "last_modified_by":   last_user,
                "_catalog_extracted_at": None,  # filled in Spark below
            })

        print(f"  ✔ [{layer}] {table_name}: {len(cols)} columns, {row_cnt:,} rows")

    except Exception as e:
        print(f"  ✖ [{layer}] {full_table}: {e}")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Build DataFrame and Write
# ──────────────────────────────────────────────────────────────
df_catalog = spark.createDataFrame(column_rows)
df_catalog = df_catalog.withColumn("_catalog_extracted_at", current_timestamp())

df_catalog.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUTPUT_TABLE)

print(f"\n[LH Catalog] {OUTPUT_TABLE} written → {df_catalog.count():,} column records")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Also Capture via SQL INFORMATION_SCHEMA for DDL fidelity
#           (catches computed columns, constraints, comments set via SQL)
# ──────────────────────────────────────────────────────────────
IS_OUTPUT_TABLE = "lakehouse_information_schema"

try:
    df_is_cols = spark.sql("""
        SELECT
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION,
            IS_NULLABLE,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        ORDER BY TABLE_CATALOG, TABLE_NAME, ORDINAL_POSITION
    """)

    df_is_cols = df_is_cols.withColumn("_extracted_at", current_timestamp())
    df_is_cols.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(IS_OUTPUT_TABLE)

    print(f"[LH Catalog] {IS_OUTPUT_TABLE} written → {df_is_cols.count():,} rows")

except Exception as e:
    print(f"[LH Catalog] INFORMATION_SCHEMA query skipped: {e}")
    print("  (Use per-lakehouse SQL endpoint if cross-lakehouse IS is unavailable)")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Quick Explore
# ──────────────────────────────────────────────────────────────
df_out = spark.read.format("delta").table(OUTPUT_TABLE)

print("\n── Table Count per Layer ──")
display(df_out.groupBy("layer").agg(
    count("table_name").alias("column_records"),
    col("table_name")
).groupBy("layer").count())

print("\n── All Tables with Row Counts ──")
display(
    df_out.select("layer", "table_name", "table_row_count", "last_modified", "last_operation")
    .dropDuplicates(["layer", "table_name"])
    .orderBy("layer", "table_name")
)

print("\n── Columns per Table ──")
display(
    df_out.groupBy("layer", "table_name")
    .count()
    .withColumnRenamed("count", "column_count")
    .orderBy("layer", "table_name")
)
