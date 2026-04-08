# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup and configuration (Alteryx → Databricks)
# MAGIC
# MAGIC **Purpose:** Unity Catalog names, ADLS-backed storage paths, Spark settings, and a shared `pipeline_config` temp view for downstream notebooks (`01` bronze, `02` silver, etc.).
# MAGIC
# MAGIC **Naming convention**
# MAGIC - **Catalog** — top-level UC boundary (e.g. training workspace catalog).
# MAGIC - **Schemas** — `{user}_{layer}_alteryx_rootbeer`: bronze = raw Designer exports, silver = cleansed joins / macro parity, gold = KPIs and reporting tables.
# MAGIC - **Tables** — `{layer}_alteryx_rootbeer_{entity}` (e.g. `bronze_alteryx_rootbeer_transaction`). **Fact** and **dimension** tables from config are written in **`SILVER_SCHEMA`** in notebook 01 (star-schema naming); **gold** tables hold aggregates.
# MAGIC - **Paths** — under an ADLS container exposed as a **Unity Catalog external volume** or **`abfss://`** URI. Adjust `STORAGE_ACCOUNT` and `ADLS_CONTAINER` to match your landing zone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration parameters

# COMMAND ----------

# --- Unity Catalog ----------------------------------------------------------
CATALOG = "na-dbxtraining"

# One schema per medallion layer (Alteryx / Rootbeer pipeline)
BRONZE_SCHEMA = "biju_bronze_alteryx_rootbeer"
SILVER_SCHEMA = "biju_silver_alteryx_rootbeer"
GOLD_SCHEMA = "biju_gold_alteryx_rootbeer"

# --- Bronze — raw CSV / export parity (Alteryx input filenames) -------------
BRONZE_TABLE_TRANSACTION = "bronze_alteryx_rootbeer_transaction"
BRONZE_TABLE_CUSTOMER = "bronze_alteryx_rootbeer_customer"
BRONZE_TABLE_ROOTBEER = "bronze_alteryx_rootbeer_product"
BRONZE_TABLE_ROOTBEER_REVIEW = "bronze_alteryx_rootbeer_review"
BRONZE_TABLE_GEOLOCATION = "bronze_alteryx_rootbeer_geolocation"
BRONZE_TABLE_GOOGLE_TRENDS = "bronze_alteryx_rootbeer_google_trends"

# --- Silver — cleansed / joined (Designer workflow parity) -----------------
SILVER_TABLE_TRANSACTION_ENRICHED = "silver_alteryx_rootbeer_transaction_enriched"
SILVER_TABLE_CUSTOMER = "silver_alteryx_rootbeer_customer"
SILVER_TABLE_REVIEW = "silver_alteryx_rootbeer_review"
SILVER_TABLE_GOOGLE_TRENDS = "silver_alteryx_rootbeer_google_trends_union"
# Optional when brand Excel is exported to Delta
SILVER_TABLE_BRAND = "silver_alteryx_rootbeer_brand"

FACT_TABLE = "fact_alteryx_rootbeer_transaction"
DIM_CUSTOMER = "dim_alteryx_rootbeer_customer"
DIM_ROOTBEER_PRODUCT = "dim_alteryx_rootbeer_product"
DIM_LOCATION = "dim_alteryx_rootbeer_location"
DIM_CREDIT_CARD_TYPE = "dim_alteryx_rootbeer_creditcard_type"
DIM_DATE = "dim_alteryx_rootbeer_date"

# --- Gold — aggregates (reports / batch macro style outputs) ---------------
GOLD_CREDITCARD_REVENUE_SUMMARY = "gold_alteryx_rootbeer_creditcard_revenue_profit"
GOLD_DAILY_TRANSACTION_SUMMARY = "gold_alteryx_rootbeer_daily_transaction_summary"
GOLD_CUSTOMER_REVIEW_ENGAGEMENT = "gold_alteryx_rootbeer_customer_review_engagement"

# --- ADLS / storage ----------------------------------------------------------
# Azure Data Lake Gen2: storage account + container for Unity Catalog external locations / volumes.
STORAGE_ACCOUNT = "dlsdbxtraining002"
ADLS_CONTAINER = "alteryx"

# Canonical base inside the container (no trailing slash in variable below)
ADLS_BASE_PATH = f"abfss://{ADLS_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/rootbeer"

# Unity Catalog Volume paths (recommended: volume mounted to the same ADLS path).
# Example layout mirrors medallion + checkpoints + optional raw landing copy.
VOLUME_CATALOG = CATALOG
VOLUME_SCHEMA_RAW = "biju_raw"
VOLUME_NAME = "biju_vol"

SOURCE_DATA_PATH = (
    f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA_RAW}/{VOLUME_NAME}/alteryx/rootbeer/source/"
)
BRONZE_CHECKPOINT_PATH = (
    f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA_RAW}/{VOLUME_NAME}/alteryx/rootbeer/checkpoints/bronze/"
)
SILVER_CHECKPOINT_PATH = (
    f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA_RAW}/{VOLUME_NAME}/alteryx/rootbeer/checkpoints/silver/"
)
GOLD_OUTPUT_PATH = (
    f"/Volumes/{VOLUME_CATALOG}/{VOLUME_SCHEMA_RAW}/{VOLUME_NAME}/alteryx/rootbeer/gold_export/"
)

# Single legacy-style path if you prefer one tree (Autoloader / file drops)
CHECKPOINT_LOCATION = BRONZE_CHECKPOINT_PATH

# --- Spark / Delta ------------------------------------------------------------
CLUSTER_COLUMNS = ["transaction_date", "creditcard_type", "location_id"]
BROADCAST_THRESHOLD = 10 * 1024 * 1024  # 10 MB

print("✅ Configuration loaded")
print(f"   Catalog: {CATALOG}")
print(f"   Bronze schema: {BRONZE_SCHEMA}")
print(f"   Silver schema: {SILVER_SCHEMA}")
print(f"   Gold schema: {GOLD_SCHEMA}")
print(f"   Source (Volume): {SOURCE_DATA_PATH}")
print(f"   ADLS base (abfss): {ADLS_BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schemas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{GOLD_SCHEMA}")

print("✅ Schemas ensured:")
print(f"   Bronze: `{CATALOG}`.{BRONZE_SCHEMA}")
print(f"   Silver: `{CATALOG}`.{SILVER_SCHEMA}")
print(f"   Gold:   `{CATALOG}`.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark configuration

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", BROADCAST_THRESHOLD)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.clusteredTable.enableClusteringTablePreview", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("✅ Spark configuration applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save configuration for other notebooks

# COMMAND ----------

# Flatten cluster columns for a single-row temp view (avoids struct/array edge cases).
_cluster_csv = ",".join(CLUSTER_COLUMNS)

config = {
    "catalog": CATALOG,
    "bronze_schema": BRONZE_SCHEMA,
    "silver_schema": SILVER_SCHEMA,
    "gold_schema": GOLD_SCHEMA,
    "bronze_table_transaction": BRONZE_TABLE_TRANSACTION,
    "bronze_table_customer": BRONZE_TABLE_CUSTOMER,
    "bronze_table_rootbeer": BRONZE_TABLE_ROOTBEER,
    "bronze_table_rootbeer_review": BRONZE_TABLE_ROOTBEER_REVIEW,
    "bronze_table_geolocation": BRONZE_TABLE_GEOLOCATION,
    "bronze_table_google_trends": BRONZE_TABLE_GOOGLE_TRENDS,
    "silver_table_transaction_enriched": SILVER_TABLE_TRANSACTION_ENRICHED,
    "silver_table_customer": SILVER_TABLE_CUSTOMER,
    "silver_table_review": SILVER_TABLE_REVIEW,
    "silver_table_google_trends": SILVER_TABLE_GOOGLE_TRENDS,
    "silver_table_brand": SILVER_TABLE_BRAND,
    "fact_table": FACT_TABLE,
    "dim_customer": DIM_CUSTOMER,
    "dim_rootbeer_product": DIM_ROOTBEER_PRODUCT,
    "dim_location": DIM_LOCATION,
    "dim_creditcard_type": DIM_CREDIT_CARD_TYPE,
    "dim_date": DIM_DATE,
    "gold_creditcard_revenue_summary": GOLD_CREDITCARD_REVENUE_SUMMARY,
    "gold_daily_transaction_summary": GOLD_DAILY_TRANSACTION_SUMMARY,
    "gold_customer_review_engagement": GOLD_CUSTOMER_REVIEW_ENGAGEMENT,
    "source_data_path": SOURCE_DATA_PATH,
    "bronze_checkpoint_path": BRONZE_CHECKPOINT_PATH,
    "silver_checkpoint_path": SILVER_CHECKPOINT_PATH,
    "gold_output_path": GOLD_OUTPUT_PATH,
    "checkpoint_location": CHECKPOINT_LOCATION,
    "adls_base_path": ADLS_BASE_PATH,
    "storage_account": STORAGE_ACCOUNT,
    "adls_container": ADLS_CONTAINER,
    "cluster_columns_csv": _cluster_csv,
    "broadcast_threshold": str(BROADCAST_THRESHOLD),
}

spark.createDataFrame([config]).createOrReplaceTempView("pipeline_config")

print("✅ Configuration saved to temp view `pipeline_config`")
print("\n📋 Full configuration:")
for key, value in sorted(config.items()):
    print(f"   {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ready
# MAGIC
# MAGIC - **`notebook_01_rootbeer_transactions_pipeline.py`** and **`notebook_02_google_trends_union.py`** call `%run ./notebook_00_setup_configuration` and read paths from **`pipeline_config`** (`source_data_path`, etc.). You can still run this notebook alone to refresh schemas, Spark settings, and the temp view.
# MAGIC - **ADLS:** External location / volume should cover `abfss://alteryx@dlsdbxtraining002.dfs.core.windows.net/` (container `alteryx`, account `dlsdbxtraining002`). Subfolder `rootbeer/` matches `ADLS_BASE_PATH`.
