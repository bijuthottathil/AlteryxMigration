# Databricks notebook source
# MAGIC %md
# MAGIC # Rootbeer — Google trends CSVs (`google*.csv`)
# MAGIC Matches the Designer input **google\*.csv**: unions all `google_rootbeer_YYYY.csv` files with the same schema.
# MAGIC
# MAGIC **Prerequisite:** **`notebook_00_setup_configuration`** is `%run` below. The union result is saved to **`bronze_table_google_trends`** (see `pipeline_config`).

# COMMAND ----------

# MAGIC %run ./notebook_00_setup_configuration

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

_rows = spark.sql("SELECT * FROM pipeline_config LIMIT 1").collect()
if not _rows:
    raise RuntimeError("pipeline_config is empty; run notebook_00_setup_configuration first.")
cfg = _rows[0].asDict()
BASE_PATH = cfg["source_data_path"].rstrip("/")
print(f"Using source_data_path: {BASE_PATH}")

# COMMAND ----------

google_trends = read_google_rootbeer_union(BASE_PATH)
preview(google_trends, 20, label="google trends (union)")

# COMMAND ----------

# Persist to bronze (config): single Delta table for all years unioned
save_as_uc_table_from_config(
    google_trends,
    cfg,
    schema_key="bronze_schema",
    table_key="bronze_table_google_trends",
)

# Optional: silver copy for a cleansed layer name (same data; use if you add transforms later)
save_as_uc_table_from_config(
    google_trends,
    cfg,
    schema_key="silver_schema",
    table_key="silver_table_google_trends",
)
