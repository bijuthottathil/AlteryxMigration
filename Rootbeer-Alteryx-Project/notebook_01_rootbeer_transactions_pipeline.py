# Databricks notebook source
# MAGIC %md
# MAGIC # Rootbeer — Transactions pipeline (Alteryx → PySpark)
# MAGIC Mirrors **Transactions – Transform Data** in `Rootbeer Alteryx Project.yxmd`: input hygiene, time fields, days-since-last-purchase (with imputation), joins to `rootbeer.csv` and `geolocation.csv`, optional brand URLs, **Time on Shelf**.
# MAGIC
# MAGIC **Prerequisite:** **`notebook_00_setup_configuration`** is `%run` below. CSVs are read from **`source_data_path`**. This notebook **persists** DataFrames to the **bronze / silver / gold** (and **fact / dim**) table names defined in `pipeline_config`.

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

# MAGIC %md
# MAGIC ## Bronze — raw CSV → Delta (config table names)

# COMMAND ----------

import os
from pyspark.sql import functions as F
from pyspark.sql import Window

_bronze_files = [
    ("transaction.csv", "bronze_table_transaction"),
    ("customer.csv", "bronze_table_customer"),
    ("rootbeer.csv", "bronze_table_rootbeer"),
    ("rootbeerreview.csv", "bronze_table_rootbeer_review"),
    ("geolocation.csv", "bronze_table_geolocation"),
]

for fname, table_key in _bronze_files:
    fp = f"{BASE_PATH}/{fname}"
    if not os.path.exists(fp):
        print(f"[bronze] skip (file missing): {fp}")
        continue
    _df = read_csv(fp)
    save_as_uc_table_from_config(_df, cfg, schema_key="bronze_schema", table_key=table_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — conformed copies (`silver_table_customer`, `silver_table_review`, optional brand)

# COMMAND ----------

_scust = f"{BASE_PATH}/customer.csv"
if os.path.exists(_scust):
    _sdf = read_csv(_scust)
    if "customerid" in _sdf.columns:
        _sdf = _sdf.withColumn("customerid", F.col("customerid").cast("int"))
    if "zipcode" in _sdf.columns:
        _sdf = _sdf.withColumn("zipcode", F.col("zipcode").cast("long"))
    save_as_uc_table_from_config(_sdf, cfg, schema_key="silver_schema", table_key="silver_table_customer")

_srev = f"{BASE_PATH}/rootbeerreview.csv"
if os.path.exists(_srev):
    _srv = read_csv(_srev)
    for c, t in [("customerid", "int"), ("brandid", "int"), ("starrating", "int")]:
        if c in _srv.columns:
            _srv = _srv.withColumn(c, F.col(c).cast(t))
    if "reviewdate" in _srv.columns:
        _srv = _srv.withColumn("reviewdate", F.to_date(F.col("reviewdate")))
    save_as_uc_table_from_config(_srv, cfg, schema_key="silver_schema", table_key="silver_table_review")

_sbrand = f"{BASE_PATH}/rootbeerbrand.csv"
if os.path.exists(_sbrand):
    _sbr = read_csv(_sbrand)
    save_as_uc_table_from_config(_sbr, cfg, schema_key="silver_schema", table_key="silver_table_brand")
else:
    print("[silver] skip silver_table_brand (rootbeerbrand.csv not in source path — export from Designer if needed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — enriched transactions (Alteryx workflow parity)

# COMMAND ----------

enriched_tx = build_rootbeer_enriched_transactions(
    BASE_PATH,
    include_brand_join=False,
    brand_csv_path=None,
)

save_as_uc_table_from_config(
    enriched_tx,
    cfg,
    schema_key="silver_schema",
    table_key="silver_table_transaction_enriched",
)

# COMMAND ----------

preview(enriched_tx, 15, label="enriched transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — fact & dimensions (star schema names from config)

# COMMAND ----------

# Fact = same grain as enriched transactions (one row per transaction).
save_as_uc_table_from_config(
    enriched_tx,
    cfg,
    schema_key="silver_schema",
    table_key="fact_table",
)

# dim_location ← geolocation.csv
_geo_path = f"{BASE_PATH}/geolocation.csv"
if os.path.exists(_geo_path):
    _geo = read_csv(_geo_path)
    save_as_uc_table_from_config(_geo, cfg, schema_key="silver_schema", table_key="dim_location")

# dim_customer ← customer.csv (one row per customerid)
_cust_path = f"{BASE_PATH}/customer.csv"
if os.path.exists(_cust_path):
    _cust = read_csv(_cust_path)
    if "customerid" in _cust.columns:
        _cust = _cust.dropDuplicates(["customerid"])
    save_as_uc_table_from_config(_cust, cfg, schema_key="silver_schema", table_key="dim_customer")

# dim_rootbeer_product ← rootbeer.csv
_rb_path = f"{BASE_PATH}/rootbeer.csv"
if os.path.exists(_rb_path):
    _rb = read_csv(_rb_path)
    if "rootbeerid" in _rb.columns:
        _rb = _rb.dropDuplicates(["rootbeerid"])
    save_as_uc_table_from_config(_rb, cfg, schema_key="silver_schema", table_key="dim_rootbeer_product")

# dim_creditcard_type ← distinct card types from raw transactions
_tx_path = f"{BASE_PATH}/transaction.csv"
if os.path.exists(_tx_path):
    _txr = read_csv(_tx_path)
    if "creditcardtype" in _txr.columns:
        _ccd = (
            _txr.select(F.col("creditcardtype").alias("creditcard_type"))
            .where(F.col("creditcard_type").isNotNull())
            .distinct()
        )
        _w = Window.orderBy("creditcard_type")
        _ccd = _ccd.withColumn("creditcard_type_id", F.row_number().over(_w))
        save_as_uc_table_from_config(_ccd, cfg, schema_key="silver_schema", table_key="dim_creditcard_type")

# dim_date ← distinct transaction dates from enriched stream
_dimd = (
    enriched_tx.select(F.col("Transaction Date").alias("date_key"))
    .where(F.col("date_key").isNotNull())
    .distinct()
)
_dimd = _dimd.select(
    "date_key",
    F.year("date_key").alias("cal_year"),
    F.month("date_key").alias("cal_month"),
    F.dayofmonth("date_key").alias("cal_day"),
    F.date_format("date_key", "EEEE").alias("day_name"),
)
save_as_uc_table_from_config(_dimd, cfg, schema_key="silver_schema", table_key="dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — batch macro & daily summary

# COMMAND ----------

# Batch macro (Tool 226) — raw `transaction.csv` + placeholder Profit
batch_summary = batch_macro_creditcard_revenue_profit_from_volume(BASE_PATH)
save_as_uc_table_from_config(
    batch_summary,
    cfg,
    schema_key="gold_schema",
    table_key="gold_creditcard_revenue_summary",
)
preview(batch_summary, 20, label="batch macro: Revenue & Sum_Profit by card type")

# Daily aggregates from enriched fact stream
_gold_daily = enriched_tx.groupBy(F.col("Transaction Date").alias("transaction_date")).agg(
    F.count(F.lit(1)).alias("transaction_count"),
    F.sum(F.col("Purchase Price").cast("double")).alias("total_purchase_price"),
)
save_as_uc_table_from_config(
    _gold_daily,
    cfg,
    schema_key="gold_schema",
    table_key="gold_daily_transaction_summary",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — standard macro (customer × reviews × state names)

# COMMAND ----------

# Standard macro (Tool 225) — customers + reviews + US state names (State Names.xlsx parity)
customers = read_csv(f"{BASE_PATH}/customer.csv")
reviews = read_csv(f"{BASE_PATH}/rootbeerreview.csv")
for c, t in [("customerid", "int"), ("brandid", "int")]:
    if c in reviews.columns:
        reviews = reviews.withColumn(c, F.col(c).cast(t))
if "customerid" in customers.columns:
    customers = customers.withColumn("customerid", F.col("customerid").cast("int"))

maven_preview = standard_macro_maven_join(customers, reviews, spark=spark)
save_as_uc_table_from_config(
    maven_preview,
    cfg,
    schema_key="gold_schema",
    table_key="gold_customer_review_engagement",
)
preview(maven_preview, 15, label="standard macro (approx)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional file export
# MAGIC
# MAGIC Uncomment to also write Parquet under **`gold_output_path`** from config.

# COMMAND ----------

# write_output(enriched_tx, f"{cfg.get('gold_output_path', BASE_PATH).rstrip('/')}/enriched_transactions", fmt="parquet")
