# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — 01: Ingest & Data Quality
# MAGIC **Source:** `Enterprise_Sales_Commission_Pipeline.yxzp`
# MAGIC
# MAGIC **This notebook (Layer 1 + Layer 2):**
# MAGIC - Reads all 5 source CSVs from Unity Catalog Volume
# MAGIC - Splits returned transactions (ReturnFlag = Y) into a separate flagged table
# MAGIC - Derives date fields: `TransactionDate`, `Quarter`, `Year`, `Month`
# MAGIC - Derives revenue fields: `GrossRevenue`, `NetRevenue`, `DiscountAmount`
# MAGIC - Standardises column names (drops raw fields)
# MAGIC - Writes two staging Delta tables:
# MAGIC   - `managedcatalog.biju_gold.staging_valid_transactions`
# MAGIC   - `managedcatalog.biju_gold.staging_returned_orders`
# MAGIC
# MAGIC **Alteryx tools:** InputData ×5, Filter (Tool 10), Formula (Tool 11), Select (Tool 12)

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

DATA_PATH = "/Volumes/managedcatalog/default/data/Alteryx/enterprise_pipeline"
CATALOG   = "managedcatalog"
SCHEMA    = "biju_gold"

# COMMAND ----------

# MAGIC %md ## Layer 1 — Read all 5 input files (Alteryx: InputData Tools 1–5)

# COMMAND ----------

df_transactions     = read_csv(f"{DATA_PATH}/sales_transactions.csv")
df_customers        = read_csv(f"{DATA_PATH}/customers.csv")
df_products         = read_csv(f"{DATA_PATH}/products.csv")
df_sales_reps       = read_csv(f"{DATA_PATH}/sales_reps.csv")
df_regional_targets = read_csv(f"{DATA_PATH}/regional_targets.csv")

preview(df_transactions,     label="sales_transactions")
preview(df_customers,        label="customers")
preview(df_products,         label="products")
preview(df_sales_reps,       label="sales_reps")
preview(df_regional_targets, label="regional_targets")

# COMMAND ----------

# MAGIC %md ## Layer 2a — Split returns vs valid transactions (Alteryx: Filter Tool 10)
# MAGIC **Expression:** `[ReturnFlag] = 'N'`
# MAGIC - TRUE stream  → valid sales → continue pipeline
# MAGIC - FALSE stream → returned orders → flagged output

# COMMAND ----------

df_valid_raw   = filter_rows(df_transactions, "ReturnFlag = 'N'")
df_returned    = df_transactions.filter("ReturnFlag = 'Y'")

preview(df_valid_raw, label="valid_transactions")
preview(df_returned,  label="returned_orders")

# COMMAND ----------

# MAGIC %md ## Layer 2b — Derive date and revenue fields (Alteryx: Formula Tool 11)
# MAGIC Derives: `TransactionDate` (parsed), `Quarter`, `Year`, `Month`, `GrossRevenue`, `NetRevenue`, `DiscountAmount`

# COMMAND ----------

from pyspark.sql import functions as F

df_valid_raw = cast_cols(df_valid_raw, {
    "Quantity"  : "double",
    "UnitPrice" : "double",
    "Discount"  : "double"
})

df_formula = (
    df_valid_raw
    .withColumn("TransactionDate", F.to_date("TransactionDate", "yyyy-MM-dd"))
    .withColumn("Quarter",  F.concat(F.lit("Q"), F.quarter("TransactionDate").cast("string")))
    .withColumn("Year",     F.year("TransactionDate").cast("string"))
    .withColumn("Month",    F.date_format("TransactionDate", "MMMM"))
    .withColumn("GrossRevenue",   F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    .withColumn("NetRevenue",     F.round(F.col("GrossRevenue") * (1 - F.col("Discount")), 2))
    .withColumn("DiscountAmount", F.round(F.col("GrossRevenue") - F.col("NetRevenue"), 2))
)

preview(df_formula, label="with date+revenue fields")

# COMMAND ----------

# MAGIC %md ## Layer 2c — Standardise columns (Alteryx: Select Tool 12)
# MAGIC Drops raw `TransactionDate` string and `ReturnFlag`; renames `Discount` → `DiscountRate`

# COMMAND ----------

df_standardised = select_cols(df_formula, [
    "TransactionID", "OrderID", "CustomerID", "ProductID", "SalesRepID",
    "TransactionDate", "Quarter", "Year", "Month",
    "Quantity", "UnitPrice",
    F.col("Discount").alias("DiscountRate"),
    "DiscountAmount", "GrossRevenue", "NetRevenue",
    "PaymentMethod"
])

preview(df_standardised, label="standardised")

# COMMAND ----------

# MAGIC %md ## Output — Write staging Delta tables

# COMMAND ----------

save_as_delta_table(df_standardised, CATALOG, SCHEMA, "staging_valid_transactions")
save_as_delta_table(df_returned,     CATALOG, SCHEMA, "staging_returned_orders")
