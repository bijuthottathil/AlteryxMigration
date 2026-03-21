# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — 04: Product Performance
# MAGIC **Source:** `Enterprise_Sales_Commission_Pipeline.yxzp`
# MAGIC
# MAGIC **This notebook (Layer 6B + Layer 7):**
# MAGIC - Reads `enriched_transactions`
# MAGIC - Summarize B: product-level aggregation (Tool 52)
# MAGIC - Writes: `managedcatalog.biju_gold.product_performance`
# MAGIC
# MAGIC **Output:** ~10 rows — one row per product with units sold, revenue, margin, unique customers

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

CATALOG = "managedcatalog"
SCHEMA  = "biju_gold"

# COMMAND ----------

# MAGIC %md ## Read enriched transactions

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.enriched_transactions")
preview(df, n=5, label="enriched_transactions")

# COMMAND ----------

# MAGIC %md ## Summarize B — Product Performance (Alteryx: Summarize Tool 52)
# MAGIC GroupBy: ProductID, ProductName, Category, SubCategory, Brand, MSRP, CostPrice, WarrantyMonths
# MAGIC Aggregates: TotalOrders, TotalUnitsSold, TotalRevenue, TotalMargin, TotalDiscount,
# MAGIC AvgSalePrice, UniqueCustomers

# COMMAND ----------

from pyspark.sql import functions as F

df_product_perf = (
    df.groupBy(
        "ProductID", "ProductName", "Category", "SubCategory",
        "Brand", "MSRP", "CostPrice", "WarrantyMonths"
    )
    .agg(
        F.count("TransactionID")          .alias("TotalOrders"),
        F.sum("Quantity")                 .alias("TotalUnitsSold"),
        F.round(F.sum("NetRevenue"), 2)   .alias("TotalRevenue"),
        F.round(F.sum("GrossMargin"), 2)  .alias("TotalMargin"),
        F.round(F.sum("DiscountAmount"), 2).alias("TotalDiscount"),
        F.round(F.avg("NetRevenue"), 2)   .alias("AvgSalePrice"),
        F.countDistinct("CustomerID")     .alias("UniqueCustomers"),
    )
)

preview(df_product_perf, label="product_performance")

# COMMAND ----------

# MAGIC %md ## Output — Write Delta table (Alteryx: Output Tool 61 → product_performance.csv)

# COMMAND ----------

save_as_delta_table(df_product_perf, CATALOG, SCHEMA, "product_performance")
