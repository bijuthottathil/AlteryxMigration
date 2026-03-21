# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — 03: Customer KPI Report
# MAGIC **Source:** `Enterprise_Sales_Commission_Pipeline.yxzp`
# MAGIC
# MAGIC **This notebook (Layer 6A + Layer 7):**
# MAGIC - Reads `enriched_transactions`
# MAGIC - Summarize A: customer-level aggregation (Tools 50–51)
# MAGIC - Post-agg formulas: `MarginPct`, `AvgOrderValue`, `CustomerValueBand`
# MAGIC - Writes: `managedcatalog.biju_gold.customer_kpi_report`
# MAGIC
# MAGIC **Output:** ~20 rows — one row per customer with revenue, margin, order stats, and value band

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

# MAGIC %md ## Summarize A — Customer KPIs (Alteryx: Summarize Tool 50)
# MAGIC GroupBy: CustomerID, CustomerName, Segment, Region, AccountTier
# MAGIC Aggregates: TotalOrders, TotalRevenue, TotalGross, TotalMargin, TotalCOGS,
# MAGIC TotalUnits, AvgOrderValue, MaxOrderValue, TotalCommission, FirstOrderDate, LastOrderDate

# COMMAND ----------

from pyspark.sql import functions as F

df_agg = (
    df.groupBy("CustomerID", "CustomerName", "Segment", "Region", "AccountTier")
    .agg(
        F.count("TransactionID")          .alias("TotalOrders"),
        F.sum("NetRevenue")               .alias("TotalRevenue"),
        F.sum("GrossRevenue")             .alias("TotalGross"),
        F.sum("GrossMargin")              .alias("TotalMargin"),
        F.sum("COGS")                     .alias("TotalCOGS"),
        F.sum("Quantity")                 .alias("TotalUnits"),
        F.avg("NetRevenue")               .alias("AvgOrderValue"),
        F.max("NetRevenue")               .alias("MaxOrderValue"),
        F.sum("FinalCommission")          .alias("TotalCommission"),
        F.min("TransactionDate")          .alias("FirstOrderDate"),
        F.max("TransactionDate")          .alias("LastOrderDate"),
    )
)

preview(df_agg, label="customer aggregation")

# COMMAND ----------

# MAGIC %md ## Post-aggregation formulas (Alteryx: Formula Tool 51)
# MAGIC Derives: `MarginPct`, `AvgOrderValue` (recalculated), `CustomerValueBand`

# COMMAND ----------

df_customer_kpi = (
    df_agg
    .withColumn("MarginPct",
        F.round(F.when(F.col("TotalGross") > 0,
            F.col("TotalMargin") / F.col("TotalGross") * 100
        ).otherwise(0), 1))
    .withColumn("AvgOrderValue",
        F.round(F.col("TotalRevenue") / F.col("TotalOrders"), 2))
    .withColumn("CustomerValueBand",
        F.when(F.col("TotalRevenue") >= 10000, "VIP")
         .when(F.col("TotalRevenue") >= 5000,  "High Value")
         .when(F.col("TotalRevenue") >= 2000,  "Growth")
         .otherwise("Emerging"))
    .withColumn("TotalRevenue",    F.round("TotalRevenue",    2))
    .withColumn("TotalGross",      F.round("TotalGross",      2))
    .withColumn("TotalMargin",     F.round("TotalMargin",     2))
    .withColumn("TotalCOGS",       F.round("TotalCOGS",       2))
    .withColumn("TotalCommission", F.round("TotalCommission", 2))
)

preview(df_customer_kpi, label="customer_kpi_report")

# COMMAND ----------

# MAGIC %md ## Output — Write Delta table (Alteryx: Output Tool 60 → customer_kpi_report.csv)

# COMMAND ----------

save_as_delta_table(df_customer_kpi, CATALOG, SCHEMA, "customer_kpi_report")
