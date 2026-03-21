# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — 05: Rep Commission Report
# MAGIC **Source:** `Enterprise_Sales_Commission_Pipeline.yxzp`
# MAGIC
# MAGIC **This notebook (Layer 6C + Layer 7):**
# MAGIC - Reads `enriched_transactions`
# MAGIC - Summarize C: sales rep-level aggregation (Tools 53–54)
# MAGIC - Post-agg formulas: `QuotaAttainmentPct`, `PerformanceBand`, `AvgDealSize`
# MAGIC - Writes: `managedcatalog.biju_gold.rep_commission_report`
# MAGIC
# MAGIC **Output:** 5 rows — one row per rep with deals, revenue, commission, quota attainment, and performance band

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

# MAGIC %md ## Summarize C — Rep Performance (Alteryx: Summarize Tool 53)
# MAGIC GroupBy: SalesRepID, RepName, Territory, Region, CommissionRate, YTDQuota
# MAGIC Aggregates: TotalDeals, TotalRevenue, TotalMargin, TotalCommission,
# MAGIC TotalUnitsSold, UniqueAccounts, LargestDeal, MostCommonTier

# COMMAND ----------

from pyspark.sql import functions as F

df_agg = (
    df.groupBy(
        "SalesRepID", "RepName", "Territory", "Region",
        "CommissionRate", "YTDQuota"
    )
    .agg(
        F.count("TransactionID")              .alias("TotalDeals"),
        F.round(F.sum("NetRevenue"), 2)        .alias("TotalRevenue"),
        F.round(F.sum("GrossMargin"), 2)       .alias("TotalMargin"),
        F.round(F.sum("FinalCommission"), 2)   .alias("TotalCommission"),
        F.sum("Quantity")                      .alias("TotalUnitsSold"),
        F.countDistinct("CustomerID")          .alias("UniqueAccounts"),
        F.round(F.max("NetRevenue"), 2)        .alias("LargestDeal"),
    )
)

preview(df_agg, label="rep aggregation")

# COMMAND ----------

# MAGIC %md ## Post-aggregation formulas (Alteryx: Formula Tool 54)
# MAGIC Derives: `QuotaAttainmentPct`, `PerformanceBand`, `AvgDealSize`

# COMMAND ----------

df_rep_commission = (
    df_agg
    .withColumn("QuotaAttainmentPct",
        F.round(F.when(F.col("YTDQuota") > 0,
            F.col("TotalRevenue") / F.col("YTDQuota") * 100
        ).otherwise(0), 1))
    .withColumn("PerformanceBand",
        F.when(F.col("QuotaAttainmentPct") >= 100, "Overachiever")
         .when(F.col("QuotaAttainmentPct") >= 80,  "On Track")
         .when(F.col("QuotaAttainmentPct") >= 60,  "At Risk")
         .otherwise("Underperforming"))
    .withColumn("AvgDealSize",
        F.round(F.col("TotalRevenue") / F.col("TotalDeals"), 2))
)

preview(df_rep_commission, label="rep_commission_report")

# COMMAND ----------

# MAGIC %md ## Output — Write Delta table (Alteryx: Output Tool 62 → rep_commission_report.csv)

# COMMAND ----------

save_as_delta_table(df_rep_commission, CATALOG, SCHEMA, "rep_commission_report")
