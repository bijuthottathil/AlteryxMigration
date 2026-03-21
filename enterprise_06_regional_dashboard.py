# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — 06: Regional Dashboard
# MAGIC **Source:** `Enterprise_Sales_Commission_Pipeline.yxzp`
# MAGIC
# MAGIC **This notebook (Layer 6D + Layer 7):**
# MAGIC - Reads `enriched_transactions`
# MAGIC - Summarize D: region × quarter aggregation (Tools 55–56)
# MAGIC - Post-agg formulas: `RevenueAttainmentPct`, `RevenueGap`, `TargetStatus`, `MarginPct`
# MAGIC - Writes: `managedcatalog.biju_gold.regional_dashboard`
# MAGIC - Also writes: `managedcatalog.biju_gold.returned_orders` (from staging)
# MAGIC
# MAGIC **Output:** 8 rows — one row per Region × Quarter with actual vs target revenue, gap analysis

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

# MAGIC %md ## Summarize D — Regional Dashboard (Alteryx: Summarize Tool 55)
# MAGIC GroupBy: Region, Quarter, RevenueTarget, UnitTarget
# MAGIC Aggregates: TotalTransactions, ActualRevenue, ActualMargin, ActualUnits,
# MAGIC TotalCommission, ActiveAccounts, AvgTransactionValue

# COMMAND ----------

from pyspark.sql import functions as F

df_agg = (
    df.groupBy("Region", "Quarter", "RevenueTarget", "UnitTarget")
    .agg(
        F.count("TransactionID")                  .alias("TotalTransactions"),
        F.round(F.sum("NetRevenue"), 2)            .alias("ActualRevenue"),
        F.round(F.sum("GrossMargin"), 2)           .alias("ActualMargin"),
        F.sum("Quantity")                          .alias("ActualUnits"),
        F.round(F.sum("FinalCommission"), 2)       .alias("TotalCommission"),
        F.countDistinct("CustomerID")              .alias("ActiveAccounts"),
        F.round(F.avg("NetRevenue"), 2)            .alias("AvgTransactionValue"),
    )
    .orderBy("Region", "Quarter")
)

preview(df_agg, label="regional aggregation")

# COMMAND ----------

# MAGIC %md ## Post-aggregation formulas (Alteryx: Formula Tool 56)
# MAGIC Derives: `RevenueAttainmentPct`, `RevenueGap`, `TargetStatus`, `MarginPct`

# COMMAND ----------

df_regional = (
    df_agg
    .withColumn("RevenueAttainmentPct",
        F.round(F.when(F.col("RevenueTarget") > 0,
            F.col("ActualRevenue") / F.col("RevenueTarget") * 100
        ).otherwise(0), 1))
    .withColumn("RevenueGap",
        F.round(F.col("ActualRevenue") - F.col("RevenueTarget"), 2))
    .withColumn("TargetStatus",
        F.when(F.col("RevenueAttainmentPct") >= 100, "Target Met")
         .when(F.col("RevenueAttainmentPct") >= 80,  "Close to Target")
         .otherwise("Off Track"))
    .withColumn("MarginPct",
        F.round(F.when(F.col("ActualRevenue") > 0,
            F.col("ActualMargin") / F.col("ActualRevenue") * 100
        ).otherwise(0), 1))
)

preview(df_regional, label="regional_dashboard")

# COMMAND ----------

# MAGIC %md ## Output 1 — Regional Dashboard Delta table (Alteryx: Output Tool 63 → regional_dashboard.csv)

# COMMAND ----------

save_as_delta_table(df_regional, CATALOG, SCHEMA, "regional_dashboard")

# COMMAND ----------

# MAGIC %md ## Output 2 — Returned Orders Delta table (Alteryx: Output Tool 64 → returned_orders_flagged.csv)
# MAGIC Promoted from staging to final gold table for downstream review.

# COMMAND ----------

df_returned = spark.table(f"{CATALOG}.{SCHEMA}.staging_returned_orders")
preview(df_returned, label="returned_orders")
save_as_delta_table(df_returned, CATALOG, SCHEMA, "returned_orders_flagged")
