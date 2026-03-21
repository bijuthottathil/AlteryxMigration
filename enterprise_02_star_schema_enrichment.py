# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — 02: Star Schema Enrichment
# MAGIC **Source:** `Enterprise_Sales_Commission_Pipeline.yxzp`
# MAGIC
# MAGIC **This notebook (Layer 3 + Layer 4 + Layer 5):**
# MAGIC - Reads `staging_valid_transactions` + 4 dimension tables
# MAGIC - Join 1: + Customers (CustomerID)
# MAGIC - Join 2: + Products (ProductID)
# MAGIC - Join 3: + Sales Reps (SalesRepID)
# MAGIC - Join 4: + Regional Targets (Region + Quarter — composite key)
# MAGIC - KPI formula enrichment: COGS, GrossMargin, DealTier, DiscountTier, PaymentRisk, AccountTier, etc.
# MAGIC - Commission Calculator (inline from `CommissionCalculator.yxmc`): TierMultiplier, FinalCommission, CommissionBand
# MAGIC - Writes: `managedcatalog.biju_gold.enriched_transactions`
# MAGIC
# MAGIC **Alteryx tools:** Join ×4 (Tools 20–23), Formula (Tool 30), Macro (Tool 40/41)

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

DATA_PATH = "/Volumes/managedcatalog/default/data/Alteryx/enterprise_pipeline"
CATALOG   = "managedcatalog"
SCHEMA    = "biju_gold"

# COMMAND ----------

# MAGIC %md ## Read staging transactions and dimension CSVs

# COMMAND ----------

df_txn     = spark.table(f"{CATALOG}.{SCHEMA}.staging_valid_transactions")
df_cust    = read_csv(f"{DATA_PATH}/customers.csv")
df_prod    = read_csv(f"{DATA_PATH}/products.csv")
df_reps    = read_csv(f"{DATA_PATH}/sales_reps.csv")
df_targets = read_csv(f"{DATA_PATH}/regional_targets.csv")

preview(df_txn, label="staging_transactions")

# COMMAND ----------

# MAGIC %md ## Layer 3 — Star Schema Joins (Alteryx: Join Tools 20–23)

# COMMAND ----------

# MAGIC %md ### Join 1 — Transactions + Customers on CustomerID (Tool 20)

# COMMAND ----------

df_cust_slim = select_cols(df_cust, [
    "CustomerID", "CustomerName", "Segment", "Region", "Country",
    "State", "City", "CreditLimit", "AccountOpenDate", "PreferredPayment"
])

df_j1 = join_dfs(df_txn, df_cust_slim, on="CustomerID", how="left")
preview(df_j1, label="after join 1 (customers)")

# COMMAND ----------

# MAGIC %md ### Join 2 — + Products on ProductID (Tool 21)

# COMMAND ----------

df_prod_slim = select_cols(df_prod, [
    "ProductID", "ProductName", "Category", "SubCategory",
    "Brand", "CostPrice", "MSRP", "WarrantyMonths"
])

df_j2 = join_dfs(df_j1, df_prod_slim, on="ProductID", how="left")
df_j2 = cast_cols(df_j2, {"CostPrice": "double", "MSRP": "double"})
preview(df_j2, label="after join 2 (products)")

# COMMAND ----------

# MAGIC %md ### Join 3 — + Sales Reps on SalesRepID (Tool 22)

# COMMAND ----------

df_reps_slim = select_cols(df_reps, [
    "SalesRepID", "RepName", "Territory", "CommissionRate", "YTDQuota"
])
df_reps_slim = cast_cols(df_reps_slim, {"CommissionRate": "double", "YTDQuota": "double"})

df_j3 = join_dfs(df_j2, df_reps_slim, on="SalesRepID", how="left")
preview(df_j3, label="after join 3 (sales reps)")

# COMMAND ----------

# MAGIC %md ### Join 4 — + Regional Targets on Region + Quarter (composite key, Tool 23)

# COMMAND ----------

df_targets_slim = select_cols(df_targets, [
    "Region", "Quarter", "RevenueTarget", "UnitTarget", "RetentionTarget"
])
df_targets_slim = cast_cols(df_targets_slim, {
    "RevenueTarget": "double",
    "UnitTarget":    "double"
})

df_j4 = join_dfs(df_j3, df_targets_slim, on=["Region", "Quarter"], how="left")
preview(df_j4, label="after join 4 (regional targets)")

# COMMAND ----------

# MAGIC %md ## Layer 4 — KPI Formula Enrichment (Alteryx: Formula Tool 30)
# MAGIC Derives: COGS, GrossMargin, MarginPct, DealTier, RevenuePerUnit,
# MAGIC PctDailyTarget, DiscountTier, PaymentRisk, AccountAgeDays, AccountTier

# COMMAND ----------

from pyspark.sql import functions as F

df_kpi = (
    df_j4
    # COGS = CostPrice × Quantity
    .withColumn("COGS",
        F.round(F.col("CostPrice") * F.col("Quantity"), 2))
    # Gross Margin = NetRevenue − COGS
    .withColumn("GrossMargin",
        F.round(F.col("NetRevenue") - F.col("COGS"), 2))
    # Margin %
    .withColumn("MarginPct",
        F.round(F.when(F.col("GrossRevenue") > 0,
            (F.col("GrossRevenue") - F.col("COGS")) / F.col("GrossRevenue") * 100
        ).otherwise(0), 1))
    # Deal Tier: Platinum / Gold / Silver / Bronze
    .withColumn("DealTier",
        F.when(F.col("NetRevenue") >= 2000, "Platinum")
         .when(F.col("NetRevenue") >= 800,  "Gold")
         .when(F.col("NetRevenue") >= 300,  "Silver")
         .otherwise("Bronze"))
    # Revenue per unit
    .withColumn("RevenuePerUnit",
        F.round(F.when(F.col("Quantity") > 0,
            F.col("NetRevenue") / F.col("Quantity")
        ).otherwise(0), 2))
    # % of regional daily target (≈90 selling days/quarter)
    .withColumn("PctDailyTarget",
        F.round(F.when(F.col("RevenueTarget") > 0,
            F.col("NetRevenue") / (F.col("RevenueTarget") / 90) * 100
        ).otherwise(0), 1))
    # Discount tier label
    .withColumn("DiscountTier",
        F.when(F.col("DiscountRate") >= 0.15, "Heavy Discount")
         .when(F.col("DiscountRate") >= 0.05, "Standard Discount")
         .otherwise("No Discount"))
    # Payment risk flag
    .withColumn("PaymentRisk",
        F.when((F.col("PaymentMethod") == "Wire Transfer") & (F.col("NetRevenue") >= 1000), "Low Risk")
         .when(F.col("PaymentMethod") == "Credit Card", "Medium Risk")
         .otherwise("PayPal Risk"))
    # Account age in days
    .withColumn("AccountAgeDays",
        F.datediff(F.col("TransactionDate"), F.to_date("AccountOpenDate", "yyyy-MM-dd")))
    # Account tier based on credit limit
    .withColumn("AccountTier",
        F.when(F.col("CreditLimit") >= 80000, "Strategic")
         .when(F.col("CreditLimit") >= 30000, "Major")
         .when(F.col("CreditLimit") >= 12000, "Standard")
         .otherwise("Starter"))
)

preview(df_kpi, label="after KPI enrichment")

# COMMAND ----------

# MAGIC %md ## Layer 5 — Commission Calculator (inline from `CommissionCalculator.yxmc`, Tools 40/41)
# MAGIC
# MAGIC **Macro logic:**
# MAGIC - `TierMultiplier`: Platinum=1.5×, Gold=1.25×, Silver=1.0×, Bronze=0.75×
# MAGIC - `ReturnMultiplier`: always 1.0 (all records here are valid sales, ReturnFlag=N)
# MAGIC - `BaseCommission` = NetRevenue × CommissionRate
# MAGIC - `FinalCommission` = BaseCommission × TierMultiplier × ReturnMultiplier (rounded to 2dp)
# MAGIC - `CommissionBand`: High (≥500) / Mid (≥200) / Standard

# COMMAND ----------

df_enriched = (
    df_kpi
    .withColumn("TierMultiplier",
        F.when(F.col("DealTier") == "Platinum", 1.50)
         .when(F.col("DealTier") == "Gold",     1.25)
         .when(F.col("DealTier") == "Silver",   1.00)
         .otherwise(0.75))
    .withColumn("ReturnMultiplier", F.lit(1.0))   # all valid sales = no clawback
    .withColumn("BaseCommission",
        F.round(F.col("NetRevenue") * F.col("CommissionRate"), 2))
    .withColumn("FinalCommission",
        F.round(F.col("BaseCommission") * F.col("TierMultiplier") * F.col("ReturnMultiplier"), 2))
    .withColumn("CommissionBand",
        F.when(F.col("FinalCommission") >= 500, "High Commission")
         .when(F.col("FinalCommission") >= 200, "Mid Commission")
         .otherwise("Standard Commission"))
)

preview(df_enriched, label="enriched_transactions (final)")

# COMMAND ----------

# MAGIC %md ## Output — Write enriched transactions Delta table

# COMMAND ----------

save_as_delta_table(df_enriched, CATALOG, SCHEMA, "enriched_transactions")
