# Databricks notebook source
# MAGIC %md
# MAGIC # Enterprise Sales Commission Pipeline — Data Explorer
# MAGIC
# MAGIC Interactive exploration of all Delta tables produced by the Enterprise Sales Commission Pipeline.
# MAGIC
# MAGIC **Tables covered:**
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `staging_valid_transactions` | 57 cleansed transactions (ReturnFlag=N) |
# MAGIC | `staging_returned_orders` | 3 returned/flagged transactions |
# MAGIC | `enriched_transactions` | Fully enriched — 4 joins + KPIs + commission |
# MAGIC | `customer_kpi_report` | Customer-level revenue, margin, value band |
# MAGIC | `product_performance` | Product-level units, revenue, margin |
# MAGIC | `rep_commission_report` | Rep-level deals, commission, quota attainment |
# MAGIC | `regional_dashboard` | Region × Quarter actual vs target |
# MAGIC | `returned_orders_flagged` | Returned orders promoted to gold |

# COMMAND ----------

CATALOG = "managedcatalog"
SCHEMA  = "biju_gold"

def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 1. Staging Tables

# COMMAND ----------

# MAGIC %md ### 1a. Valid Transactions (staging_valid_transactions)

# COMMAND ----------

df_txn = spark.table(f"{CATALOG}.{SCHEMA}.staging_valid_transactions")

section("staging_valid_transactions — Schema")
df_txn.printSchema()

section("staging_valid_transactions — Sample (10 rows)")
df_txn.show(10, truncate=False)

section("staging_valid_transactions — Row count & date range")
from pyspark.sql import functions as F
df_txn.agg(
    F.count("*").alias("TotalRows"),
    F.min("TransactionDate").alias("EarliestDate"),
    F.max("TransactionDate").alias("LatestDate"),
    F.countDistinct("CustomerID").alias("UniqueCustomers"),
    F.countDistinct("ProductID").alias("UniqueProducts"),
    F.countDistinct("SalesRepID").alias("UniqueReps"),
).show(truncate=False)

# COMMAND ----------

section("Transactions by Quarter")
df_txn.groupBy("Quarter").agg(
    F.count("*").alias("Transactions"),
    F.round(F.sum("NetRevenue"), 2).alias("TotalNetRevenue"),
    F.round(F.avg("NetRevenue"), 2).alias("AvgNetRevenue"),
).orderBy("Quarter").show(truncate=False)

# COMMAND ----------

section("Transactions by Payment Method")
df_txn.groupBy("PaymentMethod").agg(
    F.count("*").alias("Count"),
    F.round(F.sum("NetRevenue"), 2).alias("TotalRevenue"),
).orderBy(F.desc("TotalRevenue")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ### 1b. Returned Orders (staging_returned_orders)

# COMMAND ----------

df_returned = spark.table(f"{CATALOG}.{SCHEMA}.staging_returned_orders")

section("staging_returned_orders — All rows")
df_returned.show(truncate=False)

section("staging_returned_orders — Summary")
df_returned.agg(
    F.count("*").alias("ReturnedOrders"),
    F.round(F.sum(F.col("Quantity") * F.col("UnitPrice")), 2).alias("GrossValueAtRisk"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 2. Enriched Transactions

# COMMAND ----------

# MAGIC %md ### 2a. Schema & Sample

# COMMAND ----------

df_enr = spark.table(f"{CATALOG}.{SCHEMA}.enriched_transactions")

section("enriched_transactions — Schema")
df_enr.printSchema()

section("enriched_transactions — Sample (5 rows)")
df_enr.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ### 2b. Deal Tier Distribution

# COMMAND ----------

section("Deal Tier Distribution")
df_enr.groupBy("DealTier").agg(
    F.count("*").alias("Transactions"),
    F.round(F.sum("NetRevenue"), 2).alias("TotalRevenue"),
    F.round(F.avg("NetRevenue"), 2).alias("AvgRevenue"),
    F.round(F.sum("FinalCommission"), 2).alias("TotalCommission"),
).orderBy(F.desc("TotalRevenue")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ### 2c. Commission Band Distribution

# COMMAND ----------

section("Commission Band Distribution")
df_enr.groupBy("CommissionBand").agg(
    F.count("*").alias("Transactions"),
    F.round(F.sum("FinalCommission"), 2).alias("TotalCommission"),
    F.round(F.avg("FinalCommission"), 2).alias("AvgCommission"),
    F.round(F.min("FinalCommission"), 2).alias("MinCommission"),
    F.round(F.max("FinalCommission"), 2).alias("MaxCommission"),
).orderBy(F.desc("TotalCommission")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ### 2d. Discount Tier Distribution

# COMMAND ----------

section("Discount Tier Distribution")
df_enr.groupBy("DiscountTier").agg(
    F.count("*").alias("Transactions"),
    F.round(F.sum("DiscountAmount"), 2).alias("TotalDiscountGiven"),
    F.round(F.avg("DiscountRate") * 100, 1).alias("AvgDiscountPct"),
).orderBy(F.desc("TotalDiscountGiven")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ### 2e. Payment Risk Breakdown

# COMMAND ----------

section("Payment Risk Breakdown")
df_enr.groupBy("PaymentRisk", "PaymentMethod").agg(
    F.count("*").alias("Transactions"),
    F.round(F.sum("NetRevenue"), 2).alias("TotalRevenue"),
).orderBy("PaymentRisk").show(truncate=False)

# COMMAND ----------

# MAGIC %md ### 2f. Monthly Revenue Trend

# COMMAND ----------

section("Monthly Revenue Trend")
df_enr.groupBy("Year", "Month", "Quarter").agg(
    F.count("*").alias("Transactions"),
    F.round(F.sum("NetRevenue"), 2).alias("TotalRevenue"),
    F.round(F.sum("GrossMargin"), 2).alias("TotalMargin"),
    F.round(F.sum("FinalCommission"), 2).alias("TotalCommission"),
).orderBy("Year", F.min("TransactionDate")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 3. Customer KPI Report

# COMMAND ----------

df_cust = spark.table(f"{CATALOG}.{SCHEMA}.customer_kpi_report")

section("customer_kpi_report — Schema")
df_cust.printSchema()

section("customer_kpi_report — All customers ranked by revenue")
df_cust.select(
    "CustomerID", "CustomerName", "Segment", "Region", "AccountTier",
    "TotalOrders", "TotalRevenue", "TotalMargin", "MarginPct",
    "AvgOrderValue", "TotalCommission", "CustomerValueBand"
).orderBy(F.desc("TotalRevenue")).show(25, truncate=False)

# COMMAND ----------

section("Customer Value Band Summary")
df_cust.groupBy("CustomerValueBand").agg(
    F.count("*").alias("Customers"),
    F.round(F.sum("TotalRevenue"), 2).alias("TotalRevenue"),
    F.round(F.avg("TotalRevenue"), 2).alias("AvgRevenue"),
    F.sum("TotalOrders").alias("TotalOrders"),
).orderBy(F.desc("TotalRevenue")).show(truncate=False)

# COMMAND ----------

section("Customer KPIs by Segment")
df_cust.groupBy("Segment").agg(
    F.count("*").alias("Customers"),
    F.round(F.sum("TotalRevenue"), 2).alias("TotalRevenue"),
    F.round(F.avg("MarginPct"), 1).alias("AvgMarginPct"),
    F.round(F.avg("AvgOrderValue"), 2).alias("AvgOrderValue"),
).orderBy(F.desc("TotalRevenue")).show(truncate=False)

# COMMAND ----------

section("Top 5 Customers by Revenue")
df_cust.select(
    "CustomerName", "Region", "Segment", "TotalRevenue",
    "TotalOrders", "CustomerValueBand"
).orderBy(F.desc("TotalRevenue")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 4. Product Performance

# COMMAND ----------

df_prod = spark.table(f"{CATALOG}.{SCHEMA}.product_performance")

section("product_performance — Schema")
df_prod.printSchema()

section("product_performance — All products ranked by revenue")
df_prod.select(
    "ProductID", "ProductName", "Category", "SubCategory",
    "TotalOrders", "TotalUnitsSold", "TotalRevenue",
    "TotalMargin", "AvgSalePrice", "UniqueCustomers"
).orderBy(F.desc("TotalRevenue")).show(truncate=False)

# COMMAND ----------

section("Product Performance by Category")
df_prod.groupBy("Category").agg(
    F.count("*").alias("Products"),
    F.sum("TotalUnitsSold").alias("TotalUnitsSold"),
    F.round(F.sum("TotalRevenue"), 2).alias("TotalRevenue"),
    F.round(F.sum("TotalMargin"), 2).alias("TotalMargin"),
    F.round(F.avg("AvgSalePrice"), 2).alias("AvgSalePrice"),
).orderBy(F.desc("TotalRevenue")).show(truncate=False)

# COMMAND ----------

section("Top 3 Products by Units Sold")
df_prod.select("ProductName", "Category", "TotalUnitsSold", "TotalRevenue") \
    .orderBy(F.desc("TotalUnitsSold")).show(3, truncate=False)

section("Top 3 Products by Unique Customers")
df_prod.select("ProductName", "Category", "UniqueCustomers", "TotalRevenue") \
    .orderBy(F.desc("UniqueCustomers")).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 5. Rep Commission Report

# COMMAND ----------

df_rep = spark.table(f"{CATALOG}.{SCHEMA}.rep_commission_report")

section("rep_commission_report — Schema")
df_rep.printSchema()

section("rep_commission_report — All reps ranked by commission")
df_rep.select(
    "RepName", "Region", "Territory", "CommissionRate",
    "TotalDeals", "TotalRevenue", "TotalCommission",
    "UniqueAccounts", "LargestDeal", "AvgDealSize",
    "QuotaAttainmentPct", "PerformanceBand"
).orderBy(F.desc("TotalCommission")).show(truncate=False)

# COMMAND ----------

section("Performance Band Summary")
df_rep.groupBy("PerformanceBand").agg(
    F.count("*").alias("Reps"),
    F.round(F.sum("TotalRevenue"), 2).alias("TotalRevenue"),
    F.round(F.avg("QuotaAttainmentPct"), 1).alias("AvgQuotaAttainmentPct"),
    F.round(F.sum("TotalCommission"), 2).alias("TotalCommission"),
).orderBy(F.desc("AvgQuotaAttainmentPct")).show(truncate=False)

# COMMAND ----------

section("Rep Revenue vs Quota Gap")
df_rep.select(
    "RepName",
    "YTDQuota",
    "TotalRevenue",
    F.round(F.col("YTDQuota") - F.col("TotalRevenue"), 2).alias("RevenueGap"),
    "QuotaAttainmentPct",
    "PerformanceBand"
).orderBy(F.desc("QuotaAttainmentPct")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 6. Regional Dashboard

# COMMAND ----------

df_reg = spark.table(f"{CATALOG}.{SCHEMA}.regional_dashboard")

section("regional_dashboard — Schema")
df_reg.printSchema()

section("regional_dashboard — All rows (Region × Quarter)")
df_reg.select(
    "Region", "Quarter",
    "RevenueTarget", "ActualRevenue", "RevenueGap",
    "RevenueAttainmentPct", "TargetStatus",
    "ActualUnits", "UnitTarget",
    "TotalCommission", "ActiveAccounts", "MarginPct"
).orderBy("Region", "Quarter").show(truncate=False)

# COMMAND ----------

section("Regional Revenue Totals (H1 2024)")
df_reg.groupBy("Region").agg(
    F.round(F.sum("RevenueTarget"), 2).alias("H1Target"),
    F.round(F.sum("ActualRevenue"), 2).alias("H1Actual"),
    F.round(F.sum("ActualRevenue") - F.sum("RevenueTarget"), 2).alias("H1Gap"),
    F.round(F.sum("ActualRevenue") / F.sum("RevenueTarget") * 100, 1).alias("H1AttainmentPct"),
).orderBy(F.desc("H1Actual")).show(truncate=False)

# COMMAND ----------

section("Target Status Summary")
df_reg.groupBy("TargetStatus").agg(
    F.count("*").alias("RegionQuarters"),
    F.round(F.sum("ActualRevenue"), 2).alias("TotalRevenue"),
).orderBy("TargetStatus").show(truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 7. Returned Orders

# COMMAND ----------

df_ret = spark.table(f"{CATALOG}.{SCHEMA}.returned_orders_flagged")

section("returned_orders_flagged — All rows")
df_ret.show(truncate=False)

section("Returned Orders — Revenue Impact")
df_ret.agg(
    F.count("*").alias("ReturnedOrders"),
    F.round(F.sum(F.col("Quantity") * F.col("UnitPrice")), 2).alias("GrossValueAtRisk"),
    F.round(F.sum(F.col("Quantity") * F.col("UnitPrice") * (1 - F.col("Discount"))), 2).alias("NetValueAtRisk"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md ---
# MAGIC ## 8. Pipeline Summary

# COMMAND ----------

section("Full Pipeline Summary — All Tables")

tables = {
    "staging_valid_transactions" : spark.table(f"{CATALOG}.{SCHEMA}.staging_valid_transactions"),
    "staging_returned_orders"    : spark.table(f"{CATALOG}.{SCHEMA}.staging_returned_orders"),
    "enriched_transactions"      : spark.table(f"{CATALOG}.{SCHEMA}.enriched_transactions"),
    "customer_kpi_report"        : spark.table(f"{CATALOG}.{SCHEMA}.customer_kpi_report"),
    "product_performance"        : spark.table(f"{CATALOG}.{SCHEMA}.product_performance"),
    "rep_commission_report"      : spark.table(f"{CATALOG}.{SCHEMA}.rep_commission_report"),
    "regional_dashboard"         : spark.table(f"{CATALOG}.{SCHEMA}.regional_dashboard"),
    "returned_orders_flagged"    : spark.table(f"{CATALOG}.{SCHEMA}.returned_orders_flagged"),
}

print(f"{'Table':<40} {'Rows':>6}  {'Columns':>7}")
print("-" * 58)
for name, df in tables.items():
    print(f"{name:<40} {df.count():>6}  {len(df.columns):>7}")

# COMMAND ----------

section("Pipeline KPI Snapshot")

df_e = spark.table(f"{CATALOG}.{SCHEMA}.enriched_transactions")
df_e.agg(
    F.count("*")                          .alias("TotalTransactions"),
    F.round(F.sum("GrossRevenue"), 2)     .alias("TotalGrossRevenue"),
    F.round(F.sum("NetRevenue"), 2)       .alias("TotalNetRevenue"),
    F.round(F.sum("DiscountAmount"), 2)   .alias("TotalDiscountGiven"),
    F.round(F.sum("COGS"), 2)             .alias("TotalCOGS"),
    F.round(F.sum("GrossMargin"), 2)      .alias("TotalGrossMargin"),
    F.round(F.sum("FinalCommission"), 2)  .alias("TotalCommissionPaid"),
    F.round(F.avg("MarginPct"), 1)        .alias("AvgMarginPct"),
    F.round(F.avg("NetRevenue"), 2)       .alias("AvgTransactionValue"),
    F.countDistinct("CustomerID")         .alias("UniqueCustomers"),
    F.countDistinct("ProductID")          .alias("UniqueProducts"),
    F.countDistinct("SalesRepID")         .alias("UniqueReps"),
).show(truncate=False)
