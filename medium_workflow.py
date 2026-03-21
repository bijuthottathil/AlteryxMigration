# Databricks notebook source
# MAGIC %md
# MAGIC # Medium Workflow
# MAGIC **Converted from Alteryx:** `medium_workflow.yxmd`
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Read `customers.csv` and `orders.csv` from Unity Catalog Volume
# MAGIC 2. Join customers with orders on `customer_id`
# MAGIC 3. Filter rows where `amount > 100`
# MAGIC 4. Summarize: group by `country`, sum `amount`
# MAGIC
# MAGIC **Data path:** `/Volumes/managedcatalog/default/data/Alteryx/medium_workflow/`

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

DATA_PATH   = "/Volumes/managedcatalog/default/data/Alteryx/medium_workflow"
OUTPUT_PATH = f"{DATA_PATH}/output"

# COMMAND ----------

# MAGIC %md ## Step 1: Read customers.csv and orders.csv (Alteryx: InputData Tools)

# COMMAND ----------

df_customers = read_csv(f"{DATA_PATH}/customers.csv")
df_orders    = read_csv(f"{DATA_PATH}/orders.csv")

preview(df_customers, label="customers")
preview(df_orders,    label="orders")

# COMMAND ----------

# MAGIC %md ## Step 2: Join customers and orders on customer_id (Alteryx: Join Tool)

# COMMAND ----------

df_joined = join_dfs(df_orders, df_customers, on="customer_id")
preview(df_joined, label="joined")

# COMMAND ----------

# MAGIC %md ## Step 3: Filter — amount > 100 (Alteryx: Filter Tool)

# COMMAND ----------

df_joined   = cast_cols(df_joined, {"amount": "double"})
df_filtered = filter_rows(df_joined, "amount > 100")
preview(df_filtered, label="filtered")

# COMMAND ----------

# MAGIC %md ## Step 4: Summarize — group by country, sum amount (Alteryx: Summarize Tool)

# COMMAND ----------

df_summary = summarize(df_filtered, group_cols="country", agg_map={"total_amount": ("amount", "sum")})
preview(df_summary, label="summary")

# COMMAND ----------

# MAGIC %md ## Output

# COMMAND ----------

write_output(df_summary, OUTPUT_PATH)

# COMMAND ----------

# MAGIC %md ## Delta Table — managedcatalog.biju_gold.medium_workflow

# COMMAND ----------

save_as_delta_table(df_summary, catalog="managedcatalog", schema="biju_gold", table="medium_workflow")
