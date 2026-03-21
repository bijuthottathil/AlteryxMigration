# Databricks notebook source
# MAGIC %md
# MAGIC # Complex Workflow
# MAGIC **Converted from Alteryx:** `complex_workflow.yxmd`
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Read `customers.csv`, `orders.csv`, and `products.csv` from Unity Catalog Volume
# MAGIC 2. Join customers with orders on `customer_id`
# MAGIC 3. Join result with products on `product_id`
# MAGIC 4. Formula: `discounted_amount = amount * 0.9` (10% discount, from macro `macro_calculate_discount.yxmc`)
# MAGIC 5. Summarize: group by `country`, sum `discounted_amount`
# MAGIC
# MAGIC **Data path:** `/Volumes/managedcatalog/default/data/Alteryx/complex_workflow/`

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

DATA_PATH   = "/Volumes/managedcatalog/default/data/Alteryx/complex_workflow"
OUTPUT_PATH = f"{DATA_PATH}/output"

# COMMAND ----------

# MAGIC %md ## Step 1: Read all input files (Alteryx: InputData Tools — Nodes 1, 2, 3)

# COMMAND ----------

df_customers = read_csv(f"{DATA_PATH}/customers.csv")
df_orders    = read_csv(f"{DATA_PATH}/orders.csv")
df_products  = read_csv(f"{DATA_PATH}/products.csv")

preview(df_customers, label="customers")
preview(df_orders,    label="orders")
preview(df_products,  label="products")

# COMMAND ----------

# MAGIC %md ## Step 2: Join customers + orders on customer_id (Alteryx: Join Tool — Node 4)

# COMMAND ----------

df_cust_orders = join_dfs(df_orders, df_customers, on="customer_id")
preview(df_cust_orders, label="customers+orders")

# COMMAND ----------

# MAGIC %md ## Step 3: Join result with products on product_id (Alteryx: Join Tool — Node 5)

# COMMAND ----------

df_joined = join_dfs(df_cust_orders, df_products, on="product_id")
preview(df_joined, label="full join")

# COMMAND ----------

# MAGIC %md ## Step 4: Formula — discounted_amount = amount * 0.9 (Alteryx: Formula Tool — Node 6)
# MAGIC Equivalent to macro `macro_calculate_discount.yxmc` which applied a 10% discount.

# COMMAND ----------

df_joined     = cast_cols(df_joined, {"amount": "double"})
df_discounted = add_column(df_joined, "discounted_amount", "amount * 0.9")
preview(df_discounted, label="with discount")

# COMMAND ----------

# MAGIC %md ## Step 5: Summarize — group by country, sum discounted_amount (Alteryx: Summarize Tool — Node 7)

# COMMAND ----------

df_summary = summarize(
    df_discounted,
    group_cols="country",
    agg_map={"total_discounted_amount": ("discounted_amount", "sum")}
)
preview(df_summary, label="summary")

# COMMAND ----------

# MAGIC %md ## Output

# COMMAND ----------

write_output(df_summary, OUTPUT_PATH)

# COMMAND ----------

# MAGIC %md ## Delta Table — managedcatalog.biju_gold.complex_workflow

# COMMAND ----------

save_as_delta_table(df_summary, catalog="managedcatalog", schema="biju_gold", table="complex_workflow")
