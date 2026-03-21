# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Workflow
# MAGIC **Converted from Alteryx:** `simple_workflow.yxmd`
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Read `input.csv` from Unity Catalog Volume
# MAGIC 2. Filter rows where `age > 30`
# MAGIC 3. Write output
# MAGIC
# MAGIC **Data path:** `/Volumes/managedcatalog/default/data/Alteryx/simple_workflow/`

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

DATA_PATH   = "/Volumes/managedcatalog/default/data/Alteryx/simple_workflow"
OUTPUT_PATH = f"{DATA_PATH}/output"

# COMMAND ----------

# MAGIC %md ## Step 1: Read input.csv (Alteryx: InputData Tool)

# COMMAND ----------

df_input = read_csv(f"{DATA_PATH}/input.csv")
preview(df_input, label="input")

# COMMAND ----------

# MAGIC %md ## Step 2: Filter — age > 30 (Alteryx: Filter Tool)

# COMMAND ----------

df_filtered = filter_rows(df_input, "age > 30")
preview(df_filtered, label="filtered")

# COMMAND ----------

# MAGIC %md ## Step 3: Output (Alteryx: OutputData Tool)

# COMMAND ----------

write_output(df_filtered, OUTPUT_PATH)

# COMMAND ----------

# MAGIC %md ## Delta Table — managedcatalog.biju_gold.simple_workflow

# COMMAND ----------

save_as_delta_table(df_filtered, catalog="managedcatalog", schema="biju_gold", table="simple_workflow")
