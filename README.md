# AlteryxMigration — Alteryx to Databricks

Migration of Alteryx workflows to Databricks PySpark notebooks using Unity Catalog for storage and Delta Lake for output tables.

---

## Repository Structure

```
AlteryxMigration/
│
├── transform.py                        # Shared PySpark utility library
│
├── simple_workflow.py                  # Databricks notebook — simple pipeline
├── medium_workflow.py                  # Databricks notebook — join + summarize
├── complex_workflow.py                 # Databricks notebook — multi-join + formula + discount
│
├── simple_workflow/
│   ├── simple_workflow.yxmd            # Original Alteryx workflow
│   └── input.csv                       # Source data
│
├── medium_workflow/
│   ├── medium_workflow.yxmd            # Original Alteryx workflow
│   ├── customers.csv                   # Source data
│   └── orders.csv                      # Source data
│
└── complex_workflow/
    ├── complex_workflow.yxmd           # Original Alteryx workflow
    ├── macro_calculate_discount.yxmc   # Original Alteryx macro (10% discount)
    ├── customers.csv                   # Source data
    ├── orders.csv                      # Source data (includes product_id)
    └── products.csv                    # Source data
```

---

## Infrastructure

| Component | Location |
|---|---|
| Databricks Workspace | `https://dbc-1109a291-3564.cloud.databricks.com` |
| Notebooks path | `/Workspace/Users/btucker5543@gmail.com/Databricks/Alteryx/example3` |
| Source CSV data | `/Volumes/managedcatalog/default/data/Alteryx/` |
| Output Delta tables | `managedcatalog.biju_gold.*` |

---

## transform.py — Shared Utility Library

`transform.py` is a Databricks notebook loaded at the top of every workflow notebook via:

```python
%run ./transform
```

This makes all helper functions available in the calling notebook's scope without any imports. Every function logs its operation and row counts to the cell output for observability.

### I/O Functions

#### `read_csv(path, infer_schema=True, header=True) → DataFrame`
Reads a CSV file from a Unity Catalog Volume or DBFS path into a Spark DataFrame.

```python
df = read_csv("/Volumes/managedcatalog/default/data/Alteryx/simple_workflow/input.csv")
```

#### `write_output(df, path, fmt="csv", mode="overwrite")`
Writes a DataFrame to a Volume path. Supports `csv`, `parquet`, and `delta` formats.

```python
write_output(df_result, "/Volumes/managedcatalog/default/data/Alteryx/simple_workflow/output")
write_output(df_result, "/Volumes/.../output", fmt="parquet")
```

#### `save_as_delta_table(df, catalog, schema, table, mode="overwrite")`
Writes a DataFrame as a managed Delta table in Unity Catalog. Uses `overwriteSchema=true` to handle schema evolution. Equivalent to a full replace of the table on each run.

```python
save_as_delta_table(df_summary, catalog="managedcatalog", schema="biju_gold", table="simple_workflow")
# Creates/replaces: managedcatalog.biju_gold.simple_workflow
```

---

### Filtering Functions

#### `filter_rows(df, condition) → DataFrame`
Filters rows using a SQL expression string. Directly maps to the Alteryx **Filter Tool**.

```python
df_filtered = filter_rows(df, "age > 30")
df_filtered = filter_rows(df, "amount > 100 AND country = 'US'")
```

#### `drop_nulls(df, subset=None) → DataFrame`
Removes rows containing null values. Optionally scoped to specific columns.

```python
df_clean = drop_nulls(df)
df_clean = drop_nulls(df, subset=["customer_id", "amount"])
```

#### `drop_duplicates(df, subset=None) → DataFrame`
Removes duplicate rows. Optionally considers only a subset of columns for deduplication.

```python
df_deduped = drop_duplicates(df)
df_deduped = drop_duplicates(df, subset=["customer_id"])
```

---

### Join Functions

#### `join_dfs(left, right, on, how="inner") → DataFrame`
Joins two DataFrames on one or more columns. Maps to the Alteryx **Join Tool**. Supports all Spark join types: `inner`, `left`, `right`, `outer`, `semi`, `anti`.

```python
df_joined = join_dfs(df_orders, df_customers, on="customer_id")
df_joined = join_dfs(df_a, df_b, on=["key1", "key2"], how="left")
```

---

### Column Transformation Functions

#### `add_column(df, col_name, expr_str) → DataFrame`
Adds or replaces a column using a SQL expression string. Maps to the Alteryx **Formula Tool**.

```python
df = add_column(df, "discounted_amount", "amount * 0.9")
df = add_column(df, "full_name", "concat(first_name, ' ', last_name)")
```

#### `cast_cols(df, schema_map) → DataFrame`
Casts columns to specified Spark SQL types. Used to enforce correct types after CSV ingestion where `inferSchema` may produce strings.

```python
df = cast_cols(df, {"amount": "double", "age": "integer"})
```

#### `rename_cols(df, mapping) → DataFrame`
Renames columns using a `{old_name: new_name}` dictionary.

```python
df = rename_cols(df, {"cust_id": "customer_id", "amt": "amount"})
```

#### `select_cols(df, cols) → DataFrame`
Selects a subset of columns, dropping the rest. Maps to the Alteryx **Select Tool**.

```python
df = select_cols(df, ["customer_id", "country", "total_amount"])
```

---

### Aggregation Functions

#### `summarize(df, group_cols, agg_map) → DataFrame`
Groups rows and applies named aggregations. Maps to the Alteryx **Summarize Tool**. Supported aggregation functions: `sum`, `avg`, `count`, `min`, `max`, `countDistinct`.

```python
# Single group column
df_summary = summarize(df, group_cols="country", agg_map={"total_amount": ("amount", "sum")})

# Multiple group columns and aggregations
df_summary = summarize(df, group_cols=["country", "category"], agg_map={
    "total_sales":    ("amount", "sum"),
    "avg_order":      ("amount", "avg"),
    "order_count":    ("order_id", "count"),
})
```

---

### Inspection Helpers

#### `preview(df, n=10, label="") → DataFrame`
Prints the schema, a sample of `n` rows, and total row count. Returns the DataFrame so it can be used inline in a pipeline. Maps to the Alteryx **Browse Tool**.

```python
df = preview(df_joined, n=5, label="after join")
```

---

## Notebooks

### simple_workflow.py

**Source:** `simple_workflow/simple_workflow.yxmd`

**Pipeline:**

```
input.csv  →  Filter (age > 30)  →  CSV output  →  Delta table
```

**Alteryx tools converted:**

| Alteryx Tool | transform.py function |
|---|---|
| InputData | `read_csv()` |
| Filter (`[age] > 30`) | `filter_rows(df, "age > 30")` |
| OutputData | `write_output()` + `save_as_delta_table()` |

**Data:**
- Input: `/Volumes/managedcatalog/default/data/Alteryx/simple_workflow/input.csv`
- Output CSV: `/Volumes/managedcatalog/default/data/Alteryx/simple_workflow/output`
- Output Delta table: `managedcatalog.biju_gold.simple_workflow`

**Schema:**

| Column | Type | Notes |
|---|---|---|
| id | integer | Customer ID |
| name | string | Customer name |
| age | integer | Filtered: keep age > 30 |
| country | string | Country code |

---

### medium_workflow.py

**Source:** `medium_workflow/medium_workflow.yxmd`

**Pipeline:**

```
customers.csv ─┐
               ├─ Join (customer_id) → Filter (amount > 100) → Summarize (country, SUM amount) → CSV + Delta
orders.csv    ─┘
```

**Alteryx tools converted:**

| Alteryx Tool | transform.py function |
|---|---|
| InputData × 2 | `read_csv()` |
| Join | `join_dfs(df_orders, df_customers, on="customer_id")` |
| Filter (`[amount] > 100`) | `cast_cols()` + `filter_rows(df, "amount > 100")` |
| Summarize (GroupBy `country`, Sum `amount`) | `summarize(df, "country", {"total_amount": ("amount", "sum")})` |
| OutputData | `write_output()` + `save_as_delta_table()` |

**Data:**
- Input: `medium_workflow/customers.csv`, `medium_workflow/orders.csv`
- Output CSV: `/Volumes/managedcatalog/default/data/Alteryx/medium_workflow/output`
- Output Delta table: `managedcatalog.biju_gold.medium_workflow`

**Output schema:**

| Column | Type | Notes |
|---|---|---|
| country | string | Group key |
| total_amount | double | Sum of order amounts > 100 |

---

### complex_workflow.py

**Source:** `complex_workflow/complex_workflow.yxmd` + `macro_calculate_discount.yxmc`

**Pipeline:**

```
customers.csv ─┐
               ├─ Join (customer_id) ─┐
orders.csv    ─┘                      ├─ Join (product_id) → Formula (amount * 0.9) → Summarize → CSV + Delta
                                      │
products.csv ─────────────────────────┘
```

**Alteryx tools converted:**

| Alteryx Tool | Node | transform.py function |
|---|---|---|
| InputData (customers) | Node 1 | `read_csv()` |
| InputData (orders) | Node 2 | `read_csv()` |
| InputData (products) | Node 3 | `read_csv()` |
| Join customers + orders | Node 4 | `join_dfs(df_orders, df_customers, on="customer_id")` |
| Join result + products | Node 5 | `join_dfs(df_cust_orders, df_products, on="product_id")` |
| Formula (`[amount] * 0.9`) / macro | Node 6 | `cast_cols()` + `add_column(df, "discounted_amount", "amount * 0.9")` |
| Summarize (GroupBy `country`, Sum `discounted_amount`) | Node 7 | `summarize(df, "country", {"total_discounted_amount": ("discounted_amount", "sum")})` |
| OutputData | — | `write_output()` + `save_as_delta_table()` |

**Macro conversion note:** `macro_calculate_discount.yxmc` applied a 10% discount via a Formula tool (`[amount] * 0.9`). This is inlined as `add_column(df_joined, "discounted_amount", "amount * 0.9")` — no separate macro file is needed in PySpark.

**Data:**
- Input: `complex_workflow/customers.csv`, `complex_workflow/orders.csv` (includes `product_id`), `complex_workflow/products.csv`
- Output CSV: `/Volumes/managedcatalog/default/data/Alteryx/complex_workflow/output`
- Output Delta table: `managedcatalog.biju_gold.complex_workflow`

**Output schema:**

| Column | Type | Notes |
|---|---|---|
| country | string | Group key |
| total_discounted_amount | double | Sum of discounted order amounts (10% off) |

---

## Alteryx → PySpark Tool Mapping Reference

| Alteryx Tool | transform.py function | Notes |
|---|---|---|
| InputData | `read_csv()` | Reads CSV from Unity Catalog Volume |
| OutputData | `write_output()` | Writes CSV/Parquet/Delta to Volume |
| — | `save_as_delta_table()` | Writes managed Delta table to Unity Catalog |
| Filter | `filter_rows()` | SQL expression string |
| Join | `join_dfs()` | Supports inner/left/right/outer |
| Formula | `add_column()` | SQL expression string |
| Summarize | `summarize()` | sum/avg/count/min/max/countDistinct |
| Select | `select_cols()` | Column subset selection |
| Dynamic Rename | `rename_cols()` | Dict-based column renaming |
| Data Cleanse | `drop_nulls()` | Remove null rows |
| Unique | `drop_duplicates()` | Remove duplicate rows |
| Browse | `preview()` | Schema + sample + row count |
| — | `cast_cols()` | Enforce column types after CSV read |

---

## Running the Notebooks

1. Open the notebook in the Databricks workspace at:
   `https://dbc-1109a291-3564.cloud.databricks.com`
   under `/Workspace/Users/btucker5543@gmail.com/Databricks/Alteryx/example3`

2. All notebooks depend on `transform` being in the **same workspace folder**. The `%run ./transform` at the top of each notebook resolves it by relative path.

3. Run notebooks in any order — they are independent of each other.

4. After a successful run, query the output Delta tables:
   ```sql
   SELECT * FROM managedcatalog.biju_gold.simple_workflow;
   SELECT * FROM managedcatalog.biju_gold.medium_workflow;
   SELECT * FROM managedcatalog.biju_gold.complex_workflow;
   ```
