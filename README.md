# AlteryxMigration — Alteryx to Databricks

Migration of Alteryx workflows to Databricks PySpark notebooks using Unity Catalog for storage and Delta Lake for output tables.

---

## Repository Structure

```
AlteryxMigration/
│
├── transform.py                              # Shared PySpark utility library
│
│── ── Simple / Medium / Complex Workflows ──
├── simple_workflow.py                        # Databricks notebook
├── medium_workflow.py                        # Databricks notebook
├── complex_workflow.py                       # Databricks notebook
├── complex_orders.csv                        # Corrected orders data with product_id
│
├── simple_workflow/
│   ├── simple_workflow.yxmd                  # Original Alteryx workflow
│   └── input.csv                             # Source data
│
├── medium_workflow/
│   ├── medium_workflow.yxmd                  # Original Alteryx workflow
│   ├── customers.csv                         # Source data
│   └── orders.csv                            # Source data
│
├── complex_workflow/
│   ├── complex_workflow.yxmd                 # Original Alteryx workflow
│   ├── macro_calculate_discount.yxmc         # Original Alteryx macro (10% discount)
│   ├── customers.csv                         # Source data
│   ├── orders.csv                            # Source data (includes product_id)
│   └── products.csv                          # Source data
│
│── ── Enterprise Sales Commission Pipeline ──
├── enterprise_01_ingest_quality.py           # Databricks notebook — Layer 1+2
├── enterprise_02_star_schema_enrichment.py   # Databricks notebook — Layer 3+4+5
├── enterprise_03_customer_kpi_report.py      # Databricks notebook — Layer 6A+7
├── enterprise_04_product_performance.py      # Databricks notebook — Layer 6B+7
├── enterprise_05_rep_commission_report.py    # Databricks notebook — Layer 6C+7
└── enterprise_06_regional_dashboard.py       # Databricks notebook — Layer 6D+7
```

---

## Infrastructure

| Component | Location |
|---|---|
| Databricks Workspace | `https://dbc-1109a291-3564.cloud.databricks.com` |
| Notebooks path | `/Workspace/Users/btucker5543@gmail.com/Databricks/Alteryx/example3` |
| Simple/Medium/Complex data | `/Volumes/managedcatalog/default/data/Alteryx/` |
| Enterprise pipeline data | `/Volumes/managedcatalog/default/data/Alteryx/enterprise_pipeline/` |
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

---

## Enterprise Sales Commission Pipeline

### Package: `Enterprise_Sales_Commission_Pipeline.yxzp`

A zipped Alteryx package (`.yxzp`) containing a 27-tool, 7-layer enterprise workflow for computing sales commissions, KPIs, and regional performance dashboards across a Jan–Jun 2024 transaction dataset.

**Package metadata (`MANIFEST.json`):**

| Field | Value |
|---|---|
| Version | 3.0 |
| Alteryx Version | 2024.1 |
| Tool count | 27 |
| Transaction rows | 60 (Jan–Jun 2024) |
| Outputs | 5 CSV reports |

#### Package Contents

```
Enterprise_Sales_Commission_Pipeline.yxzp (zip)
│
├── Enterprise_Sales_Commission_Pipeline.yxmd   # Main workflow (27 tools, 7 layers)
├── macros/
│   └── CommissionCalculator.yxmc               # Tiered commission + return penalty macro
├── data/
│   ├── sales_transactions.csv                  # 60 rows — raw transaction records Jan–Jun 2024
│   ├── customers.csv                           # 20 rows — customer master dimension
│   ├── products.csv                            # 10 rows — product catalogue dimension
│   ├── sales_reps.csv                          # 5 rows  — sales rep master with commission rates
│   └── regional_targets.csv                    # 8 rows  — Q1+Q2 2024 regional revenue targets
├── output/
│   └── README.txt
└── MANIFEST.json
```

#### Source Data Schemas

**`sales_transactions.csv`** — 60 rows

| Column | Type | Description |
|---|---|---|
| TransactionID | string | Unique transaction ID (TXN-XXXX) |
| OrderID | string | Order reference (ORD-XXXX) |
| CustomerID | string | FK → customers |
| ProductID | string | FK → products |
| SalesRepID | string | FK → sales_reps |
| TransactionDate | date | Transaction date (YYYY-MM-DD) |
| Quantity | integer | Units ordered |
| UnitPrice | double | Price per unit |
| Discount | double | Discount rate (0.00–0.20) |
| PaymentMethod | string | Credit Card / PayPal / Wire Transfer |
| ReturnFlag | string | Y = returned, N = valid sale |

**`customers.csv`** — 20 rows

| Column | Description |
|---|---|
| CustomerID | Primary key |
| CustomerName | Company name |
| Segment | Enterprise / Mid-Market / SMB |
| Region | North / South / East / West |
| Country, State, City | Geography |
| AccountOpenDate | Account creation date |
| CreditLimit | Credit limit (USD) |
| PreferredPayment | Preferred payment method |

**`products.csv`** — 10 rows

| Column | Description |
|---|---|
| ProductID | Primary key (PROD-XXX) |
| ProductName | Product name |
| Category | Hardware / Software |
| SubCategory | Peripherals / Servers / ERP / Analytics / etc. |
| Brand | Brand name |
| CostPrice | Cost to company (USD) |
| MSRP | Retail price (USD) |
| WarrantyMonths | Warranty period |

**`sales_reps.csv`** — 5 reps

| Column | Description |
|---|---|
| SalesRepID | Primary key (REP-XX) |
| RepName | Full name |
| Region | Assigned region |
| Territory | Sub-region territory |
| CommissionRate | Base commission rate (0.07–0.09) |
| YTDQuota | Annual revenue quota (USD) |

**`regional_targets.csv`** — 8 rows (4 regions × 2 quarters)

| Column | Description |
|---|---|
| Region | North / South / East / West |
| Quarter | Q1 / Q2 |
| RevenueTarget | Revenue target for the quarter (USD) |
| UnitTarget | Unit volume target |
| RetentionTarget | Customer retention rate target |

#### Macro: `CommissionCalculator.yxmc`

Calculates tiered commission with return penalty for each transaction.

**Inputs:** `NetRevenue`, `CommissionRate`, `ReturnFlag`, `DealTier`

**Logic:**

| Step | Expression | Output |
|---|---|---|
| Tier multiplier | Platinum=1.5×, Gold=1.25×, Silver=1.0×, Bronze=0.75× | `TierMultiplier` |
| Return multiplier | ReturnFlag=Y → 0.5 (50% clawback), N → 1.0 | `ReturnMultiplier` |
| Base commission | `NetRevenue × CommissionRate` | `BaseCommission` |
| Final commission | `ROUND(BaseCommission × TierMultiplier × ReturnMultiplier, 2)` | `FinalCommission` |
| Commission band | ≥500 → High, ≥200 → Mid, else → Standard | `CommissionBand` |

#### Workflow Layers (27 tools)

| Layer | Tools | Description |
|---|---|---|
| 1 — Input | 1–5 | Read 5 CSV files |
| 2 — Data Quality | 10–12 | Filter returns, derive date/revenue fields, standardise columns |
| 3 — Join | 20–23 | Star-schema enrichment: 4 LEFT JOINs |
| 4 — Formula Enrichment | 30 | COGS, MarginPct, DealTier, DiscountTier, PaymentRisk, AccountTier, PctDailyTarget |
| 5 — Macro | 40–41 | CommissionCalculator: FinalCommission, CommissionBand |
| 6 — Aggregation | 50–56 | 4 Summarize nodes + 2 post-agg Formula nodes |
| 7 — Output | 60–64 | 5 CSV outputs |

---

### Enterprise Notebooks

The workflow is split into 6 focused Databricks notebooks. Notebooks 03–06 are independent and can run in parallel once notebook 02 completes.

**Execution order:**
```
01_ingest_quality  →  02_star_schema_enrichment  →  03_customer_kpi_report
                                                 →  04_product_performance
                                                 →  05_rep_commission_report
                                                 →  06_regional_dashboard
```

---

### enterprise_01_ingest_quality.py

**Alteryx layers:** Input (Tools 1–5) + Data Quality (Tools 10–12)

**Pipeline:**
```
sales_transactions.csv ─┐
customers.csv          ─┤
products.csv           ─┤─ (dimension reads only, used in notebook 02)
sales_reps.csv         ─┤
regional_targets.csv   ─┘

sales_transactions.csv
  → Filter ReturnFlag
      ├─ [N] valid → Formula (date + revenue fields) → Select (standardise) → staging_valid_transactions
      └─ [Y] returned ──────────────────────────────────────────────────────→ staging_returned_orders
```

**Key transformations:**

| Step | Alteryx Tool | Implementation |
|---|---|---|
| Read 5 CSVs | InputData ×5 (Tools 1–5) | `read_csv()` for each file |
| Split returns | Filter Tool 10 (`ReturnFlag = 'N'`) | `filter_rows()` + `.filter()` |
| Parse date | Formula Tool 11 — `DateTimeParse` | `F.to_date("TransactionDate", "yyyy-MM-dd")` |
| Derive Quarter | Formula Tool 11 — `'Q' + ToString(IIF(...))` | `F.concat(F.lit("Q"), F.quarter(...))` |
| Derive Year / Month | Formula Tool 11 | `F.year()`, `F.date_format(..., "MMMM")` |
| GrossRevenue | `Quantity × UnitPrice` | `F.col("Quantity") * F.col("UnitPrice")` |
| NetRevenue | `GrossRevenue × (1 − Discount)` | `F.col("GrossRevenue") * (1 - F.col("Discount"))` |
| DiscountAmount | `GrossRevenue − NetRevenue` | Column subtraction |
| Standardise columns | Select Tool 12 | `select_cols()` — drops `ReturnFlag`, renames `Discount` → `DiscountRate` |

**Output Delta tables:**
- `managedcatalog.biju_gold.staging_valid_transactions` — 57 valid rows
- `managedcatalog.biju_gold.staging_returned_orders` — 3 returned rows

---

### enterprise_02_star_schema_enrichment.py

**Alteryx layers:** Join (Tools 20–23) + Formula Enrichment (Tool 30) + Macro (Tools 40–41)

**Pipeline:**
```
staging_valid_transactions ─┐
customers.csv ──────────────┤─ Join 1 (CustomerID)
                            ├─ Join 2 (ProductID)    + products.csv
                            ├─ Join 3 (SalesRepID)   + sales_reps.csv
                            ├─ Join 4 (Region+Quarter) + regional_targets.csv
                            ├─ KPI Formula block
                            └─ Commission Calculator (inlined macro)
                                         ↓
                              enriched_transactions (Delta)
```

**Join details:**

| Join | Alteryx Tool | Key | Columns added |
|---|---|---|---|
| Join 1 | Tool 20 | `CustomerID` | CustomerName, Segment, Region, Country, State, City, CreditLimit, AccountOpenDate |
| Join 2 | Tool 21 | `ProductID` | ProductName, Category, SubCategory, Brand, CostPrice, MSRP, WarrantyMonths |
| Join 3 | Tool 22 | `SalesRepID` | RepName, Territory, CommissionRate, YTDQuota |
| Join 4 | Tool 23 | `Region + Quarter` (composite) | RevenueTarget, UnitTarget, RetentionTarget |

**KPI formula enrichment (Tool 30):**

| Column | Formula |
|---|---|
| `COGS` | `CostPrice × Quantity` |
| `GrossMargin` | `NetRevenue − COGS` |
| `MarginPct` | `(GrossRevenue − COGS) / GrossRevenue × 100` |
| `DealTier` | NetRevenue ≥2000 → Platinum, ≥800 → Gold, ≥300 → Silver, else Bronze |
| `RevenuePerUnit` | `NetRevenue / Quantity` |
| `PctDailyTarget` | `NetRevenue / (RevenueTarget / 90) × 100` |
| `DiscountTier` | DiscountRate ≥0.15 → Heavy, ≥0.05 → Standard, else None |
| `PaymentRisk` | Wire+High → Low Risk, Credit Card → Medium Risk, PayPal → PayPal Risk |
| `AccountAgeDays` | `DATEDIFF(TransactionDate, AccountOpenDate)` |
| `AccountTier` | CreditLimit ≥80k → Strategic, ≥30k → Major, ≥12k → Standard, else Starter |

**Commission macro (Tools 40–41, inlined):**

| Column | Logic |
|---|---|
| `TierMultiplier` | Platinum=1.5, Gold=1.25, Silver=1.0, Bronze=0.75 |
| `ReturnMultiplier` | Always 1.0 (all records are valid sales here) |
| `BaseCommission` | `NetRevenue × CommissionRate` |
| `FinalCommission` | `ROUND(BaseCommission × TierMultiplier × ReturnMultiplier, 2)` |
| `CommissionBand` | ≥500 → High Commission, ≥200 → Mid Commission, else Standard |

**Output Delta table:** `managedcatalog.biju_gold.enriched_transactions` — 57 fully enriched rows

---

### enterprise_03_customer_kpi_report.py

**Alteryx layers:** Summarize A (Tool 50) + Post-agg Formula (Tool 51) + Output (Tool 60)

**Source:** `managedcatalog.biju_gold.enriched_transactions`

**GroupBy:** `CustomerID`, `CustomerName`, `Segment`, `Region`, `AccountTier`

**Aggregations:**

| Output Column | Aggregation | Source |
|---|---|---|
| `TotalOrders` | COUNT | TransactionID |
| `TotalRevenue` | SUM | NetRevenue |
| `TotalGross` | SUM | GrossRevenue |
| `TotalMargin` | SUM | GrossMargin |
| `TotalCOGS` | SUM | COGS |
| `TotalUnits` | SUM | Quantity |
| `AvgOrderValue` | AVG | NetRevenue |
| `MaxOrderValue` | MAX | NetRevenue |
| `TotalCommission` | SUM | FinalCommission |
| `FirstOrderDate` | MIN | TransactionDate |
| `LastOrderDate` | MAX | TransactionDate |

**Post-aggregation formulas:**

| Column | Formula |
|---|---|
| `MarginPct` | `TotalMargin / TotalGross × 100` |
| `AvgOrderValue` | `TotalRevenue / TotalOrders` (recalculated) |
| `CustomerValueBand` | TotalRevenue ≥10k → VIP, ≥5k → High Value, ≥2k → Growth, else Emerging |

**Output Delta table:** `managedcatalog.biju_gold.customer_kpi_report` — ~20 rows

---

### enterprise_04_product_performance.py

**Alteryx layers:** Summarize B (Tool 52) + Output (Tool 61)

**Source:** `managedcatalog.biju_gold.enriched_transactions`

**GroupBy:** `ProductID`, `ProductName`, `Category`, `SubCategory`, `Brand`, `MSRP`, `CostPrice`, `WarrantyMonths`

**Aggregations:**

| Output Column | Aggregation | Source |
|---|---|---|
| `TotalOrders` | COUNT | TransactionID |
| `TotalUnitsSold` | SUM | Quantity |
| `TotalRevenue` | SUM | NetRevenue |
| `TotalMargin` | SUM | GrossMargin |
| `TotalDiscount` | SUM | DiscountAmount |
| `AvgSalePrice` | AVG | NetRevenue |
| `UniqueCustomers` | COUNT DISTINCT | CustomerID |

**Output Delta table:** `managedcatalog.biju_gold.product_performance` — 10 rows (one per product)

---

### enterprise_05_rep_commission_report.py

**Alteryx layers:** Summarize C (Tool 53) + Post-agg Formula (Tool 54) + Output (Tool 62)

**Source:** `managedcatalog.biju_gold.enriched_transactions`

**GroupBy:** `SalesRepID`, `RepName`, `Territory`, `Region`, `CommissionRate`, `YTDQuota`

**Aggregations:**

| Output Column | Aggregation | Source |
|---|---|---|
| `TotalDeals` | COUNT | TransactionID |
| `TotalRevenue` | SUM | NetRevenue |
| `TotalMargin` | SUM | GrossMargin |
| `TotalCommission` | SUM | FinalCommission |
| `TotalUnitsSold` | SUM | Quantity |
| `UniqueAccounts` | COUNT DISTINCT | CustomerID |
| `LargestDeal` | MAX | NetRevenue |

**Post-aggregation formulas:**

| Column | Formula |
|---|---|
| `QuotaAttainmentPct` | `TotalRevenue / YTDQuota × 100` |
| `PerformanceBand` | ≥100% → Overachiever, ≥80% → On Track, ≥60% → At Risk, else Underperforming |
| `AvgDealSize` | `TotalRevenue / TotalDeals` |

**Output Delta table:** `managedcatalog.biju_gold.rep_commission_report` — 5 rows (one per rep)

---

### enterprise_06_regional_dashboard.py

**Alteryx layers:** Summarize D (Tool 55) + Post-agg Formula (Tool 56) + Outputs (Tools 63–64)

**Source:** `managedcatalog.biju_gold.enriched_transactions`

**GroupBy:** `Region`, `Quarter`, `RevenueTarget`, `UnitTarget`

**Aggregations:**

| Output Column | Aggregation | Source |
|---|---|---|
| `TotalTransactions` | COUNT | TransactionID |
| `ActualRevenue` | SUM | NetRevenue |
| `ActualMargin` | SUM | GrossMargin |
| `ActualUnits` | SUM | Quantity |
| `TotalCommission` | SUM | FinalCommission |
| `ActiveAccounts` | COUNT DISTINCT | CustomerID |
| `AvgTransactionValue` | AVG | NetRevenue |

**Post-aggregation formulas:**

| Column | Formula |
|---|---|
| `RevenueAttainmentPct` | `ActualRevenue / RevenueTarget × 100` |
| `RevenueGap` | `ActualRevenue − RevenueTarget` |
| `TargetStatus` | ≥100% → Target Met, ≥80% → Close to Target, else Off Track |
| `MarginPct` | `ActualMargin / ActualRevenue × 100` |

**Output Delta tables:**
- `managedcatalog.biju_gold.regional_dashboard` — 8 rows (4 regions × 2 quarters)
- `managedcatalog.biju_gold.returned_orders_flagged` — 3 rows (promoted from staging)

---

## Macro Transformation Logic

All Alteryx macros (`.yxmc`) have been inlined directly into PySpark notebooks. No separate macro files are needed in Databricks. This section documents every macro, its original logic, where it is embedded, and the exact PySpark implementation.

---

### Macro 1: `macro_calculate_discount.yxmc`

**Source workflow:** `complex_workflow.yxmd`
**Embedded in:** `complex_workflow.py` — Step 4 (Formula cell)
**Purpose:** Apply a flat 10% discount to every transaction's `amount` field.

#### Original Alteryx Formula Tool Expression
```
[amount] * 0.9
```

#### PySpark Implementation
```python
# complex_workflow.py — Step 4
df_discounted = add_column(df_joined, "discounted_amount", "amount * 0.9")
```
Calls `transform.py → add_column()`, which uses `F.expr()` under the hood:
```python
df.withColumn("discounted_amount", F.expr("amount * 0.9"))
```

#### Input / Output Fields

| Field | Direction | Type | Description |
|---|---|---|---|
| `amount` | Input | double | Order amount (cast before use via `cast_cols`) |
| `discounted_amount` | Output | double | `amount * 0.9` — 10% discount applied |

#### Column Cast Required
Because `amount` is read from CSV and may be inferred as string, a `cast_cols` call precedes the formula:
```python
df_joined = cast_cols(df_joined, {"amount": "double"})
df_discounted = add_column(df_joined, "discounted_amount", "amount * 0.9")
```

---

### Macro 2: `CommissionCalculator.yxmc`

**Source workflow:** `Enterprise_Sales_Commission_Pipeline.yxmd` (Tool 40–41)
**Embedded in:** `enterprise_02_star_schema_enrichment.py` — Layer 5 cell
**Purpose:** Calculate tiered sales commission with a return penalty clawback, producing `FinalCommission` and `CommissionBand` for every transaction.

#### Original Alteryx Macro Inputs
| Field | Type | Description |
|---|---|---|
| `NetRevenue` | Double | Net transaction revenue after discount |
| `CommissionRate` | Double | Rep's base commission rate (e.g. 0.08) |
| `ReturnFlag` | String | `Y` = returned order, `N` = valid sale |
| `DealTier` | String | Platinum / Gold / Silver / Bronze |

#### Original Alteryx Formula Expressions (verbatim)

```
# TierMultiplier
IIF([DealTier]='Platinum', 1.50,
  IIF([DealTier]='Gold', 1.25,
    IIF([DealTier]='Silver', 1.00, 0.75)))

# ReturnMultiplier — 50% clawback on returned orders
IIF([ReturnFlag]='Y', 0.50, 1.00)

# BaseCommission
[NetRevenue] * [CommissionRate]

# FinalCommission
ROUND([BaseCommission] * [TierMultiplier] * [ReturnMultiplier], 2)

# CommissionBand
IIF([FinalCommission] >= 500, 'High Commission',
  IIF([FinalCommission] >= 200, 'Mid Commission', 'Standard Commission'))
```

#### PySpark Implementation
```python
# enterprise_02_star_schema_enrichment.py — Layer 5
df_enriched = (
    df_kpi
    .withColumn("TierMultiplier",
        F.when(F.col("DealTier") == "Platinum", 1.50)
         .when(F.col("DealTier") == "Gold",     1.25)
         .when(F.col("DealTier") == "Silver",   1.00)
         .otherwise(0.75))
    .withColumn("ReturnMultiplier", F.lit(1.0))   # all records here are valid sales (ReturnFlag=N)
    .withColumn("BaseCommission",
        F.round(F.col("NetRevenue") * F.col("CommissionRate"), 2))
    .withColumn("FinalCommission",
        F.round(F.col("BaseCommission") * F.col("TierMultiplier") * F.col("ReturnMultiplier"), 2))
    .withColumn("CommissionBand",
        F.when(F.col("FinalCommission") >= 500, "High Commission")
         .when(F.col("FinalCommission") >= 200, "Mid Commission")
         .otherwise("Standard Commission"))
)
```

> **Note on ReturnMultiplier:** In the original macro, `ReturnFlag=Y` triggers a 0.5× clawback. In the Databricks pipeline, returned transactions are split off in notebook 01 (`staging_returned_orders`) before this logic runs, so all records reaching notebook 02 are valid sales and `ReturnMultiplier` is always `1.0`. The clawback logic is preserved as a constant rather than removed, so it can be re-activated if returns are ever processed through the enrichment pipeline.

#### Tier Multiplier Reference

| DealTier | NetRevenue Threshold | TierMultiplier | Effect |
|---|---|---|---|
| Platinum | ≥ $2,000 | 1.50× | 50% bonus on commission |
| Gold | ≥ $800 | 1.25× | 25% bonus on commission |
| Silver | ≥ $300 | 1.00× | Standard commission |
| Bronze | < $300 | 0.75× | 25% reduction on commission |

#### Commission Band Reference

| CommissionBand | FinalCommission Threshold |
|---|---|
| High Commission | ≥ $500 |
| Mid Commission | ≥ $200 |
| Standard Commission | < $200 |

#### Output Fields Written to `enriched_transactions`

| Field | Type | Description |
|---|---|---|
| `TierMultiplier` | double | Multiplier based on DealTier |
| `ReturnMultiplier` | double | Always 1.0 in current pipeline |
| `BaseCommission` | double | `NetRevenue × CommissionRate` |
| `FinalCommission` | double | `BaseCommission × TierMultiplier × ReturnMultiplier` |
| `CommissionBand` | string | High / Mid / Standard Commission |

#### Downstream Usage of `FinalCommission`

`FinalCommission` is computed once in notebook 02 and consumed by three downstream notebooks:

| Notebook | Usage |
|---|---|
| `enterprise_03_customer_kpi_report.py` | `SUM(FinalCommission)` → `TotalCommission` per customer |
| `enterprise_05_rep_commission_report.py` | `SUM(FinalCommission)` → `TotalCommission` per rep |
| `enterprise_06_regional_dashboard.py` | `SUM(FinalCommission)` → `TotalCommission` per region/quarter |

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

1. Open notebooks in the Databricks workspace at:
   `https://dbc-1109a291-3564.cloud.databricks.com`
   under `/Workspace/Users/btucker5543@gmail.com/Databricks/Alteryx/example3`

2. All notebooks depend on `transform` being in the **same workspace folder**. The `%run ./transform` at the top of each notebook resolves it by relative path.

### Simple / Medium / Complex workflows

Run in any order — they are fully independent.

```sql
SELECT * FROM managedcatalog.biju_gold.simple_workflow;
SELECT * FROM managedcatalog.biju_gold.medium_workflow;
SELECT * FROM managedcatalog.biju_gold.complex_workflow;
```

### Enterprise Sales Commission Pipeline

Run in the order below. Notebooks 03–06 can run in parallel once 02 completes.

```
Step 1:  enterprise_01_ingest_quality             (produces staging tables)
Step 2:  enterprise_02_star_schema_enrichment     (produces enriched_transactions)
Step 3+: enterprise_03_customer_kpi_report        ┐
         enterprise_04_product_performance        ├─ run in parallel
         enterprise_05_rep_commission_report      │
         enterprise_06_regional_dashboard         ┘
```

Query output tables after a full run:

```sql
-- Staging
SELECT * FROM managedcatalog.biju_gold.staging_valid_transactions;
SELECT * FROM managedcatalog.biju_gold.staging_returned_orders;

-- Enriched
SELECT * FROM managedcatalog.biju_gold.enriched_transactions;

-- Reports
SELECT * FROM managedcatalog.biju_gold.customer_kpi_report      ORDER BY TotalRevenue DESC;
SELECT * FROM managedcatalog.biju_gold.product_performance       ORDER BY TotalRevenue DESC;
SELECT * FROM managedcatalog.biju_gold.rep_commission_report     ORDER BY TotalCommission DESC;
SELECT * FROM managedcatalog.biju_gold.regional_dashboard        ORDER BY Region, Quarter;
SELECT * FROM managedcatalog.biju_gold.returned_orders_flagged;
```
