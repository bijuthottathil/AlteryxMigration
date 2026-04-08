# Rootbeer-Alteryx-Project

This folder contains the **Rootbeer** Alteryx Designer sample and its migration to **Databricks** (**PySpark** + **Unity Catalog** + **Delta**).

The source workflow is **`Rootbeer Alteryx Project.yxmd`** (Alteryx document version **2023.1**). The sections below inventory **all major tool containers, data sources, native tools, and external macros** referenced in that file. Paths inside the `.yxmd` point at a local `Datasets` folder from the original author’s machine; in this repo, use the **same filenames** next to the workflow or on your Databricks Volume.

---

## Databricks assets in this folder

| Asset | Description |
|-------|-------------|
| `transform.py` | Shared notebook library: generic helpers + Rootbeer pipeline + macro-style functions + `save_as_uc_table_from_config` |
| `notebook_00_setup_configuration.ipynb` | UC catalog/schemas/table names, ADLS `abfss` settings, Volume paths, `pipeline_config` temp view |
| `notebook_01_rootbeer_transactions_pipeline.ipynb` | Bronze/silver/gold loads; enriched transactions; fact/dim tables; batch + gold summaries |
| `notebook_02_google_trends_union.ipynb` | Unions `google_rootbeer_*.csv` into configured tables |

**Run order:** `notebook_00` → `notebook_01` / `notebook_02`. See the repository root **`README.md`** for layout and infrastructure notes.

---

## Complete inventory (Rootbeer Alteryx Project.yxmd)

Generated from the workflow XML (`yxmdVer="2023.1"`). **Tool Container** captions can appear more than once on the canvas; the table below lists each distinct region.

### Tool containers (canvas regions)

| Container caption | Role (summary) |
|-------------------|----------------|
| **Data Inputs** | Primary **Input Data** tools for CSV/XLSX sources used across the workflow |
| **Transactions – Transform Data** | Core transaction ETL: **Select**, **Auto Field**, **Formula**, **Sort**, **Multi Row Formula**, **Imputation_v3** macro, **Join** (root beer, geolocation, brand), **Union**, **Time on Shelf** formula; **DateTimeNow** macro; **Browse** QA nodes |
| **Transactions – Transform Data** *(second region)* | Duplicate titled region in the `.yxmd` carrying the same transform pattern (Designer layout) |
| **Transactions – Combine Data** | **Join**, **Formula**, spatial **Summarize**, **Append Fields**, **Filter**, **Select**, **Running Total**, **Sort**, **Multi Row Formula**, **Transpose** — brand/market combination logic |
| **Reviews** | **Record ID**, **Sort**, **Cleanse** macro, **Auto Field**, **Join**, **Formula**, **Multi Field Formula**, **Union**, **Join Multiple**, **Text To Columns**, **DateTime**, **RegEx**, **Cross Tab**, **WeightedAvg** macro |
| **Sampling** | **Sample** (first row per `brandid` + `starrating`) and **SelectRecords** macro |
| **Geospatial Analysis** | **Filter** (`locationid != 0`), **Create Points**, **Distance**, **Trade Area**, **Append Fields**, **Summarize**, **Formula**, **Report Map**, **Browse** |
| **Reviews vs. Customers** | **Unique**, **Join**, **Union**, **Find Replace** (state abbrev → full name via **State Names.xlsx**) |
| **Batch Macro** | Spatial **Summarize** → distinct `creditcardtype` as control stream → **Alteryx_Batch_Macro_Final.yxmc** |
| **Standard Macro** | **Alteryx_Maven_Standard_Macro_Final.yxmc** (customers × reviews × states) |
| **Output – Data Sources** | **Output Data** (`.yxdb` / file output) |
| **Output – Reports** | **Composer Render** targets, **Email** |
| **Sample Brand Insights Dashboard** | **Join Multiple** (by record position) assembling insight streams |
| **Sample Market Analysis Report [Using Visual Layout]** | **Composer Text**, **Browse**, **Plotly**, **Composer Image**, **Visual Layout** |
| **Sample Market Analysis Report [Using Overlay]** | **Composer Text**, **Browse**, **Plotly**, **Join Multiple**, **Composer Overlay** (repeated YOY blocks) |
| **Trade Area Map Report** | **Composer Text**, **Composer Table**, **Report Map**, **Browse**, **Join Multiple**, **Composer Layout**, **Legend_Splitter**, **Legend_Builder**, **ReportHeader** (`RHEngine.yxmc`), **FooterMacro** |
| **Test Tools** | **Text Input**, **Directory**, **Dynamic Input** (`google*.csv`), **Input Data**, **Browse**, **Output Data**, **HtmlBox**, **Union** — labeled as a testing ground in the workflow |
| **Disclaimer (annotation)** | **Text Box** (ToolID **37**): states **Test Tools** are not part of the final flow |

### Native Alteryx tools (unique types in this `.yxmd`) → PySpark

Every **Plugin** value below appears at least once in the workflow. The **PySpark / `transform.py`** column is the usual migration target; Portfolio/spatial-reporting tools are recreated outside Spark unless noted.

| Alteryx tool (friendly name) | Designer `Plugin` (suffix) | PySpark / Databricks approach | `transform.py` helper (this folder) |
|------------------------------|----------------------------|-------------------------------|-------------------------------------|
| Select | `…AlteryxSelect` | `select`, `withColumnRenamed` | `select_cols`, `rename_cols` |
| Auto Field | `…AutoField` | `inferSchema` / explicit `cast` | `cast_cols` |
| Browse | `…BrowseV2` | `display` / `show` / schema | `preview` |
| Cross Tab | `…CrossTab` | `groupBy` / `pivot` | (custom `groupBy` in notebook) |
| DateTime | `…DateTime` | `to_timestamp`, `date_trunc` | `add_column` with `F.*` |
| Input Data | `…DbFileInput` | `spark.read` CSV/XLSX | `read_csv` |
| Output Data | `…DbFileOutput` | Delta / files | `write_output`, `save_as_delta_table`, `save_as_uc_table_from_config` |
| Directory | `…Directory` | path listing for **Dynamic Input** | configured in notebook / `base_path` |
| Dynamic Input | `…DynamicInput` | wildcard read + union | `read_google_rootbeer_union` |
| Filter | `…Filter` | `filter(expr)` | `filter_rows` |
| Find Replace | `…FindReplace` | dimension join or `regexp_replace` | `join_dfs` + `state_names_dataframe` |
| Formula | `…Formula` | `withColumn` | `add_column` |
| Append Fields | `…AppendFields` | `join` | `join_dfs` |
| Join | `…Join` | `join` | `join_dfs` |
| Join Multiple | `…JoinMultiple` | chained joins | multiple `join_dfs` |
| Multi Field Formula | `…MultiFieldFormula` | conditional columns | `macro_multi_field_formula_brand_urls` |
| Multi Row Formula | `…MultiRowFormula` | `Window` + `lag` | `add_days_since_last_purchase` |
| Record ID | `…RecordID` | `row_number` / monotonic id | (inline `Window` in notebook) |
| RegEx | `…RegEx` | `regexp_extract` | `add_column` |
| Running Total | `…RunningTotal` | `Window` `sum` | (inline `Window` in notebook) |
| Sample | `…Sample` | `sampleBy` / `row_number` | Alteryx sample + `macro_select_records_row_ranges` for record-window logic |
| Sort | `…Sort` | `orderBy` | (inline in pipeline functions) |
| Text Input | `…TextInput` | `createDataFrame` / static data | (rare; notebook only) |
| Text To Columns | `…TextToColumns` | `split` | `F.split` / `add_column` |
| Transpose | `…Transpose` | `stack` / SQL unpivot | Spark SQL |
| Union | `…Union` | `unionByName` | `DataFrame.unionByName` |
| Unique | `…Unique` | `dropDuplicates` | `drop_duplicates` |
| Summarize (spatial plugin) | `AlteryxSpatialPluginsGui.Summarize` | `groupBy` + aggregates | `summarize` |
| Create Points | `…CreatePoints` | lat/lon → geometry (e.g. Sedona, H3, or BI map) | not in `transform.py` |
| Distance | `…Distance` | spatial distance | not in `transform.py` |
| Trade Area | `…TradeArea` | buffers / polygons | not in `transform.py` |
| Report Map | `…ReportMap` | map output | Lakeview / GIS / BI |
| DateTime Now (tool) | `DateTimeNow` | current timestamp | `macro_datetime_now_column` |
| Composer Render / Text / Image / Table / Overlay / Layout | `PortfolioPluginsGui.*` | reports | Composer / Word / PDF in Designer → **Dashboards** on Delta |
| Email | `…Email.Email` | alerting | Databricks Jobs / email outside Spark |
| Plotly Charting | `PlotlyCharting` | charts | BI tool on gold tables |
| Visual Layout | `VisualLayout` | layout | BI |
| Report Header (tool shell) | `ReportHeader` | header chrome | paired with `RHEngine.yxmc` (not ported) |
| Html Box | `…HtmlBox` | documentation link | N/A |
| Text Box | `…TextBox` | canvas annotation | N/A |
| Tool Container | `…ToolContainer` | grouping only | N/A |

**I/O and hygiene helpers** (used across patterns): `read_csv`, `write_output`, `save_as_delta_table`, `save_as_uc_table_from_config`, `drop_nulls`, `summarize`, etc.

### External macros (`.yxmc`) → `transform.py` functions

| Macro referenced in `.yxmd` | PySpark function(s) in `Rootbeer-Alteryx-Project/transform.py` | Notes |
|----------------------------|------------------------------------------------------------------|--------|
| `Alteryx_Batch_Macro_Final.yxmc` | `distinct_creditcardtypes_for_batch_macro`, `macro_batch_summarize_revenue_profit`, `add_placeholder_profit`, `batch_macro_creditcard_revenue_profit_from_volume` | Per `creditcardtype`: **Revenue**, **Sum_Profit** |
| `Alteryx_Maven_Standard_Macro_Final.yxmc` | `standard_macro_maven_join` | Uses `state_names_dataframe` for **State Names.xlsx** parity |
| `Imputation_v3.yxmc` | `macro_imputation_v3_fill_null` | Nulls in target numeric column → constant (default `0`) |
| `Cleanse.yxmc` | `macro_cleanse_review_text` | Trim + collapse whitespace on text column |
| `WeightedAvg.yxmc` | `macro_weighted_average` | **Weighted Average** column per group |
| `SelectRecords.yxmc` | `macro_select_records_row_ranges` | Row index windows after sort |
| `DateTimeNow\Supporting_Macros\DTNEngine.yxmc` | `macro_datetime_now_column` | **As-of** date column |
| Multi-field formula (reviews / brand URLs; aligns with Tool 75 pattern in workflow) | `macro_multi_field_formula_brand_urls` | Fills `website`, `facebookpage`, `twitter` from `brandname` when blank |
| `Legend_Splitter.yxmc`, `Legend_Builder.yxmc`, `ReportHeader\Supporting_Macros\RHEngine.yxmc`, `FooterMacro.yxmc` | *none* | Map/report chrome only; recreate in dashboards |

### Rootbeer pipeline functions (native tool chains in `transform.py`)

These compose **Select / Auto Field / Formula / Sort / Multi Row Formula / Join / Union** style logic (not single `.yxmc` files):

| Function | Purpose |
|----------|---------|
| `prepare_rootbeer_transactions` | Rename/select columns from raw transactions |
| `add_transaction_time_features` | Month / year fields from transaction date |
| `add_days_since_last_purchase` | **Multi Row Formula**–style lag by customer |
| `build_rootbeer_enriched_transactions` | End-to-end enriched fact stream (joins, imputation, **Time on Shelf**) |
| `read_google_rootbeer_union` | **Dynamic Input** `google*.csv` union |

Docstrings in **`transform.py`** list **input columns**, **output columns**, and **grain** where relevant. The first notebook **Markdown** cell duplicates the **macro ↔ PySpark** matrix for quick reference in Databricks.

### All functions defined in `Rootbeer-Alteryx-Project/transform.py`

| Category | Functions |
|----------|-----------|
| I/O | `read_csv`, `write_output`, `save_as_delta_table`, `save_as_uc_table_from_config` |
| Filtering / keys | `filter_rows`, `drop_nulls`, `drop_duplicates` |
| Joins | `join_dfs` |
| Columns / types | `add_column`, `rename_cols`, `cast_cols`, `select_cols` |
| Aggregation | `summarize` |
| Inspection | `preview` |
| State lookup (replaces **State Names.xlsx** in macro path) | `state_names_dataframe` |
| Macro parity | `macro_imputation_v3_fill_null`, `macro_multi_field_formula_brand_urls`, `macro_cleanse_review_text`, `distinct_creditcardtypes_for_batch_macro`, `macro_batch_summarize_revenue_profit`, `add_placeholder_profit`, `standard_macro_maven_join`, `macro_weighted_average`, `macro_select_records_row_ranges`, `macro_datetime_now_column` |
| Batch macro (end-to-end from Volume path) | `batch_macro_creditcard_revenue_profit_from_volume` |
| Rootbeer native pipeline | `prepare_rootbeer_transactions`, `add_transaction_time_features`, `add_days_since_last_purchase`, `build_rootbeer_enriched_transactions` |
| Google Trends input | `read_google_rootbeer_union` |

---

## Data inputs (Input Data tools)

Files referenced in the workflow (use these names on your Volume or in this repo).

| Source file | Format | Typical use in workflow |
|-------------|--------|-------------------------|
| `transaction.csv` | CSV | Transactions; drives batch macro input; transform/rename to “Credit Card Number”, “Customer ID”, etc. |
| `customer.csv` | CSV | Customer dimension; reviews vs. customers; standard macro |
| `rootbeer.csv` | CSV | Root beer product / purchase date / `brandid` / `locationid` |
| `rootbeerreview.csv` | CSV | Reviews; brand insights; sampling; weighted metrics |
| `rootbeerbrand.xlsx` (`rootbeerbrand$`) | Excel | Brand attributes, ingredients flags, URLs, wholesale/retail pricing |
| `geolocation.csv` | CSV | Lat/long and `location` text for `locationid` |
| `location.csv` | CSV | Location dimension: name, address, city, state, zip |
| `customer_geocoding.xlsx` (`geocoding$`) | Excel | Geocoded customer addresses (lat/lon, formatted address, confidence, etc.) |
| `State Names.xlsx` (`Sheet1$`) | Excel | US state **Name** and **Abbreviation** for Find Replace |
| `google_rootbeer_2018.csv` (and wildcard) | CSV | **Google Trends** style weekly series; workflow uses **`google*.csv`** on one input |
| `geolocation.csv` (ToolID 56) | CSV | Standalone input + **Browse** (duplicate path used for profiling) |

> **Repo note:** Some inputs exist only as **Excel** in the original Designer bundle; this repo includes **`rootbeerbrand.xlsx`** where provided. If a file is missing, the matching Databricks bronze/silver step may skip or fail until you upload it to `SOURCE_DATA_PATH`.

---

## Core transaction pipeline (inside **Transactions – Transform Data**)

Approximate logical order (tool types as in the `.yxmd`):

1. **Select** — Rename fields to business labels (e.g. `creditcardnumber` → **Credit Card Number**, `rootbeerid` → **Root Beer ID**).
2. **Auto Field** — Infer/set field types after CSV read.
3. **Select** — Pass-through / column housekeeping.
4. **Formula** — **Month** (`DateTimeTrim` month), **Month Num**, **Year** from **Transaction Date**.
5. **Select** — All fields.
6. **Sort** — **Customer ID**, **Transaction Date** ascending (feeds multi-row logic).
7. **Multi Row Formula** — **Days Since Last Purchase** = day difference from prior row, **Group By** **Customer ID**.
8. **Imputation_v3.yxmc** — Replace nulls in **Days Since Last Purchase** with **0**.
9. **Join** — Transactions to **rootbeer** on **Root Beer ID** = `rootbeerid`.
10. **Join** — Root beer (non-zero locations) to **geolocation** on `locationid`.
11. **Join** — Combine location context with transaction **Location ID** (then **Union** of join + left output as in Designer).
12. **Union** (by name) — Merge parallel branches before brand join.
13. **Join** — Attach **brand** attributes on `brandid` (from brand stream with city/state for reporting).
14. **Formula** — **Time on Shelf** = `DateTimeDiff([Transaction Date], [purchasedate], 'days')`.

Supporting tools in the same canvas area include **Browse** nodes for profiling and **DateTimeNow** (macro) where configured.

---

## Reviews and brand paths (inside **Reviews**)

- **Auto Field** on reviews; **Sort** by `reviewdate`.
- **Record ID** → **Review ID**.
- **Cleanse.yxmc** on text / description fields (per macro configuration in Designer).
- **Auto Field** on brand (`rootbeerbrand.xlsx`).
- **Join** reviews to brands on `brandid`.
- **Formula** — e.g. customer full name from first + last name.
- **Multi Field Formula** — If `website` / `facebookpage` / `twitter` null, substitute Google search URL using `brandname`.
- **Join** / **Union** / **Join Multiple** — Assemble streams for downstream reporting and **Cross Tab** / **WeightedAvg.yxmc** (weighted average of star rating by `brandname`, weight `Length`, output **Weighted Average**).

---

## Geospatial analysis (inside **Geospatial Analysis**)

- **Filter** — `[locationid] != 0` on root beer stream.
- Joins to **geolocation** and spatial **Allocate**-style preparation (point/region assignment per workflow).
- **Filter** on latitude/longitude (e.g. non-null checks).
- **Report Map** — Thematic map output with color ramp / smart tiling.
- Additional **Browse** tools for QA.

---

## Reporting & output (Portfolio / Composer)

Native **Portfolio** and charting tools used include:

| Engine entry / plugin (representative) | Purpose |
|----------------------------------------|---------|
| **Composer Render** | Renders composed reports to files (e.g. `.docx` paths in configuration) |
| **Composer Text** | Rich text / KPI headers (YOY customer %, revenue %, etc.) |
| **Composer Image** | Images in layout |
| **Composer Table** | Tables with bar-style formatting |
| **Composer Overlay** | Overlay layout sections |
| **Composer Visual Layout** | Page layout composition |
| **Composer Layout** | Section layout |
| **Plotly Charting** | Interactive charts |
| **Email** | Composer email output configuration |
| **Report Map** | Map output from spatial tools |

These are **Designer-specific**; on Databricks you typically recreate visuals in **Lakeview**, **Dashboards**, or BI tools reading **Delta** tables produced by the notebooks.

Macro ↔ function and native-tool ↔ PySpark mappings are summarized in **[Complete inventory (Rootbeer Alteryx Project.yxmd)](#complete-inventory-rootbeer-alteryx-projectyxmd)** above and in the first **Markdown** cell of **`transform.py`**.

---

## Original project summary

Import and prepare raw data, fix data issues, join sources, derive metrics, visualize, and report. Alteryx Designer was used to explore ETL patterns for this dataset.

![workflow](https://github.com/nikkimutallim/Rootbeer-Alteryx-Project/assets/116984010/0980e551-5240-4ef5-a392-5cb59550880c)
