# AlteryxMigration

Migration of Alteryx Designer workflows to **Databricks** using **PySpark**, **Unity Catalog**, and **Delta Lake**. This repository combines a **Rootbeer sample pipeline** (medallion architecture, ADLS-backed paths) with an **Enterprise Sales Commission** workflow.

---

## Repository layout (current)

```
AlteryxMigration/
├── README.md
├── transform.py                          # Generic helpers for enterprise notebooks (%run ./transform)
│
├── Rootbeer-Alteryx-Project/
│   ├── transform.py                      # Generic + Rootbeer helpers, macros, UC save helpers
│   ├── notebook_00_setup_configuration.ipynb
│   ├── notebook_01_rootbeer_transactions_pipeline.ipynb
│   ├── notebook_02_google_trends_union.ipynb
│   ├── Rootbeer Alteryx Project.yxmd     # Source Alteryx workflow
│   ├── *.csv                             # Sample source data (transactions, customers, etc.)
│   └── README.md                         # Original project notes
│
├── Enterprise_Sales_Commission_Pipeline/   # Alteryx package (.yxmd, macros, data)
├── enterprise_01_ingest_quality.py
├── enterprise_02_star_schema_enrichment.py
├── enterprise_03_customer_kpi_report.py
├── enterprise_04_product_performance.py
├── enterprise_05_rep_commission_report.py
├── enterprise_06_regional_dashboard.py
└── enterprise_explore.py
```

**Note:** The **simple / medium / complex** demo workflows were removed from the repo; the **Rootbeer** folder is the maintained Alteryx-to-Databricks reference for Designer workflows and Unity Catalog table naming.

---

## Rootbeer-Alteryx-Project (latest)

Databricks notebooks are **`.ipynb`** files. They `%run` the configuration notebook, then `%run ./transform` from the **same folder** (`Rootbeer-Alteryx-Project`).

| Notebook | Purpose |
|----------|---------|
| `notebook_00_setup_configuration.ipynb` | Unity Catalog catalog/schemas/table names, ADLS `abfss` base, Volume paths, Spark settings, `pipeline_config` temp view |
| `notebook_01_rootbeer_transactions_pipeline.ipynb` | Bronze/silver/gold loads; enriched transactions; fact/dim tables; batch-macro and gold summaries — all names driven by `pipeline_config` |
| `notebook_02_google_trends_union.ipynb` | Union `google_rootbeer_*.csv` and persist to configured bronze/silver tables |

**Run order:** `00` → `01` (and `02` as needed). Notebook `01`/`02` call `00` via `%run ./notebook_00_setup_configuration`.

**Shared library:** `Rootbeer-Alteryx-Project/transform.py` includes generic helpers (`read_csv`, `filter_rows`, `join_dfs`, …), Rootbeer pipeline functions (`build_rootbeer_enriched_transactions`, …), macro-style helpers (`macro_imputation_v3_fill_null`, `standard_macro_maven_join`, …), and **`save_as_uc_table_from_config`** to create/replace Delta tables using keys from `pipeline_config`.

**ADLS / config (see notebook 00):** Storage account **`dlsdbxtraining002`**, container **`alteryx`**, with `abfss://alteryx@dlsdbxtraining002.dfs.core.windows.net/...` for external locations. CSV landing path is configured as a **Volume** path under `SOURCE_DATA_PATH` (adjust to your workspace volume).

**Source workflow:** `Rootbeer Alteryx Project.yxmd` (Alteryx 2023.1).

---

## Enterprise Sales Commission pipeline

Python notebooks **`enterprise_01`** … **`enterprise_06`** implement the packaged workflow in `Enterprise_Sales_Commission_Pipeline/` (ingest → star schema → KPI / product / rep / regional outputs). They use the **repository root** `transform.py` for shared I/O and transforms.

**Suggested order:**

```
enterprise_01_ingest_quality
  → enterprise_02_star_schema_enrichment
  → enterprise_03 … enterprise_06 (parallel after 02)
```

Table names and paths in those notebooks follow your Unity Catalog deployment (examples in older docs may say `managedcatalog.biju_gold.*` — align with your catalog/schema in the notebook code).

Details (layers, macro logic, column mappings) are unchanged in substance; see the **Enterprise** sections in git history if you need the long-form reference, or read the `enterprise_*.py` files directly.

---

## `transform.py` — two copies

| Location | Use |
|----------|-----|
| **Repo root** `transform.py` | Enterprise notebooks (`enterprise_*.py`) |
| **`Rootbeer-Alteryx-Project/transform.py`** | Rootbeer notebooks; extends root helpers with Rootbeer + UC `pipeline_config` integration |

Load in Databricks with:

```python
# MAGIC %run ./transform
```

(`spark` must exist in the notebook session.)

---

## Alteryx → PySpark tool mapping (summary)

| Alteryx | `transform.py` (typical) |
|---------|---------------------------|
| Input Data | `read_csv` |
| Filter | `filter_rows` |
| Join | `join_dfs` |
| Formula | `add_column` |
| Summarize | `summarize` |
| Select | `select_cols` |
| Browse | `preview` |
| Output | `write_output` / `save_as_delta_table` / `save_as_uc_table_from_config` |

---

## Running in Databricks

1. Import the repo (**Repos**) or sync files into a workspace folder.
2. For **Rootbeer**, open the **`Rootbeer-Alteryx-Project`** folder so `%run ./transform` and `%run ./notebook_00_setup_configuration` resolve correctly.
3. Set **`SOURCE_DATA_PATH`** / Volume paths in **`notebook_00`** to the location where CSVs are uploaded (must match your ADLS-backed volume).
4. Run **`notebook_00`**, then **`notebook_01`** and **`notebook_02`** as needed.

---

## License / attribution

Rootbeer sample data and workflow originate from the Alteryx Designer learning project; enterprise pipeline assets live under `Enterprise_Sales_Commission_Pipeline/`.
