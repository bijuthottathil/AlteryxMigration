# Rootbeer-Alteryx-Project

This folder contains the **Rootbeer** Alteryx Designer sample migrated to **Databricks** (**PySpark** + **Unity Catalog** + **Delta**).

## What’s here

| Asset | Description |
|-------|-------------|
| `Rootbeer Alteryx Project.yxmd` | Original Alteryx workflow (2023.1) |
| `*.csv` | Source data (transactions, customers, root beer catalog, reviews, geolocation, Google Trends, etc.) |
| `transform.py` | Shared notebook library: generic helpers + Rootbeer pipeline + macro-style functions + `save_as_uc_table_from_config` |
| `notebook_00_setup_configuration.ipynb` | Catalog, schemas, table names, ADLS `abfss` settings, Volume paths, `pipeline_config` temp view |
| `notebook_01_rootbeer_transactions_pipeline.ipynb` | End-to-end loads into bronze/silver/gold and fact/dim tables defined in config |
| `notebook_02_google_trends_union.ipynb` | Unions `google_rootbeer_*.csv` into configured tables |

## How to run

1. Upload or sync CSVs to the **Volume path** set as `SOURCE_DATA_PATH` in **notebook 00** (see parent repo **`README.md`**).
2. Run **`notebook_00_setup_configuration.ipynb`**, then **`notebook_01`** / **`notebook_02`**.

## Documentation

See the repository root **`../README.md`** for full layout, Enterprise pipeline pointers, and ADLS/UC notes.

---

## Original project summary

Import and prepare raw data, fix data issues, join sources, derive metrics, visualize, and report. Alteryx Designer was used to explore ETL patterns for this dataset.

![workflow](https://github.com/nikkimutallim/Rootbeer-Alteryx-Project/assets/116984010/0980e551-5240-4ef5-a392-5cb59550880c)
