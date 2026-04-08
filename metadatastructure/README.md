# Metadata Structure

This folder contains a metadata-driven Databricks pipeline that orchestrates all workflows defined in this repository.

## Files

- `workflow_metadata.json`  
  Defines workflow steps, notebook paths, dependencies, timeouts, and enable/disable flags.
- `metadata_pipeline.py`  
  Databricks notebook-style runner that reads metadata and executes workflows in dependency order.

## How to Run in Databricks

1. Import/sync this repository to Databricks Repos.
2. Open and run `metadatastructure/metadata_pipeline.py`.
3. Optionally set `PIPELINE_DRY_RUN = True` to validate dependency order without executing notebooks.

## Behavior

- Executes all enabled workflows.
- Validates unknown dependencies and cycles before execution.
- Supports per-workflow timeout and notebook parameters.
- Runs level-by-level based on dependency graph.
