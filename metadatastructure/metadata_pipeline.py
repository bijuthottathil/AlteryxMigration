# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata-Driven Workflow Pipeline
# MAGIC Executes all repository workflows from metadata with dependency-aware ordering.

# COMMAND ----------

import json
from pathlib import PurePosixPath
from collections import defaultdict, deque
from typing import Dict, List


METADATA_FILE = "./metadatastructure/workflow_metadata.json"


def _get_notebook_path() -> str:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return ctx.notebookPath().get()


def _workspace_repo_root() -> str:
    current_nb = PurePosixPath(_get_notebook_path())
    return str(current_nb.parent)


def _resolve_notebook_path(notebook_ref: str) -> str:
    root = PurePosixPath(_workspace_repo_root())
    ref = PurePosixPath(notebook_ref)
    if str(ref).startswith("/"):
        return str(ref)
    return str(root / ref)


def _load_metadata(metadata_file: str) -> Dict:
    with open(metadata_file, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return metadata


def _validate_workflows(workflows: List[Dict]) -> Dict[str, Dict]:
    by_id = {}
    for workflow in workflows:
        wf_id = workflow["id"]
        if wf_id in by_id:
            raise ValueError(f"Duplicate workflow id found: {wf_id}")
        by_id[wf_id] = workflow

    for wf_id, workflow in by_id.items():
        for dep_id in workflow.get("depends_on", []):
            if dep_id not in by_id:
                raise ValueError(f"Workflow '{wf_id}' has unknown dependency '{dep_id}'")

    return by_id


def _build_levels(workflows_by_id: Dict[str, Dict]) -> List[List[str]]:
    in_degree = {wf_id: 0 for wf_id in workflows_by_id}
    children = defaultdict(list)

    for wf_id, workflow in workflows_by_id.items():
        for dep in workflow.get("depends_on", []):
            children[dep].append(wf_id)
            in_degree[wf_id] += 1

    queue = deque(sorted([wf_id for wf_id, deg in in_degree.items() if deg == 0]))
    levels = []
    processed = 0

    while queue:
        level_size = len(queue)
        level = []
        for _ in range(level_size):
            node = queue.popleft()
            level.append(node)
            processed += 1
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
        levels.append(sorted(level))

    if processed != len(workflows_by_id):
        raise ValueError("Cycle detected in workflow dependencies.")

    return levels


def _run_notebook(workflow: Dict, default_timeout: int, dry_run: bool) -> str:
    timeout = int(workflow.get("timeout_seconds", default_timeout))
    params = workflow.get("params", {})
    notebook_path = _resolve_notebook_path(workflow["path"])
    wf_id = workflow["id"]

    if dry_run:
        print(f"[DRY RUN] {wf_id} -> {notebook_path} (timeout={timeout}s, params={params})")
        return "DRY_RUN"

    print(f"[RUN] {wf_id} -> {notebook_path} (timeout={timeout}s)")
    result = dbutils.notebook.run(notebook_path, timeout, params)
    print(f"[DONE] {wf_id} result: {result}")
    return result


def run_pipeline(metadata_file: str = METADATA_FILE, dry_run: bool = False) -> Dict[str, str]:
    metadata = _load_metadata(metadata_file)
    workflows = [wf for wf in metadata["workflows"] if wf.get("enabled", True)]
    default_timeout = int(metadata.get("default_timeout_seconds", 7200))

    workflows_by_id = _validate_workflows(workflows)
    levels = _build_levels(workflows_by_id)

    print(f"Pipeline: {metadata.get('pipeline_name', 'unnamed_pipeline')}")
    print(f"Workflow count: {len(workflows_by_id)}")
    print(f"Execution levels: {len(levels)}")

    results = {}
    for level_idx, level in enumerate(levels, start=1):
        print(f"\n=== Level {level_idx} ===")
        for wf_id in level:
            wf = workflows_by_id[wf_id]
            if wf.get("type", "notebook") != "notebook":
                raise ValueError(f"Unsupported workflow type for '{wf_id}': {wf.get('type')}")
            try:
                results[wf_id] = _run_notebook(wf, default_timeout, dry_run=dry_run)
            except Exception as exc:
                print(f"[FAILED] {wf_id}: {exc}")
                raise

    print("\nPipeline execution complete.")
    return results


# COMMAND ----------

# Default run mode
PIPELINE_DRY_RUN = False
pipeline_results = run_pipeline(dry_run=PIPELINE_DRY_RUN)
pipeline_results
