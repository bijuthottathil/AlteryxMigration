# Databricks notebook source
# MAGIC %md
# MAGIC # transform — Generic DataFrame Transformation Utilities
# MAGIC Shared helper library for all Alteryx-converted notebooks.
# MAGIC Use via `%run ./transform` at the top of any notebook.

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DataType
from typing import Union


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def read_csv(path: str, infer_schema: bool = True, header: bool = True) -> DataFrame:
    """Read a CSV file from a Unity Catalog Volume or DBFS path."""
    return (
        spark.read
        .option("header", header)
        .option("inferSchema", infer_schema)
        .csv(path)
    )


def write_output(df: DataFrame, path: str, fmt: str = "csv", mode: str = "overwrite") -> None:
    """Write a DataFrame to a Volume/DBFS path. Default: CSV, overwrite."""
    writer = df.write.mode(mode)
    if fmt == "csv":
        writer.option("header", True).csv(path)
    elif fmt == "parquet":
        writer.parquet(path)
    elif fmt == "delta":
        writer.format("delta").save(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}. Use 'csv', 'parquet', or 'delta'.")
    print(f"[write_output] Written to: {path} (format={fmt}, mode={mode})")


def save_as_delta_table(df: DataFrame, catalog: str, schema: str, table: str, mode: str = "overwrite") -> None:
    """Write a DataFrame as a managed Delta table in Unity Catalog.

    Creates or replaces the table at <catalog>.<schema>.<table>.
    Uses MERGE schema on overwrite so column additions don't fail.
    """
    full_name = f"{catalog}.{schema}.{table}"
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    print(f"[save_as_delta_table] Saved → {full_name} (mode={mode})")


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_rows(df: DataFrame, condition: str) -> DataFrame:
    """Filter rows using a SQL expression string, e.g. 'age > 30'."""
    result = df.filter(condition)
    print(f"[filter_rows] '{condition}' → {result.count()} rows (was {df.count()})")
    return result


def drop_nulls(df: DataFrame, subset: list = None) -> DataFrame:
    """Drop rows with any null values. Optionally restrict to a column subset."""
    result = df.dropna(subset=subset)
    print(f"[drop_nulls] subset={subset} → {result.count()} rows kept")
    return result


def drop_duplicates(df: DataFrame, subset: list = None) -> DataFrame:
    """Remove duplicate rows, optionally considering only specific columns."""
    result = df.dropDuplicates(subset) if subset else df.dropDuplicates()
    print(f"[drop_duplicates] subset={subset} → {result.count()} rows kept")
    return result


# ---------------------------------------------------------------------------
# Joins
# ---------------------------------------------------------------------------

def join_dfs(left: DataFrame, right: DataFrame, on: Union[str, list], how: str = "inner") -> DataFrame:
    """Join two DataFrames. 'on' can be a column name or list of column names."""
    result = left.join(right, on=on, how=how)
    print(f"[join_dfs] {how.upper()} JOIN on {on} → {result.count()} rows")
    return result


# ---------------------------------------------------------------------------
# Column transformations
# ---------------------------------------------------------------------------

def add_column(df: DataFrame, col_name: str, expr_str: str) -> DataFrame:
    """Add or replace a column using a SQL expression string.
    Example: add_column(df, 'discounted_amount', 'amount * 0.9')
    """
    result = df.withColumn(col_name, F.expr(expr_str))
    print(f"[add_column] Added column '{col_name}' = {expr_str}")
    return result


def rename_cols(df: DataFrame, mapping: dict) -> DataFrame:
    """Rename columns using a {old_name: new_name} dict."""
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    print(f"[rename_cols] Renamed: {mapping}")
    return df


def cast_cols(df: DataFrame, schema_map: dict) -> DataFrame:
    """Cast columns to specified types using a {col_name: spark_type_str} dict.
    Example: cast_cols(df, {'age': 'integer', 'amount': 'double'})
    """
    for col_name, dtype in schema_map.items():
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    print(f"[cast_cols] Cast: {schema_map}")
    return df


def select_cols(df: DataFrame, cols: list) -> DataFrame:
    """Select a subset of columns."""
    result = df.select(cols)
    print(f"[select_cols] Selected columns: {cols}")
    return result


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def summarize(df: DataFrame, group_cols: Union[str, list], agg_map: dict) -> DataFrame:
    """Group by columns and apply aggregations.

    agg_map format: {output_col: ('source_col', 'func')}
    Supported funcs: 'sum', 'avg', 'count', 'min', 'max', 'countDistinct'

    Example:
        summarize(df, 'country', {'total_amount': ('amount', 'sum')})
    """
    _agg_funcs = {
        "sum":           F.sum,
        "avg":           F.avg,
        "count":         F.count,
        "min":           F.min,
        "max":           F.max,
        "countDistinct": F.countDistinct,
    }

    if isinstance(group_cols, str):
        group_cols = [group_cols]

    agg_exprs = []
    for alias, (src_col, func_name) in agg_map.items():
        if func_name not in _agg_funcs:
            raise ValueError(f"Unsupported aggregation: '{func_name}'. Choose from {list(_agg_funcs)}")
        agg_exprs.append(_agg_funcs[func_name](src_col).alias(alias))

    result = df.groupBy(group_cols).agg(*agg_exprs)
    print(f"[summarize] GROUP BY {group_cols}, agg={list(agg_map.keys())} → {result.count()} rows")
    return result


# ---------------------------------------------------------------------------
# Inspection helpers
# ---------------------------------------------------------------------------

def preview(df: DataFrame, n: int = 10, label: str = "") -> DataFrame:
    """Show a sample of the DataFrame and return it (chainable)."""
    tag = f"[{label}] " if label else ""
    print(f"{tag}Schema:")
    df.printSchema()
    print(f"{tag}Sample ({n} rows):")
    df.show(n, truncate=False)
    print(f"{tag}Total rows: {df.count()}")
    return df


print("[transform] Utilities loaded: read_csv, write_output, save_as_delta_table,")
print("            filter_rows, drop_nulls, drop_duplicates, join_dfs, add_column,")
print("            rename_cols, cast_cols, select_cols, summarize, preview")
