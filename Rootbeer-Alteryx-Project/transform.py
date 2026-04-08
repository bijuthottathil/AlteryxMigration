# Databricks notebook source
# MAGIC %md
# MAGIC # transform — Generic DataFrame Transformation Utilities
# MAGIC Shared helper library for all Alteryx-converted notebooks.
# MAGIC Use via `%run ./transform` at the top of any notebook.

# COMMAND ----------

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DataType
from typing import Optional, Union


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


def save_as_uc_table_from_config(
    df: DataFrame,
    cfg: dict,
    *,
    schema_key: str,
    table_key: str,
    temp_view: str = "_tmp_pipeline_uc_write",
) -> None:
    """Create or replace a Delta table using `pipeline_config` keys from notebook 00.

    Uses SQL with backticks so catalog names like ``na-dbxtraining`` work.
    """
    catalog = cfg["catalog"]
    schema_name = cfg[schema_key]
    table_name = cfg[table_key]
    df.createOrReplaceTempView(temp_view)
    fqtn = f"`{catalog}`.`{schema_name}`.`{table_name}`"
    spark.sql(f"CREATE OR REPLACE TABLE {fqtn} AS SELECT * FROM {temp_view}")
    spark.catalog.dropTempView(temp_view)
    print(f"[save_as_uc_table_from_config] {fqtn}")


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


# ---------------------------------------------------------------------------
# Alteryx macro parity — Rootbeer-Alteryx-Project (see Rootbeer Alteryx Project.yxmd)
# External .yxmc files are not in repo; logic is inferred from workflow metadata.
# ---------------------------------------------------------------------------

# State Names.xlsx equivalent (Name, Abbreviation) for FindReplace / Standard Macro path
_US_STATES = (
    "Alabama,AL;Alaska,AK;Arizona,AZ;Arkansas,AR;California,CA;Colorado,CO;Connecticut,CT;"
    "Delaware,DE;Florida,FL;Georgia,GA;Hawaii,HI;Idaho,ID;Illinois,IL;Indiana,IN;Iowa,IA;"
    "Kansas,KS;Kentucky,KY;Louisiana,LA;Maine,ME;Maryland,MD;Massachusetts,MA;Michigan,MI;"
    "Minnesota,MN;Mississippi,MS;Missouri,MO;Montana,MT;Nebraska,NE;Nevada,NV;New Hampshire,NH;"
    "New Jersey,NJ;New Mexico,NM;New York,NY;North Carolina,NC;North Dakota,ND;Ohio,OH;"
    "Oklahoma,OK;Oregon,OR;Pennsylvania,PA;Rhode Island,RI;South Carolina,SC;South Dakota,SD;"
    "Tennessee,TN;Texas,TX;Utah,UT;Vermont,VT;Virginia,VA;Washington,WA;West Virginia,WV;"
    "Wisconsin,WI;Wyoming,WY;District of Columbia,DC"
)


def state_names_dataframe(spark) -> DataFrame:
    """Mirror State Names.xlsx: columns Name, Abbreviation."""
    rows = [p.split(",") for p in _US_STATES.split(";") if p.strip()]
    return spark.createDataFrame(rows, ["Name", "Abbreviation"])


def macro_imputation_v3_fill_null(
    df: DataFrame,
    column: str,
    with_value: Union[int, float] = 0,
) -> DataFrame:
    """Imputation_v3.yxmc: replace nulls in a numeric column with a constant (workflow used 0)."""
    return df.withColumn(column, F.coalesce(F.col(column), F.lit(with_value)))


def macro_multi_field_formula_brand_urls(
    df: DataFrame,
    brandname_col: str = "brandname",
    url_fields: tuple = ("website", "facebookpage", "twitter"),
) -> DataFrame:
    """Multi-field formula on brand data: null URL -> Google search for brandname (ToolID 75)."""
    google = F.concat(F.lit("http://www.google.com/search?q="), F.col(brandname_col))
    out = df
    for c in url_fields:
        if c in df.columns:
            out = out.withColumn(
                c,
                F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), google).otherwise(F.col(c)),
            )
    return out


def macro_cleanse_review_text(df: DataFrame, description_col: str = "description") -> DataFrame:
    """Cleanse.yxmc (approximation): trim and normalize whitespace on text fields present."""
    if description_col not in df.columns:
        return df
    return df.withColumn(description_col, F.trim(F.regexp_replace(F.col(description_col), r"\s+", " ")))


def distinct_creditcardtypes_for_batch_macro(transactions: DataFrame) -> DataFrame:
    """Control stream for Alteryx_Batch_Macro_Final: distinct creditcardtype (Summarize ToolID 170)."""
    return transactions.select("creditcardtype").distinct().filter(F.col("creditcardtype").isNotNull())


def macro_batch_summarize_revenue_profit(
    transactions: DataFrame,
    profit_col: str = "Profit",
) -> DataFrame:
    """Alteryx_Batch_Macro_Final.yxmc output shape: creditcardtype, Revenue, Sum_Profit.

    Workflow MetaInfo: Revenue = Sum(purchaseprice), Sum_Profit = Sum(Profit).
    Profit is not in raw CSV; compute before calling (see add_placeholder_profit).
    """
    if profit_col not in transactions.columns:
        raise ValueError(f"Missing {profit_col}; join costs or run add_placeholder_profit() first.")
    pp = F.col("purchaseprice").cast("double")
    pr = F.col(profit_col).cast("double")
    return transactions.groupBy("creditcardtype").agg(
        F.sum(pp).alias("Revenue"),
        F.sum(pr).alias("Sum_Profit"),
    )


def add_placeholder_profit(
    df: DataFrame,
    purchase_col: str = "purchaseprice",
    unit_cost: float = 1.0,
) -> DataFrame:
    """When wholesale cost is unavailable, Profit = purchaseprice - unit_cost (batch macro input)."""
    p = F.col(purchase_col).cast("double")
    return df.withColumn("Profit", p - F.lit(unit_cost))


def standard_macro_maven_join(
    customers: DataFrame,
    reviews: DataFrame,
    states: Optional[DataFrame] = None,
    spark=None,
) -> DataFrame:
    """Alteryx_Maven_Standard_Macro_Final.yxmc (approximation).

    Workflow feeds Customers, Reviews, States. Implement as customers left join reviews on customerid,
    then map state abbreviation -> full Name using state lookup (FindReplace equivalent).
    """
    if states is None:
        if spark is None:
            raise ValueError("Provide states DataFrame or spark for default US state names.")
        states = state_names_dataframe(spark)
    rev = reviews
    j = customers.join(rev, on="customerid", how="left")
    st = states.select(
        F.col("Abbreviation").alias("_abbr"),
        F.col("Name").alias("_state_full"),
    )
    if "state" not in j.columns:
        return j
    return (
        j.join(st, F.upper(F.trim(F.col("state"))) == F.upper(F.col("_abbr")), how="left")
        .withColumn("state", F.coalesce(F.col("_state_full"), F.col("state")))
        .drop("_abbr", "_state_full")
    )


def prepare_rootbeer_transactions(raw: DataFrame) -> DataFrame:
    """Tools 60 + 65: rename to Designer labels and cast common types."""
    mapping = {
        "creditcardnumber": "Credit Card Number",
        "creditcardtype": "Credit Card Type",
        "customerid": "Customer ID",
        "locationid": "Location ID",
        "purchaseprice": "Purchase Price",
        "rootbeerid": "Root Beer ID",
        "transactiondate": "Transaction Date",
        "transactionid": "Transaction ID",
    }
    df = raw
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    td = F.to_date(F.col("Transaction Date"))
    df = df.withColumn("Transaction Date", td)
    for c, t in [
        ("Credit Card Number", "long"),
        ("Customer ID", "int"),
        ("Location ID", "int"),
        ("Purchase Price", "double"),
        ("Root Beer ID", "int"),
        ("Transaction ID", "int"),
    ]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))
    return df


def add_transaction_time_features(df: DataFrame) -> DataFrame:
    """Tool 74 Formula: Month (first of month), Month Num, Year."""
    d = F.col("Transaction Date")
    return (
        df.withColumn("Month", F.date_trunc("month", d))
        .withColumn("Month Num", F.month(d).cast("string"))
        .withColumn("Year", F.year(d).cast("string"))
    )


def add_days_since_last_purchase(df: DataFrame) -> DataFrame:
    """Tool 76 Multi-Row Formula grouped by Customer ID."""
    w = Window.partitionBy("Customer ID").orderBy("Transaction Date")
    prev = F.lag("Transaction Date").over(w)
    return df.withColumn(
        "Days Since Last Purchase",
        F.when(prev.isNull(), F.lit(None).cast("int")).otherwise(F.datediff(F.col("Transaction Date"), prev)),
    )


def build_rootbeer_enriched_transactions(
    base_path: str,
    include_brand_join: bool = False,
    brand_csv_path: Optional[str] = None,
) -> DataFrame:
    """Replays 'Transactions - Transform Data' through Union (86): select, types, time features,
    lag imputation, join rootbeer, geolocation, left-union pattern as single left join, optional brand.
    """
    tx_path = f"{base_path.rstrip('/')}/transaction.csv"
    rb_path = f"{base_path.rstrip('/')}/rootbeer.csv"
    geo_path = f"{base_path.rstrip('/')}/geolocation.csv"

    raw_tx = read_csv(tx_path)
    rb = read_csv(rb_path)
    geo = read_csv(geo_path)

    for c, t in [
        ("rootbeerid", "int"),
        ("brandid", "int"),
        ("locationid", "int"),
        ("purchasedate", "date"),
    ]:
        if c in rb.columns:
            rb = rb.withColumn(c, F.col(c).cast(t))

    for c, t in [("locationid", "int"), ("latitude", "double"), ("longitude", "double")]:
        if c in geo.columns:
            geo = geo.withColumn(c, F.col(c).cast(t))

    tx = prepare_rootbeer_transactions(raw_tx)
    tx = add_transaction_time_features(tx)
    tx = tx.orderBy("Customer ID", "Transaction Date")
    tx = add_days_since_last_purchase(tx)
    tx = macro_imputation_v3_fill_null(tx, "Days Since Last Purchase", 0)

    rb_f = rb.filter(F.col("locationid") != 0)
    rb_geo = rb_f.join(geo, on="locationid", how="inner").select(
        *([F.col(c) for c in rb_f.columns]
        + [
            F.col("latitude").alias("geo_latitude"),
            F.col("longitude").alias("geo_longitude"),
            F.col("location").alias("geo_location_text"),
        ])
    )

    tx_rb = tx.join(rb, tx["Root Beer ID"] == rb["rootbeerid"], how="inner")
    enriched = (
        tx_rb.alias("tr")
        .join(
            rb_geo.alias("rg"),
            F.col("tr.`Location ID`") == F.col("rg.locationid"),
            how="left",
        )
        .selectExpr("tr.*", "rg.geo_latitude", "rg.geo_longitude", "rg.geo_location_text")
    )

    if include_brand_join and brand_csv_path:
        brands = read_csv(brand_csv_path)
        for c in ("brandid",):
            if c in brands.columns:
                brands = brands.withColumn(c, F.col(c).cast("int"))
        brands = macro_multi_field_formula_brand_urls(brands)
        bcols = ["brandid", "city", "state", "website", "facebookpage", "twitter", "brandname"]
        present = [c for c in bcols if c in brands.columns]
        b = brands.select(*[F.col(c) for c in present]).alias("b")
        if "brandid" in enriched.columns:
            enriched = enriched.join(b, on="brandid", how="left")

    if "purchasedate" in enriched.columns and "Transaction Date" in enriched.columns:
        enriched = enriched.withColumn(
            "Time on Shelf",
            F.datediff(F.col("Transaction Date"), F.col("purchasedate")),
        )
    return enriched


def batch_macro_creditcard_revenue_profit_from_volume(base_path: str) -> DataFrame:
    """Batch macro output using raw transaction.csv (same stream as Alteryx ToolID 4 -> 226)."""
    raw = read_csv(f"{base_path.rstrip('/')}/transaction.csv")
    raw = add_placeholder_profit(raw, "purchaseprice", 1.0)
    return macro_batch_summarize_revenue_profit(raw, "Profit")


def read_google_rootbeer_union(base_path: str) -> DataFrame:
    """Input tool google*.csv: union all matching files (same schema)."""
    import glob

    paths = sorted(glob.glob(f"{base_path.rstrip('/')}/google_rootbeer_*.csv"))
    if not paths:
        raise FileNotFoundError(f"No google_rootbeer_*.csv under {base_path}")
    dfs = [read_csv(p) for p in paths]
    acc = dfs[0]
    for d in dfs[1:]:
        acc = acc.unionByName(d, allowMissingColumns=True)
    return acc


print("[transform] Utilities loaded: read_csv, write_output, save_as_delta_table,")
print("            save_as_uc_table_from_config, filter_rows, drop_nulls, drop_duplicates,")
print("            join_dfs, add_column, rename_cols, cast_cols, select_cols, summarize, preview")
print("[transform] Rootbeer macros: state_names_dataframe, macro_imputation_v3_fill_null,")
print("            macro_multi_field_formula_brand_urls, macro_cleanse_review_text,")
print("            macro_batch_summarize_revenue_profit, batch_macro_creditcard_revenue_profit_from_volume,")
print("            add_placeholder_profit, standard_macro_maven_join,")
print("            build_rootbeer_enriched_transactions, read_google_rootbeer_union")
