---
name: parquet-arrow-peek
description: Zero-egress schema-first inspection workflow for Parquet, Arrow, Feather, CSV, and JSONL data files before writing analytical scripts or transformations.
---

# `parquet-arrow-peek`: Zero-Egress Tabular Data Workflow

This skill defines the schema-first, zero-egress workflow for interacting with columnar (`.parquet`, `.arrow`, `.feather`, `.ipc`) and tabular files (`.csv`, `.tsv`, `.jsonl`, `.ndjson`).

## Core Principles

1. **Schema-First Inspection:**
   - ALWAYS invoke the `inspect_schema` tool (`mcp__parquet-arrow-peek__inspect_schema` or CLI equivalent) before writing any code that processes tabular data.
   - Review column names, logical types, null rates, and footer statistics to understand data shapes and edge cases.

2. **Never Read Raw Data into Context:**
   - Direct raw reads of columnar files and tabular text files `> 50 KB` are strictly blocked by guardrail hooks.
   - Do NOT attempt to read `.parquet`, `.arrow`, or large `.csv`/`.jsonl` files using file reading tools (`read_file`, `view_file`).

3. **Zero Raw Row Dumps to Stdout:**
   - NEVER generate scripts or commands that print raw tabular rows or sample records to `stdout`.
   - Prohibited patterns:
     - `print(df.head())`, `print(df)`
     - `df.show()`, `df.take(N)`
     - `SELECT * FROM tbl LIMIT N`
     - Iterating rows and printing row contents.

4. **Aggregate or File-Based Downstream Execution:**
   - Generated Polars, DuckDB, Arrow, or PySpark scripts must:
     - Write transformed results directly to disk (e.g., `output.parquet`, `summary.csv`).
     - Print ONLY scalar aggregations (e.g., `COUNT(*)`, `SUM(...)`, `AVG(...)`, or execution status).
     - Validate schemas programmatically without displaying row values.

## Recommended Code Patterns

### DuckDB
```python
import duckdb

# Inspect aggregates or export results without dumping rows
con = duckdb.connect()
con.execute("""
    COPY (
        SELECT category, COUNT(*) AS count, AVG(amount) AS avg_amount
        FROM 'data.parquet'
        GROUP BY category
    ) TO 'summary.parquet' (FORMAT PARQUET)
""")
scalar_metric = con.execute("SELECT COUNT(*) FROM 'summary.parquet'").fetchone()[0]
print(f"Processed {scalar_metric} categories.")
```

### Polars
```python
import polars as pl

# Scan lazily, transform, and write sink directly to disk
q = (
    pl.scan_parquet("data.parquet")
    .filter(pl.col("status") == "active")
    .group_by("region")
    .agg(pl.col("revenue").sum().alias("total_revenue"))
)
q.sink_parquet("region_revenue.parquet")
print("Aggregation written successfully to region_revenue.parquet")
```
