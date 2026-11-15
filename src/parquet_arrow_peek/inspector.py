"""O(1) Parquet and Arrow footer reader plus bounded 64KB CSV/JSONL sampler."""

from __future__ import annotations

import csv
import io
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from .budget import enforce_budget, format_error_response

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.ipc as pa_ipc
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


def _arrow_logical_type_str(pa_type: Any) -> str:
    """Format PyArrow data type into clean logical type string."""
    if not HAS_PYARROW or pa_type is None:
        return "UNKNOWN"
    if pa.types.is_list(pa_type) or pa.types.is_large_list(pa_type):
        return f"LIST<{_arrow_logical_type_str(pa_type.value_type)}>"
    if pa.types.is_map(pa_type):
        k = _arrow_logical_type_str(pa_type.key_type)
        v = _arrow_logical_type_str(pa_type.item_type)
        return f"MAP<{k}, {v}>"
    if pa.types.is_struct(pa_type):
        return "STRUCT"
    if pa.types.is_timestamp(pa_type):
        return f"TIMESTAMP[{pa_type.unit}]"
    if pa.types.is_date(pa_type):
        return "DATE"
    if pa.types.is_int64(pa_type):
        return "INT64"
    if pa.types.is_int32(pa_type):
        return "INT32"
    if pa.types.is_int16(pa_type):
        return "INT16"
    if pa.types.is_int8(pa_type):
        return "INT8"
    if pa.types.is_float64(pa_type):
        return "FLOAT64"
    if pa.types.is_float32(pa_type):
        return "FLOAT32"
    if pa.types.is_boolean(pa_type):
        return "BOOLEAN"
    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
        return "STRING"
    if pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
        return "BINARY"
    return str(pa_type).upper()


def _arrow_physical_type_str(pa_type: Any) -> str:
    """Map Arrow type to physical type string matching Parquet conventions."""
    if not HAS_PYARROW or pa_type is None:
        return "BYTE_ARRAY"
    if pa.types.is_boolean(pa_type):
        return "BOOLEAN"
    if pa.types.is_int32(pa_type) or pa.types.is_int16(pa_type) or pa.types.is_int8(pa_type):
        return "INT32"
    if pa.types.is_int64(pa_type) or pa.types.is_timestamp(pa_type) or pa.types.is_date(pa_type):
        return "INT64"
    if pa.types.is_float32(pa_type):
        return "FLOAT"
    if pa.types.is_float64(pa_type):
        return "DOUBLE"
    if pa.types.is_string(pa_type) or pa.types.is_binary(pa_type) or pa.types.is_large_string(pa_type):
        return "BYTE_ARRAY"
    if pa.types.is_fixed_size_binary(pa_type):
        return "FIXED_LEN_BYTE_ARRAY"
    return "BYTE_ARRAY"


def _flatten_arrow_fields(
    fields: Iterable[Any],
    prefix: str = "",
    depth: int = 1,
    max_depth: int = 3,
) -> List[Tuple[str, Any]]:
    """Traverse and flatten nested Arrow Struct fields up to depth 3."""
    flat: List[Tuple[str, Any]] = []
    for f in fields:
        name = f"{prefix}.{f.name}" if prefix else f.name
        if HAS_PYARROW and pa.types.is_struct(f.type) and depth < max_depth:
            sub = [f.type[i] for i in range(f.type.num_fields)]
            flat.extend(_flatten_arrow_fields(sub, prefix=name, depth=depth + 1, max_depth=max_depth))
        else:
            flat.append((name, f))
    return flat


def _inspect_parquet(path: str, size_bytes: int, include_stats: bool) -> str:
    """Inspect Parquet file by reading footer metadata O(1)."""
    if not HAS_PYARROW:
        return format_error_response(
            "PYARROW_NOT_INSTALLED",
            "pyarrow is required to inspect Parquet files.",
            {"path": path, "format": "parquet", "size_bytes": size_bytes},
        )

    # Validate Parquet footer magic bytes (PAR1) without reading whole file
    if size_bytes < 12:
        return format_error_response(
            "CORRUPT_FOOTER",
            "Invalid or truncated Parquet/Arrow footer magic bytes.",
            {"path": path, "format": "parquet", "size_bytes": size_bytes},
        )

    try:
        with open(path, "rb") as f:
            f.seek(0)
            head = f.read(4)
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
            if head != b"PAR1" or tail != b"PAR1":
                return format_error_response(
                    "CORRUPT_FOOTER",
                    "Invalid or truncated Parquet/Arrow footer magic bytes.",
                    {"path": path, "format": "parquet", "size_bytes": size_bytes},
                )
    except Exception as e:
        return format_error_response(
            "CORRUPT_FOOTER",
            f"Failed to read magic bytes: {e}",
            {"path": path, "format": "parquet", "size_bytes": size_bytes},
        )

    try:
        pq_file = pq.ParquetFile(path)
        meta = pq_file.metadata
    except Exception as exc:
        return format_error_response(
            "CORRUPT_FOOTER",
            f"Invalid or truncated Parquet/Arrow footer magic bytes: {exc}",
            {"path": path, "format": "parquet", "size_bytes": size_bytes},
        )

    num_rows = meta.num_rows
    num_row_groups = meta.num_row_groups

    file_info = {
        "format": "parquet",
        "path": path,
        "row_count": num_rows,
        "row_count_exact": True,
        "row_groups": num_row_groups,
        "size_bytes": size_bytes,
    }

    # Aggregate column statistics across row groups
    stats_by_col: Dict[str, Dict[str, Any]] = {}
    null_counts: Dict[str, int] = {}

    if num_rows > 0 and include_stats:
        for rg_idx in range(num_row_groups):
            rg = meta.row_group(rg_idx)
            for c_idx in range(rg.num_columns):
                c = rg.column(c_idx)
                col_path = c.path_in_schema
                s = c.statistics
                if s is None:
                    continue

                if col_path not in null_counts:
                    null_counts[col_path] = 0
                if s.null_count is not None:
                    null_counts[col_path] += s.null_count

                if s.has_min_max:
                    col_stats = stats_by_col.setdefault(col_path, {})
                    c_min = s.min
                    c_max = s.max

                    if c_min is not None:
                        prev_min = col_stats.get("min")
                        if prev_min is None or c_min < prev_min:
                            col_stats["min"] = c_min
                    if c_max is not None:
                        prev_max = col_stats.get("max")
                        if prev_max is None or c_max > prev_max:
                            col_stats["max"] = c_max

                if getattr(s, "distinct_count", None) is not None:
                    col_stats = stats_by_col.setdefault(col_path, {})
                    col_stats["distinct_est"] = s.distinct_count

    # Extract schema columns
    schema_arrow = pq_file.schema_arrow
    raw_fields = [schema_arrow.field(i) for i in range(len(schema_arrow))]
    flattened = _flatten_arrow_fields(raw_fields)

    columns: List[Dict[str, Any]] = []
    for col_name, field in flattened:
        logical_type = _arrow_logical_type_str(field.type)
        physical_type = _arrow_physical_type_str(field.type)

        if num_rows == 0:
            null_pct = None
            col_stats = None
        else:
            total_nulls = null_counts.get(col_name)
            if total_nulls is not None and num_rows > 0:
                null_pct = round((total_nulls / num_rows) * 100.0, 2)
            else:
                null_pct = 0.0
            col_stats = stats_by_col.get(col_name)

        # Composite / struct fields don't store footer stats
        if "." in col_name or pa.types.is_nested(field.type):
            col_stats = None

        columns.append({
            "name": col_name,
            "physical_type": physical_type,
            "logical_type": logical_type,
            "nullable": field.nullable,
            "null_pct": null_pct,
            "stats": col_stats,
        })

    return enforce_budget(file_info, columns, include_stats=include_stats)


def _inspect_arrow_feather(path: str, size_bytes: int, include_stats: bool) -> str:
    """Inspect Arrow / Feather IPC file footer O(1)."""
    if not HAS_PYARROW:
        return format_error_response(
            "PYARROW_NOT_INSTALLED",
            "pyarrow is required to inspect Arrow/Feather files.",
            {"path": path, "format": "arrow", "size_bytes": size_bytes},
        )

    format_name = "feather" if path.endswith(".feather") else "arrow"

    reader = None
    try:
        reader = pa_ipc.open_file(path)
    except Exception:
        try:
            reader = pa_ipc.open_stream(path)
        except Exception as exc:
            return format_error_response(
                "CORRUPT_FOOTER",
                f"Invalid or truncated Parquet/Arrow footer magic bytes: {exc}",
                {"path": path, "format": format_name, "size_bytes": size_bytes},
            )

    row_count = 0
    if hasattr(reader, "count_rows"):
        try:
            row_count = reader.count_rows()
        except Exception:
            row_count = 0

    num_batches = getattr(reader, "num_record_batches", 1)

    file_info = {
        "format": format_name,
        "path": path,
        "row_count": row_count,
        "row_count_exact": True,
        "row_groups": num_batches,
        "size_bytes": size_bytes,
    }

    schema = reader.schema
    raw_fields = [schema.field(i) for i in range(len(schema))]
    flattened = _flatten_arrow_fields(raw_fields)

    columns: List[Dict[str, Any]] = []
    for col_name, field in flattened:
        logical_type = _arrow_logical_type_str(field.type)
        physical_type = _arrow_physical_type_str(field.type)

        columns.append({
            "name": col_name,
            "physical_type": physical_type,
            "logical_type": logical_type,
            "nullable": field.nullable,
            "null_pct": 0.0 if row_count > 0 else None,
            "stats": None,  # Arrow IPC footers do not store column chunk min/max
        })

    return enforce_budget(file_info, columns, include_stats=include_stats)


def _infer_primitive_type(val: str) -> str:
    """Infer primitive logical type from sample string value."""
    s = val.strip()
    if not s:
        return "null"
    if s.lower() in ("true", "false", "t", "f"):
        return "bool"
    if re.match(r"^-?\d+$", s):
        return "int64"
    if re.match(r"^-?\d*\.\d+([eE][-+]?\d+)?$", s):
        return "float64"
    if re.match(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$", s):
        return "timestamp"
    return "utf8"


def _inspect_delimited(path: str, size_bytes: int, delimiter: str, format_name: str, include_stats: bool) -> str:
    """Inspect CSV/TSV with bounded 64KB first-window sampling."""
    try:
        with open(path, "rb") as f:
            chunk = f.read(65536)
    except Exception as exc:
        return format_error_response("READ_ERROR", f"Failed reading file: {exc}", {"path": path, "size_bytes": size_bytes})

    if not chunk:
        return format_error_response("EMPTY_FILE", "File is empty (0 bytes).", {"path": path, "size_bytes": 0})

    text = chunk.decode("utf-8", errors="replace")
    lines = text.splitlines()

    # Discard incomplete trailing line if chunk is truncated
    if size_bytes > 65536 and len(lines) > 1 and not (chunk.endswith(b"\n") or chunk.endswith(b"\r")):
        lines.pop()

    if not lines:
        return format_error_response("EMPTY_FILE", "File has no lines.", {"path": path, "size_bytes": size_bytes})

    try:
        header_reader = csv.reader([lines[0]], delimiter=delimiter)
        header = next(header_reader)
    except Exception as exc:
        return format_error_response("PARSE_ERROR", f"Failed to parse CSV header: {exc}", {"path": path, "size_bytes": size_bytes})

    header = [h.strip() for h in header]
    num_cols = len(header)

    sample_lines = lines[1:251]
    total_samples = len(sample_lines)

    null_markers = {"", "null", "none", "nan", "na", "n/a", "\\n"}
    col_nulls = [0] * num_cols
    col_type_counts = [dict() for _ in range(num_cols)]

    for line in sample_lines:
        try:
            row_reader = csv.reader([line], delimiter=delimiter)
            row = next(row_reader)
        except Exception:
            continue

        for i in range(num_cols):
            val = row[i].strip() if i < len(row) else ""
            if val.lower() in null_markers:
                col_nulls[i] += 1
            else:
                t = _infer_primitive_type(val)
                col_type_counts[i][t] = col_type_counts[i].get(t, 0) + 1

    # Estimate total rows based on average line byte length
    if sample_lines:
        avg_line_bytes = sum(len(line.encode("utf-8")) + 1 for line in sample_lines) / len(sample_lines)
        est_rows = max(1, int(size_bytes / avg_line_bytes))
    else:
        est_rows = 1

    file_info = {
        "format": format_name,
        "path": path,
        "row_count": est_rows,
        "row_count_exact": False,
        "row_groups": 1,
        "size_bytes": size_bytes,
    }

    type_hierarchy = ["int64", "float64", "bool", "timestamp", "utf8"]
    columns: List[Dict[str, Any]] = []

    for i, col_name in enumerate(header):
        null_count = col_nulls[i]
        null_pct = round((null_count / total_samples) * 100.0, 2) if total_samples > 0 else 0.0

        counts = col_type_counts[i]
        if not counts:
            col_type = "utf8"
        elif len(counts) == 1:
            col_type = next(iter(counts.keys()))
        else:
            # Type resolution: fallback to float64 for int+float, else utf8
            keys = set(counts.keys())
            if keys.issubset({"int64", "float64"}):
                col_type = "float64"
            else:
                col_type = "utf8"

        phys_map = {
            "int64": "INT64",
            "float64": "DOUBLE",
            "bool": "BOOLEAN",
            "timestamp": "INT64",
            "utf8": "BYTE_ARRAY",
        }

        columns.append({
            "name": col_name,
            "physical_type": phys_map.get(col_type, "BYTE_ARRAY"),
            "logical_type": col_type.upper(),
            "nullable": True,
            "null_pct": null_pct,
            "stats": None,
        })

    return enforce_budget(file_info, columns, include_stats=include_stats)


def _inspect_jsonl(path: str, size_bytes: int, include_stats: bool) -> str:
    """Inspect JSONL / NDJSON with bounded 64KB first-window sampling."""
    try:
        with open(path, "rb") as f:
            chunk = f.read(65536)
    except Exception as exc:
        return format_error_response("READ_ERROR", f"Failed reading file: {exc}", {"path": path, "size_bytes": size_bytes})

    if not chunk:
        return format_error_response("EMPTY_FILE", "File is empty (0 bytes).", {"path": path, "size_bytes": 0})

    text = chunk.decode("utf-8", errors="replace")
    lines = text.splitlines()

    if size_bytes > 65536 and len(lines) > 1 and not (chunk.endswith(b"\n") or chunk.endswith(b"\r")):
        lines.pop()

    sample_lines = lines[:250]
    if not sample_lines:
        return format_error_response("EMPTY_FILE", "File has no lines.", {"path": path, "size_bytes": size_bytes})

    keys_order: List[str] = []
    col_nulls: Dict[str, int] = {}
    col_types: Dict[str, Dict[str, int]] = {}
    parsed_samples = 0

    for line in sample_lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except Exception:
            continue

        if not isinstance(record, dict):
            continue

        parsed_samples += 1
        for k, v in record.items():
            if k not in keys_order:
                keys_order.append(k)
                col_nulls[k] = 0
                col_types[k] = {}

            if v is None:
                col_nulls[k] += 1
            elif isinstance(v, bool):
                col_types[k]["BOOLEAN"] = col_types[k].get("BOOLEAN", 0) + 1
            elif isinstance(v, int):
                col_types[k]["INT64"] = col_types[k].get("INT64", 0) + 1
            elif isinstance(v, float):
                col_types[k]["FLOAT64"] = col_types[k].get("FLOAT64", 0) + 1
            elif isinstance(v, list):
                col_types[k]["LIST"] = col_types[k].get("LIST", 0) + 1
            elif isinstance(v, dict):
                col_types[k]["STRUCT"] = col_types[k].get("STRUCT", 0) + 1
            else:
                s_val = str(v)
                inferred = _infer_primitive_type(s_val)
                t_str = "TIMESTAMP" if inferred == "timestamp" else "STRING"
                col_types[k][t_str] = col_types[k].get(t_str, 0) + 1

    if sample_lines:
        avg_line_bytes = sum(len(line.encode("utf-8")) + 1 for line in sample_lines) / len(sample_lines)
        est_rows = max(1, int(size_bytes / avg_line_bytes))
    else:
        est_rows = 1

    file_info = {
        "format": "jsonl",
        "path": path,
        "row_count": est_rows,
        "row_count_exact": False,
        "row_groups": 1,
        "size_bytes": size_bytes,
    }

    columns: List[Dict[str, Any]] = []
    for k in keys_order:
        null_count = col_nulls[k] + (parsed_samples - sum(col_types[k].values()) - col_nulls[k])
        null_pct = round((null_count / max(1, parsed_samples)) * 100.0, 2)

        counts = col_types[k]
        if not counts:
            log_type = "STRING"
        elif len(counts) == 1:
            log_type = next(iter(counts.keys()))
        else:
            if set(counts.keys()).issubset({"INT64", "FLOAT64"}):
                log_type = "FLOAT64"
            else:
                log_type = "STRING"

        phys_map = {
            "INT64": "INT64",
            "FLOAT64": "DOUBLE",
            "BOOLEAN": "BOOLEAN",
            "TIMESTAMP": "INT64",
            "STRING": "BYTE_ARRAY",
            "LIST": "BYTE_ARRAY",
            "STRUCT": "BYTE_ARRAY",
        }

        columns.append({
            "name": k,
            "physical_type": phys_map.get(log_type, "BYTE_ARRAY"),
            "logical_type": log_type,
            "nullable": True,
            "null_pct": null_pct,
            "stats": None,
        })

    return enforce_budget(file_info, columns, include_stats=include_stats)


def inspect_schema(path: str, include_stats: bool = True) -> str:
    """Inspect schema and footer metadata for Parquet, Arrow, CSV, and JSONL files."""
    if not path:
        return format_error_response("INVALID_ARGUMENT", "Path parameter cannot be empty.", {"path": ""})

    # Catch path traversal or invalid paths cleanly
    try:
        resolved = os.path.realpath(path)
    except Exception as exc:
        return format_error_response("INVALID_PATH", f"Could not resolve path: {exc}", {"path": path})

    if not os.path.exists(resolved):
        return format_error_response("FILE_NOT_FOUND", f"File '{path}' does not exist.", {"path": path})

    try:
        size_bytes = os.path.getsize(resolved)
    except PermissionError:
        return format_error_response("PERMISSION_DENIED", f"Permission denied for '{path}'.", {"path": path})
    except Exception as exc:
        return format_error_response("OS_ERROR", str(exc), {"path": path})

    if size_bytes == 0:
        return format_error_response("EMPTY_FILE", "File is empty (0 bytes).", {"path": path, "size_bytes": 0})

    lower_path = path.lower()
    if lower_path.endswith(".parquet"):
        return _inspect_parquet(resolved, size_bytes, include_stats=include_stats)
    elif lower_path.endswith((".arrow", ".feather", ".ipc")):
        return _inspect_arrow_feather(resolved, size_bytes, include_stats=include_stats)
    elif lower_path.endswith(".csv"):
        return _inspect_delimited(resolved, size_bytes, delimiter=",", format_name="csv", include_stats=include_stats)
    elif lower_path.endswith(".tsv"):
        return _inspect_delimited(resolved, size_bytes, delimiter="\t", format_name="tsv", include_stats=include_stats)
    elif lower_path.endswith((".jsonl", ".ndjson")):
        return _inspect_jsonl(resolved, size_bytes, include_stats=include_stats)
    else:
        return format_error_response(
            "UNSUPPORTED_FORMAT",
            f"Unsupported file format for '{path}'. Supported: .parquet, .arrow, .feather, .ipc, .csv, .tsv, .jsonl, .ndjson",
            {"path": path, "size_bytes": size_bytes},
        )
