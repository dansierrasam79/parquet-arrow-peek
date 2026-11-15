"""Deterministic budgeter, token-cap enforcement, and PII stats redaction."""

from __future__ import annotations

import datetime
from decimal import Decimal
import json
from typing import Any, Dict, List, Optional


def sanitize_stats(
    stats: Optional[Dict[str, Any]],
    logical_type: str = "",
    physical_type: str = "",
) -> Optional[Dict[str, Any]]:
    """Sanitize footer column statistics.

    String and binary min/max values are redacted to length metadata (min_len, max_len).
    Only numeric, boolean, and temporal min/max values are preserved verbatim.
    """
    if stats is None:
        return None

    log_upper = (logical_type or "").upper()
    phys_upper = (physical_type or "").upper()

    is_string_or_binary = (
        any(t in log_upper for t in ("STRING", "UTF8", "BINARY", "TEXT"))
        or any(t in phys_upper for t in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"))
    )

    clean: Dict[str, Any] = {}

    min_val = stats.get("min")
    max_val = stats.get("max")

    is_string_value = isinstance(min_val, (str, bytes)) or isinstance(max_val, (str, bytes))

    if is_string_or_binary or is_string_value:
        min_len = None
        max_len = None
        if min_val is not None:
            min_len = len(min_val.encode("utf-8") if isinstance(min_val, str) else min_val)
        if max_val is not None:
            max_len = len(max_val.encode("utf-8") if isinstance(max_val, str) else max_val)

        if min_len is not None:
            clean["min_len"] = min_len
        if max_len is not None:
            clean["max_len"] = max_len
    else:
        def _sanitize_scalar(val: Any) -> Any:
            if val is None:
                return None
            if isinstance(val, (int, float, bool)):
                return val
            if isinstance(val, Decimal):
                return float(val)
            if isinstance(val, (datetime.date, datetime.datetime)):
                return val.isoformat()
            # fallback if unknown object has isoformat
            if hasattr(val, "isoformat") and callable(val.isoformat):
                return val.isoformat()
            return None

        clean_min = _sanitize_scalar(min_val)
        clean_max = _sanitize_scalar(max_val)
        if clean_min is not None:
            clean["min"] = clean_min
        if clean_max is not None:
            clean["max"] = clean_max

    distinct = stats.get("distinct_est", stats.get("distinct_count"))
    if isinstance(distinct, (int, float)):
        clean["distinct_est"] = int(distinct)

    return clean if clean else None


def format_error_response(
    error_code: str,
    message: str,
    file_info: Dict[str, Any],
) -> str:
    """Format structured error JSON response within budget."""
    payload = {
        "status": "error",
        "error_code": error_code,
        "message": message,
        "file": file_info,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def enforce_budget(
    file_info: Dict[str, Any],
    columns: List[Dict[str, Any]],
    include_stats: bool = True,
    max_chars: int = 2000,
) -> str:
    """Serialize metadata response deterministically bounded to <= max_chars.

    Follows two-pass truncation:
    1. Drop stats objects on columns.
    2. Sequentially prune trailing columns until serialized JSON <= max_chars.
    """
    total_cols = len(columns)

    # Prepare base columns with sanitized stats
    processed_cols: List[Dict[str, Any]] = []
    for col in columns:
        c = dict(col)
        if not include_stats or c.get("stats") is None:
            c["stats"] = None
        else:
            c["stats"] = sanitize_stats(
                c["stats"],
                logical_type=str(c.get("logical_type", "")),
                physical_type=str(c.get("physical_type", "")),
            )
        processed_cols.append(c)

    def _serialize(cols: List[Dict[str, Any]], trunc: Dict[str, Any]) -> str:
        data = {
            "status": "ok",
            "file": file_info,
            "columns": cols,
            "truncation": trunc,
        }
        return json.dumps(data, separators=(",", ":"), sort_keys=True)

    # Attempt 0: All columns with sanitized stats
    trunc_full = {
        "truncated": False,
        "total_columns": total_cols,
        "shown_columns": total_cols,
    }
    candidate = _serialize(processed_cols, trunc_full)
    if len(candidate) <= max_chars:
        return candidate

    # Pass 1: Drop stats across all columns
    cols_no_stats = [dict(c, stats=None) for c in processed_cols]
    candidate = _serialize(cols_no_stats, trunc_full)
    if len(candidate) <= max_chars:
        return candidate

    # Pass 2: Truncate columns sequentially in schema order
    # Binary search for maximum number of columns that fit
    low = 0
    high = total_cols
    best_candidate = ""

    while low <= high:
        mid = (low + high) // 2
        trunc_info = {
            "truncated": True,
            "total_columns": total_cols,
            "shown_columns": mid,
            "omitted_columns_summary": f"{mid + 1}..{total_cols} omitted due to 600-token cap",
        }
        # Try without stats first for consistency under heavy load
        serialized = _serialize(cols_no_stats[:mid], trunc_info)
        if len(serialized) <= max_chars:
            best_candidate = serialized
            low = mid + 1  # Try fitting more columns
        else:
            high = mid - 1  # Needs to fit fewer columns

    if best_candidate and len(best_candidate) <= max_chars:
        return best_candidate

    # Fallback to 0 columns if even 1 column exceeds budget
    fallback_trunc = {
        "truncated": True,
        "total_columns": total_cols,
        "shown_columns": 0,
        "omitted_columns_summary": f"1..{total_cols} omitted due to 600-token cap",
    }
    return _serialize([], fallback_trunc)
