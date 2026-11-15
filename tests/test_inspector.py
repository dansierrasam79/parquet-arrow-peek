"""Acceptance and unit tests for inspector.py and budget.py."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_arrow_peek.inspector import inspect_schema


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_at_01_parquet_metadata_and_stats():
    """AT-01: Parquet Metadata & Stats.

    valid_metrics.parquet (100,000 rows, 8 columns: int64, float64, bool, timestamp[us],
    utf8, 5% nulls in float64).
    inspect_schema reads footer only, returns exact row_count: 100000, row_count_exact: true,
    physical/logical types, null_pct: 5.0, numeric min/max, and min_len/max_len for utf8.
    Assert: status == 'ok', len(serialized_json) <= 2000 chars, zero raw cell values.
    """
    file_path = str(FIXTURES_DIR / "valid_metrics.parquet")
    raw_output = inspect_schema(file_path, include_stats=True)

    assert len(raw_output) <= 2000, f"Token cap exceeded: {len(raw_output)} chars"
    data = json.loads(raw_output)

    assert data["status"] == "ok"
    assert data["file"]["row_count"] == 100000
    assert data["file"]["row_count_exact"] is True
    assert data["file"]["row_groups"] >= 1
    assert data["file"]["format"] == "parquet"

    cols_by_name = {c["name"]: c for c in data["columns"]}
    assert len(cols_by_name) == 8

    # Check c_float64 null_pct
    assert cols_by_name["c_float64"]["null_pct"] == 5.0
    assert cols_by_name["c_float64"]["logical_type"] == "FLOAT64"
    assert "min" in cols_by_name["c_float64"]["stats"]
    assert "max" in cols_by_name["c_float64"]["stats"]

    # Check c_utf8 sanitization (min_len, max_len instead of raw string values)
    utf8_stats = cols_by_name["c_utf8"]["stats"]
    assert "min_len" in utf8_stats
    assert "max_len" in utf8_stats
    assert "min" not in utf8_stats
    assert "max" not in utf8_stats
    assert "user_000000" not in raw_output

    # Check timestamp logical type
    assert "TIMESTAMP" in cols_by_name["c_timestamp"]["logical_type"]


def test_at_02_arrow_feather_ipc():
    """AT-02: Arrow / Feather IPC.

    events.arrow & events.feather (Arrow IPC File format, 50,000 rows, 5 columns).
    inspect_schema opens IPC footer without loading record batches, extracts exact schema,
    row count, and column types.
    Assert: status == 'ok', file.format in ('arrow', 'feather'), row_count == 50000,
    token count < 600 (len <= 2000 chars).
    """
    for ext in ("arrow", "feather"):
        file_path = str(FIXTURES_DIR / f"events.{ext}")
        raw_output = inspect_schema(file_path)

        assert len(raw_output) <= 2000, f"Output exceeded 2000 chars: {len(raw_output)}"
        data = json.loads(raw_output)

        assert data["status"] == "ok"
        assert data["file"]["format"] in ("arrow", "feather")
        assert data["file"]["row_count"] == 50000
        assert data["file"]["row_count_exact"] is True
        assert len(data["columns"]) == 5

        col_names = [c["name"] for c in data["columns"]]
        assert "event_id" in col_names
        assert "user_id" in col_names
        assert "score" in col_names


def test_at_03_large_csv_bounded_inspection():
    """AT-03: Large CSV Bounded Inspection.

    transactions_large.csv (25 MB, 250,000 rows, 12 columns, mixed numeric/string/nulls).
    inspect_schema reads only first 65,536 bytes, infers column names, types, null percentages,
    and estimated row count (row_count_exact: false).
    Assert: execution time < 100 ms, zero raw rows in output, len <= 2000 chars.
    """
    file_path = str(FIXTURES_DIR / "transactions_large.csv")

    t0 = time.perf_counter()
    raw_output = inspect_schema(file_path)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    assert elapsed_ms < 100, f"Execution took too long: {elapsed_ms:.2f} ms"
    assert len(raw_output) <= 2000, f"Output length exceeded: {len(raw_output)}"

    data = json.loads(raw_output)
    assert data["status"] == "ok"
    assert data["file"]["format"] == "csv"
    assert data["file"]["row_count_exact"] is False
    assert data["file"]["row_count"] > 200000
    assert len(data["columns"]) == 12

    # Verify zero raw row records leaked
    assert "Amazon" not in raw_output
    assert "Walmart" not in raw_output
    assert "ref_1" not in raw_output


def test_at_07_corrupt_footer_resilience():
    """AT-07: Corrupt Footer Resilience.

    corrupt.parquet (valid PAR1 header, truncated/corrupted trailing metadata bytes).
    inspect_schema catches parser exception cleanly without crashing.
    Assert: status == 'error', error_code == 'CORRUPT_FOOTER'.
    """
    file_path = str(FIXTURES_DIR / "corrupt.parquet")
    raw_output = inspect_schema(file_path)

    data = json.loads(raw_output)
    assert data["status"] == "error"
    assert data["error_code"] == "CORRUPT_FOOTER"
    assert "corrupt.parquet" in data["file"]["path"]


def test_at_08_nested_and_wide_schema_cap():
    """AT-08: Nested & Wide Schema Cap.

    wide_nested.parquet (600 columns including Struct and List<Struct> fields).
    inspect_schema flattens nested paths and enforces the < 600 token output cap.
    Assert: len(serialized_json) <= 2000 chars, truncation.truncated == true,
    truncation.total_columns >= 600.
    """
    file_path = str(FIXTURES_DIR / "wide_nested.parquet")
    raw_output = inspect_schema(file_path)

    assert len(raw_output) <= 2000, f"Hard cap exceeded: {len(raw_output)} chars"
    data = json.loads(raw_output)

    assert data["status"] == "ok"
    assert data["truncation"]["truncated"] is True
    assert data["truncation"]["total_columns"] >= 600
    assert data["truncation"]["shown_columns"] < data["truncation"]["total_columns"]
    assert "omitted due to 600-token cap" in data["truncation"]["omitted_columns_summary"]


def test_at_09_zero_egress_pii_redaction():
    """AT-09: Zero-Egress / PII Redaction.

    pii_users.parquet containing email column (min="aaron@corp.com", max="zoe@corp.com").
    inspect_schema redacts string min/max values into byte lengths (min_len, max_len).
    Assert: Substrings "aaron@corp.com" and "zoe@corp.com" do NOT appear anywhere in the output.
    """
    file_path = str(FIXTURES_DIR / "pii_users.parquet")
    raw_output = inspect_schema(file_path)

    assert "aaron@corp.com" not in raw_output
    assert "zoe@corp.com" not in raw_output
    assert "000-11-2222" not in raw_output

    data = json.loads(raw_output)
    email_col = next(c for c in data["columns"] if c["name"] == "email")
    assert email_col["stats"] == {"max_len": 12, "min_len": 14}


def test_empty_file_edge_case(tmp_path):
    """Empty file (0 bytes) returns structured EMPTY_FILE error."""
    empty_f = tmp_path / "empty.parquet"
    empty_f.write_bytes(b"")

    raw_output = inspect_schema(str(empty_f))
    data = json.loads(raw_output)
    assert data["status"] == "error"
    assert data["error_code"] == "EMPTY_FILE"


def test_zero_row_valid_parquet(tmp_path):
    """Zero-row valid Parquet returns full schema with row_count: 0 and stats: null."""
    zero_row_f = tmp_path / "zero_rows.parquet"
    table = pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.string())})
    pq.write_table(table, str(zero_row_f))

    raw_output = inspect_schema(str(zero_row_f))
    data = json.loads(raw_output)
    assert data["status"] == "ok"
    assert data["file"]["row_count"] == 0
    assert len(data["columns"]) == 2
    for c in data["columns"]:
        assert c["null_pct"] is None
        assert c["stats"] is None


def test_missing_file_error():
    """Non-existent file returns structured FILE_NOT_FOUND error."""
    raw_output = inspect_schema("/nonexistent/file/path.parquet")
    data = json.loads(raw_output)
    assert data["status"] == "error"
    assert data["error_code"] == "FILE_NOT_FOUND"


def test_jsonl_inspection(tmp_path):
    """JSONL inspection parses keys, types, and bounded row count."""
    jsonl_f = tmp_path / "data.jsonl"
    lines = [
        json.dumps({"id": 1, "name": "alice", "active": True, "score": 9.5}),
        json.dumps({"id": 2, "name": "bob", "active": False, "score": None}),
    ]
    jsonl_f.write_text("\n".join(lines) + "\n")

    raw_output = inspect_schema(str(jsonl_f))
    data = json.loads(raw_output)
    assert data["status"] == "ok"
    assert data["file"]["format"] == "jsonl"
    assert len(data["columns"]) == 4
    # Zero raw rows present
    assert "alice" not in raw_output
    assert "bob" not in raw_output
