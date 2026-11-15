"""Acceptance and unit tests for hook_guard.py."""

from __future__ import annotations

import json
from pathlib import Path

from parquet_arrow_peek.hook_guard import (
    evaluate_interception,
    extract_tool_and_path,
    process_hook,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_at_04_small_csv_hook_allow():
    """AT-04: Small CSV Hook Allow.

    lookup_small.csv (12 KB, <= 50 KB threshold) passed to PreToolUse / BeforeTool hook for read_file.
    Hook inspects st_size (<= 51,200) and permits the read operation.
    Assert: Hook outputs allow with exit code 0.
    """
    file_path = str(FIXTURES_DIR / "lookup_small.csv")

    # Antigravity PreToolUse payload
    ag_payload = json.dumps({
        "toolCall": {
            "name": "read_file",
            "args": {"path": file_path},
        }
    })
    res_ag = process_hook(ag_payload)
    assert res_ag["decision"] == "allow"
    assert res_ag["hookSpecificOutput"]["permissionDecision"] == "allow"

    # Gemini CLI BeforeTool payload
    gemini_payload = json.dumps({
        "tool_call": {
            "name": "read_file",
            "args": {"path": file_path},
        }
    })
    res_gemini = process_hook(gemini_payload)
    assert res_gemini["decision"] == "allow"


def test_at_05_large_csv_hook_interception():
    """AT-05: Large CSV Hook Interception.

    transactions_large.csv (25 MB, > 50 KB threshold) passed to PreToolUse (view_file)
    and BeforeTool (read_file).
    Hook blocks the raw read and emits the deterministic redirect message naming inspect_schema.
    Assert: Hook JSON permissionDecision == 'deny' and decision == 'deny'; reason contains 'inspect_schema'.
    """
    file_path = str(FIXTURES_DIR / "transactions_large.csv")

    # Antigravity view_file
    ag_payload = json.dumps({
        "toolCall": {
            "name": "view_file",
            "args": {"path": file_path},
        }
    })
    res_ag = process_hook(ag_payload)
    assert res_ag["decision"] == "deny"
    assert res_ag["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "inspect_schema" in res_ag["reason"]
    assert "BLOCKED BY PARQUET-ARROW-PEEK ZERO-EGRESS GUARDRAIL" in res_ag["reason"]
    assert file_path in res_ag["reason"]

    # Gemini CLI read_file
    gemini_payload = json.dumps({
        "tool_call": {
            "name": "read_file",
            "args": {"path": file_path},
        }
    })
    res_gemini = process_hook(gemini_payload)
    assert res_gemini["decision"] == "deny"
    assert "inspect_schema" in res_gemini["reason"]


def test_at_06_parquet_arrow_hook_interception():
    """AT-06: Parquet / Arrow Hook Interception.

    tiny.parquet (2 KB, < 50 KB) and data.feather (10 KB) passed to PreToolUse / BeforeTool for view_file.
    Hook unconditionally denies binary columnar extensions regardless of small file size.
    Assert: Hook returns deny for both files with deterministic redirect instruction.
    """
    for fname in ("tiny.parquet", "data.feather"):
        file_path = str(FIXTURES_DIR / fname)

        payload = json.dumps({
            "toolCall": {
                "name": "view_file",
                "args": {"path": file_path},
            }
        })
        res = process_hook(payload)
        assert res["decision"] == "deny"
        assert res["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert "inspect_schema" in res["reason"]
        assert "BLOCKED BY PARQUET-ARROW-PEEK" in res["reason"]
        assert file_path in res["reason"]


def test_non_existent_csv_allowed():
    """Section 3.2: If view_file targets non-existent CSV, hook allows it to fail natively."""
    payload = json.dumps({
        "toolCall": {
            "name": "view_file",
            "args": {"path": "/tmp/non_existent_file.csv"},
        }
    })
    res = process_hook(payload)
    assert res["decision"] == "allow"


def test_non_existent_parquet_still_denied():
    """Columnar extension is unconditionally denied even if file does not exist."""
    payload = json.dumps({
        "toolCall": {
            "name": "view_file",
            "args": {"path": "/tmp/non_existent_file.parquet"},
        }
    })
    res = process_hook(payload)
    assert res["decision"] == "deny"


def test_non_target_tool_allowed():
    """Non-file-reading tools (e.g. bash, write_file) pass through untouched."""
    payload = json.dumps({
        "toolCall": {
            "name": "bash",
            "args": {"command": "ls -la"},
        }
    })
    res = process_hook(payload)
    assert res["decision"] == "allow"


def test_non_target_file_extension_allowed():
    """Non-tabular extensions (.py, .txt, .md) are allowed regardless of size."""
    payload = json.dumps({
        "toolCall": {
            "name": "read_file",
            "args": {"path": "src/parquet_arrow_peek/server.py"},
        }
    })
    res = process_hook(payload)
    assert res["decision"] == "allow"


def test_malformed_and_empty_payloads():
    """Empty or invalid JSON payloads fail open to allow without crashing."""
    assert process_hook("")["decision"] == "allow"
    assert process_hook("{invalid_json")["decision"] == "allow"
    assert process_hook("{}")["decision"] == "allow"
