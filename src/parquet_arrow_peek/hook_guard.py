"""100% stdlib PreToolUse / BeforeTool guardrail interceptor."""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

COLUMNAR_EXTENSIONS = {".parquet", ".arrow", ".feather", ".ipc"}
TABULAR_EXTENSIONS = {".csv", ".tsv", ".jsonl", ".ndjson"}
FILE_SIZE_THRESHOLD_BYTES = 51200  # 50 KB

TARGET_TOOLS = {"view_file", "read_file", "read", "read_text_file"}


def get_redirect_message(target_path: str) -> str:
    """Deterministic redirect message matching SPEC."""
    return (
        "BLOCKED BY PARQUET-ARROW-PEEK ZERO-EGRESS GUARDRAIL: Direct raw reads of columnar "
        "files (.parquet, .arrow, .feather) and tabular files > 50 KB are prohibited to protect "
        "context budget and prevent data egress. Invoke the 'inspect_schema' MCP tool on "
        f"'{target_path}' to inspect its schema and statistics, then write a local DuckDB or "
        "Polars script to process the data."
    )


def extract_tool_and_path(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Extract tool name and target file path from diverse host payloads."""
    tool_name = None
    args: Dict[str, Any] = {}

    if "tool_call" in payload and isinstance(payload["tool_call"], dict):
        tool_name = payload["tool_call"].get("name")
        args = payload["tool_call"].get("args") or payload["tool_call"].get("arguments") or {}
    elif "toolCall" in payload and isinstance(payload["toolCall"], dict):
        tool_name = payload["toolCall"].get("name")
        args = payload["toolCall"].get("args") or payload["toolCall"].get("arguments") or {}
    elif "tool_input" in payload and isinstance(payload["tool_input"], dict):
        tool_name = payload.get("tool_name") or payload.get("name")
        args = payload["tool_input"]
    elif "name" in payload:
        tool_name = payload.get("name")
        args = payload.get("args") or payload.get("arguments") or payload.get("parameters") or {}

    # Extract target path
    file_path = None
    path_keys = ["path", "Path", "file_path", "filePath", "TargetFile", "target_file", "file", "uri"]
    for k in path_keys:
        if k in args and isinstance(args[k], str):
            file_path = args[k]
            break

    if not file_path:
        for k in path_keys:
            if k in payload and isinstance(payload[k], str):
                file_path = payload[k]
                break

    return tool_name, file_path


def evaluate_interception(tool_name: Optional[str], file_path: Optional[str]) -> Tuple[bool, str]:
    """Evaluate whether to deny the tool execution.

    Returns:
        (should_deny, redirect_reason)
    """
    if not tool_name or not file_path:
        return False, ""

    normalized_tool = tool_name.strip().lower()
    if normalized_tool not in TARGET_TOOLS:
        return False, ""

    _, ext = os.path.splitext(file_path)
    ext_lower = ext.lower()

    # Rule 1: Binary columnar formats are unconditionally DENIED (any size >= 0 bytes)
    if ext_lower in COLUMNAR_EXTENSIONS:
        return True, get_redirect_message(file_path)

    # Rule 2: Tabular formats (.csv, .tsv, .jsonl, .ndjson) > 50 KB (51,200 bytes) are DENIED
    if ext_lower in TABULAR_EXTENSIONS:
        if not os.path.exists(file_path):
            # If missing, allow host tool to fail natively with standard file-not-found error
            return False, ""

        try:
            st_size = os.path.getsize(file_path)
            if st_size > FILE_SIZE_THRESHOLD_BYTES:
                return True, get_redirect_message(file_path)
        except OSError:
            return False, ""

    # Rule 3: All other files or tabular files <= 50 KB are ALLOWED
    return False, ""


def process_hook(payload_str: str) -> Dict[str, Any]:
    """Process incoming JSON payload and return dual-compatible Antigravity & Gemini CLI decision."""
    if not payload_str.strip():
        return {
            "decision": "allow",
            "hookSpecificOutput": {"permissionDecision": "allow"},
        }

    try:
        payload = json.loads(payload_str)
    except Exception:
        return {
            "decision": "allow",
            "hookSpecificOutput": {"permissionDecision": "allow"},
        }

    tool_name, file_path = extract_tool_and_path(payload)
    should_deny, redirect_msg = evaluate_interception(tool_name, file_path)

    if should_deny:
        return {
            "decision": "deny",
            "reason": redirect_msg,
            "hookSpecificOutput": {
                "permissionDecision": "deny",
                "permissionDecisionReason": redirect_msg,
            },
        }

    return {
        "decision": "allow",
        "hookSpecificOutput": {
            "permissionDecision": "allow",
        },
    }


def main() -> None:
    """Execute hook guard from CLI/stdin, always exiting 0."""
    try:
        raw_input = sys.stdin.read()
        decision = process_hook(raw_input)
        sys.stdout.write(json.dumps(decision) + "\n")
        sys.stdout.flush()
    except Exception as exc:
        fallback = {
            "decision": "allow",
            "hookSpecificOutput": {"permissionDecision": "allow"},
        }
        sys.stdout.write(json.dumps(fallback) + "\n")
        sys.stdout.flush()
    finally:
        sys.exit(0)


if __name__ == "__main__":
    main()
