"""Acceptance and unit tests for Stdio MCP server and Manifest validation."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from parquet_arrow_peek.server import handle_request

REPO_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"


def test_mcp_initialize():
    """MCP initialize protocol exchange."""
    req = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "1.0"},
        },
    }
    resp = handle_request(req)
    assert resp["jsonrpc"] == "2.0"
    assert resp["id"] == 1
    assert resp["result"]["serverInfo"]["name"] == "parquet-arrow-peek"
    assert "tools" in resp["result"]["capabilities"]


def test_mcp_tools_list():
    """MCP tools/list exposes inspect_schema."""
    req = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {},
    }
    resp = handle_request(req)
    assert resp["id"] == 2
    tools = resp["result"]["tools"]
    assert len(tools) == 1
    assert tools[0]["name"] == "inspect_schema"
    assert "path" in tools[0]["inputSchema"]["properties"]
    assert "include_stats" in tools[0]["inputSchema"]["properties"]


def test_mcp_tools_call_parquet():
    """MCP tools/call executes inspect_schema successfully."""
    req = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "inspect_schema",
            "arguments": {
                "path": str(FIXTURES_DIR / "valid_metrics.parquet"),
                "include_stats": True,
            },
        },
    }
    resp = handle_request(req)
    assert resp["id"] == 3
    assert resp["result"]["isError"] is False
    content = resp["result"]["content"]
    assert len(content) == 1
    assert content[0]["type"] == "text"

    data = json.loads(content[0]["text"])
    assert data["status"] == "ok"
    assert data["file"]["row_count"] == 100000


def test_mcp_stdio_server_subprocess_and_resilience():
    """AT-07 via Stdio: Server stays alive across corrupt requests and subsequent valid calls."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "parquet_arrow_peek.server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Step 1: Call with corrupt parquet
        req1 = json.dumps({
            "jsonrpc": "2.0",
            "id": 101,
            "method": "tools/call",
            "params": {
                "name": "inspect_schema",
                "arguments": {"path": str(FIXTURES_DIR / "corrupt.parquet")},
            },
        }) + "\n"
        proc.stdin.write(req1)
        proc.stdin.flush()

        line1 = proc.stdout.readline()
        resp1 = json.loads(line1)
        assert resp1["id"] == 101
        assert resp1["result"]["isError"] is True
        data1 = json.loads(resp1["result"]["content"][0]["text"])
        assert data1["status"] == "error"
        assert data1["error_code"] == "CORRUPT_FOOTER"

        # Step 2: Ensure server is still alive for subsequent valid request
        req2 = json.dumps({
            "jsonrpc": "2.0",
            "id": 102,
            "method": "tools/call",
            "params": {
                "name": "inspect_schema",
                "arguments": {"path": str(FIXTURES_DIR / "tiny.parquet")},
            },
        }) + "\n"
        proc.stdin.write(req2)
        proc.stdin.flush()

        line2 = proc.stdout.readline()
        resp2 = json.loads(line2)
        assert resp2["id"] == 102
        assert resp2["result"]["isError"] is False
        data2 = json.loads(resp2["result"]["content"][0]["text"])
        assert data2["status"] == "ok"
        assert data2["file"]["row_count"] == 3

    finally:
        proc.stdin.close()
        proc.terminate()
        proc.wait(timeout=5)


def test_at_10_manifest_and_cli_validation():
    """AT-10: Manifest & CLI Validation.

    Repository root validated against agy plugin validate --strict.
    Both manifests parse cleanly, all referenced scripts/hooks/skills exist and have +x.
    Exit code 0 with zero warnings or missing-path errors.
    """
    # 1. Antigravity plugin validation
    agy_bin = shutil.which("agy")
    assert agy_bin is not None, "agy binary must be in PATH"

    result = subprocess.run(
        [agy_bin, "plugin", "validate", str(REPO_ROOT), "--strict", "--json"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"agy plugin validate failed ({result.returncode}): {result.stderr}\n{result.stdout}"
    val_data = json.loads(result.stdout)
    warnings_or_errors = [f for f in val_data.get("findings", []) if f.get("severity") in ("error", "warning")]
    assert len(warnings_or_errors) == 0, f"Validation findings found: {warnings_or_errors}"

    # 2. Gemini extension manifest check
    gemini_manifest = REPO_ROOT / "gemini-extension.json"
    assert gemini_manifest.exists()
    ext_data = json.loads(gemini_manifest.read_text())
    assert ext_data["name"] == "parquet-arrow-peek"
    assert "mcpServers" in ext_data
    assert "hooks" in ext_data
    assert "${extensionPath}" in str(ext_data["hooks"])

    # 3. Check executable permissions on hook wrapper
    hook_wrapper = REPO_ROOT / "hooks" / "run_guard.sh"
    assert hook_wrapper.exists()
    assert os.access(str(hook_wrapper), os.X_OK), "hooks/run_guard.sh must be executable (+x)"

    # 4. Optional: gemini extensions validate if installed
    gemini_bin = shutil.which("gemini")
    if gemini_bin:
        gemini_res = subprocess.run(
            [gemini_bin, "extensions", "validate", str(REPO_ROOT)],
            capture_output=True,
            text=True,
        )
        assert gemini_res.returncode == 0, f"gemini validate failed: {gemini_res.stderr}"
