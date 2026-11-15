"""Zero-dependency Stdio JSON-RPC 2.0 MCP server exposing inspect_schema."""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional

from .inspector import inspect_schema

SERVER_NAME = "parquet-arrow-peek"
SERVER_VERSION = "0.1.0"
PROTOCOL_VERSION = "2024-11-05"

INSPECT_SCHEMA_TOOL = {
    "name": "inspect_schema",
    "description": (
        "Inspects the schema, column types, and aggregate footer statistics of "
        "local Parquet, Arrow, Feather, CSV, or JSONL files without reading or returning "
        "raw tabular data."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": (
                    "Absolute or workspace-relative path to a local "
                    ".parquet, .arrow, .feather, .csv, or .jsonl file."
                ),
            },
            "include_stats": {
                "type": "boolean",
                "default": True,
                "description": "Include footer-level min/max/null_pct statistics when available.",
            },
        },
        "required": ["path"],
        "additionalProperties": False,
    },
}


def _send_response(resp: Dict[str, Any]) -> None:
    """Send JSON-RPC response on stdout."""
    payload = json.dumps(resp, separators=(",", ":"), ensure_ascii=False)
    sys.stdout.write(payload + "\n")
    sys.stdout.flush()


def handle_request(req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process a single JSON-RPC 2.0 request or notification."""
    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params") or {}

    # Handle notifications (requests with no ID)
    if req_id is None:
        if method == "notifications/initialized":
            return None
        return None

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {},
                },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": SERVER_VERSION,
                },
            },
        }

    elif method == "ping":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {},
        }

    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [INSPECT_SCHEMA_TOOL],
            },
        }

    elif method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments") or {}

        if tool_name != "inspect_schema":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32601,
                    "message": f"Unknown tool: '{tool_name}'",
                },
            }

        file_path = arguments.get("path", "")
        include_stats = arguments.get("include_stats", True)

        inspected_json = inspect_schema(file_path, include_stats=include_stats)
        is_error = False
        try:
            parsed = json.loads(inspected_json)
            if parsed.get("status") == "error":
                is_error = True
        except Exception:
            is_error = True

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": inspected_json,
                    }
                ],
                "isError": is_error,
            },
        }

    else:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {
                "code": -32601,
                "message": f"Method '{method}' not found",
            },
        }


def run_server() -> None:
    """Run MCP server over stdio handling newline and header-framed requests."""
    while True:
        line = sys.stdin.readline()
        if not line:
            break

        line_str = line.strip()
        if not line_str:
            continue

        # Handle optional Content-Length header framing
        if line_str.lower().startswith("content-length:"):
            try:
                length = int(line_str.split(":", 1)[1].strip())
                # Read following empty line
                sep = sys.stdin.readline()
                body = sys.stdin.read(length)
                req = json.loads(body)
            except Exception as e:
                continue
        else:
            try:
                req = json.loads(line_str)
            except Exception:
                continue

        if isinstance(req, dict):
            resp = handle_request(req)
            if resp is not None:
                _send_response(resp)


def main() -> None:
    """Console script entrypoint."""
    if len(sys.argv) > 1 and sys.argv[1] in ("-v", "--version"):
        print(f"{SERVER_NAME} {SERVER_VERSION}")
        sys.exit(0)
    run_server()


if __name__ == "__main__":
    main()
