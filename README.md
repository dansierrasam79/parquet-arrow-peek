# parquet-arrow-peek

[![Validation](https://img.shields.io/badge/agy%20plugin-validated-brightgreen)](https://antigravity.google)
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://python.org)
[![Zero-Egress](https://img.shields.io/badge/zero--egress-enforced-success)](https://antigravity.google)

A deterministic, dual-layer, zero-egress local inspection tool and guardrail hook built for **Google Antigravity** and **Gemini CLI**.

`parquet-arrow-peek` prevents raw tabular records from flooding the LLM context window by separating metadata discovery from execution:
1. **Guardrail Hook (`PreToolUse` / `BeforeTool`):** Blocks file-reading tools (`view_file`, `read_file`) from reading `.parquet`, `.arrow`, `.feather` or large `.csv`/`.jsonl` files (`> 50 KB`).
2. **Stdio MCP Server (`inspect_schema`):** Reads metadata directly from file footers ($O(1)$ seek) or bounded 64 KB sample windows, enforcing a deterministic output budget of $\le 2,000$ characters ($< 600$ tokens) with zero raw cell value leakage and PII min/max redaction.
3. **Downstream Execution Skill:** Guides the agent to write local DuckDB or Polars transformation scripts that output files or scalar aggregations only.

---

## Architecture

```
Agent Host (Google Antigravity / Gemini CLI)
  │
  ├─► Direct Read Attempt: view_file / read_file("data.parquet" | >50KB csv)
  │     │
  │     ▼
  │   Guardrail Hook (PreToolUse / BeforeTool)
  │   • Intercepts tool invocation
  │   • DENIES raw read and emits deterministic redirect instruction
  │
  ├─► Redirected Tool Call: inspect_schema("data.parquet")
  │     │
  │     ▼
  │   Stdio JSON-RPC 2.0 MCP Server (Zero Network Sockets)
  │   • Reads Parquet/Arrow footer & IPC metadata (O(1) seek)
  │   • Reads bounded 64 KB window for CSV/JSONL
  │   • Redacts string/binary min/max stats to length bounds (min_len, max_len)
  │   • Enforces strict < 600 token (<= 2,000 chars) output cap
  │
  └─► Downstream Execution (guided by SKILL.md)
        • Agent writes Polars / DuckDB script
        • Writes output to local disk or outputs scalar metrics only
```

---

## Installation

### 1. Python Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. Google Antigravity Integration
`parquet-arrow-peek` is structured as a native Antigravity plugin:
- Manifest: `plugin.json`
- MCP Config: `mcp_config.json`
- Hooks: `hooks.json`
- Skill: `skills/parquet-arrow-peek/SKILL.md`

Validate the plugin:
```bash
agy plugin validate --strict
```

### 3. Gemini CLI Integration
The extension manifest is declared in `gemini-extension.json` using `${extensionPath}`:
```bash
gemini extensions install .
```

---

## Usage

### Direct CLI / Module Usage
Inspect a Parquet, Arrow, CSV, or JSONL file schema directly:
```bash
python3 -c "from parquet_arrow_peek import inspect_schema; print(inspect_schema('data.parquet'))"
```

### MCP Tool: `inspect_schema`
**Input Arguments:**
- `path` (string, required): Absolute or workspace-relative file path.
- `include_stats` (boolean, optional, default: `true`): Include footer-level column statistics when available.

**Example Compact Output:**
```json
{
  "columns": [
    {
      "logical_type": "STRING",
      "name": "event_id",
      "null_pct": 0.0,
      "nullable": false,
      "physical_type": "BYTE_ARRAY",
      "stats": {
        "max_len": 36,
        "min_len": 36
      }
    },
    {
      "logical_type": "FLOAT64",
      "name": "amount_usd",
      "null_pct": 1.42,
      "nullable": true,
      "physical_type": "DOUBLE",
      "stats": {
        "distinct_est": 84120,
        "max": 99420.5,
        "min": 0.01
      }
    }
  ],
  "file": {
    "format": "parquet",
    "path": "/workspace/data/events.parquet",
    "row_count": 14500000,
    "row_count_exact": true,
    "row_groups": 12,
    "size_bytes": 1073741824
  },
  "status": "ok",
  "truncation": {
    "shown_columns": 2,
    "total_columns": 2,
    "truncated": false
  }
}
```

---

## Verification & Testing

Run the full acceptance test suite:
```bash
./.venv/bin/pytest -v
```

All 10 acceptance tests (AT-01 through AT-10) are validated:
- **AT-01:** Parquet metadata & stats extraction with string length redaction.
- **AT-02:** Arrow / Feather IPC footer inspection without loading batches.
- **AT-03:** Large CSV bounded 64 KB inspection (< 100 ms execution, zero raw rows).
- **AT-04:** Small CSV (<= 50 KB) hook pass-through.
- **AT-05:** Large CSV (> 50 KB) hook interception and redirect message.
- **AT-06:** Columnar extensions (.parquet, .feather) unconditionally denied.
- **AT-07:** Corrupt footer resilience without MCP server crash.
- **AT-08:** Nested structs & 600-column wide schema truncation cap (<= 2,000 chars).
- **AT-09:** Zero-egress PII string min/max redaction.
- **AT-10:** Plugin and extension manifest validation with `agy plugin validate --strict`.
