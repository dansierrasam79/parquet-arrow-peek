# `SPEC.md`: `parquet-arrow-peek`

## Section 1: Architecture, Runtime Choice & Directory Contract

### 1.1 Architectural Overview
`parquet-arrow-peek` is a dual-layer, zero-egress local inspection tool built for **Google Antigravity** and **Gemini CLI**. It prevents raw tabular records from entering the LLM context window by separating metadata discovery from code execution:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Agent Host (Google Antigravity / Gemini CLI)                            │
│                                                                         │
│  1. Raw Read Attempt: view_file / read_file("data.parquet" | >50KB csv) │
│        │                                                                │
│        ▼                                                                │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Guardrail Hook (Antigravity: PreToolUse | Gemini CLI: BeforeTool) │  │
│  │ • Intercepts target path & checks extension / byte size           │  │
│  │ • DENIES raw file read; emits deterministic redirect instruction   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│        │                                                                │
│        ▼                                                                │
│  2. Redirected Call: mcp__parquet-arrow-peek__inspect_schema(path)      │
│        │                                                                │
│        ▼                                                                │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Stdio MCP Server (Local Process — Zero Network Sockets)           │  │
│  │ • Reads Parquet/Arrow footers & IPC headers (O(1) seek)           │  │
│  │ • Samples bounded head/byte-window for CSV/JSONL                  │  │
│  │ • Enforces < 600-token deterministic JSON output budget           │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│        │                                                                │
│        ▼                                                                │
│  3. Downstream Execution (Guided by skills/parquet-arrow-peek/SKILL.md)  │
│     • Agent generates Polars / DuckDB / SQL script referencing `path`   │
│     • Script writes output to disk or prints aggregate metrics only     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Lightweight Runtime Strategy & Trade-Off Analysis

| Criterion | Option A: Standalone Rust Binary (`arrow` + `parquet` crates) | Option B: Python + `duckdb` / `pyarrow` | Option C: Python Stdlib + Minimal `pyarrow` (Selected Hybrid) |
| :--- | :--- | :--- | :--- |
| **Artifact Footprint** | ~14–18 MB single ELF binary (`strip` + LTO) | ~120–250 MB virtual environment (`duckdb` + `pyarrow` wheels) | `< 35 KB` pure Python package (zero required deps for CSV/JSONL; optional `pyarrow` for Parquet/Arrow) |
| **Cold Start Latency** | `~4 ms` | `~180–350 ms` | `~25 ms` (stdlib) / `~90 ms` (with `pyarrow`) |
| **Installation Friction on Linux** | Requires precompiled multi-arch release binaries (`x86_64`, `aarch64`) or a Rust toolchain at install time | High: requires `pip`/`uv` resolution and C++ wheel downloads | **Near-zero**: Runs directly on system Python 3.10+ (`uv run` or `python3`), invoking `pyarrow` if present in the active environment |
| **Parquet/Arrow Footer Read** | Native zero-copy footer seek | Native via C++ `libarrow` | Native `pyarrow.parquet.read_metadata()` (reads footer bytes only, zero row-group materialization) |

**Selected Strategy:** **Dual-Entrypoint Python (`>=3.10`) with Zero-Dependency Fallback** (packaged for `uvx` / `python3 -m`), designed so a drop-in Rust binary (`bin/parquet-arrow-peek`) can replace the backend without altering MCP or hook contracts.
* **Parquet (`.parquet`) & Arrow/Feather (`.arrow`, `.feather`, `.ipc`):** Uses `pyarrow.parquet.ParquetFile` and `pyarrow.ipc.open_file` / `open_stream` to read physical/logical schemas and row-group column statistics (`min`, `max`, `null_count`, `num_rows`) directly from file footers without decoding data pages.
* **CSV (`.csv`, `.tsv`) & JSONL (`.jsonl`, `.ndjson`):** Uses Python standard library (`csv`, `json`, `os.stat`) over a strictly bounded **first 64 KB byte window** (max 250 lines) to infer column names, primitive types (`int64`, `float64`, `bool`, `timestamp`, `utf8`), null rates, and estimated row count (`file_size_bytes / avg_line_bytes`). No external C extensions are required for delimited or newline-delimited files.
* **Shared Hook & MCP Runtime:** The guardrail hook script (`hooks/guardrail.py`) uses **100% Python standard library** (`sys`, `json`, `os`, `pathlib`) so hook execution completes in `< 15 ms` with zero third-party import overhead.

### 1.3 Cross-Platform Repository Layout
This layout satisfies both `agy plugin validate --strict` (Google Antigravity) and `gemini extensions install` (Gemini CLI) from a single repository root:

```text
parquet-arrow-peek/
├── .claude-plugin/                  # Optional compatibility symlink/manifest if needed
├── plugin.json                      # Google Antigravity plugin manifest (strict schema)
├── gemini-extension.json            # Gemini CLI extension manifest
├── pyproject.toml                   # Python package metadata & console entrypoints
├── README.md                        # Installation & verification instructions
├── SPEC.md                          # This architectural specification
├── src/
│   └── parquet_arrow_peek/
│       ├── __init__.py
│       ├── server.py                # Stdio JSON-RPC 2.0 MCP server (zero external SDK required)
│       ├── inspector.py             # Deterministic metadata & stats extraction engine
│       ├── budget.py                # Strict <600-token deterministic truncation & serialization
│       └── hook_guard.py            # Shared PreToolUse / BeforeTool interception logic
├── hooks/
│   ├── hooks.json                   # Antigravity PreToolUse & Gemini CLI BeforeTool bindings
│   └── run_guard.sh                 # POSIX executable wrapper invoking python3 -m parquet_arrow_peek.hook_guard
├── skills/
│   └── parquet-arrow-peek/
│       └── SKILL.md                 # Agent workflow instructions (schema-first, zero raw dumps)
└── tests/
    ├── fixtures/                    # Synthetic Parquet, Arrow, large CSV, and corrupt test files
    ├── test_inspector.py            # Unit tests for metadata extraction & token cap
    ├── test_hook_guard.py           # Unit tests for PreToolUse / BeforeTool interception
    └── test_e2e_mcp.py              # Stdio MCP protocol & acceptance matrix verification
```

**Manifest & Validation Alignment:**
* **`plugin.json` (Antigravity):** Declares `name`, `version`, `description`, `mcpServers` (pointing to `python3 -m parquet_arrow_peek.server` over `stdio`), `hooks` (`./hooks/hooks.json`), and `skills` (`./skills`). Every referenced relative path exists on disk to pass `agy plugin validate --strict`.
* **`gemini-extension.json` (Gemini CLI):** Declares `name`, `version`, `mcpServers` (`command: "python3"`, `args: ["-m", "parquet_arrow_peek.server"]`), `contextFileName` / `skills`, and `hooks` configuration using `${extensionPath}` variables so `gemini extensions install` resolves paths regardless of installation directory.

---

## Section 2: Deterministic Metadata Schema & Hook Interception Contract

### 2.1 MCP Tool Contract (`inspect_schema`)
The Stdio MCP server exposes a single read-only tool: `inspect_schema`.

* **Input Schema:**
  ```json
  {
    "type": "object",
    "properties": {
      "path": {
        "type": "string",
        "description": "Absolute or workspace-relative path to a local .parquet, .arrow, .feather, .csv, or .jsonl file."
      },
      "include_stats": {
        "type": "boolean",
        "default": true,
        "description": "Include footer-level min/max/null_pct statistics when available."
      }
    },
    "required": ["path"],
    "additionalProperties": false
  }
  ```

### 2.2 Deterministic Output JSON Schema (`< 600` Token Hard Cap)
All responses are serialized as compact, deterministic JSON (keys sorted, no unnecessary whitespace, deterministic column ordering matching file schema order). Using a conservative `1 token ≈ 3.5 characters` budget, the serialized JSON output is strictly bounded to **`<= 2,000 characters` (`~570 tokens`)**.

```json
{
  "status": "ok",
  "file": {
    "path": "/workspace/data/events.parquet",
    "format": "parquet",
    "size_bytes": 1073741824,
    "row_count": 14500000,
    "row_count_exact": true,
    "row_groups": 12
  },
  "columns": [
    {
      "name": "event_id",
      "physical_type": "BYTE_ARRAY",
      "logical_type": "STRING",
      "nullable": false,
      "null_pct": 0.0,
      "stats": { "min_len": 36, "max_len": 36 }
    },
    {
      "name": "amount_usd",
      "physical_type": "DOUBLE",
      "logical_type": "FLOAT64",
      "nullable": true,
      "null_pct": 1.42,
      "stats": { "min": 0.01, "max": 99420.5, "distinct_est": 84120 }
    }
  ],
  "truncation": {
    "truncated": false,
    "total_columns": 2,
    "shown_columns": 2
  }
}
```

**Deterministic Token-Cap Enforcement Algorithm (`budget.py`):**
1. **String Stat Sanitization:** String/binary `min` and `max` values from Parquet column chunk statistics are **never** emitted verbatim (preventing PII leakage via lexicographical min/max emails, names, or IDs). Instead, string/binary stats are replaced with structural length metadata: `{"min_len": int, "max_len": int}`. Only numeric (`int`, `float`, `decimal`), boolean, and temporal (`date`, `timestamp`) min/max values are emitted.
2. **Column Budget Pruning:** Columns are serialized sequentially in schema order. Before appending column `i`, the serializer checks cumulative byte/character length plus a reserved 120-character footer buffer.
3. **Graceful Truncation:** If adding column `i` would exceed `2,000 characters`:
   * First pass: Drop `stats` objects on remaining columns, keeping `{name, physical_type, logical_type, null_pct}`.
   * Second pass: Stop appending columns and populate `"truncation": {"truncated": true, "total_columns": N, "shown_columns": K, "omitted_columns_summary": "K+1..N omitted due to 600-token cap"}`.

### 2.3 Guardrail Hook Interception Contract (`PreToolUse` / `BeforeTool`)
The hook intercepts file-reading tool invocations before execution in both host environments:

* **Target Tool Names Matched:** `view_file`, `read_file`, `Read`, `read_text_file` (case-insensitive normalization).
* **Path Extraction:** Inspects `tool_input.path`, `tool_input.file_path`, or `tool_input.TargetFile`.
* **Interception Decision Matrix:**

| File Extension | File Size Condition | Hook Decision | Action / Reason |
| :--- | :--- | :--- | :--- |
| `.parquet`, `.arrow`, `.feather`, `.ipc` | Any size (`>= 0` bytes) | **`DENY`** | Binary columnar format; raw reads corrupt context and waste tokens. |
| `.csv`, `.tsv`, `.jsonl`, `.ndjson` | `> 51,200 bytes` (`> 50 KB`) | **`DENY`** | Exceeds 50 KB tabular threshold; raw read leaks rows into LLM context. |
| `.csv`, `.tsv`, `.jsonl`, `.ndjson` | `<= 51,200 bytes` (`<= 50 KB`) | **`ALLOW`** | Permitted for small lookup/config files. |
| All other extensions | Any size | **`ALLOW`** | Pass-through with zero interference. |

* **Standardized Denial Payload:**
  * **Antigravity (`PreToolUse`):** Exits with JSON on `stdout` (`{"hookSpecificOutput": {"permissionDecision": "deny", "permissionDecisionReason": "..."}}`).
  * **Gemini CLI (`BeforeTool`):** Exits with JSON on `stdout` (`{"decision": "deny", "reason": "..."}`).
  * **Deterministic Redirect Message:**
    > `BLOCKED BY PARQUET-ARROW-PEEK ZERO-EGRESS GUARDRAIL: Direct raw reads of columnar files (.parquet, .arrow, .feather) and tabular files > 50 KB are prohibited to protect context budget and prevent data egress. Invoke the 'inspect_schema' MCP tool on '<resolved_path>' to inspect its schema and statistics, then write a local DuckDB or Polars script to process the data.`

---

## Section 3: Edge Cases & Privacy Boundaries

### 3.1 Privacy & Zero-Egress Boundaries
* **Zero Network Sockets:** `parquet-arrow-peek` binds exclusively to `stdin`/`stdout` (JSON-RPC 2.0). It imports no HTTP/socket libraries (`urllib`, `requests`, `httpx`, `socket`) and performs zero telemetry, update checks, or remote schema registry lookups.
* **Zero Raw-Row Leakage:**
  * For Parquet/Arrow: Data pages and record batches are never decoded. Only file-level schema trees and row-group metadata headers are read.
  * For CSV/JSONL: The 64 KB sample buffer is held strictly in local process memory for type/null inference and discarded prior to response construction. **Zero sample values, row snippets, or example records** are ever included in the JSON output.
  * String/Binary Column Statistics: Because Parquet footers store literal min/max byte arrays per row group (which frequently contain sensitive PII such as `alice@example.com` or `SSN-000-...`), string and binary `min`/`max` stats are stripped and replaced with byte-length bounds (`min_len`, `max_len`).
* **AI Skill Behavioral Guardrail (`SKILL.md`):**
  * Instructs the agent to always call `inspect_schema` before generating data processing code.
  * Prohibits generating scripts that execute `df.head()`, `print(df)`, `SELECT * FROM ... LIMIT N`, or `df.show()` to stdout.
  * Mandates that generated Polars/DuckDB/SQL scripts write transformed outputs to a local file (e.g., `output.parquet`) or print only scalar aggregations (`COUNT`, `SUM`, `AVG`, schema validation checks).

### 3.2 Edge Case Handling

| Edge Case | Handling Strategy | Deterministic Output Behavior |
| :--- | :--- | :--- |
| **Corrupt / Truncated Parquet Footer** | Catch `ArrowInvalid` / `OSError` when reading magic bytes (`PAR1`) or thrift footer length. Never attempt full-file scan fallback. | Returns `{"status": "error", "error_code": "CORRUPT_FOOTER", "message": "Invalid or truncated Parquet/Arrow footer magic bytes.", "file": {...}}` (`< 80` tokens). |
| **Empty File (`0` bytes) or Zero-Row Valid Parquet** | Check `st_size == 0` before parser dispatch. For valid Parquet with `num_rows == 0`, parse schema normally and set `null_pct: null` and `stats: null`. | `0`-byte file returns `error_code: "EMPTY_FILE"`. Zero-row Parquet returns full schema with `"row_count": 0`. |
| **Nested `Struct`, `List`, and `Map` Columns** | Traverse Arrow/Parquet `Schema` tree up to depth `3`. Flatten nested struct paths using dot-notation (`user.address.zip`) and represent lists/maps compactly (`LIST<INT64>`, `MAP<STRING, FLOAT64>`). | Emits flattened path with `"stats": null` (since Parquet footers do not store composite min/max stats) while preserving exact logical types. |
| **Wide Schemas (`500+` Columns)** | Governed by the 2,000-character (`< 600` token) deterministic budgeter (`budget.py`). | Emits as many leading columns as fit within budget, strips per-column stats if needed, and sets `"truncation.truncated": true` with exact `total_columns` and `shown_columns`. |
| **Multi-GB / Multi-TB Files (`> 10 GB`)** | **Parquet/Arrow:** `O(1)` footer seek (`seek(-8, os.SEEK_END)`) reads only the trailing footer bytes (`typically 10 KB – 2 MB`), keeping peak RSS `< 35 MB`. **CSV/JSONL:** Reads strictly `65,536` bytes from offset `0` (`file.read(65536)`), never reading the full file into memory. | Peak memory usage remains bounded under **`50 MB` RSS** regardless of whether the target file is `100 KB` or `100 GB`. |
| **Missing File / Permission Denied / Path Traversal** | Resolve path via `os.path.realpath`. Catch `FileNotFoundError` and `PermissionError`. | Returns structured JSON error (`FILE_NOT_FOUND` or `PERMISSION_DENIED`) without stack traces. |
| **Hook Invoked on Non-Existent Large CSV** | If `view_file` targets `/path/to/missing.csv`, `os.stat()` fails; hook allows `view_file` to fail natively with its standard file-not-found error (unless extension is `.parquet`/`.arrow`/`.feather`, which is always denied). | Prevents masking standard filesystem errors on small/missing text files while strictly blocking binary columnar extensions. |

---

## Section 4: Acceptance Test Matrix

All tests must pass automated execution via `pytest` before release sign-off.

| Test ID | Category | Input Fixture / Stimulus | Expected System Behavior | Pass / Fail Assertion |
| :--- | :--- | :--- | :--- | :--- |
| **AT-01** | **Parquet Metadata & Stats** | `valid_metrics.parquet` (`100,000` rows, 8 columns: `int64`, `float64`, `bool`, `timestamp[us]`, `utf8`, 5% nulls in `float64`) | `inspect_schema` reads footer only, returns exact `row_count: 100000`, `row_count_exact: true`, physical/logical types, `null_pct: 5.0`, numeric `min`/`max`, and `min_len`/`max_len` (no raw strings) for `utf8`. | `status == "ok"`, `len(serialized_json) <= 2000` chars (`< 600` tokens), zero raw cell values present in output. |
| **AT-02** | **Arrow / Feather IPC** | `events.arrow` & `events.feather` (Arrow IPC File format, `50,000` rows, 5 columns) | `inspect_schema` opens IPC footer without loading record batches, extracts exact schema, row count, and column types. | `status == "ok"`, `file.format in ("arrow", "feather")`, `row_count == 50000`, token count `< 600`. |
| **AT-03** | **Large CSV Bounded Inspection** | `transactions_large.csv` (`25 MB`, `250,000` rows, 12 columns, mixed numeric/string/nulls) | `inspect_schema` reads only first `65,536` bytes, infers column names, types, null percentages, and estimated row count (`row_count_exact: false`). | Peak RSS `< 50 MB`, execution time `< 100 ms`, zero raw rows in output, token count `< 600`. |
| **AT-04** | **Small CSV Hook Allow** | `lookup_small.csv` (`12 KB`, `< 50 KB` threshold) passed to `PreToolUse` / `BeforeTool` hook for `read_file` | Hook inspects `st_size` (`12,288 <= 51,200`) and permits the read operation. | Hook outputs `allow` (or empty pass-through) with exit code `0`. |
| **AT-05** | **Large CSV Hook Interception** | `transactions_large.csv` (`25 MB`, `> 50 KB` threshold) passed to `PreToolUse` (`view_file`) and `BeforeTool` (`read_file`) | Hook blocks the raw read and emits the deterministic redirect message naming `inspect_schema` and the target file path. | Hook JSON `permissionDecision == "deny"` (Antigravity) and `decision == "deny"` (Gemini CLI); reason contains `"inspect_schema"`. |
| **AT-06** | **Parquet / Arrow Hook Interception** | `tiny.parquet` (`2 KB`, `< 50 KB`) and `data.feather` (`10 KB`) passed to `PreToolUse` / `BeforeTool` for `view_file` | Hook unconditionally denies binary columnar extensions regardless of small file size. | Hook returns `deny` for both files with deterministic redirect instruction. |
| **AT-07** | **Corrupt Footer Resilience** | `corrupt.parquet` (valid `PAR1` header, truncated/corrupted trailing metadata bytes) | `inspect_schema` catches parser exception cleanly without crashing the Stdio MCP server process. | Returns `status == "error"`, `error_code == "CORRUPT_FOOTER"`, MCP server stays alive for subsequent requests. |
| **AT-08** | **Nested & Wide Schema Cap** | `wide_nested.parquet` (`600` columns including `Struct` and `List<Struct>` fields) | `inspect_schema` flattens nested paths and enforces the `< 600` token output cap via deterministic pruning. | `len(serialized_json) <= 2000` chars (`< 600` tokens), `truncation.truncated == true`, `truncation.total_columns == 600`. |
| **AT-09** | **Zero-Egress / PII Redaction** | `pii_users.parquet` containing `email` column (`min="aaron@corp.com"`, `max="zoe@corp.com"`) | `inspect_schema` redacts string min/max values into byte lengths (`min_len`, `max_len`). | Substrings `"aaron@corp.com"` and `"zoe@corp.com"` do not appear anywhere in the MCP response. |
| **AT-10** | **Manifest & CLI Validation** | Repository root validated against `agy plugin validate --strict` and `gemini extensions validate` | Both manifests parse cleanly, all referenced scripts/hooks/skills exist and have executable permissions (`+x`). | Exit code `0` with zero warnings or missing-path errors. |