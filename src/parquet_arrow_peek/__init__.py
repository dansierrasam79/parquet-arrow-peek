"""parquet-arrow-peek: Deterministic zero-egress inspection tool for columnar and tabular data."""

__version__ = "0.1.0"

def __getattr__(name: str):
    if name == "inspect_schema":
        from .inspector import inspect_schema
        return inspect_schema
    if name in ("enforce_budget", "sanitize_stats"):
        from . import budget
        return getattr(budget, name)
    if name in ("evaluate_interception", "process_hook"):
        from . import hook_guard
        return getattr(hook_guard, name)
    if name in ("handle_request", "run_server"):
        from . import server
        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "__version__",
    "inspect_schema",
    "enforce_budget",
    "sanitize_stats",
    "process_hook",
    "evaluate_interception",
    "handle_request",
    "run_server",
]
