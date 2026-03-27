from __future__ import annotations

import importlib
import sys
from pathlib import Path


def _candidate_roots() -> list[Path]:
    base = Path(__file__).resolve().parent
    return [
        base,
        base / "mlb_abs_bot",
    ]


def _configure_path() -> None:
    for root in _candidate_roots():
        if (root / "abs_bot" / "__init__.py").exists():
            sys.path.insert(0, str(root))
            return
    raise ModuleNotFoundError(
        "Could not find the abs_bot package in the current deploy layout."
    )


def main() -> int:
    _configure_path()
    module = importlib.import_module("abs_bot.main")
    return int(module.main())


if __name__ == "__main__":
    raise SystemExit(main())
