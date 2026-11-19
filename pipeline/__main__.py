"""Command line entry point.

    python -m pipeline all     demo, then the test suite (what `docker compose run --rm runner` does)
    python -m pipeline demo    build the model from the sample batches and answer the two questions
    python -m pipeline test    run the test suite only
    python -m pipeline check   verify the model invariants of the current model
    python -m pipeline rebuild rebuild every derived table from the raw events (historical recalculation)
"""

from __future__ import annotations

import sys

from pipeline import demo


def run_tests() -> bool:
    import pytest

    return pytest.main(["-q", "-p", "no:cacheprovider", "tests"]) == 0


def main(argv: list[str]) -> int:
    command = argv[1] if len(argv) > 1 else "all"
    if command not in ("all", "demo", "test", "check", "rebuild"):
        print(__doc__)
        return 2
    if command in ("check", "rebuild"):
        from pipeline.warehouse import Warehouse

        warehouse = Warehouse()
        warehouse.wait_until_ready()
        if command == "rebuild":
            print("Rebuilt from raw events, batches: " + ", ".join(warehouse.rebuild()))
        warehouse.check()
        print("Model invariants hold")
        return 0
    ok = True
    if command in ("all", "demo"):
        print("=== DEMO ===")
        ok = demo.run() and ok
        print("DEMO " + ("PASSED" if ok else "FAILED"))
    if command in ("all", "test"):
        print("\n=== TESTS ===")
        ok = run_tests() and ok
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
