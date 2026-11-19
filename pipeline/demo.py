"""End-to-end demo: build the Meta DWH from the sample batches and answer the two questions from the task.

The answers are compared with fixtures/expected/, written down before the code was run.
"""

from __future__ import annotations

import json
from dataclasses import asdict

from pipeline.warehouse import REPO_ROOT, Warehouse, utc

FIXTURES = REPO_ROOT / "fixtures"


def load_sample(warehouse: Warehouse) -> list[str]:
    """Create the model, load reference data and the sample delivery batches. Returns the batch ids."""
    warehouse.init()
    warehouse.load_reference_files(FIXTURES / "reference" / "teams.csv", FIXTURES / "reference" / "user_team_history.csv")
    manifest = json.loads((FIXTURES / "events" / "manifest.json").read_text(encoding="utf-8"))
    for batch in manifest:
        events = (FIXTURES / "events" / batch["file"]).read_text(encoding="utf-8").splitlines()
        warehouse.ingest(batch["batch_id"], batch["cluster_id"], batch["received_at"], [e for e in events if e.strip()])
    return warehouse.load_pending()


def run() -> bool:
    """Run the demo against a fresh model. Returns True when every answer matches the expected value."""
    expected = json.loads((FIXTURES / "expected" / "sample_expected.json").read_text(encoding="utf-8"))
    cluster_id, start, end = expected["cluster_id"], expected["period_start"], expected["period_end"]

    warehouse = Warehouse()
    version = warehouse.wait_until_ready()
    print(f"Trino {version}, catalog 'iceberg' (Apache Iceberg)")
    warehouse.drop()
    load_sample(warehouse)
    print("\nLoad log (etl_batch)")
    print(f"    {'batch':<12} {'status':<10} {'messages':>8} {'new queries':>11} {'already loaded':>14}")
    for row in warehouse.query("SELECT batch_id, status, messages, queries_new, queries_already_loaded FROM etl_batch ORDER BY batch_id"):
        print(f"    {row[0]:<12} {row[1]:<10} {row[2]:>8} {row[3]:>11} {row[4]:>14}")

    checks: list[tuple[str, object, object]] = []
    rows = warehouse.query("SELECT count(*) FROM fact_query")[0][0]
    checks.append(("rows in fact_query", rows, expected["fact_query_rows"]))

    print(f"\nQ1. Distinct users who queried a table, period [{start}, {end})")
    for table, want in expected["distinct_users"].items():
        got = warehouse.distinct_users(cluster_id, table, start, end)
        print(f"    {table:<28} {got}")
        checks.append((f"distinct users of {table}", got, want))

    print(f"\nQ2. Resources consumed by a team, period [{start}, {end})")
    print(f"    {'team':<12} {'queries':>7} {'failed':>6} {'cpu_seconds':>11} {'input_bytes':>11} {'max_peak_memory_bytes':>21}")
    total_cpu = 0.0
    for team, want in expected["team_resources"].items():
        got = asdict(warehouse.team_resources(team, start, end))
        total_cpu += got["cpu_seconds"] or 0.0  # NULL for a team without queries
        print(
            f"    {team:<12} {got['query_count']:>7} {got['failed_query_count']:>6} {got['cpu_seconds']:>11} "
            f"{got['physical_input_bytes']:>11} {got['max_query_peak_memory_bytes']:>21}"
        )
        for key, value in want.items():
            checks.append((f"{team}.{key}", got[key], value))

    rows_in_period, fact_cpu = warehouse.query(
        "SELECT count(*), sum(cpu_time_seconds) FROM fact_query WHERE end_time >= ? AND end_time < ?", [utc(start), utc(end)]
    )[0]
    checks.append(("sum of team CPU seconds", total_cpu, expected["total_cpu_seconds_in_period"]))
    checks.append(("CPU seconds of all queries in the period, from fact_query", fact_cpu, expected["total_cpu_seconds_in_period"]))
    print(f"    {'all teams':<12} {'':>7} {'':>6} {total_cpu:>11}   = {fact_cpu}, the CPU seconds of all {rows_in_period} queries "
          "that ended in the period (fact_query)")

    volume_expected = json.loads((FIXTURES / "expected" / "read_volume_expected.json").read_text(encoding="utf-8"))
    print(f"\nQ3. Data read per table, period [{start}, {end})")
    print(f"    {'table':<28} {'queries':>7} {'input_bytes':>11} {'input_rows':>10} {'without_bytes':>13}")
    volume = warehouse.table_read_volume(cluster_id, start, end)
    for row in volume:
        table = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
        print(f"    {table:<28} {row['queries']:>7} {row['physical_input_bytes']:>11} {row['physical_input_rows']:>10} "
              f"{row['queries_without_input_bytes']:>13}")
    total_bytes = sum(row["physical_input_bytes"] or 0 for row in volume)
    query_bytes = warehouse.query(
        "SELECT sum(physical_input_bytes) FROM fact_query WHERE end_time >= ? AND end_time < ?", [utc(start), utc(end)]
    )[0][0]
    print(f"    {'all tables':<28} {'':>7} {total_bytes:>11}   (the queries themselves report {query_bytes})")
    for index, want in enumerate(volume_expected["read_volume"]):
        got = volume[index] if index < len(volume) else None
        checks.append((f"read volume row {index + 1}", got, want))
    checks.append(("number of tables read in the period", len(volume), len(volume_expected["read_volume"])))
    checks.append(("bytes read, all tables", total_bytes, volume_expected["total_physical_input_bytes_in_period"]))

    print("\nTrend: distinct users of lake.sales.orders per day (per-day counts must not be added up)")
    for day, users, queries in warehouse.table_users_daily(cluster_id, "lake.sales.orders", start, end):
        print(f"    {day}  users={users}  queries={queries}")
    print("\nTrend: CPU seconds of team analytics per day (additive)")
    for day, queries, _failed, cpu, *_ in warehouse.team_resources_daily("analytics", start, end):
        print(f"    {day}  cpu_seconds={cpu}  queries={queries}")

    warehouse.check()
    print("\nModel invariants hold (unique keys, no orphan rows, every query attributed to exactly one team)")

    print("\nTable comments, read back from the Iceberg catalog")
    for table, comment in warehouse.table_comments():
        print(f"    {table}: {comment}")

    failures = [(name, got, want) for name, got, want in checks if got != want]
    print(f"\nChecked {len(checks)} values against the answers written down in advance in fixtures/expected/: {len(checks) - len(failures)} match")
    for name, got, want in failures:
        print(f"    MISMATCH {name}: got {got!r}, expected {want!r}")
    return not failures
