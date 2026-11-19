"""Builds the sample delivery batches in fixtures/events/.

The events are synthetic, but they follow the shape of real QueryCompletedEvent JSON captured from
Trino 484 (see fixtures/captured/): durations are numbers in seconds, timestamps are ISO-8601 text with
nanoseconds, optional keys without a value are absent. They are synthetic because real queries do not
produce CPU times that can be written down in advance, and the tests need exact expected numbers.
Large fields the model never reads (query plan, operator statistics) are left out.

The generated files are committed; run this script only to regenerate them:

    python -m fixtures.build_sample_events
"""

from __future__ import annotations

import json
from pathlib import Path

EVENTS_DIR = Path(__file__).resolve().parent / "events"
CLUSTER_ID = "demo"

ORDERS = "lake.sales.orders"
CUSTOMERS = "lake.sales.customers"
DAILY_REVENUE = "lake.sales.daily_revenue"
V_ORDERS = "lake.sales.v_orders"
PG_ORDERS = "pg.sales.orders"

CONNECTOR_BY_CATALOG = {"lake": "iceberg", "pg": "postgresql"}


def _timestamp(text: str) -> str:
    """'2026-11-01 10:00:00' -> '2026-11-01T10:00:00.000000000Z' (the precision Trino emits)."""
    if "T" in text:
        return text
    date, clock = text.split(" ")
    return f"{date}T{clock}.000000000Z"


def _identity(table: str) -> tuple[str, str, str]:
    catalog, schema, name = table.split(".", 2)
    return catalog, schema, name


def make_event(
    query_id: str,
    user: str,
    create_time: str,
    end_time: str,
    cpu_seconds: float,
    *,
    state: str = "FINISHED",
    inputs: tuple[str, ...] = (),
    output: str | None = None,
    through_view: dict[str, str] | None = None,
    physical_input_bytes: int = 0,
    peak_memory_bytes: int = 0,
    query_type: str | None = "SELECT",
    update_type: str | None = None,
    error_name: str | None = None,
    sql: str = "SELECT ...",
    referenced: tuple[str, ...] | None = None,
    input_bytes: tuple[int | None, ...] | None = None,
    input_rows: tuple[int | None, ...] | None = None,
) -> dict:
    """One QueryCompletedEvent in the shape Trino 484 emits.

    inputs: tables Trino scanned, one entry per scan (a self-join lists the table twice).
    through_view: {base_table: view} for inputs reached only through a view.
    referenced: tables listed in metadata.tables; defaults to the distinct inputs.
    input_bytes / input_rows: per-input values, one per entry of `inputs` (None = the key is absent).
      By default the query's bytes are split evenly over its inputs, with bytes // 100 rows each.
    """
    through_view = through_view or {}
    bytes_per_input = physical_input_bytes // len(inputs) if inputs else 0

    io_inputs = []
    for position, table in enumerate(inputs):
        catalog, schema, name = _identity(table)
        entry = {
            "catalogName": catalog,
            "catalogVersion": "default",
            "schema": schema,
            "table": name,
            "columns": [],
            "connectorName": CONNECTOR_BY_CATALOG.get(catalog, "iceberg"),
        }
        entry_bytes = bytes_per_input if input_bytes is None else input_bytes[position]
        entry_rows = bytes_per_input // 100 if input_rows is None else input_rows[position]
        if entry_bytes is not None:
            entry["physicalInputBytes"] = entry_bytes
        if entry_rows is not None:
            entry["physicalInputRows"] = entry_rows
        io_inputs.append(entry)

    tables = []
    for table in dict.fromkeys(inputs if referenced is None else referenced):
        catalog, schema, name = _identity(table)
        if table in through_view:
            view_catalog, view_schema, view_name = _identity(through_view[table])
            tables.append(
                {
                    "catalog": catalog,
                    "schema": schema,
                    "table": name,
                    "authorization": user,
                    "filters": [],
                    "columns": [],
                    "directlyReferenced": False,
                    "referenceChain": [
                        {"@type": "view", "catalogName": view_catalog, "schemaName": view_schema, "viewName": view_name}
                    ],
                }
            )
            tables.append(
                {
                    "catalog": view_catalog,
                    "schema": view_schema,
                    "table": view_name,
                    "authorization": user,
                    "filters": [],
                    "columns": [],
                    "directlyReferenced": True,
                    "viewText": f"SELECT * FROM {table}",
                    "referenceChain": [],
                }
            )
        else:
            tables.append(
                {
                    "catalog": catalog,
                    "schema": schema,
                    "table": name,
                    "authorization": user,
                    "filters": [],
                    "columns": [],
                    "directlyReferenced": True,
                    "referenceChain": [],
                }
            )

    context = {
        "user": user,
        "originalUser": user,
        "principal": user,
        "enabledRoles": [],
        "groups": [],
        "source": "trino-cli",
        "clientTags": [],
        "resourceGroupId": ["global"],
        "sessionProperties": {},
        "serverVersion": "484",
        "environment": "production",
        "timezone": "UTC",
        "retryPolicy": "NONE",
    }
    if query_type is not None:
        context["queryType"] = query_type

    metadata = {"queryId": query_id, "query": sql, "queryState": state, "tables": tables, "routines": []}
    if update_type is not None:
        metadata["updateType"] = update_type

    failed = state == "FAILED"
    event = {
        "metadata": metadata,
        "statistics": {
            "cpuTime": cpu_seconds,
            "failedCpuTime": cpu_seconds if failed else 0.0,
            "wallTime": round(cpu_seconds * 1.5, 3),
            "queuedTime": 0.001,
            "peakUserMemoryBytes": peak_memory_bytes,
            "physicalInputBytes": physical_input_bytes,
            "physicalInputRows": physical_input_bytes // 100,
            "writtenBytes": 0 if output is None else 1024,
            "complete": True,
        },
        "context": context,
        "ioMetadata": {"inputs": io_inputs},
        "warnings": [],
        "createTime": _timestamp(create_time),
        "executionStartTime": _timestamp(create_time),
        "endTime": _timestamp(end_time),
    }
    if output is not None:
        catalog, schema, name = _identity(output)
        event["ioMetadata"]["output"] = {"catalogName": catalog, "catalogVersion": "default", "schema": schema, "table": name}
    if failed:
        event["failureInfo"] = {
            "errorCode": {"code": 8, "name": error_name or "GENERIC_INTERNAL_ERROR", "type": "USER_ERROR", "fatal": True},
            "failureType": "io.trino.spi.TrinoException",
            "failureMessage": "Division by zero",
        }
    return event


def sample_queries() -> dict[str, dict]:
    """The 14 sample queries. Each one exists to exercise one rule; see the comment on each."""
    q = {}
    # Two tables in one query: its 10 CPU seconds must count once for the team.
    q["q01"] = make_event("20261101_100000_00001_demo1", "alice", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10,
                          inputs=(ORDERS, CUSTOMERS), physical_input_bytes=1000, peak_memory_bytes=500)
    # Same user, same table, another day: still one distinct user of ORDERS over the period.
    q["q02"] = make_event("20261102_090000_00002_demo1", "alice", "2026-11-02 09:00:00", "2026-11-02 09:00:05", 4,
                          inputs=(ORDERS,), physical_input_bytes=400, peak_memory_bytes=300)
    # Read through a view: the base table is what Trino reports as input.
    q["q03"] = make_event("20261102_110000_00003_demo1", "bob", "2026-11-02 11:00:00", "2026-11-02 11:00:05", 6,
                          inputs=(ORDERS,), through_view={ORDERS: V_ORDERS}, physical_input_bytes=600, peak_memory_bytes=900)
    # Submitted after bob moved from analytics to platform (2026-11-03 00:00).
    q["q04"] = make_event("20261103_120000_00004_demo1", "bob", "2026-11-03 12:00:00", "2026-11-03 12:00:05", 8,
                          inputs=(CUSTOMERS,), physical_input_bytes=800, peak_memory_bytes=200)
    # Runs across midnight AND across bob's team change: team by submit time, period by end time.
    q["q05"] = make_event("20261102_235000_00005_demo1", "bob", "2026-11-02 23:50:00", "2026-11-03 00:20:00", 3,
                          inputs=(ORDERS,), physical_input_bytes=300, peak_memory_bytes=100)
    # A user with no team membership.
    q["q06"] = make_event("20261103_150000_00006_demo1", "carol", "2026-11-03 15:00:00", "2026-11-03 15:00:05", 5,
                          inputs=(ORDERS,), physical_input_bytes=500, peak_memory_bytes=700)
    # Failed at run time: consumes team resources, but dave did not "query" ORDERS.
    q["q07"] = make_event("20261103_160000_00007_demo1", "dave", "2026-11-03 16:00:00", "2026-11-03 16:00:01", 2,
                          state="FAILED", error_name="DIVISION_BY_ZERO", inputs=(ORDERS,), peak_memory_bytes=50)
    # MERGE by a service account: Trino reports the target both as input and as output.
    q["q08"] = make_event("20261102_020000_00008_demo1", "svc_etl", "2026-11-02 02:00:00", "2026-11-02 02:01:00", 20,
                          inputs=(ORDERS, DAILY_REVENUE), referenced=(ORDERS,), output=DAILY_REVENUE,
                          physical_input_bytes=2000, peak_memory_bytes=4000, query_type="MERGE", update_type="MERGE")
    # Self-join: Trino lists the same input twice.
    q["q09"] = make_event("20261104_100000_00009_demo1", "alice", "2026-11-04 10:00:00", "2026-11-04 10:00:01", 1,
                          inputs=(ORDERS, ORDERS), physical_input_bytes=200, peak_memory_bytes=60)
    # No table at all: still the team's consumption.
    q["q10"] = make_event("20261104_110000_00010_demo1", "alice", "2026-11-04 11:00:00", "2026-11-04 11:00:01", 0.5,
                          peak_memory_bytes=10)
    # Ends exactly at the upper bound of the sample period: outside [from, to).
    q["q11"] = make_event("20261104_235900_00011_demo1", "alice", "2026-11-04 23:59:00", "2026-11-05 00:00:00", 100,
                          inputs=(ORDERS,), physical_input_bytes=9000, peak_memory_bytes=9000)
    # Ends exactly at the lower bound: inside. Delivered late, in the last batch.
    q["q12"] = make_event("20261031_235950_00012_demo1", "alice", "2026-10-31 23:59:50", "2026-11-01 00:00:00", 7,
                          inputs=(CUSTOMERS,), physical_input_bytes=700, peak_memory_bytes=250)
    # Entirely before the sample period.
    q["q13"] = make_event("20261030_100000_00013_demo1", "frank", "2026-10-30 10:00:00", "2026-10-30 10:00:05", 50,
                          inputs=(ORDERS,), physical_input_bytes=5000, peak_memory_bytes=800)
    # Two data sources in one query; pg.sales.orders shares its name with lake.sales.orders but is another table.
    q["q14"] = make_event("20261104_140000_00014_demo1", "dave", "2026-11-04 14:00:00", "2026-11-04 14:00:05", 9,
                          inputs=(PG_ORDERS, CUSTOMERS), physical_input_bytes=900, peak_memory_bytes=1000)
    return q


# Delivery batches: (batch_id, received_at, query keys). Batches 2 and 3 redeliver an event already sent,
# and batch 3 carries q12, which finished days earlier.
BATCHES = [
    ("batch-001", "2026-11-02T12:00:00Z", ["q13", "q01", "q02", "q08"]),
    ("batch-002", "2026-11-03T18:00:00Z", ["q03", "q04", "q05", "q06", "q07", "q02"]),
    ("batch-003", "2026-11-05T01:00:00Z", ["q09", "q10", "q11", "q14", "q12", "q01"]),
]


def main() -> None:
    queries = sample_queries()
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    for batch_id, received_at, keys in BATCHES:
        file_name = f"{batch_id}.jsonl"
        with open(EVENTS_DIR / file_name, "w", encoding="utf-8", newline="\n") as f:
            for key in keys:
                f.write(json.dumps(queries[key], separators=(",", ":")) + "\n")
        manifest.append({"batch_id": batch_id, "cluster_id": CLUSTER_ID, "received_at": received_at, "file": file_name})
    with open(EVENTS_DIR / "manifest.json", "w", encoding="utf-8", newline="\n") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    print(f"wrote {len(manifest)} batches, {sum(len(keys) for _, _, keys in BATCHES)} messages")


if __name__ == "__main__":
    main()
