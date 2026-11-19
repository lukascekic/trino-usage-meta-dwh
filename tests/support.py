"""Small helpers shared by the tests."""

from __future__ import annotations

import json

from fixtures.build_sample_events import make_event  # noqa: F401  (re-exported for tests)
from pipeline.warehouse import REPO_ROOT, Warehouse

CAPTURED_FILE = REPO_ROOT / "fixtures" / "captured" / "trino484_query_completed.jsonl"

TEAMS = [("analytics", "Product Analytics"), ("platform", "Data Platform"), ("UNASSIGNED", "Unassigned")]


def load(warehouse: Warehouse, events: list, *, batch_id: str = "b1", cluster_id: str = "demo",
         memberships: list | None = None) -> None:
    """Land events (dicts or raw JSON text) as one delivery batch and load it."""
    warehouse.load_reference(TEAMS, memberships or [])
    texts = [e if isinstance(e, str) else json.dumps(e) for e in events]
    warehouse.ingest(batch_id, cluster_id, "2026-11-10T00:00:00Z", texts)
    warehouse.load_batch(batch_id)


def captured_events() -> dict[str, dict]:
    """Real QueryCompletedEvent JSON captured from Trino 484, keyed by scenario label."""
    events = {}
    for line in CAPTURED_FILE.read_text(encoding="utf-8").splitlines():
        if line.strip():
            event = json.loads(line)
            events[event["context"]["source"]] = event
    return events


def bridge_rows(warehouse: Warehouse, query_id: str) -> list[tuple]:
    """Bridge rows of one query as a sorted LIST, so that a duplicated row is visible."""
    rows = warehouse.query(
        "SELECT catalog_name || '.' || schema_name || '.' || table_name, access_type, is_directly_referenced "
        "FROM bridge_query_table WHERE query_id = ? ORDER BY 1, 2",
        [query_id],
    )
    return [tuple(r) for r in rows]


FACT_COLUMNS = (
    "cluster_id, query_id, user_name, principal, original_user, source, client_tags, resource_group, query_type, "
    "update_type, query_state, error_name, error_type, create_time, execution_start_time, end_time, query_date, "
    "cpu_time_seconds, failed_cpu_time_seconds, wall_time_seconds, queued_time_seconds, physical_input_bytes, "
    "physical_input_rows, peak_user_memory_bytes, written_bytes, server_version, content_hash"
)


def snapshot(warehouse: Warehouse) -> dict[str, list]:
    """Complete content of the model, as ordered LISTS of rows.

    Only the audit columns loaded_batch_id / loaded_at are left out: they depend on load order by design.
    """
    return {
        "fact_query": warehouse.query(f"SELECT {FACT_COLUMNS} FROM fact_query ORDER BY cluster_id, query_id"),
        "bridge_query_table": warehouse.query("SELECT * FROM bridge_query_table ORDER BY 1, 2, 3, 4, 5, 6"),
        "dim_table": warehouse.query("SELECT * FROM dim_table ORDER BY 1, 2, 3, 4"),
    }


def batch_status(warehouse: Warehouse) -> dict[str, str]:
    return {row[0]: row[1] for row in warehouse.query("SELECT batch_id, status FROM etl_batch")}


def sample_batches() -> list[tuple[str, str, str, list[str]]]:
    """The sample delivery batches: (batch_id, cluster_id, received_at, raw event texts)."""
    events_dir = REPO_ROOT / "fixtures" / "events"
    batches = []
    for batch in json.loads((events_dir / "manifest.json").read_text(encoding="utf-8")):
        lines = [l for l in (events_dir / batch["file"]).read_text(encoding="utf-8").splitlines() if l.strip()]
        batches.append((batch["batch_id"], batch["cluster_id"], batch["received_at"], lines))
    return batches


def prepare_sample(warehouse: Warehouse, order: tuple[int, ...] = (0, 1, 2)) -> list[str]:
    """Load reference data and land the sample batches in the raw table without loading them.

    Returns the batch ids in the requested order.
    """
    reference = REPO_ROOT / "fixtures" / "reference"
    warehouse.load_reference_files(reference / "teams.csv", reference / "user_team_history.csv")
    batches = sample_batches()
    for batch_id, cluster_id, received_at, events in batches:
        warehouse.ingest(batch_id, cluster_id, received_at, events)
    return [batches[i][0] for i in order]


def bridge_measures(warehouse: Warehouse, query_id: str) -> list[tuple]:
    """(table, access_type, physical_input_bytes, physical_input_rows) of one query, as a sorted list."""
    rows = warehouse.query(
        "SELECT catalog_name || '.' || schema_name || '.' || table_name, access_type, physical_input_bytes, "
        "physical_input_rows FROM bridge_query_table WHERE query_id = ? ORDER BY 1, 2",
        [query_id],
    )
    return [tuple(r) for r in rows]
