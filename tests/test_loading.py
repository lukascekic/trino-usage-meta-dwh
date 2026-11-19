"""How the model is updated: repeated, reordered, interrupted and conflicting deliveries.

Every comparison is on the complete content of the model (tests.support.snapshot), not on a few totals."""

from __future__ import annotations

import itertools
import json

import pytest

from pipeline.warehouse import BatchRejected, InjectedFailure, InvariantViolated
from tests.conftest import _fresh_warehouse
from tests.support import batch_status, load, make_event, prepare_sample, sample_batches, snapshot

X, Z = "lake.sales.x", "lake.sales.z"
MEMBERSHIP = [("u", "analytics", "2026-01-01T00:00:00Z", None)]
START, END = "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z"


@pytest.fixture(scope="module")
def baseline(sample):
    """Model content after the three sample batches were loaded once, in delivery order."""
    return snapshot(sample)


def test_baseline_is_not_empty(baseline):
    # 14 queries; 17 relationships (q01 and q14 have two tables, q08 has three rows, q10 has none); 4 tables.
    assert (len(baseline["fact_query"]), len(baseline["bridge_query_table"]), len(baseline["dim_table"])) == (14, 17, 4)


def test_load_log_and_counters(sample):
    rows = sample.query("SELECT batch_id, status, messages, queries_new, queries_already_loaded, error FROM etl_batch ORDER BY 1")
    assert rows == [
        ["batch-001", "SUCCEEDED", 4, 4, 0, None],
        ["batch-002", "SUCCEEDED", 6, 5, 1, None],  # q02 redelivered
        ["batch-003", "SUCCEEDED", 6, 5, 1, None],  # q01 redelivered
    ]
    assert sample.pending_batches() == []


def test_loading_every_batch_again_changes_nothing(warehouse, baseline):
    for batch_id in prepare_sample(warehouse):
        warehouse.load_batch(batch_id)
    for batch_id in ("batch-003", "batch-001", "batch-002", "batch-001"):
        warehouse.load_batch(batch_id)
    assert snapshot(warehouse) == baseline


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))), ids=lambda o: "".join(str(i + 1) for i in o))
def test_any_order_of_batches_gives_the_same_model(warehouse, baseline, order):
    for batch_id in prepare_sample(warehouse, order):
        warehouse.load_batch(batch_id)
    assert snapshot(warehouse) == baseline
    assert set(batch_status(warehouse).values()) == {"SUCCEEDED"}


def test_one_big_batch_equals_three_small_ones(warehouse, baseline):
    everything = [event for _, _, _, events in sample_batches() for event in events]
    prepare_sample(warehouse)  # reference data; the three batches stay unloaded
    warehouse.ingest("all-at-once", "demo", "2026-11-06T00:00:00Z", everything)
    warehouse.load_batch("all-at-once")
    assert snapshot(warehouse) == baseline


@pytest.mark.parametrize("crash_after", ["fact", "bridge"])
@pytest.mark.parametrize("crash_in", ["batch-001", "batch-002", "batch-003"])
def test_interrupted_load_recovers_on_the_next_run(warehouse, baseline, crash_in, crash_after):
    prepare_sample(warehouse)
    for batch_id in warehouse.pending_batches():
        if batch_id == crash_in:
            with pytest.raises(InjectedFailure):
                warehouse.load_batch(batch_id, stop_after=crash_after)
            break
        warehouse.load_batch(batch_id)
    # The interrupted batch is not complete and is first in line again.
    assert batch_status(warehouse)[crash_in] == "STARTED"
    assert warehouse.pending_batches()[0] == crash_in
    assert snapshot(warehouse) != baseline

    warehouse.load_pending()
    assert snapshot(warehouse) == baseline
    assert set(batch_status(warehouse).values()) == {"SUCCEEDED"}
    assert warehouse.pending_batches() == []


def test_counters_after_recovery_do_not_call_the_batch_s_own_rows_redeliveries(warehouse):
    prepare_sample(warehouse)
    warehouse.load_batch("batch-001")
    with pytest.raises(InjectedFailure):
        warehouse.load_batch("batch-002", stop_after="fact")  # the second attempt finds its own rows loaded
    warehouse.load_pending()
    rows = warehouse.query("SELECT batch_id, queries_new, queries_already_loaded FROM etl_batch ORDER BY 1")
    assert rows == [["batch-001", 4, 0], ["batch-002", 5, 1], ["batch-003", 5, 1]]


def test_team_without_queries_has_zero_queries_and_null_sums(sample):
    team = sample.team_resources("nobody", START, END)
    assert (team.query_count, team.cpu_seconds, team.physical_input_bytes, team.max_query_peak_memory_bytes) == (0, None, None, None)


def test_rebuild_from_a_fully_loaded_model_gives_the_same_model(warehouse, baseline):
    prepare_sample(warehouse)
    warehouse.load_pending()
    assert set(batch_status(warehouse).values()) == {"SUCCEEDED"}
    reference_before = warehouse.query("SELECT * FROM user_team_history ORDER BY 1, 3")
    raw_before = warehouse.query(f"SELECT count(*) FROM {warehouse.raw_schema}.query_events")[0][0]

    rebuilt = warehouse.rebuild()

    assert rebuilt == ["batch-001", "batch-002", "batch-003"]  # every batch was processed again
    assert snapshot(warehouse) == baseline
    assert warehouse.query("SELECT * FROM user_team_history ORDER BY 1, 3") == reference_before
    assert warehouse.query(f"SELECT count(*) FROM {warehouse.raw_schema}.query_events")[0][0] == raw_before == 16


def test_late_event_lands_in_its_own_day(sample):
    # q12 ended on 2026-11-01 but was delivered in the last batch, received 2026-11-05.
    row = sample.query("SELECT CAST(query_date AS varchar), loaded_batch_id FROM fact_query WHERE query_id LIKE '%_00012_%'")[0]
    assert row == ["2026-11-01", "batch-003"]


# ---- what counts as "the same event" ---------------------------------------------------------------------

def _first_load(warehouse):
    event = make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X,), physical_input_bytes=100)
    load(warehouse, [event], batch_id="b1", memberships=MEMBERSHIP)
    return event, snapshot(warehouse)


def _deliver(warehouse, batch_id, event):
    text = event if isinstance(event, str) else json.dumps(event)
    warehouse.ingest(batch_id, "demo", "2026-11-11T00:00:00Z", [text])
    warehouse.load_batch(batch_id)


def test_same_event_in_a_later_batch_is_ignored(warehouse):
    event, before = _first_load(warehouse)
    _deliver(warehouse, "b2", event)
    assert snapshot(warehouse) == before
    assert batch_status(warehouse) == {"b1": "SUCCEEDED", "b2": "SUCCEEDED"}
    assert warehouse.query("SELECT queries_new, queries_already_loaded FROM etl_batch WHERE batch_id = 'b2'") == [[0, 1]]


def test_same_event_serialized_differently_is_the_same_event(warehouse):
    event, before = _first_load(warehouse)
    reordered = json.dumps(event, sort_keys=True, indent=2)
    assert reordered != json.dumps(event)
    _deliver(warehouse, "b2", reordered)
    assert snapshot(warehouse) == before
    assert batch_status(warehouse)["b2"] == "SUCCEEDED"


def test_extra_fields_the_model_does_not_read_do_not_make_a_conflict(warehouse):
    event, before = _first_load(warehouse)
    with_plan = json.loads(json.dumps(event))
    with_plan["metadata"]["plan"] = "Fragment 0 [SINGLE] ..."
    _deliver(warehouse, "b2", with_plan)
    assert snapshot(warehouse) == before


@pytest.mark.parametrize("change", ["measure", "input_table", "extra_input_table", "state", "user"])
def test_conflicting_version_rejects_the_batch_and_changes_nothing(warehouse, change):
    event, before = _first_load(warehouse)
    other = json.loads(json.dumps(event))
    if change == "measure":
        other["statistics"]["cpuTime"] = 11
    elif change == "input_table":
        other["ioMetadata"]["inputs"][0]["table"] = "z"
        other["metadata"]["tables"][0]["table"] = "z"
    elif change == "extra_input_table":
        other["ioMetadata"]["inputs"].append({"catalogName": "lake", "schema": "sales", "table": "z"})
    elif change == "state":
        other["metadata"]["queryState"] = "FAILED"
    elif change == "user":
        other["context"]["user"] = "someone_else"

    with pytest.raises(BatchRejected) as rejected:
        _deliver(warehouse, "b2", other)

    assert rejected.value.problems == [("b2/00001", "conflicts with the already loaded version of query demo/q1")]
    assert snapshot(warehouse) == before  # in particular: no table was added to the bridge from the conflicting message
    status, error = warehouse.query("SELECT status, error FROM etl_batch WHERE batch_id = 'b2'")[0]
    assert status == "FAILED" and "demo/q1" in error
    assert warehouse.pending_batches() == ["b2"]


def test_conflicting_versions_inside_one_batch_reject_the_batch(warehouse):
    first = make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X,))
    second = make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X, Z))
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [first, second], memberships=MEMBERSHIP)
    assert rejected.value.problems == [
        ("b1/00001", "conflicts with another message of this batch for query demo/q1"),
        ("b1/00002", "conflicts with another message of this batch for query demo/q1"),
    ]
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 0
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0


def test_rejected_batch_blocks_the_batches_behind_it(warehouse):
    prepare_sample(warehouse)
    warehouse.ingest("batch-000", "demo", "2026-11-01T00:00:00Z", ["not json"])
    with pytest.raises(BatchRejected):
        warehouse.load_pending()
    assert batch_status(warehouse) == {"batch-000": "FAILED"}
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 0
    assert warehouse.pending_batches() == ["batch-000", "batch-001", "batch-002", "batch-003"]


def test_unknown_batch_id_is_rejected_instead_of_succeeding_with_nothing(warehouse):
    prepare_sample(warehouse)
    with pytest.raises(BatchRejected) as rejected:
        warehouse.load_batch("batch-0O1")  # a typo
    assert rejected.value.problems == [("-", "the raw table has no messages for this batch")]
    assert batch_status(warehouse) == {"batch-0O1": "FAILED"}
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 0


# ---- invariants and trends ----------------------------------------------------------------------------------

def test_invariant_check_passes_on_the_sample_and_detects_a_duplicate(warehouse):
    prepare_sample(warehouse)
    warehouse.load_pending()
    warehouse.check()
    warehouse.query("INSERT INTO fact_query SELECT * FROM fact_query WHERE query_id LIKE '%_00001_%'")
    with pytest.raises(InvariantViolated) as violated:
        warehouse.check()
    assert violated.value.problems[0].startswith("fact_query: duplicate key demo/20261101_100000_00001_demo1")


def test_daily_trends(sample):
    users = sample.table_users_daily("demo", "lake.sales.orders", START, END)
    assert [[str(day), n, q] for day, n, q in users] == [
        ["2026-11-01", 1, 1], ["2026-11-02", 3, 3], ["2026-11-03", 2, 2], ["2026-11-04", 1, 1],
    ]
    team = sample.team_resources_daily("analytics", START, END)
    assert [[str(r[0]), r[1], r[3]] for r in team] == [
        ["2026-11-01", 2, 17.0], ["2026-11-02", 2, 10.0], ["2026-11-03", 2, 5.0], ["2026-11-04", 3, 10.5],
    ]
    # Additive measures: the days add up to the period. Distinct users do not (7 vs 4).
    assert sum(r[3] for r in team) == sample.team_resources("analytics", START, END).cpu_seconds == 42.5
    assert sum(n for _, n, _ in users) == 7 and sample.distinct_users("demo", "lake.sales.orders", START, END) == 4


def test_schema_name_with_a_dot_is_one_table(warehouse):
    event = make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 1, inputs=("lake.x.orders",))
    event["ioMetadata"]["inputs"][0]["schema"] = "sales.eu"
    event["metadata"]["tables"][0]["schema"] = "sales.eu"
    load(warehouse, [event], memberships=MEMBERSHIP)
    period = ("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z")
    table = ("lake", "sales.eu", "orders")
    assert warehouse.distinct_users("demo", table, *period) == 1
    assert [users for _, users, _ in warehouse.table_users_daily("demo", table, *period)] == [1]


def test_every_table_and_column_of_the_model_is_described_in_the_catalog(sample):
    # Grain, key and additivity are documented where a person or an agent browsing the catalog will find them.
    tables = sample.query(
        "SELECT t.table_name FROM information_schema.tables t "
        "LEFT JOIN system.metadata.table_comments c ON c.catalog_name = 'iceberg' AND c.schema_name = t.table_schema AND c.table_name = t.table_name "
        "WHERE t.table_schema = ? AND t.table_type = 'BASE TABLE' AND t.table_name NOT LIKE 'stg%' AND c.comment IS NULL",
        [sample.dwh_schema],
    )
    columns = sample.query(
        "SELECT c.table_name || '.' || c.column_name FROM information_schema.columns c "
        "JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
        "WHERE c.table_schema = ? AND t.table_type = 'BASE TABLE' AND c.table_name NOT LIKE 'stg%' AND c.comment IS NULL",
        [sample.dwh_schema],
    )
    assert tables == [] and columns == []
    assert [name for name, _ in sample.table_comments()] == [
        "bridge_query_table", "dim_table", "dim_team", "etl_batch", "fact_query", "user_team_history",
    ]
