"""Real QueryCompletedEvent JSON captured from Trino 484 must pass through the reading layer unchanged in meaning.

The expected values below were read off the captured events by hand (fixtures/captured/README.md)."""

from __future__ import annotations

import pytest

from tests.support import bridge_rows, captured_events, load

ORDERS, CUSTOMERS, ORDERS_COPY = "iceberg.demo.orders", "iceberg.demo.customers", "iceberg.demo.orders_copy"
DAY = ("2026-11-17T00:00:00Z", "2026-11-18T00:00:00Z")


@pytest.fixture(scope="module")
def captured(request):
    from tests.conftest import _fresh_warehouse

    wh = _fresh_warehouse(request.module.__name__)
    events = captured_events()
    load(wh, list(events.values()), cluster_id="local")
    yield wh, {label: e["metadata"]["queryId"] for label, e in events.items()}
    wh.drop()


def test_all_captured_events_load(captured):
    wh, ids = captured
    assert wh.query("SELECT count(*) FROM fact_query")[0][0] == len(ids) == 7


def test_typed_values_of_a_real_event(captured):
    wh, ids = captured
    row = wh.query(
        "SELECT user_name, query_state, query_type, cpu_time_seconds, physical_input_bytes, peak_user_memory_bytes, "
        "server_version, resource_group, CAST(create_time AS varchar), CAST(end_time AS varchar), CAST(query_date AS varchar) "
        "FROM fact_query WHERE query_id = ?",
        [ids["k10-join-two-tables"]],
    )[0]
    assert row == [
        "alice", "FINISHED", "SELECT", 0.041, 1006, 131384, "484", "global",
        # Source: 01:11:39.310748153Z and 01:11:39.564564586Z - truncated to microseconds, not rounded.
        "2026-11-17 01:11:39.310748 UTC", "2026-11-17 01:11:39.564564 UTC", "2026-11-17",
    ]


def test_join_of_two_tables(captured):
    wh, ids = captured
    assert bridge_rows(wh, ids["k10-join-two-tables"]) == [(CUSTOMERS, "READ", True), (ORDERS, "READ", True)]


def test_read_through_a_view_reports_the_base_table(captured):
    wh, ids = captured
    assert bridge_rows(wh, ids["k10-via-view"]) == [(ORDERS, "READ", False)]


def test_self_join_gives_one_read_row(captured):
    wh, ids = captured
    assert bridge_rows(wh, ids["k10-self-join"]) == [(ORDERS, "READ", True)]


def test_insert_select_from_the_same_table_is_both_read_and_write(captured):
    # INSERT INTO orders SELECT ... FROM orders: the read must not be lost because the table is also the target.
    wh, ids = captured
    assert bridge_rows(wh, ids["k10-insert-select"]) == [(ORDERS, "READ", True), (ORDERS, "WRITE", True)]


def test_merge_reads_its_target(captured):
    wh, ids = captured
    assert bridge_rows(wh, ids["k10-merge"]) == [
        (ORDERS, "READ", True),
        (ORDERS_COPY, "READ", None),
        (ORDERS_COPY, "WRITE", None),
    ]


def test_failed_query_keeps_its_error_and_its_resources(captured):
    wh, ids = captured
    row = wh.query(
        "SELECT user_name, query_state, error_name, error_type, cpu_time_seconds FROM fact_query WHERE query_id = ?",
        [ids["k10-fail-runtime"]],
    )[0]
    assert row == ["carol", "FAILED", "DIVISION_BY_ZERO", "USER_ERROR", 0.001]


def test_distinct_users_over_real_events(captured):
    wh, _ = captured
    # alice (join, self-join), bob (view), etl (insert-select, merge). carol's only query failed.
    assert wh.distinct_users("local", ORDERS, *DAY) == 3
    assert wh.distinct_users("local", CUSTOMERS, *DAY) == 1
    assert wh.distinct_users("local", ORDERS_COPY, *DAY) == 1


def test_iceberg_metadata_table_counts_as_a_table_of_its_own(captured):
    wh, ids = captured
    assert bridge_rows(wh, ids["k10-metadata-table"]) == [("iceberg.demo.orders$snapshots", "READ", True)]
    assert wh.distinct_users("local", "iceberg.demo.orders$snapshots", *DAY) == 1  # erin
    assert "erin" not in {r[0] for r in wh.query(
        "SELECT DISTINCT f.user_name FROM fact_query f JOIN bridge_query_table b ON b.query_id = f.query_id "
        "WHERE b.table_name = 'orders'")}

