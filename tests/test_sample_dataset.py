"""The sample batches must give the answers worked out by hand in fixtures/expected/sample_expected.json."""

from __future__ import annotations

import json
from dataclasses import asdict

import pytest

from pipeline.warehouse import REPO_ROOT
from tests.support import bridge_rows

EXPECTED = json.loads((REPO_ROOT / "fixtures" / "expected" / "sample_expected.json").read_text(encoding="utf-8"))
CLUSTER, START, END = EXPECTED["cluster_id"], EXPECTED["period_start"], EXPECTED["period_end"]
ORDERS = "lake.sales.orders"


def test_one_row_per_query_despite_redelivery(sample):
    # 16 messages were delivered; q01 and q02 arrived twice.
    assert sample.query("SELECT count(*) FROM fact_query")[0][0] == EXPECTED["fact_query_rows"]
    assert sample.query("SELECT count(*) FROM (SELECT cluster_id, query_id FROM fact_query GROUP BY 1, 2 HAVING count(*) > 1)")[0][0] == 0


@pytest.mark.parametrize("table,expected", EXPECTED["distinct_users"].items())
def test_distinct_users_of_table(sample, table, expected):
    assert sample.distinct_users(CLUSTER, table, START, END) == expected


@pytest.mark.parametrize("team,expected", EXPECTED["team_resources"].items())
def test_team_resources(sample, team, expected):
    assert asdict(sample.team_resources(team, START, END)) == expected


def test_distinct_users_are_exactly_these(sample):
    rows = sample.query(
        "SELECT DISTINCT f.user_name FROM fact_query f JOIN bridge_query_table b "
        "ON b.cluster_id = f.cluster_id AND b.query_id = f.query_id "
        "WHERE b.catalog_name = 'lake' AND b.schema_name = 'sales' AND b.table_name = 'orders' AND b.access_type = 'READ' "
        "AND f.query_state = 'FINISHED' AND f.end_time >= TIMESTAMP '2026-11-01 00:00:00 UTC' AND f.end_time < TIMESTAMP '2026-11-05 00:00:00 UTC'"
    )
    # dave only has a FAILED query on the table and a query on pg.sales.orders; frank and q11 are outside the period.
    assert sorted(r[0] for r in rows) == ["alice", "bob", "carol", "svc_etl"]


def test_period_count_is_not_the_sum_of_daily_counts(sample):
    daily = sample.query(
        "SELECT f.query_date, count(DISTINCT f.user_name) FROM fact_query f JOIN bridge_query_table b "
        "ON b.cluster_id = f.cluster_id AND b.query_id = f.query_id "
        "WHERE b.catalog_name = 'lake' AND b.schema_name = 'sales' AND b.table_name = 'orders' AND b.access_type = 'READ' "
        "AND f.query_state = 'FINISHED' AND f.end_time >= TIMESTAMP '2026-11-01 00:00:00 UTC' AND f.end_time < TIMESTAMP '2026-11-05 00:00:00 UTC' "
        "GROUP BY 1 ORDER BY 1"
    )
    assert [count for _, count in daily] == [1, 3, 2, 1]
    assert sum(count for _, count in daily) == 7
    assert sample.distinct_users(CLUSTER, ORDERS, START, END) == 4


def test_team_totals_add_up_to_all_queries(sample):
    period = "end_time >= TIMESTAMP '2026-11-01 00:00:00 UTC' AND end_time < TIMESTAMP '2026-11-05 00:00:00 UTC'"
    by_team = sample.query(f"SELECT team_id, sum(cpu_time_seconds), count(*) FROM v_query_team WHERE {period} GROUP BY 1")
    total_cpu, total_rows = sample.query(f"SELECT sum(cpu_time_seconds), count(*) FROM fact_query WHERE {period}")[0]
    assert sum(cpu for _, cpu, _ in by_team) == total_cpu == EXPECTED["total_cpu_seconds_in_period"]
    assert sum(rows for _, _, rows in by_team) == total_rows == 12
    # The view never adds or drops a query, in any period.
    assert sample.query("SELECT count(*) FROM v_query_team")[0][0] == sample.query("SELECT count(*) FROM fact_query")[0][0]


def test_query_spanning_a_team_change_belongs_to_the_team_at_submit_time(sample):
    # q05: submitted 2026-11-02 23:50 (bob in analytics), ended 2026-11-03 00:20 (bob in platform).
    team, query_date = sample.query("SELECT team_id, CAST(query_date AS varchar) FROM v_query_team WHERE query_id LIKE '%_00005_%'")[0]
    assert (team, query_date) == ("analytics", "2026-11-03")


def test_bridge_has_one_row_per_table_and_access_type(sample):
    # Self-join lists the input twice; one READ row.
    assert bridge_rows(sample, "20261104_100000_00009_demo1") == [(ORDERS, "READ", True)]
    # Read through a view: base table, flagged as not directly referenced; the view itself is not a table.
    assert bridge_rows(sample, "20261102_110000_00003_demo1") == [(ORDERS, "READ", False)]
    # MERGE: source is read; the target is both read and written, and is absent from metadata.tables.
    assert bridge_rows(sample, "20261102_020000_00008_demo1") == [
        ("lake.sales.daily_revenue", "READ", None),
        ("lake.sales.daily_revenue", "WRITE", None),
        (ORDERS, "READ", True),
    ]
    # A query that read no table has no bridge rows but is still a query.
    assert bridge_rows(sample, "20261104_110000_00010_demo1") == []


def test_loading_a_batch_again_changes_nothing(sample):
    before = sample.query("SELECT count(*) FROM fact_query")[0][0], sample.query("SELECT count(*) FROM bridge_query_table")[0][0]
    sample.load_batch("batch-001")
    after = sample.query("SELECT count(*) FROM fact_query")[0][0], sample.query("SELECT count(*) FROM bridge_query_table")[0][0]
    assert before == after
    assert sample.distinct_users(CLUSTER, ORDERS, START, END) == 4
    assert sample.team_resources("analytics", START, END).cpu_seconds == 42.5
