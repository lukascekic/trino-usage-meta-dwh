"""Trino emits nanoseconds; Iceberg stores microseconds. Timestamps are truncated, never rounded, so a query
cannot slip across a reporting-period boundary or a team-membership boundary."""

from __future__ import annotations

import pytest

from tests.support import load, make_event

X = "lake.sales.x"
START, END = "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z"
# bob moves from analytics to platform at 2026-11-03 00:00:00 UTC.
BOB = [
    ("bob", "analytics", "2026-01-01T00:00:00Z", "2026-11-03T00:00:00Z"),
    ("bob", "platform", "2026-11-03T00:00:00Z", None),
]


@pytest.mark.parametrize(
    "end_time,inside",
    [
        ("2026-11-04T23:59:59.999999900Z", True),   # 100 ns before the end: rounding would push it out
        ("2026-11-05T00:00:00.000000000Z", False),  # exactly the end: [from, to) excludes it
        ("2026-11-05T00:00:00.000000100Z", False),  # 100 ns after
        ("2026-11-01T00:00:00.000000000Z", True),   # exactly the start
        ("2026-10-31T23:59:59.999999900Z", False),  # 100 ns before the start
        ("2026-11-05T01:59:59.999999900+02:00", True),  # same instant as the first case, written with an offset
    ],
)
def test_period_boundary(warehouse, end_time, inside):
    load(warehouse, [make_event("q1", "bob", "2026-10-31T23:00:00.000000000Z", end_time, 10, inputs=(X,))], memberships=BOB)
    assert warehouse.distinct_users("demo", X, START, END) == (1 if inside else 0)
    assert warehouse.team_resources("analytics", START, END).query_count == (1 if inside else 0)


def test_stored_time_is_truncated_to_microseconds(warehouse):
    load(warehouse, [make_event("q1", "bob", "2026-11-04T23:59:59.123456789Z", "2026-11-04T23:59:59.999999900Z", 1)], memberships=BOB)
    row = warehouse.query("SELECT CAST(create_time AS varchar), CAST(end_time AS varchar), CAST(query_date AS varchar) FROM fact_query")[0]
    assert row == ["2026-11-04 23:59:59.123456 UTC", "2026-11-04 23:59:59.999999 UTC", "2026-11-04"]


@pytest.mark.parametrize(
    "create_time,team",
    [
        ("2026-11-02T23:59:59.999999900Z", "analytics"),  # 100 ns before the change
        ("2026-11-03T00:00:00.000000000Z", "platform"),   # exactly at the change
        ("2026-11-03T00:00:00.000000100Z", "platform"),   # 100 ns after
    ],
)
def test_team_boundary(warehouse, create_time, team):
    load(warehouse, [make_event("q1", "bob", create_time, "2026-11-03T12:00:00.000000000Z", 10)], memberships=BOB)
    assert warehouse.query("SELECT team_id FROM v_query_team")[0][0] == team


def test_period_given_in_another_time_zone_is_not_cut_short_by_the_partition_filter(warehouse):
    # 2026-11-01 00:00 in Belgrade is 2026-10-31 23:00 UTC. A query that ended at 23:30 UTC on 10-31 is inside
    # the period, although its UTC date is 10-31: the date filter must use the UTC date of the bound.
    from datetime import datetime
    from zoneinfo import ZoneInfo

    load(warehouse, [make_event("q1", "bob", "2026-10-31T23:20:00.000000000Z", "2026-10-31T23:30:00.000000000Z", 1,
                                inputs=(X,))], memberships=BOB)
    belgrade = ZoneInfo("Europe/Belgrade")
    start, end = datetime(2026, 11, 1, tzinfo=belgrade), datetime(2026, 11, 2, tzinfo=belgrade)
    rows = warehouse.run_file("queries/q1_distinct_users.sql", ["demo", "lake", "sales", "x", start, end, start, end])
    assert rows == [[1]]
