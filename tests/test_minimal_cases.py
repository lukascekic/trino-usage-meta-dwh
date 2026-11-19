"""The smallest inputs that tell a correct model from the two classic mistakes:
multiplying query resources by the number of tables, and adding up per-day distinct counts."""

from __future__ import annotations

from tests.support import load, make_event

X, Z = "lake.sales.x", "lake.sales.z"
MEMBERSHIP = [("u", "analytics", "2026-01-01T00:00:00Z", None)]
START, END = "2026-11-01T00:00:00Z", "2026-11-03T00:00:00Z"


def test_two_queries_two_days_two_tables(warehouse):
    load(
        warehouse,
        [
            make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X, Z)),
            make_event("q2", "u", "2026-11-02 10:00:00", "2026-11-02 10:00:04", 4, inputs=(X,)),
        ],
        memberships=MEMBERSHIP,
    )
    team = warehouse.team_resources("analytics", START, END)
    assert team.cpu_seconds == 14  # not 24: the first query touched two tables but ran once
    assert team.query_count == 2
    assert warehouse.distinct_users("demo", X, START, END) == 1  # not 2: same user on both days
    assert warehouse.distinct_users("demo", Z, START, END) == 1


def test_one_query_over_two_tables_counts_once(warehouse):
    load(warehouse, [make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X, Z))], memberships=MEMBERSHIP)
    assert warehouse.team_resources("analytics", START, END).cpu_seconds == 10  # not 20
    assert warehouse.distinct_users("demo", X, START, END) == 1
    assert warehouse.distinct_users("demo", Z, START, END) == 1


def test_control_same_query_over_one_table(warehouse):
    load(warehouse, [make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X,))], memberships=MEMBERSHIP)
    assert warehouse.team_resources("analytics", START, END).cpu_seconds == 10
    assert warehouse.distinct_users("demo", X, START, END) == 1
    assert warehouse.distinct_users("demo", Z, START, END) == 0


def test_same_event_delivered_twice_in_one_batch_is_one_query(warehouse):
    event = make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=(X, Z))
    load(warehouse, [event, event], memberships=MEMBERSHIP)
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 1
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 2
    assert warehouse.team_resources("analytics", START, END).cpu_seconds == 10
