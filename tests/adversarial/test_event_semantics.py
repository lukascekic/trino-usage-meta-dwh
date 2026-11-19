"""Vary cluster, identity, lineage, period, missing metrics, and membership history."""

from __future__ import annotations

import copy
from dataclasses import asdict

import pytest

from tests.adversarial.reference import Reference
from tests.support import TEAMS, bridge_rows, load, make_event

X = "lake.sales.x"
START, END = "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z"


@pytest.mark.parametrize("start,end", [
    (START, END),
    ("2026-11-01T12:00:00Z", "2026-11-02T00:00:00Z"),
    ("2026-11-02T23:55:00Z", "2026-11-03T00:25:00Z"),
    ("2026-11-04T23:00:00-02:00", "2026-11-05T03:00:00+01:00"),
])
def test_sql_reports_match_independent_reference(sample, start, end):
    ref = Reference.from_files()
    expected = ref.report("demo", start, end)
    for table, users in expected["distinct_users"].items():
        assert sample.distinct_users("demo", table, start, end) == users
    for team, resources in expected["team_resources"].items():
        assert asdict(sample.team_resources(team, start, end)) == resources


def test_same_query_id_in_two_clusters_keeps_both_queries_and_table_scopes(warehouse):
    first = make_event("same-id", "first-user", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 2, inputs=(X,))
    second = make_event("same-id", "second-user", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 7, inputs=(X,))
    load(warehouse, [first], batch_id="east-batch", cluster_id="east")
    load(warehouse, [second], batch_id="west-batch", cluster_id="west")
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 2
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 2
    assert warehouse.distinct_users("east", X, START, END) == 1
    assert warehouse.distinct_users("west", X, START, END) == 1
    assert warehouse.distinct_users("absent", X, START, END) == 0
    assert warehouse.team_resources("UNASSIGNED", START, END).cpu_seconds == 9


def test_schema_and_case_are_part_of_table_identity(warehouse):
    names = ("lake.sales.x", "lake.other.x", "lake.sales.X")
    events = [make_event(f"q{i}", f"u{i}", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 1,
                         inputs=(name,)) for i, name in enumerate(names)]
    load(warehouse, events)
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 3
    assert [warehouse.distinct_users("demo", name, START, END) for name in names] == [1, 1, 1]


def test_effective_user_decides_distinct_users_and_team(warehouse):
    events = []
    for index, user in enumerate(("alice", "bob")):
        event = make_event(f"q{index}", user, "2026-11-01 10:00:00", "2026-11-01 10:00:10", 3, inputs=(X,))
        event["context"].update(principal="shared-service", originalUser="shared-service")
        events.append(event)
    memberships = [("alice", "analytics", "2026-01-01T00:00:00Z", None),
                   ("bob", "analytics", "2026-01-01T00:00:00Z", None),
                   ("shared-service", "platform", "2026-01-01T00:00:00Z", None)]
    load(warehouse, events, memberships=memberships)
    assert warehouse.distinct_users("demo", X, START, END) == 2
    assert warehouse.team_resources("analytics", START, END).cpu_seconds == 6
    assert warehouse.team_resources("platform", START, END).query_count == 0


def test_write_only_target_does_not_count_as_read(warehouse):
    event = make_event("write-only", "writer", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 3,
                       output=X, referenced=(X,), query_type="INSERT", update_type="INSERT")
    load(warehouse, [event])
    assert bridge_rows(warehouse, "write-only") == [(X, "WRITE", True)]
    assert warehouse.distinct_users("demo", X, START, END) == 0
    assert warehouse.team_resources("UNASSIGNED", START, END).cpu_seconds == 3


@pytest.mark.parametrize("reverse", [False, True])
def test_direct_and_indirect_references_to_same_table_do_not_multiply_rows(warehouse, reverse):
    event = make_event("mixed", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 2,
                       inputs=(X, X), output=X)
    direct = event["metadata"]["tables"][0]
    indirect = copy.deepcopy(direct)
    indirect["directlyReferenced"] = False
    event["metadata"]["tables"] = [indirect, direct] if reverse else [direct, indirect]
    load(warehouse, [event])
    assert bridge_rows(warehouse, "mixed") == [(X, "READ", True), (X, "WRITE", True)]
    assert warehouse.team_resources("UNASSIGNED", START, END).cpu_seconds == 2


def test_known_zero_and_missing_metrics_are_distinct(warehouse):
    events = [make_event(f"q{i}", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", i + 1,
                         physical_input_bytes=value, peak_memory_bytes=peak)
              for i, (value, peak) in enumerate(((8, 9), (0, 0), (0, 0)))]
    del events[2]["statistics"]["physicalInputBytes"]
    del events[2]["statistics"]["peakUserMemoryBytes"]
    load(warehouse, events)
    assert asdict(warehouse.team_resources("UNASSIGNED", START, END)) == {
        "query_count": 3, "failed_query_count": 0, "cpu_seconds": 6,
        "physical_input_bytes": 8, "queries_without_input_bytes": 1, "max_query_peak_memory_bytes": 9,
    }


def test_membership_gap_and_later_correction_reassign_existing_queries(warehouse):
    memberships = [("u", "analytics", "2026-01-01T00:00:00Z", "2026-11-02T00:00:00Z"),
                   ("u", "platform", "2026-11-03T00:00:00Z", None)]
    events = [make_event(f"q{day}", "u", f"2026-11-0{day}T00:00:00Z", f"2026-11-0{day}T00:01:00Z", cpu)
              for day, cpu in ((1, 2), (2, 3), (3, 5))]
    load(warehouse, events, memberships=memberships)
    assert warehouse.query("SELECT query_id, team_id FROM v_query_team ORDER BY query_id") == [
        ["q1", "analytics"], ["q2", "UNASSIGNED"], ["q3", "platform"]
    ]
    before = warehouse.query("SELECT * FROM fact_query ORDER BY query_id")
    warehouse.load_reference(TEAMS, [("u", "platform", "2026-01-01T00:00:00Z", None)])
    assert warehouse.query("SELECT * FROM fact_query ORDER BY query_id") == before
    assert warehouse.team_resources("platform", START, END).cpu_seconds == 10
    assert warehouse.team_resources("UNASSIGNED", START, END).query_count == 0
    assert warehouse.team_resources("analytics", START, END).query_count == 0
    assert warehouse.query("SELECT count(*) FROM v_query_team")[0][0] == 3
