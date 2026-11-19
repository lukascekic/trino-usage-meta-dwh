"""Observe published mappings while rejecting, replacing and retrying a full reference snapshot."""

from __future__ import annotations

from datetime import datetime

import pytest

from pipeline.warehouse import ReferenceDataInvalid, Warehouse
from tests.adversarial.test_read_volume_integrity import event, land
from tests.support import snapshot

START = "2026-11-01T00:00:00Z"
BOUNDARY = "2026-11-01T12:00:00Z"
OLD_TEAMS = [("UNASSIGNED", "Unassigned"), ("alpha", "Alpha"), ("retired", "Retired")]
NEW_TEAMS = [("UNASSIGNED", "Unassigned"), ("alpha", "Renamed alpha"), ("beta", "Beta")]
OLD_HISTORY = [("u", "retired", START, None), ("removed", "alpha", START, None)]
NEW_HISTORY = [
    ("u", "beta", START, BOUNDARY),
    ("u", "alpha", BOUNDARY, None),
    ("added", "beta", START, None),
]
OLD_ATTRIBUTION = [
    ["added", "UNASSIGNED", 1.0], ["at-boundary", "retired", 1.0],
    ["before-boundary", "retired", 1.0], ["removed", "alpha", 1.0],
]
NEW_ATTRIBUTION = [
    ["added", "beta", 1.0], ["at-boundary", "alpha", 1.0],
    ["before-boundary", "beta", 1.0], ["removed", "UNASSIGNED", 1.0],
]


def published(warehouse):
    return (
        warehouse.query("SELECT team_id, team_name FROM dim_team ORDER BY team_id"),
        warehouse.query("SELECT user_name, team_id, valid_from, valid_to FROM user_team_history "
                        "ORDER BY user_name, valid_from"),
    )


def expected_reference(teams, memberships):
    def moment(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None

    history = [[user, team, moment(start), moment(end)] for user, team, start, end in memberships]
    return sorted([list(row) for row in teams]), sorted(history, key=lambda row: (row[0], row[2]))


def attribution(warehouse):
    return warehouse.query("SELECT query_id, team_id, cpu_time_seconds FROM v_query_team ORDER BY query_id")


@pytest.fixture
def mapped(warehouse):
    warehouse.load_reference(OLD_TEAMS, OLD_HISTORY)
    events = []
    for query_id, user, submitted in [
        ("before-boundary", "u", "2026-11-01T11:59:59.999999Z"),
        ("at-boundary", "u", BOUNDARY),
        ("removed", "removed", START),
        ("added", "added", START),
    ]:
        item = event(query_id, scans=[], ended="2026-11-01T13:00:00Z")
        item["context"]["user"] = user
        item["createTime"] = submitted
        events.append(item)
    land(warehouse, "original", events)
    warehouse.load_pending()
    assert attribution(warehouse) == OLD_ATTRIBUTION
    return warehouse


@pytest.mark.parametrize("damage,reason", [
    ("overlap", "overlapping membership"),
    ("duplicate-start", "duplicate membership start"),
    ("empty-interval", "empty or negative membership interval"),
    ("unknown-team", "membership refers to unknown team"),
    ("duplicate-team", "duplicate team"),
    ("missing-unassigned", "reserved team UNASSIGNED is missing"),
])
def test_rejected_snapshot_preserves_both_tables_and_existing_reports(mapped, damage, reason):
    teams, memberships = list(NEW_TEAMS), list(NEW_HISTORY)
    if damage == "overlap":
        memberships.append(("u", "alpha", "2026-11-01T06:00:00Z", None))
    elif damage == "duplicate-start":
        memberships.append(("u", "alpha", START, BOUNDARY))
    elif damage == "empty-interval":
        memberships.append(("zero", "alpha", START, START))
    elif damage == "unknown-team":
        memberships.append(("unknown", "missing", START, None))
    elif damage == "duplicate-team":
        teams.append(("beta", "A conflicting name"))
    else:
        teams = [row for row in teams if row[0] != "UNASSIGNED"]
    before, facts_before = published(mapped), snapshot(mapped)

    with pytest.raises(ReferenceDataInvalid, match=reason):
        mapped.load_reference(teams, memberships)

    assert published(mapped) == before
    assert attribution(mapped) == OLD_ATTRIBUTION
    assert snapshot(mapped) == facts_before
    # Rejected staging data must not poison the next valid replacement.
    mapped.load_reference(NEW_TEAMS, NEW_HISTORY)
    assert published(mapped) == expected_reference(NEW_TEAMS, NEW_HISTORY)
    assert attribution(mapped) == NEW_ATTRIBUTION


def test_readers_see_complete_table_versions_between_statements(mapped, monkeypatch):
    old, new = published(mapped), expected_reference(NEW_TEAMS, NEW_HISTORY)
    observations = []
    original_query = mapped.query
    reader = Warehouse(prefix=mapped.dwh_schema.removesuffix("meta_dwh"))

    def execute_and_observe(sql, params=None):
        rows = original_query(sql, params)
        observed = published(reader)
        # The two tables are separate commits; each table must nevertheless be complete.
        assert observed[0] in (old[0], new[0])
        assert observed[1] in (old[1], new[1])
        report = attribution(reader)
        assert report in (OLD_ATTRIBUTION, NEW_ATTRIBUTION)
        observations.append(report)
        return rows

    with monkeypatch.context() as patch:
        patch.setattr(mapped, "query", execute_and_observe)
        mapped.load_reference(NEW_TEAMS, NEW_HISTORY)
    assert OLD_ATTRIBUTION in observations
    assert NEW_ATTRIBUTION in observations
    assert published(mapped) == new


@pytest.mark.parametrize("table", ["dim_team", "user_team_history"])
def test_lost_reference_commit_acknowledgement_recovers_with_a_new_client(mapped, monkeypatch, table):
    before = snapshot(mapped)
    original_query = mapped.query
    tripped = []

    def execute_then_disconnect(sql, params=None):
        rows = original_query(sql, params)
        if sql.lstrip().startswith(f"MERGE INTO {table} AS"):
            tripped.append(table)
            raise ConnectionError("reference commit succeeded but its acknowledgement was lost")
        return rows

    with monkeypatch.context() as patch:
        patch.setattr(mapped, "query", execute_then_disconnect)
        with pytest.raises(ConnectionError, match="acknowledgement was lost"):
            mapped.load_reference(NEW_TEAMS, NEW_HISTORY)
    assert tripped == [table]
    fresh = Warehouse(prefix=mapped.dwh_schema.removesuffix("meta_dwh"))
    assert attribution(fresh) == (OLD_ATTRIBUTION if table == "dim_team" else NEW_ATTRIBUTION)
    fresh.load_reference(NEW_TEAMS, NEW_HISTORY)
    fresh.load_reference(NEW_TEAMS, NEW_HISTORY)
    assert published(fresh) == expected_reference(NEW_TEAMS, NEW_HISTORY)
    assert attribution(fresh) == NEW_ATTRIBUTION
    assert snapshot(fresh) == before
    fresh.check()


def test_empty_history_removes_last_membership_without_losing_queries(mapped):
    teams = [("UNASSIGNED", "Unassigned")]
    before = snapshot(mapped)
    mapped.load_reference(teams, [])
    mapped.load_reference(teams, [])
    assert published(mapped) == expected_reference(teams, [])
    assert attribution(mapped) == [[row[0], "UNASSIGNED", row[2]] for row in OLD_ATTRIBUTION]
    assert snapshot(mapped) == before
    mapped.check()
