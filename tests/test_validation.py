"""A delivered event is either loaded or its batch is rejected with a reason. It is never dropped silently,
and a damaged event is never mistaken for a query that read no table."""

from __future__ import annotations

import copy
import json

import pytest

from pipeline.warehouse import BatchRejected, ReferenceDataInvalid
from tests.support import TEAMS, captured_events, load, make_event


def real_event() -> dict:
    return copy.deepcopy(captured_events()["k10-join-two-tables"])


def without(event: dict, *path: str) -> dict:
    node = event
    for key in path[:-1]:
        node = node[key]
    del node[path[-1]]
    return event


def _drop_inputs(e):
    return without(e, "ioMetadata", "inputs")


def _drop_table_of_first_input(e):
    del e["ioMetadata"]["inputs"][0]["table"]
    return e


def _inputs_not_an_array(e):
    e["ioMetadata"]["inputs"] = {"table": "orders"}
    return e


def _statistic_of_wrong_type(e):
    e["statistics"]["physicalInputBytes"] = "many"
    return e


def _unknown_version(e):
    e["context"]["serverVersion"] = "999"
    return e


def _cpu_time_as_text(e):
    e["statistics"]["cpuTime"] = "0.041"
    return e


def _output_without_table(e):
    e["ioMetadata"]["output"] = {"catalogName": "iceberg", "schema": "demo"}
    return e


def _negative_cpu(e):
    e["statistics"]["cpuTime"] = -0.5
    return e


def _negative_bytes(e):
    e["statistics"]["physicalInputBytes"] = -1
    return e


def _ends_before_it_starts(e):
    e["endTime"] = "2026-11-17T01:11:39.000000000Z"  # createTime is 01:11:39.310748153Z
    return e


def _still_running(e):
    e["metadata"]["queryState"] = "RUNNING"
    return e


def _end_time_without_zone(e):
    e["endTime"] = "2026-11-17T12:00:00.000000000"  # would be read in the session's time zone
    return e


REJECTED = [
    (_negative_cpu, "a statistic is negative"),
    (_negative_bytes, "a statistic is negative"),
    (_ends_before_it_starts, "endTime is before createTime"),
    (_still_running, "metadata.queryState is RUNNING, expected FINISHED or FAILED"),
    (_end_time_without_zone, "a timestamp has no time zone (createTime, executionStartTime or endTime)"),
    (_drop_inputs, "ioMetadata.inputs is missing or not an array"),
    (_inputs_not_an_array, "ioMetadata.inputs is missing or not an array"),
    (_drop_table_of_first_input, "an ioMetadata.inputs entry lacks catalogName, schema or table"),
    (_output_without_table, "ioMetadata.output lacks catalogName, schema or table"),
    (_statistic_of_wrong_type, "an optional statistic is present but not a number"),
    (_cpu_time_as_text, "statistics.cpuTime is missing or not a number"),
    (_unknown_version, "unsupported serverVersion 999"),
    (lambda e: without(e, "context", "user"), "context.user is missing or not text"),
    (lambda e: without(e, "metadata", "queryId"), "metadata.queryId is missing or not text"),
    (lambda e: without(e, "endTime"), "endTime is missing or unreadable"),
    (lambda e: without(e, "statistics", "cpuTime"), "statistics.cpuTime is missing or not a number"),
]


@pytest.mark.parametrize("mutate,problem", REJECTED, ids=[p for _, p in REJECTED])
def test_damaged_event_rejects_the_whole_batch(warehouse, mutate, problem):
    healthy = make_event("healthy", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=("lake.sales.x",))
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [healthy, mutate(real_event())])
    # The damaged message is named; the healthy one in the same batch is not loaded either.
    assert rejected.value.problems == [("b1/00002", problem)]
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 0
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0


def test_text_that_is_not_json_rejects_the_batch(warehouse):
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, ['{"metadata": {"queryId": "broken"'])
    assert rejected.value.problems == [("b1/00001", "event_json is not valid JSON")]


def test_empty_inputs_is_a_valid_query_without_tables(warehouse):
    event = real_event()
    event["ioMetadata"]["inputs"] = []
    load(warehouse, [event])
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 1
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0


def test_absent_optional_statistic_becomes_null_not_zero(warehouse):
    event = without(real_event(), "statistics", "physicalInputBytes")
    load(warehouse, [event])
    assert warehouse.query("SELECT physical_input_bytes FROM fact_query")[0][0] is None
    team = warehouse.team_resources("UNASSIGNED", "2026-11-17T00:00:00Z", "2026-11-18T00:00:00Z")
    assert (team.physical_input_bytes, team.queries_without_input_bytes) == (None, 1)


def test_raw_event_is_stored_untouched(warehouse):
    text = json.dumps(real_event())
    load(warehouse, [text])
    assert warehouse.query(f"SELECT event_json FROM {warehouse.raw_schema}.query_events")[0][0] == text


def test_rejected_membership_version_leaves_the_published_one_in_place(warehouse):
    load(warehouse, [make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 3)],
         memberships=[("u", "analytics", "2026-01-01T00:00:00Z", None)])
    published = warehouse.query("SELECT * FROM user_team_history")
    # Published as it is, this version would put q1 into analytics twice: 2 queries and 6 CPU seconds.
    overlapping = [
        ("u", "analytics", "2026-01-01T00:00:00Z", None),
        ("u", "analytics", "2026-06-01T00:00:00Z", None),
    ]
    with pytest.raises(ReferenceDataInvalid) as invalid:
        warehouse.load_reference(TEAMS, overlapping)
    assert "overlapping membership: u" in invalid.value.problems[0]
    assert warehouse.query("SELECT * FROM user_team_history") == published
    team = warehouse.team_resources("analytics", "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z")
    assert (team.query_count, team.cpu_seconds) == (1, 3.0)


def test_load_stops_when_the_published_memberships_overlap(warehouse):
    # The gate before every load still catches a published table that was changed some other way.
    warehouse.load_reference(TEAMS, [("u", "analytics", "2026-01-01T00:00:00Z", None)])
    warehouse.query("INSERT INTO user_team_history VALUES ('u', 'platform', TIMESTAMP '2026-05-01 00:00:00 UTC', NULL)")
    with pytest.raises(ReferenceDataInvalid) as invalid:
        warehouse.load_batch("any")
    assert "overlapping membership: u" in invalid.value.problems[0]


def test_new_membership_version_replaces_the_old_one(warehouse):
    start = "2026-01-01T00:00:00Z"
    warehouse.load_reference(TEAMS, [("a", "analytics", start, None), ("b", "analytics", start, None)])
    new_version = [("a", "platform", start, None), ("c", "analytics", start, None)]
    for _ in range(2):  # loading the same version again changes nothing
        warehouse.load_reference(TEAMS, new_version)
        assert warehouse.query("SELECT user_name, team_id FROM user_team_history ORDER BY 1") == [
            ["a", "platform"], ["c", "analytics"]
        ]
    assert warehouse.query("SELECT team_id FROM dim_team ORDER BY 1") == [["UNASSIGNED"], ["analytics"], ["platform"]]


def test_membership_of_unknown_team_stops_the_load(warehouse):
    with pytest.raises(ReferenceDataInvalid) as invalid:
        warehouse.load_reference(TEAMS, [("u", "ghosts", "2026-01-01T00:00:00Z", None)])
    assert invalid.value.problems == ["membership refers to unknown team: u -> ghosts"]
