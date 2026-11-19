"""Vary per-input measurements independently of query totals, delivery layout and cluster."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pipeline.warehouse import BatchRejected, InjectedFailure
from tests.adversarial.read_volume_reference import bridge_measures, read_volume
from tests.adversarial.reference import Reference
from tests.support import batch_status, snapshot

ROOT = Path(__file__).resolve().parents[2]
DAY = ("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z")


def scan(table="c.s.x", byte_count=300, row_count=3):
    catalog, schema, name = table.split(".")
    result = {"catalogName": catalog, "schema": schema, "table": name}
    if byte_count is not None:
        result["physicalInputBytes"] = byte_count
    if row_count is not None:
        result["physicalInputRows"] = row_count
    return result


def event(query_id="q", scans=None, *, ended="2026-11-01T10:00:00Z", state="FINISHED", output=None):
    # This minimal event is independent of the production fixture generator and its even byte split.
    result = {
        "metadata": {"queryId": query_id, "queryState": state},
        "context": {"user": "u", "serverVersion": "484"},
        "createTime": DAY[0], "endTime": ended,
        "statistics": {"cpuTime": 1, "physicalInputBytes": 1000, "physicalInputRows": 10},
        "ioMetadata": {"inputs": scans if scans is not None else [scan(), scan("c.s.z", 700, 7)]},
    }
    if output is not None:
        result["ioMetadata"]["output"] = {k: v for k, v in scan(output).items()
                                           if k in ("catalogName", "schema", "table")}
    return result


@pytest.fixture
def ready(warehouse):
    warehouse.load_reference([("UNASSIGNED", "Unassigned")], [])
    return warehouse


def land(warehouse, batch, events, cluster="a"):
    warehouse.ingest(batch, cluster, "2026-11-10T00:00:00Z", [json.dumps(e) for e in events])


def assert_bridge(warehouse, deliveries):
    actual = warehouse.query(
        "SELECT cluster_id, query_id, catalog_name, schema_name, table_name, access_type, "
        "physical_input_bytes, physical_input_rows FROM bridge_query_table ORDER BY 1, 2, 3, 4, 5, 6"
    )
    assert [tuple(row) for row in actual] == bridge_measures(deliveries)


def test_mixed_coverage_failed_queries_clusters_and_periods(ready):
    events = [
        event("q", [scan(byte_count=11, row_count=None), scan(byte_count=5, row_count=2),
                    scan("c.s.z", 5, 1), scan("other.s.x", 16, 1)], ended=DAY[0], output="c.s.x"),
        event("failed", [scan(byte_count=None, row_count=4), scan(byte_count=7, row_count=6),
                         scan("c.s.z", 0, 0)], state="FAILED", ended="2026-11-01T12:00:00Z"),
        event("write-only", [], output="c.s.x"),
        event("zero", [scan(byte_count=0, row_count=0)], ended="2026-11-01T23:59:59.999999900Z"),
        event("unknown", [scan("c.s.unknown", None, None)]),
        event("tomorrow", [scan(byte_count=9000)], ended=DAY[1]),
    ]
    other = event("q", [scan(byte_count=999, row_count=99)])  # same query id, different cluster
    land(ready, "first", events)
    land(ready, "duplicate", [copy.deepcopy(events[0])])
    land(ready, "other", [other], cluster="b")
    ready.load_pending()
    deliveries = [("a", e) for e in events] + [("a", events[0]), ("b", other)]
    assert_bridge(ready, deliveries)
    periods = [DAY, ("2026-11-01T12:00:00Z", "2026-11-01T23:59:00Z"),
               ("2026-11-01T03:00:00+03:00", "2026-11-02T03:00:00+03:00"),
               (DAY[1], "2026-11-03T00:00:00Z"), (DAY[0], DAY[0])]
    for cluster in ("a", "b", "absent"):
        for start, end in periods:
            assert ready.table_read_volume(cluster, start, end) == read_volume(deliveries, cluster, start, end)


def test_real_events_match_the_independent_projection(ready):
    events = [json.loads(line) for line in
              (ROOT / "fixtures/captured/trino484_query_completed.jsonl").read_text().splitlines()]
    land(ready, "captured", events, cluster="local")
    ready.load_pending()
    deliveries = [("local", e) for e in events]
    assert_bridge(ready, deliveries)
    assert ready.table_read_volume("local", "2026-11-17T00:00:00Z", "2026-11-18T00:00:00Z") == read_volume(
        deliveries, "local", "2026-11-17T00:00:00Z", "2026-11-18T00:00:00Z")


def test_sample_report_matches_raw_json_reference(sample):
    deliveries = [(d.cluster_id, d.event) for d in Reference.from_files().deliveries]
    assert sample.table_read_volume("demo", "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z") == read_volume(
        deliveries, "demo", "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z")


@pytest.mark.parametrize("same_batch", [False, True])
def test_equivalent_scan_splits_have_one_stored_projection(ready, same_batch):
    original = event()
    split = event(scans=[scan("c.s.z", 700, 7), scan(byte_count=200, row_count=2), scan(byte_count=100, row_count=1)])
    land(ready, "first", [original, split] if same_batch else [original])
    ready.load_pending()
    before = snapshot(ready)
    if not same_batch:
        land(ready, "later", [split])
        ready.load_pending()
        assert snapshot(ready) == before
    assert_bridge(ready, [("a", original)])
    assert ready.query("SELECT count(*) FROM fact_query") == [[1]]


@pytest.mark.parametrize("field", ["physicalInputBytes", "physicalInputRows"])
@pytest.mark.parametrize("same_batch", [False, True])
def test_positive_measure_conflict_rejects_surrounding_healthy_events(ready, field, same_batch):
    original = event()
    changed = copy.deepcopy(original)
    changed["ioMetadata"]["inputs"][0][field] += 1
    changed["ioMetadata"]["inputs"][1][field] -= 1  # query total and the sum over tables stay fixed
    if not same_batch:
        land(ready, "original", [original])
        ready.load_pending()
    before = snapshot(ready)
    mixed = [event("healthy-before"), changed, event("healthy-after")]
    if same_batch:
        mixed.insert(1, original)
    land(ready, "conflict", mixed)
    with pytest.raises(BatchRejected) as error:
        ready.load_batch("conflict")
    expected = {"conflict/00002", "conflict/00003"} if same_batch else {"conflict/00002"}
    assert {delivery for delivery, _ in error.value.problems} == expected
    assert snapshot(ready) == before
    assert batch_status(ready)["conflict"] == "FAILED"


@pytest.mark.parametrize("point", ["fact", "bridge"])
def test_partial_measure_recovery_and_rebuild_preserve_the_report(ready, point):
    events = [event(scans=[scan(byte_count=11, row_count=None), scan(byte_count=5, row_count=2)]),
              event("later", [scan(byte_count=None, row_count=6)])]
    land(ready, "interrupted", events)
    with pytest.raises(InjectedFailure):
        ready.load_batch("interrupted", stop_after=point)
    ready.load_pending()
    deliveries = [("a", e) for e in events]
    expected = read_volume(deliveries, "a", *DAY)
    assert_bridge(ready, deliveries)
    assert ready.table_read_volume("a", *DAY) == expected
    ready.rebuild()
    assert_bridge(ready, deliveries)
    assert ready.table_read_volume("a", *DAY) == expected


@pytest.mark.parametrize("field", ["physicalInputBytes", "physicalInputRows"])
def test_separate_tables_do_not_share_one_storage_overflow_limit(ready, field):
    original = event()
    for entry in original["ioMetadata"]["inputs"]:
        entry[field] = 2 ** 62  # each table's aggregate fits bigint; the cross-table sum does not
    land(ready, "large", [original])
    ready.load_pending()
    assert_bridge(ready, [("a", original)])
    assert ready.table_read_volume("a", *DAY) == read_volume([("a", original)], "a", *DAY)


@pytest.mark.parametrize("field", ["physicalInputBytes", "physicalInputRows"])
def test_invalid_large_numbers_do_not_overflow_the_validator(ready, field):
    damaged = event()
    for entry in damaged["ioMetadata"]["inputs"]:
        entry[field] = 10 ** 38 - 1  # each fits decimal(38, 0), their sum does not; neither is a valid bigint
    before = snapshot(ready)
    land(ready, "damaged", [event("healthy"), damaged])
    with pytest.raises(BatchRejected) as error:
        ready.load_batch("damaged")
    assert {delivery for delivery, _ in error.value.problems} == {"damaged/00002"}
    assert snapshot(ready) == before
    assert batch_status(ready)["damaged"] == "FAILED"


@pytest.mark.parametrize("field,value", [
    ("physicalInputBytes", {}), ("physicalInputRows", []), ("physicalInputBytes", True),
    ("physicalInputRows", "3"), ("physicalInputRows", 1.5),
])
def test_per_input_json_types_are_rejected_without_coercion(ready, field, value):
    damaged = event()
    damaged["ioMetadata"]["inputs"][0][field] = value
    before = snapshot(ready)
    land(ready, "damaged", [event("healthy"), damaged])
    with pytest.raises(BatchRejected) as error:
        ready.load_batch("damaged")
    assert {delivery for delivery, _ in error.value.problems} == {"damaged/00002"}
    assert snapshot(ready) == before
    assert batch_status(ready)["damaged"] == "FAILED"
