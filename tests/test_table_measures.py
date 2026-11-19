"""Bytes and rows read per table: the only measures the bridge carries.

Trino reports them per input (ioMetadata.inputs[]), once per scan. They are summed over every scan of a table
in a query, so a self-join counts both scans. A measure a scan did not report makes the table's value NULL;
bytes and rows are independent. CPU and memory stay in fact_query."""

from __future__ import annotations

import copy
import json

import pytest

from pipeline.warehouse import BatchRejected
from tests.conftest import _fresh_warehouse
from tests.support import bridge_measures, captured_events, load, make_event, snapshot

X, Z = "lake.sales.x", "lake.sales.z"
DAY = ("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z")
ORDERS, CUSTOMERS, ORDERS_COPY = "iceberg.demo.orders", "iceberg.demo.customers", "iceberg.demo.orders_copy"


def event(query_id="q1", inputs=(X, Z), input_bytes=(300, 700), input_rows=(3, 7), total=1000, **kw):
    return make_event(query_id, "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 10, inputs=inputs,
                      input_bytes=input_bytes, input_rows=input_rows, physical_input_bytes=total, **kw)


# ---- real events ---------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def captured(request):
    wh = _fresh_warehouse(request.module.__name__)
    events = captured_events()
    load(wh, list(events.values()), cluster_id="local")
    yield wh, {label: e["metadata"]["queryId"] for label, e in events.items()}
    wh.drop()


def test_self_join_counts_both_scans(captured):
    # Trino reports two identical inputs of 690 bytes / 3 rows. Removing duplicates first would give 690.
    wh, ids = captured
    assert bridge_measures(wh, ids["k10-self-join"]) == [(ORDERS, "READ", 1380, 6)]


def test_join_of_two_tables_keeps_each_table_s_own_measure(captured):
    wh, ids = captured
    assert bridge_measures(wh, ids["k10-join-two-tables"]) == [(CUSTOMERS, "READ", 316, 2), (ORDERS, "READ", 690, 3)]


def test_read_and_write_of_one_table_does_not_double_the_read(captured):
    wh, ids = captured
    assert bridge_measures(wh, ids["k10-insert-select"]) == [(ORDERS, "READ", 690, 3), (ORDERS, "WRITE", None, None)]
    assert bridge_measures(wh, ids["k10-merge"]) == [
        (ORDERS, "READ", 1380, 6), (ORDERS_COPY, "READ", 740, 6), (ORDERS_COPY, "WRITE", None, None),
    ]


def test_zero_is_a_known_value(captured):
    wh, ids = captured
    assert bridge_measures(wh, ids["k10-fail-runtime"]) == [(ORDERS, "READ", 0, 0)]
    assert bridge_measures(wh, ids["k10-metadata-table"]) == [("iceberg.demo.orders$snapshots", "READ", 0, 3)]


def test_on_the_captured_events_tables_add_up_to_the_query(captured):
    # Observed on these seven events; not guaranteed by Trino and not required by the load.
    wh, _ = captured
    rows = wh.query(
        "SELECT f.query_id, f.physical_input_bytes, f.physical_input_rows, "
        "sum(b.physical_input_bytes), sum(b.physical_input_rows) "
        "FROM fact_query f JOIN bridge_query_table b ON b.query_id = f.query_id AND b.access_type = 'READ' "
        "GROUP BY 1, 2, 3"
    )
    assert len(rows) == 7
    assert all(r[1] == r[3] and r[2] == r[4] for r in rows)


# ---- missing and damaged values ----------------------------------------------------------------------------

def test_one_scan_without_bytes_makes_that_table_null_and_leaves_rows_alone(warehouse):
    load(warehouse, [event(inputs=(X, X, Z), input_bytes=(100, None, 700), input_rows=(1, 2, 7))])
    assert bridge_measures(warehouse, "q1") == [(X, "READ", None, 3), (Z, "READ", 700, 7)]


def test_all_measures_absent(warehouse):
    load(warehouse, [event(input_bytes=(None, None), input_rows=(None, None))])
    assert bridge_measures(warehouse, "q1") == [(X, "READ", None, None), (Z, "READ", None, None)]
    volume = warehouse.table_read_volume("demo", *DAY)
    assert [(r["table_name"], r["physical_input_bytes"], r["queries_without_input_bytes"], r["queries_without_input_rows"])
            for r in volume] == [("x", None, 1, 1), ("z", None, 1, 1)]


@pytest.mark.parametrize(
    "field,value,problem",
    [
        ("physicalInputBytes", -1, "an ioMetadata.inputs byte or row count is negative"),
        ("physicalInputRows", -3, "an ioMetadata.inputs byte or row count is negative"),
        ("physicalInputBytes", 1.5, "an ioMetadata.inputs byte or row count is present but not a whole number within the 64-bit range"),
        ("physicalInputBytes", "690", "an ioMetadata.inputs byte or row count is present but not a whole number within the 64-bit range"),
    ],
)
def test_damaged_per_table_measure_rejects_the_batch(warehouse, field, value, problem):
    damaged = event()
    damaged["ioMetadata"]["inputs"][1][field] = value
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [event("healthy"), damaged])
    assert rejected.value.problems == [("b1/00002", problem)]
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0


def test_per_table_measure_beyond_64_bits_rejects_the_batch(warehouse):
    damaged = event()
    damaged["ioMetadata"]["inputs"][1]["physicalInputRows"] = 2 ** 63
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [event("healthy"), damaged])
    # Out of range on its own. An invalid value is not added into the per-table sum, so only one problem.
    assert rejected.value.problems == [
        ("b1/00002", "an ioMetadata.inputs byte or row count is present but not a whole number within the 64-bit range"),
    ]
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0


def test_per_table_sum_beyond_64_bits_rejects_the_batch(warehouse):
    big = 2 ** 62
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [event(inputs=(X, X), input_bytes=(big, big), input_rows=(1, 1))])
    assert rejected.value.problems == [("b1/00001", "the byte or row counts of one table in ioMetadata.inputs add up to more than a 64-bit integer")]


# ---- identity of content --------------------------------------------------------------------------------------

def _redeliver(warehouse, other):
    warehouse.ingest("b2", "demo", "2026-11-11T00:00:00Z", [json.dumps(other)])
    warehouse.load_batch("b2")


def test_same_query_total_but_different_split_is_a_conflict(warehouse):
    load(warehouse, [event()])
    before = snapshot(warehouse)
    with pytest.raises(BatchRejected) as rejected:
        _redeliver(warehouse, event(input_bytes=(400, 600)))  # the query still reports 1000 bytes
    assert rejected.value.problems == [("b2/00001", "conflicts with the already loaded version of query demo/q1")]
    assert snapshot(warehouse) == before


def test_different_rows_only_is_a_conflict(warehouse):
    load(warehouse, [event()])
    with pytest.raises(BatchRejected):
        _redeliver(warehouse, event(input_rows=(3, 8)))


def test_reordered_inputs_are_the_same_event(warehouse):
    load(warehouse, [event()])
    before = snapshot(warehouse)
    reordered = copy.deepcopy(event())
    reordered["ioMetadata"]["inputs"].reverse()
    _redeliver(warehouse, reordered)
    assert snapshot(warehouse) == before


def test_same_table_name_in_two_catalogs_keeps_two_measures(warehouse):
    load(warehouse, [event(inputs=("lake.sales.x", "pg.sales.x"), input_bytes=(100, 900), input_rows=(1, 9))])
    assert bridge_measures(warehouse, "q1") == [("lake.sales.x", "READ", 100, 1), ("pg.sales.x", "READ", 900, 9)]


# ---- the example query ----------------------------------------------------------------------------------------

def test_read_volume_counts_failed_queries_and_respects_the_period(warehouse):
    load(warehouse, [
        event("ok", inputs=(X,), input_bytes=(100,), input_rows=(1,), total=100),
        make_event("failed", "u", "2026-11-01 11:00:00", "2026-11-01 11:00:01", 1, state="FAILED", inputs=(X,),
                   input_bytes=(40,), input_rows=(4,)),
        make_event("tomorrow", "u", "2026-11-01 23:59:00", "2026-11-02 00:00:00", 1, inputs=(X,),
                   input_bytes=(5000,), input_rows=(50,)),
    ])
    assert warehouse.table_read_volume("demo", *DAY) == [{
        "catalog_name": "lake", "schema_name": "sales", "table_name": "x", "queries": 2,
        "physical_input_bytes": 140, "physical_input_rows": 5,
        "queries_without_input_bytes": 0, "queries_without_input_rows": 0,
    }]


def test_read_volume_of_the_sample(sample):
    from pipeline.warehouse import REPO_ROOT

    expected = json.loads((REPO_ROOT / "fixtures" / "expected" / "read_volume_expected.json").read_text(encoding="utf-8"))
    assert sample.table_read_volume("demo", "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z") == expected["read_volume"]
