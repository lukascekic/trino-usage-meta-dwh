"""Canonical content, complete conflict rejection, and transport-independent identity."""

from __future__ import annotations

import copy
import json

import pytest

from pipeline.warehouse import BatchRejected
from tests.support import batch_status, load, make_event, snapshot

X, Z = "lake.sales.x", "lake.sales.z"


def base_event():
    return make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 2,
                      inputs=(X, Z), output=X, physical_input_bytes=0)


def land(warehouse, batch_id, events):
    warehouse.ingest(batch_id, "demo", "2026-11-10T00:00:00Z", [json.dumps(e) for e in events])


def equivalent_event(event, variant):
    result = copy.deepcopy(event)
    if variant == "relation-order-and-multiplicity":
        result["ioMetadata"]["inputs"].reverse()
        result["ioMetadata"]["inputs"].append(copy.deepcopy(result["ioMetadata"]["inputs"][0]))
        result["metadata"]["tables"].reverse()
        indirect = copy.deepcopy(result["metadata"]["tables"][0])
        indirect["directlyReferenced"] = False
        result["metadata"]["tables"].append(indirect)
    else:
        # Same typed instants: the offset is normalized and sub-microsecond digits are discarded.
        result["createTime"] = "2026-11-01T11:00:00.000000099+01:00"
        result["executionStartTime"] = "2026-11-01T11:00:00.000000099+01:00"
        result["endTime"] = "2026-11-01T11:00:10.000000099+01:00"
        result["statistics"]["cpuTime"] = 2.0
    return result


@pytest.mark.parametrize("variant", ["relation-order-and-multiplicity", "typed-numbers-and-times"])
@pytest.mark.parametrize("same_batch", [True, False], ids=["same-batch", "later-batch"])
def test_equivalent_projections_have_one_content_hash(warehouse, variant, same_batch):
    event = base_event()
    other = equivalent_event(event, variant)
    load(warehouse, [event, other] if same_batch else [event], batch_id="original")
    before = snapshot(warehouse)
    if not same_batch:
        land(warehouse, "redelivery", [other])
        warehouse.load_batch("redelivery")
        assert snapshot(warehouse) == before
    assert warehouse.query("SELECT count(*) FROM fact_query") == [[1]]
    assert warehouse.query("SELECT count(*) FROM bridge_query_table") == [[3]]
    assert warehouse.query("SELECT count(DISTINCT content_hash) FROM stg_query_content") == [[1]]
    assert set(batch_status(warehouse).values()) == {"SUCCEEDED"}


def changed_event(event, variant):
    result = copy.deepcopy(event)
    if variant == "output-only":
        result["ioMetadata"]["output"]["table"] = "different_target"
    elif variant == "direct-reference-flag":
        result["metadata"]["tables"][0]["directlyReferenced"] = False
    elif variant == "unknown-instead-of-zero":
        del result["statistics"]["physicalInputBytes"]
    elif variant == "optional-source":
        result["context"]["source"] = "different-client"
    else:
        result["executionStartTime"] = "2026-11-01T10:00:00.000001000Z"
    return result


@pytest.mark.parametrize("variant", ["output-only", "direct-reference-flag", "unknown-instead-of-zero",
                                     "optional-source", "execution-start"])
def test_one_conflict_rejects_new_queries_before_and_after_it(warehouse, variant):
    event = base_event()
    load(warehouse, [event], batch_id="original")
    before = snapshot(warehouse)
    fresh = [make_event(f"new-{i}", "new-user", "2026-11-02 10:00:00", "2026-11-02 10:00:10", 3,
                        inputs=(f"lake.sales.new_table_{i}",)) for i in range(2)]
    land(warehouse, "mixed", [fresh[0], changed_event(event, variant), fresh[1]])
    with pytest.raises(BatchRejected) as rejected:
        warehouse.load_batch("mixed")
    assert {delivery for delivery, _ in rejected.value.problems} == {"mixed/00002"}
    assert snapshot(warehouse) == before
    assert batch_status(warehouse) == {"original": "SUCCEEDED", "mixed": "FAILED"}
    assert warehouse.pending_batches() == ["mixed"]


def test_intra_batch_conflict_keeps_unrelated_new_query_out_of_model(warehouse):
    event = base_event()
    unrelated = make_event("new", "u", "2026-11-02 10:00:00", "2026-11-02 10:00:10", 3,
                           inputs=("lake.sales.new_table",))
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [event, unrelated, changed_event(event, "direct-reference-flag")], batch_id="mixed")
    assert {delivery for delivery, _ in rejected.value.problems} == {"mixed/00001", "mixed/00003"}
    assert snapshot(warehouse) == {"fact_query": [], "bridge_query_table": [], "dim_table": []}
    assert batch_status(warehouse) == {"mixed": "FAILED"}
