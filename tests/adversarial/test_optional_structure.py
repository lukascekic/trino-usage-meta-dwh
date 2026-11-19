"""An invalid optional parent must not make its consumed children look absent."""

from __future__ import annotations

import copy

import pytest

from pipeline.warehouse import BatchRejected
from tests.support import captured_events, load, make_event


@pytest.mark.parametrize("failure_info", [
    "damaged",
    [],
    {"errorCode": "damaged"},
    {"errorCode": []},
], ids=["failure-info-text", "failure-info-array", "error-code-text", "error-code-array"])
def test_wrong_type_of_optional_parent_rejects_batch(warehouse, failure_info):
    damaged = copy.deepcopy(captured_events()["k10-fail-runtime"])
    assert damaged["metadata"]["queryState"] == "FAILED"
    assert isinstance(damaged["failureInfo"]["errorCode"], dict)
    damaged["failureInfo"] = failure_info
    healthy = make_event("healthy", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 2)

    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [healthy, damaged], batch_id="structure")
    assert rejected.value.problems
    assert {delivery for delivery, _ in rejected.value.problems} == {"structure/00002"}
    assert all(problem for _, problem in rejected.value.problems)
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 0
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0
