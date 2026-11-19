"""Distinguish absent optional fields from damaged values and coercible wrong types."""

from __future__ import annotations

import pytest

from pipeline.warehouse import BatchRejected
from tests.support import bridge_rows, load, make_event

X = "lake.sales.x"


def event(query_id: str) -> dict:
    return make_event(query_id, "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 2,
                      inputs=(X,), output=X, query_type="INSERT", update_type="INSERT")


# Mutate exactly one consumed field. Unknown fields outside the adapter contract
# remain allowed; the adapter need not validate the entire Trino event schema.
MALFORMED = [
    ("query-id-number", ("metadata", "queryId"), 17),
    ("query-state-boolean", ("metadata", "queryState"), True),
    ("user-number", ("context", "user"), 17),
    ("version-number", ("context", "serverVersion"), 484),
    ("input-catalog-number", ("ioMetadata", "inputs", 0, "catalogName"), 17),
    ("input-schema-boolean", ("ioMetadata", "inputs", 0, "schema"), True),
    ("input-table-number", ("ioMetadata", "inputs", 0, "table"), 17),
    ("output-table-number", ("ioMetadata", "output", "table"), 17),
    ("execution-start-object", ("executionStartTime",), {}),
    ("principal-array", ("context", "principal"), []),
    ("original-user-object", ("context", "originalUser"), {}),
    ("source-object", ("context", "source"), {}),
    ("query-type-array", ("context", "queryType"), []),
    ("update-type-object", ("metadata", "updateType"), {}),
    ("client-tag-number", ("context", "clientTags"), [17]),
    ("resource-group-boolean", ("context", "resourceGroupId"), ["global", False]),
    ("failure-code-object", ("failureInfo",), {"errorCode": {"name": {}, "type": "USER_ERROR"}}),
    ("metadata-table-without-identity", ("metadata", "tables"), [{}]),
    ("directly-referenced-object", ("metadata", "tables", 0, "directlyReferenced"), {}),
    ("directly-referenced-string", ("metadata", "tables", 0, "directlyReferenced"), "false"),
    ("fractional-byte-count", ("statistics", "physicalInputBytes"), 1.5),
    ("overflowing-byte-count", ("statistics", "peakUserMemoryBytes"), 2 ** 63),
]


@pytest.mark.parametrize("name,path,value", MALFORMED, ids=[case[0] for case in MALFORMED])
def test_bad_consumed_field_rejects_batch_with_delivery_id(warehouse, name, path, value):
    damaged = event("damaged")
    node = damaged
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(BatchRejected) as rejected:
        load(warehouse, [event("healthy"), damaged], batch_id="typed")
    assert rejected.value.problems
    assert {delivery for delivery, _ in rejected.value.problems} == {"typed/00002"}
    assert all(problem for _, problem in rejected.value.problems)
    assert warehouse.query("SELECT count(*) FROM fact_query")[0][0] == 0
    assert warehouse.query("SELECT count(*) FROM bridge_query_table")[0][0] == 0


def test_absent_optional_fields_are_still_accepted(warehouse):
    minimal = event("minimal")
    for field in ("principal", "originalUser", "source", "clientTags", "resourceGroupId", "queryType"):
        minimal["context"].pop(field, None)
    for field in ("updateType", "tables"):
        minimal["metadata"].pop(field, None)
    minimal.pop("executionStartTime", None)
    minimal["statistics"] = {"cpuTime": 2}
    minimal["ioMetadata"].pop("output")
    load(warehouse, [minimal])
    assert warehouse.query(
        "SELECT principal, original_user, source, client_tags, resource_group, query_type, update_type, "
        "execution_start_time, failed_cpu_time_seconds, physical_input_bytes, peak_user_memory_bytes FROM fact_query"
    ) == [[None] * 11]
    assert bridge_rows(warehouse, "minimal") == [(X, "READ", None)]


def test_text_identifiers_that_look_numeric_remain_valid(warehouse):
    valid = event("17")
    valid["context"].update(user="17", clientTags=["17"], resourceGroupId=["global", "17"])
    load(warehouse, [valid])
    assert warehouse.query("SELECT query_id, user_name, client_tags, resource_group FROM fact_query") == [
        ["17", "17", ["17"], "global.17"]
    ]
