"""Vary completion fields, session zones and ambiguous printed table names independently."""

from __future__ import annotations

import copy
from datetime import date, datetime, timedelta, timezone

import pytest

from pipeline.warehouse import BatchRejected
from tests.adversarial.test_read_volume_integrity import DAY, event, land
from tests.support import batch_status, snapshot


@pytest.fixture
def ready(warehouse):
    warehouse.load_reference([("UNASSIGNED", "Unassigned")], [])
    return warehouse


@pytest.mark.parametrize("field", ["createTime", "executionStartTime", "endTime"])
def test_naive_timestamp_in_any_read_field_rejects_the_whole_batch(ready, field):
    ready.query("SET TIME ZONE 'America/Los_Angeles'")
    land(ready, "saved", [event("saved")])
    ready.load_pending()
    before = snapshot(ready)
    damaged = event("damaged")
    damaged["executionStartTime"] = "2026-11-01T01:00:00Z"
    damaged[field] = damaged[field].removesuffix("Z")
    land(ready, "mixed", [event("healthy-before"), damaged, event("healthy-after")])

    with pytest.raises(BatchRejected) as error:
        ready.load_batch("mixed")

    assert {delivery for delivery, _ in error.value.problems} == {"mixed/00002"}
    assert any("no time zone" in problem for _, problem in error.value.problems)
    assert snapshot(ready) == before
    assert batch_status(ready) == {"saved": "SUCCEEDED", "mixed": "FAILED"}


def test_nonterminal_states_cannot_hide_among_valid_completions(ready):
    land(ready, "saved", [event("saved")])
    ready.load_pending()
    before = snapshot(ready)
    states = ["QUEUED", "RUNNING", "FINISHING", "finished"]
    mixed = [event("healthy-finished"), event("healthy-failed", state="FAILED")]
    mixed += [event(f"invalid-{state}", state=state) for state in states]
    land(ready, "mixed", mixed)

    with pytest.raises(BatchRejected) as error:
        ready.load_batch("mixed")

    assert {delivery for delivery, _ in error.value.problems} == {
        f"mixed/{number:05d}" for number in range(3, 7)
    }
    assert all("expected FINISHED or FAILED" in problem for _, problem in error.value.problems)
    assert snapshot(ready) == before
    assert batch_status(ready)["mixed"] == "FAILED"


def test_zoned_redeliveries_keep_the_same_projection_across_session_zones(ready):
    events = [event("finished"), event("failed", state="FAILED"),
              event("absent-start"), event("null-start")]
    for item in events[:2]:
        item["executionStartTime"] = "2026-11-01T01:00:00Z"
    events[-1]["executionStartTime"] = None
    land(ready, "utc", events)
    ready.load_pending()
    before = snapshot(ready)
    assert ready.distinct_users("a", ("c", "s", "x"), *DAY) == 1

    for index, (session_zone, offset_minutes) in enumerate([
        ("America/Los_Angeles", 345), ("Pacific/Chatham", -210),
    ]):
        ready.query(f"SET TIME ZONE '{session_zone}'")
        changed = copy.deepcopy(events)
        for item in changed:
            for field in ("createTime", "executionStartTime", "endTime"):
                if item.get(field) is not None:
                    moment = datetime.fromisoformat(item[field].replace("Z", "+00:00"))
                    item[field] = moment.astimezone(timezone(timedelta(minutes=offset_minutes))).isoformat()
        land(ready, f"offset-{index}", changed)
        ready.load_pending()
        assert snapshot(ready) == before
        assert ready.query("SELECT DISTINCT query_date FROM fact_query") == [[date(2026, 11, 1)]]
    assert set(batch_status(ready).values()) == {"SUCCEEDED"}
    assert ready.query("SELECT count(*) FROM fact_query") == [[4]]


def test_tuple_names_disambiguate_identical_printed_names_and_preserve_literals(ready):
    identities = [
        ("lake.eu", "sales", "orders"),
        ("lake", "eu.sales", "orders"),
        ("lake", "eu", "sales.orders"),
        ("la'ke.λ", 'sales."eu', "or'ders.v1"),
    ]
    assert len({".".join(parts) for parts in identities[:3]}) == 1
    events = []
    for index, (catalog, schema, table) in enumerate(identities, start=1):
        for user in range(index):
            item = event(f"q-{index}-{user}", scans=[{
                "catalogName": catalog, "schema": schema, "table": table,
            }])
            item["context"]["user"] = f"u-{index}-{user}"
            events.append(item)
    land(ready, "names", events)
    ready.load_pending()

    for count, parts in enumerate(identities, start=1):
        assert ready.distinct_users("a", parts, *DAY) == count
        assert ready.table_users_daily("a", parts, *DAY) == [[date(2026, 11, 1), count, count]]
    # The existing string API still splits at the first two dots.
    assert ready.distinct_users("a", "lake.eu.sales.orders", *DAY) == 3
    assert ready.table_users_daily("a", "lake.eu.sales.orders", *DAY) == [[date(2026, 11, 1), 3, 3]]
    assert ready.distinct_users("a", ("lake.eu", "sales", "absent"), *DAY) == 0
    assert ready.table_users_daily("a", ("lake.eu", "sales", "absent"), *DAY) == []
