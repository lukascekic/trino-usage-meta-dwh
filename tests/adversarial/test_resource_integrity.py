"""Unknown measures and floating-point arithmetic must not misreport valid data."""

from __future__ import annotations

import json
from dataclasses import asdict

import pytest

from pipeline.warehouse import InvariantViolated, ReferenceDataInvalid, utc
from tests.support import TEAMS, load, make_event

START, END = "2026-11-01T00:00:00Z", "2026-11-03T00:00:00Z"


def test_daily_resources_expose_missing_input_bytes(warehouse):
    events = [make_event(f"q{i}", "u", f"2026-11-0{day} 10:00:00", f"2026-11-0{day} 10:00:10", 2,
                         physical_input_bytes=value, peak_memory_bytes=5)
              for i, (day, value) in enumerate(((1, 11), (1, 0), (1, 0), (2, 0)))]
    del events[2]["statistics"]["physicalInputBytes"]
    del events[3]["statistics"]["physicalInputBytes"]
    load(warehouse, events)

    # Columns are matched by name, so the test does not depend on their order.
    cursor = warehouse._conn.cursor()
    cursor.execute(warehouse._render("queries/trend_team_resources_daily.sql")[0],
                   ["UNASSIGNED", utc(START), utc(END)])
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    assert "queries_without_input_bytes" in columns, "a partial SUM must expose its missing-value count"
    daily = [dict(zip(columns, row)) for row in rows]
    for row, lower, upper in zip(daily, (START, "2026-11-02T00:00:00Z"), ("2026-11-02T00:00:00Z", END)):
        date = row.pop("query_date")
        assert str(date) == lower[:10]
        assert row == asdict(warehouse.team_resources("UNASSIGNED", lower, upper))
    assert len(daily) == 2
    assert daily[0]["physical_input_bytes"] == 11 and daily[0]["queries_without_input_bytes"] == 1
    assert daily[1]["physical_input_bytes"] is None and daily[1]["queries_without_input_bytes"] == 1


@pytest.mark.parametrize("reverse", [False, True])
def test_non_binary_decimal_metrics_do_not_fail_conservation(warehouse, reverse):
    # The sample data uses whole and half CPU seconds, which are exact in binary floating point. Values such
    # as 0.1 are not, so here the order and grouping of a sum can change its last bits.
    memberships = [(f"u{i}", "analytics" if i % 2 else "platform", "2026-01-01T00:00:00Z", None)
                   for i in range(5)]
    warehouse.load_reference(TEAMS, memberships)
    values = [0.1, 0.2, 0.3, 0.001, 1.23456789, 1000.0000001] * 20
    if reverse:
        values.reverse()
    events = [make_event(f"q{i:03d}", f"u{i % 8}", "2026-11-01 10:00:00", "2026-11-01 10:00:10", value)
              for i, value in enumerate(values)]
    for index in range(3):
        warehouse.ingest(f"b{index}", "demo", "2026-11-10T00:00:00Z",
                         [json.dumps(e) for e in events[index * 40:(index + 1) * 40]])
    warehouse.load_pending()
    warehouse.check()
    # No loss or multiplication at the row level, independently of floating-point aggregate order.
    columns = "cluster_id, query_id, cpu_time_seconds"
    facts = warehouse.query(f"SELECT {columns} FROM fact_query ORDER BY 1, 2")
    attributed = warehouse.query(f"SELECT {columns} FROM v_query_team ORDER BY 1, 2")
    assert facts == attributed and len(facts) == 120
    total = warehouse.query("SELECT sum(cpu_time_seconds) FROM fact_query")[0][0]
    teams = [warehouse.team_resources(team, START, END).cpu_seconds or 0
             for team in ("analytics", "platform", "UNASSIGNED")]
    assert sum(teams) == pytest.approx(total, rel=1e-12, abs=1e-12)


def test_conservation_still_rejects_real_row_multiplication(warehouse):
    membership = [("u", "analytics", "2026-01-01T00:00:00Z", None)]
    load(warehouse, [make_event("q1", "u", "2026-11-01 10:00:00", "2026-11-01 10:00:10", 0.1)],
         memberships=membership)
    warehouse.query(
        "INSERT INTO user_team_history VALUES ('u', 'platform', TIMESTAMP '2026-02-01 00:00:00 UTC', NULL)"
    )
    with pytest.raises((InvariantViolated, ReferenceDataInvalid)):
        warehouse.check()
