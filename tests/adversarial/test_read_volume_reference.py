"""Literal checks of the independent calculation, runnable without Trino using unittest."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from tests.adversarial.read_volume_reference import read_volume, table_measures
from tests.adversarial.reference import Reference

ROOT = Path(__file__).resolve().parents[2]
DAY = ("2026-11-17T00:00:00Z", "2026-11-18T00:00:00Z")


class ReadVolumeReferenceTests(unittest.TestCase):
    def test_real_self_join_retains_both_identical_scans(self):
        events = [json.loads(line) for line in
                  (ROOT / "fixtures/captured/trino484_query_completed.jsonl").read_text().splitlines()]
        event = next(e for e in events if e["context"]["source"] == "k10-self-join")
        self.assertEqual(table_measures(event), {("iceberg", "demo", "orders", "READ"): (1380, 6)})
        row = read_volume([("local", event), ("local", event)], "local", *DAY)[0]
        self.assertEqual((row["queries"], row["physical_input_bytes"], row["physical_input_rows"]), (1, 1380, 6))

    def test_partial_and_zero_are_independent_for_bytes_and_rows(self):
        event = {
            "metadata": {"queryId": "q"}, "endTime": DAY[0],
            "ioMetadata": {"inputs": [
                {"catalogName": "c", "schema": "s", "table": "x", "physicalInputBytes": 11, "physicalInputRows": 0},
                {"catalogName": "c", "schema": "s", "table": "x", "physicalInputRows": 0},
            ]},
        }
        self.assertEqual(table_measures(event), {("c", "s", "x", "READ"): (None, 0)})
        self.assertEqual(read_volume([("local", event)], "local", *DAY), [{
            "catalog_name": "c", "schema_name": "s", "table_name": "x", "queries": 1,
            "physical_input_bytes": None, "physical_input_rows": 0,
            "queries_without_input_bytes": 1, "queries_without_input_rows": 0,
        }])

    def test_committed_sample_answers_match_raw_events(self):
        reference = Reference.from_files()
        deliveries = [(d.cluster_id, d.event) for d in reference.deliveries]
        expected = json.loads((ROOT / "fixtures/expected/read_volume_expected.json").read_text())
        actual = read_volume(deliveries, "demo", "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z")
        self.assertEqual(actual, expected["read_volume"])
        self.assertEqual(sum(r["physical_input_bytes"] for r in actual), 7400)


if __name__ == "__main__":
    unittest.main()
