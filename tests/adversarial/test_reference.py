"""Checks for the independent oracle; runnable without pytest or Trino.

python3 -m unittest tests.adversarial.test_reference -v
"""

from __future__ import annotations

import copy
import unittest
from decimal import Decimal

from tests.adversarial.reference import Delivery, Membership, Reference, check_sample, instant


class ReferenceTests(unittest.TestCase):
    def test_all_sample_answers_match(self):
        check_sample()

    def test_manifest_preserves_deliveries_but_reports_unique_queries(self):
        ref = Reference.from_files()
        self.assertEqual(len(ref.deliveries), 16)
        self.assertEqual(len(ref.queries), 14)
        self.assertEqual(ref.deliveries[-1].delivery_id, "batch-003/00006")
        self.assertTrue(any(isinstance(e["statistics"]["cpuTime"], Decimal) for e in ref.queries.values()))

    def test_nanoseconds_are_floored_and_offsets_normalized(self):
        before = instant("2026-11-05T01:59:59.999999900+02:00")
        self.assertEqual(before, instant("2026-11-04T23:59:59.999999Z"))
        self.assertLess(before, instant("2026-11-05T00:00:00Z"))

    def test_empty_subday_period_does_not_use_daily_totals(self):
        ref = Reference.from_files()
        start, end = "2026-11-01T12:00:00Z", "2026-11-02T00:00:00Z"
        self.assertEqual(ref.distinct_users("demo", ("lake", "sales", "orders"), start, end), 0)
        resources = ref.team_resources("analytics", start, end)
        self.assertEqual(resources["query_count"], 0)
        self.assertIsNone(resources["cpu_seconds"])
        self.assertIsNone(resources["physical_input_bytes"])

    def test_correction_changes_old_team_totals_without_new_events(self):
        ref = Reference.from_files()
        boundary = instant("2026-11-02T00:00:00Z")
        ref.memberships = [Membership(m.user, m.team, boundary if m.user == "bob" and m.team == "platform" else m.start,
                                      boundary if m.user == "bob" and m.team == "analytics" else m.end)
                           for m in ref.memberships]
        start, end = "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z"
        self.assertEqual([ref.team_resources(team, start, end)["cpu_seconds"]
                          for team in ("analytics", "platform", "UNASSIGNED")], [33.5, 37.0, 5.0])

    def test_cluster_is_part_of_query_and_table_identity(self):
        ref = Reference.from_files()
        extra = copy.deepcopy(ref.deliveries[1].event)
        extra["context"]["user"] = "another-user"
        extended = Reference(ref.deliveries + [Delivery("other", "other/00001", "other-cluster", extra)],
                             ref.memberships, ref.teams)
        self.assertEqual(len(extended.queries), 15)
        start, end = "2026-11-01T00:00:00Z", "2026-11-05T00:00:00Z"
        self.assertEqual(extended.distinct_users("demo", ("lake", "sales", "orders"), start, end), 4)
        self.assertEqual(extended.distinct_users("other-cluster", ("lake", "sales", "orders"), start, end), 1)


if __name__ == "__main__":
    unittest.main()
