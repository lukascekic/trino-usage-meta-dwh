"""Calculate both reports directly from delivered JSON and membership CSVs.

Standard library only; no SQL, pipeline imports, or fixture-generator imports.
Run from the repository root: python3 -m tests.adversarial.reference
Only request parameters are read from sample_expected.json; its answers are
compared after the calculation. Duplicate events must have identical payloads:
conflicting versions and recovery belong to the later loading-protocol tests.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures"


def instant(text: str) -> datetime:
    """Interpret an explicit ISO offset and floor nanoseconds to microseconds."""
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})", text
    )
    if match is None:
        raise ValueError(f"Expected an ISO timestamp with an explicit offset: {text}")
    seconds, fraction, offset = match.groups()
    microseconds = (fraction or "")[:6].ljust(6, "0")
    return datetime.fromisoformat(
        f"{seconds}.{microseconds}{'+00:00' if offset == 'Z' else offset}"
    ).astimezone(timezone.utc)


@dataclass(frozen=True)
class Delivery:
    batch_id: str
    delivery_id: str
    cluster_id: str
    event: dict


@dataclass(frozen=True)
class Membership:
    user: str
    team: str
    start: datetime
    end: datetime | None


class Reference:
    def __init__(self, deliveries: list[Delivery], memberships: list[Membership], teams: set[str]):
        self.deliveries = deliveries
        self.memberships = memberships
        self.teams = teams | {"UNASSIGNED"}
        self.queries: dict[tuple[str, str], dict] = {}
        for delivery in deliveries:
            key = delivery.cluster_id, delivery.event["metadata"]["queryId"]
            if key in self.queries and self.queries[key] != delivery.event:
                raise ValueError(f"Conflicting payload outside this reference's scope: {key}")
            self.queries[key] = delivery.event

    @classmethod
    def from_files(cls, fixtures: Path = FIXTURES) -> Reference:
        deliveries = []
        manifest = json.loads((fixtures / "events" / "manifest.json").read_text(encoding="utf-8"))
        for batch in manifest:
            lines = (fixtures / "events" / batch["file"]).read_text(encoding="utf-8").splitlines()
            for number, line in enumerate((line for line in lines if line.strip()), start=1):
                deliveries.append(Delivery(
                    batch["batch_id"], f"{batch['batch_id']}/{number:05d}", batch["cluster_id"],
                    json.loads(line, parse_float=Decimal),
                ))
        with (fixtures / "reference" / "teams.csv").open(newline="", encoding="utf-8") as handle:
            teams = {row["team_id"] for row in csv.DictReader(handle)}
        with (fixtures / "reference" / "user_team_history.csv").open(newline="", encoding="utf-8") as handle:
            memberships = [Membership(
                row["user_name"], row["team_id"], instant(row["valid_from"]),
                instant(row["valid_to"]) if row["valid_to"] else None,
            ) for row in csv.DictReader(handle)]
        return cls(deliveries, memberships, teams)

    def in_period(self, start: str, end: str) -> list[tuple[str, dict]]:
        lower, upper = instant(start), instant(end)
        return [(cluster, event) for (cluster, _), event in self.queries.items()
                if lower <= instant(event["endTime"]) < upper]

    def team_at_submission(self, event: dict) -> str:
        submitted = instant(event["createTime"])
        teams = [m.team for m in self.memberships
                 if m.user == event["context"]["user"] and m.start <= submitted
                 and (m.end is None or submitted < m.end)]
        if len(teams) > 1:
            raise ValueError("Overlapping memberships cannot define one team for a query")
        return teams[0] if teams else "UNASSIGNED"

    def distinct_users(self, cluster_id: str, table: tuple[str, str, str], start: str, end: str) -> int:
        users = set()
        for cluster, event in self.in_period(start, end):
            if cluster != cluster_id or event["metadata"]["queryState"] != "FINISHED":
                continue
            inputs = {(i["catalogName"], i["schema"], i["table"]) for i in event["ioMetadata"]["inputs"]}
            if table in inputs:
                users.add(event["context"]["user"])
        return len(users)

    def team_resources(self, team: str, start: str, end: str) -> dict:
        events = [event for _, event in self.in_period(start, end) if self.team_at_submission(event) == team]
        inputs = [e["statistics"].get("physicalInputBytes") for e in events]
        known_inputs = [value for value in inputs if value is not None]
        peaks = [e["statistics"].get("peakUserMemoryBytes") for e in events]
        known_peaks = [value for value in peaks if value is not None]
        return {
            "query_count": len(events),
            "failed_query_count": sum(e["metadata"]["queryState"] == "FAILED" for e in events),
            "cpu_seconds": float(sum(Decimal(str(e["statistics"]["cpuTime"])) for e in events)) if events else None,
            "physical_input_bytes": sum(known_inputs) if known_inputs else None,
            "queries_without_input_bytes": sum(value is None for value in inputs),
            "max_query_peak_memory_bytes": max(known_peaks) if known_peaks else None,
        }

    def report(self, cluster: str, start: str, end: str) -> dict:
        tables = set()
        for (source_cluster, _), event in self.queries.items():
            if source_cluster == cluster:
                identities = list(event["ioMetadata"]["inputs"])
                if event["ioMetadata"].get("output") is not None:
                    identities.append(event["ioMetadata"]["output"])
                tables.update((i["catalogName"], i["schema"], i["table"]) for i in identities)
        return {
            "cluster_id": cluster,
            "period_start": start,
            "period_end": end,
            "fact_query_rows": len(self.queries),
            "distinct_users": {".".join(t): self.distinct_users(cluster, t, start, end) for t in sorted(tables)},
            "team_resources": {t: self.team_resources(t, start, end) for t in sorted(self.teams)},
            "total_cpu_seconds_in_period": float(sum(
                Decimal(str(e["statistics"]["cpuTime"])) for _, e in self.in_period(start, end)
            )),
        }


def check_sample() -> dict:
    expected = json.loads((FIXTURES / "expected" / "sample_expected.json").read_text(encoding="utf-8"))
    reference = Reference.from_files()
    actual = reference.report(expected["cluster_id"], expected["period_start"], expected["period_end"])
    answers = {key: value for key, value in expected.items() if key != "comment"}
    if actual != answers:
        raise AssertionError(f"Independent report differs.\nActual: {actual}\nExpected: {answers}")
    return actual


if __name__ == "__main__":
    print(json.dumps(check_sample(), indent=2))
    print("REFERENCE PASSED: every expected value matches the independent JSONL/CSV calculation")
