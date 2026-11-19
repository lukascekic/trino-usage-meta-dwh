"""Read-volume reference calculated directly from accepted event JSON.

Standard library and the independent timestamp parser only. Delivery duplicates must have the same
projected table measures; conflict detection is tested separately. No SQL or pipeline helpers are used.
"""

from __future__ import annotations

from collections import defaultdict

from tests.adversarial.reference import instant


def table_measures(event: dict) -> dict[tuple, tuple]:
    scans = defaultdict(list)
    for entry in event["ioMetadata"]["inputs"]:
        scans[(entry["catalogName"], entry["schema"], entry["table"])].append(entry)
    result = {}
    for table, entries in scans.items():
        values = []
        for field in ("physicalInputBytes", "physicalInputRows"):
            reported = [entry.get(field) for entry in entries]
            values.append(None if any(value is None for value in reported) else sum(reported))
        result[(*table, "READ")] = tuple(values)
    output = event["ioMetadata"].get("output")
    if output is not None:
        result[(output["catalogName"], output["schema"], output["table"], "WRITE")] = (None, None)
    return result


def projected_queries(deliveries: list[tuple[str, dict]]) -> dict:
    result = {}
    for cluster, event in deliveries:
        key = (cluster, event["metadata"]["queryId"])
        projection = (instant(event["endTime"]), table_measures(event))
        if key in result and result[key] != projection:
            raise ValueError("Conflicting table projection is outside this reference's accepted-input scope")
        result[key] = projection
    return result


def bridge_measures(deliveries: list[tuple[str, dict]]) -> list[tuple]:
    return sorted((*key, *table, *values)
                  for key, (_, tables) in projected_queries(deliveries).items()
                  for table, values in tables.items())


def read_volume(deliveries: list[tuple[str, dict]], cluster: str, start: str, end: str) -> list[dict]:
    lower, upper = instant(start), instant(end)
    groups = defaultdict(list)
    for (source, _), (ended, tables) in projected_queries(deliveries).items():
        if source == cluster and lower <= ended < upper:
            for (*table, access), values in tables.items():
                if access == "READ":
                    groups[tuple(table)].append(values)
    result = []
    for (catalog, schema, table), measures in groups.items():
        row = dict(catalog_name=catalog, schema_name=schema, table_name=table, queries=len(measures))
        for index, name in enumerate(("bytes", "rows")):
            known = [values[index] for values in measures if values[index] is not None]
            row[f"physical_input_{name}"] = sum(known) if known else None
            row[f"queries_without_input_{name}"] = len(measures) - len(known)
        result.append(row)
    return sorted(result, key=lambda r: (
        r["physical_input_bytes"] is None, -(r["physical_input_bytes"] or 0),
        r["catalog_name"], r["schema_name"], r["table_name"],
    ))
