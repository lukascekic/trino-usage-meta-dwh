# Captured events

`trino484_query_completed.jsonl` holds seven real `QueryCompletedEvent` messages, exactly as Trino 484 sent
them through its HTTP event listener (`http-event-listener.log-completed=true`) while running queries
against Iceberg tables. One event per line; nothing was edited. Each query was run with a label passed as
`--source`, so the scenario is in `context.source`; the `k10-` prefix is just the name of the capture run.

| `context.source` | Query | What the event shows |
|---|---|---|
| `k10-join-two-tables` | join of `orders` and `customers` | two entries in `ioMetadata.inputs` |
| `k10-via-view` | `SELECT` from a view over `orders` | the input is the base table; `metadata.tables` marks it `directlyReferenced = false` |
| `k10-self-join` | `orders` joined with itself | the same input is listed twice |
| `k10-fail-runtime` | division by zero | `queryState = FAILED`, `failureInfo` present, the input is still listed |
| `k10-insert-select` | `INSERT INTO orders SELECT ... FROM orders` | the table is both an input and the output |
| `k10-merge` | `MERGE INTO orders_copy USING orders` | the target is listed as an input too, but is absent from `metadata.tables` |
| `k10-metadata-table` | `SELECT count(*) FROM "orders$snapshots"` | an Iceberg metadata table is reported under its own name, `orders$snapshots` |

The reading layer (`sql/load/10_*.sql`, `20_*.sql`, `21_*.sql`) was written against this shape, and
`tests/test_captured_events.py` loads these events and checks the result. Things worth knowing:

- durations (`cpuTime`, `wallTime`, ...) are JSON numbers in **seconds**;
- timestamps are ISO-8601 text with **nanosecond** precision;
- an optional key without a value is **absent**, not `null` (`failureInfo`, `ioMetadata.output`, `context.queryType`);
- one event is 70-580 KB, almost all of it query plan and operator statistics that the model does not read.

The synthetic sample events in `../events/` follow the same shape but omit those large fields.
