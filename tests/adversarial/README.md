# Independent checks

Run the reference calculation and its unit tests without Docker or third-party packages:

```sh
python3 -m tests.adversarial.reference
python3 -m unittest tests.adversarial.test_reference -v
python3 -m unittest tests.adversarial.test_read_volume_reference -v
```

The calculation reads the committed JSONL files in manifest order, retains their batch and delivery
identities, and reads the team CSV files. It imports neither the pipeline nor the fixture generator.
Only the request parameters come from the expected-answer file. All answers are calculated before
comparison; decimal CPU values are added using `Decimal`. Conflicting duplicate payloads are outside
this reference's scope and raise an error.

Run all checks in this directory against the real Trino/Iceberg stack:

```sh
docker compose run --rm runner python -m pytest -q -p no:cacheprovider tests/adversarial
```

The integration tests use the existing isolated warehouse fixtures. They vary cluster identity,
effective versus authenticated user, table schema and case, read/write roles, mixed direct and indirect
lineage, subday periods, UTC offsets, missing measures, and membership corrections. Validation cases
require a structured rejection naming the damaged delivery and no model rows from that batch. Controls
keep absent optional fields and string identifiers that look numeric valid.

Loading checks also cover equivalent typed timestamps and relation sets, conflicts mixed with unrelated
new queries, lost commit acknowledgements, recovery through a new client after staging is overwritten,
and restarting a rebuild after each deletion. Resource checks cover missing-value counts in daily reports
and conservation with CPU values that are not exactly representable in binary floating-point arithmetic.

`read_volume_reference.py` computes per-table measures and the read-volume report directly from event JSON.
Its integration checks vary scan multiplicity, partial coverage, query totals, cluster identity and period
boundaries, and test conflicts and recovery with these measures. Numeric boundary cases distinguish each
table's storage limit from a sum across unrelated tables, and require malformed values to be rejected
without crashing the validator.
