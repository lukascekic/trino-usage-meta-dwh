"""Thin runner around the SQL in sql/.

All business logic lives in SQL. This module only connects to Trino, executes the SQL files in order,
lands sample events into the raw table and reports problems. Nothing here computes a usage statistic.
"""

from __future__ import annotations

import csv
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import trino

REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = REPO_ROOT / "sql"
CATALOG = "iceberg"
UNASSIGNED_TEAM = "UNASSIGNED"

# Trino rejects statements longer than query.max-length (1,000,000 characters by default).
MAX_INSERT_CHARS = 700_000


class BatchRejected(Exception):
    """A delivery batch failed validation. Nothing from the batch was loaded."""

    def __init__(self, batch_id: str, problems: list[tuple[str, str]]):
        self.batch_id = batch_id
        self.problems = problems
        lines = "\n".join(f"  {delivery_id}: {problem}" for delivery_id, problem in problems)
        super().__init__(f"batch {batch_id} rejected, {len(problems)} problem(s):\n{lines}")


class InvariantViolated(Exception):
    """The model broke one of its own invariants (sql/checks/model_invariants.sql)."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("model invariant violated:\n" + "\n".join(f"  {p}" for p in problems))


class InjectedFailure(Exception):
    """Raised on purpose by tests to simulate a crash in the middle of a load."""


class ReferenceDataInvalid(Exception):
    """Team reference data breaks a rule the model depends on (for example overlapping memberships)."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("reference data rejected:\n" + "\n".join(f"  {p}" for p in problems))


@dataclass(frozen=True)
class TeamResources:
    query_count: int
    failed_query_count: int
    cpu_seconds: float | None
    physical_input_bytes: int | None
    queries_without_input_bytes: int
    max_query_peak_memory_bytes: int | None


def utc(text: str) -> datetime:
    """Parse an ISO-8601 timestamp into an aware UTC datetime (microsecond precision)."""
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


TableName = str | tuple[str, str, str]


def table_parts(table: TableName) -> tuple[str, str, str]:
    """(catalog, schema, table) of a table name.

    A text name is split at its first two dots. Schema and table names may themselves contain dots, so for
    such a table pass the three parts as a tuple; the SQL always takes them separately.
    """
    if isinstance(table, str):
        catalog_name, schema_name, table_name = table.split(".", 2)
        return catalog_name, schema_name, table_name
    catalog_name, schema_name, table_name = table
    return catalog_name, schema_name, table_name


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into statements. Full-line comments are dropped; a statement ends at a line ending in ';'."""
    statements, current = [], []
    for line in sql.splitlines():
        if line.lstrip().startswith("--"):
            continue
        current.append(line)
        if line.rstrip().endswith(";"):
            statements.append("\n".join(current).rstrip().rstrip(";"))
            current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)
    return [s for s in statements if s.strip()]


class Warehouse:
    """One Meta DWH instance: a raw schema and a model schema inside the Iceberg catalog.

    `prefix` lets tests run in isolated schemas next to each other.
    """

    def __init__(self, prefix: str = "", host: str | None = None, port: int | None = None):
        self.raw_schema = f"{prefix}raw"
        self.dwh_schema = f"{prefix}meta_dwh"
        self._conn = trino.dbapi.connect(
            host=host or os.environ.get("TRINO_HOST", "localhost"),
            port=port or int(os.environ.get("TRINO_PORT", "8080")),
            user="meta_dwh_etl",
            source="meta-dwh-pipeline",
            catalog=CATALOG,
            schema=self.dwh_schema,
        )

    # ---- plumbing -------------------------------------------------------------------------------------

    def query(self, sql: str, params: list | None = None) -> list[list]:
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall()

    def _render(self, relative_path: str, names: dict[str, str] | None = None) -> list[str]:
        text = (SQL_DIR / relative_path).read_text(encoding="utf-8")
        text = text.replace("{{raw_schema}}", self.raw_schema).replace("{{dwh_schema}}", self.dwh_schema)
        for placeholder, value in (names or {}).items():
            text = text.replace("{{" + placeholder + "}}", value)
        return split_statements(text)

    def run_file(self, relative_path: str, params: list | None = None) -> list[list]:
        """Execute every statement of a SQL file; return the rows of the last one."""
        rows: list[list] = []
        for statement in self._render(relative_path):
            rows = self.query(statement, params)
        return rows

    def wait_until_ready(self, timeout_seconds: int = 180) -> str:
        """Wait for Trino and the Iceberg catalog; return the Trino version."""
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                version = self.query("SELECT version()")[0][0]
                self.query(f"SHOW SCHEMAS FROM {CATALOG}")
                return version
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(2)

    # ---- lifecycle ------------------------------------------------------------------------------------

    def init(self) -> None:
        """Create schemas, tables and views. Safe to run repeatedly."""
        self.run_file("ddl/01_raw.sql")
        self.run_file("ddl/02_model.sql")
        self.run_file("views/v_query_team.sql")

    def drop(self) -> None:
        """Remove both schemas with everything in them."""
        for schema in (self.dwh_schema, self.raw_schema):
            self.query(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")

    # ---- reference data (additional metadata) ---------------------------------------------------------

    def load_reference(self, teams: list[tuple[str, str]], memberships: list[tuple[str, str, str, str | None]]) -> None:
        """Replace team reference data with a full snapshot.

        The new version is written to staging tables and checked there first. If it breaks a rule, it is
        rejected and the published tables stay as they were. Otherwise each published table is brought to
        the new version with one MERGE, which is one commit, so a reader sees either the old or the new
        memberships, never a mix. The staging tables are shared, so this must not run during a load.

        teams: (team_id, team_name). memberships: (user_name, team_id, valid_from, valid_to or None), ISO-8601 UTC.
        """
        self.query("CREATE OR REPLACE TABLE stg_dim_team AS SELECT * FROM dim_team WITH NO DATA")
        self.query("CREATE OR REPLACE TABLE stg_user_team_history AS SELECT * FROM user_team_history WITH NO DATA")
        if teams:
            values = ", ".join(f"({sql_literal(team_id)}, {sql_literal(name)})" for team_id, name in teams)
            self.query(f"INSERT INTO stg_dim_team (team_id, team_name) VALUES {values}")
        if memberships:
            values = ", ".join(
                "({}, {}, {}, {})".format(
                    sql_literal(user),
                    sql_literal(team_id),
                    self._timestamp_literal(valid_from),
                    self._timestamp_literal(valid_to) if valid_to else "CAST(NULL AS timestamp(6) with time zone)",
                )
                for user, team_id, valid_from, valid_to in memberships
            )
            self.query(f"INSERT INTO stg_user_team_history (user_name, team_id, valid_from, valid_to) VALUES {values}")
        self.check_reference(teams="stg_dim_team", memberships="stg_user_team_history")
        self.run_file("load/02_replace_reference.sql")
        self.check_reference()

    def load_reference_files(self, teams_csv: Path, memberships_csv: Path) -> None:
        with open(teams_csv, newline="", encoding="utf-8") as f:
            teams = [(r["team_id"], r["team_name"]) for r in csv.DictReader(f)]
        with open(memberships_csv, newline="", encoding="utf-8") as f:
            memberships = [
                (r["user_name"], r["team_id"], r["valid_from"], r["valid_to"] or None) for r in csv.DictReader(f)
            ]
        self.load_reference(teams, memberships)

    def check_reference(self, teams: str = "dim_team", memberships: str = "user_team_history") -> None:
        """Check team reference data; by default the published tables, which every load does first.

        The table names fill the {{teams}} and {{memberships}} placeholders of the SQL file.
        """
        rows: list[list] = []
        for statement in self._render("load/01_check_reference.sql", {"teams": teams, "memberships": memberships}):
            rows = self.query(statement)
        problems = [row[0] for row in rows]
        if problems:
            raise ReferenceDataInvalid(problems)

    @staticmethod
    def _timestamp_literal(iso_text: str) -> str:
        moment = utc(iso_text)
        return "TIMESTAMP '" + moment.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC'"

    # ---- raw layer: stands in for the collector that lands events in Iceberg --------------------------

    def ingest(self, batch_id: str, cluster_id: str, received_at: str, events: list[str]) -> int:
        """Append one closed delivery batch of raw event JSON to the raw table. Returns the number of rows."""
        received = self._timestamp_literal(received_at)
        rows = [
            f"({sql_literal(f'{batch_id}/{number:05d}')}, {sql_literal(batch_id)}, {sql_literal(cluster_id)}, "
            f"{received}, {sql_literal(event)})"
            for number, event in enumerate(events, start=1)
        ]
        header = (
            f"INSERT INTO {self.raw_schema}.query_events "
            "(delivery_id, batch_id, cluster_id, received_at, event_json) VALUES "
        )
        chunk: list[str] = []
        size = 0
        for row in rows:
            if chunk and size + len(row) > MAX_INSERT_CHARS:
                self.query(header + ", ".join(chunk))
                chunk, size = [], 0
            chunk.append(row)
            size += len(row)
        if chunk:
            self.query(header + ", ".join(chunk))
        return len(rows)

    # ---- load -----------------------------------------------------------------------------------------

    def pending_batches(self) -> list[str]:
        """Delivery batches not yet SUCCEEDED, in the order they were received."""
        return [row[0] for row in self.run_file("load/00_pending_batches.sql")]

    def load_pending(self) -> list[str]:
        """Load every pending batch, one after another. Stops at the first batch that fails."""
        loaded = []
        for batch_id in self.pending_batches():
            self.load_batch(batch_id)
            loaded.append(batch_id)
        return loaded

    def load_batch(self, batch_id: str, stop_after: str | None = None) -> None:
        """Load one delivery batch from the raw table into the model.

        The steps are separate Iceberg commits, not one transaction. The batch is marked SUCCEEDED only
        after the last step, and every step is safe to repeat, so a batch interrupted anywhere is simply
        loaded again from the start. `stop_after` ("fact" or "bridge") exists for tests: it simulates a
        crash right after that step.
        """
        self.query(
            "MERGE INTO etl_batch AS t USING (VALUES (?)) AS s(batch_id) ON t.batch_id = s.batch_id "
            "WHEN MATCHED THEN UPDATE SET status = 'STARTED', started_at = current_timestamp, finished_at = NULL, "
            "messages = NULL, queries_new = NULL, queries_already_loaded = NULL, error = NULL "
            "WHEN NOT MATCHED THEN INSERT (batch_id, status, started_at) VALUES (s.batch_id, 'STARTED', current_timestamp)",
            [batch_id],
        )
        try:
            self.check_reference()

            # The raw table is partitioned by day of received_at. Passing the batch's own time range lets
            # every following step read only those partitions instead of the whole raw history.
            first_received, last_received = self.query(
                f"SELECT min(received_at), max(received_at) FROM {self.raw_schema}.query_events WHERE batch_id = ?",
                [batch_id],
            )[0]
            batch = [batch_id, first_received, last_received]

            # Reading layer: validate, then build the typed projection of the batch.
            problems = [(row[0], row[1]) for row in self.run_file("load/10_validate_batch.sql", batch)]
            if problems:
                raise BatchRejected(batch_id, problems)
            self.run_file("load/20_stage_query.sql", batch)
            self.run_file("load/21_stage_query_table.sql", batch)
            self.run_file("load/22_stage_query_content.sql")

            # Nothing has been written to the model yet. A conflicting version stops the batch here.
            conflicts = [(row[0], row[1]) for row in self.run_file("load/23_find_conflicts.sql")]
            if conflicts:
                raise BatchRejected(batch_id, conflicts)
            # "Already loaded" means loaded by ANOTHER batch (a redelivery). Rows written by an interrupted
            # earlier attempt of this same batch count as this batch's own new queries.
            messages, queries, already_loaded = self.query(
                "SELECT count(*), count_if(c.delivery_rank = 1), "
                "count_if(c.delivery_rank = 1 AND f.query_id IS NOT NULL AND f.loaded_batch_id <> ?) "
                "FROM stg_query_content AS c LEFT JOIN fact_query AS f "
                "ON f.cluster_id = c.cluster_id AND f.query_id = c.query_id",
                [batch_id],
            )[0]

            if messages == 0:
                raise BatchRejected(batch_id, [("-", "the raw table has no messages for this batch")])

            # Model: each step only adds what is missing.
            self.run_file("load/30_merge_fact_query.sql")
            if stop_after == "fact":
                raise InjectedFailure("stopped after fact_query")
            self.run_file("load/31_merge_bridge_query_table.sql")
            if stop_after == "bridge":
                raise InjectedFailure("stopped after bridge_query_table")
            self.run_file("load/32_merge_dim_table.sql")

            missing = [row[0] for row in self.run_file("load/40_check_batch_loaded.sql")]
            if missing:
                raise InvariantViolated(missing)
            self.check()
        except InjectedFailure:
            raise  # like a real crash: the batch stays STARTED and is picked up again
        except (BatchRejected, ReferenceDataInvalid, InvariantViolated) as error:
            self.query(
                "UPDATE etl_batch SET status = 'FAILED', finished_at = current_timestamp, error = ? WHERE batch_id = ?",
                [str(error)[:2000], batch_id],
            )
            raise
        self.query(
            "UPDATE etl_batch SET status = 'SUCCEEDED', finished_at = current_timestamp, messages = ?, "
            "queries_new = ?, queries_already_loaded = ? WHERE batch_id = ?",
            [messages, queries - already_loaded, already_loaded, batch_id],
        )

    def check(self) -> None:
        """Verify the model invariants and the reference-data rules; raise on any violation."""
        problems = [row[0] for row in self.run_file("checks/model_invariants.sql")]
        if problems:
            raise InvariantViolated(problems)
        self.check_reference()

    def rebuild(self) -> list[str]:
        """Historical recalculation: rebuild every derived table from the raw events.

        Clears the model tables AND the load log - with the log left in place, every batch would still look
        loaded and nothing would be rebuilt. Raw events and team reference data are kept.
        """
        for table in ("fact_query", "bridge_query_table", "dim_table", "etl_batch"):
            self.query(f"DELETE FROM {table}")
        return self.load_pending()

    # ---- the two questions from the task --------------------------------------------------------------

    def distinct_users(self, cluster_id: str, table: TableName, period_start: str, period_end: str) -> int:
        """Q1. `table` is catalog.schema.table (see table_parts); the period is [period_start, period_end) in UTC."""
        catalog_name, schema_name, table_name = table_parts(table)
        start, end = utc(period_start), utc(period_end)
        rows = self.run_file(
            "queries/q1_distinct_users.sql",
            [cluster_id, catalog_name, schema_name, table_name, start, end, start, end],
        )
        return rows[0][0]

    def team_resources(self, team_id: str, period_start: str, period_end: str) -> TeamResources:
        """Q2. The period is [period_start, period_end) in UTC."""
        rows = self.run_file("queries/q2_team_resources.sql", [team_id, utc(period_start), utc(period_end)])
        return TeamResources(*rows[0])

    def table_read_volume(self, cluster_id: str, period_start: str, period_end: str) -> list[dict]:
        """Q3. Bytes and rows read per table in [period_start, period_end), largest first."""
        start, end = utc(period_start), utc(period_end)
        cursor = self._conn.cursor()
        cursor.execute(self._render("queries/q3_table_read_volume.sql")[0], [cluster_id, start, end, start, end])
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def table_comments(self) -> list[list]:
        """(table_name, comment) of the model tables, read from the catalog."""
        return self.query(
            "SELECT table_name, comment FROM system.metadata.table_comments "
            "WHERE catalog_name = ? AND schema_name = ? AND table_name NOT LIKE 'stg%' AND comment IS NOT NULL ORDER BY 1",
            [CATALOG, self.dwh_schema],
        )

    def table_users_daily(self, cluster_id: str, table: TableName, period_start: str, period_end: str) -> list[list]:
        """Trend for Q1: (query_date, distinct_users, queries) per UTC day that has queries."""
        catalog_name, schema_name, table_name = table_parts(table)
        start, end = utc(period_start), utc(period_end)
        return self.run_file(
            "queries/trend_table_users_daily.sql",
            [cluster_id, catalog_name, schema_name, table_name, start, end, start, end],
        )

    def team_resources_daily(self, team_id: str, period_start: str, period_end: str) -> list[list]:
        """Trend for Q2: one row of resources per UTC day."""
        return self.run_file("queries/trend_team_resources_daily.sql", [team_id, utc(period_start), utc(period_end)])
