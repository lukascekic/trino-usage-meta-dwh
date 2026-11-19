"""Restart from persisted state after lost acknowledgements and interrupted rebuilds."""

from __future__ import annotations

import pytest

from pipeline.warehouse import InjectedFailure, Warehouse
from tests.support import batch_status, prepare_sample, snapshot


def reconnect(warehouse):
    # Same isolated schemas, new client; recovery must not depend on Python object state.
    return Warehouse(prefix=warehouse.dwh_schema.removesuffix("meta_dwh"))


@pytest.mark.parametrize("point", ["fact", "dimension", "success-marker"])
def test_lost_commit_acknowledgement_recovers_with_a_new_client(warehouse, sample, monkeypatch, point):
    prepare_sample(warehouse)
    original_run = warehouse.run_file
    original_query = warehouse.query
    tripped = []

    def run_then_disconnect(path, params=None):
        rows = original_run(path, params)
        target = "load/30_merge_fact_query.sql" if point == "fact" else "load/32_merge_dim_table.sql"
        if path == target:
            tripped.append(path)
            raise ConnectionError("commit succeeded but its acknowledgement was lost")
        return rows

    def query_then_disconnect(sql, params=None):
        rows = original_query(sql, params)
        if sql.startswith("UPDATE etl_batch SET status = 'SUCCEEDED'"):
            tripped.append("success-marker")
            raise ConnectionError("status committed but its acknowledgement was lost")
        return rows

    with monkeypatch.context() as patch:
        if point == "success-marker":
            patch.setattr(warehouse, "query", query_then_disconnect)
        else:
            patch.setattr(warehouse, "run_file", run_then_disconnect)
        with pytest.raises(ConnectionError):
            warehouse.load_batch("batch-001")
    assert len(tripped) == 1
    assert batch_status(warehouse)["batch-001"] == ("SUCCEEDED" if point == "success-marker" else "STARTED")

    fresh = reconnect(warehouse)
    fresh.load_pending()
    assert snapshot(fresh) == snapshot(sample)
    assert fresh.pending_batches() == []
    assert set(batch_status(fresh).values()) == {"SUCCEEDED"}
    fresh.check()


def test_retry_rebuilds_staging_after_another_batch_overwrites_it(warehouse, sample):
    prepare_sample(warehouse)
    warehouse.load_batch("batch-001")
    with pytest.raises(InjectedFailure):
        warehouse.load_batch("batch-002", stop_after="fact")
    # Serial loads may be explicitly requested in any order. This overwrites every staging table.
    warehouse.load_batch("batch-003")
    assert batch_status(warehouse)["batch-002"] == "STARTED"
    fresh = reconnect(warehouse)
    assert fresh.load_pending() == ["batch-002"]
    assert snapshot(fresh) == snapshot(sample)


@pytest.mark.parametrize("deleted_table", ["fact_query", "bridge_query_table", "dim_table", "etl_batch"])
def test_rebuild_can_be_retried_after_each_destructive_step(warehouse, monkeypatch, deleted_table):
    prepare_sample(warehouse)
    warehouse.load_pending()
    before = snapshot(warehouse)
    raw = warehouse.query(f"SELECT * FROM {warehouse.raw_schema}.query_events ORDER BY delivery_id")
    teams = warehouse.query("SELECT * FROM dim_team ORDER BY team_id")
    memberships = warehouse.query("SELECT * FROM user_team_history ORDER BY user_name, valid_from")
    original_query = warehouse.query
    tripped = []

    def delete_then_disconnect(sql, params=None):
        result = original_query(sql, params)
        if sql == f"DELETE FROM {deleted_table}":
            tripped.append(deleted_table)
            raise ConnectionError("rebuild interrupted after a committed delete")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(warehouse, "query", delete_then_disconnect)
        with pytest.raises(ConnectionError):
            warehouse.rebuild()
    assert tripped == [deleted_table]
    fresh = reconnect(warehouse)
    assert fresh.rebuild() == ["batch-001", "batch-002", "batch-003"]
    assert snapshot(fresh) == before
    assert fresh.query(f"SELECT * FROM {fresh.raw_schema}.query_events ORDER BY delivery_id") == raw
    assert fresh.query("SELECT * FROM dim_team ORDER BY team_id") == teams
    assert fresh.query("SELECT * FROM user_team_history ORDER BY user_name, valid_from") == memberships
    assert set(batch_status(fresh).values()) == {"SUCCEEDED"}


def test_rebuild_removes_obsolete_derived_rows(warehouse, sample):
    prepare_sample(warehouse)
    warehouse.load_pending()
    warehouse.query(
        "INSERT INTO dim_table SELECT cluster_id, catalog_name, schema_name, 'obsolete', first_seen_date, last_seen_date "
        "FROM dim_table WHERE catalog_name = 'lake' AND schema_name = 'sales' AND table_name = 'orders'"
    )
    warehouse.query(
        "INSERT INTO bridge_query_table "
        "(cluster_id, query_id, catalog_name, schema_name, table_name, access_type, is_directly_referenced, query_date) "
        "SELECT cluster_id, query_id, catalog_name, schema_name, 'obsolete', access_type, is_directly_referenced, query_date "
        "FROM bridge_query_table WHERE query_id = '20261101_100000_00001_demo1' AND table_name = 'orders'"
    )
    assert warehouse.query("SELECT count(*) FROM dim_table WHERE table_name = 'obsolete'") == [[1]]
    assert warehouse.query("SELECT count(*) FROM bridge_query_table WHERE table_name = 'obsolete'") == [[1]]
    assert snapshot(warehouse) != snapshot(sample)
    warehouse.rebuild()
    assert snapshot(warehouse) == snapshot(sample)
