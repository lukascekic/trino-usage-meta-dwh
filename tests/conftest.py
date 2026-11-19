"""Test fixtures. Every test gets its own pair of Iceberg schemas on the running Trino, so tests are independent."""

from __future__ import annotations

import re
import uuid

import pytest

from pipeline import demo
from pipeline.warehouse import Warehouse


def _fresh_warehouse(name: str) -> Warehouse:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:30]
    warehouse = Warehouse(prefix=f"t_{slug}_{uuid.uuid4().hex[:6]}_")
    warehouse.init()
    return warehouse


@pytest.fixture(scope="session", autouse=True)
def trino_is_ready():
    Warehouse().wait_until_ready()


@pytest.fixture
def warehouse(request):
    """An empty Meta DWH with the default teams loaded."""
    wh = _fresh_warehouse(request.node.name)
    yield wh
    wh.drop()


@pytest.fixture(scope="module")
def sample(request):
    """A Meta DWH loaded with the three sample delivery batches from fixtures/events/."""
    wh = _fresh_warehouse(request.module.__name__)
    demo.load_sample(wh)
    yield wh
    wh.drop()
