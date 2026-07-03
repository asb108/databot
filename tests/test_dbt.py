"""Tests for the dbt metadata tool."""

from __future__ import annotations

import json

import pytest

from databot.tools.dbt import DbtTool

_MANIFEST = {
    "nodes": {
        "model.shop.stg_orders": {
            "resource_type": "model",
            "name": "stg_orders",
            "schema": "staging",
            "description": "Cleaned orders",
            "config": {"materialized": "view"},
            "tags": ["staging"],
            "columns": {
                "order_id": {"data_type": "int", "description": "PK"},
                "amount": {"data_type": "decimal", "description": ""},
            },
            "depends_on": {"nodes": ["source.shop.raw.orders"]},
        },
        "model.shop.fct_orders": {
            "resource_type": "model",
            "name": "fct_orders",
            "schema": "marts",
            "description": "Order facts",
            "config": {"materialized": "table"},
            "columns": {},
            "depends_on": {"nodes": ["model.shop.stg_orders"]},
        },
        "test.shop.not_null_stg_orders_order_id": {
            "resource_type": "test",
            "name": "not_null_stg_orders_order_id",
            "depends_on": {"nodes": ["model.shop.stg_orders"]},
        },
    },
    "sources": {
        "source.shop.raw.orders": {
            "resource_type": "source",
            "source_name": "raw",
            "name": "orders",
            "schema": "raw",
        }
    },
    "child_map": {
        "model.shop.stg_orders": ["model.shop.fct_orders", "test.shop.not_null_stg_orders_order_id"],
        "model.shop.fct_orders": [],
    },
}


@pytest.fixture
def manifest_file(tmp_path):
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(_MANIFEST))
    return str(p)


class TestDbtTool:
    async def test_missing_manifest(self, tmp_path):
        tool = DbtTool(manifest_path=str(tmp_path / "nope.json"))
        out = await tool.execute(action="list_models")
        assert "not found" in out

    async def test_list_models(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="list_models")
        assert "stg_orders" in out and "fct_orders" in out
        assert "2 dbt models" in out

    async def test_get_model(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="get_model", name="stg_orders")
        assert "Cleaned orders" in out
        assert "order_id" in out and "amount" in out
        assert "staging" in out  # tag
        assert "orders (source)" in out  # upstream dependency rendered

    async def test_get_model_unknown(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="get_model", name="does_not_exist")
        assert "No dbt model" in out

    async def test_lineage(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="lineage", name="stg_orders")
        # upstream source, downstream fct + test
        assert "orders (source)" in out
        assert "fct_orders (model)" in out

    async def test_list_tests(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="list_tests")
        assert "not_null_stg_orders_order_id" in out

    async def test_list_sources(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="list_sources")
        assert "raw" in out and "orders" in out

    async def test_search(self, manifest_file):
        tool = DbtTool(manifest_path=manifest_file)
        out = await tool.execute(action="search", name="fct")
        assert "fct_orders" in out
        assert "stg_orders" not in out

    async def test_project_dir_resolution(self, tmp_path):
        (tmp_path / "target").mkdir()
        (tmp_path / "target" / "manifest.json").write_text(json.dumps(_MANIFEST))
        tool = DbtTool(project_dir=str(tmp_path))
        out = await tool.execute(action="list_models")
        assert "stg_orders" in out
