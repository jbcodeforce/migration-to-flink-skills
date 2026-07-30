from __future__ import annotations

from pathlib import Path

from manifest.manifest import DeployManifest, create_manifest_from_folder

REPO_ROOT = Path(__file__).resolve().parents[3]
MULTI_JOINS = REPO_ROOT / "references" / "flink" / "valid" / "joins" / "multi-joins"


def test_create_manifest_from_multi_joins_folder() -> None:
    assert MULTI_JOINS.is_dir(), f"missing reference folder: {MULTI_JOINS}"

    manifest = create_manifest_from_folder(MULTI_JOINS, write=False)
    payload = manifest.model_dump(exclude_none=True)

    assert payload["deploy_all"] == ["ddl", "pipeline", "data"]
    assert payload["undeploy_all"] == ["data", "pipeline"]
    assert payload["drop_statement_prefix"] == "multi-joins-drop"
    assert "cc-sql-tools" in payload["user_agent"]

    ddl_files = [entry["file"] for entry in payload["groups"]["ddl"]]
    assert ddl_files == [
        "customers/ddl.customers.sql",
        "items/ddl.items.sql",
        "orders/ddl.orders.sql",
        "orders_enriched/ddl.orders_enriched.sql",
    ]
    assert all("/tests/" not in path for path in ddl_files)

    pipeline = payload["groups"]["pipeline"]
    assert len(pipeline) == 1
    assert pipeline[0]["file"] == "orders_enriched/dml.orders_enriched.sql"
    assert pipeline[0]["name"] == "multi-joins-orders-enriched-pipeline-orders-enriched"

    data_files = [entry["file"] for entry in payload["groups"]["data"]]
    assert data_files == [
        "orders_enriched/tests/insert_customers.sql",
        "orders_enriched/tests/insert_items.sql",
        "orders_enriched/tests/insert_orders.sql",
    ]

    assert payload["drop_tables"] == [
        "orders_enriched",
        "orders",
        "items",
        "customers",
    ]

    round_trip = DeployManifest.model_validate(payload)
    assert round_trip.model_dump(exclude_none=True) == payload
