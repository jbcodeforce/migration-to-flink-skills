"""Unit tests for curated Flink mapping loader."""

from __future__ import annotations

from pathlib import Path

from flink_skill_common.curated_mappings import (
    build_curated_context_block,
    exact_pipeline_dir,
    infer_category_from_path,
    list_category_exemplars,
    load_table_sql,
)
from flink_skill_common.flink_sql_compare import FLINK_VALID_ROOT


def test_infer_category_from_path_joins():
    path = Path("/repo/references/ksql/sources/joins/multi-joins.ksql")
    assert infer_category_from_path(path) == "joins"


def test_infer_category_from_path_unknown():
    assert infer_category_from_path(Path("/tmp/customer/pipeline.ksql")) is None
    assert infer_category_from_path(None) is None


def test_exact_pipeline_dir_multi_joins():
    pipeline = exact_pipeline_dir("joins", "multi-joins", flink_valid_root=FLINK_VALID_ROOT)
    assert pipeline is not None
    assert pipeline.name == "multi-joins"


def test_load_table_sql_orders_enriched():
    pipeline = exact_pipeline_dir("joins", "multi-joins", flink_valid_root=FLINK_VALID_ROOT)
    assert pipeline is not None
    sql = load_table_sql(pipeline, "orders_enriched")
    assert sql is not None
    assert "CREATE TABLE" in sql["ddl"]
    assert "INSERT INTO orders_enriched" in sql["dml"]
    assert "PRIMARY KEY" in sql["ddl"]


def test_load_table_sql_sql_scripts_layout():
    pipeline = exact_pipeline_dir("joins", "stream_stream", flink_valid_root=FLINK_VALID_ROOT)
    assert pipeline is not None
    sql = load_table_sql(pipeline, "shipped_orders")
    assert sql is not None
    assert sql["ddl"].strip()
    assert sql["dml"].strip()


def test_list_category_exemplars_excludes_stem_and_limits():
    exemplars = list_category_exemplars(
        "joins",
        exclude_stem="multi-joins",
        limit=1,
        flink_valid_root=FLINK_VALID_ROOT,
    )
    assert len(exemplars) == 1
    assert exemplars[0].stem != "multi-joins"
    assert exemplars[0].ddl or exemplars[0].dml


def test_build_curated_context_exact_match():
    src = Path("references/ksql/sources/joins/multi-joins.ksql")
    block = build_curated_context_block(
        table_name="orders_enriched",
        ksql="CREATE STREAM orders_enriched AS SELECT 1;",
        src_ksql="CREATE STREAM orders_enriched AS SELECT 1;",
        src_file=src,
        flink_valid_root=FLINK_VALID_ROOT,
        classify_fn=lambda *_: "unknown",
    )
    assert "Curated Flink reference" in block
    assert "Exact curated pipeline" in block
    assert "orders_enriched" in block
    assert "PRIMARY KEY" in block


def test_build_curated_context_exemplars_when_no_exact():
    src = Path("references/ksql/sources/joins/not-a-real-pipeline.ksql")
    block = build_curated_context_block(
        table_name="whatever",
        ksql="CREATE STREAM x AS SELECT 1;",
        src_ksql="CREATE STREAM x AS SELECT 1;",
        src_file=src,
        flink_valid_root=FLINK_VALID_ROOT,
        exemplar_limit=1,
        classify_fn=lambda *_: "unknown",
    )
    assert "Curated Flink reference" in block
    assert "Category exemplar" in block


def test_build_curated_context_empty_when_unknown(tmp_path: Path):
    block = build_curated_context_block(
        table_name="t",
        ksql="CREATE STREAM t (id INT);",
        src_ksql="CREATE STREAM t (id INT);",
        src_file=tmp_path / "orphan.ksql",
        flink_valid_root=FLINK_VALID_ROOT,
        classify_fn=lambda *_: "unknown",
    )
    assert block == ""


def test_build_curated_context_uses_classify_fallback(tmp_path: Path):
    block = build_curated_context_block(
        table_name="shipped_orders",
        ksql="CREATE STREAM shipped_orders AS SELECT 1;",
        src_ksql="CREATE STREAM shipped_orders AS SELECT 1;",
        src_file=tmp_path / "orphan.ksql",
        flink_valid_root=FLINK_VALID_ROOT,
        classify_fn=lambda *_: "joins",
        exemplar_limit=1,
    )
    assert "Curated Flink reference" in block
    assert "Category exemplar" in block
