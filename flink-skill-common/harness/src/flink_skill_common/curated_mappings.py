"""Load curated Flink goldens for migrate-agent few-shot context.

Shared by ksql-to-flink (and later spark-to-flink). Curated pairs live under
``references/flink/valid/{category}/{stem}/``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from flink_skill_common.flink_sql_compare import FLINK_VALID_ROOT

KNOWN_CATEGORIES: frozenset[str] = frozenset(
    {
        "joins",
        "routing",
        "aggregations",
        "windows",
        "transformations",
        "misc",
    }
)

ClassifyFn = Callable[[str, str], str]


@dataclass(frozen=True)
class Exemplar:
    category: str
    stem: str
    table_name: str
    ddl: str
    dml: str


def infer_category_from_path(src_file: Path | str | None) -> str | None:
    """Return known category if any path part matches (e.g. .../joins/foo.ksql)."""
    if src_file is None:
        return None
    parts = {p.lower() for p in Path(src_file).parts}
    for cat in KNOWN_CATEGORIES:
        if cat in parts:
            return cat
    return None


def exact_pipeline_dir(
    category: str,
    stem: str,
    *,
    flink_valid_root: Path | None = None,
) -> Path | None:
    root = flink_valid_root or FLINK_VALID_ROOT
    path = root / category / stem
    return path if path.is_dir() else None


def load_table_sql(pipeline_dir: Path, table_name: str) -> dict[str, str] | None:
    """Find ddl.{table}.sql / dml.{table}.sql under pipeline (flat or sql-scripts/)."""
    if not pipeline_dir.is_dir():
        return None
    ddl_name = f"ddl.{table_name}.sql".lower()
    dml_name = f"dml.{table_name}.sql".lower()
    ddl: str | None = None
    dml: str | None = None
    for path in sorted(pipeline_dir.rglob("*.sql")):
        if "tests" in path.parts:
            continue
        name = path.name.lower()
        if name == ddl_name:
            ddl = path.read_text()
        elif name == dml_name:
            dml = path.read_text()
    if ddl is None and dml is None:
        return None
    return {"ddl": ddl or "", "dml": dml or ""}


def _pipeline_has_sql(pipeline_dir: Path) -> bool:
    for path in pipeline_dir.rglob("*.sql"):
        if "tests" in path.parts:
            continue
        if path.name.lower().startswith(("ddl.", "dml.")):
            return True
    return False


def _preferred_table_in_pipeline(pipeline_dir: Path) -> str | None:
    """Prefer a table that has DML; else first DDL table name."""
    dml_tables: list[str] = []
    ddl_tables: list[str] = []
    for path in sorted(pipeline_dir.rglob("*.sql")):
        if "tests" in path.parts:
            continue
        name = path.name
        lower = name.lower()
        if lower.startswith("dml.") and lower.endswith(".sql"):
            dml_tables.append(name[4:-4])
        elif lower.startswith("ddl.") and lower.endswith(".sql"):
            ddl_tables.append(name[4:-4])
    if dml_tables:
        return dml_tables[0]
    if ddl_tables:
        return ddl_tables[0]
    return None


def list_category_exemplars(
    category: str,
    *,
    exclude_stem: str | None = None,
    limit: int = 2,
    flink_valid_root: Path | None = None,
) -> list[Exemplar]:
    """Other pipelines in category with at least one ddl/dml (cap ``limit``)."""
    root = flink_valid_root or FLINK_VALID_ROOT
    cat_dir = root / category
    if not cat_dir.is_dir():
        return []

    exemplars: list[Exemplar] = []
    for child in sorted(cat_dir.iterdir()):
        if not child.is_dir():
            continue
        if exclude_stem and child.name == exclude_stem:
            continue
        if not _pipeline_has_sql(child):
            continue
        table = _preferred_table_in_pipeline(child)
        if not table:
            continue
        sql = load_table_sql(child, table)
        if not sql:
            continue
        exemplars.append(
            Exemplar(
                category=category,
                stem=child.name,
                table_name=table,
                ddl=sql["ddl"],
                dml=sql["dml"],
            )
        )
        if len(exemplars) >= limit:
            break
    return exemplars


def _format_sql_pair(*, label: str, ddl: str, dml: str) -> str:
    parts = [f"### {label}"]
    if ddl.strip():
        parts.append(f"DDL:\n```sql\n{ddl.strip()}\n```")
    if dml.strip():
        parts.append(f"DML:\n```sql\n{dml.strip()}\n```")
    return "\n\n".join(parts)


def _default_llm_classify(ksql: str, src_ksql: str) -> str:
    from flink_skill_common.agents.factory import (
        build_skilled_agent,
        make_openai_model,
        resolve_llm_model,
        run_agent_process_response,
    )
    from flink_skill_common.config import llm_api_key, llm_base_url

    cats = ", ".join(sorted(KNOWN_CATEGORIES))
    prompt = (
        "Classify this ksqlDB statement into exactly one category token.\n"
        f"Allowed tokens: {cats}, unknown\n"
        "Reply with only the token, nothing else.\n\n"
        f"Statement:\n```sql\n{ksql.strip()[:4000]}\n```\n\n"
        f"Full script excerpt:\n```sql\n{src_ksql.strip()[:2000]}\n```"
    )
    agent = build_skilled_agent(
        name="KsqlCategoryClassifier",
        skill_dirs=[],
        instructions=[
            "Return only one category token from the allowed list (or unknown).",
        ],
        model=make_openai_model(
            base_url=llm_base_url(),
            api_key=llm_api_key(),
            model_id=resolve_llm_model(),
        ),
        tools=[],
    )
    raw = run_agent_process_response(agent, prompt).strip().lower()
    token = re.split(r"\s+|[,:;]", raw, maxsplit=1)[0].strip("`\"'")
    return token if token in KNOWN_CATEGORIES else "unknown"


def classify_ksql_category(
    ksql: str,
    src_ksql: str,
    *,
    classify_fn: ClassifyFn | None = None,
) -> str:
    """LLM (or injectable) category classification; returns known category or unknown."""
    fn = classify_fn or _default_llm_classify
    token = fn(ksql, src_ksql).strip().lower()
    return token if token in KNOWN_CATEGORIES else "unknown"


def build_curated_context_block(
    *,
    table_name: str,
    ksql: str,
    src_ksql: str,
    src_file: Path | str | None = None,
    category: str | None = None,
    flink_valid_root: Path | None = None,
    classify_fn: ClassifyFn | None = None,
    exemplar_limit: int = 2,
) -> str:
    """Markdown block with curated Flink SQL, or empty if nothing curated."""
    root = flink_valid_root or FLINK_VALID_ROOT
    stem = Path(src_file).stem if src_file is not None else None
    resolved_category = category or infer_category_from_path(src_file)

    if resolved_category is None:
        resolved_category = classify_ksql_category(
            ksql, src_ksql, classify_fn=classify_fn
        )
        if resolved_category == "unknown":
            return ""

    header = (
        "## Curated Flink reference (same pattern — follow shape, adapt names/columns)\n"
        "Match PRIMARY KEY / join / changelog shape from the reference. "
        "Do not copy table names blindly; serdes may differ.\n"
    )

    if stem:
        pipeline = exact_pipeline_dir(
            resolved_category, stem, flink_valid_root=root
        )
        if pipeline is not None:
            sql = load_table_sql(pipeline, table_name)
            if sql is not None:
                body = _format_sql_pair(
                    label=f"Exact curated pipeline `{resolved_category}/{stem}` "
                    f"table `{table_name}`",
                    ddl=sql["ddl"],
                    dml=sql["dml"],
                )
                return f"{header}\n{body}"

    exemplars = list_category_exemplars(
        resolved_category,
        exclude_stem=stem,
        limit=exemplar_limit,
        flink_valid_root=root,
    )
    if not exemplars:
        return ""

    chunks = [
        _format_sql_pair(
            label=f"Category exemplar `{ex.category}/{ex.stem}` "
            f"(table `{ex.table_name}`)",
            ddl=ex.ddl,
            dml=ex.dml,
        )
        for ex in exemplars
    ]
    return f"{header}\n" + "\n\n".join(chunks)


__all__ = [
    "KNOWN_CATEGORIES",
    "Exemplar",
    "build_curated_context_block",
    "classify_ksql_category",
    "exact_pipeline_dir",
    "infer_category_from_path",
    "list_category_exemplars",
    "load_table_sql",
]
