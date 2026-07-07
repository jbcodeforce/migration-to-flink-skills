"""Filter skill markdown by runtime target (Agno, Cursor, Claude)."""

from __future__ import annotations

import re

RUNTIME_BLOCK_RE = re.compile(
    r"<!-- runtime:([^>]+) -->\s*(.*?)\s*<!-- /runtime:\1 -->",
    re.DOTALL,
)

FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n?", re.DOTALL)


def parse_skill_name(skill_md: str, fallback: str) -> str:
    match = FRONTMATTER_RE.match(skill_md)
    if not match:
        return fallback
    for line in match.group(1).splitlines():
        if line.strip().startswith("name:"):
            value = line.split(":", 1)[1].strip().strip("'\"")
            if value:
                return value
    return fallback


def adapt_skill_content(content: str, target: str) -> str:
    """Keep only runtime blocks matching target; strip others."""

    def _replace_block(match: re.Match[str]) -> str:
        runtimes = [runtime.strip() for runtime in match.group(1).split(",")]
        if target in runtimes:
            return match.group(2).strip()
        return ""

    adapted = RUNTIME_BLOCK_RE.sub(_replace_block, content)
    adapted = re.sub(r"\n{3,}", "\n\n", adapted)
    return adapted
