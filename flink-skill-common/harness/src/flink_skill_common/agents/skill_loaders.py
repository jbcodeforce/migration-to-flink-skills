"""Agno skill loaders with runtime-specific instruction filtering."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from agno.skills import LocalSkills, Skill
from agno.utils.log import log_warning

from flink_skill_common.skill_adapt import adapt_skill_content


class AgnoAdaptedLocalSkills(LocalSkills):
    """LocalSkills loader that filters SKILL.md to Agno runtime blocks only."""

    def _load_skill_from_folder(self, folder: Path) -> Optional[Skill]:
        skill_md_path = folder / "SKILL.md"
        try:
            raw = skill_md_path.read_text(encoding="utf-8")
            content = adapt_skill_content(raw, "agno")
            frontmatter, instructions = self._parse_skill_md(content)

            name = frontmatter.get("name", folder.name)
            description = frontmatter.get("description", "")

            return Skill(
                name=name,
                description=description,
                instructions=instructions,
                source_path=str(folder),
                scripts=self._discover_scripts(folder),
                references=self._discover_references(folder),
                metadata=frontmatter.get("metadata"),
                license=frontmatter.get("license"),
                compatibility=frontmatter.get("compatibility"),
                allowed_tools=frontmatter.get("allowed-tools"),
            )
        except Exception as exc:
            log_warning(f"Error loading skill from {folder}: {exc}")
            return None
