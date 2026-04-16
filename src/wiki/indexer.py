"""Wiki index and log manager for the Bundeshaushalt Q&A system.

Maintains ``wiki/index.md`` (catalog of all pages) and ``wiki/log.md``
(chronological activity log), following the conventions in SCHEMA.md.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

from src.config import config

logger = logging.getLogger(__name__)


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------------------
# Simple YAML frontmatter parser (no external dependency)
# ---------------------------------------------------------------------------

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---", re.DOTALL)


def _parse_frontmatter(text: str) -> dict[str, str]:
    """Extract a flat dict from YAML frontmatter.

    Only handles simple ``key: value`` and ``key: [a, b, c]`` lines — enough
    for our own pages.  Returns an empty dict if no frontmatter is found.
    """
    m = _FM_RE.match(text)
    if not m:
        return {}
    result: dict[str, str] = {}
    for line in m.group(1).splitlines():
        line = line.strip()
        if ":" not in line or line.startswith("#"):
            continue
        key, _, val = line.partition(":")
        result[key.strip()] = val.strip().strip('"').strip("'")
    return result


# ---------------------------------------------------------------------------
# WikiIndexer
# ---------------------------------------------------------------------------


class WikiIndexer:
    """Scan wiki pages and maintain *index.md* and *log.md*."""

    def __init__(self, wiki_dir: Path | None = None):
        self.wiki_dir = Path(wiki_dir or config.WIKI_DIR)

    # ------------------------------------------------------------------
    # Index
    # ------------------------------------------------------------------

    def rebuild_index(self) -> Path:
        """Scan all wiki pages and write a fresh ``index.md``."""
        entities: list[dict] = []
        concepts: list[dict] = []
        analyses: list[dict] = []

        for md_path in sorted(self.wiki_dir.rglob("*.md")):
            # Skip the special pages themselves
            if md_path.name in ("index.md", "log.md"):
                continue

            try:
                text = md_path.read_text(encoding="utf-8")
            except Exception as exc:
                logger.warning("Could not read %s: %s", md_path, exc)
                continue

            fm = _parse_frontmatter(text)
            title = fm.get("title", md_path.stem)
            page_type = fm.get("type", "")
            years = fm.get("years", "")
            tags = fm.get("tags", "")

            rel = md_path.relative_to(self.wiki_dir).as_posix()

            entry = {
                "rel": rel,
                "name": md_path.stem,
                "title": title,
                "years": years,
                "tags": tags,
            }

            if "entities" in rel:
                entities.append(entry)
            elif "concepts" in rel:
                concepts.append(entry)
            elif "analyses" in rel:
                analyses.append(entry)
            else:
                # Anything else goes under analyses as a catch-all
                analyses.append(entry)

        # -- Render index --------------------------------------------------
        lines: list[str] = [
            "---",
            f'title: "Wiki-Index"',
            'type: "summary"',
            f'last_updated: "{_today()}"',
            "---",
            "",
        ]

        # Entities
        lines += [
            "## Einzelpläne und Entitäten",
            "",
            "| Seite | Titel | Jahre |",
            "|-------|-------|-------|",
        ]
        for e in entities:
            lines.append(f"| [{e['name']}]({e['rel']}) | {e['title']} | {e['years']} |")
        lines.append("")

        # Concepts
        lines += [
            "## Konzepte",
            "",
            "| Seite | Kurzbeschreibung |",
            "|-------|-----------------|",
        ]
        for c in concepts:
            lines.append(f"| [{c['name']}]({c['rel']}) | {c['title']} |")
        lines.append("")

        # Analyses
        lines += [
            "## Analysen",
            "",
            "| Seite | Datum | Thema |",
            "|-------|-------|-------|",
        ]
        for a in analyses:
            # Extract date from name like 2026-04-14-slug
            date_match = re.match(r"(\d{4}-\d{2}-\d{2})", a["name"])
            date_str = date_match.group(1) if date_match else "–"
            lines.append(f"| [{a['name']}]({a['rel']}) | {date_str} | {a['title']} |")
        lines.append("")

        index_path = self.wiki_dir / "index.md"
        index_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Rebuilt index.md – %d entities, %d concepts, %d analyses",
                     len(entities), len(concepts), len(analyses))
        return index_path

    # ------------------------------------------------------------------
    # Log
    # ------------------------------------------------------------------

    def append_log(self, action: str, details: str) -> None:
        """Append a timestamped entry to ``log.md``.

        Creates the file with frontmatter if it doesn't exist yet.
        """
        log_path = self.wiki_dir / "log.md"

        if not log_path.exists():
            header = "\n".join([
                "---",
                'title: "Aktivitätsprotokoll"',
                'type: "summary"',
                f'last_updated: "{_today()}"',
                "---",
                "",
                "## Protokoll",
                "",
                "| Zeitstempel | Aktion | Details |",
                "|-------------|--------|---------|",
                "",
            ])
            log_path.write_text(header, encoding="utf-8")

        entry = f"| {_now()} | {action} | {details} |\n"
        with log_path.open("a", encoding="utf-8") as f:
            f.write(entry)

        logger.debug("log.md ← %s | %s", action, details)


# ---------------------------------------------------------------------------
# Standalone smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    indexer = WikiIndexer()
    idx = indexer.rebuild_index()
    print(f"Index written to {idx}")
    print(idx.read_text(encoding="utf-8")[:1000])

    indexer.append_log("test", "Standalone smoke test run")
    print("\nlog.md tail:")
    log_text = (indexer.wiki_dir / "log.md").read_text(encoding="utf-8")
    for line in log_text.splitlines()[-5:]:
        print(f"  {line}")
