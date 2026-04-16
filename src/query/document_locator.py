"""Smart document page locator for the Bundeshaushalt Q&A system.

Replicates how an experienced budget clerk navigates:
1. ORIENT — identify which PDF document(s) contain the answer
2. LOCATE — find the exact page range for a given EP/Kapitel/Titel
3. CONTEXT — expand the page range slightly for surrounding context

Uses the source_documents table and source_page columns in haushaltsdaten
to build a navigable index of all ingested budget PDFs.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from src.config import config
from src.db.schema import get_connection

logger = logging.getLogger(__name__)

# Years where budgets are split into per-Einzelplan PDFs (epl01.pdf, …)
_PER_EP_YEARS = range(2005, 2012)

# Tables that carry source_pdf / source_page provenance columns
_SOURCE_TABLES = [
    ("haushaltsdaten", True),          # has einzelplan column
    ("personalhaushalt", True),
    ("verpflichtungsermachtigungen", True),
    ("sachverhalte", True),
]


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class DocumentLocation:
    """A located region within a budget PDF."""

    year: int
    version: str
    pdf_path: Path          # Full path to the PDF file
    pdf_filename: str       # Just the filename
    page_start: int         # 1-indexed, inclusive
    page_end: int           # 1-indexed, inclusive
    einzelplan: str | None = None
    kapitel: str | None = None
    titel: str | None = None
    context: str = ""       # Description of what's at this location
    entry_count: int = 0    # How many DB entries are in this range


# ---------------------------------------------------------------------------
# Locator
# ---------------------------------------------------------------------------

class DocumentLocator:
    """Navigate budget PDFs like a clerk with a Table of Contents."""

    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or config.DB_PATH

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        return get_connection(self._db_path)

    # ------------------------------------------------------------------
    # Core locate
    # ------------------------------------------------------------------

    def locate(
        self,
        year: int,
        einzelplan: str | None = None,
        kapitel: str | None = None,
        titel: str | None = None,
        context_pages: int = 2,
    ) -> list[DocumentLocation]:
        """Find PDF page locations for a budget query.

        Works like a clerk:
        1. Find which PDF(s) contain this year's data
        2. Narrow to the Einzelplan section
        3. Narrow to the Kapitel within it
        4. Narrow to specific Titel if given
        5. Add context pages (clerk reads surrounding pages too)
        """
        conn = self._connect()
        try:
            # Build page-count lookup from source_documents
            page_counts = self._page_counts(conn, year)
            if not page_counts:
                logger.warning("No source documents found for year %d", year)
                return []

            # Query every provenance table and aggregate results per PDF
            aggregated: dict[str, _AggRange] = {}
            for table_name, _ in _SOURCE_TABLES:
                self._collect_ranges(
                    conn, table_name, year,
                    einzelplan, kapitel, titel,
                    aggregated,
                )

            if not aggregated:
                logger.info(
                    "No page data found for year=%d ep=%s kap=%s titel=%s",
                    year, einzelplan, kapitel, titel,
                )
                return []

            # Build DocumentLocation objects
            results: list[DocumentLocation] = []
            for pdf_name, agg in sorted(aggregated.items()):
                total_pages = page_counts.get(pdf_name)
                version = self._version_for(conn, year, pdf_name)
                pdf_path = self._resolve_path(conn, year, pdf_name)

                # Apply context expansion
                page_start = max(1, agg.min_page - context_pages)
                page_end = agg.max_page + context_pages
                if total_pages is not None:
                    page_end = min(total_pages, page_end)

                ctx_parts: list[str] = [str(year)]
                if einzelplan:
                    ctx_parts.append(f"EP {einzelplan}")
                if kapitel:
                    ctx_parts.append(f"Kap {kapitel}")
                if titel:
                    ctx_parts.append(f"Titel {titel}")

                results.append(DocumentLocation(
                    year=year,
                    version=version,
                    pdf_path=pdf_path or Path(pdf_name),
                    pdf_filename=pdf_name,
                    page_start=page_start,
                    page_end=page_end,
                    einzelplan=einzelplan,
                    kapitel=kapitel,
                    titel=titel,
                    context=" / ".join(ctx_parts),
                    entry_count=agg.count,
                ))
            return results
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Natural-language locate
    # ------------------------------------------------------------------

    def locate_by_query(self, question: str) -> list[DocumentLocation]:
        """Extract year/EP/Kap/Titel from a natural language question and locate.

        Uses regex patterns to find structural references in the question.
        """
        year = self._extract_year(question)
        if year is None:
            logger.info("No year found in question: %s", question[:80])
            return []

        einzelplan = self._extract_einzelplan(question)
        kapitel = self._extract_kapitel(question)
        titel = self._extract_titel(question)

        logger.info(
            "Extracted from question: year=%s ep=%s kap=%s titel=%s",
            year, einzelplan, kapitel, titel,
        )
        return self.locate(
            year=year,
            einzelplan=einzelplan,
            kapitel=kapitel,
            titel=titel,
        )

    # ------------------------------------------------------------------
    # PDF path resolution
    # ------------------------------------------------------------------

    def get_pdf_path(self, year: int, filename: str) -> Path | None:
        """Resolve a PDF filename to its full path in data/budgets/."""
        budgets_dir = config.DATA_DIR / "budgets" / str(year)
        path = budgets_dir / filename
        return path if path.exists() else None

    # ------------------------------------------------------------------
    # Listing helpers
    # ------------------------------------------------------------------

    def list_available_documents(self, year: int) -> list[dict]:
        """List all source documents for a year with page counts."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT filename, filepath, page_count, version "
                "FROM source_documents WHERE year = ? ORDER BY filename",
                (year,),
            ).fetchall()
            return [
                {
                    "filename": r["filename"],
                    "filepath": r["filepath"],
                    "page_count": r["page_count"],
                    "version": r["version"],
                }
                for r in rows
            ]
        finally:
            conn.close()

    def get_main_budget_pdf(self, year: int) -> Path | None:
        """Find the main comprehensive budget PDF for a year.

        For 2005-2011: returns None (per-EP PDFs, use locate with einzelplan)
        For 2012+: returns the large consolidated PDF
        """
        if year in _PER_EP_YEARS:
            return None

        conn = self._connect()
        try:
            # The main PDF is the one with the highest page count
            row = conn.execute(
                "SELECT filepath FROM source_documents "
                "WHERE year = ? ORDER BY page_count DESC LIMIT 1",
                (year,),
            ).fetchone()
            if row is None:
                return None
            path = Path(row["filepath"])
            return path if path.exists() else None
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @dataclass
    class _AggRange:
        """Mutable aggregation of page range and entry count."""
        min_page: int
        max_page: int
        count: int


    def _collect_ranges(
        self,
        conn: sqlite3.Connection,
        table: str,
        year: int,
        einzelplan: str | None,
        kapitel: str | None,
        titel: str | None,
        out: dict[str, _AggRange],
    ) -> None:
        """Query *table* for matching rows and merge into *out*."""
        conditions = ["year = ?", "source_pdf IS NOT NULL", "source_page IS NOT NULL"]
        params: list = [year]

        if einzelplan:
            conditions.append("einzelplan = ?")
            params.append(einzelplan)
        if kapitel:
            conditions.append("kapitel = ?")
            params.append(kapitel)
        # titel column may not exist in all tables; check safely
        if titel:
            try:
                conn.execute(f"SELECT titel FROM {table} LIMIT 0")
            except sqlite3.OperationalError:
                return
            conditions.append("titel = ?")
            params.append(titel)

        where = " AND ".join(conditions)
        sql = (
            f"SELECT source_pdf, MIN(source_page) AS p_min, "
            f"MAX(source_page) AS p_max, COUNT(*) AS cnt "
            f"FROM {table} WHERE {where} GROUP BY source_pdf"
        )

        try:
            rows = conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            logger.debug("Skipping table %s: %s", table, exc)
            return

        for row in rows:
            pdf = row["source_pdf"]
            p_min, p_max, cnt = row["p_min"], row["p_max"], row["cnt"]
            if pdf in out:
                existing = out[pdf]
                existing.min_page = min(existing.min_page, p_min)
                existing.max_page = max(existing.max_page, p_max)
                existing.count += cnt
            else:
                out[pdf] = DocumentLocator._AggRange(
                    min_page=p_min, max_page=p_max, count=cnt,
                )

    def _page_counts(
        self, conn: sqlite3.Connection, year: int,
    ) -> dict[str, int | None]:
        """Return {filename: page_count} for all source docs in *year*."""
        rows = conn.execute(
            "SELECT filename, page_count FROM source_documents WHERE year = ?",
            (year,),
        ).fetchall()
        return {r["filename"]: r["page_count"] for r in rows}

    def _version_for(
        self, conn: sqlite3.Connection, year: int, filename: str,
    ) -> str:
        """Look up the version string for a source document."""
        row = conn.execute(
            "SELECT version FROM source_documents "
            "WHERE year = ? AND filename = ? LIMIT 1",
            (year, filename),
        ).fetchone()
        return row["version"] if row else "soll"

    def _resolve_path(
        self, conn: sqlite3.Connection, year: int, filename: str,
    ) -> Path | None:
        """Resolve filepath from source_documents, falling back to convention."""
        row = conn.execute(
            "SELECT filepath FROM source_documents "
            "WHERE year = ? AND filename = ? LIMIT 1",
            (year, filename),
        ).fetchone()
        if row and Path(row["filepath"]).exists():
            return Path(row["filepath"])
        # Fallback: conventional path
        return self.get_pdf_path(year, filename)

    # ------------------------------------------------------------------
    # Regex extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_year(text: str) -> int | None:
        # "im Jahr 2020", "für 2020", "in 2020", "von 2020", "auf 2020", bare "2020"
        m = re.search(
            r'(?:Jahr|für|in|von|auf|aus|ab|bis|seit)\s+(\d{4})', text,
        )
        if m:
            return int(m.group(1))
        # Bare four-digit year between 2000 and 2099
        m = re.search(r'\b(20\d{2})\b', text)
        return int(m.group(1)) if m else None

    @staticmethod
    def _extract_einzelplan(text: str) -> str | None:
        # "Einzelplan 14", "EP 06", "Epl. 14", "Epl 6"
        m = re.search(r'(?:Einzelplan|EP|Epl\.?)\s+(\d{1,2})', text, re.IGNORECASE)
        if m:
            return m.group(1).zfill(2)
        return None

    @staticmethod
    def _extract_kapitel(text: str) -> str | None:
        # "Kapitel 1403", "Kap. 0622", "Kap 0455"
        m = re.search(r'(?:Kapitel|Kap\.?)\s+(\d{4})', text, re.IGNORECASE)
        return m.group(1) if m else None

    @staticmethod
    def _extract_titel(text: str) -> str | None:
        # "Titel 531 01", "Titel F 811 01"
        m = re.search(r'Titel\s+F?\s*(\d{3}\s+\d{2})', text, re.IGNORECASE)
        return m.group(1) if m else None


# _AggRange needs to be accessible as a nested reference; re-export at module level
_AggRange = DocumentLocator._AggRange
