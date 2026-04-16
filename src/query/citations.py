"""Structured citation system for grounded budget answers.

Every answer carries citations linking to exact PDF pages,
enabling teams to verify data by opening the source document.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Citation:
    """A traceable reference to a source document location."""

    source_pdf: str          # PDF filename, e.g. "Epl_Gesamt_mit_HG_und_Vorspann.pdf"
    page_number: int         # 1-indexed page number
    year: int
    einzelplan: str | None = None
    kapitel: str | None = None
    titel: str | None = None
    excerpt: str = ""        # Brief text excerpt from the page
    confidence: str = "high"  # high/medium/low
    path: str = ""           # Relative path: "data/budgets/2020/Epl_Gesamt..."

    def to_display(self) -> str:
        """Format for display: 📄 Bundeshaushalt-2020.pdf, S. 142 (Kap 1403)"""
        parts = [f"\U0001f4c4 {self.source_pdf}, S. {self.page_number}"]
        if self.kapitel:
            parts.append(f"Kap {self.kapitel}")
        if self.titel:
            parts.append(f"Titel {self.titel}")
        return " | ".join(parts)

    def to_dict(self) -> dict:
        """Serialize for JSON output."""
        return {
            "source_pdf": self.source_pdf,
            "page_number": self.page_number,
            "year": self.year,
            "einzelplan": self.einzelplan,
            "kapitel": self.kapitel,
            "titel": self.titel,
            "excerpt": self.excerpt,
            "path": self.path,
        }


def extract_citations_from_sql(
    sql_result_rows: list, columns: list[str]
) -> list[Citation]:
    """Extract citations from SQL query results that include source columns.

    When the SQL query includes ``source_pdf`` and ``source_page`` in the
    SELECT, we can create a citation for each unique (pdf, page) pair.
    """
    citations: list[Citation] = []

    # Find column indices
    pdf_idx: int | None = None
    page_idx: int | None = None
    year_idx: int | None = None
    ep_idx: int | None = None
    kap_idx: int | None = None
    titel_idx: int | None = None

    for i, col in enumerate(columns):
        if col == "source_pdf":
            pdf_idx = i
        elif col == "source_page":
            page_idx = i
        elif col == "year":
            year_idx = i
        elif col == "einzelplan":
            ep_idx = i
        elif col == "kapitel":
            kap_idx = i
        elif col == "titel":
            titel_idx = i

    if pdf_idx is None or page_idx is None:
        return []

    seen: set[tuple[str, int]] = set()
    for row in sql_result_rows:
        pdf = row[pdf_idx]
        page = row[page_idx]
        if not pdf or not page:
            continue
        key = (str(pdf), int(page))
        if key in seen:
            continue
        seen.add(key)

        citations.append(
            Citation(
                source_pdf=str(pdf),
                page_number=int(page),
                year=int(row[year_idx]) if year_idx is not None and row[year_idx] else 0,
                einzelplan=str(row[ep_idx]) if ep_idx is not None and row[ep_idx] else None,
                kapitel=str(row[kap_idx]) if kap_idx is not None and row[kap_idx] else None,
                titel=str(row[titel_idx]) if titel_idx is not None and row[titel_idx] else None,
            )
        )

    return citations[:10]  # Cap at 10 citations per answer


def extract_citations_from_scan(scan_result) -> list[Citation]:
    """Extract citations from a *PageScanResult*."""
    citations: list[Citation] = []
    if hasattr(scan_result, "pages_scanned") and hasattr(scan_result, "pdf_path"):
        pdf_name = Path(scan_result.pdf_path).name
        for page in scan_result.pages_scanned:
            citations.append(
                Citation(
                    source_pdf=pdf_name,
                    page_number=page,
                    year=0,  # Caller may enrich later
                    excerpt=scan_result.answer[:100] if scan_result.answer else "",
                )
            )
    return citations
