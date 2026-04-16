"""PDF extraction module for German federal budget (Bundeshaushalt) documents.

Extracts text and tables from budget PDFs that contain hierarchical structures
(Einzelplan → Kapitel → Titel), German number formatting, and personnel tables.
Uses PyMuPDF (fitz) for fast text extraction and pdfplumber for table detection.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import fitz  # PyMuPDF
import pdfplumber

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExtractedPage:
    """Extraction result for a single PDF page."""

    page_number: int
    text: str
    tables: list[list[list[str]]] = field(default_factory=list)
    # Each table is a list of rows; each row is a list of cell strings.


@dataclass
class ExtractedDocument:
    """Extraction result for an entire PDF."""

    source_path: Path
    total_pages: int
    pages: list[ExtractedPage] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# German number parsing
# ---------------------------------------------------------------------------

_GERMAN_NUMBER_RE = re.compile(
    r"^\s*"
    r"(?P<neg>[-−]|\()"            # optional leading minus / open paren
    r"?\s*"
    r"(?P<int>\d{1,3}(?:\.\d{3})*)"  # integer part with dot-thousands
    r"(?:,(?P<dec>\d+))?"          # optional comma-decimal
    r"(?P<suffix>[kK])?"           # optional k-suffix (thousands)
    r"\s*\)?"                      # optional closing paren
    r"\s*$"
)


def parse_german_number(text: str) -> float | None:
    """Parse a German-formatted number string into a float.

    Examples
    --------
    >>> parse_german_number("16.161.139")
    16161139.0
    >>> parse_german_number("102,03")
    102.03
    >>> parse_german_number("1.234,56")
    1234.56
    >>> parse_german_number("-500")
    -500.0
    >>> parse_german_number("(1.200)")
    -1200.0
    >>> parse_german_number("50k")
    50000.0
    >>> parse_german_number("—")  # dash
    >>> parse_german_number("")
    """
    if text is None:
        return None

    cleaned = text.strip()

    # Treat dashes, em-dashes, en-dashes, empty strings as "no value"
    if not cleaned or cleaned in {"—", "–", "-", "−", ".", ".."}:
        return None

    m = _GERMAN_NUMBER_RE.match(cleaned)
    if not m:
        return None

    integer_part = m.group("int").replace(".", "")  # strip thousand separators
    decimal_part = m.group("dec") or ""
    is_negative = m.group("neg") is not None
    has_k_suffix = m.group("suffix") is not None

    number_str = integer_part
    if decimal_part:
        number_str += "." + decimal_part

    try:
        value = float(number_str)
    except ValueError:
        return None

    if is_negative:
        value = -abs(value)
    if has_k_suffix:
        value *= 1_000

    return value


# ---------------------------------------------------------------------------
# PDF Extractor
# ---------------------------------------------------------------------------

class PDFExtractor:
    """Extract text and tables from a German federal budget PDF."""

    # pdfplumber table-detection settings tuned for budget PDFs
    _TABLE_SETTINGS: dict = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 5,
        "join_tolerance": 5,
        "edge_min_length": 20,
        "min_words_vertical": 2,
        "min_words_horizontal": 2,
    }

    def __init__(self, pdf_path: Path) -> None:
        self.pdf_path = Path(pdf_path)
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_full(self) -> ExtractedDocument:
        """Extract all text and tables from the PDF."""
        logger.info("Starting full extraction of %s", self.pdf_path.name)
        metadata = self._read_metadata()
        total_pages = self._page_count()
        pages = self.extract_pages(start=0, end=total_pages)
        doc = ExtractedDocument(
            source_path=self.pdf_path,
            total_pages=total_pages,
            pages=pages,
            metadata=metadata,
        )
        logger.info(
            "Extraction complete: %d pages, %d total tables",
            total_pages,
            sum(len(p.tables) for p in pages),
        )
        return doc

    def extract_text_only(self) -> str:
        """Extract just the text content (faster, skips table detection)."""
        logger.info("Extracting text-only from %s", self.pdf_path.name)
        parts: list[str] = []
        try:
            with fitz.open(str(self.pdf_path)) as doc:
                for page in doc:
                    try:
                        parts.append(page.get_text("text"))
                    except Exception:
                        logger.warning(
                            "Text extraction failed on page %d", page.number + 1
                        )
                        parts.append("")
        except Exception:
            logger.exception("Failed to open PDF for text extraction")
            raise
        return "\n".join(parts)

    def extract_pages(
        self, start: int = 0, end: int | None = None
    ) -> list[ExtractedPage]:
        """Extract a range of pages (0-indexed start, exclusive end)."""
        total = self._page_count()
        if end is None or end > total:
            end = total
        start = max(0, start)

        logger.info("Extracting pages %d–%d of %d", start + 1, end, total)

        # --- Text via PyMuPDF (fast) ---
        texts: dict[int, str] = {}
        try:
            with fitz.open(str(self.pdf_path)) as doc:
                for page_num in range(start, end):
                    try:
                        texts[page_num] = doc[page_num].get_text("text")
                    except Exception:
                        logger.warning(
                            "PyMuPDF text extraction failed on page %d", page_num + 1
                        )
                        texts[page_num] = ""
        except Exception:
            logger.exception("Failed to open PDF with PyMuPDF")
            raise

        # --- Tables via pdfplumber (accurate) ---
        tables_by_page: dict[int, list[list[list[str]]]] = {
            p: [] for p in range(start, end)
        }
        try:
            with pdfplumber.open(str(self.pdf_path)) as pdf:
                for page_num in range(start, end):
                    try:
                        page = pdf.pages[page_num]
                        raw_tables = page.extract_tables(
                            table_settings=self._TABLE_SETTINGS
                        )
                        for raw in raw_tables or []:
                            cleaned = self._clean_table(raw)
                            if cleaned:
                                tables_by_page[page_num].append(cleaned)
                    except Exception:
                        logger.warning(
                            "Table extraction failed on page %d", page_num + 1
                        )
        except Exception:
            logger.exception("Failed to open PDF with pdfplumber")
            # Continue – we still have text

        pages: list[ExtractedPage] = []
        for page_num in range(start, end):
            pages.append(
                ExtractedPage(
                    page_number=page_num + 1,  # 1-indexed for display
                    text=texts.get(page_num, ""),
                    tables=tables_by_page.get(page_num, []),
                )
            )
        return pages

    def extract_tables(self) -> list[dict]:
        """Extract all tables with page numbers and position metadata."""
        logger.info("Extracting all tables from %s", self.pdf_path.name)
        results: list[dict] = []
        try:
            with pdfplumber.open(str(self.pdf_path)) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    try:
                        found = page.find_tables(
                            table_settings=self._TABLE_SETTINGS
                        )
                        for idx, table_obj in enumerate(found or []):
                            raw = table_obj.extract()
                            cleaned = self._clean_table(raw)
                            if not cleaned:
                                continue
                            results.append(
                                {
                                    "page": page_num + 1,
                                    "table_index": idx,
                                    "bbox": list(table_obj.bbox),
                                    "rows": len(cleaned),
                                    "cols": len(cleaned[0]) if cleaned else 0,
                                    "data": cleaned,
                                    "header": cleaned[0] if cleaned else [],
                                }
                            )
                    except Exception:
                        logger.warning(
                            "Table detection failed on page %d", page_num + 1
                        )
        except Exception:
            logger.exception("Failed to open PDF with pdfplumber for table extraction")
            raise

        logger.info("Found %d tables across all pages", len(results))
        return results

    def save_extraction(self, output_dir: Path) -> None:
        """Save extracted text and tables to files for inspection.

        Creates ``output_dir/extracted_text.txt`` and
        ``output_dir/extracted_tables.json``.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        doc = self.extract_full()

        # --- Full text ---
        text_path = output_dir / "extracted_text.txt"
        full_text_parts: list[str] = []
        for page in doc.pages:
            full_text_parts.append(
                f"{'='*60}\n"
                f"PAGE {page.page_number}\n"
                f"{'='*60}\n"
                f"{page.text}\n"
            )
        text_path.write_text("\n".join(full_text_parts), encoding="utf-8")
        logger.info("Saved extracted text to %s", text_path)

        # --- Tables as JSON ---
        tables_path = output_dir / "extracted_tables.json"
        tables_data: list[dict] = []
        for page in doc.pages:
            for idx, table in enumerate(page.tables):
                tables_data.append(
                    {
                        "page": page.page_number,
                        "table_index": idx,
                        "rows": len(table),
                        "cols": len(table[0]) if table else 0,
                        "data": table,
                    }
                )
        tables_path.write_text(
            json.dumps(tables_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Saved %d tables to %s", len(tables_data), tables_path)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _page_count(self) -> int:
        with fitz.open(str(self.pdf_path)) as doc:
            return len(doc)

    def _read_metadata(self) -> dict:
        try:
            with fitz.open(str(self.pdf_path)) as doc:
                raw = doc.metadata or {}
                return {k: v for k, v in raw.items() if v}
        except Exception:
            logger.warning("Could not read PDF metadata")
            return {}

    @staticmethod
    def _clean_table(raw_table: list | None) -> list[list[str]]:
        """Normalise a raw pdfplumber table into clean string cells.

        * Replaces ``None`` cells with empty strings.
        * Collapses internal whitespace and strips each cell.
        * Drops entirely empty rows.
        """
        if not raw_table:
            return []

        cleaned: list[list[str]] = []
        for row in raw_table:
            if row is None:
                continue
            new_row: list[str] = []
            for cell in row:
                if cell is None:
                    new_row.append("")
                else:
                    # Normalise whitespace (newlines inside cells are common)
                    new_row.append(re.sub(r"\s+", " ", str(cell)).strip())
            # Keep the row only if it has at least one non-empty cell
            if any(c for c in new_row):
                cleaned.append(new_row)
        return cleaned


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Allow running from project root:  python -m src.extract.pdf_extractor
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from src.config import config  # noqa: E402

    pdf_path = config.DOCS_DIR / "0350-25.pdf"
    if not pdf_path.exists():
        print(f"PDF not found at {pdf_path}")
        sys.exit(1)

    extractor = PDFExtractor(pdf_path)

    # Extract first 5 pages
    pages = extractor.extract_pages(0, 5)
    for page in pages:
        print(
            f"Page {page.page_number}: {len(page.text)} chars, "
            f"{len(page.tables)} tables"
        )
        if page.tables:
            for i, table in enumerate(page.tables):
                cols = len(table[0]) if table else 0
                print(f"  Table {i}: {len(table)} rows x {cols} cols")

    # Print first page text preview
    if pages:
        print("\n--- First page text (first 500 chars) ---")
        print(pages[0].text[:500])
