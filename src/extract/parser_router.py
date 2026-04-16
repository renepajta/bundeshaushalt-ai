"""Auto-routing parser that selects the correct era-specific parser.

Detects era from PDF characteristics and dispatches to:
- EarlyEraParser (2005-2011) — per-Einzelplan PDFs
- MidEraParser (2012-2026) — consolidated budget PDFs (Bundeshaushalt-*.pdf)
- BudgetParser — modern per-Einzelplan format (e.g. 0350-25.pdf)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from src.extract.budget_parser import ParsedBudget

logger = logging.getLogger(__name__)

# Patterns indicating early-era per-Einzelplan PDFs
_RE_EARLY_FILENAME = re.compile(r"^Epl\d+\.pdf$", re.IGNORECASE)
_EARLY_SPECIAL_FILES = {"vorspann.pdf", "hg.pdf"}

# Patterns indicating a consolidated budget PDF (should use MidEraParser)
_RE_CONSOLIDATED = re.compile(
    r"(?:bundeshaushalt|haushaltsplan|gesamt|epl_gesamt)",
    re.IGNORECASE,
)


class BudgetParserRouter:
    """Routes PDF parsing to the appropriate era-specific parser."""

    def parse(
        self,
        pdf_path: str | Path,
        year: int | None = None,
        version: str = "soll",
    ) -> ParsedBudget:
        """Parse a budget PDF using the appropriate era parser.

        Auto-detects era from:
        1. Explicit year parameter
        2. Parent directory name (data/budgets/2005/ → 2005)
        3. Filename patterns (Epl*.pdf → early era)
        4. PDF content (first pages scanned for year keywords)
        """
        pdf_path = Path(pdf_path)

        # --- resolve year from directory if not given ---
        if year is None:
            year = self._year_from_directory(pdf_path)

        # --- resolve year from PDF content as last resort ---
        if year is None:
            year = self._year_from_content(pdf_path)

        era = self._detect_era(pdf_path, year)
        logger.info("Router: %s → era=%s, year=%s", pdf_path.name, era, year)

        if era == "early":
            return self._parse_early(pdf_path, year, version)
        elif era == "mid":
            return self._parse_mid(pdf_path, year, version)
        else:
            return self._parse_modern(pdf_path, year, version)

    # ------------------------------------------------------------------
    # Era detection
    # ------------------------------------------------------------------

    def _detect_era(self, pdf_path: Path, year: int | None) -> str:
        """Return 'early', 'mid', or 'modern'."""
        fname = pdf_path.name.lower()

        # Filename heuristic: Epl##.pdf / Vorspann.pdf / HG.pdf in a pre-2012 dir
        is_early_name = bool(_RE_EARLY_FILENAME.match(pdf_path.name)) or fname in _EARLY_SPECIAL_FILES
        if is_early_name and (year is not None and year < 2012):
            return "early"

        # Consolidated budget PDFs (Bundeshaushalt-*.pdf etc.) always use mid-era
        # parser regardless of year — these share the same format from 2012-2026.
        is_consolidated = bool(_RE_CONSOLIDATED.search(fname))

        if year is not None:
            if year <= 2011:
                return "early"
            if is_consolidated:
                return "mid"
            if year <= 2023:
                return "mid"
            # year >= 2024, non-consolidated → modern (per-EP format)
            return "modern"

        # Fallback: if filename looks like per-EP, assume early
        if is_early_name:
            return "early"

        # Fallback: consolidated without known year → mid
        if is_consolidated:
            return "mid"

        return "modern"

    @staticmethod
    def _year_from_directory(pdf_path: Path) -> int | None:
        """Try to extract year from parent directory name."""
        parent = pdf_path.parent.name
        if parent.isdigit() and 1990 <= int(parent) <= 2100:
            return int(parent)
        return None

    @staticmethod
    def _year_from_content(pdf_path: Path) -> int | None:
        """Scan first pages of PDF for a year reference."""
        try:
            import fitz

            with fitz.open(str(pdf_path)) as doc:
                text = ""
                for i in range(min(5, len(doc))):
                    text += doc[i].get_text("text") + "\n"
            # Look for "Haushaltsplan YYYY" or "Bundeshaushalt YYYY" etc.
            m = re.search(r"(?:Haushaltsplan|Bundeshaushalt|Haushalt)\s+(\d{4})", text)
            if m:
                return int(m.group(1))
            # Broad 4-digit year near start
            m = re.search(r"\b(20\d{2})\b", text)
            if m:
                return int(m.group(1))
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Dispatch methods
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_early(pdf_path: Path, year: int | None, version: str) -> ParsedBudget:
        from src.extract.early_era_parser import EarlyEraParser

        parser = EarlyEraParser()
        return parser.parse(pdf_path, year=year, version=version)

    @staticmethod
    def _parse_mid(pdf_path: Path, year: int | None, version: str) -> ParsedBudget:
        from src.extract.budget_parser import _extract_text_only_doc
        from src.extract.mid_era_parser import MidEraParser
        from src.extract.pdf_extractor import PDFExtractor

        extractor = PDFExtractor(pdf_path)
        doc = _extract_text_only_doc(extractor)
        parser = MidEraParser(doc)
        return parser.parse()

    @staticmethod
    def _parse_modern(pdf_path: Path, year: int | None, version: str) -> ParsedBudget:
        from src.extract.budget_parser import BudgetParser, _extract_text_only_doc
        from src.extract.pdf_extractor import PDFExtractor

        extractor = PDFExtractor(pdf_path)
        doc = _extract_text_only_doc(extractor)
        parser = BudgetParser(doc)
        return parser.parse()


def parse_budget_pdf_routed(
    pdf_path: str | Path,
    year: int | None = None,
    version: str = "soll",
) -> ParsedBudget:
    """One-shot parse using auto-routing."""
    router = BudgetParserRouter()
    return router.parse(pdf_path, year=year, version=version)
