"""Unified bookmark extraction for budget PDFs.

For rich PDFs (2008+): extracts native bookmarks via doc.get_toc()
For sparse PDFs (2005-2007, 2009): builds synthetic bookmarks from page headings.
Output format is identical regardless of source.
"""

from __future__ import annotations

import re
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import fitz

logger = logging.getLogger(__name__)


class BookmarkExtractor:
    """Extract or build bookmarks for a budget PDF."""

    # Threshold: PDFs with more than this many native bookmarks are considered "rich"
    _NATIVE_THRESHOLD = 100

    def extract(self, pdf_path: Path | str, year: int) -> list[dict]:
        """Extract bookmarks — native or synthetic.

        Returns list of dicts:
        {level, title, page_number, einzelplan, kapitel, nav_type}
        """
        pdf_path = Path(pdf_path)
        doc = fitz.open(str(pdf_path))
        try:
            toc = doc.get_toc()

            if len(toc) > self._NATIVE_THRESHOLD:
                result = self._parse_native_bookmarks(toc, pdf_path.name)
            else:
                result = self._build_synthetic_bookmarks(doc, pdf_path.name, year)
        finally:
            doc.close()

        return result

    # ------------------------------------------------------------------
    # Native bookmark parsing
    # ------------------------------------------------------------------

    def _parse_native_bookmarks(self, toc: list, pdf_name: str) -> list[dict]:
        """Parse native PDF bookmarks into structured entries."""
        entries: list[dict] = []
        current_ep: str | None = None
        current_kap: str | None = None

        for level, title, page in toc:
            title = title.strip()
            ep = current_ep
            kap = current_kap
            nav_type = "other"

            # Detect Einzelplan
            ep_match = re.search(r"Einzelplan\s+(\d{2})", title)
            if ep_match:
                current_ep = ep_match.group(1)
                ep = current_ep
                current_kap = None
                nav_type = "ep_title"

            # Detect Kapitel (4-digit code at start)
            kap_match = re.match(r"^(\d{4})\s", title)
            if kap_match:
                current_kap = kap_match.group(1)
                kap = current_kap
                nav_type = "kap_title"

            # Classify nav_type from title text
            nav_type = self._classify_nav_type(title, level, nav_type)

            entries.append(
                {
                    "level": level,
                    "title": title,
                    "page_number": page,
                    "einzelplan": ep,
                    "kapitel": kap,
                    "nav_type": nav_type,
                }
            )

        return entries

    @staticmethod
    def _classify_nav_type(title: str, level: int, default: str) -> str:
        """Derive nav_type from bookmark title text."""
        t = title.lower()

        if "überblick zum einzelplan" in t:
            return "ep_ueberblick"
        if "überblick zum kapitel" in t or (
            "überblick" in t and default not in ("ep_title",)
        ):
            return "kap_ueberblick"
        if "vorbemerkung" in t or "vorwort" in t:
            return "vorbemerkung"
        if "haushaltsvermerk" in t:
            return "haushaltsvermerk"
        if "personalhaushalt" in t or "planstellen" in t:
            return "personal"
        if "erläuterung" in t:
            return "erlaeuterung"
        if "einnahmen" in t and level >= 5:
            return "einnahmen"
        if "ausgaben" in t and level >= 5:
            return "ausgaben"
        if re.match(r"^\d{3}\s+\d{2}$", title):
            return "titel"
        if "gesamtplan" in t:
            return "gesamtplan"

        return default

    # ------------------------------------------------------------------
    # Synthetic bookmark generation (older PDFs without native TOC)
    # ------------------------------------------------------------------

    def _build_synthetic_bookmarks(
        self, doc: fitz.Document, pdf_name: str, year: int
    ) -> list[dict]:
        """Build synthetic bookmarks from page headings for older PDFs."""
        entries: list[dict] = []

        # Detect EP from first page
        page1 = doc[0].get_text("text") if len(doc) > 0 else ""
        ep_match = re.search(r"Einzelplan\s+(\d{2})", page1)
        current_ep = ep_match.group(1) if ep_match else None

        # EP-level entry
        if current_ep:
            name_match = re.search(
                r"Einzelplan\s+\d{2}\s*\n\s*(.+?)(?:\n|$)", page1
            )
            ep_name = name_match.group(1).strip() if name_match else ""
            ep_label = f"Einzelplan {current_ep}"
            if ep_name:
                ep_label += f" {ep_name}"
            entries.append(
                {
                    "level": 3,
                    "title": ep_label,
                    "page_number": 1,
                    "einzelplan": current_ep,
                    "kapitel": None,
                    "nav_type": "ep_title",
                }
            )

        # Parse text TOC on page 1 (format: "0601 Ministry Name ......... 3")
        for m in re.finditer(r"(\d{4})\s+(.+?)\.{2,}\s*(\d+)", page1):
            kap, name, page_num = m.group(1), m.group(2).strip(), int(m.group(3))
            entries.append(
                {
                    "level": 4,
                    "title": f"{kap} {name}",
                    "page_number": page_num,
                    "einzelplan": current_ep,
                    "kapitel": kap,
                    "nav_type": "kap_title",
                }
            )

        # Scan all pages for Kapitel transitions and section markers
        current_kap: str | None = None
        seen: set[tuple[str, int]] = set()  # deduplicate (nav_type, page)

        for i in range(len(doc)):
            text = doc[i].get_text("text")[:500]
            page_num = i + 1

            # Kapitel code in header (4-digit on its own line near top)
            kap_match = re.search(r"^(\d{4})\s*$", text[:150], re.MULTILINE)
            if kap_match:
                new_kap = kap_match.group(1)
                if new_kap != current_kap and (
                    not current_ep or new_kap[:2] == current_ep
                ):
                    current_kap = new_kap

            # Section markers (only first occurrence per page)
            marker = self._detect_section_marker(text)
            if marker and (marker, page_num) not in seen:
                seen.add((marker, page_num))
                label = self._section_label(marker, current_kap)
                entries.append(
                    {
                        "level": 5 if marker != "personal" else 4,
                        "title": label,
                        "page_number": page_num,
                        "einzelplan": current_ep,
                        "kapitel": current_kap,
                        "nav_type": marker,
                    }
                )

        return entries

    @staticmethod
    def _detect_section_marker(text: str) -> str | None:
        """Return nav_type string if a section marker is found in text."""
        head = text[:200]
        if "Überblick" in head:
            return "kap_ueberblick"
        if "Personalhaushalt" in text[:150]:
            return "personal"
        if "Vorbemerkung" in text[:150]:
            return "vorbemerkung"
        if "Haushaltsvermerk" in text[:150]:
            return "haushaltsvermerk"
        return None

    @staticmethod
    def _section_label(nav_type: str, kapitel: str | None) -> str:
        """Build human-readable label for a synthetic section marker."""
        suffix = f" {kapitel}" if kapitel else ""
        labels = {
            "kap_ueberblick": f"Überblick zum Kapitel{suffix}",
            "personal": "Personalhaushalt",
            "vorbemerkung": "Vorbemerkung",
            "haushaltsvermerk": "Haushaltsvermerk",
        }
        return labels.get(nav_type, nav_type)


# ------------------------------------------------------------------
# Batch / parallel extraction helper
# ------------------------------------------------------------------


def extract_all_bookmarks(
    budgets_dir: Path,
    years: list[int] | None = None,
    max_workers: int = 4,
) -> dict[int, list[tuple[str, list[dict]]]]:
    """Extract bookmarks from all PDFs in parallel.

    Returns ``{year: [(pdf_name, entries), ...]}``
    """
    results: dict[int, list[tuple[str, list[dict]]]] = {}
    extractor = BookmarkExtractor()
    tasks: list[tuple[int, Path]] = []

    for year_dir in sorted(budgets_dir.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        if years and year not in years:
            continue
        for pdf in sorted(year_dir.glob("*.pdf")):
            tasks.append((year, pdf))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for year, pdf in tasks:
            future = pool.submit(extractor.extract, pdf, year)
            futures[future] = (year, pdf.name)

        for future in futures:
            year, pdf_name = futures[future]
            try:
                entries = future.result()
                results.setdefault(year, []).append((pdf_name, entries))
                logger.info(
                    "Bookmarks: %s/%s → %d entries", year, pdf_name, len(entries)
                )
            except Exception as exc:
                logger.warning(
                    "Bookmark extraction failed: %s/%s — %s", year, pdf_name, exc
                )

    return results
