"""Build hierarchical Table of Contents from budget PDFs.

Analyzes PDF page text to detect EP and Kapitel boundaries,
creating a 3-level navigation index:

  Level 1 (ep):      Einzelplan sections       → "EP06 = pages 532-872"
  Level 2 (kapitel):  Kapitel within EP          → "Kap 0622 = pages 708-710"
  Level 3 (section):  Section type within Kapitel → "VE = page 710"

Works with both consolidated PDFs (2012+, all EPs in one file)
and per-EP PDFs (2005-2011, one EP per file).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import fitz

logger = logging.getLogger(__name__)

# Pattern for EP start pages in consolidated PDFs:
# "Bundeshaushaltsplan YYYY\nEinzelplan NN\nMinistry Name\nInhalt"
_RE_EP_HEADER = re.compile(
    r'Bundeshaushaltsplan\s+\d{4}\s*\n\s*Einzelplan\s+(\d{2})\s*\n\s*(.+?)(?:\s*\n)',
)

# Pattern for EP start in per-EP PDFs (first page):
# "Einzelplan NN\nMinistry Name"
_RE_EP_SINGLE = re.compile(
    r'Einzelplan\s+(\d{2})\s*\n\s*(.+?)(?:\s*\n)',
)

# Kapitel "Überblick" pages — the most reliable Kapitel boundary marker
_RE_UEBERBLICK_KAP = re.compile(
    r'[ÜU]berblick\s+zum\s+Kapitel\s+(\d{4})',
)

# General Kapitel reference in page headers/text
_RE_KAP_HEADER = re.compile(
    r'(?:Kapitel|Kap\.?)\s+(\d{4})',
)

# Kapitel number alone on a line (footer pattern)
_RE_KAP_FOOTER = re.compile(
    r'^(\d{4})\s*$',
    re.MULTILINE,
)

# Personalhaushalt section header (appears once per EP)
_RE_PERSONAL_HEADER = re.compile(
    r'Personalhaushalt\s*\n.*?Einzelplan\s+(\d{2})',
    re.DOTALL,
)


class TOCBuilder:
    """Build hierarchical TOC from a budget PDF."""

    def __init__(self, pdf_path: Path | str, year: int):
        self.pdf_path = Path(pdf_path)
        self.year = year

    def build(self) -> list[dict]:
        """Build 3-level TOC entries.

        Returns list of dicts with keys:
            level, einzelplan, kapitel, section_type, label,
            page_start, page_end
        """
        doc = fitz.open(str(self.pdf_path))
        total_pages = len(doc)
        if total_pages == 0:
            doc.close()
            return []

        # Phase 1: Find EP boundaries
        ep_entries = self._find_ep_boundaries(doc)
        if not ep_entries:
            logger.info("No EP boundaries found in %s", self.pdf_path.name)
            doc.close()
            return []

        logger.debug(
            "%s: found %d EP entries", self.pdf_path.name, len(ep_entries)
        )

        # Phase 2: Within each EP, find Kapitel boundaries
        kap_entries: list[dict] = []
        for ep in ep_entries:
            kaps = self._find_kap_boundaries(doc, ep)
            kap_entries.extend(kaps)

        logger.debug(
            "%s: found %d Kapitel entries", self.pdf_path.name, len(kap_entries)
        )

        # Phase 3: Within each Kapitel, detect section types
        section_entries: list[dict] = []
        for kap in kap_entries:
            sections = self._find_sections_in_kapitel(doc, kap)
            section_entries.extend(sections)

        doc.close()

        all_entries = ep_entries + kap_entries + section_entries
        logger.info(
            "%s: TOC built — %d EPs, %d Kapitels, %d sections",
            self.pdf_path.name,
            len(ep_entries),
            len(kap_entries),
            len(section_entries),
        )
        return all_entries

    # ------------------------------------------------------------------
    # Phase 1: EP boundaries
    # ------------------------------------------------------------------

    def _find_ep_boundaries(self, doc) -> list[dict]:
        """Scan for Einzelplan header pages.

        Consolidated PDFs have "Bundeshaushaltsplan YYYY / Einzelplan NN"
        title pages.  Per-EP PDFs have a single EP on page 1.
        """
        entries: list[dict] = []
        total = len(doc)

        # Try consolidated format first: look for Bundeshaushaltsplan header
        ep_starts: list[tuple[int, str, str]] = []  # (0-idx, ep_num, name)
        for i in range(total):
            text = doc[i].get_text("text")[:500]
            m = _RE_EP_HEADER.search(text)
            if m:
                ep_num = m.group(1)
                ep_name = m.group(2).strip()
                # Avoid duplicates (Personalhaushalt pages also match)
                if "Personalhaushalt" in text[:50]:
                    continue
                # Deduplicate: keep first occurrence of each EP
                if ep_starts and ep_starts[-1][1] == ep_num:
                    continue
                ep_starts.append((i, ep_num, ep_name))

        if ep_starts:
            # Consolidated PDF — close each EP at the start of the next
            for idx, (page_0, ep_num, ep_name) in enumerate(ep_starts):
                if idx + 1 < len(ep_starts):
                    page_end = ep_starts[idx + 1][0]  # page before next EP
                else:
                    page_end = total
                entries.append({
                    "level": "ep",
                    "einzelplan": ep_num,
                    "kapitel": None,
                    "section_type": None,
                    "label": ep_name,
                    "page_start": page_0 + 1,  # 1-indexed
                    "page_end": page_end,       # 1-indexed inclusive
                })
            return entries

        # Per-EP PDF: single EP, first page has "Einzelplan NN"
        text_p1 = doc[0].get_text("text")[:500]
        m = _RE_EP_SINGLE.search(text_p1)
        if m:
            entries.append({
                "level": "ep",
                "einzelplan": m.group(1),
                "kapitel": None,
                "section_type": None,
                "label": m.group(2).strip(),
                "page_start": 1,
                "page_end": total,
            })

        return entries

    # ------------------------------------------------------------------
    # Phase 2: Kapitel boundaries within an EP
    # ------------------------------------------------------------------

    def _find_kap_boundaries(self, doc, ep_entry: dict) -> list[dict]:
        """Find Kapitel boundaries within an EP page range.

        Strategy:
        1. First pass — collect Überblick pages (most reliable markers).
        2. Second pass — track the current Kapitel via header/footer patterns
           to find Kapitel that lack Überblick pages.
        3. Merge and close ranges.
        """
        ep = ep_entry["einzelplan"]
        start_0 = ep_entry["page_start"] - 1  # 0-indexed
        end_0 = ep_entry["page_end"]           # exclusive for range()
        total = len(doc)

        # Collect (first_0idx, kap_num, label) for each distinct Kapitel
        kap_first_seen: dict[str, tuple[int, str]] = {}  # kap → (0-idx, label)

        for i in range(start_0, min(end_0, total)):
            text = doc[i].get_text("text")

            # Überblick pages are the best markers
            m = _RE_UEBERBLICK_KAP.search(text[:500])
            if m and m.group(1)[:2] == ep:
                kap = m.group(1)
                if kap not in kap_first_seen:
                    kap_first_seen[kap] = (i, f"Kapitel {kap}")
                continue

            # Header pattern: "Kapitel XXYY" or "Kap XXYY"
            for hm in _RE_KAP_HEADER.finditer(text[:400]):
                kap = hm.group(1)
                if kap[:2] == ep and kap not in kap_first_seen:
                    # Try to get name from text after match
                    after = text[hm.end():hm.end() + 200]
                    nm = re.search(r'\s*\n?\s*(\S.+?)(?:\n|$)', after)
                    label = nm.group(1).strip()[:100] if nm else ""
                    kap_first_seen[kap] = (i, label or f"Kapitel {kap}")
                break  # only first match per page

            # Footer: standalone 4-digit line
            if not any(k[:2] == ep for k in kap_first_seen if kap_first_seen[k][0] == i):
                fm = _RE_KAP_FOOTER.search(text)
                if fm:
                    kap = fm.group(1)
                    if kap[:2] == ep and kap not in kap_first_seen:
                        kap_first_seen[kap] = (i, f"Kapitel {kap}")

        if not kap_first_seen:
            return []

        # Sort by first-seen page
        sorted_kaps = sorted(kap_first_seen.items(), key=lambda x: x[1][0])

        entries: list[dict] = []
        for idx, (kap, (page_0, label)) in enumerate(sorted_kaps):
            if idx + 1 < len(sorted_kaps):
                page_end = sorted_kaps[idx + 1][1][0]  # page before next Kap
            else:
                page_end = ep_entry["page_end"]

            entries.append({
                "level": "kapitel",
                "einzelplan": ep,
                "kapitel": kap,
                "section_type": None,
                "label": label,
                "page_start": page_0 + 1,   # 1-indexed
                "page_end": page_end,        # 1-indexed inclusive
            })

        return entries

    # ------------------------------------------------------------------
    # Phase 3: Section types within a Kapitel
    # ------------------------------------------------------------------

    def _find_sections_in_kapitel(self, doc, kap_entry: dict) -> list[dict]:
        """Detect section type transitions within a Kapitel page range."""
        from src.extract.section_detector import detect_section_type

        ep = kap_entry["einzelplan"]
        kap = kap_entry["kapitel"]
        start_0 = kap_entry["page_start"] - 1  # 0-indexed
        end_0 = min(kap_entry["page_end"], len(doc))

        if start_0 >= end_0:
            return []

        current_section: str | None = None
        current_start: int | None = None
        entries: list[dict] = []

        for i in range(start_0, end_0):
            text = doc[i].get_text("text")
            section = detect_section_type(text)

            if section != current_section:
                # Close previous section
                if current_section is not None and current_section != "other":
                    entries.append({
                        "level": "section",
                        "einzelplan": ep,
                        "kapitel": kap,
                        "section_type": current_section,
                        "label": current_section,
                        "page_start": current_start,
                        "page_end": i,  # page before change (1-indexed)
                    })
                current_section = section
                current_start = i + 1  # 1-indexed

        # Close last section
        if current_section is not None and current_section != "other":
            entries.append({
                "level": "section",
                "einzelplan": ep,
                "kapitel": kap,
                "section_type": current_section,
                "label": current_section,
                "page_start": current_start,
                "page_end": kap_entry["page_end"],
            })

        return entries
