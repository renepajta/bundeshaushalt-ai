"""Parser for consolidated budget PDFs from 2012–2023.

Years 2012–2023 ship 1-2 large consolidated PDFs per year (2,674-3,039 pages)
containing Haushaltsgesetz, Gesamtplan, and **all** Einzelpläne sequentially.

The format is very similar to the modern ``BudgetParser`` (2024-2026) with
these differences:

* EP headers lack the ``Entwurf``/``Beschluss``/``Nachtrag`` prefix.
* 2012 lacks ``Überblick zum Kapitel`` section headers.
* **2012-2013** use a reversed column layout where amounts appear *before*
  the Titel code in extracted text (Layout B, same as early-era 2007-2011).
* **2014-2023** use the modern column layout where amounts appear *after*
  the Titel code (Layout A, same as 2024-2026).

The parser auto-detects which layout is in use.

All monetary values are in **1 000 €** as printed.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from src.extract.budget_parser import (
    BudgetParser,
    BudgetEntry,
    PersonnelEntry,
    ParsedBudget,
    _extract_text_only_doc,
    _classify_tarif,
    _RE_TITEL_FUNC,
    _RE_KAP_OVERVIEW,
    _RE_KAP_FOOTER,
    _RE_TITELGRUPPE,
    _RE_PERSONAL_SECTION,
    _RE_VE_SECTION,
    _VERRECHNUNG_PREFIXES,
)
from src.extract.pdf_extractor import ExtractedDocument, parse_german_number

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Additional regex patterns for the mid-era format
# ---------------------------------------------------------------------------

# EP header without Entwurf/Beschluss prefix — just:
# "Bundeshaushaltsplan YYYY\nEinzelplan NN\nMinistry Name"
_RE_EP_HEADER_RELAXED = re.compile(
    r"Bundeshaushaltsplan\s+\d{4}\s+"
    r"Einzelplan\s+(\d{2})\s*\n\s*(.+?)(?:\s*\n|Inhalt)",
    re.DOTALL,
)

# Titel code with optional leading F/space (used for reversed-layout detection)
_RE_TITEL_FUNC_LEADING = re.compile(
    r"(?:^|\n)\s*F?\s*(\d{3}\s+\d{2})\s*\n\s*-\s*(\d{3})\s*(?:\n|$)"
)

# Amount-only line
_RE_AMOUNT_LINE = re.compile(r"^\s*([\d]+(?:[\s.]\d+)*)\s*$")
_RE_DASH_LINE = re.compile(r"^\s*[-–—−]\s*$")

# VE total amount: "Verpflichtungsermächtigung....... 5 500 T€" or bare number
_RE_VE_TOTAL = re.compile(
    r"Verpflichtungserm[äa]chtigung(?:en)?[.\s]*([\d\s.]+?)(?:\s*T€|\s*$)",
    re.IGNORECASE | re.MULTILINE,
)

# VE maturity line: "im Haushaltsjahr YYYY bis zu....... 2 500 T€"
# or: "fällig im Haushaltsjahr YYYY bis zu....... 200 T€"
# or: "in den Folgejahren bis zu....... 335 T€"
_RE_VE_FAELLIG_LINE = re.compile(
    r"(?:im\s+Haushaltsjahr|in\s+den?\s+Folgejahr(?:en)?)\s+(\d{4})?\s*"
    r"bis\s+zu[.\s]*([\d\s.]+?)(?:\s*T€|\s*$)",
    re.IGNORECASE | re.MULTILINE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_amounts_from_lines(lines: list[str], max_amounts: int = 3) -> list[float]:
    """Extract up to *max_amounts* numeric values from consecutive lines."""
    amounts: list[float] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if any(c.isalpha() for c in stripped):
            if not re.match(r"^\([\d.\s,−-]+\)\s*$", stripped):
                continue

        if _RE_DASH_LINE.match(stripped):
            amounts.append(0.0)
        else:
            paren = re.match(r"^\((.*)\)\s*$", stripped)
            if paren:
                inner = paren.group(1).strip()
                if inner in {"—", "–", "-", "−", ""}:
                    amounts.append(0.0)
                else:
                    val = _parse_budget_amount(inner)
                    if val is not None:
                        amounts.append(val)
            else:
                val = _parse_budget_amount(stripped)
                if val is not None:
                    amounts.append(val)

        if len(amounts) >= max_amounts:
            break
    return amounts


def _parse_budget_amount(text: str) -> float | None:
    """Parse a budget amount that may use spaces between digit groups."""
    cleaned = text.strip()
    if not cleaned:
        return None
    if cleaned in {"—", "–", "-", "−"}:
        return 0.0

    if "." in cleaned or "," in cleaned:
        return parse_german_number(cleaned)

    collapsed = re.sub(r"\s+", "", cleaned)
    if collapsed.lstrip("-−").isdigit():
        try:
            return float(collapsed)
        except ValueError:
            return None

    return parse_german_number(cleaned)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class MidEraParser(BudgetParser):
    """Parser for consolidated budget PDFs (2012-2023).

    Inherits from :class:`BudgetParser` and overrides detection methods
    to handle the slightly different header patterns and two layout variants.
    """

    def __init__(self, extracted_doc: ExtractedDocument) -> None:
        super().__init__(extracted_doc)
        self._reversed_layout: bool | None = None  # auto-detected

    # ------------------------------------------------------------------
    # parse() override — adds VE extraction
    # ------------------------------------------------------------------

    def parse(self) -> ParsedBudget:
        """Parse the full document, including VE sections."""
        result = super().parse()
        result.verpflichtungen = self._parse_ve_sections()
        logger.info("VE extraction: %d entries", len(result.verpflichtungen))
        return result

    # ------------------------------------------------------------------
    # VE extraction
    # ------------------------------------------------------------------

    def _parse_ve_sections(self) -> list[dict]:
        """Extract Verpflichtungsermächtigung entries with fällig breakdown.

        Scans all pages for VE sections, tracks Einzelplan/Kapitel/Titel
        context from surrounding text, and returns one dict per maturity
        year (matching the ``verpflichtungsermachtigungen`` table schema).
        """
        results: list[dict] = []
        current_ep: str | None = None
        current_kap: str | None = None

        for page in self._pages:
            text = page.text

            # Track Einzelplan context
            ep_match = _RE_EP_HEADER_RELAXED.search(text)
            if ep_match:
                current_ep = ep_match.group(1)
                current_kap = None

            # Track Kapitel context from "Überblick zum Kapitel NNNN"
            kap_match = _RE_KAP_OVERVIEW.search(text)
            if kap_match:
                kap_num = kap_match.group(1)
                current_kap = kap_num
                if not current_ep and len(kap_num) >= 2:
                    current_ep = kap_num[:2]

            # Also detect Kapitel from page footer (e.g. "0622\n")
            if not current_kap and current_ep:
                for m in _RE_KAP_FOOTER.finditer(text):
                    candidate = m.group(1)
                    if candidate[:2] == current_ep and len(candidate) == 4:
                        current_kap = candidate

            if not _RE_VE_SECTION.search(text):
                continue

            # Skip pages without an Einzelplan context (e.g. Haushaltsgesetz)
            if not current_ep:
                continue

            # Find all VE blocks on this page
            ve_blocks = self._extract_ve_blocks(text)
            for block in ve_blocks:
                # Determine the Titel this VE belongs to by looking
                # at the text preceding the VE block
                titel = self._find_preceding_titel(text, block["start_pos"])

                base = {
                    "year": self._year,
                    "version": self._version,
                    "einzelplan": current_ep or "",
                    "kapitel": current_kap or "",
                    "titel": titel,
                    "betrag_gesamt": block["total"],
                    "source_pdf": self._source_pdf,
                    "source_page": page.page_number,
                }

                if block["faellig"]:
                    for fj, fb in block["faellig"]:
                        row = dict(base)
                        row["faellig_jahr"] = fj
                        row["faellig_betrag"] = fb
                        results.append(row)
                else:
                    # VE without fällig breakdown — store total only
                    results.append(base)

        return results

    def _extract_ve_blocks(self, text: str) -> list[dict]:
        """Find all VE blocks on a page, each with total + fällig lines."""
        blocks: list[dict] = []

        for m_total in _RE_VE_TOTAL.finditer(text):
            total = _parse_budget_amount(m_total.group(1))
            start_pos = m_total.start()

            # Define the search window for fällig lines: from VE match
            # to next VE section or end of reasonable context (~800 chars)
            next_ve = _RE_VE_TOTAL.search(text, m_total.end())
            end_pos = next_ve.start() if next_ve else min(m_total.end() + 800, len(text))
            window = text[m_total.end():end_pos]

            faellig: list[tuple[int | None, float]] = []
            for m_fl in _RE_VE_FAELLIG_LINE.finditer(window):
                yr_str = m_fl.group(1)
                amt = _parse_budget_amount(m_fl.group(2))
                if amt is not None:
                    yr = int(yr_str) if yr_str else None
                    faellig.append((yr, amt))

            blocks.append({
                "total": total,
                "faellig": faellig,
                "start_pos": start_pos,
            })

        return blocks

    @staticmethod
    def _find_preceding_titel(text: str, ve_pos: int) -> str | None:
        """Find the most recent Titel code before *ve_pos* in the text."""
        preceding = text[:ve_pos]
        # Match "NNN NN\n-NNN" pattern (Titel + Funktionskennziffer)
        matches = list(_RE_TITEL_FUNC.finditer(preceding))
        if matches:
            last = matches[-1]
            return last.group(1).replace(" ", " ")  # normalise spacing
        return None

    # ------------------------------------------------------------------
    # Phase 0: document metadata
    # ------------------------------------------------------------------

    def _detect_document_info(self) -> tuple[int, str]:
        """Detect year and version with relaxed patterns.

        Consolidated PDFs are the approved budget, so default to
        ``'beschluss'`` instead of ``'entwurf'``.
        """
        year, version = super()._detect_document_info()

        # If the base class fell back to 2026, try harder
        if year == 2026:
            sample = "\n".join(
                p.text for p in self._pages[:min(50, len(self._pages))]
            )
            m = re.search(r"Haushaltsjahr\s+(\d{4})", sample)
            if not m:
                m = re.search(r"Haushaltsgesetz\s+(\d{4})", sample)
            if not m:
                m = re.search(r"Bundeshaushaltsplan\s+(\d{4})", sample)
            if not m:
                m = re.search(r"Haushaltsplan[- ](\d{4})", sample)
            if m:
                year = int(m.group(1))

        # Default to 'beschluss' for consolidated PDFs
        if version == "entwurf":
            version = "beschluss"

        return year, version

    # ------------------------------------------------------------------
    # Layout detection
    # ------------------------------------------------------------------

    def _detect_layout(self) -> bool:
        """Return *True* if amounts appear BEFORE the Titel code (reversed).

        Checks a sample of Titel-bearing pages to determine column order.
        2012-2013 use reversed layout; 2014+ use modern layout.
        """
        if self._reversed_layout is not None:
            return self._reversed_layout

        # Quick heuristic: if year is known and >= 2014, not reversed
        if self._year >= 2014:
            self._reversed_layout = False
            return False

        # For 2012-2013, verify by sampling
        votes_reversed = 0
        votes_modern = 0

        for page in self._pages[50:min(300, len(self._pages))]:
            matches = list(_RE_TITEL_FUNC.finditer(page.text))
            if not matches:
                continue

            for m in matches[:2]:
                # Check text before Titel for amounts
                before_text = page.text[max(0, m.start() - 200):m.start()]
                before_lines = before_text.split("\n")
                before_amounts = _extract_amounts_from_lines(
                    reversed(before_lines), max_amounts=3
                )

                # Check text after Titel for amounts
                after_text = page.text[m.end():m.end() + 300]
                after_lines = after_text.split("\n")
                after_amounts = _extract_amounts_from_lines(
                    after_lines, max_amounts=3
                )

                if len(before_amounts) >= 2 and len(after_amounts) < 2:
                    votes_reversed += 1
                elif len(after_amounts) >= 2 and len(before_amounts) < 2:
                    votes_modern += 1

            if votes_reversed + votes_modern >= 5:
                break

        self._reversed_layout = votes_reversed > votes_modern
        logger.info(
            "Layout detection: reversed=%s (votes: reversed=%d, modern=%d)",
            self._reversed_layout, votes_reversed, votes_modern,
        )
        return self._reversed_layout

    # ------------------------------------------------------------------
    # Phase 1: Einzelplan section detection
    # ------------------------------------------------------------------

    def _parse_einzelplan_sections(self) -> list[tuple[str, str, int, int]]:
        """Find Einzelplan boundaries with relaxed header matching.

        Accepts headers without ``Entwurf``/``Beschluss`` prefix.
        """
        # Try the base class first (handles Entwurf/Beschluss prefixes)
        sections = super()._parse_einzelplan_sections()
        if sections:
            return sections

        # Relaxed: accept "Bundeshaushaltsplan YYYY  Einzelplan NN  Name"
        raw: list[tuple[str, str, int]] = []

        for idx, page in enumerate(self._pages):
            text = page.text[:500]
            if "Personalhaushalt" in text[:50]:
                continue

            m = _RE_EP_HEADER_RELAXED.search(text)
            if m:
                ep_num = m.group(1)
                ep_name = m.group(2).strip()
                # Avoid duplicate detections on consecutive pages
                if raw and raw[-1][0] == ep_num and idx - raw[-1][2] < 3:
                    continue
                raw.append((ep_num, ep_name, idx))

        result: list[tuple[str, str, int, int]] = []
        for i, (ep, name, start) in enumerate(raw):
            end = raw[i + 1][2] if i + 1 < len(raw) else len(self._pages)
            result.append((ep, name, start, end))

        return result

    # ------------------------------------------------------------------
    # Phase 2: parse content within one Einzelplan
    # ------------------------------------------------------------------

    def _parse_einzelplan_content(
        self,
        ep_num: str,
        start_idx: int,
        end_idx: int,
    ) -> tuple[list[dict], list[BudgetEntry], list[PersonnelEntry]]:
        """Parse Kapitel, Titel entries, and personnel for one EP.

        Delegates to reversed-layout parser for 2012-2013 or the base
        class for 2014+.
        """
        if self._detect_layout():
            return self._parse_ep_content_reversed(ep_num, start_idx, end_idx)
        return super()._parse_einzelplan_content(ep_num, start_idx, end_idx)

    # ------------------------------------------------------------------
    # Reversed-layout parser (2012-2013)
    # ------------------------------------------------------------------

    def _parse_ep_content_reversed(
        self,
        ep_num: str,
        start_idx: int,
        end_idx: int,
    ) -> tuple[list[dict], list[BudgetEntry], list[PersonnelEntry]]:
        """Parse EP content when amounts precede the Titel code."""
        kap_meta: list[dict] = []
        entries: list[BudgetEntry] = []
        personnel: list[PersonnelEntry] = []

        current_kapitel: str | None = None
        current_titelgruppe: str | None = None
        in_personal_section = False
        in_einnahmen = False
        in_flexibilisiert = False

        pages_slice = self._pages[start_idx:end_idx]

        for page in pages_slice:
            text = page.text

            # Detect Personalhaushalt section
            if _RE_PERSONAL_SECTION.search(text[:300]):
                in_personal_section = True

            # Detect Kapitel from "Überblick zum Kapitel XXXX"
            kap_match = _RE_KAP_OVERVIEW.search(text)
            if kap_match:
                kap_num = kap_match.group(1)
                if kap_num[:2] == ep_num:
                    current_kapitel = kap_num
                    current_titelgruppe = None
                    in_einnahmen = False
                    in_flexibilisiert = False
                    kap_name = self._extract_kapitel_name(text, kap_num)
                    kap_meta.append({
                        "einzelplan": ep_num,
                        "kapitel": kap_num,
                        "kapitel_text": kap_name,
                        "source_pdf": self._source_pdf,
                        "source_page": page.page_number,
                    })

            if not current_kapitel or in_personal_section:
                # In personnel sections, Kapitel changes across pages
                # without an "Überblick zum Kapitel" header, so always
                # re-detect from the page footer.
                detected_kap = self._detect_kapitel_from_page(text, ep_num)
                if detected_kap:
                    current_kapitel = detected_kap

            if not current_kapitel:
                continue

            if in_personal_section:
                pers = self._parse_personnel_page(
                    text, ep_num, current_kapitel, page.page_number
                )
                personnel.extend(pers)
                continue

            # Detect Titelgruppe
            tg_match = _RE_TITELGRUPPE.search(text)
            if tg_match:
                current_titelgruppe = tg_match.group(1)

            # Detect section context
            if "Einnahmen" in text[:200] and "Ausgaben" not in text[:200]:
                in_einnahmen = True
            if "Ausgaben" in text[:200]:
                in_einnahmen = False

            if "Flexibilisierte Ausgaben" in text or "flexibilisierte" in text.lower()[:300]:
                in_flexibilisiert = True

            # Parse Titel entries (reversed layout)
            page_entries = self._parse_titel_reversed(
                text,
                ep_num,
                current_kapitel,
                current_titelgruppe,
                in_einnahmen,
                in_flexibilisiert,
                page.page_number,
            )
            entries.extend(page_entries)

        return kap_meta, entries, personnel

    def _parse_titel_reversed(
        self,
        text: str,
        ep_num: str,
        kapitel: str,
        titelgruppe: str | None,
        in_einnahmen: bool,
        in_flexibilisiert: bool,
        page_number: int | None = None,
    ) -> list[BudgetEntry]:
        """Extract Titel entries when amounts precede the Titel code.

        Layout: amounts → description → [F|B] → ``NNN NN`` → ``-FFF``
        """
        entries: list[BudgetEntry] = []
        titel_matches = list(_RE_TITEL_FUNC_LEADING.finditer(text))

        section_is_einnahmen = in_einnahmen

        for i, m in enumerate(titel_matches):
            titel_num = m.group(1).strip()

            # Pre-block: text between previous Titel end and current Titel start
            if i == 0:
                pre_start = 0
            else:
                pre_start = titel_matches[i - 1].end()
            pre_block = text[pre_start:m.start()]

            # Post-block: text after Titel until next Titel start (for notes)
            post_end = titel_matches[i + 1].start() if i + 1 < len(titel_matches) else len(text)
            post_block = text[m.end():post_end]

            # Section header detection
            pre_lower = pre_block.lower()
            if "ausgaben" in pre_lower and "einnahmen" not in pre_lower:
                section_is_einnahmen = False
            elif "einnahmen" in pre_lower:
                section_is_einnahmen = True

            # Titel Hauptgruppe determines revenue vs expenditure
            titel_hauptgruppe = int(titel_num[0])
            is_einnahme_by_titel = titel_hauptgruppe <= 3

            # Extract amounts from pre-block (reversed layout)
            pre_lines = pre_block.split("\n")
            # Amounts are typically the last numeric lines before desc/markers
            amounts = self._extract_amounts_reversed(pre_lines)

            # Extract description from pre-block
            titel_text = self._extract_description_reversed(pre_lines)

            # Detect flags
            titel_prefix = titel_num[:3].strip()
            is_verrech = titel_prefix in _VERRECHNUNG_PREFIXES

            is_flex = in_flexibilisiert or bool(
                re.search(r"^\s*F\s*$", pre_block[-200:] if len(pre_block) > 200 else pre_block, re.MULTILINE)
            )

            is_deck = bool(
                re.search(r"^\s*B\s*$", pre_block[-200:] if len(pre_block) > 200 else pre_block, re.MULTILINE)
            ) or "deckungsfähig" in pre_block.lower()[-500:]

            entry = BudgetEntry(
                year=self._year,
                version=self._version,
                einzelplan=ep_num,
                kapitel=kapitel,
                titel=titel_num,
                titel_text=titel_text,
                titelgruppe=titelgruppe,
                is_verrechnungstitel=is_verrech,
                flexibilisiert=is_flex,
                deckungsfaehig=is_deck,
                source_pdf=self._source_pdf,
                source_page=page_number,
            )

            # Reversed layout columns: [Ist old, Soll prev, Soll current]
            # Map: amounts[-1] = Soll current year, amounts[0] = Ist oldest
            is_einnahme = is_einnahme_by_titel
            soll_idx = len(amounts) - 1  # last = current Soll
            ist_idx = 0                  # first = Ist
            if len(amounts) >= 1:
                if is_einnahme:
                    entry.einnahmen_soll = amounts[soll_idx]
                else:
                    entry.ausgaben_soll = amounts[soll_idx]
            if len(amounts) >= 3:
                if is_einnahme:
                    entry.einnahmen_ist = amounts[ist_idx]
                else:
                    entry.ausgaben_ist = amounts[ist_idx]

            entries.append(entry)

        return entries

    @staticmethod
    def _extract_amounts_reversed(lines: list[str]) -> list[float]:
        """Extract amounts from lines preceding the Titel code.

        Pre-block layout (bottom to top):
            description ← alphabetic text
            amounts     ← 1-3 numeric lines
            section hdr ← e.g. "Personalausgaben"

        We walk backwards, first skipping description text, then collecting
        the numeric amount lines above it.
        """
        amounts_rev: list[float] = []
        phase = "skip_desc"  # skip_desc → collect_amounts

        for line in reversed(lines):
            stripped = line.strip()
            if not stripped:
                if phase == "collect_amounts" and amounts_rev:
                    break
                continue

            if phase == "skip_desc":
                # Skip description text and markers (F, B)
                if stripped in {"F", "B"}:
                    continue
                # If this line is numeric, switch to collecting
                if not any(c.isalpha() for c in stripped) or re.match(
                    r"^\([\d.\s,−-]+\)\s*$", stripped
                ):
                    phase = "collect_amounts"
                    # Fall through to process this line
                else:
                    continue

            # phase == "collect_amounts"
            # Stop on alphabetic text (section header above amounts)
            if any(c.isalpha() for c in stripped):
                if not re.match(r"^\([\d.\s,−-]+\)\s*$", stripped):
                    break

            if _RE_DASH_LINE.match(stripped):
                amounts_rev.append(0.0)
            else:
                paren = re.match(r"^\((.*)\)\s*$", stripped)
                if paren:
                    inner = paren.group(1).strip()
                    if inner in {"—", "–", "-", "−", ""}:
                        amounts_rev.append(0.0)
                    else:
                        val = _parse_budget_amount(inner)
                        if val is not None:
                            amounts_rev.append(val)
                else:
                    val = _parse_budget_amount(stripped)
                    if val is not None:
                        amounts_rev.append(val)

            if len(amounts_rev) >= 3:
                break

        # Reverse to get chronological order: [oldest, previous, current]
        amounts_rev.reverse()
        return amounts_rev

    @staticmethod
    def _extract_description_reversed(lines: list[str]) -> str | None:
        """Extract description text from lines preceding the Titel code.

        In reversed layout: amounts → description → markers → Titel.
        Description lines sit between the amounts and the markers/Titel.
        """
        # Walk backwards, skip markers, skip amounts at the end
        # Description text is between the amounts and the end markers
        desc_parts: list[str] = []
        phase = "markers"  # start from end: markers → description → amounts

        for line in reversed(lines):
            stripped = line.strip()
            if not stripped:
                if phase == "description" and desc_parts:
                    break
                continue

            if phase == "markers":
                # Skip F, B markers
                if stripped in {"F", "B"}:
                    continue
                # Skip pure numbers (part of amounts)
                if re.match(r"^[\d.,\s()−-]+$", stripped):
                    continue
                # This is description text
                phase = "description"
                desc_parts.append(stripped)
            elif phase == "description":
                # Stop if we hit a number (amounts above description)
                if re.match(r"^[\d.,\s()−-]+$", stripped):
                    break
                # Stop if we hit section headers
                if stripped.lower() in {
                    "einnahmen", "ausgaben", "personalausgaben",
                    "sächliche verwaltungsausgaben", "übrige einnahmen",
                    "verwaltungseinnahmen",
                }:
                    break
                desc_parts.append(stripped)
                if len(desc_parts) >= 4:
                    break

        if desc_parts:
            desc_parts.reverse()
            return " ".join(desc_parts)[:500]
        return None

    # ------------------------------------------------------------------
    # Personnel parsing (multiline-aware)
    # ------------------------------------------------------------------

    # Grade name on its own line, trailed by dots (≥3).
    # Covers A/B/R salary grades, E/EG tariff grades, and AT.
    _RE_GRADE_LINE = re.compile(
        r"^("
        r"(?:[RBA]\s*\d+(?:\s*(?:\+\s*Z|[a-z]+(?:\+Z)?))?)"
        r"|(?:AT\s*\(?B?\)?)"
        r"|(?:EG?\s*\d+(?:\s*[a-z]+(?:\+Z)?)?)"
        r")\s*[.]{3,}\s*$",
        re.MULTILINE | re.IGNORECASE,
    )

    def _parse_personnel_page(
        self,
        text: str,
        ep_num: str,
        kapitel: str,
        page_number: int | None = None,
    ) -> list[PersonnelEntry]:
        """Parse Planstellen/Stellen rows from a personnel page.

        Handles the multiline tabular format where grade names and
        their counts are on separate lines.

        * **Reversed layout (2012-2013):** counts appear *before* the
          grade line (the line immediately preceding the grade is the
          current-year Soll).
        * **Normal layout (2014+):** counts appear *after* the grade
          line (the line immediately following the grade is the
          current-year Soll).
        """
        entries: list[PersonnelEntry] = []

        # Detect Titel context (e.g. "Titel 422 01")
        titel_match = re.search(r"Titel\s+(\d{3}\s+\d{2})", text)
        current_titel = titel_match.group(1) if titel_match else None

        lines = text.split("\n")
        reversed_layout = self._detect_layout()

        for m in self._RE_GRADE_LINE.finditer(text):
            grade = m.group(1).strip()

            # Determine which line index this grade is on
            line_start = text.rfind("\n", 0, m.start()) + 1
            line_idx = text[:line_start].count("\n")

            count: float | None = None
            if reversed_layout:
                # Count is on a preceding line; the line immediately
                # before the grade is the current-year Soll.
                for j in range(line_idx - 1, max(line_idx - 4, -1), -1):
                    if 0 <= j < len(lines):
                        val = parse_german_number(lines[j].strip())
                        if val is not None:
                            count = val
                            break
            else:
                # Count is on a following line; the line immediately
                # after the grade is the current-year Soll.
                for j in range(line_idx + 1, min(line_idx + 4, len(lines))):
                    val = parse_german_number(lines[j].strip())
                    if val is not None:
                        count = val
                        break

            if count is not None:
                entries.append(PersonnelEntry(
                    year=self._year,
                    version=self._version,
                    einzelplan=ep_num,
                    kapitel=kapitel,
                    titel=current_titel,
                    besoldungsgruppe=grade,
                    planstellen_gesamt=(
                        int(count) if count == int(count) else None
                    ),
                    source_pdf=self._source_pdf,
                    source_page=page_number,
                ))
                _classify_tarif(entries[-1])

        # Fall back to the base-class regex parser to catch any
        # entries where grade + count happen to be on the same line.
        base_entries = super()._parse_personnel_page(
            text, ep_num, kapitel, page_number
        )
        # Deduplicate: keep base entries whose grade is not already found.
        seen_grades = {e.besoldungsgruppe for e in entries}
        for be in base_entries:
            if be.besoldungsgruppe not in seen_grades:
                entries.append(be)

        return entries

    # ------------------------------------------------------------------
    # Kapitel detection (relaxed)
    # ------------------------------------------------------------------

    def _detect_kapitel_from_page(self, text: str, ep_num: str) -> str | None:
        """Detect Kapitel from page footer — handles both formats.

        2012-style: ``\\n0101\\nBundespräsident\\n``
        2020-style: ``Bundespräsident 0101`` (inline)
        """
        # Try base class first (standalone 4-digit number)
        result = super()._detect_kapitel_from_page(text, ep_num)
        if result:
            return result

        # Try inline pattern: "ministry_name NNNN" at end of page
        m = re.search(
            r"\b(\d{4})\s*$",
            text.rstrip(),
            re.MULTILINE,
        )
        if m:
            kap = m.group(1)
            if kap[:2] == ep_num and len(kap) == 4:
                return kap

        return None


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def parse_mid_era_pdf(pdf_path: Path, text_only: bool = True) -> ParsedBudget:
    """One-shot: extract and parse a consolidated budget PDF (2012-2023).

    Parameters
    ----------
    pdf_path : Path
        Path to the consolidated budget PDF.
    text_only : bool
        If True (default), skip table detection for speed.
    """
    from src.extract.pdf_extractor import PDFExtractor

    extractor = PDFExtractor(pdf_path)
    if text_only:
        doc = _extract_text_only_doc(extractor)
    else:
        doc = extractor.extract_full()
    parser = MidEraParser(doc)
    return parser.parse()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import time

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

    if len(sys.argv) > 1:
        pdf_path = Path(sys.argv[1])
    else:
        pdf_path = Path("data/budgets/2012/Haushaltsplan-2012.pdf")

    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}")
        sys.exit(1)

    print(f"Parsing {pdf_path.name} ...")
    t0 = time.time()
    budget = parse_mid_era_pdf(pdf_path)
    t1 = time.time()

    print(f"Done in {t1 - t0:.1f}s")
    print(f"Year: {budget.year}, Version: {budget.version}")
    print(f"Einzelpläne: {len(budget.einzelplan_meta)}")
    print(f"Kapitel: {len(budget.kapitel_meta)}")
    print(f"Entries: {len(budget.entries)}")
    print(f"Personnel: {len(budget.personnel)}")

    if budget.entries[:5]:
        print("\nFirst 5 entries:")
        for e in budget.entries[:5]:
            print(
                f"  ep={e.einzelplan} kap={e.kapitel} titel={e.titel} "
                f"soll={e.ausgaben_soll} ist={e.ausgaben_ist} "
                f"text={e.titel_text!r:.60} page={e.source_page}"
            )
