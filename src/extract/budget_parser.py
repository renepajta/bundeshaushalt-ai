"""Parse extracted PDF content into structured German federal budget data.

Takes an ``ExtractedDocument`` from :mod:`src.extract.pdf_extractor` and
produces a ``ParsedBudget`` containing hierarchical budget entries
(Einzelplan → Kapitel → Titel), personnel data, and reference metadata.

All monetary values are in **1 000 €** (thousands of Euros) as printed in the
budget document unless stated otherwise.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from src.extract.pdf_extractor import ExtractedDocument, ExtractedPage, parse_german_number

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class BudgetEntry:
    """One budget line item (Titel) with financial figures."""

    year: int
    version: str  # 'entwurf', 'beschluss', etc.
    einzelplan: str
    kapitel: str
    titel: str | None = None
    titel_text: str | None = None
    titelgruppe: str | None = None
    ausgaben_soll: float | None = None
    ausgaben_ist: float | None = None
    einnahmen_soll: float | None = None
    einnahmen_ist: float | None = None
    is_verrechnungstitel: bool = False
    flexibilisiert: bool = False
    deckungsfaehig: bool = False
    source_pdf: str | None = None
    source_page: int | None = None
    notes: str | None = None
    source_pdf: str | None = None
    source_page: int | None = None


@dataclass
class PersonnelEntry:
    """One row from a Planstellen-/Stellenübersicht."""

    year: int
    version: str
    einzelplan: str
    kapitel: str
    titel: str | None = None
    titelgruppe: str | None = None
    besoldungsgruppe: str | None = None
    planstellen_gesamt: int | None = None
    planstellen_tariflich: int | None = None
    planstellen_aussertariflich: int | None = None
    source_pdf: str | None = None
    source_page: int | None = None
    source_pdf: str | None = None
    source_page: int | None = None


@dataclass
class ParsedBudget:
    """Complete parse result for a single budget document."""

    source_file: str
    year: int
    version: str
    einzelplan_meta: list[dict] = field(default_factory=list)
    kapitel_meta: list[dict] = field(default_factory=list)
    entries: list[BudgetEntry] = field(default_factory=list)
    personnel: list[PersonnelEntry] = field(default_factory=list)
    verpflichtungen: list[dict] = field(default_factory=list)
    sachverhalte: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Regex patterns for German budget documents
# ---------------------------------------------------------------------------

# Match "Einzelplan 01" on its own or in a header context
_RE_EP_HEADER = re.compile(
    r"Einzelplan\s+(\d{2})\s*\n\s*(.+?)(?:\s*\n|$)", re.MULTILINE
)

# Match "Überblick zum Einzelplan XX" — summary table header
_RE_EP_OVERVIEW = re.compile(
    r"[ÜU]berblick\s+zum\s+Einzelplan\s+(\d{2})", re.IGNORECASE
)

# Match "Überblick zum Kapitel XXXX" — chapter summary header
_RE_KAP_OVERVIEW = re.compile(
    r"[ÜU]berblick\s+zum\s+Kapitel\s+(\d{4})", re.IGNORECASE
)

# Kapitel from page footer like "- 13 -\nZentral veranschlagte...\n0111"
_RE_KAP_FOOTER = re.compile(r"^(\d{4})\s*$", re.MULTILINE)

# Titel pattern: "NNN NN" (3 digits space 2 digits) — main budget line
_RE_TITEL = re.compile(
    r"^(\d{3}\s+\d{2})\s*$", re.MULTILINE
)

# Titel with function code: "NNN NN\n-NNN\n" in the text stream
_RE_TITEL_FUNC = re.compile(
    r"(\d{3}\s+\d{2})\s*\n\s*-(\d{3})\s*\n"
)

# Titelgruppe header: "Tgr. NN" or "Titelgruppe NN"
_RE_TITELGRUPPE = re.compile(
    r"(?:Tgr\.\s*|Titelgruppe\s+)(\d{2})\b"
)

# Personalhaushalt section start
_RE_PERSONAL_SECTION = re.compile(
    r"Personalhaushalt\s+Einzelplan\s+(\d{2})"
)

# Planstellen overview table header
_RE_PLANSTELLEN = re.compile(
    r"Planstellen-?/?Stellen[üu]bersicht", re.IGNORECASE
)

# Besoldungsgruppe pattern in personnel pages
_RE_BESOLDUNG = re.compile(
    r"^((?:[RBA]\s*\d+(?:\s*[a-z]+)?)|(?:AT\s*\(?B?\)?)|(?:E\s*\d+\s*[a-z]?)|(?:EG\s*\d+))"
    r"\s*[.\s]*\s+"
    r"([\d.,]+)",
    re.MULTILINE | re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Personnel grade classification helpers
# ---------------------------------------------------------------------------

_RE_TARIF_GRADE = re.compile(r"^E(?:G)?\s*\d", re.IGNORECASE)
_RE_AT_GRADE = re.compile(r"^AT\b", re.IGNORECASE)


def _classify_tarif(entry: PersonnelEntry) -> None:
    """Set ``planstellen_tariflich`` / ``planstellen_aussertariflich`` from grade.

    * E / EG grades → tariflich (Tarifbeschäftigte)
    * AT grades     → außertariflich
    * A / B / R     → Beamte — left as ``None``
    """
    bes = (entry.besoldungsgruppe or "").strip()
    if not bes or entry.planstellen_gesamt is None:
        return
    if _RE_TARIF_GRADE.match(bes):
        entry.planstellen_tariflich = entry.planstellen_gesamt
        entry.planstellen_aussertariflich = 0
    elif _RE_AT_GRADE.match(bes):
        entry.planstellen_tariflich = 0
        entry.planstellen_aussertariflich = entry.planstellen_gesamt


# Detect Verrechnungstitel by Titel number in range 381, 981, 982, etc.
_VERRECHNUNG_PREFIXES = {"381", "981", "982", "983", "984", "985", "986"}

# Detect flexibilised titles (marked with 'F' after the function code)
_RE_FLEX_MARKER = re.compile(r"\bF\b")

# Year detection from text
_RE_YEAR = re.compile(r"Haushaltsjahr\s+(\d{4})")
_RE_YEAR_PLAN = re.compile(r"Bundeshaushaltsplan\s+(\d{4})")
_RE_YEAR_HG = re.compile(r"Haushaltsgesetz\s+(\d{4})")

# Version detection
_RE_VERSION_ENTWURF = re.compile(r"\bEntwurf\b", re.IGNORECASE)
_RE_VERSION_BESCHLUSS = re.compile(r"\bBeschluss\b", re.IGNORECASE)
_RE_VERSION_NACHTRAG = re.compile(r"\bNachtrag\b", re.IGNORECASE)

# Gesamtplan overview with Einzelplan summaries
_RE_GESAMTPLAN_AUSGABEN = re.compile(
    r"Gesamtplan.*Ausgaben", re.IGNORECASE | re.DOTALL
)

# Number in budget lines — amounts appear as single numbers on lines
_RE_AMOUNT = re.compile(r"^\s*([\d.]+(?:,\d+)?)\s*$", re.MULTILINE)

# Kapitel line in a Titel page (bottom of text, e.g. "0111")
_RE_KAP_IN_TEXT = re.compile(r"(?:^|\n)\s*(\d{4})\s*\n\s*\S", re.MULTILINE)

# VE (Verpflichtungsermächtigung) section
_RE_VE_SECTION = re.compile(
    r"Verpflichtungserm[äa]chtigungs?(?:en)?", re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class BudgetParser:
    """Parse an ``ExtractedDocument`` into structured budget data."""

    def __init__(self, extracted_doc: ExtractedDocument) -> None:
        self.doc = extracted_doc
        self._pages = extracted_doc.pages
        self._source_pdf = extracted_doc.source_path.name
        self._year: int = 0
        self._version: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self) -> ParsedBudget:
        """Parse the full document into structured budget data."""
        logger.info(
            "Parsing %d pages from %s",
            len(self._pages),
            self.doc.source_path.name,
        )

        self._year, self._version = self._detect_document_info()
        logger.info("Detected: year=%d, version=%s", self._year, self._version)

        # Phase 1: find Einzelplan boundaries and metadata
        ep_sections = self._parse_einzelplan_sections()
        ep_meta = [
            {"einzelplan": ep, "einzelplan_text": txt, "source_pdf": self._source_pdf, "source_page": self._pages[start].page_number if start < len(self._pages) else None}
            for ep, txt, start, _ in ep_sections
        ]
        logger.info("Found %d Einzelpläne", len(ep_meta))

        # Phase 2: parse Kapitel metadata and Titel entries within each EP
        kap_meta: list[dict] = []
        entries: list[BudgetEntry] = []
        personnel: list[PersonnelEntry] = []

        for ep_num, ep_text, start_page, end_page in ep_sections:
            kaps, ents, pers = self._parse_einzelplan_content(
                ep_num, start_page, end_page
            )
            kap_meta.extend(kaps)
            entries.extend(ents)
            personnel.extend(pers)

        # Phase 3: parse Gesamtplan summary entries if present
        gp_entries = self._parse_gesamtplan()
        entries.extend(gp_entries)

        result = ParsedBudget(
            source_file=str(self.doc.source_path.name),
            year=self._year,
            version=self._version,
            einzelplan_meta=ep_meta,
            kapitel_meta=kap_meta,
            entries=entries,
            personnel=personnel,
        )

        logger.info(
            "Parse complete: %d EP, %d Kapitel, %d entries, %d personnel",
            len(result.einzelplan_meta),
            len(result.kapitel_meta),
            len(result.entries),
            len(result.personnel),
        )
        return result

    # ------------------------------------------------------------------
    # Phase 0: document metadata detection
    # ------------------------------------------------------------------

    def _detect_document_info(self) -> tuple[int, str]:
        """Detect year and version from the first pages of the document."""
        # Sample the first 30 pages for metadata
        sample_text = "\n".join(
            p.text for p in self._pages[:min(30, len(self._pages))]
        )

        # Detect year
        year = 0
        for pattern in [_RE_YEAR_PLAN, _RE_YEAR_HG, _RE_YEAR]:
            m = pattern.search(sample_text)
            if m:
                year = int(m.group(1))
                break
        if not year:
            logger.warning("Could not detect budget year, defaulting to 2026")
            year = 2026

        # Detect version
        version = "entwurf"  # default
        if _RE_VERSION_NACHTRAG.search(sample_text):
            version = "nachtrag"
        elif _RE_VERSION_BESCHLUSS.search(sample_text):
            version = "beschluss"
        elif _RE_VERSION_ENTWURF.search(sample_text):
            version = "entwurf"

        return year, version

    # ------------------------------------------------------------------
    # Phase 1: Einzelplan section detection
    # ------------------------------------------------------------------

    def _parse_einzelplan_sections(self) -> list[tuple[str, str, int, int]]:
        """Find Einzelplan boundaries.

        Returns list of (einzelplan_number, name, start_page_idx, end_page_idx).
        """
        sections: list[tuple[str, str, int]] = []  # (ep, name, page_idx)

        for idx, page in enumerate(self._pages):
            text = page.text[:400]
            # Look for "Entwurf zum Bundeshaushaltsplan YYYY Einzelplan XX Name"
            m = re.search(
                r"(?:Entwurf|Beschluss|Nachtrag)\s+"
                r"(?:zum\s+)?Bundeshaushaltsplan\s+\d{4}\s+"
                r"Einzelplan\s+(\d{2})\s*\n\s*(.+?)(?:\s*\n|Inhalt)",
                text,
                re.DOTALL,
            )
            if m:
                ep_num = m.group(1)
                ep_name = m.group(2).strip()
                # Skip Personalhaushalt section starts
                if "Personalhaushalt" not in text[:50]:
                    sections.append((ep_num, ep_name, idx))

        # Build (ep, name, start, end) tuples
        result: list[tuple[str, str, int, int]] = []
        for i, (ep, name, start) in enumerate(sections):
            if i + 1 < len(sections):
                end = sections[i + 1][2]
            else:
                end = len(self._pages)
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
        """Parse Kapitel, Titel entries, and personnel for one Einzelplan."""
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

                    # Extract Kapitel name from context
                    kap_name = self._extract_kapitel_name(text, kap_num)
                    kap_meta.append({
                        "einzelplan": ep_num,
                        "kapitel": kap_num,
                        "kapitel_text": kap_name,
                        "source_pdf": self._source_pdf,
                        "source_page": page.page_number,
                    })

            if not current_kapitel or in_personal_section:
                # Try to detect kapitel from page content.
                # In personnel sections, Kapitel changes across pages
                # without an "Überblick zum Kapitel" header, so always
                # re-detect from the page footer.
                detected_kap = self._detect_kapitel_from_page(text, ep_num)
                if detected_kap:
                    current_kapitel = detected_kap

            if not current_kapitel:
                continue

            if in_personal_section:
                # Parse personnel entries from Planstellen pages
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

            # Parse Titel entries from this page
            page_entries = self._parse_titel_from_text(
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

    # ------------------------------------------------------------------
    # Titel parsing from text
    # ------------------------------------------------------------------

    def _parse_titel_from_text(
        self,
        text: str,
        ep_num: str,
        kapitel: str,
        titelgruppe: str | None,
        in_einnahmen: bool,
        in_flexibilisiert: bool,
        page_number: int | None = None,
    ) -> list[BudgetEntry]:
        """Extract Titel entries from a page's text.

        Each Titel line follows the pattern:
            NNN NN
            -FFF           (function code)
            [F|B]          (optional flex/deckungsfähig marker)
            Description text
            amount_soll_2026
            (whitespace)
            amount_soll_2025
            (whitespace)
            amount_ist_2024
        """
        entries: list[BudgetEntry] = []

        # Find all Titel patterns with their positions
        titel_matches = list(_RE_TITEL_FUNC.finditer(text))

        # Track Einnahmen/Ausgaben context through the page.
        # Section headers appear as standalone lines before Titel blocks.
        section_is_einnahmen = in_einnahmen

        for i, m in enumerate(titel_matches):
            titel_num = m.group(1).strip()
            # Determine the text block for this Titel (until next Titel or end)
            start_pos = m.end()
            if i + 1 < len(titel_matches):
                end_pos = titel_matches[i + 1].start()
            else:
                end_pos = len(text)

            # Check text between previous and current Titel for section headers
            if i == 0:
                preamble = text[: m.start()]
            else:
                preamble = text[titel_matches[i - 1].end() : m.start()]

            preamble_lower = preamble.lower()
            if "ausgaben" in preamble_lower and "einnahmen" not in preamble_lower:
                section_is_einnahmen = False
            elif "einnahmen" in preamble_lower:
                section_is_einnahmen = True

            # Titel numbers in ranges 1xx–3xx are revenue; 4xx–9xx expenditure
            titel_hauptgruppe = int(titel_num[0])
            is_einnahme_by_titel = titel_hauptgruppe <= 3

            block = text[start_pos:end_pos]

            # Extract description (first substantial line after function code)
            titel_text = self._extract_titel_description(block)

            # Extract amounts from the block
            amounts = self._extract_amounts_from_block(block)

            # Detect Verrechnungstitel
            titel_prefix = titel_num[:3].strip()
            is_verrech = titel_prefix in _VERRECHNUNG_PREFIXES

            # Detect flexibilisation marker ("F" on its own line)
            is_flex = in_flexibilisiert or bool(
                re.search(r"^\s*F\s*$", block[:100], re.MULTILINE)
            )

            # Detect deckungsfähig marker ("B" on its own line)
            is_deck = bool(
                re.search(r"^\s*B\s*$", block[:100], re.MULTILINE)
            ) or "deckungsfähig" in block.lower()[:500]

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

            # Assign amounts: Titel Hauptgruppe is the authoritative signal.
            # Section headers serve as fallback context only.
            is_einnahme = is_einnahme_by_titel
            if len(amounts) >= 1:
                if is_einnahme:
                    entry.einnahmen_soll = amounts[0]
                else:
                    entry.ausgaben_soll = amounts[0]

            if len(amounts) >= 3:
                if is_einnahme:
                    entry.einnahmen_ist = amounts[2]
                else:
                    entry.ausgaben_ist = amounts[2]

            entries.append(entry)

        return entries

    def _extract_titel_description(self, block: str) -> str | None:
        """Extract the description text for a Titel from its text block."""
        lines = block.split("\n")
        desc_parts: list[str] = []
        started = False

        for line in lines:
            stripped = line.strip()
            if not stripped:
                if started:
                    break
                continue

            # Skip pure numbers (amounts)
            if re.match(r"^[\d.,\s()−-]+$", stripped):
                if started:
                    break
                continue

            # Skip short markers like "F", "B"
            if len(stripped) <= 2 and stripped.isalpha():
                continue

            # This looks like description text
            if not started:
                started = True
            desc_parts.append(stripped)

            # Don't collect too much
            if len(desc_parts) >= 4:
                break

        if desc_parts:
            return " ".join(desc_parts)[:500]
        return None

    def _extract_amounts_from_block(self, block: str) -> list[float]:
        """Extract up to 3 financial amounts from a Titel's text block.

        Returns [soll_2026, soll_2025, ist_2024] where available.

        PDF amounts appear as space-separated digit groups like ``1 348``
        (meaning 1,348 in 1 000 € units) or as dashes ``-`` for zero.
        Parenthesised values like ``(4)`` indicate the prior-year actual.
        """
        amounts: list[float] = []
        lines = block.split("\n")

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Skip description text and annotations
            if any(c.isalpha() for c in stripped) and not re.match(
                r"^\([\d.\s,−-]+\)\s*$", stripped
            ):
                continue

            # Handle dash = zero/no value
            if stripped in {"-", "–", "—", "−"}:
                amounts.append(0.0)
                if len(amounts) >= 3:
                    break
                continue

            # Handle parenthesised amounts like "(4)" or "(1.200)" or "(-)"
            paren_match = re.match(r"^\((.*)\)\s*$", stripped)
            if paren_match:
                inner = paren_match.group(1).strip()
                if inner in {"-", "–", "—", "−", ""}:
                    amounts.append(0.0)
                else:
                    inner_val = self._parse_budget_amount(inner)
                    if inner_val is not None:
                        amounts.append(inner_val)
                if len(amounts) >= 3:
                    break
                continue

            # Try to parse as a budget amount (may have spaces between groups)
            val = self._parse_budget_amount(stripped)
            if val is not None:
                amounts.append(val)
                if len(amounts) >= 3:
                    break

        return amounts

    @staticmethod
    def _parse_budget_amount(text: str) -> float | None:
        """Parse a budget amount that may have thin spaces between groups.

        Examples: ``1 348``, ``16 161 139``, ``102,03``, ``-500``
        """
        cleaned = text.strip()
        if not cleaned:
            return None

        # Remove thin/non-breaking spaces that separate digit groups
        # These appear as regular spaces in extracted text: "1 348" → "1348"
        # But don't collapse if it's a standard German number with dots
        if "." in cleaned or "," in cleaned:
            return parse_german_number(cleaned)

        # Try collapsing spaces for space-separated digit groups
        collapsed = re.sub(r"\s+", "", cleaned)
        if collapsed.lstrip("-−").isdigit():
            try:
                return float(collapsed)
            except ValueError:
                return None

        return parse_german_number(cleaned)

    # ------------------------------------------------------------------
    # Kapitel helpers
    # ------------------------------------------------------------------

    def _extract_kapitel_name(self, text: str, kap_num: str) -> str | None:
        """Extract Kapitel name from an overview page."""
        # Look for "Vorbemerkung\n...\ntext about the Kapitel"
        # or from the table-of-contents style header
        lines = text.split("\n")
        for i, line in enumerate(lines):
            if kap_num in line and i > 0:
                # Check preceding lines for a name/description
                for j in range(max(0, i - 5), i):
                    candidate = lines[j].strip()
                    if (
                        len(candidate) > 10
                        and not candidate.startswith("Soll")
                        and not candidate.startswith("Ist")
                        and not re.match(r"^\d", candidate)
                        and "1 000" not in candidate
                    ):
                        return candidate[:200]
        return None

    def _detect_kapitel_from_page(self, text: str, ep_num: str) -> str | None:
        """Try to detect the current Kapitel from page markers."""
        # Look for 4-digit kapitel numbers at the bottom of pages
        # Pattern: "- NN -\n<text>\nNNNN"
        matches = list(_RE_KAP_FOOTER.findall(text))
        for kap in matches:
            if kap[:2] == ep_num and len(kap) == 4:
                return kap
        return None

    # ------------------------------------------------------------------
    # Personnel parsing
    # ------------------------------------------------------------------

    def _parse_personnel_page(
        self,
        text: str,
        ep_num: str,
        kapitel: str,
        page_number: int | None = None,
    ) -> list[PersonnelEntry]:
        """Parse Planstellen/Stellen rows from a personnel page."""
        entries: list[PersonnelEntry] = []

        # Detect which Titel context (e.g. "Titel 422 01")
        titel_match = re.search(r"Titel\s+(\d{3}\s+\d{2})", text)
        current_titel = titel_match.group(1) if titel_match else None

        # Look for Besoldungsgruppe lines with counts
        for m in _RE_BESOLDUNG.finditer(text):
            bes_gr = m.group(1).strip()
            count_str = m.group(2).strip()
            count = parse_german_number(count_str)
            if count is not None:
                entries.append(PersonnelEntry(
                    year=self._year,
                    version=self._version,
                    einzelplan=ep_num,
                    kapitel=kapitel,
                    titel=current_titel,
                    besoldungsgruppe=bes_gr,
                    planstellen_gesamt=int(count) if count == int(count) else None,
                    source_pdf=self._source_pdf,
                    source_page=page_number,
                ))
                _classify_tarif(entries[-1])

        return entries

    # ------------------------------------------------------------------
    # Gesamtplan parsing
    # ------------------------------------------------------------------

    def _parse_gesamtplan(self) -> list[BudgetEntry]:
        """Parse the Gesamtplan summary tables (Ausgaben by Einzelplan)."""
        entries: list[BudgetEntry] = []

        # The Gesamtplan is typically in the first ~150 pages
        search_limit = min(150, len(self._pages))

        for page in self._pages[:search_limit]:
            text = page.text
            if "Gesamtplan" not in text[:200]:
                continue

            # Parse the B. Ausgaben table from text
            if "B. Ausgaben" in text or "Summe Ausgaben" in text:
                gp_entries = self._parse_gesamtplan_ausgaben(text, page.tables, page.page_number)
                entries.extend(gp_entries)

            # Parse A. Einnahmen table
            if "A. Einnahmen" in text or "Summe Einnahmen" in text:
                gp_entries = self._parse_gesamtplan_einnahmen(text, page.tables, page.page_number)
                entries.extend(gp_entries)

        return entries

    def _parse_gesamtplan_ausgaben(
        self, text: str, tables: list[list[list[str]]], page_number: int | None = None
    ) -> list[BudgetEntry]:
        """Parse Gesamtplan Ausgaben (expenditure overview by EP)."""
        entries: list[BudgetEntry] = []

        # The Gesamtplan tables in these PDFs are typically extracted as a
        # single merged cell.  Parse from text instead using line patterns.
        # Lines like: "01  Bundespräsident...  67 388  58 940  +8 448"
        pattern = re.compile(
            r"^(\d{2})\s+(.+?)\s{2,}"  # EP number + name
            r"([\d.\s]+?)(?:\s{2,}|$)",    # amounts
            re.MULTILINE,
        )

        for m in pattern.finditer(text):
            ep_num = m.group(1)
            # The amounts in the Gesamtplan are separated by spaces
            # We just capture the first amount as Soll 2026
            amount_text = m.group(3).strip()
            val = parse_german_number(amount_text.replace(" ", ""))
            if val is not None:
                entries.append(BudgetEntry(
                    year=self._year,
                    version=self._version,
                    einzelplan=ep_num,
                    kapitel="",
                    titel=None,
                    titel_text=f"Gesamtausgaben EP {ep_num}",
                    ausgaben_soll=val,
                    notes="gesamtplan_summary",
                    source_pdf=self._source_pdf,
                    source_page=page_number,
                ))

        return entries

    def _parse_gesamtplan_einnahmen(
        self, text: str, tables: list[list[list[str]]], page_number: int | None = None
    ) -> list[BudgetEntry]:
        """Parse Gesamtplan Einnahmen (revenue overview by EP)."""
        entries: list[BudgetEntry] = []

        pattern = re.compile(
            r"^(\d{2})\s+(.+?)\s{2,}"
            r"([\d.\s]+?)(?:\s{2,}|$)",
            re.MULTILINE,
        )

        for m in pattern.finditer(text):
            ep_num = m.group(1)
            amount_text = m.group(3).strip()
            val = parse_german_number(amount_text.replace(" ", ""))
            if val is not None:
                entries.append(BudgetEntry(
                    year=self._year,
                    version=self._version,
                    einzelplan=ep_num,
                    kapitel="",
                    titel=None,
                    titel_text=f"Gesamteinnahmen EP {ep_num}",
                    einnahmen_soll=val,
                    notes="gesamtplan_summary",
                    source_pdf=self._source_pdf,
                    source_page=page_number,
                ))

        return entries


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def _parse_budget_pdf_modern(pdf_path: Path, text_only: bool = False) -> ParsedBudget:
    """Direct modern-era parse (2024+). Used internally by the router."""
    from src.extract.pdf_extractor import PDFExtractor

    extractor = PDFExtractor(pdf_path)
    if text_only:
        doc = _extract_text_only_doc(extractor)
    else:
        doc = extractor.extract_full()
    parser = BudgetParser(doc)
    return parser.parse()


def parse_budget_pdf(pdf_path: Path, text_only: bool = False, year: int | None = None, version: str = 'soll') -> ParsedBudget:
    """One-shot: extract PDF and parse into structured budget data.

    Uses the parser router to auto-detect era and dispatch appropriately.
    """
    from src.extract.parser_router import BudgetParserRouter
    router = BudgetParserRouter()
    return router.parse(pdf_path, year=year, version=version)


def _extract_text_only_doc(extractor) -> ExtractedDocument:
    """Fast text-only extraction — creates an ExtractedDocument without tables."""
    import fitz

    pages: list[ExtractedPage] = []
    with fitz.open(str(extractor.pdf_path)) as doc:
        total = len(doc)
        for i in range(total):
            try:
                text = doc[i].get_text("text")
            except Exception:
                text = ""
            pages.append(ExtractedPage(page_number=i + 1, text=text, tables=[]))

    return ExtractedDocument(
        source_path=extractor.pdf_path,
        total_pages=len(pages),
        pages=pages,
        metadata={},
    )


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

    pdf_path = Path(__file__).resolve().parent.parent.parent / "docs" / "0350-25.pdf"
    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}")
        sys.exit(1)

    print(f"Extracting PDF (text-only): {pdf_path.name} ...")
    t0 = time.time()
    from src.extract.pdf_extractor import PDFExtractor

    extractor = PDFExtractor(pdf_path)
    doc = _extract_text_only_doc(extractor)
    t1 = time.time()
    print(f"Extraction took {t1 - t0:.1f}s ({doc.total_pages} pages)")

    print("Parsing budget structure ...")
    parser = BudgetParser(doc)
    budget = parser.parse()
    t2 = time.time()
    print(f"Parsing took {t2 - t1:.1f}s")

    print(f"\n=== Parse Results ===")
    print(f"Year:          {budget.year}")
    print(f"Version:       {budget.version}")
    print(f"Source:        {budget.source_file}")
    print(f"Einzelpläne:   {len(budget.einzelplan_meta)}")
    print(f"Kapitel:       {len(budget.kapitel_meta)}")
    print(f"Budget entries:{len(budget.entries)}")
    print(f"Personnel:     {len(budget.personnel)}")

    if budget.einzelplan_meta:
        print(f"\nEinzelpläne:")
        for ep in budget.einzelplan_meta:
            print(f"  EP {ep['einzelplan']}: {ep['einzelplan_text']}")

    if budget.kapitel_meta:
        print(f"\nFirst 10 Kapitel:")
        for kap in budget.kapitel_meta[:10]:
            print(f"  Kap {kap['kapitel']}: {kap.get('kapitel_text', '?')}")

    if budget.entries:
        with_a = sum(1 for e in budget.entries if e.ausgaben_soll)
        with_e = sum(1 for e in budget.entries if e.einnahmen_soll)
        print(f"\nEntries with ausgaben_soll: {with_a}")
        print(f"Entries with einnahmen_soll: {with_e}")
        print(f"\nSample entries (first 10 with amounts):")
        count = 0
        for e in budget.entries:
            if e.ausgaben_soll or e.einnahmen_soll:
                a = e.ausgaben_soll or 0
                ei = e.einnahmen_soll or 0
                t = (e.titel_text or "")[:50]
                print(f"  EP{e.einzelplan}/{e.kapitel}/{e.titel}: A={a} E={ei} {t}")
                count += 1
                if count >= 10:
                    break

    if budget.personnel:
        print(f"\nSample personnel (first 10):")
        for p in budget.personnel[:10]:
            print(f"  EP{p.einzelplan}/{p.kapitel} {p.besoldungsgruppe}: {p.planstellen_gesamt}")
