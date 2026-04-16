"""Parser for per-Einzelplan budget PDFs from 2005–2011.

Each year in this era has 24-25 individual PDF files in ``data/budgets/{year}/``,
named ``Epl01.pdf`` … ``Epl23.pdf`` (plus ``Vorspann.pdf``, ``HG.pdf``, etc.).
Every Epl PDF covers **one** Einzelplan.

Amounts are in **1 000 €** as printed.  Three amount columns per entry:
Soll current year | Soll prior year | Ist two-years-ago.

Two distinct text-extraction layouts exist:
  * **Layout A (2005–2006):** Titel code → function → description → amounts
  * **Layout B (2007–2011):** amounts → description → [F] → Titel code → function
The parser auto-detects which layout is used on each page.
"""

from __future__ import annotations

import logging
import re
from dataclasses import field
from pathlib import Path

import fitz  # PyMuPDF

from src.extract.budget_parser import BudgetEntry, PersonnelEntry, ParsedBudget
from src.extract.pdf_extractor import parse_german_number

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Titel code:  "NNN NN" on its own line (optionally prefixed with F on same line)
_RE_TITEL_CODE = re.compile(
    r"^[F ]?\s*(\d{3})\s+(\d{2})\s*$", re.MULTILINE
)

# Titel code followed by function code on next line
_RE_TITEL_FUNC = re.compile(
    r"(?:^|\n)\s*F?\s*(\d{3}\s+\d{2})\s*\n\s*-\s*(\d{3})\s*(?:\n|$)"
)

# Year on title page
_RE_YEAR = re.compile(r"Bundeshaushaltsplan\s+(\d{4})")

# Einzelplan on title page
_RE_EP = re.compile(r"Einzelplan\s+(\d{2})")

# Kapitel number (4 digits, first 2 = Einzelplan)
_RE_KAP_MARKER = re.compile(r"(?:^|\n)\s*(\d{4})\s*(?:\n|$)")

# Personalhaushalt section
_RE_PERSONAL = re.compile(r"Personalhaushalt", re.IGNORECASE)

# Planstellen/Stellenübersicht
_RE_PLANSTELLEN = re.compile(r"Planstellen.*[Üü]bersicht", re.IGNORECASE)

# Besoldungsgruppe with count
_RE_BESOLDUNG = re.compile(
    r"^((?:[BA]\s*\d+(?:\s*[a-z+]+)?)|(?:AT(?:\s*\(?B?\))?)|(?:E\s*\d+\s*[a-z]?)|(?:EG\s*\d+))"
    r"\s*[.\s]*\s+"
    r"([\d.,]+)",
    re.MULTILINE | re.IGNORECASE,
)

# Verpflichtungsermächtigung
_RE_VE = re.compile(r"Verpflichtungserm[äa]chtigung", re.IGNORECASE)

# "davon fällig" year-amount pairs
_RE_DAVON_FAELLIG = re.compile(
    r"(\d{4})\s*(?:bis)?\s*(?:\d{4})?\s*[:.]?\s*([\d.\s]+)", re.MULTILINE
)

# Abschluss des Kapitels
_RE_KAP_ABSCHLUSS = re.compile(r"Abschluss\s+des\s+Kapitels\s+(\d{4})", re.IGNORECASE)

# Section headers
_RE_EINNAHMEN_HEADER = re.compile(r"^\s*Einnahmen\s*$", re.MULTILINE)
_RE_AUSGABEN_HEADER = re.compile(r"^\s*Ausgaben\s*$", re.MULTILINE)

# Amount-only line (digits, dots, spaces, dashes)
_RE_AMOUNT_LINE = re.compile(r"^\s*([\d]+(?:[\s.]\d+)*)\s*$")
_RE_DASH_LINE = re.compile(r"^\s*[-–—−]\s*$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_budget_amount(text: str) -> float | None:
    """Parse a budget amount that may use spaces between digit groups."""
    cleaned = text.strip()
    if not cleaned:
        return None
    if cleaned in {"—", "–", "-", "−"}:
        return 0.0

    # Remove spaces between digit groups: "76 104" → "76104"
    if "." in cleaned or "," in cleaned:
        return parse_german_number(cleaned)

    collapsed = re.sub(r"\s+", "", cleaned)
    if collapsed.lstrip("-−").isdigit():
        try:
            return float(collapsed)
        except ValueError:
            return None

    return parse_german_number(cleaned)


def _extract_amounts_from_lines(lines: list[str], max_amounts: int = 3) -> list[float]:
    """Extract up to *max_amounts* numeric values from consecutive lines."""
    amounts: list[float] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        # Skip lines with alphabetic chars (descriptions, annotations)
        if any(c.isalpha() for c in stripped):
            # Allow parenthesized amounts like "(4)"
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


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class EarlyEraParser:
    """Parser for per-Einzelplan budget PDFs (2005-2011).

    Each PDF covers one Einzelplan. Multiple PDFs per year.
    Amounts are in 1,000 €.
    """

    def parse(
        self,
        pdf_path: str | Path,
        year: int | None = None,
        version: str = "soll",
    ) -> ParsedBudget:
        """Parse a single Einzelplan PDF into structured budget data.

        Parameters
        ----------
        pdf_path : path to the Epl PDF
        year : budget year override (auto-detected from title page if *None*)
        version : budget version label (default ``'soll'``)
        """
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        logger.info("Parsing %s", pdf_path.name)

        # Extract text per page
        pages_text: list[str] = []
        with fitz.open(str(pdf_path)) as doc:
            for page in doc:
                try:
                    pages_text.append(page.get_text("text"))
                except Exception:
                    logger.warning("Text extraction failed on page %d", page.number + 1)
                    pages_text.append("")

        # --- Detect year ---
        detected_year = self._detect_year(pages_text)
        if detected_year:
            year = detected_year
        elif year is None:
            # Try parent directory name
            try:
                year = int(pdf_path.parent.name)
            except ValueError:
                raise ValueError(
                    f"Cannot determine budget year for {pdf_path.name}"
                )

        # --- Detect Einzelplan ---
        einzelplan = self._detect_einzelplan(pages_text)
        if einzelplan is None:
            raise ValueError(
                f"Cannot detect Einzelplan number from {pdf_path.name}"
            )

        # --- Detect ministry name ---
        ministry = self._detect_ministry(pages_text, einzelplan)

        ep_meta = [
            {
                "einzelplan": einzelplan,
                "einzelplan_text": ministry or "",
                "source_pdf": pdf_path.name,
                "source_page": 1,
            }
        ]

        # --- Parse Kapitel sections ---
        kap_meta = self._parse_kapitel_sections(pages_text, einzelplan)

        # --- Parse Titel entries ---
        entries = self._parse_titel_entries(pages_text, einzelplan, pdf_path.name)

        # --- Parse personnel ---
        personnel = self._parse_personnel(pages_text, einzelplan, pdf_path.name, year, version)

        # --- Parse VE ---
        ve = self._parse_verpflichtungen(pages_text, einzelplan)

        # Stamp year/version/source on all entries
        for e in entries:
            e.year = year
            e.version = version
            e.source_pdf = pdf_path.name

        result = ParsedBudget(
            source_file=pdf_path.name,
            year=year,
            version=version,
            einzelplan_meta=ep_meta,
            kapitel_meta=kap_meta,
            entries=entries,
            personnel=personnel,
            verpflichtungen=ve,
        )

        logger.info(
            "Parsed %s: %d entries, %d personnel, %d kapitel",
            pdf_path.name,
            len(entries),
            len(personnel),
            len(kap_meta),
        )
        return result

    # ------------------------------------------------------------------
    # Detection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_year(pages_text: list[str]) -> int | None:
        for text in pages_text[:3]:
            m = _RE_YEAR.search(text)
            if m:
                return int(m.group(1))
        return None

    @staticmethod
    def _detect_einzelplan(pages_text: list[str]) -> str | None:
        for text in pages_text[:3]:
            m = _RE_EP.search(text)
            if m:
                return m.group(1)
        return None

    @staticmethod
    def _detect_ministry(pages_text: list[str], ep: str) -> str | None:
        """Extract ministry name from title page (line after 'Einzelplan NN')."""
        for text in pages_text[:2]:
            m = re.search(
                rf"Einzelplan\s+{re.escape(ep)}\s*\n\s*(.+?)(?:\s*\n)",
                text,
            )
            if m:
                name = m.group(1).strip()
                if name and name != "Inhalt" and len(name) > 3:
                    return name
        return None

    # ------------------------------------------------------------------
    # Kapitel section parsing
    # ------------------------------------------------------------------

    def _parse_kapitel_sections(
        self, pages_text: list[str], einzelplan: str
    ) -> list[dict]:
        """Find chapter headers and their page ranges."""
        kap_meta: list[dict] = []
        seen: set[str] = set()

        for page_idx, text in enumerate(pages_text):
            # "Abschluss des Kapitels NNNN" is a definitive chapter marker
            for m in _RE_KAP_ABSCHLUSS.finditer(text):
                kap = m.group(1)
                if kap[:2] == einzelplan and kap not in seen:
                    # Find chapter name from earlier in the document
                    name = self._find_kapitel_name(pages_text, kap, einzelplan)
                    kap_meta.append({
                        "einzelplan": einzelplan,
                        "kapitel": kap,
                        "kapitel_text": name,
                        "source_page": page_idx + 1,
                    })
                    seen.add(kap)

            # Also detect from page headers / footers: "NNNN" + chapter name
            # on pages that have the Titel table header
            if "Z w e c k b e s t i m m u n g" in text or "Zweckbestimmung" in text:
                kap = self._detect_kapitel_on_page(text, einzelplan)
                if kap and kap not in seen:
                    name = self._find_kapitel_name(pages_text, kap, einzelplan)
                    kap_meta.append({
                        "einzelplan": einzelplan,
                        "kapitel": kap,
                        "kapitel_text": name,
                        "source_page": page_idx + 1,
                    })
                    seen.add(kap)

        return kap_meta

    @staticmethod
    def _detect_kapitel_on_page(text: str, einzelplan: str) -> str | None:
        """Detect the active Kapitel from page margins."""
        matches = _RE_KAP_MARKER.findall(text)
        for kap in matches:
            if kap[:2] == einzelplan:
                return kap
        return None

    @staticmethod
    def _find_kapitel_name(
        pages_text: list[str], kap: str, einzelplan: str
    ) -> str | None:
        """Search the document for the chapter name associated with *kap*."""
        # On the table-of-contents page, pattern is "NNNN Kapitel Name ...."
        for text in pages_text[:5]:
            pattern = re.compile(
                rf"{re.escape(kap)}\s+(.+?)(?:\s*\.{{3,}}|\s*\n)", re.MULTILINE
            )
            m = pattern.search(text)
            if m:
                name = m.group(1).strip().rstrip(".")
                if len(name) > 3:
                    return name

        # Also try from the first page of the chapter itself
        for text in pages_text:
            if kap in text[:100]:
                lines = text.split("\n")
                for i, line in enumerate(lines):
                    if kap in line:
                        # Check nearby lines for a name
                        for j in range(max(0, i - 3), min(len(lines), i + 3)):
                            candidate = lines[j].strip()
                            if (
                                len(candidate) > 10
                                and not candidate.startswith("Soll")
                                and not candidate.startswith("Ist")
                                and "1 000" not in candidate
                                and not re.match(r"^[\d\s.-]+$", candidate)
                                and kap not in candidate
                                and "Titel" not in candidate
                                and "Funktion" not in candidate
                            ):
                                return candidate[:200]
                        break
        return None

    # ------------------------------------------------------------------
    # Titel entry parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_layout(pages_text: list[str]) -> str:
        """Detect whether the document uses Layout A or B.

        Layout A (2005-06): Titel code → description → amounts
        Layout B (2007-11): amounts → description → Titel code

        Heuristic: inspect the text right after the first Titel match.
        If it starts with 'Erläuterungen' or 'Haushaltsvermerk', amounts
        are *not* in the after-block → Layout B.
        """
        for text in pages_text[1:10]:
            matches = list(_RE_TITEL_FUNC.finditer(text))
            if not matches:
                continue
            # Check what follows the first Titel match
            after_text = text[matches[0].end():].lstrip()
            first_line = after_text.split("\n")[0].strip() if after_text else ""
            if first_line.startswith("Erläuterungen") or first_line.startswith("Haushaltsvermerk"):
                return "B"
            # If the first line is description text (has alpha), it's Layout A
            if any(c.isalpha() for c in first_line) and len(first_line) > 5:
                return "A"
        return "A"  # default

    def _parse_titel_entries(
        self,
        pages_text: list[str],
        einzelplan: str,
        source_pdf: str,
    ) -> list[BudgetEntry]:
        """Parse all Titel entries across all pages."""
        all_entries: list[BudgetEntry] = []
        current_kapitel: str | None = None
        current_titelgruppe: str | None = None
        in_einnahmen = False
        in_personal_section = False

        layout = self._detect_layout(pages_text)
        logger.info("Detected text layout: %s", layout)

        for page_idx, text in enumerate(pages_text):
            page_num = page_idx + 1

            # Skip title page and purely textual preamble pages
            if page_idx == 0:
                continue

            # Detect Personalhaushalt section — stop parsing Titel entries
            if _RE_PERSONAL.search(text[:300]) and "Inhalt" in text[:500]:
                in_personal_section = True
            if in_personal_section:
                continue

            # Detect Kapitel from page markers
            kap = self._detect_kapitel_on_page(text, einzelplan)
            if kap:
                current_kapitel = kap

            if not current_kapitel:
                continue

            # Skip Abschluss (summary) pages
            if _RE_KAP_ABSCHLUSS.search(text):
                continue
            if "Abschluss des Einzelplans" in text:
                continue

            # Detect Einnahmen / Ausgaben sections
            einnahmen_pos = -1
            ausgaben_pos = -1
            m_ein = _RE_EINNAHMEN_HEADER.search(text)
            m_aus = _RE_AUSGABEN_HEADER.search(text)
            if m_ein:
                einnahmen_pos = m_ein.start()
            if m_aus:
                ausgaben_pos = m_aus.start()

            # Detect Titelgruppe
            tg_match = re.search(r"(?:Tgr\.\s*|Titelgruppe\s+)(\d{2})\b", text)
            if tg_match:
                current_titelgruppe = tg_match.group(1)

            # Find all Titel codes on this page
            titel_matches = list(_RE_TITEL_FUNC.finditer(text))
            if not titel_matches:
                continue

            for i, m in enumerate(titel_matches):
                try:
                    entry = self._extract_single_entry(
                        text,
                        titel_matches,
                        i,
                        einzelplan,
                        current_kapitel,
                        current_titelgruppe,
                        page_num,
                        source_pdf,
                        einnahmen_pos,
                        ausgaben_pos,
                        layout,
                    )
                    if entry:
                        all_entries.append(entry)
                except Exception:
                    logger.warning(
                        "Failed to parse Titel on page %d, match %d",
                        page_num, i, exc_info=True,
                    )

        return all_entries

    def _extract_single_entry(
        self,
        text: str,
        matches: list[re.Match],
        idx: int,
        einzelplan: str,
        kapitel: str,
        titelgruppe: str | None,
        page_num: int,
        source_pdf: str,
        einnahmen_pos: int,
        ausgaben_pos: int,
        layout: str,
    ) -> BudgetEntry | None:
        """Extract one BudgetEntry from the text surrounding a Titel match."""
        m = matches[idx]
        titel_num = m.group(1).strip()  # "NNN NN"
        func_code = m.group(2).strip()  # "NNN"

        # Determine block boundaries
        # Block before this Titel (from previous Titel end or page start)
        if idx > 0:
            block_before_start = matches[idx - 1].end()
        else:
            block_before_start = 0
        block_before = text[block_before_start : m.start()]

        # Block after this Titel (from Titel end to next Titel start or page end)
        if idx + 1 < len(matches):
            block_after_end = matches[idx + 1].start()
        else:
            block_after_end = len(text)
        block_after = text[m.end() : block_after_end]

        # --- Extract amounts using the detected layout ---
        after_lines = block_after.split("\n")
        before_lines = block_before.split("\n")

        if layout == "A":
            # Layout A: amounts and description follow the Titel code
            amounts = self._extract_amounts_layout_a(after_lines)
            desc = self._extract_description(after_lines)
        else:
            # Layout B: amounts and description precede the Titel code
            amounts = self._extract_amounts_from_end(before_lines, 3)
            desc = self._extract_description_from_before(before_lines)
            # Fallback: if before-block yielded nothing, try after-block
            if not amounts:
                amounts = self._extract_amounts_layout_a(after_lines)
            if not desc:
                desc = self._extract_description(after_lines)

        # Check for F marker (flexibilisiert)
        is_flex = bool(
            re.search(r"(?:^|\n)\s*F\s*(?:\n|$)", text[max(0, m.start() - 20) : m.start()])
        ) or titel_num.startswith("F ") or "F " in text[max(0, m.start() - 5) : m.start()]

        # Detect if Einnahmen or Ausgaben based on Titel Hauptgruppe
        titel_prefix = int(titel_num[0])
        is_einnahme = titel_prefix <= 3

        # Also check section context
        titel_pos = m.start()
        if einnahmen_pos >= 0 and ausgaben_pos >= 0:
            if einnahmen_pos < titel_pos < ausgaben_pos:
                is_einnahme = True
            elif ausgaben_pos < titel_pos:
                is_einnahme = False

        entry = BudgetEntry(
            year=0,  # stamped later
            version="",  # stamped later
            einzelplan=einzelplan,
            kapitel=kapitel,
            titel=titel_num,
            titel_text=desc,
            titelgruppe=titelgruppe,
            flexibilisiert=is_flex,
            source_pdf=source_pdf,
            source_page=page_num,
        )

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

        return entry

    @staticmethod
    def _extract_amounts_from_end(lines: list[str], max_amounts: int = 3) -> list[float]:
        """Extract amounts from the END of a line list (for Layout B).

        In Layout B the block before a Titel code looks like::

            ...Erläuterungen text from previous Titel...
            amount1
            amount2
            amount3
            Description text (possibly multi-line)
            F          ← optional flex marker

        We scan backwards, skipping F/B markers and description text,
        then collect amounts until we hit another text block.
        """
        amounts_reversed: list[float] = []
        phase = "skip_markers"  # → skip_description → collect

        # Only scan the last ~15 non-blank lines to avoid picking up
        # amounts from a *previous* Titel entry (relevant for Layout A docs
        # where amounts live in the after-block, not before).
        tail = [l for l in lines if l.strip()][-15:]

        for line in reversed(tail):
            stripped = line.strip()
            if not stripped:
                continue

            is_alpha_line = any(c.isalpha() for c in stripped)

            if phase == "skip_markers":
                # Skip trailing single-char markers like F, B
                if len(stripped) <= 2 and is_alpha_line:
                    continue
                phase = "skip_description"

            if phase == "skip_description":
                if is_alpha_line:
                    # Still in description text — keep skipping
                    continue
                # Hit a non-alpha line — must be an amount
                phase = "collect"

            if phase == "collect":
                if is_alpha_line:
                    break  # Hit previous text block, stop
                if _RE_DASH_LINE.match(stripped):
                    amounts_reversed.append(0.0)
                else:
                    val = _parse_budget_amount(stripped)
                    if val is not None:
                        amounts_reversed.append(val)
                    else:
                        break

                if len(amounts_reversed) >= max_amounts:
                    break

        amounts_reversed.reverse()
        return amounts_reversed

    @staticmethod
    def _extract_description(lines: list[str]) -> str | None:
        """Extract description text from lines after a Titel code (Layout A)."""
        desc_parts: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                if desc_parts:
                    break
                continue
            # Skip pure numbers / amounts
            if re.match(r"^[\d.,\s()−–—-]+$", stripped):
                if desc_parts:
                    break
                continue
            # Skip short markers
            if len(stripped) <= 2 and stripped.isalpha():
                continue
            # Stop at annotation headers
            if stripped in {"Erläuterungen", "Haushaltsvermerk", "Bezeichnung"}:
                break
            if stripped.startswith("1 000"):
                break
            desc_parts.append(stripped)
            if len(desc_parts) >= 4:
                break
        if desc_parts:
            return " ".join(desc_parts)[:500]
        return None

    @staticmethod
    def _extract_amounts_layout_a(lines: list[str]) -> list[float]:
        """Extract amounts for Layout A: amounts follow description text.

        Stop at annotation headers (Erläuterungen, Haushaltsvermerk, Bezeichnung).
        """
        amounts: list[float] = []
        past_description = False
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            # Stop at annotation headers
            if stripped in {"Erläuterungen", "Haushaltsvermerk", "Bezeichnung"}:
                break
            if stripped.startswith("1 000"):
                break

            is_alpha = any(c.isalpha() for c in stripped)
            if is_alpha:
                if not re.match(r"^\([\d.\s,−-]+\)\s*$", stripped):
                    past_description = True
                    continue

            if _RE_DASH_LINE.match(stripped):
                amounts.append(0.0)
            else:
                val = _parse_budget_amount(stripped)
                if val is not None:
                    amounts.append(val)

            if len(amounts) >= 3:
                break
        return amounts

    @staticmethod
    def _extract_description_from_before(lines: list[str]) -> str | None:
        """Extract description from lines before a Titel code (Layout B).

        In Layout B the description sits between the amounts and the Titel code.
        """
        desc_parts: list[str] = []
        # Walk lines from end backwards, skipping amounts, collecting text
        collecting = False
        for line in reversed(lines):
            stripped = line.strip()
            if not stripped:
                if collecting:
                    break
                continue
            if re.match(r"^[\d.,\s()−–—-]+$", stripped):
                if collecting:
                    break
                continue
            if len(stripped) <= 2 and stripped.isalpha():
                continue
            if stripped in {"Erläuterungen", "Haushaltsvermerk", "Bezeichnung"}:
                continue
            if stripped.startswith("1 000"):
                continue
            collecting = True
            desc_parts.append(stripped)
            if len(desc_parts) >= 4:
                break
        desc_parts.reverse()
        if desc_parts:
            return " ".join(desc_parts)[:500]
        return None

    # ------------------------------------------------------------------
    # Personnel parsing
    # ------------------------------------------------------------------

    def _parse_personnel(
        self,
        pages_text: list[str],
        einzelplan: str,
        source_pdf: str,
        year: int,
        version: str,
    ) -> list[PersonnelEntry]:
        """Extract personnel (Planstellen) entries from the Personalhaushalt section."""
        entries: list[PersonnelEntry] = []
        in_section = False
        current_kapitel: str | None = None
        current_titel: str | None = None

        for page_idx, text in enumerate(pages_text):
            page_num = page_idx + 1

            if _RE_PLANSTELLEN.search(text):
                in_section = True

            if not in_section:
                # Also check for "Personalhaushalt" section start
                if _RE_PERSONAL.search(text[:300]):
                    in_section = True
                else:
                    continue

            # Detect Kapitel
            kap = self._detect_kapitel_on_page(text, einzelplan)
            if kap:
                current_kapitel = kap

            # Detect Titel context
            titel_match = re.search(r"Titel\s+(\d{3}\s+\d{2})", text)
            if titel_match:
                current_titel = titel_match.group(1)

            # Parse Besoldungsgruppe lines
            for m in _RE_BESOLDUNG.finditer(text):
                bes_gr = m.group(1).strip()
                count_str = m.group(2).strip()
                count = parse_german_number(count_str)
                if count is not None:
                    entries.append(PersonnelEntry(
                        year=year,
                        version=version,
                        einzelplan=einzelplan,
                        kapitel=current_kapitel or einzelplan + "00",
                        titel=current_titel,
                        besoldungsgruppe=bes_gr,
                        planstellen_gesamt=int(count) if count == int(count) else None,
                        source_pdf=source_pdf,
                        source_page=page_num,
                    ))

        return entries

    # ------------------------------------------------------------------
    # Verpflichtungsermächtigungen
    # ------------------------------------------------------------------

    def _parse_verpflichtungen(
        self, pages_text: list[str], einzelplan: str
    ) -> list[dict]:
        """Extract Verpflichtungsermächtigung (VE) entries."""
        results: list[dict] = []

        for page_idx, text in enumerate(pages_text):
            if not _RE_VE.search(text):
                continue

            # Look for "davon fällig" breakdown
            if "davon fällig" in text.lower() or "davon f\u00e4llig" in text.lower():
                faellig: dict[str, float | None] = {}
                for m in _RE_DAVON_FAELLIG.finditer(text):
                    yr = m.group(1)
                    amt = _parse_budget_amount(m.group(2))
                    if amt is not None:
                        faellig[yr] = amt
                if faellig:
                    results.append({
                        "einzelplan": einzelplan,
                        "page": page_idx + 1,
                        "faellig": faellig,
                    })

        return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python -m src.extract.early_era_parser <pdf_path> [year]")
        sys.exit(1)

    path = sys.argv[1]
    yr = int(sys.argv[2]) if len(sys.argv) > 2 else None

    parser = EarlyEraParser()
    result = parser.parse(path, year=yr)

    print(f"Year: {result.year}, Version: {result.version}")
    print(f"EP meta: {result.einzelplan_meta}")
    print(f"Kapitel: {len(result.kapitel_meta)}")
    print(f"Entries: {len(result.entries)}")
    print(f"Personnel: {len(result.personnel)}")
    if result.entries:
        e = result.entries[0]
        print(
            f"First entry: ep={e.einzelplan}, kap={e.kapitel}, "
            f"titel={e.titel}, soll={e.ausgaben_soll}, page={e.source_page}"
        )
