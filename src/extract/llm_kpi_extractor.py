"""LLM-based structured KPI extractor for German federal budget PDFs.

Uses GPT-4o to extract budget KPIs from PDF pages into a consistent
structured format, bridging format differences across 2005-2026.
Falls back gracefully if LLM is unavailable.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
from openai import AzureOpenAI, APIConnectionError, APIStatusError, RateLimitError

from src.config import config
from src.extract.budget_parser import BudgetEntry, PersonnelEntry, ParsedBudget

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PAGES_PER_CHUNK = 5
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0  # seconds
_MIN_DIGIT_RATIO = 0.03  # pages with <3 % digits are likely non-data

# ---------------------------------------------------------------------------
# System prompts (German, budget-specific)
# ---------------------------------------------------------------------------

_KPI_SYSTEM_PROMPT = """\
Du extrahierst strukturierte Haushaltsdaten aus deutschen Bundeshaushaltsplänen.

Analysiere den Text und extrahiere ALLE Haushaltstitel (Budget-Zeilen) als JSON.

Für jeden Titel extrahiere:
{
  "einzelplan": "NN",        // 2-stellig, z.B. "01", "06", "14"
  "kapitel": "NNNN",         // 4-stellig, z.B. "0111", "1403"
  "titel": "NNN NN",         // z.B. "421 01", "531 01"
  "titel_text": "...",       // Beschreibung/Zweckbestimmung
  "ausgaben_soll": 12345,    // Soll-Ausgaben in 1000 EUR (null wenn nicht vorhanden)
  "ausgaben_ist": null,       // Ist-Ausgaben in 1000 EUR (null wenn nicht vorhanden)
  "einnahmen_soll": null,    // Soll-Einnahmen in 1000 EUR
  "is_verrechnungstitel": false,  // true wenn Titel 381/981-986
  "flexibilisiert": false    // true wenn mit F markiert
}

REGELN:
- Alle Geldbeträge in 1.000 EUR (Tausend Euro)
- Wenn Beträge in Mio EUR angegeben sind, multipliziere mit 1000
- Deutsche Zahlen: Punkt = Tausender, Komma = Dezimal (1.234,56 = 1234.56)
- "—" oder "-" als Betrag = 0
- Gib NUR valides JSON zurück: {"entries": [...], "meta": {...}}
- meta enthält: {"einzelplan": "NN", "einzelplan_text": "...", \
"kapitel": "NNNN", "kapitel_text": "..."}
"""

_PERSONNEL_SYSTEM_PROMPT = """\
Du extrahierst Personalhaushaltsdaten (Planstellen) aus dem Bundeshaushalt.

Für jede Besoldungsgruppe/Entgeltgruppe extrahiere:
{
  "einzelplan": "NN",
  "kapitel": "NNNN",
  "titel": "NNN NN",
  "besoldungsgruppe": "A 13",
  "planstellen_gesamt": 42,
  "planstellen_tariflich": 40,
  "planstellen_aussertariflich": 2
}

Gib NUR valides JSON zurück: {"personnel": [...]}
"""


# ---------------------------------------------------------------------------
# LLMKPIExtractor
# ---------------------------------------------------------------------------

class LLMKPIExtractor:
    """Extract structured budget KPIs using GPT-4o.

    Sends page text (and optionally rendered images) to GPT-4o
    with a structured JSON output schema.  The LLM understands
    the semantic meaning of budget tables regardless of layout.
    """

    def __init__(self) -> None:
        self._client: AzureOpenAI | None = None
        self._deployment: str = config.AZURE_OPENAI_DEPLOYMENT

    # ------------------------------------------------------------------
    # Lazy client initialisation
    # ------------------------------------------------------------------

    def _get_client(self) -> AzureOpenAI:
        """Return (and lazily create) the Azure OpenAI client."""
        if self._client is not None:
            return self._client

        if not config.AZURE_OPENAI_ENDPOINT or not config.AZURE_OPENAI_API_KEY:
            raise RuntimeError(
                "Azure OpenAI nicht konfiguriert — "
                "AZURE_OPENAI_ENDPOINT und AZURE_OPENAI_API_KEY erforderlich."
            )

        self._client = AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION,
        )
        logger.info(
            "LLMKPIExtractor client initialised – deployment=%s",
            self._deployment,
        )
        return self._client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_page_kpis(
        self,
        pdf_path: Path | str,
        page_numbers: list[int],
        year: int,
        version: str = "soll",
    ) -> list[BudgetEntry]:
        """Extract budget entries from specific PDF pages.

        Parameters
        ----------
        pdf_path:
            Path to the budget PDF.
        page_numbers:
            0-indexed page numbers to process.
        year:
            Budget year (e.g. 2024).
        version:
            Budget version (``'soll'``, ``'entwurf'``, etc.).

        Returns
        -------
        list[BudgetEntry]
        """
        pdf_path = Path(pdf_path)
        page_texts = self._extract_page_texts(pdf_path, page_numbers)
        if not page_texts:
            return []

        user_message = self._build_text_block(page_texts, year, version)
        raw_json = self._call_llm_json(
            _KPI_SYSTEM_PROMPT, user_message, label="budget-kpi"
        )
        return self._parse_budget_response(
            raw_json, year, version, str(pdf_path), page_numbers
        )

    def extract_personnel_kpis(
        self,
        pdf_path: Path | str,
        page_numbers: list[int],
        year: int,
        version: str = "soll",
    ) -> list[PersonnelEntry]:
        """Extract personnel entries from specific PDF pages.

        Parameters
        ----------
        pdf_path:
            Path to the budget PDF.
        page_numbers:
            0-indexed page numbers to process.
        year:
            Budget year.
        version:
            Budget version.

        Returns
        -------
        list[PersonnelEntry]
        """
        pdf_path = Path(pdf_path)
        page_texts = self._extract_page_texts(pdf_path, page_numbers)
        if not page_texts:
            return []

        user_message = self._build_text_block(page_texts, year, version)
        raw_json = self._call_llm_json(
            _PERSONNEL_SYSTEM_PROMPT, user_message, label="personnel"
        )
        return self._parse_personnel_response(
            raw_json, year, version, str(pdf_path), page_numbers
        )

    def extract_full_pdf(
        self,
        pdf_path: Path | str,
        year: int,
        version: str = "soll",
    ) -> ParsedBudget:
        """Process an entire PDF in page chunks and aggregate results.

        Pages are processed in chunks of ~5.  Non-data pages (TOC, legal
        text, blank pages) are skipped automatically.

        Parameters
        ----------
        pdf_path:
            Path to the budget PDF.
        year:
            Budget year.
        version:
            Budget version.

        Returns
        -------
        ParsedBudget
        """
        pdf_path = Path(pdf_path)
        doc = fitz.open(str(pdf_path))
        total_pages = len(doc)
        doc.close()

        all_entries: list[BudgetEntry] = []
        all_personnel: list[PersonnelEntry] = []

        # Identify data pages first, then process in chunks.
        data_pages = self._find_data_pages(pdf_path, total_pages)
        logger.info(
            "Full PDF extraction: %d data pages of %d total in %s",
            len(data_pages),
            total_pages,
            pdf_path.name,
        )

        for chunk_start in range(0, len(data_pages), _PAGES_PER_CHUNK):
            chunk = data_pages[chunk_start : chunk_start + _PAGES_PER_CHUNK]
            logger.info(
                "Processing chunk pages %s (%d/%d)",
                [p + 1 for p in chunk],
                chunk_start + len(chunk),
                len(data_pages),
            )
            try:
                entries = self.extract_page_kpis(pdf_path, chunk, year, version)
                all_entries.extend(entries)
            except Exception:
                logger.warning(
                    "Budget extraction failed for pages %s – skipping chunk",
                    chunk,
                    exc_info=True,
                )
            try:
                personnel = self.extract_personnel_kpis(
                    pdf_path, chunk, year, version
                )
                all_personnel.extend(personnel)
            except Exception:
                logger.warning(
                    "Personnel extraction failed for pages %s – skipping chunk",
                    chunk,
                    exc_info=True,
                )

        return ParsedBudget(
            source_file=str(pdf_path),
            year=year,
            version=version,
            entries=all_entries,
            personnel=all_personnel,
        )

    def extract_as_fallback(
        self,
        pdf_path: Path,
        year: int,
        version: str = "soll",
    ) -> ParsedBudget | None:
        """Use LLM extraction as fallback when regex parser fails.

        Returns ``None`` if Azure OpenAI is not configured.
        """
        if not config.AZURE_OPENAI_ENDPOINT or not config.AZURE_OPENAI_API_KEY:
            logger.warning(
                "LLM KPI extractor unavailable — no Azure OpenAI config"
            )
            return None
        return self.extract_full_pdf(pdf_path, year, version)

    # ------------------------------------------------------------------
    # PDF text extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_page_texts(
        pdf_path: Path, page_numbers: list[int]
    ) -> list[tuple[int, str]]:
        """Return ``(page_number, text)`` pairs using PyMuPDF."""
        result: list[tuple[int, str]] = []
        doc = fitz.open(str(pdf_path))
        try:
            for pn in page_numbers:
                if 0 <= pn < len(doc):
                    text = doc[pn].get_text()
                    result.append((pn, text))
                else:
                    logger.warning(
                        "Page %d out of range for %s (max %d)",
                        pn,
                        pdf_path.name,
                        len(doc) - 1,
                    )
        finally:
            doc.close()
        return result

    @staticmethod
    def _is_data_page(text: str) -> bool:
        """Heuristic: a page is 'data' if it has enough digits."""
        if not text or len(text) < 40:
            return False
        digit_count = sum(1 for c in text if c.isdigit())
        return (digit_count / len(text)) >= _MIN_DIGIT_RATIO

    def _find_data_pages(
        self, pdf_path: Path, total_pages: int
    ) -> list[int]:
        """Return 0-indexed page numbers that likely contain budget data."""
        doc = fitz.open(str(pdf_path))
        data_pages: list[int] = []
        try:
            for i in range(total_pages):
                text = doc[i].get_text()
                if self._is_data_page(text):
                    data_pages.append(i)
        finally:
            doc.close()
        return data_pages

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_text_block(
        page_texts: list[tuple[int, str]],
        year: int,
        version: str,
    ) -> str:
        """Assemble page texts into a single user message."""
        sections: list[str] = [
            f"Haushaltsjahr: {year}, Version: {version}\n"
        ]
        for page_num, text in page_texts:
            sections.append(
                f"--- Seite {page_num + 1} ---\n{text}"
            )
        return "\n\n".join(sections)

    # ------------------------------------------------------------------
    # LLM call with retry
    # ------------------------------------------------------------------

    def _call_llm_json(
        self, system_prompt: str, user_message: str, *, label: str = ""
    ) -> dict[str, Any]:
        """Call GPT-4o and parse the JSON response.

        Retries up to ``_MAX_RETRIES`` times with exponential back-off on
        rate-limit errors and once on JSON parse failures (with error
        feedback to the model).
        """
        client = self._get_client()
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        last_error: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                response = client.chat.completions.create(
                    model=self._deployment,
                    messages=messages,
                    temperature=0.0,
                    max_tokens=4096,
                )
                raw_text: str = response.choices[0].message.content or ""
                logger.debug(
                    "[%s] LLM response attempt %d – %d chars",
                    label,
                    attempt,
                    len(raw_text),
                )
                return self._parse_json(raw_text)

            except (json.JSONDecodeError, ValueError) as exc:
                logger.warning(
                    "[%s] JSON parse failed (attempt %d): %s",
                    label,
                    attempt,
                    exc,
                )
                last_error = exc
                # Give the model error feedback so it can fix the output.
                messages.append({"role": "assistant", "content": raw_text})
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Deine Antwort war kein valides JSON. Fehler: {exc}\n"
                            "Bitte antworte NUR mit validem JSON."
                        ),
                    },
                )
                continue

            except RateLimitError as exc:
                wait = _BACKOFF_BASE ** attempt
                logger.warning(
                    "[%s] Rate-limit (attempt %d) – waiting %.1fs",
                    label,
                    attempt,
                    wait,
                )
                last_error = exc
                time.sleep(wait)
                continue

            except (APIConnectionError, APIStatusError) as exc:
                logger.error("[%s] API error (attempt %d): %s", label, attempt, exc)
                last_error = exc
                if attempt < _MAX_RETRIES:
                    time.sleep(_BACKOFF_BASE)
                continue

        raise RuntimeError(
            f"LLM JSON extraction failed after {_MAX_RETRIES} attempts: {last_error}"
        )

    # ------------------------------------------------------------------
    # JSON parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_json(raw: str) -> dict[str, Any]:
        """Extract the first JSON object from the LLM response text.

        The model sometimes wraps JSON in markdown fences — strip those.
        """
        text = raw.strip()
        # Strip markdown code fences if present.
        if text.startswith("```"):
            # Remove opening fence (possibly with language tag).
            text = text.split("\n", 1)[-1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        # Find the first { … } block if there is surrounding text.
        brace_start = text.find("{")
        if brace_start == -1:
            raise ValueError("No JSON object found in LLM response")
        brace_end = text.rfind("}")
        if brace_end == -1:
            raise ValueError("Unclosed JSON object in LLM response")
        text = text[brace_start : brace_end + 1]

        return json.loads(text)

    # ------------------------------------------------------------------
    # Response → dataclass mapping
    # ------------------------------------------------------------------

    def _parse_budget_response(
        self,
        data: dict[str, Any],
        year: int,
        version: str,
        source_pdf: str,
        page_numbers: list[int],
    ) -> list[BudgetEntry]:
        """Convert the LLM JSON into ``BudgetEntry`` objects."""
        entries: list[BudgetEntry] = []
        meta = data.get("meta", {})
        meta_ep = meta.get("einzelplan", "")
        meta_kap = meta.get("kapitel", "")

        for item in data.get("entries", []):
            try:
                entry = BudgetEntry(
                    year=year,
                    version=version,
                    einzelplan=item.get("einzelplan", meta_ep),
                    kapitel=item.get("kapitel", meta_kap),
                    titel=item.get("titel"),
                    titel_text=item.get("titel_text"),
                    ausgaben_soll=self._to_float(item.get("ausgaben_soll")),
                    ausgaben_ist=self._to_float(item.get("ausgaben_ist")),
                    einnahmen_soll=self._to_float(item.get("einnahmen_soll")),
                    is_verrechnungstitel=bool(
                        item.get("is_verrechnungstitel", False)
                    ),
                    flexibilisiert=bool(item.get("flexibilisiert", False)),
                    source_pdf=source_pdf,
                    source_page=page_numbers[0] if page_numbers else None,
                )
                entries.append(entry)
            except Exception:
                logger.warning(
                    "Skipping malformed budget entry: %s", item, exc_info=True
                )
        logger.info("Parsed %d budget entries from LLM response", len(entries))
        return entries

    def _parse_personnel_response(
        self,
        data: dict[str, Any],
        year: int,
        version: str,
        source_pdf: str,
        page_numbers: list[int],
    ) -> list[PersonnelEntry]:
        """Convert the LLM JSON into ``PersonnelEntry`` objects."""
        entries: list[PersonnelEntry] = []
        for item in data.get("personnel", []):
            try:
                entry = PersonnelEntry(
                    year=year,
                    version=version,
                    einzelplan=item.get("einzelplan", ""),
                    kapitel=item.get("kapitel", ""),
                    titel=item.get("titel"),
                    besoldungsgruppe=item.get("besoldungsgruppe"),
                    planstellen_gesamt=self._to_int(
                        item.get("planstellen_gesamt")
                    ),
                    planstellen_tariflich=self._to_int(
                        item.get("planstellen_tariflich")
                    ),
                    planstellen_aussertariflich=self._to_int(
                        item.get("planstellen_aussertariflich")
                    ),
                    source_pdf=source_pdf,
                    source_page=page_numbers[0] if page_numbers else None,
                )
                entries.append(entry)
            except Exception:
                logger.warning(
                    "Skipping malformed personnel entry: %s",
                    item,
                    exc_info=True,
                )
        logger.info("Parsed %d personnel entries from LLM response", len(entries))
        return entries

    # ------------------------------------------------------------------
    # Type coercion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_float(value: Any) -> float | None:
        """Coerce a value to float, returning ``None`` for null/empty."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_int(value: Any) -> int | None:
        """Coerce a value to int, returning ``None`` for null/empty."""
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
