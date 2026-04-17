"""Multimodal page scanner for German federal budget (Bundeshaushalt) PDFs.

Renders specific PDF pages as images and sends both extracted text AND page
images to GPT-4o for deep visual analysis.  This is critical for budget PDFs
where complex tables with precise financial figures may be misaligned or lost
during pure text extraction.

Inspired by the "scanpages" pattern from the VI_PaulDewez RAG agent.
"""

from __future__ import annotations

import base64
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import fitz  # PyMuPDF
from openai import AzureOpenAI, APIConnectionError, APIStatusError, RateLimitError

from src.config import config
from src.extract.pdf_extractor import PDFExtractor

logger = logging.getLogger(__name__)

# Maximum number of page images per single scan request.
# Bookmarks typically resolve to 1-5 pages; 20 is a generous safety cap.
_MAX_PAGES_PER_SCAN = 20

# Render resolution in DPI – 150 gives good readability without huge payloads.
_RENDER_DPI = 150

# ---------------------------------------------------------------------------
# Module-level PDF handle cache (avoids reopening 3000-page files)
# ---------------------------------------------------------------------------

_pdf_cache: dict[str, fitz.Document] = {}
_pdf_cache_lock = threading.Lock()
_PDF_CACHE_MAX = 10


def _get_cached_doc(pdf_path: str) -> fitz.Document:
    """Get or open a cached PDF document handle."""
    with _pdf_cache_lock:
        if pdf_path not in _pdf_cache:
            _pdf_cache[pdf_path] = fitz.open(pdf_path)
            # Evict oldest entry when cache is full
            if len(_pdf_cache) > _PDF_CACHE_MAX:
                oldest = next(iter(_pdf_cache))
                _pdf_cache[oldest].close()
                del _pdf_cache[oldest]
        return _pdf_cache[pdf_path]

# ---------------------------------------------------------------------------
# System prompt (German, budget-specific)
# ---------------------------------------------------------------------------

_PAGE_SCAN_SYSTEM_PROMPT = """\
Du analysierst Seiten aus dem deutschen Bundeshaushalt (Haushaltsplan des Bundes).
Die Seiten enthalten Tabellen mit Haushaltsdaten: Einzelpläne, Kapitel, Titel, \
Ausgaben (Soll/Ist), Einnahmen, Personalstellen und Verpflichtungsermächtigungen.

Antworte AUSSCHLIESSLICH basierend auf dem Inhalt der gezeigten Seiten.
Gib präzise Zahlen an, wie sie in den Tabellen stehen.
Beachte das deutsche Zahlenformat: Punkte als Tausendertrennzeichen.
Wenn die Antwort nicht auf den Seiten zu finden ist, sage das klar.
Zitiere die genaue Seitennummer.

Bewerte am Ende deine Konfidenz:
- "high": Die Antwort ist direkt und eindeutig auf den Seiten zu finden.
- "medium": Die Antwort ist ableitbar, aber erfordert Interpretation.
- "low": Die Information ist nur teilweise vorhanden oder unsicher.

Formatiere deine Antwort so:
ANTWORT:
<deine Antwort>

KONFIDENZ: <high|medium|low>
"""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PageScanResult:
    """Result of a multimodal page scan."""

    question: str
    pdf_path: str
    pages_scanned: list[int]
    answer: str
    confidence: str  # 'high', 'medium', 'low'


# ---------------------------------------------------------------------------
# PageScanner
# ---------------------------------------------------------------------------

class PageScanner:
    """Deeply analyse PDF pages using text extraction AND rendered images.

    Combines PyMuPDF text extraction with page-image rendering so that GPT-4o
    can visually read tables that pure text extraction may misalign.
    """

    def __init__(self, pdf_path: Path | None = None) -> None:
        """Initialise with a path to a budget PDF.

        If *pdf_path* is ``None`` the first ``.pdf`` found in ``config.DOCS_DIR``
        is used.
        """
        if pdf_path is not None:
            self.pdf_path = Path(pdf_path)
        else:
            self.pdf_path = self._find_default_pdf()

        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        self._extractor = PDFExtractor(self.pdf_path)

        # Build the Azure OpenAI client directly (we need multimodal messages).
        if not config.AZURE_OPENAI_ENDPOINT or not config.AZURE_OPENAI_API_KEY:
            raise ValueError(
                "AZURE_OPENAI_ENDPOINT und AZURE_OPENAI_API_KEY müssen "
                "konfiguriert sein (in .env oder als Umgebungsvariable)."
            )
        self._client = AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION,
        )
        self._deployment = config.AZURE_OPENAI_DEPLOYMENT

        logger.info(
            "PageScanner initialised – pdf=%s, deployment=%s",
            self.pdf_path.name,
            self._deployment,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(
        self, question: str, start_page: int, end_page: int,
        text_only: bool = False,
    ) -> PageScanResult:
        """Analyse a contiguous range of pages with text + images.

        Parameters
        ----------
        question:
            Natural-language question (German preferred).
        start_page:
            First page to scan (0-indexed).
        end_page:
            Exclusive upper bound (0-indexed), i.e. pages
            ``[start_page, end_page)`` are scanned.
        text_only:
            If ``True``, skip image rendering (faster for overview pages).

        Returns
        -------
        PageScanResult
        """
        page_numbers = list(range(start_page, end_page))
        return self._scan_pages(question, page_numbers, text_only=text_only)

    def scan_for_table(
        self, question: str, page_numbers: list[int],
        text_only: bool = False,
    ) -> PageScanResult:
        """Scan specific (possibly non-contiguous) pages for table data.

        Parameters
        ----------
        question:
            Natural-language question.
        page_numbers:
            0-indexed page numbers to scan.
        text_only:
            If ``True``, skip image rendering (faster for overview pages).

        Returns
        -------
        PageScanResult
        """
        return self._scan_pages(question, page_numbers, text_only=text_only)

    # ------------------------------------------------------------------
    # Core scanning logic
    # ------------------------------------------------------------------

    def _scan_pages(
        self, question: str, page_numbers: list[int],
        text_only: bool = False,
    ) -> PageScanResult:
        """Shared implementation with parallel page extraction."""
        if not page_numbers:
            raise ValueError("page_numbers darf nicht leer sein.")

        # Enforce the per-request image cap.
        if len(page_numbers) > _MAX_PAGES_PER_SCAN:
            logger.debug(
                "Requested %d pages exceeds cap of %d – truncating.",
                len(page_numbers),
                _MAX_PAGES_PER_SCAN,
            )
            page_numbers = page_numbers[:_MAX_PAGES_PER_SCAN]

        # 1. Parallel extraction of text and (optionally) page images.
        def extract_page(pn: int) -> tuple[int, str, str | None]:
            text_pages = self._extractor.extract_pages(start=pn, end=pn + 1)
            text = text_pages[0].text if text_pages else ""
            if text_only:
                return pn, text, None
            try:
                b64 = self._render_page_image(pn)
            except Exception:
                logger.warning(
                    "Image rendering failed for page %d – text-only fallback.",
                    pn,
                    exc_info=True,
                )
                b64 = None
            return pn, text, b64

        workers = min(len(page_numbers), 4)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(extract_page, page_numbers))

        page_texts: list[tuple[int, str]] = []
        page_images: list[tuple[int, str]] = []
        for pn, text, b64 in results:
            page_texts.append((pn, text))
            if b64:
                page_images.append((pn, b64))

        # 2. Build multimodal prompt and call GPT-4o.
        messages = self._build_multimodal_prompt(question, page_texts, page_images)
        raw_answer = self._call_llm(messages)

        # 3. Parse structured answer.
        answer, confidence = self._parse_response(raw_answer)

        scanned_display = [pn + 1 for pn in page_numbers]  # 1-indexed for user
        return PageScanResult(
            question=question,
            pdf_path=str(self.pdf_path),
            pages_scanned=scanned_display,
            answer=answer,
            confidence=confidence,
        )

    # ------------------------------------------------------------------
    # Page rendering
    # ------------------------------------------------------------------

    def _render_page_image(self, page_number: int) -> str:
        """Render a single PDF page as a base64-encoded PNG.

        Parameters
        ----------
        page_number:
            0-indexed page number.

        Returns
        -------
        str
            Base64-encoded PNG data.
        """
        doc = _get_cached_doc(str(self.pdf_path))
        page = doc[page_number]
        mat = fitz.Matrix(_RENDER_DPI / 72, _RENDER_DPI / 72)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        return base64.b64encode(img_bytes).decode("utf-8")

    # ------------------------------------------------------------------
    # Multimodal prompt construction
    # ------------------------------------------------------------------

    def _build_multimodal_prompt(
        self,
        question: str,
        page_texts: list[tuple[int, str]],
        page_images: list[tuple[int, str]],
    ) -> list[dict]:
        """Build an OpenAI messages array with interleaved text and images.

        The user message contains:
        1. Combined extracted text for all pages.
        2. For each rendered page, an image_url content block.
        3. The user question.
        """
        # Build the combined extracted-text block.
        text_sections: list[str] = []
        for page_num, text in page_texts:
            text_sections.append(
                f"--- Seite {page_num + 1} (extrahierter Text) ---\n{text}"
            )
        combined_text = "\n\n".join(text_sections)

        # Construct user content parts.
        user_content: list[dict] = [
            {"type": "text", "text": f"Extrahierter Text der Seiten:\n\n{combined_text}"},
        ]

        # Build a lookup for quick access to images by page number.
        image_map = dict(page_images)

        for page_num, _ in page_texts:
            b64 = image_map.get(page_num)
            if b64 is not None:
                user_content.append(
                    {"type": "text", "text": f"[Bild von Seite {page_num + 1}]"},
                )
                user_content.append(
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{b64}",
                            "detail": "high",
                        },
                    },
                )

        user_content.append({"type": "text", "text": f"\nFrage: {question}"})

        return [
            {"role": "system", "content": _PAGE_SCAN_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ]

    # ------------------------------------------------------------------
    # LLM call
    # ------------------------------------------------------------------

    def _call_llm(self, messages: list[dict]) -> str:
        """Send the multimodal messages to Azure OpenAI and return the text."""
        logger.debug(
            "Sending multimodal request – %d message(s), deployment=%s",
            len(messages),
            self._deployment,
        )
        try:
            response = self._client.chat.completions.create(
                model=self._deployment,
                messages=messages,
                temperature=0.0,
                max_tokens=4096,
            )
        except RateLimitError as exc:
            logger.warning("Rate-limit reached: %s", exc)
            raise RuntimeError(
                "Azure OpenAI Rate-Limit erreicht. Bitte kurz warten und erneut versuchen."
            ) from exc
        except APIConnectionError as exc:
            logger.error("Connection error: %s", exc)
            raise RuntimeError(
                "Verbindung zu Azure OpenAI fehlgeschlagen. "
                "Endpoint und Netzwerk prüfen."
            ) from exc
        except APIStatusError as exc:
            logger.error("API error (HTTP %d): %s", exc.status_code, exc)
            raise RuntimeError(
                "Azure OpenAI API-Fehler (HTTP %d): %s"
                % (exc.status_code, exc.message)
            ) from exc

        content: str = response.choices[0].message.content or ""
        logger.debug("LLM response – %d chars", len(content))
        return content

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_response(raw: str) -> tuple[str, str]:
        """Extract the answer text and confidence from the LLM response.

        Expected format::

            ANTWORT:
            <answer text>

            KONFIDENZ: high|medium|low

        Falls back gracefully if the model doesn't follow the template.
        """
        confidence = "medium"  # default
        answer = raw.strip()

        # Try to extract confidence tag.
        for label in ("high", "medium", "low"):
            tag = f"KONFIDENZ: {label}"
            if tag.lower() in raw.lower():
                confidence = label
                # Remove the tag line from the answer body.
                idx = raw.lower().rfind(tag.lower())
                answer = raw[:idx].strip()
                break

        # Strip leading "ANTWORT:" header if present.
        for prefix in ("ANTWORT:", "ANTWORT :", "Antwort:"):
            if answer.startswith(prefix):
                answer = answer[len(prefix):].strip()
                break

        return answer, confidence

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_default_pdf() -> Path:
        """Return the first .pdf file found in ``config.DOCS_DIR``."""
        docs = config.DOCS_DIR
        if not docs.exists():
            raise FileNotFoundError(f"Docs directory not found: {docs}")
        pdfs = sorted(docs.glob("*.pdf"))
        if not pdfs:
            raise FileNotFoundError(f"No PDF files found in {docs}")
        logger.info("Using default PDF: %s", pdfs[0].name)
        return pdfs[0]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Allow running from project root: python -m src.query.page_scanner
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from src.config import config as _cfg  # noqa: E402, F811

    scanner = PageScanner()
    result = scanner.scan(
        "Welche Einzelpläne sind auf dieser Seite aufgeführt?",
        start_page=5,
        end_page=7,
    )
    print(f"Pages scanned: {result.pages_scanned}")
    print(f"Confidence:    {result.confidence}")
    print(f"Answer:\n{result.answer}")
