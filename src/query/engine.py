"""Document-first query engine for the German federal budget (Bundeshaushalt) Q&A.

Implements a lightweight agent loop using OpenAI function calling — no LangGraph
or LangChain dependency required.  The agent navigates original budget documents
(like a clerk) rather than querying a spreadsheet.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from openai import AzureOpenAI, APIConnectionError, APIStatusError, RateLimitError

from src.config import config
from src.query.citations import Citation, extract_citations_from_scan

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result data class
# ---------------------------------------------------------------------------


@dataclass
class AnswerResult:
    """Structured result of the query engine."""

    question: str
    answer: str
    sources: list[str] = field(default_factory=list)
    tools_used: list[str] = field(default_factory=list)
    sql_queries: list[str] = field(default_factory=list)
    confidence: str = "medium"
    citations: list = field(default_factory=list)  # list[Citation]


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
Du bist ein erfahrener Haushaltssachbearbeiter des Bundes.
Du beantwortest Fragen zum Bundeshaushalt, indem du die Original-Dokumente liest.

Du hast zwei Werkzeuge:
1. read_document — Bundeshaushalt-Dokument öffnen und lesen (dein Hauptwerkzeug)
2. compute — Taschenrechner für exakte Berechnungen

ARBEITSWEISE (wie ein Sachbearbeiter):

Schritt 1 — NAVIGIEREN: Finde die richtige Stelle im Dokument
  • Für bekannte Strukturen (EP/Kap/Titel): Nutze year + einzelplan + kapitel
  • Für Begriffe (z.B. "Datenschutz", "Reservedienstleistende"): Nutze search_term
  • Für bestimmte Abschnitte: Nutze section_type ('personal', 've', 'ueberblick', 'vermerk')

Schritt 2 — LESEN: Lies die gefundenen Seiten sorgfältig
  • Achte auf Spaltenüberschriften (Soll 2024, Soll 2023, Ist 2022)
  • Lies Erläuterungen und Fußnoten
  • Beachte Haushaltsvermerke

Schritt 3 — BERECHNEN: Nutze compute für Prozente, Differenzen, Summen
  • IMMER compute nutzen, NIEMALS Kopfrechnen
  • Beispiel: compute("round((13717181 - 16161139) / 16161139 * 100, 2)")

Schritt 4 — ANTWORTEN: Formuliere die Antwort mit Quellenangabe
  • Nenne die exakte Seite und das Dokument
  • Zeige Berechnungswege

NAVIGATION-TIPPS:
• Für Ausgabenvergleiche zwischen Jahren:
  → read_document für JEDES Jahr einzeln, dann compute für Differenz/Prozent
• Für "In welchem Kapitel war X?":
  → search_term mit dem Suchbegriff, verschiedene Jahre ausprobieren
• Für Personalstellen:
  → section_type='personal' + kapitel, um direkt zum Stellenplan zu springen
• Für Verpflichtungsermächtigungen:
  → section_type='ve' + kapitel
• Für historische Zuordnungen (Kapitel-Migrationen):
  → search_term über mehrere Jahre hinweg

WICHTIG:
• Du kannst read_document MEHRFACH aufrufen (verschiedene Jahre, verschiedene Kapitel)
• Bei Vergleichsfragen: Lies BEIDE Dokumente und vergleiche die Zahlen
• Achte auf historische Begriffsänderungen: {semantic_terms}
• GEBE NICHT AUF — wenn eine Suche nichts findet, probiere andere Begriffe oder Navigationswege

KONTEXTWISSEN:
{semantic_context}
"""

# ---------------------------------------------------------------------------
# Tool definitions (OpenAI function calling schema)
# ---------------------------------------------------------------------------

_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "read_document",
            "description": (
                "Navigiere und lese den Bundeshaushalt wie ein erfahrener Sachbearbeiter. "
                "Dies ist dein Hauptwerkzeug für ALLE Fragen. "
                "Drei Navigationsmodi: "
                "(1) SUCHE: search_term='Datenschutz' findet alle Seiten mit diesem Begriff. "
                "(2) INHALTSVERZEICHNIS: year+einzelplan+kapitel navigiert zur richtigen Stelle. "
                "(3) DIREKT: page_numbers=[142,143] springt zu bekannten Seiten. "
                "Liest die gefundenen Seiten visuell und extrahiert präzise Zahlen, "
                "Tabellen, Erläuterungen und Haushaltsvermerke direkt aus dem Original."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Die Frage, die auf den Seiten beantwortet werden soll",
                    },
                    "year": {
                        "type": "integer",
                        "description": "Haushaltsjahr (z.B. 2020)",
                    },
                    "einzelplan": {
                        "type": "string",
                        "description": "2-stelliger Einzelplan-Code (z.B. '06', '14')",
                    },
                    "kapitel": {
                        "type": "string",
                        "description": "4-stelliger Kapitel-Code (z.B. '0622', '1403')",
                    },
                    "search_term": {
                        "type": "string",
                        "description": "Suchbegriff für Volltextsuche über alle Seiten (z.B. 'Aufwandsentschädigungen', 'Reservedienstleistende')",
                    },
                    "section_type": {
                        "type": "string",
                        "enum": ["ueberblick", "titel", "personal", "ve", "vermerk", "erlaeuterung", "gesamtplan"],
                        "description": "Abschnittstyp zum gezielten Navigieren (z.B. 'personal' für Personalhaushalt, 've' für Verpflichtungsermächtigungen)",
                    },
                    "page_numbers": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "description": "Explizite Seitenzahlen (1-indiziert)",
                    },
                    "pdf_filename": {
                        "type": "string",
                        "description": "Expliziter PDF-Dateiname",
                    },
                },
                "required": ["question", "year"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compute",
            "description": (
                "Sichere Berechnung eines mathematischen Ausdrucks. "
                "Nutze dies für exakte Prozentberechnungen, Differenzen, "
                "Quotienten und Aggregationen statt Kopfrechnen. "
                "Beispiel: '(13717181 - 16161139) / 16161139 * 100' ergibt -15.12"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string",
                        "description": (
                            "Python-Ausdruck mit Zahlen und Operatoren. "
                            "Erlaubt: +, -, *, /, //, %, **, round(), abs(), "
                            "sum(), min(), max(), len(). "
                            "Beispiele: '(54820073 / 4469910000) * 100', "
                            "'round((19292503 - 18502187) / 18502187 * 100, 2)'"
                        ),
                    },
                },
                "required": ["expression"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# QueryEngine
# ---------------------------------------------------------------------------


class QueryEngine:
    """Document-first agent that answers Bundeshaushalt questions.

    Uses OpenAI function calling to navigate original budget PDFs
    like a clerk, using read_document as the primary data tool.
    """

    def __init__(
        self,
        db_path: Path | None = None,
        pdf_path: Path | None = None,
    ) -> None:
        self._db_path = db_path or config.DB_PATH
        self._pdf_path = pdf_path

        # Lazy-initialised tool instances (created on first use)
        self._page_scanner: Any | None = None

        # Semantic bridge for cross-era terminology
        from src.extract.semantic_bridge import SemanticBridge
        self._semantic_bridge = SemanticBridge()
        self._system_prompt = self._build_system_prompt()

        # Build the Azure OpenAI client directly — the existing LLMClient.chat()
        # returns only a string and does not support function calling.
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
            "QueryEngine initialised – db=%s, pdf=%s",
            self._db_path,
            self._pdf_path or "(auto-detect)",
        )

    # ------------------------------------------------------------------
    # System prompt builder
    # ------------------------------------------------------------------

    def _build_system_prompt(self) -> str:
        """Fill the system prompt template with SemanticBridge context."""
        sb = self._semantic_bridge

        # Build terminology aliases string
        term_lines = []
        for old, new in sb.TERM_ALIASES.items():
            term_lines.append(f'"{old}" = "{new}"')
        semantic_terms = ", ".join(term_lines) if term_lines else "(keine)"

        # Build semantic context block
        context_parts = []

        context_parts.append("Begriffe die sich geändert haben:")
        for old, new in sb.TERM_ALIASES.items():
            context_parts.append(f'  - "{old}" → "{new}"')

        context_parts.append("\nOrganisatorische Umzüge (Kapitel-Migrationen):")
        for migration in sb.KAPITEL_MIGRATIONS:
            steps = []
            for period in migration.periods:
                start, end = period["years"]
                steps.append(f'Kap {period["kapitel"]} ({start}-{end})')
            context_parts.append(f"  - {migration.institution}: {' → '.join(steps)}")

        context_parts.append("\nMinisteriums-Umbenennungen:")
        for ep_code, periods in sb.EINZELPLAN_NAMES.items():
            if len(periods) > 1:
                names = []
                for p in periods:
                    start, end = p["years"]
                    names.append(f'"{p["name"]}" ({start}-{end})')
                context_parts.append(f"  - EP {ep_code}: {' → '.join(names)}")

        semantic_context = "\n".join(context_parts)

        return _SYSTEM_PROMPT_TEMPLATE.format(
            semantic_terms=semantic_terms,
            semantic_context=semantic_context,
        )

    # ------------------------------------------------------------------
    # Lazy tool accessors
    # ------------------------------------------------------------------

    def _get_page_scanner(self):
        """Return the PageScanner instance (lazy init)."""
        if self._page_scanner is None:
            from src.query.page_scanner import PageScanner

            self._page_scanner = PageScanner(self._pdf_path)
        return self._page_scanner

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ask(
        self,
        question: str,
        max_iterations: int = 8,
        conversation_history: list[dict[str, str]] | None = None,
    ) -> AnswerResult:
        """Answer a question using the ReAct agent loop.

        The agent has two tools (document reader and calculator) and
        decides autonomously which to call — possibly multiple
        in sequence — before synthesising a final answer.

        Parameters
        ----------
        question:
            Natural-language question in German.
        max_iterations:
            Safety cap on agent loop iterations.
        conversation_history:
            Optional list of prior ``{"role": "user"/"assistant",
            "content": "..."}`` dicts so the LLM can resolve references
            such as "und davon?" that depend on earlier exchanges.

        Returns
        -------
        AnswerResult
        """
        logger.info("ask() – question=%r", question)
        try:
            return self._run_agent_loop(
                question, max_iterations, conversation_history=conversation_history
            )
        except Exception:
            logger.exception("Agent loop failed for question: %s", question)
            return AnswerResult(
                question=question,
                answer="Ein Fehler ist bei der Verarbeitung aufgetreten.",
                confidence="low",
            )

    # ------------------------------------------------------------------
    # Tool definitions
    # ------------------------------------------------------------------

    def _define_tools(self) -> list[dict[str, Any]]:
        """Return the OpenAI function calling tool schemas."""
        return _TOOL_DEFINITIONS

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------

    def _execute_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        sources: list[str],
        sql_queries: list[str],
        citations: list[Citation],
    ) -> str:
        """Execute a tool by name and return a text summary.

        Side-effects: appends to *sources*, *sql_queries*, and *citations* in-place.
        """
        logger.info("Executing tool %s with args %s", tool_name, arguments)

        if tool_name == "read_document":
            result_text, new_citations = self._exec_read_document(
                question=arguments.get("question", ""),
                year=arguments.get("year", 0),
                einzelplan=arguments.get("einzelplan"),
                kapitel=arguments.get("kapitel"),
                page_numbers=arguments.get("page_numbers"),
                pdf_filename=arguments.get("pdf_filename"),
                search_term=arguments.get("search_term"),
                section_type=arguments.get("section_type"),
            )
            citations.extend(new_citations)
            source = "read_document"
            if source not in sources:
                sources.append(source)
            return result_text

        if tool_name == "compute":
            return self._exec_compute(arguments.get("expression", ""))

        return f"Unbekanntes Werkzeug: {tool_name}"

    # -- individual tool implementations --------------------------------

    def _exec_compute(self, expression: str) -> str:
        """Safely evaluate a mathematical expression.

        Whitelist approach: only allow safe math operations.
        """
        import ast
        import math

        # Allowed names (safe builtins + math)
        allowed_names = {
            'abs': abs, 'round': round, 'sum': sum, 'min': min, 'max': max,
            'len': len, 'int': int, 'float': float,
            'pi': math.pi, 'e': math.e,
        }

        # Allowed node types in the AST
        _allowed_nodes = [
            ast.Module, ast.Expr, ast.Expression,
            ast.BinOp, ast.UnaryOp, ast.Compare,
            ast.Constant,  # numbers and literals
            ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow,
            ast.USub, ast.UAdd,  # unary minus/plus
            ast.Call, ast.Name, ast.Load,
            ast.List, ast.Tuple,  # for sum([...])
            ast.Lt, ast.Gt, ast.LtE, ast.GtE, ast.Eq, ast.NotEq,
            ast.IfExp,  # ternary
        ]
        # ast.Num was removed in Python 3.12+
        if hasattr(ast, 'Num'):
            _allowed_nodes.append(ast.Num)
        allowed_nodes = tuple(_allowed_nodes)

        expression = expression.strip()
        if not expression:
            return "Fehler: Leerer Ausdruck"

        # Reject dangerous patterns
        for forbidden in ('import', 'exec', 'eval', 'open', '__', 'os.', 'sys.', 'subprocess'):
            if forbidden in expression:
                return f"Fehler: '{forbidden}' ist nicht erlaubt"

        try:
            tree = ast.parse(expression, mode='eval')
            # Validate all nodes are safe
            for node in ast.walk(tree):
                if not isinstance(node, allowed_nodes):
                    return f"Fehler: Nicht erlaubter Ausdruck-Typ: {type(node).__name__}"
                # Check function calls only use allowed names
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    if node.func.id not in allowed_names:
                        return f"Fehler: Funktion '{node.func.id}' ist nicht erlaubt"
                # Check variable names
                if isinstance(node, ast.Name) and node.id not in allowed_names:
                    return f"Fehler: Variable '{node.id}' ist nicht erlaubt"

            result = eval(compile(tree, '<compute>', 'eval'), {"__builtins__": {}}, allowed_names)

            # Format result nicely
            if isinstance(result, float):
                if result == int(result) and abs(result) < 1e15:
                    return str(int(result))
                return f"{result:.6f}".rstrip('0').rstrip('.')
            return str(result)

        except SyntaxError as exc:
            return f"Syntax-Fehler: {exc}"
        except ZeroDivisionError:
            return "Fehler: Division durch Null"
        except Exception as exc:
            return f"Berechnungsfehler: {exc}"

    def _exec_read_document(
        self,
        question: str,
        year: int,
        einzelplan: str | None = None,
        kapitel: str | None = None,
        page_numbers: list[int] | None = None,
        pdf_filename: str | None = None,
        search_term: str | None = None,
        section_type: str | None = None,
    ) -> tuple[str, list[Citation]]:
        """Navigate and read budget documents using hierarchical TOC.

        Strategy:
        1. If explicit page_numbers given → go directly to those pages
        2. If search_term given → FTS5 fulltext search with smart ranking
        3. Hierarchical TOC lookup → EP → Kapitel → Section → 1-3 pages
        4. Fallback → old DocumentLocator
        """
        from src.query.document_locator import DocumentLocator

        locator = DocumentLocator()

        # STRATEGY 1: Explicit pages — clerk already knows where
        if page_numbers:
            pdf_path = self._resolve_pdf(locator, year, pdf_filename)
            if not pdf_path:
                return f"Kein PDF für Jahr {year} gefunden.", []
            return self._scan_and_cite(question, pdf_path, page_numbers, year, einzelplan, kapitel)

        # STRATEGY 2: FTS5 term search — clerk's Ctrl+F
        if search_term:
            # Resolve organizational mapping for EP/Kap context
            if not einzelplan and not kapitel:
                from src.extract.semantic_bridge import SemanticBridge
                bridge = SemanticBridge()
                org = bridge.resolve_organization(search_term)
                if org:
                    if not einzelplan:
                        einzelplan = org.get('einzelplan')
                    if not kapitel:
                        kapitel = org.get('kapitel')
            return self._search_and_scan(question, search_term, year, einzelplan, kapitel, section_type, locator)

        # STRATEGY 3: Hierarchical TOC lookup — clerk checks Table of Contents
        return self._toc_navigate_and_scan(question, year, einzelplan, kapitel, section_type, locator)

    # ------------------------------------------------------------------
    # Hierarchical TOC navigation
    # ------------------------------------------------------------------

    def _toc_navigate_and_scan(self, question, year, einzelplan, kapitel, section_type, locator):
        """Navigate via hierarchical TOC: EP → Kapitel → Section → 1-3 pages."""
        from src.db.schema import get_connection

        conn = get_connection(config.DB_PATH)
        try:
            # Level 1: section_type + kapitel → most precise (1-3 pages)
            if kapitel and section_type:
                rows = conn.execute(
                    "SELECT source_pdf, page_start, page_end FROM budget_toc "
                    "WHERE year=? AND kapitel=? AND section_type=? AND level='section' "
                    "ORDER BY page_start LIMIT 3",
                    (year, kapitel, section_type)
                ).fetchall()
                if rows:
                    return self._scan_toc_result(question, rows, year, einzelplan, kapitel, locator)

            # Level 2: kapitel only → find section-level pages within Kapitel
            if kapitel:
                # Try section-level entries first (most precise)
                rows = conn.execute(
                    "SELECT source_pdf, page_start, page_start FROM budget_toc "
                    "WHERE year=? AND kapitel=? AND level='section' "
                    "ORDER BY page_start LIMIT 5",
                    (year, kapitel)
                ).fetchall()
                if not rows:
                    # Fallback to kapitel-level range but cap at first 5 pages
                    rows = conn.execute(
                        "SELECT source_pdf, page_start, MIN(page_start + 4, page_end) FROM budget_toc "
                        "WHERE year=? AND kapitel=? AND level='kapitel' "
                        "ORDER BY page_start LIMIT 1",
                        (year, kapitel)
                    ).fetchall()
                if rows:
                    return self._scan_toc_result(question, rows, year, einzelplan, kapitel, locator)

            # Level 3: einzelplan + section_type → first matching section in EP
            if einzelplan and section_type:
                rows = conn.execute(
                    "SELECT source_pdf, page_start, page_start FROM budget_toc "
                    "WHERE year=? AND einzelplan=? AND section_type=? AND level='section' "
                    "ORDER BY page_start LIMIT 3",
                    (year, einzelplan, section_type)
                ).fetchall()
                if rows:
                    return self._scan_toc_result(question, rows, year, einzelplan, kapitel, locator)

            # Level 4: einzelplan only → EP Überblick (first summary page only)
            if einzelplan:
                # First try: first ueberblick section (EP-level summary, not per-Kapitel)
                rows = conn.execute(
                    "SELECT source_pdf, page_start, page_start FROM budget_toc "
                    "WHERE year=? AND einzelplan=? AND section_type='ueberblick' AND level='section' "
                    "ORDER BY page_start LIMIT 1",
                    (year, einzelplan)
                ).fetchall()
                if rows:
                    return self._scan_toc_result(question, rows, year, einzelplan, kapitel, locator)
                # Fallback: first pages of the EP
                rows = conn.execute(
                    "SELECT source_pdf, page_start, page_end FROM budget_toc "
                    "WHERE year=? AND einzelplan=? AND level='ep' "
                    "ORDER BY page_start LIMIT 1",
                    (year, einzelplan)
                ).fetchall()
                if rows:
                    r = rows[0]
                    rows = [(r[0], r[1], min(r[1] + 4, r[2]))]
                    return self._scan_toc_result(question, rows, year, einzelplan, kapitel, locator)

            # Level 5: Year only → Gesamtplan overview
            rows = conn.execute(
                "SELECT source_pdf, page_start, page_end FROM budget_toc "
                "WHERE year=? AND section_type='gesamtplan' AND level='section' "
                "ORDER BY page_start LIMIT 3",
                (year,)
            ).fetchall()
            if rows:
                return self._scan_toc_result(question, rows, year, einzelplan, kapitel, locator)

        finally:
            conn.close()

        # Fallback: FTS5 grep for Kapitel/EP number in page text
        if kapitel or einzelplan:
            from src.db.loader import DataLoader
            search_terms = []
            if kapitel:
                search_terms.extend([f'"Kapitel {kapitel}"', f'"{kapitel}"'])
            if einzelplan:
                search_terms.extend([f'"Einzelplan {einzelplan}"'])

            for term in search_terms:
                results = DataLoader.search_fulltext(
                    config.DB_PATH, query=term, year=year if year else None, limit=10
                )
                if results:
                    # Filter to most relevant (prefer pages that mention both EP and Kap)
                    best = sorted(results, key=lambda r: (
                        -(1 if kapitel and kapitel in (r.get('snippet') or '') else 0),
                        r['page_number']
                    ))[:5]

                    best_pdf = best[0]['source_pdf']
                    pages = sorted(set(r['page_number'] for r in best if r['source_pdf'] == best_pdf))[:5]

                    pdf_path = locator.get_pdf_path(year, best_pdf)
                    if pdf_path and pdf_path.exists():
                        return self._scan_and_cite(question, pdf_path, pages, year, einzelplan, kapitel)

        # Also try the old DocumentLocator as last resort
        locations = locator.locate(year=year, einzelplan=einzelplan, kapitel=kapitel, context_pages=1)
        if locations:
            loc = max(locations, key=lambda l: l.entry_count)
            pdf_path = locator.get_pdf_path(loc.year, loc.pdf_filename)
            if pdf_path:
                pages = list(range(loc.page_start, min(loc.page_end + 1, loc.page_start + 5)))
                return self._scan_and_cite(question, pdf_path, pages, year, einzelplan, kapitel)

        return f"Keine relevanten Seiten für Jahr {year}, EP {einzelplan}, Kap {kapitel} gefunden.", []

    def _scan_toc_result(self, question, toc_rows, year, einzelplan, kapitel, locator):
        """Scan pages from TOC lookup results.
        
        Smart page collection:
        - For small ranges (≤5 pages): include all pages in range
        - For large ranges (>5 pages): take only the start page (section header)
          plus up to 4 subsequent pages
        This avoids scanning 100+ page ranges when a section spans many pages.
        """
        all_pages: list[int] = []
        pdf_name = toc_rows[0][0]
        for _, page_start, page_end in toc_rows:
            span = page_end - page_start + 1
            if span <= 5:
                # Small range — include all pages
                for p in range(page_start, page_end + 1):
                    if p not in all_pages:
                        all_pages.append(p)
            else:
                # Large range — take start page + up to 4 more
                for p in range(page_start, min(page_start + 5, page_end + 1)):
                    if p not in all_pages:
                        all_pages.append(p)
            if len(all_pages) >= 8:
                all_pages = all_pages[:8]
                break

        pdf_path = locator.get_pdf_path(year, pdf_name)
        if not pdf_path or not pdf_path.exists():
            return f"PDF nicht gefunden: {pdf_name}", []

        return self._scan_and_cite(question, pdf_path, all_pages, year, einzelplan, kapitel)

    # ------------------------------------------------------------------
    # FTS5 search with smart ranking
    # ------------------------------------------------------------------

    def _search_and_scan(self, question, search_term, year, einzelplan, kapitel, section_type, locator):
        """FTS5 search with synonym expansion and combined scoring."""
        from src.db.loader import DataLoader
        from src.extract.semantic_bridge import SemanticBridge

        bridge = SemanticBridge()

        # Check for organizational mapping first
        org = bridge.resolve_organization(search_term)
        if org and not einzelplan:
            einzelplan = org.get('einzelplan')
        if org and not kapitel:
            kapitel = org.get('kapitel')

        # Expand search terms
        all_terms = bridge.expand_search_terms(search_term)

        # Build FTS5 OR query
        fts_query = " OR ".join(f'"{t}"' for t in all_terms if t.strip())

        db_path = self._db_path if hasattr(self, '_db_path') else config.DB_PATH
        results = DataLoader.search_fulltext(
            db_path,
            query=fts_query,
            year=year if year else None,
            limit=30,
        )

        if not results:
            # Try individual terms
            for term in all_terms:
                results = DataLoader.search_fulltext(
                    db_path,
                    query=term,
                    year=year if year else None,
                    limit=30,
                )
                if results:
                    break

        if not results:
            if einzelplan or kapitel:
                return self._toc_navigate_and_scan(question, year, einzelplan, kapitel, section_type, locator)
            return f"Keine Treffer für '{search_term}' (auch mit Synonymen: {all_terms}).", []

        # Score results
        scored_results = self._score_search_results(results, einzelplan, kapitel, search_term, all_terms)

        # Take top 5 pages
        top_pages = scored_results[:5]
        if not top_pages:
            return f"Keine relevanten Seiten gefunden.", []

        best_pdf = top_pages[0]['source_pdf']
        found_pages = sorted(set(r['page_number'] for r in top_pages if r['source_pdf'] == best_pdf))

        pdf_path = locator.get_pdf_path(year, best_pdf)
        if pdf_path and pdf_path.exists():
            snippets = "\n".join(
                f"  S.{r['page_number']} (score:{r['score']}): {r['snippet']}"
                for r in top_pages[:3]
            )[:500]
            scan_result, citations = self._scan_and_cite(
                question, pdf_path, found_pages, year, einzelplan, kapitel
            )
            terms_used = ", ".join(all_terms[:5])
            return (
                f"Suche '{search_term}' (+ Synonyme: {terms_used}) → {len(found_pages)} Seiten:\n"
                f"{snippets}\n\n{scan_result}"
            ), citations

        return f"PDF nicht gefunden für Suchergebnisse.", []

    def _score_search_results(self, results, einzelplan, kapitel, primary_term, all_terms):
        """Score and rank FTS5 results using combined signals."""
        scored = []
        for r in results:
            score = 0
            snippet = r.get('snippet', '').lower()

            # FTS5 relevance (exact primary term)
            if primary_term.lower() in snippet:
                score += 40
            elif any(t.lower() in snippet for t in all_terms):
                score += 30
            else:
                score += 10  # At least matched via FTS5

            # Structural match
            if einzelplan and r.get('einzelplan') == einzelplan:
                score += 10
            if kapitel and r.get('kapitel') == kapitel:
                score += 20

            r['score'] = score
            scored.append(r)

        scored.sort(key=lambda x: -x['score'])
        return scored

    # ------------------------------------------------------------------
    # PDF resolution helper
    # ------------------------------------------------------------------

    def _resolve_pdf(self, locator, year, pdf_filename=None):
        """Resolve PDF path for a given year."""
        if pdf_filename:
            return locator.get_pdf_path(year, pdf_filename)
        return locator.get_main_budget_pdf(year)

    def _scan_and_cite(
        self,
        question: str,
        pdf_path: Path,
        page_numbers: list[int],
        year: int,
        einzelplan: str | None,
        kapitel: str | None,
    ) -> tuple[str, list[Citation]]:
        """Shared scanning + citation logic."""
        from src.query.page_scanner import PageScanner

        try:
            scanner = PageScanner(pdf_path)
            # PageScanner.scan() uses 0-indexed start and exclusive end
            page_start_0 = min(page_numbers) - 1  # convert 1-indexed → 0-indexed
            page_end_excl = max(page_numbers)      # exclusive upper bound
            result = scanner.scan(
                question=question,
                start_page=page_start_0,
                end_page=page_end_excl,
            )

            citations: list[Citation] = []
            for page_num in result.pages_scanned:
                citations.append(Citation(
                    source_pdf=pdf_path.name,
                    page_number=page_num,
                    year=year,
                    einzelplan=einzelplan,
                    kapitel=kapitel,
                    path=(
                        str(pdf_path.relative_to(config.PROJECT_ROOT))
                        if pdf_path
                        else ""
                    ),
                ))

            filename = pdf_path.name
            return (
                f"Dokument-Analyse ({filename}, "
                f"Seiten {page_numbers[0]}-{page_numbers[-1]}):\n"
                f"{result.answer}\n"
                f"(Konfidenz: {result.confidence})"
            ), citations

        except Exception as exc:
            logger.warning("read_document scan failed: %s", exc)
            return f"Dokument-Scan fehlgeschlagen: {exc}", []

    # ------------------------------------------------------------------
    # Agent loop
    # ------------------------------------------------------------------

    def _run_agent_loop(
        self,
        question: str,
        max_iterations: int,
        conversation_history: list[dict[str, str]] | None = None,
    ) -> AnswerResult:
        """Run the ReAct agent loop with OpenAI function calling."""
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": self._system_prompt},
        ]

        # Inject prior conversation so the LLM can resolve follow-up
        # references (e.g. "und davon?").  Only user/assistant pairs are
        # included — tool-call details from earlier rounds are omitted so
        # the model doesn't try to re-invoke tools for old questions.
        if conversation_history:
            messages.extend(conversation_history)

        messages.append({"role": "user", "content": question})

        tools_used: list[str] = []
        sql_queries: list[str] = []
        sources: list[str] = []
        citations: list[Citation] = []

        for iteration in range(max_iterations):
            logger.debug("Agent iteration %d/%d", iteration + 1, max_iterations)

            response = self._call_chat_completions(messages)
            message = response.choices[0].message

            # Append the assistant message to conversation history.
            # We must serialise it properly for the next round-trip.
            messages.append(self._assistant_message_to_dict(message))

            if not message.tool_calls:
                # No more tool calls → the model produced a final answer.
                answer_text = message.content or ""
                confidence = self._infer_confidence(tools_used, answer_text)
                return AnswerResult(
                    question=question,
                    answer=answer_text,
                    sources=sources,
                    tools_used=tools_used,
                    sql_queries=sql_queries,
                    confidence=confidence,
                    citations=citations,
                )

            # Process each tool call.
            for tool_call in message.tool_calls:
                fn_name = tool_call.function.name
                try:
                    fn_args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError:
                    fn_args = {}

                result_text = self._execute_tool(
                    fn_name,
                    fn_args,
                    sources=sources,
                    sql_queries=sql_queries,
                    citations=citations,
                )
                tools_used.append(fn_name)

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": result_text,
                    }
                )

        # Safety cap reached without a final answer.
        logger.warning(
            "Max iterations (%d) reached for question: %s",
            max_iterations,
            question,
        )
        return AnswerResult(
            question=question,
            answer=(
                "Die maximale Anzahl an Iterationen wurde erreicht. "
                "Bitte versuche eine spezifischere Frage — z.B. mit konkretem Einzelplan, "
                "Kapitel oder Jahr."
            ),
            sources=sources,
            tools_used=tools_used,
            sql_queries=sql_queries,
            confidence="low",
            citations=citations,
        )

    # ------------------------------------------------------------------
    # OpenAI API helpers
    # ------------------------------------------------------------------

    def _call_chat_completions(
        self, messages: list[dict[str, Any]]
    ) -> Any:
        """Call Azure OpenAI chat completions with function calling support."""
        try:
            return self._client.chat.completions.create(
                model=self._deployment,
                messages=messages,
                tools=self._define_tools(),
                tool_choice="auto",
                temperature=0.0,
                max_tokens=4096,
            )
        except RateLimitError as exc:
            logger.warning("Rate-limit reached: %s", exc)
            raise RuntimeError(
                "Azure OpenAI Rate-Limit erreicht. "
                "Bitte kurz warten und erneut versuchen."
            ) from exc
        except APIConnectionError as exc:
            logger.error("Connection error: %s", exc)
            raise RuntimeError(
                "Verbindung zu Azure OpenAI fehlgeschlagen."
            ) from exc
        except APIStatusError as exc:
            logger.error("API error (HTTP %d): %s", exc.status_code, exc)
            raise RuntimeError(
                "Azure OpenAI API-Fehler (HTTP %d): %s"
                % (exc.status_code, exc.message)
            ) from exc

    @staticmethod
    def _assistant_message_to_dict(message: Any) -> dict[str, Any]:
        """Convert an OpenAI ChatCompletionMessage to a plain dict.

        The ``openai`` library returns Pydantic-like objects; we need
        plain dicts for subsequent API calls.
        """
        msg: dict[str, Any] = {
            "role": "assistant",
            "content": message.content,
        }
        if message.tool_calls:
            msg["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in message.tool_calls
            ]
        return msg

    @staticmethod
    def _infer_confidence(tools_used: list[str], answer: str) -> str:
        """Heuristic confidence based on which tools were actually used."""
        if not tools_used:
            return "low"  # No tools used — answered from prompt knowledge only
        used = set(tools_used)
        if "read_document" in used and "compute" in used:
            return "high"  # Read the document AND computed
        if "read_document" in used:
            return "high"  # Read the actual document
        if used:
            return "medium"
        return "low"


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------


def create_engine() -> QueryEngine:
    """Create a QueryEngine with default config paths."""
    return QueryEngine(
        db_path=config.DB_PATH,
        pdf_path=next(config.DOCS_DIR.glob("*.pdf"), None),
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    )

    engine = create_engine()
    result = engine.ask("Was ist ein Einzelplan im Bundeshaushalt?")
    print(f"Answer: {result.answer}")
    print(f"Tools:  {result.tools_used}")
    print(f"Sources: {result.sources}")
    if result.sql_queries:
        print(f"SQL:    {result.sql_queries}")
