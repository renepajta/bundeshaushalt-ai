"""SQL query agent for the German federal budget (Bundeshaushalt) Q&A system.

Translates natural-language German budget questions into SQL queries via an
LLM, executes them against a SQLite database, and returns structured results.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from src.config import config
from src.db.schema import get_connection
from src.query.llm import LLMClient

logger = logging.getLogger(__name__)

# Maximum number of LLM retry attempts when SQL execution fails.
_MAX_RETRIES = 3

# Hard cap on result rows to prevent runaway queries.
_MAX_ROWS = 1000

# Statements that must never appear in generated SQL.
_FORBIDDEN_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|REPLACE|ATTACH|DETACH|VACUUM|REINDEX"
    r"|PRAGMA|GRANT|REVOKE|SAVEPOINT|RELEASE|BEGIN|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class SQLResult:
    """Structured result of a SQL query execution."""

    query: str
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    row_count: int = 0
    error: str | None = None


# ---------------------------------------------------------------------------
# Schema description for the LLM (German budget terminology)
# ---------------------------------------------------------------------------

_BUDGET_TERMINOLOGY = """\
Wichtige Begriffe zum deutschen Bundeshaushalt:
- einzelplan: Ministeriums-Haushaltsplan (2-stelliger Code, z.B. '06' = BMI, '14' = BMVg)
- kapitel: Kapitel innerhalb eines Einzelplans (4-stelliger Code, z.B. '0111', '1403')
- titel: Haushaltsstelle / Titelzeile (z.B. '431 57', '531 01')
- ausgaben_soll: Geplante Ausgaben (Soll-Wert)
- ausgaben_ist: Tatsächliche Ausgaben (Ist-Wert)
- einnahmen_soll / einnahmen_ist: Geplante bzw. tatsächliche Einnahmen
- is_verrechnungstitel: Interne Verrechnungstitel (bei Summen oft ausschließen mit is_verrechnungstitel=0)
- version: 'entwurf' (Regierungsentwurf), 'beschluss' (vom Bundestag beschlossen), 'nachtrag' (Nachtragshaushalt)
- besoldungsgruppe: Laufbahn-/Besoldungsgruppe (z.B. 'A13', 'ORR')
- planstellen_gesamt: Gesamtzahl der Planstellen
- verpflichtungsermachtigungen: Zukünftige Zahlungszusagen (VE)
- Die Datenbank enthält Jahre von 2005 bis 2026
- Bei historischen Vergleichen: GROUP BY year, ORDER BY year
- Alte Begriffe beachten: 'Wehrübende' = 'Reservedienstleistende', 'Bundesgrenzschutz' = 'Bundespolizei'
- kapitel_meta enthält Kapitel-Zuordnungen pro Jahr — nutze für historische Fragen
- einzelplan_meta enthält Ministeriums-Namen pro Jahr
- Reservedienstleistende, Wehrübende, Soldaten: Suche in haushaltsdaten.titel_text (LIKE '%Reserv%'), NICHT in personalhaushalt.besoldungsgruppe
- personalhaushalt enthält Besoldungsgruppen (A 13, B 3, ORR, AT, E 9) und Planstellen-Anzahlen
- haushaltsdaten.titel_text enthält Beschreibungen der Haushaltstitel (z.B. 'Bezüge der Reservedienstleistenden')
- Bei Personalfragen: Erst kapitel-level in personalhaushalt abfragen, dann optional nach titel filtern
- is_verrechnungstitel=0 nur bei Summenabfragen verwenden, NICHT bei Einzeltitel-Abfragen
- ausgaben_ist: Tatsächliche Ausgaben — verfügbar für ca. 80% der Einträge 2020-2023
- Bei Vergleichsfragen ("Veränderung", "Differenz") oder wenn nach "Ausgaben" ohne "Soll" gefragt wird: PRÜFE ERST ob ausgaben_ist verfügbar ist (WHERE ausgaben_ist IS NOT NULL)
- Soll vs Ist: "Soll" = geplant, "Ist" = tatsächlich ausgegeben. Bei "Ausgaben" ohne Spezifikation, nutze ausgaben_soll
- Bei Soll-Ist-Vergleichen: SELECT SUM(ausgaben_soll) AS soll, SUM(ausgaben_ist) AS ist FROM haushaltsdaten WHERE ausgaben_ist IS NOT NULL AND ...
- IMMER source_pdf und source_page in SELECT aufnehmen wenn möglich, um Quellen zu zitieren
- Beispiel: SELECT source_pdf, source_page, kapitel, titel, ausgaben_soll FROM haushaltsdaten WHERE ...
"""

_FEW_SHOT_EXAMPLES = """\
Beispielabfragen:

1. Gesamtausgaben (Soll) eines Einzelplans in einem Jahr:
   SELECT source_pdf, source_page, SUM(ausgaben_soll) AS summe_soll
   FROM haushaltsdaten
   WHERE year = 2024 AND einzelplan = '06' AND version = 'beschluss' AND is_verrechnungstitel = 0
   GROUP BY source_pdf, source_page;

2. Vergleich Soll vs. Ist für einen Einzelplan:
   SELECT SUM(ausgaben_soll) AS soll, SUM(ausgaben_ist) AS ist
   FROM haushaltsdaten
   WHERE year = 2023 AND einzelplan = '14' AND version = 'beschluss' AND is_verrechnungstitel = 0;

3. Personalstellen eines Kapitels:
   SELECT source_pdf, source_page, besoldungsgruppe, planstellen_gesamt
   FROM personalhaushalt
   WHERE year = 2024 AND kapitel = '0455' AND version = 'beschluss';

4. Einzelplan-Übersicht (alle Ministerien eines Jahres):
   SELECT einzelplan, einzelplan_text, SUM(h.ausgaben_soll) AS soll
   FROM haushaltsdaten h
   JOIN einzelplan_meta e USING (year, version, einzelplan)
   WHERE h.year = 2024 AND h.version = 'beschluss' AND h.is_verrechnungstitel = 0
   GROUP BY einzelplan, einzelplan_text
   ORDER BY soll DESC;

5. Verpflichtungsermächtigungen nach Fälligkeitsjahr:
   SELECT faellig_jahr, SUM(faellig_betrag) AS summe
   FROM verpflichtungsermachtigungen
   WHERE year = 2024 AND einzelplan = '14' AND version = 'beschluss'
   GROUP BY faellig_jahr
   ORDER BY faellig_jahr;

6. Ausgaben-Vergleich zwischen Jahren (ohne Verrechnungstitel):
   SELECT year, SUM(ausgaben_soll) AS soll
   FROM haushaltsdaten
   WHERE einzelplan = '06' AND is_verrechnungstitel = 0 AND year IN (2021, 2022)
   GROUP BY year
   ORDER BY year;

7. Historische Kapitel-Zuordnung über alle Jahre:
   SELECT year, kapitel, kapitel_text
   FROM kapitel_meta
   WHERE kapitel_text LIKE '%Datenschutz%'
   ORDER BY year;

8. Personalstellen mit historischem Begriffsabgleich:
   SELECT year, kapitel, besoldungsgruppe, SUM(planstellen_gesamt) AS stellen
   FROM personalhaushalt
   WHERE kapitel = '1403' AND year = 2012
   GROUP BY year, kapitel, besoldungsgruppe;

9. Einzelplan-Text über Jahre hinweg:
   SELECT year, einzelplan, einzelplan_text
   FROM einzelplan_meta
   WHERE einzelplan = '12'
   ORDER BY year;

10. Verpflichtungsermächtigungen mit Fälligkeiten:
    SELECT year, kapitel, titel, betrag_gesamt, faellig_jahr, faellig_betrag
    FROM verpflichtungsermachtigungen
    WHERE year = 2020 AND kapitel = '0622'
    ORDER BY faellig_jahr;

11. Kapitel-Inhalt über Zeit verfolgen:
    SELECT year, kapitel_text
    FROM kapitel_meta
    WHERE kapitel = '1201'
    ORDER BY year;

12. Reservedienstleistende/Wehrübende suchen (in haushaltsdaten, nicht personalhaushalt!):
    SELECT source_pdf, source_page, year, kapitel, titel, titel_text, ausgaben_soll
    FROM haushaltsdaten
    WHERE (titel_text LIKE '%Reservedienstleistende%' OR titel_text LIKE '%Wehrübende%' OR titel_text LIKE '%Wehruebende%')
    AND year = 2012
    ORDER BY kapitel;

13. Planstellen für ein Kapitel (Personalhaushalt):
    SELECT besoldungsgruppe, SUM(planstellen_gesamt) AS stellen
    FROM personalhaushalt
    WHERE year = 2020 AND kapitel = '0455'
    GROUP BY besoldungsgruppe
    ORDER BY stellen DESC;

14. Planstellen mit Titelgruppen-Filter:
    SELECT besoldungsgruppe, planstellen_gesamt
    FROM personalhaushalt
    WHERE year = 2024 AND kapitel = '1513' AND titelgruppe = '05';

15. Soll-Ist-Vergleich mit Ausschöpfungsquote:
    SELECT SUM(ausgaben_soll) AS soll, SUM(ausgaben_ist) AS ist,
           ROUND(100.0 * SUM(ausgaben_ist) / SUM(ausgaben_soll), 2) AS ausschoepfung_prozent
    FROM haushaltsdaten
    WHERE year = 2023 AND einzelplan = '14' AND is_verrechnungstitel = 0 AND ausgaben_ist IS NOT NULL;

16. Alle Kapitel-Zuordnungen über alle Jahre (für historische Fragen):
    SELECT year, kapitel, kapitel_text
    FROM kapitel_meta
    WHERE kapitel_text LIKE '%Datenschutz%'
    ORDER BY year;

17. Titel-Text-Suche für Rollen/Begriffe:
    SELECT year, einzelplan, kapitel, titel, titel_text, ausgaben_soll
    FROM haushaltsdaten
    WHERE titel_text LIKE '%Reservist%' OR titel_text LIKE '%Wehrübende%'
    ORDER BY year;

18. VE für ein bestimmtes Kapitel mit Fälligkeiten:
    SELECT kapitel, titel, betrag_gesamt, faellig_jahr, faellig_betrag
    FROM verpflichtungsermachtigungen
    WHERE year = 2020 AND kapitel = '0622'
    ORDER BY titel, faellig_jahr;

19. Planstellen-Aggregation mit tariflich/AT-Aufteilung:
    SELECT SUM(planstellen_gesamt) AS gesamt,
           SUM(planstellen_tariflich) AS tariflich,
           SUM(planstellen_aussertariflich) AS aussertariflich
    FROM personalhaushalt
    WHERE year = 2020 AND kapitel = '0455' AND titel LIKE '428%';

20. Ausgaben mit Soll UND Ist für Vergleiche:
    SELECT year, SUM(ausgaben_soll) AS soll, SUM(ausgaben_ist) AS ist
    FROM haushaltsdaten
    WHERE einzelplan = '06' AND is_verrechnungstitel = 0 AND year IN (2021, 2022)
    GROUP BY year ORDER BY year;

21. Titel-Text-Suche mit Quellenverweisen (IMMER source_pdf, source_page einschließen):
    SELECT source_pdf, source_page, kapitel, titel, titel_text, ausgaben_soll
    FROM haushaltsdaten
    WHERE year = 2012 AND (titel_text LIKE '%Reservist%' OR titel_text LIKE '%Wehrübende%')
    LIMIT 20;

22. Planstellen-Summe für ein Kapitel (IMMER SUM verwenden, NICHT Einzelzeilen):
    SELECT SUM(planstellen_gesamt) AS gesamt
    FROM personalhaushalt
    WHERE year = 2020 AND kapitel = '0455';
"""


# ---------------------------------------------------------------------------
# SQL Agent
# ---------------------------------------------------------------------------


class SQLAgent:
    """Translates natural-language budget questions into SQL and executes them."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = Path(db_path) if db_path else config.DB_PATH
        self._llm = LLMClient()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, question: str) -> SQLResult:
        """Translate a natural-language question to SQL and execute it.

        The method will retry up to ``_MAX_RETRIES`` times if the generated
        SQL fails to execute, feeding the error back to the LLM for
        correction.
        """
        schema_desc = self._get_schema_description()
        sql = self._llm.generate_sql(question, schema_desc)
        logger.info("Generated SQL: %s", sql)

        last_error: str | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                sql = self._validate_sql(sql)
                return self._execute(sql)
            except ValueError as exc:
                # Validation failure – ask LLM to fix
                last_error = str(exc)
                logger.warning(
                    "SQL validation failed (attempt %d/%d): %s",
                    attempt, _MAX_RETRIES, last_error,
                )
            except sqlite3.Error as exc:
                # Execution failure – ask LLM to fix
                last_error = str(exc)
                logger.warning(
                    "SQL execution failed (attempt %d/%d): %s",
                    attempt, _MAX_RETRIES, last_error,
                )

            if attempt < _MAX_RETRIES:
                sql = self._ask_llm_to_fix(question, sql, last_error, schema_desc)
                logger.info("Retried SQL (attempt %d): %s", attempt + 1, sql)

        # All retries exhausted
        return SQLResult(query=sql, error=last_error)

    def format_results(self, result: SQLResult) -> str:
        """Format SQL results as a readable German text table."""
        return self._format_results(result)

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def _get_schema_description(self) -> str:
        """Build a human-readable schema description from the live database."""
        conn = get_connection(self._db_path)
        try:
            return self._build_schema_text(conn)
        finally:
            conn.close()

    def _build_schema_text(self, conn: sqlite3.Connection) -> str:
        parts: list[str] = [_BUDGET_TERMINOLOGY, ""]

        # Enumerate tables from sqlite_master
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()

        for (table_name,) in tables:
            parts.append(f"Tabelle: {table_name}")
            parts.append("-" * (9 + len(table_name)))

            # Column info via PRAGMA
            columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            col_lines: list[str] = []
            for col in columns:
                # col: (cid, name, type, notnull, default, pk)
                name = col[1]
                dtype = col[2] or "TEXT"
                pk = " [PK]" if col[5] else ""
                col_lines.append(f"  {name} ({dtype}){pk}")
            parts.append("\n".join(col_lines))

            # Sample data (first 3 rows) to help the LLM understand values
            try:
                sample_rows = conn.execute(
                    f"SELECT * FROM {table_name} LIMIT 3"  # noqa: S608
                ).fetchall()
                if sample_rows:
                    col_names = [c[1] for c in columns]
                    parts.append(f"  Beispieldaten ({len(sample_rows)} Zeilen):")
                    for row in sample_rows:
                        vals = ", ".join(
                            f"{c}={v!r}" for c, v in zip(col_names, row)
                        )
                        parts.append(f"    {vals}")
            except sqlite3.Error:
                pass  # table may be empty or inaccessible

            parts.append("")  # blank line between tables

        parts.append(_FEW_SHOT_EXAMPLES)

        # Add data availability context
        try:
            years = conn.execute(
                "SELECT DISTINCT year FROM haushaltsdaten ORDER BY year"
            ).fetchall()
            year_list = [str(r[0]) for r in years]
            parts.append(f"\nVerfügbare Jahre in haushaltsdaten: {', '.join(year_list)}")

            versions = conn.execute(
                "SELECT DISTINCT version FROM haushaltsdaten"
            ).fetchall()
            ver_list = [str(r[0]) for r in versions]
            parts.append(f"Verfügbare Versionen: {', '.join(ver_list)}")
        except Exception:
            pass

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # SQL validation
    # ------------------------------------------------------------------

    def _validate_sql(self, sql: str) -> str:
        """Validate and sanitise generated SQL.

        Returns the cleaned SQL string.  Raises ``ValueError`` if the
        statement is unsafe.
        """
        # Strip markdown fences the LLM might emit
        sql = self._strip_markdown(sql)

        # Remove SQL comments
        sql = re.sub(r"--[^\n]*", "", sql)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        sql = sql.strip().rstrip(";").strip()

        if not sql:
            raise ValueError("Leeres SQL-Statement erhalten.")

        # Must start with SELECT (or WITH for CTEs)
        first_word = sql.split()[0].upper()
        if first_word not in ("SELECT", "WITH"):
            raise ValueError(
                f"Nur SELECT-Abfragen sind erlaubt. Erhalten: {first_word}..."
            )

        # Reject forbidden DDL/DML keywords
        match = _FORBIDDEN_KEYWORDS.search(sql)
        if match:
            raise ValueError(
                f"Unzulässiges Schlüsselwort im SQL: {match.group()}"
            )

        # Enforce row limit if none is present
        if not re.search(r"\bLIMIT\b", sql, re.IGNORECASE):
            sql = f"{sql}\nLIMIT {_MAX_ROWS}"

        return sql

    @staticmethod
    def _strip_markdown(text: str) -> str:
        """Remove common markdown code-fence wrappers."""
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        return text.strip()

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _execute(self, sql: str) -> SQLResult:
        """Execute a validated SELECT statement and return structured results."""
        conn = get_connection(self._db_path)
        try:
            cursor = conn.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = [tuple(row) for row in cursor.fetchall()]
            return SQLResult(
                query=sql,
                columns=columns,
                rows=rows,
                row_count=len(rows),
            )
        except sqlite3.Error:
            conn.close()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Error recovery
    # ------------------------------------------------------------------

    def _ask_llm_to_fix(
        self,
        question: str,
        failed_sql: str,
        error_msg: str | None,
        schema_desc: str,
    ) -> str:
        """Ask the LLM to correct a failing SQL query."""
        system = (
            "Du bist ein SQL-Experte für den deutschen Bundeshaushalt.\n\n"
            f"Datenbank-Schema (SQLite):\n{schema_desc}\n\n"
            "Regeln:\n"
            "- Erzeuge ausschließlich ein einzelnes SELECT-Statement.\n"
            "- Gib **nur** den SQL-Code zurück – keine Erklärungen, kein Markdown.\n"
        )
        user = (
            f"Die folgende SQL-Abfrage hat einen Fehler erzeugt.\n\n"
            f"Ursprüngliche Frage: {question}\n\n"
            f"Fehlerhaftes SQL:\n{failed_sql}\n\n"
            f"Fehlermeldung:\n{error_msg}\n\n"
            f"Bitte korrigiere das SQL-Statement."
        )
        fixed = self._llm.chat_with_system(system, user, temperature=0.0)
        return self._strip_markdown(fixed)

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    def _format_results(self, result: SQLResult) -> str:
        """Format ``SQLResult`` as a human-readable German text table."""
        if result.error:
            return f"Fehler: {result.error}"

        if not result.rows:
            return "Keine Ergebnisse gefunden."

        # Calculate column widths
        col_widths: list[int] = [len(c) for c in result.columns]
        for row in result.rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(self._format_cell(val)))

        # Header
        header = " | ".join(
            c.ljust(w) for c, w in zip(result.columns, col_widths)
        )
        separator = "-+-".join("-" * w for w in col_widths)

        lines = [header, separator]

        # Data rows
        for row in result.rows:
            line = " | ".join(
                self._format_cell(val).ljust(w)
                for val, w in zip(row, col_widths)
            )
            lines.append(line)

        lines.append(f"\n({result.row_count} Zeile{'n' if result.row_count != 1 else ''})")
        return "\n".join(lines)

    @staticmethod
    def _format_cell(value: object) -> str:
        """Format a single cell value for display."""
        if value is None:
            return ""
        if isinstance(value, float):
            # German-style thousands separator for large numbers
            if abs(value) >= 1_000:
                return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{value:.2f}".replace(".", ",")
        return str(value)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    )

    agent = SQLAgent()

    sample_questions = [
        "Wie hoch sind die Ausgaben des Einzelplans 06 im Jahr 2022?",
        "Vergleiche Soll und Ist des Einzelplans 14 für 2023.",
        "Welche Besoldungsgruppen gibt es im Kapitel 0455 im Jahr 2024?",
    ]

    for q in sample_questions:
        print(f"\n{'=' * 70}")
        print(f"Frage: {q}")
        print("=" * 70)
        result = agent.query(q)
        print(f"SQL: {result.query}")
        print(f"Zeilen: {result.row_count}")
        if result.rows:
            for row in result.rows[:5]:
                print(f"  {row}")
        if result.error:
            print(f"Fehler: {result.error}")
        print()
