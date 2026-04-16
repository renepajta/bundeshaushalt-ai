"""Load parsed budget data into the SQLite database.

Takes a ``ParsedBudget`` from :mod:`src.extract.budget_parser` and inserts its
contents into the tables defined in :mod:`src.db.schema`.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from src.extract.budget_parser import ParsedBudget

logger = logging.getLogger(__name__)


class DataLoader:
    """Load parsed budget data into a SQLite database."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, parsed: ParsedBudget) -> dict:
        """Load all parsed data into the database.

        Returns a dict with row counts for each table.
        """
        stats: dict[str, int] = {}

        stats["einzelplan_meta"] = self._load_einzelplan_meta(parsed)
        stats["kapitel_meta"] = self._load_kapitel_meta(parsed)
        stats["haushaltsdaten"] = self._load_haushaltsdaten(parsed)
        stats["personalhaushalt"] = self._load_personalhaushalt(parsed)
        stats["verpflichtungsermachtigungen"] = self._load_verpflichtungen(parsed)
        stats["sachverhalte"] = self._load_sachverhalte(parsed)

        self.db.commit()
        logger.info("Load complete: %s", stats)
        return stats

    def load_reference_data(self) -> int:
        """Load macroeconomic reference data needed for budget context.

        Known values used in sample Q&A workflows.

        Returns the number of rows inserted.
        """
        reference_rows = [
            # BIP (Bruttoinlandsprodukt / GDP) in thousands of € — historical
            (2005, "BIP", 2_301_000_000, "Statistisches Bundesamt"),
            (2006, "BIP", 2_393_000_000, "Statistisches Bundesamt"),
            (2007, "BIP", 2_513_000_000, "Statistisches Bundesamt"),
            (2008, "BIP", 2_561_000_000, "Statistisches Bundesamt"),
            (2009, "BIP", 2_460_000_000, "Statistisches Bundesamt"),
            (2010, "BIP", 2_580_000_000, "Statistisches Bundesamt"),
            (2011, "BIP", 2_703_000_000, "Statistisches Bundesamt"),
            (2012, "BIP", 2_758_000_000, "Statistisches Bundesamt"),
            (2013, "BIP", 2_826_000_000, "Statistisches Bundesamt"),
            (2014, "BIP", 2_938_000_000, "Statistisches Bundesamt"),
            (2015, "BIP", 3_048_000_000, "Statistisches Bundesamt"),
            (2016, "BIP", 3_159_000_000, "Statistisches Bundesamt"),
            (2017, "BIP", 3_277_000_000, "Statistisches Bundesamt"),
            (2018, "BIP", 3_388_000_000, "Statistisches Bundesamt"),
            (2019, "BIP", 3_473_000_000, "Statistisches Bundesamt"),
            (2020, "BIP", 3_367_000_000, "Statistisches Bundesamt (COVID)"),
            (2021, "BIP", 3_613_000_000, "Statistisches Bundesamt"),
            (2022, "BIP", 3_877_000_000, "Statistisches Bundesamt"),
            (2023, "BIP", 4_122_000_000, "Statistisches Bundesamt"),
            # BIP — current/forecast
            (2024, "BIP", 4_261_350_000, "BIP 2024, Statistisches Bundesamt"),
            (2025, "BIP", 4_469_910_000, "BIP 2025, Prognose Bundesregierung"),
            (2026, "BIP", 4_646_830_000, "BIP 2026, Prognose Bundesregierung"),
            # Inflationsrate (HVPI, %) — historical
            (2005, "Inflationsrate", 1.5, "Statistisches Bundesamt"),
            (2006, "Inflationsrate", 1.6, "Statistisches Bundesamt"),
            (2007, "Inflationsrate", 2.3, "Statistisches Bundesamt"),
            (2008, "Inflationsrate", 2.6, "Statistisches Bundesamt"),
            (2009, "Inflationsrate", 0.3, "Statistisches Bundesamt"),
            (2010, "Inflationsrate", 1.1, "Statistisches Bundesamt"),
            (2011, "Inflationsrate", 2.1, "Statistisches Bundesamt"),
            (2012, "Inflationsrate", 2.0, "Statistisches Bundesamt"),
            (2013, "Inflationsrate", 1.5, "Statistisches Bundesamt"),
            (2014, "Inflationsrate", 0.9, "Statistisches Bundesamt"),
            (2015, "Inflationsrate", 0.3, "Statistisches Bundesamt"),
            (2016, "Inflationsrate", 0.5, "Statistisches Bundesamt"),
            (2017, "Inflationsrate", 1.5, "Statistisches Bundesamt"),
            (2018, "Inflationsrate", 1.8, "Statistisches Bundesamt"),
            (2019, "Inflationsrate", 1.4, "Statistisches Bundesamt"),
            (2020, "Inflationsrate", 0.5, "Statistisches Bundesamt"),
            (2021, "Inflationsrate", 3.1, "Statistisches Bundesamt"),
            (2022, "Inflationsrate", 6.9, "Statistisches Bundesamt"),
            (2023, "Inflationsrate", 5.9, "Statistisches Bundesamt"),
            # Inflationsrate — current/forecast
            (2024, "Inflationsrate", 2.2, "HVPI 2024"),
            (2025, "Inflationsrate", 2.1, "Prognose 2025"),
            (2026, "Inflationsrate", 1.9, "Prognose 2026"),
            # BIP-Deflator (%) — historical
            (2005, "BIP_Deflator", 0.7, "Statistisches Bundesamt"),
            (2010, "BIP_Deflator", 0.6, "Statistisches Bundesamt"),
            (2015, "BIP_Deflator", 2.1, "Statistisches Bundesamt"),
            (2020, "BIP_Deflator", 1.8, "Statistisches Bundesamt"),
            (2021, "BIP_Deflator", 3.0, "Statistisches Bundesamt"),
            (2022, "BIP_Deflator", 5.3, "Statistisches Bundesamt"),
            (2023, "BIP_Deflator", 5.8, "Statistisches Bundesamt"),
            # BIP-Deflator — current/forecast
            (2024, "BIP_Deflator", 3.1, "2024"),
            (2025, "BIP_Deflator", 2.5, "Prognose 2025"),
            (2026, "BIP_Deflator", 2.0, "Prognose 2026"),
            # Bundeshaushalt total (1 000 €) — from Gesamtplan
            (2025, "Bundeshaushalt_Ausgaben", 503_006_410, "HG 2025"),
            (2026, "Bundeshaushalt_Ausgaben", 520_475_593, "HG 2026 Entwurf"),
            # NATO 2%-Ziel reference: Defence share of BIP
            (2025, "NATO_Ziel_Prozent", 2.0, "NATO 2%-Ziel"),
            (2026, "NATO_Ziel_Prozent", 2.0, "NATO 2%-Ziel"),
            # Einzelplan 14 (Verteidigung) — known from document
            (2026, "EP14_Ausgaben", 82_687_437, "EP 14 Soll 2026 Entwurf"),
            (2025, "EP14_Ausgaben", 62_431_603, "EP 14 Soll 2025"),
            # Sondervermögen Bundeswehr
            (2026, "Sondervermoegen_BW", 25_509_765, "SV Bundeswehr 2026"),
        ]

        count = 0
        for year, indicator, value, notes in reference_rows:
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO referenzdaten
                       (year, indicator, value, notes)
                       VALUES (?, ?, ?, ?)""",
                    (year, indicator, value, notes),
                )
                count += 1
            except sqlite3.Error:
                logger.warning(
                    "Failed to insert reference: %s/%s", year, indicator
                )

        self.db.commit()
        logger.info("Loaded %d reference data rows", count)
        return count

    # ------------------------------------------------------------------
    # Private loaders
    # ------------------------------------------------------------------

    def _load_einzelplan_meta(self, parsed: ParsedBudget) -> int:
        """Insert Einzelplan metadata."""
        count = 0
        for ep in parsed.einzelplan_meta:
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO einzelplan_meta
                       (year, version, einzelplan, einzelplan_text,
                        source_pdf, source_page)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        parsed.year,
                        parsed.version,
                        ep["einzelplan"],
                        ep.get("einzelplan_text"),
                        ep.get("source_pdf"),
                        ep.get("source_page"),
                    ),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning("EP meta insert failed: %s — %s", ep, exc)
        return count

    def _load_kapitel_meta(self, parsed: ParsedBudget) -> int:
        """Insert Kapitel metadata."""
        count = 0
        seen: set[str] = set()
        for kap in parsed.kapitel_meta:
            key = kap["kapitel"]
            if key in seen:
                continue
            seen.add(key)
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO kapitel_meta
                       (year, version, einzelplan, kapitel, kapitel_text,
                        source_pdf, source_page)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        parsed.year,
                        parsed.version,
                        kap["einzelplan"],
                        kap["kapitel"],
                        kap.get("kapitel_text"),
                        kap.get("source_pdf"),
                        kap.get("source_page"),
                    ),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning("Kapitel meta insert failed: %s — %s", kap, exc)
        return count

    def _load_haushaltsdaten(self, parsed: ParsedBudget) -> int:
        """Insert budget line items."""
        count = 0
        for entry in parsed.entries:
            try:
                self.db.execute(
                    """INSERT INTO haushaltsdaten
                       (year, version, einzelplan, kapitel, titel,
                        titel_text, titelgruppe,
                        ausgaben_soll, ausgaben_ist,
                        einnahmen_soll, einnahmen_ist,
                        is_verrechnungstitel, flexibilisiert,
                        deckungsfaehig, source_pdf, source_page, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?,
                               ?, ?, ?, ?,
                               ?, ?, ?, ?, ?, ?)""",
                    (
                        entry.year,
                        entry.version,
                        entry.einzelplan,
                        entry.kapitel,
                        entry.titel,
                        entry.titel_text,
                        entry.titelgruppe,
                        entry.ausgaben_soll,
                        entry.ausgaben_ist,
                        entry.einnahmen_soll,
                        entry.einnahmen_ist,
                        int(entry.is_verrechnungstitel),
                        int(entry.flexibilisiert),
                        int(entry.deckungsfaehig),
                        entry.source_pdf,
                        entry.source_page,
                        entry.notes,
                    ),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning(
                    "Haushaltsdaten insert failed: EP%s/%s — %s",
                    entry.einzelplan,
                    entry.titel,
                    exc,
                )
        return count

    def _load_personalhaushalt(self, parsed: ParsedBudget) -> int:
        """Insert personnel data."""
        count = 0
        for p in parsed.personnel:
            try:
                self.db.execute(
                    """INSERT INTO personalhaushalt
                       (year, version, einzelplan, kapitel,
                        titel, titelgruppe, besoldungsgruppe,
                        planstellen_gesamt, planstellen_tariflich,
                        planstellen_aussertariflich,
                        source_pdf, source_page)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        p.year,
                        p.version,
                        p.einzelplan,
                        p.kapitel,
                        p.titel,
                        p.titelgruppe,
                        p.besoldungsgruppe,
                        p.planstellen_gesamt,
                        p.planstellen_tariflich,
                        p.planstellen_aussertariflich,
                        p.source_pdf,
                        p.source_page,
                    ),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning("Personnel insert failed: %s — %s", p, exc)
        return count

    def _load_verpflichtungen(self, parsed: ParsedBudget) -> int:
        """Insert Verpflichtungsermächtigungen."""
        count = 0
        for ve in parsed.verpflichtungen:
            try:
                self.db.execute(
                    """INSERT INTO verpflichtungsermachtigungen
                       (year, version, einzelplan, kapitel, titel,
                        betrag_gesamt, faellig_jahr, faellig_betrag,
                        source_pdf, source_page, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ve.get("year", parsed.year),
                        ve.get("version", parsed.version),
                        ve.get("einzelplan", ""),
                        ve.get("kapitel", ""),
                        ve.get("titel"),
                        ve.get("betrag_gesamt"),
                        ve.get("faellig_jahr"),
                        ve.get("faellig_betrag"),
                        ve.get("source_pdf"),
                        ve.get("source_page"),
                        ve.get("notes"),
                    ),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning("VE insert failed: %s — %s", ve, exc)
        return count

    def _load_sachverhalte(self, parsed: ParsedBudget) -> int:
        """Insert Sachverhalte."""
        count = 0
        for sv in parsed.sachverhalte:
            try:
                self.db.execute(
                    """INSERT INTO sachverhalte
                       (year, version, einzelplan, kapitel, titel,
                        kategorie, betrag, source_pdf, source_page, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        sv.get("year", parsed.year),
                        sv.get("version", parsed.version),
                        sv.get("einzelplan", ""),
                        sv.get("kapitel", ""),
                        sv.get("titel"),
                        sv.get("kategorie"),
                        sv.get("betrag"),
                        sv.get("source_pdf"),
                        sv.get("source_page"),
                        sv.get("notes"),
                    ),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning("Sachverhalt insert failed: %s — %s", sv, exc)
        return count

    # ------------------------------------------------------------------
    # Page index (section detection)
    # ------------------------------------------------------------------

    def load_page_index(self, year: int, source_pdf: str, pages: list) -> int:
        """Load page metadata index.

        Parameters
        ----------
        pages:
            list of (page_number, section_type, einzelplan, kapitel, heading) tuples.

        Returns
        -------
        int
            Number of rows inserted.
        """
        count = 0
        for page_num, section_type, ep, kap, heading in pages:
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO page_index
                       (year, source_pdf, page_number, einzelplan, kapitel,
                        section_type, heading_text)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (year, source_pdf, page_num, ep, kap, section_type, heading),
                )
                count += 1
            except Exception as exc:
                logger.warning(
                    "page_index insert failed: %s page %d — %s",
                    source_pdf, page_num, exc,
                )
        self.db.commit()
        return count

    # ------------------------------------------------------------------
    # Hierarchical TOC (budget_toc)
    # ------------------------------------------------------------------

    def load_toc(self, year: int, source_pdf: str, entries: list[dict]) -> int:
        """Load hierarchical TOC entries into the budget_toc table.

        Parameters
        ----------
        year:
            Budget year.
        source_pdf:
            PDF filename.
        entries:
            List of dicts with keys: level, einzelplan, kapitel,
            section_type, label, page_start, page_end.

        Returns
        -------
        int
            Number of rows inserted.
        """
        count = 0
        for e in entries:
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO budget_toc
                       (year, source_pdf, level, einzelplan, kapitel,
                        section_type, label, page_start, page_end)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        year,
                        source_pdf,
                        e["level"],
                        e.get("einzelplan"),
                        e.get("kapitel"),
                        e.get("section_type"),
                        e.get("label", ""),
                        e["page_start"],
                        e["page_end"],
                    ),
                )
                count += 1
            except Exception as exc:
                logger.warning("TOC insert failed: %s", exc)
        self.db.commit()
        return count

    # ------------------------------------------------------------------
    # PDF Bookmarks (native or synthetic)
    # ------------------------------------------------------------------

    def load_bookmarks(self, year: int, source_pdf: str, entries: list[dict]) -> int:
        """Load bookmark entries into pdf_bookmarks table.

        Parameters
        ----------
        year:
            Budget year.
        source_pdf:
            PDF filename.
        entries:
            List of dicts with keys: level, title, page_number,
            einzelplan, kapitel, nav_type.

        Returns
        -------
        int
            Number of rows inserted.
        """
        count = 0
        for e in entries:
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO pdf_bookmarks
                       (year, source_pdf, level, title, page_number,
                        einzelplan, kapitel, nav_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        year,
                        source_pdf,
                        e["level"],
                        e["title"],
                        e["page_number"],
                        e.get("einzelplan"),
                        e.get("kapitel"),
                        e.get("nav_type", "other"),
                    ),
                )
                count += 1
            except Exception as exc:
                logger.warning("Bookmark insert failed: %s", exc)
        self.db.commit()
        return count

    # ------------------------------------------------------------------
    # Source document registration
    # ------------------------------------------------------------------

    def load_source_document(
        self, year: int, version: str, filename: str, filepath: str, page_count: int
    ) -> int:
        """Register a source document. Returns the row id."""
        self.db.execute(
            """INSERT OR REPLACE INTO source_documents
               (year, version, filename, filepath, page_count)
               VALUES (?, ?, ?, ?, ?)""",
            (year, version, filename, filepath, page_count),
        )
        self.db.commit()
        return self.db.execute("SELECT last_insert_rowid()").fetchone()[0]

    def load_page_text(
        self, year: int, source_pdf: str, pages: list
    ) -> int:
        """Load raw page text for fulltext search.

        Parameters
        ----------
        year:
            Budget year.
        source_pdf:
            PDF filename.
        pages:
            List of ``(page_number, text, einzelplan, kapitel)`` tuples.

        Returns
        -------
        int
            Number of rows inserted.
        """
        count = 0
        for page_num, text, ep, kap in pages:
            if not text or not text.strip():
                continue
            try:
                self.db.execute(
                    """INSERT OR REPLACE INTO page_text
                       (year, source_pdf, page_number, text, einzelplan, kapitel)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (year, source_pdf, page_num, text.strip(), ep, kap),
                )
                count += 1
            except sqlite3.Error as exc:
                logger.warning(
                    "page_text insert failed: page %d — %s", page_num, exc
                )
        self.db.commit()
        return count

    @staticmethod
    def search_fulltext(
        db_path, query: str, year: int = None, limit: int = 20
    ) -> list[dict]:
        """Search page text via FTS5.

        Returns matching pages with highlighted snippets.
        """
        from src.db.schema import get_connection

        conn = get_connection(db_path)
        try:
            sql = """
                SELECT pt.year, pt.source_pdf, pt.page_number,
                       pt.einzelplan, pt.kapitel,
                       snippet(page_text_fts, 0, '>>>', '<<<', '...', 40) AS snippet
                FROM page_text_fts
                JOIN page_text pt ON page_text_fts.rowid = pt.id
                WHERE page_text_fts MATCH ?
            """
            params: list = [query]
            if year:
                sql += " AND pt.year = ?"
                params.append(year)
            sql += " ORDER BY rank LIMIT ?"
            params.append(limit)

            rows = conn.execute(sql, params).fetchall()
            return [
                {
                    "year": r[0],
                    "source_pdf": r[1],
                    "page_number": r[2],
                    "einzelplan": r[3],
                    "kapitel": r[4],
                    "snippet": r[5],
                }
                for r in rows
            ]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# CLI entry point — full pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import time

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

    from src.db.schema import init_db, reset_db

    pdf_path = project_root / "docs" / "0350-25.pdf"
    db_path = project_root / "data" / "bundeshaushalt.db"

    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}")
        sys.exit(1)

    # Step 1: Extract (text-only for speed — parser uses text patterns)
    print(f"[1/4] Extracting PDF (text-only): {pdf_path.name} ...")
    t0 = time.time()
    from src.extract.budget_parser import BudgetParser, _extract_text_only_doc
    from src.extract.pdf_extractor import PDFExtractor

    extractor = PDFExtractor(pdf_path)
    doc = _extract_text_only_doc(extractor)
    t1 = time.time()
    print(f"       Done in {t1 - t0:.1f}s — {doc.total_pages} pages")

    # Step 2: Parse
    print("[2/4] Parsing budget structure ...")
    parser = BudgetParser(doc)
    budget = parser.parse()
    t2 = time.time()
    print(f"       Done in {t2 - t1:.1f}s")
    print(
        f"       {len(budget.entries)} entries, "
        f"{len(budget.personnel)} personnel, "
        f"{len(budget.einzelplan_meta)} Einzelpläne, "
        f"{len(budget.kapitel_meta)} Kapitel"
    )

    # Step 3: Reset and load into DB
    print(f"[3/4] Loading into {db_path.name} ...")
    reset_db(db_path)
    conn = init_db(db_path)
    loader = DataLoader(conn)
    stats = loader.load(budget)
    t3 = time.time()
    print(f"       Done in {t3 - t2:.1f}s — {stats}")

    # Step 4: Load reference data
    print("[4/4] Loading reference data ...")
    ref_count = loader.load_reference_data()
    t4 = time.time()
    print(f"       Done — {ref_count} reference rows")

    conn.close()

    print(f"\n=== Pipeline Complete ({t4 - t0:.1f}s total) ===")
    print(f"Database: {db_path}")
    for table, count in stats.items():
        print(f"  {table}: {count} rows")
    print(f"  referenzdaten: {ref_count} rows")
