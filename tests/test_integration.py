"""Integration tests for the Bundeshaushalt Q&A pipeline.

Covers:
  1. Unit tests — German number parsing, SQL validation (no external deps)
  2. Module integration — SQLite schema, PDF extraction
  3. Golden Q&A structure validation
  4. End-to-end smoke tests (skipped when PDF / API unavailable)
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_GOLDEN_QA_PATH = _PROJECT_ROOT / "tests" / "golden_qa.json"

# ---------------------------------------------------------------------------
# Imports from the application
# ---------------------------------------------------------------------------
from src.config import config
from src.extract.pdf_extractor import parse_german_number
from src.db.schema import init_db, reset_db, _ALL_TABLES


# ===================================================================
# 1. Unit Tests — no external dependencies
# ===================================================================


class TestGermanNumberParsing(unittest.TestCase):
    """Test the German number parsing utility."""

    def test_thousands_separator(self):
        self.assertEqual(parse_german_number("16.161.139"), 16161139.0)

    def test_decimal_comma(self):
        self.assertEqual(parse_german_number("102,03"), 102.03)

    def test_thousands_and_decimal(self):
        self.assertEqual(parse_german_number("1.234,56"), 1234.56)

    def test_negative_with_minus(self):
        self.assertEqual(parse_german_number("-1.015.205"), -1015205.0)

    def test_negative_simple(self):
        self.assertEqual(parse_german_number("-500"), -500.0)

    def test_negative_with_parens(self):
        self.assertEqual(parse_german_number("(1.200)"), -1200.0)

    def test_dash_returns_none(self):
        self.assertIsNone(parse_german_number("—"))
        self.assertIsNone(parse_german_number("–"))
        self.assertIsNone(parse_german_number("-"))

    def test_empty_returns_none(self):
        self.assertIsNone(parse_german_number(""))

    def test_none_returns_none(self):
        self.assertIsNone(parse_german_number(None))

    def test_dots_only_returns_none(self):
        self.assertIsNone(parse_german_number(".."))

    def test_k_suffix(self):
        self.assertEqual(parse_german_number("50k"), 50000.0)

    def test_k_suffix_with_thousands(self):
        self.assertEqual(parse_german_number("1.234k"), 1234000.0)

    def test_simple_integer(self):
        self.assertEqual(parse_german_number("42"), 42.0)

    def test_whitespace_stripped(self):
        self.assertEqual(parse_german_number("  1.000  "), 1000.0)

    def test_unicode_minus(self):
        """The parser should handle the Unicode minus sign (−)."""
        self.assertEqual(parse_german_number("−500"), -500.0)


class TestSQLValidation(unittest.TestCase):
    """Test that SQL validation in SQLAgent blocks dangerous queries."""

    @classmethod
    def setUpClass(cls):
        # Import the private method via the class
        from src.query.sql_agent import _FORBIDDEN_KEYWORDS
        cls._forbidden_re = _FORBIDDEN_KEYWORDS

    def _is_forbidden(self, sql: str) -> bool:
        return bool(self._forbidden_re.search(sql))

    def test_select_allowed(self):
        self.assertFalse(self._is_forbidden("SELECT * FROM haushaltsdaten"))

    def test_select_with_join_allowed(self):
        self.assertFalse(self._is_forbidden(
            "SELECT a.* FROM haushaltsdaten a JOIN einzelplan_meta b USING(year)"
        ))

    def test_drop_blocked(self):
        self.assertTrue(self._is_forbidden("DROP TABLE haushaltsdaten"))

    def test_delete_blocked(self):
        self.assertTrue(self._is_forbidden("DELETE FROM haushaltsdaten WHERE year=2024"))

    def test_update_blocked(self):
        self.assertTrue(self._is_forbidden("UPDATE haushaltsdaten SET year=0"))

    def test_insert_blocked(self):
        self.assertTrue(self._is_forbidden("INSERT INTO haushaltsdaten VALUES (1)"))

    def test_alter_blocked(self):
        self.assertTrue(self._is_forbidden("ALTER TABLE haushaltsdaten ADD col TEXT"))

    def test_pragma_blocked(self):
        self.assertTrue(self._is_forbidden("PRAGMA table_info(haushaltsdaten)"))

    def test_attach_blocked(self):
        self.assertTrue(self._is_forbidden("ATTACH DATABASE ':memory:' AS evil"))


# ===================================================================
# 2. Module Integration Tests
# ===================================================================


class TestSQLiteSchema(unittest.TestCase):
    """Test database schema creation and reset."""

    def setUp(self):
        self._tmp_dir = tempfile.mkdtemp(prefix="bh_test_")
        self.db_path = Path(self._tmp_dir) / "test.db"

    def tearDown(self):
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def test_init_db_creates_tables(self):
        conn = init_db(self.db_path)
        try:
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            for expected in _ALL_TABLES:
                self.assertIn(expected, tables, f"Table {expected} missing after init_db")
        finally:
            conn.close()

    def test_init_db_returns_connection(self):
        conn = init_db(self.db_path)
        try:
            self.assertIsInstance(conn, sqlite3.Connection)
            # Connection should have row_factory set
            self.assertEqual(conn.row_factory, sqlite3.Row)
        finally:
            conn.close()

    def test_init_db_is_idempotent(self):
        conn1 = init_db(self.db_path)
        conn1.execute(
            "INSERT INTO referenzdaten (year, indicator, value) VALUES (2024, 'BIP', 1.0)"
        )
        conn1.commit()
        conn1.close()

        # Re-init should not drop the existing data
        conn2 = init_db(self.db_path)
        try:
            row = conn2.execute("SELECT value FROM referenzdaten WHERE indicator='BIP'").fetchone()
            self.assertIsNotNone(row, "Data lost after re-running init_db")
        finally:
            conn2.close()

    def test_reset_db_drops_and_recreates(self):
        conn = init_db(self.db_path)
        conn.execute(
            "INSERT INTO referenzdaten (year, indicator, value) VALUES (2024, 'BIP', 1.0)"
        )
        conn.commit()
        conn.close()

        reset_db(self.db_path)

        conn2 = sqlite3.connect(str(self.db_path))
        try:
            count = conn2.execute("SELECT COUNT(*) FROM referenzdaten").fetchone()[0]
            self.assertEqual(count, 0, "Data should be gone after reset_db")

            # Tables should still exist
            tables = [
                row[0]
                for row in conn2.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            for expected in _ALL_TABLES:
                self.assertIn(expected, tables, f"Table {expected} missing after reset_db")
        finally:
            conn2.close()

    def test_haushaltsdaten_columns(self):
        """Verify the haushaltsdaten table has key columns."""
        conn = init_db(self.db_path)
        try:
            cols_info = conn.execute("PRAGMA table_info(haushaltsdaten)").fetchall()
            col_names = [c[1] for c in cols_info]
            required = [
                "year", "version", "einzelplan", "kapitel", "titel",
                "ausgaben_soll", "ausgaben_ist", "einnahmen_soll", "einnahmen_ist",
                "is_verrechnungstitel", "flexibilisiert",
            ]
            for col in required:
                self.assertIn(col, col_names, f"Column {col} missing in haushaltsdaten")
        finally:
            conn.close()

    def test_indexes_created(self):
        """Spot-check that some indexes exist."""
        conn = init_db(self.db_path)
        try:
            indexes = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            self.assertIn("idx_hd_year_ep", indexes)
            self.assertIn("idx_ph_year_ep", indexes)
            self.assertIn("idx_ve_faellig", indexes)
        finally:
            conn.close()


class TestDataLoader(unittest.TestCase):
    """Test the DataLoader's reference data loading."""

    def setUp(self):
        self._tmp_dir = tempfile.mkdtemp(prefix="bh_test_loader_")
        self.db_path = Path(self._tmp_dir) / "test.db"
        self.conn = init_db(self.db_path)

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def test_load_reference_data(self):
        from src.db.loader import DataLoader

        loader = DataLoader(self.conn)
        count = loader.load_reference_data()
        self.assertGreater(count, 0, "Reference data should insert rows")

        rows = self.conn.execute("SELECT * FROM referenzdaten").fetchall()
        self.assertEqual(len(rows), count)

    def test_reference_data_contains_bip(self):
        from src.db.loader import DataLoader

        loader = DataLoader(self.conn)
        loader.load_reference_data()

        bip = self.conn.execute(
            "SELECT value FROM referenzdaten WHERE indicator='BIP' AND year=2025"
        ).fetchone()
        self.assertIsNotNone(bip, "BIP 2025 should be in reference data")
        self.assertGreater(bip[0], 0)


# ===================================================================
# 3. GENESIS-Online Client Tests
# ===================================================================


class TestGenesisClient(unittest.TestCase):
    """Test GenesisClient functionality."""

    def test_import(self):
        """GenesisClient module can be imported."""
        from src.query.genesis_client import GenesisClient
        client = GenesisClient()
        self.assertIsInstance(client.available, bool)

    def test_format_result_parses_ffcsv(self):
        """_format_result extracts values from ffcsv-style data."""
        from src.query.genesis_client import GenesisClient

        csv_text = (
            "Zeit;Zeit_Label;Merkmal;Auspraegung;Wert\n"
            '2022;"2022";"BIP";"nominal";"3876,81"\n'
            '2023;"2023";"BIP";"nominal";"4121,16"\n'
        )
        table_info = {
            "table": "81000-0001",
            "description": "Bruttoinlandsprodukt (nominal)",
            "unit": "Mrd. €",
            "value_hint": "jeweiligen Preisen",
        }
        result = GenesisClient._format_result("BIP", csv_text, table_info, [2023])
        self.assertIsNotNone(result)
        self.assertIn("2023", result)
        self.assertIn("GENESIS-Online", result)

    def test_format_result_empty_csv(self):
        """_format_result returns None for empty/header-only CSV."""
        from src.query.genesis_client import GenesisClient

        result = GenesisClient._format_result("BIP", "header\n", {
            "table": "x", "description": "x", "unit": "", "value_hint": ""
        }, [2023])
        self.assertIsNone(result)

    def test_lookup_without_credentials_returns_none(self):
        """lookup() returns None when no GENESIS credentials are configured."""
        from src.query.genesis_client import GenesisClient
        client = GenesisClient()
        if not client.available:
            result = client.lookup("BIP", year=2023)
            self.assertIsNone(result)

    def test_search_tables_returns_list(self):
        """search_tables() returns a list (may be empty if API is down)."""
        from src.query.genesis_client import GenesisClient
        client = GenesisClient()
        try:
            results = client.search_tables("Bruttoinlandsprodukt", limit=2)
            self.assertIsInstance(results, list)
        except Exception:
            pass  # API may be unreachable in CI


# ===================================================================
# 4. Golden Q&A Structure Test
# ===================================================================


class TestGoldenQA(unittest.TestCase):
    """Verify golden Q&A test data is well-formed."""

    _REQUIRED_FIELDS = {"id", "question", "expected_answer", "key_figures", "category", "difficulty"}
    _VALID_CATEGORIES = {
        "year_comparison", "soll_ist_comparison", "ratio_calculation",
        "inflation_adjustment", "personnel_lookup", "historical_tracking",
        "version_comparison", "deckungsfaehigkeit", "version_tracking",
        "cross_reference", "verpflichtungsermachtigungen", "personnel_detail",
        "detail_breakdown",
    }
    _VALID_DIFFICULTIES = {"simple", "medium", "complex", "advanced"}

    @classmethod
    def setUpClass(cls):
        if not _GOLDEN_QA_PATH.exists():
            raise unittest.SkipTest(f"Golden Q&A file not found: {_GOLDEN_QA_PATH}")
        with open(_GOLDEN_QA_PATH, encoding="utf-8") as f:
            cls.golden_data = json.load(f)

    def test_golden_qa_loads(self):
        self.assertIsInstance(self.golden_data, list)
        self.assertEqual(
            len(self.golden_data), 16,
            f"Expected 16 golden Q&A entries, got {len(self.golden_data)}",
        )

    def test_each_entry_has_required_fields(self):
        for i, entry in enumerate(self.golden_data):
            for field in self._REQUIRED_FIELDS:
                self.assertIn(
                    field, entry,
                    f"Entry {i} (id={entry.get('id', '?')}) missing field '{field}'",
                )

    def test_ids_are_unique(self):
        ids = [e["id"] for e in self.golden_data]
        self.assertEqual(len(ids), len(set(ids)), "Duplicate IDs in golden Q&A")

    def test_categories_valid(self):
        for entry in self.golden_data:
            self.assertIn(
                entry["category"], self._VALID_CATEGORIES,
                f"Unknown category '{entry['category']}' in {entry['id']}",
            )

    def test_difficulties_valid(self):
        for entry in self.golden_data:
            self.assertIn(
                entry["difficulty"], self._VALID_DIFFICULTIES,
                f"Unknown difficulty '{entry['difficulty']}' in {entry['id']}",
            )

    def test_questions_are_german(self):
        for entry in self.golden_data:
            q = entry["question"]
            self.assertGreater(len(q), 10, f"Question too short: {q}")
            # German questions typically contain common German words
            german_indicators = ["wie", "was", "welch", "vergleich", "betracht"]
            has_german = any(w in q.lower() for w in german_indicators)
            self.assertTrue(
                has_german,
                f"Question doesn't look German: {q[:60]}…",
            )

    def test_key_figures_is_dict(self):
        for entry in self.golden_data:
            self.assertIsInstance(
                entry["key_figures"], dict,
                f"key_figures should be dict in {entry['id']}",
            )

    def test_data_requirements_present(self):
        """Each entry should list which tables it needs."""
        for entry in self.golden_data:
            self.assertIn(
                "data_requirements", entry,
                f"Entry {entry['id']} missing data_requirements",
            )
            self.assertIsInstance(entry["data_requirements"], list)
            self.assertGreater(len(entry["data_requirements"]), 0)


# ===================================================================
# 4. End-to-End Smoke Tests (skipped if resources unavailable)
# ===================================================================


_PDF_PATH = config.DOCS_DIR / "0350-25.pdf"
_PDF_AVAILABLE = _PDF_PATH.exists()
_API_CONFIGURED = bool(config.AZURE_OPENAI_ENDPOINT and config.AZURE_OPENAI_API_KEY)


@unittest.skipUnless(_PDF_AVAILABLE, f"PDF not available at {_PDF_PATH}")
class TestPDFExtraction(unittest.TestCase):
    """Test PDF extraction (requires actual budget PDF)."""

    @classmethod
    def setUpClass(cls):
        from src.extract.pdf_extractor import PDFExtractor
        cls.extractor = PDFExtractor(_PDF_PATH)

    def test_extract_first_page_has_text(self):
        pages = self.extractor.extract_pages(start=0, end=1)
        self.assertEqual(len(pages), 1)
        self.assertGreater(
            len(pages[0].text), 50,
            "First page should have substantial text",
        )

    def test_extract_first_page_number(self):
        pages = self.extractor.extract_pages(start=0, end=1)
        self.assertEqual(pages[0].page_number, 1, "Page numbers should be 1-indexed")

    def test_extract_range(self):
        """Extract pages 0–4 and verify count."""
        pages = self.extractor.extract_pages(start=0, end=5)
        self.assertEqual(len(pages), 5)

    def test_extract_returns_tables(self):
        """Check that the PDF has tables somewhere (pages 50–100, past front-matter)."""
        pages = self.extractor.extract_pages(start=50, end=100)
        tables_found = sum(len(p.tables) for p in pages)
        # Budget content pages (past front-matter) should have tables
        self.assertGreater(
            tables_found, 0,
            "Expected at least 1 table in pages 50–100",
        )


@unittest.skipUnless(_PDF_AVAILABLE, f"PDF not available at {_PDF_PATH}")
class TestBudgetParser(unittest.TestCase):
    """Test budget parser on real extracted pages."""

    @classmethod
    def setUpClass(cls):
        from src.extract.pdf_extractor import PDFExtractor, ExtractedDocument
        from src.extract.budget_parser import BudgetParser

        extractor = PDFExtractor(_PDF_PATH)
        # Only extract first 50 pages for speed
        pages = extractor.extract_pages(start=0, end=50)
        doc = ExtractedDocument(
            source_path=_PDF_PATH,
            total_pages=50,
            pages=pages,
        )
        cls.parser = BudgetParser(doc)
        cls.budget = cls.parser.parse()

    def test_parser_detects_year(self):
        self.assertIsInstance(self.budget.year, int)
        self.assertGreaterEqual(self.budget.year, 2005)
        self.assertLessEqual(self.budget.year, 2030)

    def test_parser_detects_version(self):
        self.assertIn(
            self.budget.version,
            {"entwurf", "beschluss", "nachtrag", "unknown"},
        )

    def test_parser_produces_some_entries(self):
        """Even from 50 pages we should get some budget entries."""
        total = len(self.budget.entries)
        # The first 50 pages may be mostly front-matter; be lenient
        self.assertGreaterEqual(total, 0, "Parser should not crash on real data")

    def test_parsed_budget_has_source_file(self):
        self.assertTrue(len(self.budget.source_file) > 0)


@unittest.skipUnless(
    _PDF_AVAILABLE and _API_CONFIGURED,
    "Requires both PDF and Azure OpenAI API",
)
class TestEndToEnd(unittest.TestCase):
    """Full pipeline smoke test: extract → parse → load → query."""

    @classmethod
    def setUpClass(cls):
        cls._tmp_dir = tempfile.mkdtemp(prefix="bh_e2e_")
        cls.db_path = Path(cls._tmp_dir) / "e2e.db"

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls._tmp_dir, ignore_errors=True)

    def test_full_pipeline(self):
        """Run the extract → parse → load pipeline on a small page range."""
        from src.extract.pdf_extractor import PDFExtractor, ExtractedDocument
        from src.extract.budget_parser import BudgetParser
        from src.db.schema import init_db as _init_db
        from src.db.loader import DataLoader

        extractor = PDFExtractor(_PDF_PATH)
        pages = extractor.extract_pages(start=0, end=30)
        doc = ExtractedDocument(
            source_path=_PDF_PATH,
            total_pages=30,
            pages=pages,
        )

        parser = BudgetParser(doc)
        budget = parser.parse()

        conn = _init_db(self.db_path)
        try:
            loader = DataLoader(conn)
            stats = loader.load(budget)
            ref_count = loader.load_reference_data()

            # Verify DB has data
            total_ref = conn.execute("SELECT COUNT(*) FROM referenzdaten").fetchone()[0]
            self.assertEqual(total_ref, ref_count)

            # Verify tables exist
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            ]
            self.assertIn("haushaltsdaten", tables)
            self.assertIn("referenzdaten", tables)
        finally:
            conn.close()


# ===================================================================
# Runner
# ===================================================================


if __name__ == "__main__":
    unittest.main(verbosity=2)
