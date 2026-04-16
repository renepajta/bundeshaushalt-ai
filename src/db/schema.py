"""SQLite schema for the German federal budget (Bundeshaushalt) Q&A system.

Defines tables for the hierarchical budget structure:
  Einzelplan (ministry-level plan) → Kapitel (chapter) → Titel (line item)

Data spans multiple fiscal years and budget versions (Entwurf, Beschluss, Nachtrag).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# DDL statements
# ---------------------------------------------------------------------------

_CREATE_TABLES = """
-- Einzelplan metadata (ministry-level plans, e.g. EP 14 = BMVg)
CREATE TABLE IF NOT EXISTS einzelplan_meta (
    year           INTEGER NOT NULL,
    version        TEXT    NOT NULL,  -- 'entwurf', 'beschluss', 'nachtrag', …
    einzelplan     TEXT    NOT NULL,  -- e.g. '01', '04', '06', '14'
    einzelplan_text TEXT,             -- Human-readable name of the ministry/plan
    source_pdf     TEXT,             -- Source PDF filename
    source_page    INTEGER,          -- 1-indexed page number in source PDF
    PRIMARY KEY (year, version, einzelplan)
);

-- Kapitel metadata (chapters within an Einzelplan)
CREATE TABLE IF NOT EXISTS kapitel_meta (
    year        INTEGER NOT NULL,
    version     TEXT    NOT NULL,
    einzelplan  TEXT    NOT NULL,
    kapitel     TEXT    NOT NULL,  -- e.g. '0111', '1201', '1403'
    kapitel_text TEXT,             -- Name / description of the chapter
    source_pdf  TEXT,             -- Source PDF filename
    source_page INTEGER,          -- 1-indexed page number in source PDF
    PRIMARY KEY (year, version, kapitel)
);

-- Main financial data – budget line items (Titel)
CREATE TABLE IF NOT EXISTS haushaltsdaten (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    year                        INTEGER NOT NULL,
    version                     TEXT    NOT NULL,
    version_detail              TEXT,             -- e.g. '2024.0.1'
    einzelplan                  TEXT    NOT NULL,
    kapitel                     TEXT    NOT NULL,
    titel                       TEXT,             -- e.g. '431 57', '531 01'
    titel_text                  TEXT,             -- Description of the budget line
    titelgruppe                 TEXT,             -- Title group (TGr)
    ausgaben_soll               REAL,             -- Planned expenditure (Soll)
    ausgaben_ist                REAL,             -- Actual expenditure (Ist)
    einnahmen_soll              REAL,             -- Planned revenue
    einnahmen_ist               REAL,             -- Actual revenue
    is_verrechnungstitel        INTEGER DEFAULT 0, -- Boolean: internal transfer title
    flexibilisiert              INTEGER DEFAULT 0, -- Boolean: flexibilised title
    deckungsfaehig              INTEGER DEFAULT 0, -- Boolean: can cover other titles
    gegenseitig_deckungsfaehig  INTEGER DEFAULT 0, -- Boolean: mutually coverable
    source_pdf                  TEXT,             -- Source PDF filename
    source_page                 INTEGER,          -- 1-indexed page number in source PDF
    notes                       TEXT
);

-- Personnel data (Planstellen / positions)
CREATE TABLE IF NOT EXISTS personalhaushalt (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    year                        INTEGER NOT NULL,
    version                     TEXT    NOT NULL,
    einzelplan                  TEXT    NOT NULL,
    kapitel                     TEXT    NOT NULL,
    titel                       TEXT,
    titelgruppe                 TEXT,
    besoldungsgruppe            TEXT,             -- e.g. 'ORR', 'A13', 'A16'
    planstellen_gesamt          INTEGER,          -- Total positions
    planstellen_tariflich       INTEGER,          -- Civil-service (tariff) positions
    planstellen_aussertariflich INTEGER,          -- Non-tariff positions
    source_pdf                  TEXT,             -- Source PDF filename
    source_page                 INTEGER,          -- 1-indexed page number in source PDF
    notes                       TEXT
);

-- Verpflichtungsermächtigungen (commitment authorisations & maturity schedule)
CREATE TABLE IF NOT EXISTS verpflichtungsermachtigungen (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    year           INTEGER NOT NULL,
    version        TEXT    NOT NULL,
    einzelplan     TEXT    NOT NULL,
    kapitel        TEXT    NOT NULL,
    titel          TEXT,
    betrag_gesamt  REAL,             -- Total authorisation amount
    faellig_jahr   INTEGER,          -- Year when the tranche is due
    faellig_betrag REAL,             -- Amount due in that year
    source_pdf     TEXT,             -- Source PDF filename
    source_page    INTEGER,          -- 1-indexed page number in source PDF
    notes          TEXT
);

-- Sachverhalte/ special items (Erstbeschaffung, Ersatzbeschaffung, …)
CREATE TABLE IF NOT EXISTS sachverhalte (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    year       INTEGER NOT NULL,
    version    TEXT    NOT NULL,
    einzelplan TEXT    NOT NULL,
    kapitel    TEXT    NOT NULL,
    titel      TEXT,
    kategorie  TEXT,              -- e.g. 'Erstbeschaffung', 'Ersatzbeschaffung', 'Mehreinnahmen'
    betrag     REAL,
    source_pdf  TEXT,             -- Source PDF filename
    source_page INTEGER,          -- 1-indexed page number in source PDF
    notes      TEXT
);

-- Source document tracking
CREATE TABLE IF NOT EXISTS source_documents (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    year         INTEGER NOT NULL,
    version      TEXT    NOT NULL,
    filename     TEXT    NOT NULL,
    filepath     TEXT    NOT NULL,
    page_count   INTEGER,
    ingested_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(year, version, filename)
);

-- Reference/ macroeconomic data
CREATE TABLE IF NOT EXISTS referenzdaten (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    year      INTEGER NOT NULL,
    indicator TEXT    NOT NULL,   -- e.g. 'BIP', 'Inflationsrate', 'Deflator'
    value     REAL    NOT NULL,
    notes     TEXT
);

-- Raw page text for fulltext search
CREATE TABLE IF NOT EXISTS page_text (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    year        INTEGER NOT NULL,
    source_pdf  TEXT NOT NULL,
    page_number INTEGER NOT NULL,  -- 1-indexed
    text        TEXT NOT NULL,
    einzelplan  TEXT,              -- detected from page context (may be NULL)
    kapitel     TEXT,              -- detected from page context (may be NULL)
    UNIQUE(year, source_pdf, page_number)
);

-- Page-level metadata for navigation (the clerk's Table of Contents)
CREATE TABLE IF NOT EXISTS page_index (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    year          INTEGER NOT NULL,
    source_pdf    TEXT NOT NULL,
    page_number   INTEGER NOT NULL,  -- 1-indexed
    einzelplan    TEXT,
    kapitel       TEXT,
    section_type  TEXT,   -- 'ueberblick', 'titel', 'personal', 've', 'vermerk', 'erlaeuterung', 'gesamtplan', 'hg', 'vorspann', 'other'
    heading_text  TEXT,   -- First meaningful heading on the page
    UNIQUE(year, source_pdf, page_number)
);

-- Hierarchical Table of Contents (structured 3-level navigation index)
CREATE TABLE IF NOT EXISTS budget_toc (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year            INTEGER NOT NULL,
    source_pdf      TEXT NOT NULL,
    level           TEXT NOT NULL,       -- 'ep', 'kapitel', 'section'
    einzelplan      TEXT,
    kapitel         TEXT,
    section_type    TEXT,                -- for level='section': ueberblick, titel, personal, ve, vermerk, erlaeuterung
    label           TEXT,                -- display text (EP name, Kapitel name, section heading)
    page_start      INTEGER NOT NULL,    -- 1-indexed, inclusive
    page_end        INTEGER NOT NULL,    -- 1-indexed, inclusive
    UNIQUE(year, source_pdf, level, einzelplan, kapitel, section_type)
);
"""

# FTS5 virtual table+ sync triggers (executed separately — not inside executescript
# because CREATE VIRTUAL TABLE can conflict with multi-statement scripts in some
# SQLite builds).
_CREATE_FTS = """
-- FTS5 fulltext index over page text
CREATE VIRTUAL TABLE IF NOT EXISTS page_text_fts USING fts5(
    text,
    content='page_text',
    content_rowid='id',
    tokenize='unicode61'
);

-- Triggers to keep FTS5 in sync
CREATE TRIGGER IF NOT EXISTS page_text_ai AFTER INSERT ON page_text BEGIN
    INSERT INTO page_text_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS page_text_ad AFTER DELETE ON page_text BEGIN
    INSERT INTO page_text_fts(page_text_fts, rowid, text) VALUES('delete', old.id, old.text);
END;
CREATE TRIGGER IF NOT EXISTS page_text_au AFTER UPDATE ON page_text BEGIN
    INSERT INTO page_text_fts(page_text_fts, rowid, text) VALUES('delete', old.id, old.text);
    INSERT INTO page_text_fts(rowid, text) VALUES (new.id, new.text);
END;
"""

# ---------------------------------------------------------------------------
# Indexes for common query patterns
# ---------------------------------------------------------------------------

_CREATE_INDEXES = """
-- haushaltsdaten: fast lookups by year+einzelplan, year+kapitel, year+kapitel+titel
CREATE INDEX IF NOT EXISTS idx_hd_year_ep
    ON haushaltsdaten (year, einzelplan);
CREATE INDEX IF NOT EXISTS idx_hd_year_kap
    ON haushaltsdaten (year, kapitel);
CREATE INDEX IF NOT EXISTS idx_hd_year_kap_titel
    ON haushaltsdaten (year, kapitel, titel);
CREATE INDEX IF NOT EXISTS idx_hd_year_version
    ON haushaltsdaten (year, version);

-- personalhaushalt
CREATE INDEX IF NOT EXISTS idx_ph_year_ep
    ON personalhaushalt (year, einzelplan);
CREATE INDEX IF NOT EXISTS idx_ph_year_kap
    ON personalhaushalt (year, kapitel);
CREATE INDEX IF NOT EXISTS idx_ph_year_besoldung
    ON personalhaushalt (year, besoldungsgruppe);

-- verpflichtungsermachtigungen
CREATE INDEX IF NOT EXISTS idx_ve_year_ep
    ON verpflichtungsermachtigungen (year, einzelplan);
CREATE INDEX IF NOT EXISTS idx_ve_year_kap
    ON verpflichtungsermachtigungen (year, kapitel);
CREATE INDEX IF NOT EXISTS idx_ve_faellig
    ON verpflichtungsermachtigungen (faellig_jahr);

-- sachverhalte
CREATE INDEX IF NOT EXISTS idx_sv_year_ep
    ON sachverhalte (year, einzelplan);
CREATE INDEX IF NOT EXISTS idx_sv_year_kategorie
    ON sachverhalte (year, kategorie);

-- referenzdaten
CREATE INDEX IF NOT EXISTS idx_ref_year_ind
    ON referenzdaten (year, indicator);

-- Source provenance indexes
CREATE INDEX IF NOT EXISTS idx_hd_source
    ON haushaltsdaten (source_pdf, source_page);
CREATE INDEX IF NOT EXISTS idx_ph_source
    ON personalhaushalt (source_pdf, source_page);

-- page_text indexes
CREATE INDEX IF NOT EXISTS idx_pt_year
    ON page_text (year);
CREATE INDEX IF NOT EXISTS idx_pt_source
    ON page_text (source_pdf, page_number);

-- page_index indexes
CREATE INDEX IF NOT EXISTS idx_pi_year_ep
    ON page_index (year, einzelplan);
CREATE INDEX IF NOT EXISTS idx_pi_year_kap
    ON page_index (year, kapitel);
CREATE INDEX IF NOT EXISTS idx_pi_section
    ON page_index (year, section_type);

-- budget_toc indexes
CREATE INDEX IF NOT EXISTS idx_toc_year_ep
    ON budget_toc (year, einzelplan);
CREATE INDEX IF NOT EXISTS idx_toc_year_kap
    ON budget_toc (year, kapitel);
CREATE INDEX IF NOT EXISTS idx_toc_level
    ON budget_toc (year, level);
"""

# Ordered list of all application tables (used by reset_db).
# page_text_fts listed before page_text because triggers reference the FTS table.
_ALL_TABLES = [
    "page_text_fts",
    "einzelplan_meta",
    "kapitel_meta",
    "haushaltsdaten",
    "personalhaushalt",
    "verpflichtungsermachtigungen",
    "sachverhalte",
    "referenzdaten",
    "source_documents",
    "page_text",
    "page_index",
    "budget_toc",
]

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def _configure_connection(conn: sqlite3.Connection) -> None:
    """Apply common PRAGMA settings to a connection."""
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.row_factory = sqlite3.Row


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create tables and indexes if they don't exist and return a connection.

    * Enables WAL journal mode for concurrent reads.
    * Enables foreign-key enforcement.
    * Sets a 5-second busy timeout.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Created automatically if missing.

    Returns
    -------
    sqlite3.Connection
        Ready-to-use connection with ``row_factory = sqlite3.Row``.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    _configure_connection(conn)

    conn.executescript(_CREATE_TABLES)
    conn.executescript(_CREATE_FTS)
    conn.executescript(_CREATE_INDEXES)
    return conn


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open a connection to an existing database.

    Applies the same PRAGMA settings as :func:`init_db` but does **not**
    create or alter any tables.

    Parameters
    ----------
    db_path:
        Path to an existing SQLite database file.

    Returns
    -------
    sqlite3.Connection

    Raises
    ------
    FileNotFoundError
        If *db_path* does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    _configure_connection(conn)
    return conn


def reset_db(db_path: Path) -> None:
    """Drop every application table and recreate the schema from scratch.

    .. warning:: This permanently deletes **all** data in the database.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        init_db(db_path).close()
        return

    conn = sqlite3.connect(str(db_path))
    _configure_connection(conn)

    # Drop FTS virtual table first (may not exist; swallow errors)
    try:
        conn.execute("DROP TABLE IF EXISTS page_text_fts;")
    except sqlite3.Error:
        pass
    # Drop triggers that reference page_text_fts
    for trg in ("page_text_ai", "page_text_ad", "page_text_au"):
        try:
            conn.execute(f"DROP TRIGGER IF EXISTS {trg};")
        except sqlite3.Error:
            pass

    for table in _ALL_TABLES:
        if table == "page_text_fts":
            continue  # already handled above
        conn.execute(f"DROP TABLE IF EXISTS {table};")
    conn.commit()

    conn.executescript(_CREATE_TABLES)
    conn.executescript(_CREATE_FTS)
    conn.executescript(_CREATE_INDEXES)
    conn.close()
