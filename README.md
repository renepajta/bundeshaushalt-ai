# Bundeshaushalt AI

**An intelligent document navigation system for the German federal budget — built to answer complex fiscal questions across 22 years of budget documents with exact page-level citations.**

---

## Business Outcome

Government agencies, oversight bodies, and fiscal analysts spend significant time navigating thousands of pages of budget documentation to answer precise financial questions. A single question — *"How did defence spending change from 2021 to 2022?"* — requires opening multiple 3,000-page PDFs, locating the right summary tables, extracting numbers, and computing the result.

**Bundeshaushalt AI reduces this from hours to seconds.** The system reads the original budget documents the way an experienced clerk would — navigating via the document's own table of contents, reading the exact page, and citing the source — delivering verifiable, grounded answers with full traceability.

### What It Enables

- **Instant cross-year analysis** — Compare spending across any of the 22 federal budget years (2005–2026) with automatic document navigation
- **Page-level grounding** — Every answer links to the exact PDF page and section, enabling teams to verify and audit
- **Natural language access** — Ask questions in German, receive structured answers with computation breakdowns and source citations
- **Conversation continuity** — Follow-up questions retain context, enabling iterative deep-dives into budget structures

---

## Conceptual Design

The system mirrors the mental model of a tenured government budget clerk — someone who doesn't memorize 3,000 pages but knows *exactly how to navigate them*.

### The Clerk's Toolkit

```
User Question
      │
      ▼
┌─────────────────────────────────────────┐
│  AI Agent (GPT-4o)                       │
│  Thinks like an experienced clerk:       │
│                                          │
│  📖 read_document                        │
│     Navigate via PDF bookmarks (TOC)     │
│     Search by term (fulltext)            │
│     Read specific pages (GPT-4o vision)  │
│                                          │
│  🧮 compute                              │
│     Exact arithmetic on extracted values │
│                                          │
│  → Answer with citations                 │
│    📄 BHH 2021, S.547 | EP 06           │
└─────────────────────────────────────────┘
```

### Design Principles

**Document-first, not database-first.** Rather than extracting data into a separate database and querying an approximation, the system reads the *original document pages* — the same source of truth a human clerk would use. This eliminates extraction errors and scope mismatches.

**Built-in navigation over brute-force search.** German federal budget PDFs contain rich hierarchical bookmarks (11,000+ per document) mapping every Einzelplan, Kapitel, and Titel to exact pages. The system leverages these native bookmarks — just as a clerk uses the table of contents — to navigate in milliseconds rather than scanning hundreds of pages.

**Semantic awareness across eras.** Budget terminology and organizational structures evolve over 22 years. The system maintains a semantic bridge that maps historical terms to modern equivalents (*"Wehrübende" → "Reservedienstleistende"*), tracks institutional migrations across budget chapters, and resolves ministry name changes — enabling cross-era questions that would otherwise require deep domain expertise.

**Parallel execution for comparison questions.** When comparing two budget years, the system reads both documents simultaneously using parallel tool execution — halving response time for the most common analytical question pattern.

---

## Technical Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **AI Reasoning** | Azure OpenAI GPT-4o | ReAct agent loop with function calling — plans navigation, reads pages, synthesizes answers |
| **Document Vision** | GPT-4o multimodal | Reads budget tables from rendered PDF page images — handles complex table layouts that text extraction misses |
| **PDF Processing** | PyMuPDF (fitz) | Text extraction, page rendering, bookmark extraction — with parallel `ThreadPoolExecutor` |
| **Navigation Index** | SQLite + FTS5 | 184,945 PDF bookmarks for instant page lookup; fulltext search across 64,932 pages with synonym expansion |
| **Semantic Layer** | Custom SemanticBridge | Terminology aliases, organizational migrations, ministry name history, abbreviation resolution |
| **Computation** | Safe Python eval | AST-whitelisted arithmetic for exact percentage, difference, and ratio calculations |
| **Data Acquisition** | Playwright | Automated download of 212 budget PDFs (2005–2026) from the federal budget portal, including ZIP extraction |
| **CLI** | Rich + argparse | Interactive chat with conversation history, batch ingestion, PDF download management |

### Architecture Highlights

- **3-level bookmark hierarchy**: Einzelplan → Kapitel → Titel, each with exact page numbers
- **Synonym-aware search**: FTS5 queries automatically expand with historical term variants and organizational mappings
- **PDF handle caching**: Avoids reopening 3,000-page files on repeated access
- **Multi-era parser system**: Handles three distinct PDF formats (2005–2011 per-Einzelplan files, 2012–2023 consolidated, 2024–2026 modern) through auto-detecting router
- **Zero-cost TOC extraction**: Native PDF bookmarks extracted via `doc.get_toc()` in milliseconds; synthetic bookmarks generated from page headings for older documents lacking metadata

---

## Getting Started

### Prerequisites

- Python 3.11+
- Azure OpenAI deployment (GPT-4o) with API key
- Playwright (for PDF download from the federal budget portal)

### Setup

```bash
# Clone and install
git clone https://github.com/renepajta/bundeshaushalt-ai.git
cd bundeshaushalt-ai
pip install -r requirements.txt
playwright install chromium

# Configure Azure OpenAI
cp .env.example .env
# Edit .env with your Azure OpenAI endpoint and API key

# Download budget PDFs (2005–2026)
python -m src.cli download

# Ingest all documents (builds bookmark index + fulltext search)
python -m src.cli ingest-all --reset

# Start interactive chat
python -m src.cli interactive
```

### Example Interaction

```
[0] Frage: Wie hoch sind die Gesamtausgaben des Einzelplans 06 im Jahr 2021?

Antwort: Die Gesamtausgaben des Einzelplans 06 im Jahr 2021 betragen
18.457.714.000 € (Quelle: Bundeshaushalt 2021, S. 547).

📄 BHH 2021 gesamt.pdf, S. 547 | EP 06

[1] Frage: Und wie haben sie sich 2022 verändert?

Antwort: Die Ausgaben im EP 06 sind von 18.457.714 T€ (2021) auf
14.986.394 T€ (2022) gesunken — eine Veränderung von -18,81%.

📄 BHH 2021 gesamt.pdf, S. 547
📄 BHH 2022 gesamt.pdf, S. 559
```

---

## Project Structure

```
bundeshaushalt-ai/
├── src/
│   ├── cli.py                    # CLI: interactive, query, ingest-all, download
│   ├── config.py                 # Azure OpenAI + path configuration
│   ├── db/                       # SQLite schema, data loader
│   ├── extract/                  # PDF parsing, bookmark extraction, semantic bridge
│   │   ├── bookmark_extractor.py # Native + synthetic bookmark extraction
│   │   ├── parser_router.py      # Auto-detects PDF era and routes to correct parser
│   │   ├── semantic_bridge.py    # Cross-era terminology and org mapping
│   │   └── toc_builder.py        # Hierarchical TOC construction
│   └── query/                    # AI agent, page scanner, citations
│       ├── engine.py             # ReAct agent with parallel tool execution
│       ├── page_scanner.py       # GPT-4o multimodal page reading
│       ├── document_locator.py   # Page-level document navigation
│       └── citations.py          # Structured source references
├── scripts/
│   └── download_budgets.py       # Playwright-based PDF downloader
├── tests/
│   ├── golden_qa.json            # 16 expert-validated test questions
│   └── run_golden_qa.py          # Automated evaluation with fuzzy scoring
├── docs/
│   └── ARCHITECTURE.md           # Detailed technical architecture
└── wiki/                         # Generated knowledge pages per Einzelplan
```

---

## Evaluation

The system is evaluated against 16 expert-validated questions spanning year comparisons, historical tracking, personnel lookups, commitment authorizations, and cross-reference analysis.

| Category | Questions | Capability |
|----------|-----------|------------|
| Cross-year spending comparison | q01, q02 | Reads Überblick pages from multiple years, computes differences |
| Ratio and inflation calculations | q03, q04 | Combines budget data with macroeconomic reference values |
| Historical entity tracking | q06, q07 | Traces organizational migrations across 22 years via SemanticBridge |
| Personnel and staffing | q05, q13, q15 | Navigates Personalhaushalt sections by Kapitel and grade |
| Commitment authorizations (VE) | q12 | Finds VE sections with maturity schedules |
| Cross-reference analysis | q09, q11 | Identifies Titel appearances across Einzelpläne |
| Document version comparison | q08, q10, q16 | Requires draft (Entwurf) documents — a known data gap |

---

## License

This project uses publicly available German federal budget documents. The budget data is published without copyright restrictions by the Federal Ministry of Finance and may be freely used, reproduced, and distributed.

---

*Built with Azure OpenAI, PyMuPDF, and the conviction that government transparency starts with making budget documents actually navigable.*
