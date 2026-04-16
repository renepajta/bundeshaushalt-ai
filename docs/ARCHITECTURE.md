# Bundeshaushalt Q&A — Architecture Documentation

> A Retrieval-Augmented Generation (RAG) system for querying the German federal
> budget (*Bundeshaushalt*) using natural language. Built with Azure OpenAI,
> SQLite, and a custom knowledge wiki.

---

## 1  Overview

The **Bundeshaushalt Q&A** application enables users to ask natural-language
questions (in German) about the German federal budget and receive precise,
sourced answers. It combines three data access strategies:

| Strategy | Purpose | Example Question |
|----------|---------|-----------------|
| **SQL Database** | Exact financial figures, aggregations, comparisons | *"Wie hoch sind die Ausgaben des Einzelplans 14 im Jahr 2026?"* |
| **Knowledge Wiki** | Conceptual explanations, terminology, background | *"Was ist ein Verrechnungstitel?"* |
| **PDF Page Scanner** | Visual table analysis directly from source PDFs | *"Welche Tabellen stehen auf Seite 42?"* |

A **ReAct agent** (Reasoning + Acting) powered by **GPT-4o** via Azure OpenAI
orchestrates these tools autonomously — deciding which to call, in what order,
and how many times — before synthesising a final German-language answer.

---

## 2  Project Structure

```
itzbund-bundestag/
├── src/
│   ├── cli.py                  # CLI entry point — all user commands
│   ├── config.py               # Configuration (env vars, paths)
│   ├── __main__.py             # python -m src entrypoint
│   │
│   ├── extract/                # PDF extraction layer
│   │   ├── pdf_extractor.py    # Text + table extraction (PyMuPDF / pdfplumber)
│   │   └── budget_parser.py    # Structure recognition → ParsedBudget
│   │
│   ├── db/                     # Database layer
│   │   ├── schema.py           # DDL, indexes, init_db / reset_db
│   │   └── loader.py           # DataLoader — inserts ParsedBudget into SQLite
│   │
│   ├── query/                  # Query engine (THE CORE)
│   │   ├── engine.py           # ReAct agent loop with OpenAI function calling
│   │   ├── llm.py              # Azure OpenAI client (chat, SQL gen, synthesis)
│   │   ├── sql_agent.py        # NL→SQL translation + execution + retry
│   │   └── page_scanner.py     # Multimodal PDF page analysis (text + images)
│   │
│   └── wiki/                   # Knowledge wiki layer
│       ├── builder.py          # Generates Markdown wiki pages from DB
│       ├── indexer.py          # Maintains index.md and log.md
│       └── search.py           # BM25-style keyword search with German NLP
│
├── scripts/
│   └── download_budgets.py     # Playwright-based PDF scraper
│
├── data/                       # Runtime data (DB, downloaded PDFs)
├── docs/                       # Source budget PDFs
├── wiki/                       # Generated knowledge wiki (Markdown)
├── tests/                      # Test suite
├── SCHEMA.md                   # Wiki structure definition (in German)
└── requirements.txt            # Python dependencies
```

---

## 3  Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE (Ingest)                       │
│                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────┐ │
│  │  Budget   │───▶│ PDFExtractor │───▶│ BudgetParser │───▶│SQLite │ │
│  │  PDF      │    │ (PyMuPDF +   │    │ (regex-based │    │  DB   │ │
│  │  (docs/)  │    │  pdfplumber) │    │  structure   │    │       │ │
│  └──────────┘    └──────────────┘    │  recognition)│    └───┬───┘ │
│                                      └──────────────┘        │     │
│                                                              ▼     │
│                                                        ┌──────────┐│
│                                                        │WikiBuilder││
│                                                        │→ wiki/   ││
│                                                        └──────────┘│
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      QUERY PIPELINE (Runtime)                       │
│                                                                     │
│  ┌──────────┐    ┌──────────────────────────────────────────────┐   │
│  │  User     │───▶│          ReAct Agent (engine.py)            │   │
│  │  Question │    │   ┌────────────────────────────────────┐    │   │
│  └──────────┘    │   │     OpenAI Function Calling Loop    │    │   │
│                  │   │                                      │    │   │
│                  │   │  ┌─────────────┐ ┌──────────────┐   │    │   │
│                  │   │  │search_wiki  │ │query_database│   │    │   │
│                  │   │  │(WikiSearch) │ │(SQLAgent)    │   │    │   │
│                  │   │  └──────┬──────┘ └──────┬───────┘   │    │   │
│                  │   │         │                │           │    │   │
│                  │   │  ┌──────┴──────┐ ┌──────┴───────┐   │    │   │
│                  │   │  │ wiki/*.md   │ │ SQLite DB    │   │    │   │
│                  │   │  └─────────────┘ └──────────────┘   │    │   │
│                  │   │                                      │    │   │
│                  │   │  ┌─────────────┐                    │    │   │
│                  │   │  │read_document│                    │    │   │
│                  │   │  │(PageScanner)│                    │    │   │
│                  │   │  └──────┬──────┘                    │    │   │
│                  │   │         │                           │    │   │
│                  │   │  ┌──────┴──────┐                    │    │   │
│                  │   │  │ Budget PDF  │                    │    │   │
│                  │   │  │ (images)    │                    │    │   │
│                  │   │  └─────────────┘                    │    │   │
│                  │   └────────────────────────────────────┘    │   │
│                  │                    │                         │   │
│                  │                    ▼                         │   │
│                  │          ┌──────────────────┐               │   │
│                  │          │  Final Answer    │               │   │
│                  │          │  (German, with   │               │   │
│  ┌──────────┐   │          │  sources & SQL)  │               │   │
│  │  Answer   │◀──┘          └──────────────────┘               │   │
│  └──────────┘                                                  │   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4  Data Pipeline (Ingest Flow)

The ingest pipeline is triggered via `python -m src.cli ingest <pdf_path>` and
processes a budget PDF through four sequential stages.

### 4.1  PDF Extraction (`src/extract/pdf_extractor.py`)

**Two extraction engines run in tandem:**

| Engine | Library | Purpose |
|--------|---------|---------|
| Text extraction | **PyMuPDF** (`fitz`) | Fast full-text extraction per page |
| Table detection | **pdfplumber** | Accurate table structure detection with bbox metadata |

The `PDFExtractor` produces an `ExtractedDocument` containing a list of
`ExtractedPage` objects, each with `.text` (raw text) and `.tables`
(list of row×column string arrays).

**German number parsing** is handled by `parse_german_number()` which
understands dot-thousands separators (`16.161.139`), comma-decimals (`102,03`),
negative parentheses notation (`(1.200)`), and k-suffixes (`50k`).

### 4.2  Budget Parsing (`src/extract/budget_parser.py`)

The `BudgetParser` takes the `ExtractedDocument` and recognises the hierarchical
budget structure using regex patterns:

```
Einzelplan (e.g. EP 14 = BMVg)
  └── Kapitel (e.g. 1403)
        └── Titel (e.g. "431 57")
              ├── ausgaben_soll / ausgaben_ist
              ├── einnahmen_soll / einnahmen_ist
              └── metadata flags
```

It produces a `ParsedBudget` containing:
- **`entries`** — `BudgetEntry` objects (financial line items)
- **`personnel`** — `PersonnelEntry` objects (Planstellen by Besoldungsgruppe)
- **`einzelplan_meta`** / **`kapitel_meta`** — hierarchy metadata
- **`verpflichtungen`** — commitment authorisations with maturity schedules
- **`sachverhalte`** — special items (procurement categories)

All monetary values are in **1 000 €** (thousands of Euros) as printed in the
source document.

### 4.3  Database Loading (`src/db/loader.py`)

The `DataLoader` receives a `ParsedBudget` and inserts its contents into the
SQLite database using `INSERT OR REPLACE` for idempotent re-loading. It also
loads **reference data** (GDP, inflation rates, NATO targets) via
`load_reference_data()`.

The database schema is defined in `src/db/schema.py` and initialised via
`init_db()` with WAL journal mode, foreign keys, and a 5-second busy timeout.

### 4.4  Wiki Generation (`src/wiki/builder.py`)

The `WikiBuilder` queries the populated database and generates Markdown pages
in three categories:

| Category | Directory | Content |
|----------|-----------|---------|
| **Entities** | `wiki/entities/` | Einzelplan and Kapitel profiles with financial summaries |
| **Concepts** | `wiki/concepts/` | Budget terminology explanations (Verrechnungstitel, Flexibilisierung, etc.) |
| **Analyses** | `wiki/analyses/` | Comparative analyses and trend reports |

Each page has YAML frontmatter (`title`, `type`, `years`, `tags`) for
structured indexing. The `WikiIndexer` then rebuilds `wiki/index.md` (a catalog
of all pages) and appends to `wiki/log.md` (an activity log).

---

## 5  Query Execution Flow

This is the heart of the application. When a user asks a question, the
**ReAct agent** in `src/query/engine.py` orchestrates multi-step reasoning.

### 5.1  The ReAct Agent Loop

The agent uses the **Reasoning + Acting** pattern:
1. **Reason** — The LLM analyses the question and decides what action to take
2. **Act** — The LLM calls a tool (wiki search, SQL query, or page scan)
3. **Observe** — The tool result is fed back into the conversation
4. **Repeat** — The LLM decides whether to call another tool or produce a final answer

```
         ┌────────────────────────────────────┐
         │  System Prompt + Conversation       │
         │  History + User Question            │
         └────────────┬───────────────────────┘
                      │
                      ▼
         ┌────────────────────────────────────┐
    ┌───▶│  Azure OpenAI Chat Completion      │
    │    │  (with tool_choice="auto")         │
    │    └────────────┬───────────────────────┘
    │                 │
    │                 ▼
    │    ┌────────────────────────────────────┐
    │    │  Response has tool_calls?           │
    │    └──────┬─────────────┬───────────────┘
    │           │ YES         │ NO
    │           ▼             ▼
    │    ┌────────────┐  ┌──────────────────┐
    │    │ Execute     │  │ Return final     │
    │    │ tool(s)     │  │ answer with      │
    │    │ Append      │  │ sources, SQL,    │
    │    │ results to  │  │ confidence       │
    │    │ messages    │  └──────────────────┘
    │    └──────┬─────┘
    │           │
    └───────────┘  (up to max_iterations=5)
```

### 5.2  OpenAI Function Calling Mechanism

The engine uses **OpenAI's native function calling** (not a framework like
LangChain). Three tools are registered as JSON schemas:

```python
tools = [
    {"type": "function", "function": {"name": "search_wiki", ...}},
    {"type": "function", "function": {"name": "query_database", ...}},
    {"type": "function", "function": {"name": "read_document", ...}},
]
```

The API is called with `tool_choice="auto"`, letting GPT-4o decide whether and
which tool to invoke. When the model returns `tool_calls` in its response, the
engine:

1. Parses each `tool_call.function.arguments` (JSON)
2. Executes the corresponding Python function
3. Appends the result as a `{"role": "tool", "tool_call_id": ..., "content": ...}` message
4. Calls the API again with the updated conversation

This loop continues until the model produces a response **without** tool calls
(the final answer) or the safety cap of 5 iterations is reached.

### 5.3  The Three Tools

#### `search_wiki` — Knowledge Retrieval (RAG)

| Aspect | Detail |
|--------|--------|
| **Input** | `query` — a search term or question |
| **Implementation** | `WikiSearch.get_context_for_query()` |
| **Search method** | BM25-style scoring with German-aware tokenisation |
| **Features** | Umlaut normalisation (ä→ae), stop-word removal, substring matching for compound words |
| **Scoring zones** | Title (4×), Tags (3×), Headings (2×), Body (1×) |
| **Output** | Concatenated Markdown content from top wiki pages (≤4000 chars) |
| **Use case** | Terminology, background, qualitative context |

#### `query_database` — Text-to-SQL

| Aspect | Detail |
|--------|--------|
| **Input** | `question` — natural-language question |
| **Implementation** | `SQLAgent.query()` |
| **Steps** | 1. Build schema description from live DB (PRAGMA introspection + sample rows) |
|  | 2. LLM generates SQL (German system prompt + few-shot examples) |
|  | 3. Validate: only SELECT/WITH, no DDL/DML, enforce LIMIT |
|  | 4. Execute against SQLite |
|  | 5. On error: retry up to 3× feeding error back to LLM |
| **Safety** | Forbidden keyword regex blocks DROP/DELETE/UPDATE/INSERT/etc. |
| **Output** | Formatted text table with German number formatting |
| **Use case** | Precise financial figures, aggregations, comparisons |

#### `read_document` — Unified Document Reader (Multimodal PDF Analysis)

| Aspect | Detail |
|--------|--------|
| **Input** | `question`, `year`, optional `einzelplan`, `kapitel`, `page_numbers`, `pdf_filename` |
| **Implementation** | `QueryEngine._exec_read_document()` → `DocumentLocator` + `PageScanner` |
| **Steps** | 1. If explicit `page_numbers` given → go directly to those pages |
|  | 2. Otherwise → use `DocumentLocator` to find pages via year/EP/Kapitel (like a TOC lookup) |
|  | 3. Extract text via PyMuPDF for the page range |
|  | 4. Render each page as PNG at 150 DPI via PyMuPDF |
|  | 5. Build multimodal prompt (interleaved text + base64 images) |
|  | 6. Send to GPT-4o for visual table analysis |
| **Confidence** | Model rates its own answer as high/medium/low |
| **Cap** | Max 10 pages per request |
| **Use case** | Erläuterungen, Haushaltsvermerke, complex tables, verification of DB values |

### 5.4  How SQL Is Generated from Natural Language

The SQL generation pipeline in `SQLAgent` works as follows:

```
   User Question (German)
          │
          ▼
   ┌──────────────────────────────────────────────┐
   │  Schema Description (auto-generated)          │
   │  ├── German budget terminology glossary       │
   │  ├── All tables with columns + types          │
   │  ├── Sample data (3 rows per table)           │
   │  └── Few-shot SQL examples (5 patterns)       │
   └──────────────────────┬───────────────────────┘
                          │
                          ▼
   ┌──────────────────────────────────────────────┐
   │  LLM (temperature=0.0) generates SQL          │
   │  System prompt: "Du bist ein SQL-Experte …"   │
   └──────────────────────┬───────────────────────┘
                          │
                          ▼
   ┌──────────────────────────────────────────────┐
   │  Validation                                   │
   │  ├── Strip markdown fences + SQL comments     │
   │  ├── Must start with SELECT or WITH           │
   │  ├── Reject forbidden DDL/DML keywords        │
   │  └── Auto-append LIMIT 1000 if missing        │
   └──────────────────────┬───────────────────────┘
                          │
                          ▼
   ┌──────────────────────────────────────────────┐
   │  Execute → Success? Return SQLResult          │
   │            Failure? Feed error to LLM → Retry │
   │                     (up to 3 attempts)        │
   └──────────────────────────────────────────────┘
```

### 5.5  Answer Synthesis

After all tool calls complete, GPT-4o synthesises the final answer considering:
- SQL query results (precise numbers)
- Wiki context (background and explanations)
- Page scan findings (visual table data)
- Conversation history (for follow-up questions)

The system prompt instructs the model to:
- Answer **always in German** with precise figures
- Show **calculation steps** (e.g. percentage changes)
- Cite **sources** (Einzelplan, Kapitel, page numbers)
- Admit when data is insufficient

### 5.6  Conversation History Integration

The `ask()` method accepts `conversation_history` — a list of prior
user/assistant message pairs. This enables:

- **Follow-up questions**: *"Und wie war das im Vorjahr?"*
- **Pronoun resolution**: *"Wie hoch sind davon die Personalkosten?"*
- **Context continuity** across multiple turns

History is trimmed to the last 40 messages (20 exchanges) to stay within
token limits. Only user/assistant pairs are included — tool-call details from
earlier rounds are omitted.

### 5.7  Multi-Step Reasoning Example

A question like *"Vergleiche die Verteidigungsausgaben 2025 und 2026 und
berechne die Veränderung in Prozent"* might trigger:

1. **Iteration 1** → `query_database("Ausgaben Einzelplan 14 Jahr 2025")`
2. **Iteration 2** → `query_database("Ausgaben Einzelplan 14 Jahr 2026")`
3. **Iteration 3** → `search_wiki("Einzelplan 14 Verteidigung")` for context
4. **Final** → Synthesise answer: *"Die Ausgaben stiegen von 62,4 Mrd. € auf
   82,7 Mrd. € (+32,5%). Einzelplan 14 umfasst …"*

---

## 6  Key Concepts

### 6.1  ReAct (Reasoning + Acting) Pattern

The ReAct pattern interleaves reasoning traces and task-specific actions. Unlike
simple prompt→response flows, the agent can:

- **Observe** intermediate results before deciding the next step
- **Chain** multiple tool calls (e.g. two SQL queries then a wiki lookup)
- **Recover** from tool errors (SQL retry, fallback to page scan)
- **Self-terminate** when it has sufficient information

The implementation avoids heavy frameworks (no LangChain/LangGraph) — it is a
straightforward `for` loop over OpenAI API calls with tool-call dispatch.

### 6.2  OpenAI Function Calling / Tool Use

Instead of prompt-engineering the model to output structured commands, the
application uses OpenAI's **native function calling**:

- Tools are declared as JSON schemas in the API request
- `tool_choice="auto"` lets the model choose freely
- The model returns structured `tool_calls` with typed arguments
- Results are fed back via `role: "tool"` messages

This is more reliable than regex-based output parsing and supports parallel
tool calls in a single response.

### 6.3  Text-to-SQL with Schema Context

The LLM generates SQL by receiving:
1. **Full schema** — auto-introspected from the live database via `PRAGMA table_info`
2. **Sample data** — 3 rows per table so the model understands actual values
3. **Domain glossary** — German budget terminology with descriptions
4. **Few-shot examples** — 5 representative SQL patterns

This rich context allows the model to generate correct SQL even for
domain-specific queries involving German column names, Tausend-Euro units,
and the Einzelplan/Kapitel/Titel hierarchy.

### 6.4  RAG (Retrieval-Augmented Generation) via Wiki Search

The wiki acts as a **knowledge base** that grounds LLM responses in
domain-specific facts. The search pipeline:

1. **Tokenise** the query with German-aware processing (umlauts, stop words)
2. **Score** each wiki page using BM25-style relevance (weighted by zone)
3. **Retrieve** the top pages and concatenate their content
4. **Inject** this context into the LLM conversation

This prevents hallucination for conceptual questions and provides source
citations.

### 6.5  Multimodal Page Scanning as Fallback

Budget PDFs contain complex tables where:
- Text extraction may misalign columns
- Numbers may be split across cells
- Visual layout is essential for correct reading

The `PageScanner` sends **both** extracted text and rendered page images to
GPT-4o, leveraging its vision capability to read tables accurately. It reports
a self-assessed confidence level (`high`/`medium`/`low`).

### 6.6  Conversation Memory for Follow-Up Questions

The interactive mode maintains a rolling conversation history that is injected
into the system prompt. This allows the agent to resolve:
- **Anaphora**: *"Wie hoch sind davon die Personalkosten?"*
- **Ellipsis**: *"Und 2025?"*
- **Comparisons**: *"Ist das mehr als im Vorjahr?"*

---

## 7  Database Schema

The SQLite database (`data/bundeshaushalt.db`) contains 7 tables:

### Core Tables

```
┌──────────────────┐       ┌──────────────────┐
│  einzelplan_meta │       │   kapitel_meta    │
│  ────────────────│       │  ────────────────│
│  year       [PK] │──┐    │  year       [PK] │
│  version    [PK] │  │    │  version    [PK] │
│  einzelplan [PK] │  ├───▶│  einzelplan      │
│  einzelplan_text │  │    │  kapitel    [PK] │
└──────────────────┘  │    │  kapitel_text    │
                      │    └──────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│                    haushaltsdaten                     │
│  ─────────────────────────────────────────────────── │
│  id [PK]  year  version  einzelplan  kapitel  titel  │
│  titel_text  titelgruppe                             │
│  ausgaben_soll  ausgaben_ist                         │
│  einnahmen_soll  einnahmen_ist                       │
│  is_verrechnungstitel  flexibilisiert                │
│  deckungsfaehig  gegenseitig_deckungsfaehig          │
└─────────────────────────────────────────────────────┘
```

### Supporting Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `personalhaushalt` | Personnel positions | `besoldungsgruppe`, `planstellen_gesamt` |
| `verpflichtungsermachtigungen` | Future payment commitments (VE) | `betrag_gesamt`, `faellig_jahr`, `faellig_betrag` |
| `sachverhalte` | Special items | `kategorie`, `betrag` |
| `referenzdaten` | Macroeconomic reference data | `indicator` (BIP, Inflationsrate, etc.), `value` |

### Common Filter Patterns

- **Year + Einzelplan**: `WHERE year = 2026 AND einzelplan = '14'`
- **Version**: `WHERE version = 'beschluss'` (vs. `'entwurf'`, `'nachtrag'`)
- **Exclude internal transfers**: `WHERE is_verrechnungstitel = 0`

---

## 8  CLI Commands

All commands are invoked via `python -m src.cli <command>`.

| Command | Description | Example |
|---------|-------------|---------|
| `ingest <pdf>` | Full pipeline: extract → parse → load DB → build wiki | `ingest docs/0350-25.pdf --reset` |
| `query <question>` | Ask a question (uses ReAct agent) | `query "Ausgaben EP 14 im Jahr 2026?"` |
| `search <term>` | Search the wiki (keyword search) | `search Verrechnungstitel --limit 5` |
| `interactive` | Interactive Q&A mode with conversation memory | Supports `!sql`, `!search`, `!history`, `!clear` |
| `status` | Show system status (DB rows, wiki pages, PDFs) | |
| `lint` | Wiki health check (orphans, broken links, coverage) | |
| `download` | Download budget PDFs from bundeshaushalt.de | `download --year 2026 --list-only` |

### Interactive Mode Commands

| Command | Action |
|---------|--------|
| `!sql <query>` | Execute raw SQL against the database |
| `!search <term>` | Search the wiki |
| `!history` | Show conversation history |
| `!clear` | Clear conversation history |
| `quit` / `exit` | Exit interactive mode |

---

## 9  Configuration

### Environment Variables (`.env` file)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AZURE_OPENAI_ENDPOINT` | **Yes** | — | Azure OpenAI (or APIM gateway) endpoint URL |
| `AZURE_OPENAI_API_KEY` | **Yes** | — | API key / APIM subscription key |
| `AZURE_OPENAI_API_VERSION` | No | `2024-12-01-preview` | API version |
| `AZURE_OPENAI_DEPLOYMENT` | No | `gpt-4o` | Model deployment name |
| `DOCS_DIR` | No | `docs` | Directory for source PDFs |
| `WIKI_DIR` | No | `wiki` | Directory for wiki pages |
| `DATA_DIR` | No | `data` | Directory for database and downloads |

### Azure OpenAI via APIM

The LLM client routes requests through **Azure API Management (APIM)**. The
`api_key` is sent as the `api-key` HTTP header, which serves as both the APIM
subscription key and OpenAI authentication. This is handled automatically by
the `openai` Python library's `AzureOpenAI` client.

---

## 10  Download System

The `scripts/download_budgets.py` script uses **Playwright** (headless
Chromium) to scrape the official Bundeshaushalt download portal.

### Flow

```
1. Launch headless Chromium browser
2. Navigate to bundeshaushalt.de/Download-Portal
3. Dismiss cookie consent banner
4. For each year (2005–2026):
   a. Click year selector in the portal UI
   b. Wait for document list to populate
   c. Collect all <a href="*.pdf"> links
5. Download each PDF with streaming + progress bar
   - Retries up to 3× on failure
   - Skips already-downloaded files (unless --force)
   - Organises into data/budgets/<year>/<filename>.pdf
```

### CLI Options

```
python scripts/download_budgets.py [OPTIONS]
  -y, --year <N>      Download only a specific year
  -o, --output-dir    Output directory (default: data/budgets)
  -l, --list-only     List available PDFs without downloading
  -f, --force         Re-download existing files
```

---

## 11  Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `pymupdf` (fitz) | ≥1.24.0 | Fast PDF text extraction + page rendering |
| `pdfplumber` | ≥0.11.0 | Accurate table detection in PDFs |
| `openai` | ≥1.40.0 | Azure OpenAI API client |
| `python-dotenv` | ≥1.0.0 | Load `.env` configuration |
| `rich` | ≥13.7.0 | Terminal UI (tables, panels, progress bars) |
| `playwright` | ≥1.40.0 | Browser automation for PDF downloads |
| `requests` | ≥2.31.0 | HTTP client for PDF file downloads |
