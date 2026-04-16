"""CLI interface for the Bundeshaushalt Q&A application.

Main entry point for ingesting budget PDFs, querying the database,
searching the wiki, and managing the system.

Usage:
    python -m src.cli <command> [options]
    python -m src <command> [options]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import textwrap
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from src.config import config

console = Console()

# ---------------------------------------------------------------------------
# Graceful imports for modules that may not exist yet
# ---------------------------------------------------------------------------

_MISSING: dict[str, str] = {}


def _try_import(label: str, do_import):
    """Run *do_import* (a zero-arg callable) and stash errors in _MISSING."""
    try:
        return do_import()
    except Exception as exc:  # noqa: BLE001
        _MISSING[label] = str(exc)
        return None


def _require(label: str):
    """Abort with a friendly message if *label* was not imported."""
    if label in _MISSING:
        console.print(
            f"[bold red]Modul nicht verfügbar:[/] {label}\n"
            f"  Fehler: {_MISSING[label]}\n"
            f"  Dieses Modul wird für diesen Befehl benötigt.",
        )
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Command: ingest
# ---------------------------------------------------------------------------


def cmd_ingest(args: argparse.Namespace) -> None:
    """Extract, parse, load data from a PDF, and build wiki pages."""
    from rich.progress import Progress, SpinnerColumn, TextColumn

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        console.print(f"[bold red]PDF nicht gefunden:[/] {pdf_path}")
        raise SystemExit(1)

    console.print(
        Panel(
            f"[bold]Ingest-Pipeline[/]\n📄 {pdf_path.name}",
            title="Bundeshaushalt Ingest",
            border_style="blue",
        )
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        # Step 1 — PDF extraction
        task = progress.add_task("Extrahiere PDF …", total=None)
        try:
            from src.extract.pdf_extractor import PDFExtractor
        except ImportError as exc:
            console.print(f"[bold red]Import-Fehler:[/] pdf_extractor — {exc}")
            raise SystemExit(1)

        extractor = PDFExtractor(pdf_path)
        doc = extractor.extract_full()
        progress.update(
            task,
            description=(
                f"PDF extrahiert — {doc.total_pages} Seiten, "
                f"{sum(len(p.tables) for p in doc.pages)} Tabellen"
            ),
            completed=True,
        )

        # Step 2 — Budget parsing
        task = progress.add_task("Parse Haushaltsstruktur …", total=None)
        try:
            from src.extract.budget_parser import BudgetParser
        except ImportError as exc:
            console.print(f"[bold red]Import-Fehler:[/] budget_parser — {exc}")
            raise SystemExit(1)

        parser = BudgetParser(doc)
        budget = parser.parse()
        progress.update(
            task,
            description=(
                f"Geparst — {len(budget.entries)} Einträge, "
                f"{len(budget.personnel)} Personalstellen"
            ),
            completed=True,
        )

        # Step 3 — Database
        task = progress.add_task("Lade in Datenbank …", total=None)
        try:
            from src.db.schema import init_db, reset_db
            from src.db.loader import DataLoader
        except ImportError as exc:
            console.print(f"[bold red]Import-Fehler:[/] db — {exc}")
            raise SystemExit(1)

        db_path = config.DB_PATH
        if args.reset:
            reset_db(db_path)
        conn = init_db(db_path)
        loader = DataLoader(conn)
        stats = loader.load(budget)
        ref_count = loader.load_reference_data()
        conn.close()
        progress.update(
            task,
            description=(
                f"Datenbank geladen — "
                f"{sum(stats.values())} Zeilen + {ref_count} Referenzdaten"
            ),
            completed=True,
        )

        # Step 4 — Wiki
        task = progress.add_task("Erstelle Wiki-Seiten …", total=None)
        try:
            from src.wiki.builder import WikiBuilder
            from src.wiki.indexer import WikiIndexer
        except ImportError as exc:
            console.print(f"[bold red]Import-Fehler:[/] wiki — {exc}")
            raise SystemExit(1)

        builder = WikiBuilder()
        pages = builder.build_all(source_file=pdf_path.name)
        indexer = WikiIndexer()
        indexer.rebuild_index()
        indexer.append_log("ingest", f"PDF={pdf_path.name}, Zeilen={sum(stats.values())}")
        progress.update(
            task,
            description=f"Wiki aktualisiert — {len(pages)} Seiten erstellt/aktualisiert",
            completed=True,
        )

    # Summary table
    summary = Table(title="Ingest-Zusammenfassung", border_style="green")
    summary.add_column("Komponente", style="cyan")
    summary.add_column("Ergebnis", justify="right")
    summary.add_row("PDF-Seiten", str(doc.total_pages))
    summary.add_row("Haushaltsdaten", str(stats.get("haushaltsdaten", 0)))
    summary.add_row("Einzelplan-Meta", str(stats.get("einzelplan_meta", 0)))
    summary.add_row("Kapitel-Meta", str(stats.get("kapitel_meta", 0)))
    summary.add_row("Personalhaushalt", str(stats.get("personalhaushalt", 0)))
    summary.add_row("VE", str(stats.get("verpflichtungsermachtigungen", 0)))
    summary.add_row("Sachverhalte", str(stats.get("sachverhalte", 0)))
    summary.add_row("Referenzdaten", str(ref_count))
    summary.add_row("Wiki-Seiten", str(len(pages)))
    console.print(summary)
    console.print("[bold green]✓ Ingest abgeschlossen.[/]")


# ---------------------------------------------------------------------------
# Command: ingest-all
# ---------------------------------------------------------------------------


def _detect_version(filename: str) -> str:
    """Auto-detect budget version from PDF filename."""
    name_lower = filename.lower()
    if "entwurf" in name_lower:
        return "entwurf"
    if "beschluss" in name_lower:
        return "beschluss"
    if "nachtrag" in name_lower:
        return "nachtrag"
    if "haushaltsrechnung" in name_lower or "ist" in name_lower:
        return "ist"
    return "soll"


def _is_main_budget_pdf(filename: str) -> bool:
    """Check if a PDF is the main/full budget document (vs. supplementary)."""
    name_lower = filename.lower()
    for keyword in ("bundeshaushalt", "haushaltsplan", "gesamt", "epl"):
        if keyword in name_lower:
            return True
    return False


def cmd_ingest_all(args: argparse.Namespace) -> None:
    """Ingest ALL budget PDFs from data/budgets/ into the database."""
    import logging
    import time

    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    budgets_dir = config.DATA_DIR / "budgets"
    if not budgets_dir.exists():
        console.print(f"[bold red]Verzeichnis nicht gefunden:[/] {budgets_dir}")
        raise SystemExit(1)

    # Discover year directories
    year_dirs = sorted(
        d for d in budgets_dir.iterdir()
        if d.is_dir() and d.name.isdigit()
    )
    if args.year:
        year_dirs = [d for d in year_dirs if int(d.name) == args.year]
        if not year_dirs:
            console.print(f"[bold red]Jahr {args.year} nicht gefunden in {budgets_dir}[/]")
            raise SystemExit(1)

    # Count total PDFs for progress
    all_pdfs: list[tuple[int, Path]] = []
    for yd in year_dirs:
        pdfs = sorted(yd.glob("*.pdf"))
        for p in pdfs:
            all_pdfs.append((int(yd.name), p))

    console.print(
        Panel(
            f"[bold]Batch-Ingest aller Budget-PDFs[/]\n"
            f"📁 {budgets_dir}\n"
            f"📅 {len(year_dirs)} Jahre, {len(all_pdfs)} PDFs",
            title="Bundeshaushalt Batch-Ingest",
            border_style="blue",
        )
    )

    # --- Database setup ---
    try:
        from src.db.loader import DataLoader
        from src.db.schema import init_db, reset_db
    except ImportError as exc:
        console.print(f"[bold red]Import-Fehler:[/] {exc}")
        raise SystemExit(1)

    db_path = config.DB_PATH
    if args.reset:
        console.print("[yellow]Datenbank wird zurückgesetzt …[/]")
        reset_db(db_path)
    conn = init_db(db_path)
    loader = DataLoader(conn)

    # Check which years already have data (for --skip-existing)
    existing_years: set[int] = set()
    if args.skip_existing:
        try:
            rows = conn.execute(
                "SELECT DISTINCT year FROM haushaltsdaten"
            ).fetchall()
            existing_years = {r[0] for r in rows}
        except Exception:
            pass

    # --- Process PDFs ---
    try:
        from src.extract.parser_router import parse_budget_pdf_routed
    except ImportError as exc:
        console.print(f"[bold red]Import-Fehler:[/] budget_parser — {exc}")
        raise SystemExit(1)

    year_stats: list[dict] = []
    total_entries = 0
    total_personnel = 0
    total_source_docs = 0
    errors: list[str] = []
    t0 = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        year_task = progress.add_task(
            "Jahre verarbeiten …", total=len(year_dirs)
        )

        for yd in year_dirs:
            year = int(yd.name)
            pdfs = sorted(yd.glob("*.pdf"))

            if args.skip_existing and year in existing_years:
                progress.update(
                    year_task,
                    advance=1,
                    description=f"Jahr {year} übersprungen (vorhanden)",
                )
                year_stats.append({
                    "year": year,
                    "version": "—",
                    "pdfs": 0,
                    "entries": 0,
                    "personnel": 0,
                    "source_docs": 0,
                    "status": "übersprungen",
                })
                continue

            progress.update(
                year_task,
                description=f"Jahr {year} — {len(pdfs)} PDFs …",
            )

            yr_entries = 0
            yr_personnel = 0
            yr_source_docs = 0
            yr_pdfs_ok = 0
            yr_version = "soll"

            pdf_task = progress.add_task(
                f"  {year}: PDFs …", total=len(pdfs)
            )

            for pdf_path in pdfs:
                version = _detect_version(pdf_path.name)
                yr_version = version
                progress.update(
                    pdf_task,
                    description=f"  {year}: {pdf_path.name}",
                )

                try:
                    parsed = parse_budget_pdf_routed(pdf_path, year=year, version=version)
                    # Override year/version if parser couldn't detect them
                    if parsed.year == 0 or parsed.year is None:
                        parsed.year = year
                    if not parsed.version:
                        parsed.version = version

                    stats = loader.load(parsed)

                    n_entries = stats.get("haushaltsdaten", 0)
                    n_personnel = stats.get("personalhaushalt", 0)
                    yr_entries += n_entries
                    yr_personnel += n_personnel

                    # Register source document
                    try:
                        import fitz
                        with fitz.open(str(pdf_path)) as doc:
                            page_count = len(doc)
                    except Exception:
                        page_count = 0

                    loader.load_source_document(
                        year=year,
                        version=version,
                        filename=pdf_path.name,
                        filepath=str(pdf_path),
                        page_count=page_count,
                    )

                    # Load page text for FTS5 fulltext search
                    try:
                        import fitz as _fitz
                        doc_fitz = _fitz.open(str(pdf_path))
                        page_texts = []
                        for page_idx in range(len(doc_fitz)):
                            text = doc_fitz[page_idx].get_text("text")
                            if text and text.strip():
                                page_texts.append(
                                    (page_idx + 1, text, None, None)
                                )
                        doc_fitz.close()
                        if page_texts:
                            loader.load_page_text(
                                year, pdf_path.name, page_texts
                            )
                    except Exception as fts_exc:
                        logging.getLogger(__name__).warning(
                            "Page text loading failed for %s: %s",
                            pdf_path.name, fts_exc,
                        )

                    # Build page index (section detection)
                    try:
                        from src.extract.section_detector import (
                            detect_einzelplan,
                            detect_kapitel,
                            detect_section_type,
                            extract_heading,
                        )

                        doc_pi = fitz.open(str(pdf_path))
                        page_index_data = []
                        current_ep = None
                        current_kap = None
                        for page_idx in range(len(doc_pi)):
                            text = doc_pi[page_idx].get_text("text")
                            section = detect_section_type(text)
                            ep = detect_einzelplan(text) or current_ep
                            kap = detect_kapitel(text) or current_kap
                            heading = extract_heading(text)
                            if ep:
                                current_ep = ep
                            if kap:
                                current_kap = kap
                            page_index_data.append(
                                (page_idx + 1, section, ep, kap, heading)
                            )
                        doc_pi.close()
                        if page_index_data:
                            loader.load_page_index(
                                year, pdf_path.name, page_index_data
                            )
                    except Exception as pi_exc:
                        logging.getLogger(__name__).warning(
                            "Page index failed for %s: %s",
                            pdf_path.name, pi_exc,
                        )

                    # Build hierarchical TOC
                    try:
                        from src.extract.toc_builder import TOCBuilder

                        toc_builder = TOCBuilder(pdf_path, year)
                        toc_entries = toc_builder.build()
                        if toc_entries:
                            toc_count = loader.load_toc(
                                year, pdf_path.name, toc_entries
                            )
                            logging.getLogger(__name__).info(
                                "TOC: %d entries for %s",
                                toc_count, pdf_path.name,
                            )
                    except Exception as toc_exc:
                        logging.getLogger(__name__).warning(
                            "TOC building failed for %s: %s",
                            pdf_path.name, toc_exc,
                        )

                    yr_source_docs += 1
                    yr_pdfs_ok += 1

                except Exception as exc:
                    err_msg = f"{pdf_path.name}: {exc}"
                    errors.append(err_msg)
                    logging.getLogger(__name__).warning(
                        "Fehler bei %s: %s", pdf_path.name, exc
                    )

                progress.update(pdf_task, advance=1)

            progress.remove_task(pdf_task)

            total_entries += yr_entries
            total_personnel += yr_personnel
            total_source_docs += yr_source_docs

            year_stats.append({
                "year": year,
                "version": yr_version,
                "pdfs": yr_pdfs_ok,
                "entries": yr_entries,
                "personnel": yr_personnel,
                "source_docs": yr_source_docs,
                "status": "✓" if yr_pdfs_ok > 0 else "✗",
            })

            progress.update(year_task, advance=1)

    # Load reference data once
    ref_count = loader.load_reference_data()
    conn.close()
    elapsed = time.time() - t0

    # --- Summary table ---
    summary = Table(
        title=f"Batch-Ingest Ergebnis ({elapsed:.0f}s)",
        border_style="green",
    )
    summary.add_column("Jahr", style="cyan", justify="right")
    summary.add_column("Version", style="dim")
    summary.add_column("PDFs", justify="right")
    summary.add_column("Einträge", justify="right")
    summary.add_column("Personal", justify="right")
    summary.add_column("Quellen", justify="right")
    summary.add_column("Status", justify="center")

    for ys in year_stats:
        summary.add_row(
            str(ys["year"]),
            ys["version"],
            str(ys["pdfs"]),
            str(ys["entries"]),
            str(ys["personnel"]),
            str(ys["source_docs"]),
            ys["status"],
        )

    # Totals row
    summary.add_section()
    summary.add_row(
        "GESAMT",
        "",
        str(sum(ys["pdfs"] for ys in year_stats)),
        str(total_entries),
        str(total_personnel),
        str(total_source_docs),
        "",
        style="bold",
    )

    console.print(summary)

    if errors:
        console.print(f"\n[yellow]⚠ {len(errors)} Fehler:[/]")
        for e in errors[:20]:
            console.print(f"  [dim]• {e}[/]")
        if len(errors) > 20:
            console.print(f"  [dim]… und {len(errors) - 20} weitere[/]")

    console.print(
        f"\n[bold green]✓ Batch-Ingest abgeschlossen:[/] "
        f"{total_entries} Einträge, {total_personnel} Personalstellen, "
        f"{total_source_docs} Quelldokumente, {ref_count} Referenzdaten"
    )


# ---------------------------------------------------------------------------
# Command: query
# ---------------------------------------------------------------------------


def cmd_query(args: argparse.Namespace) -> None:
    """Ask a question about the Bundeshaushalt."""
    question = " ".join(args.question)
    if not question.strip():
        console.print("[bold red]Bitte eine Frage angeben.[/]")
        raise SystemExit(1)

    _display_answer(question)


def _display_answer(
    question: str,
    conversation_history: list[dict[str, str]] | None = None,
) -> "AnswerResult | None":
    """Query the engine and display a formatted answer.

    Returns the *AnswerResult* so callers (e.g. interactive mode) can
    record it in conversation history.  Returns ``None`` on import error.
    """
    try:
        from src.query.engine import QueryEngine, create_engine
    except ImportError as exc:
        console.print(
            Panel(
                f"[bold red]QueryEngine nicht verfügbar[/]\n{exc}\n\n"
                "Das Modul src.query.engine existiert noch nicht.\n"
                "Verwende stattdessen [bold]search[/] oder [bold]interactive[/] mit !sql.",
                title="Fehler",
                border_style="red",
            )
        )
        raise SystemExit(1)

    console.print(f"\n[dim]Frage:[/] {question}")
    with console.status("[bold blue]Denke nach …[/]"):
        engine = create_engine()
        result = engine.ask(question, conversation_history=conversation_history)

    # Main answer
    console.print(
        Panel(
            Markdown(result.answer),
            title="Antwort",
            border_style="green",
            padding=(1, 2),
        )
    )

    # Tools used
    if hasattr(result, "tools_used") and result.tools_used:
        tags = " ".join(f"[bold cyan][{t}][/bold cyan]" for t in result.tools_used)
        console.print(f"🔧 Werkzeuge: {tags}")

    # SQL queries
    if hasattr(result, "sql_queries") and result.sql_queries:
        for i, sql in enumerate(result.sql_queries, 1):
            console.print(f"\n[dim]SQL #{i}:[/]")
            console.print(Syntax(sql.strip(), "sql", theme="monokai", line_numbers=False))

    # Sources
    if hasattr(result, "sources") and result.sources:
        console.print("\n[dim]Quellen:[/]")
        for src in result.sources:
            console.print(f"  📄 {src}")

    # Structured citations (exact PDF page references)
    if hasattr(result, "citations") and result.citations:
        console.print("\n[dim]Quellenverweise:[/]")
        for c in result.citations:
            console.print(f"  {c.to_display()}")

    # Confidence
    if hasattr(result, "confidence") and result.confidence:
        color_map = {"high": "green", "medium": "yellow", "low": "red"}
        color = color_map.get(result.confidence, "white")
        console.print(f"\n[{color}]Konfidenz: {result.confidence}[/{color}]")

    return result


# ---------------------------------------------------------------------------
# Command: search
# ---------------------------------------------------------------------------


def cmd_search(args: argparse.Namespace) -> None:
    """Search the wiki for relevant pages."""
    term = " ".join(args.term)
    if not term.strip():
        console.print("[bold red]Bitte einen Suchbegriff angeben.[/]")
        raise SystemExit(1)

    _display_search(term, max_results=args.limit)


def _display_search(term: str, max_results: int = 10) -> None:
    """Run a wiki search and display the results."""
    try:
        from src.wiki.search import WikiSearch
    except ImportError as exc:
        console.print(f"[bold red]WikiSearch nicht verfügbar:[/] {exc}")
        raise SystemExit(1)

    ws = WikiSearch()
    results = ws.search(term, max_results=max_results)

    if not results:
        console.print(f"[yellow]Keine Treffer für:[/] {term}")
        return

    console.print(f"\n[bold]Suchergebnisse für:[/] {term}\n")

    for i, r in enumerate(results, 1):
        rel_path = r.page_path.relative_to(config.WIKI_DIR) if \
            r.page_path.is_relative_to(config.WIKI_DIR) else r.page_path
        console.print(
            Panel(
                f"[bold]{r.title}[/bold]\n"
                f"[dim]{rel_path}[/dim]\n\n"
                f"{r.excerpt}",
                title=f"#{i}  [cyan]Relevanz: {r.relevance_score:.2f}[/cyan]",
                border_style="blue",
                padding=(0, 1),
            )
        )


# ---------------------------------------------------------------------------
# Command: lint
# ---------------------------------------------------------------------------


def cmd_lint(args: argparse.Namespace) -> None:
    """Health-check the wiki."""
    console.print(Panel("[bold]Wiki-Gesundheitsprüfung[/]", border_style="blue"))

    wiki_dir = config.WIKI_DIR
    if not wiki_dir.exists():
        console.print(f"[bold red]Wiki-Verzeichnis nicht gefunden:[/] {wiki_dir}")
        raise SystemExit(1)

    md_files = list(wiki_dir.rglob("*.md"))
    special = {"index.md", "log.md"}
    content_pages = [f for f in md_files if f.name not in special]

    # Collect all internal links and page stems
    all_stems = {f.stem for f in content_pages}
    issues: list[str] = []
    link_targets: set[str] = set()
    pages_with_links: dict[str, list[str]] = {}

    import re
    link_re = re.compile(r"\[\[([^\]]+)\]\]")

    for page in content_pages:
        try:
            text = page.read_text(encoding="utf-8")
        except Exception:
            issues.append(f"⚠ Kann nicht gelesen werden: {page.name}")
            continue

        links = link_re.findall(text)
        pages_with_links[page.stem] = links
        link_targets.update(links)

    # Orphan pages (no incoming links, not index/log)
    referenced_stems = set()
    for targets in pages_with_links.values():
        referenced_stems.update(targets)
    orphans = all_stems - referenced_stems - {"index", "log", "uebersicht"}

    # Missing references (links that point to non-existent pages)
    missing = link_targets - all_stems

    # DB vs wiki consistency
    db_einzelplaene: set[str] = set()
    try:
        from src.db.schema import get_connection
        conn = get_connection(config.DB_PATH)
        rows = conn.execute(
            "SELECT DISTINCT einzelplan FROM einzelplan_meta"
        ).fetchall()
        db_einzelplaene = {r[0] for r in rows}
        conn.close()
    except Exception:
        pass

    entity_pages = {
        f.stem for f in content_pages if "entities" in str(f)
    }
    missing_entity_pages = set()
    for ep in db_einzelplaene:
        expected = f"einzelplan-{ep}"
        if expected not in entity_pages:
            missing_entity_pages.add(ep)

    # Report
    table = Table(title="Lint-Ergebnis", border_style="blue")
    table.add_column("Prüfung", style="cyan")
    table.add_column("Ergebnis", justify="right")
    table.add_column("Status")
    table.add_row(
        "Wiki-Seiten gesamt",
        str(len(content_pages)),
        "[green]✓[/]",
    )
    table.add_row(
        "Verwaiste Seiten (keine eingehenden Links)",
        str(len(orphans)),
        "[green]✓[/]" if len(orphans) == 0 else "[yellow]⚠[/]",
    )
    table.add_row(
        "Fehlende Querverweise",
        str(len(missing)),
        "[green]✓[/]" if len(missing) == 0 else "[red]✗[/]",
    )
    table.add_row(
        "EP in DB ohne Wiki-Seite",
        str(len(missing_entity_pages)),
        "[green]✓[/]" if len(missing_entity_pages) == 0 else "[yellow]⚠[/]",
    )
    console.print(table)

    if orphans:
        console.print("\n[yellow]Verwaiste Seiten:[/]")
        for o in sorted(orphans):
            console.print(f"  • {o}")

    if missing:
        console.print("\n[red]Fehlende Querverweise:[/]")
        for m in sorted(missing):
            console.print(f"  • [[{m}]]")

    if missing_entity_pages:
        console.print("\n[yellow]Einzelpläne ohne Wiki-Seite:[/]")
        for ep in sorted(missing_entity_pages):
            console.print(f"  • Einzelplan {ep}")

    if not orphans and not missing and not missing_entity_pages:
        console.print("\n[bold green]✓ Wiki ist konsistent.[/]")


# ---------------------------------------------------------------------------
# Command: status
# ---------------------------------------------------------------------------


def cmd_status(args: argparse.Namespace) -> None:
    """Show system stats."""
    console.print(Panel("[bold]Systemstatus[/]", border_style="blue"))

    # Database stats
    db_table = Table(title="Datenbank", border_style="green")
    db_table.add_column("Tabelle", style="cyan")
    db_table.add_column("Zeilen", justify="right")

    db_path = config.DB_PATH
    if db_path.exists():
        try:
            from src.db.schema import get_connection
            conn = get_connection(db_path)
            for tbl in [
                "einzelplan_meta",
                "kapitel_meta",
                "haushaltsdaten",
                "personalhaushalt",
                "verpflichtungsermachtigungen",
                "sachverhalte",
                "referenzdaten",
            ]:
                try:
                    row = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
                    db_table.add_row(tbl, str(row[0]))
                except sqlite3.OperationalError:
                    db_table.add_row(tbl, "[red]fehlt[/]")
            conn.close()
        except Exception as exc:
            db_table.add_row("[red]Fehler[/]", str(exc))
    else:
        db_table.add_row("[dim]—[/]", "[dim]Datenbank existiert nicht[/]")

    console.print(db_table)

    # Wiki stats
    wiki_dir = config.WIKI_DIR
    wiki_table = Table(title="Wiki", border_style="green")
    wiki_table.add_column("Kategorie", style="cyan")
    wiki_table.add_column("Seiten", justify="right")

    if wiki_dir.exists():
        for category in ("entities", "concepts", "analyses"):
            cat_dir = wiki_dir / category
            if cat_dir.exists():
                count = len(list(cat_dir.glob("*.md")))
                wiki_table.add_row(category, str(count))
            else:
                wiki_table.add_row(category, "0")
        special = sum(1 for f in wiki_dir.glob("*.md"))
        wiki_table.add_row("root (index, log, …)", str(special))
    else:
        wiki_table.add_row("[dim]—[/]", "[dim]Wiki existiert nicht[/]")

    console.print(wiki_table)

    # Source PDFs
    docs_dir = config.DOCS_DIR
    pdf_table = Table(title="Quelldokumente", border_style="green")
    pdf_table.add_column("Datei", style="cyan")
    pdf_table.add_column("Größe", justify="right")

    if docs_dir.exists():
        pdfs = sorted(docs_dir.glob("*.pdf"))
        if pdfs:
            for p in pdfs:
                size_mb = p.stat().st_size / (1024 * 1024)
                pdf_table.add_row(p.name, f"{size_mb:.1f} MB")
        else:
            pdf_table.add_row("[dim]—[/]", "[dim]Keine PDFs gefunden[/]")
    else:
        pdf_table.add_row("[dim]—[/]", "[dim]docs/ existiert nicht[/]")

    console.print(pdf_table)

    # Last log entries
    log_path = wiki_dir / "log.md"
    if log_path.exists():
        try:
            lines = log_path.read_text(encoding="utf-8").splitlines()
            table_lines = [l for l in lines if l.startswith("|") and "---" not in l]
            # skip header row
            data_lines = [l for l in table_lines if "Zeitstempel" not in l]
            if data_lines:
                console.print("\n[bold]Letzte Aktivitäten:[/]")
                for line in data_lines[-5:]:
                    console.print(f"  {line.strip()}")
        except Exception:
            pass

    # Paths summary
    console.print(f"\n[dim]DB:   {db_path}[/]")
    console.print(f"[dim]Wiki: {wiki_dir}[/]")
    console.print(f"[dim]Docs: {docs_dir}[/]")


# ---------------------------------------------------------------------------
# Command: download
# ---------------------------------------------------------------------------


def cmd_download(args: argparse.Namespace) -> None:
    """Download budget PDFs from the Bundeshaushalt download portal."""
    from scripts.download_budgets import run as download_run

    years = [args.year] if getattr(args, "year", None) else None
    output_dir = Path(getattr(args, "output_dir", "data/budgets"))
    download_run(
        years=years,
        output_dir=output_dir,
        list_only=getattr(args, "list_only", False),
        force=getattr(args, "force", False),
    )


# ---------------------------------------------------------------------------
# Command: interactive
# ---------------------------------------------------------------------------


def cmd_interactive(args: argparse.Namespace) -> None:
    """Enter interactive question mode."""
    console.print(
        Panel(
            "[bold]Interaktiver Modus[/]\n\n"
            "Stellen Sie Fragen zum Bundeshaushalt.\n"
            "Befehle:\n"
            "  [cyan]!sql <query>[/]    — SQL direkt ausführen\n"
            "  [cyan]!search <term>[/]  — Wiki durchsuchen\n"
            "  [cyan]!history[/]        — Gesprächsverlauf anzeigen\n"
            "  [cyan]!clear[/]          — Gesprächsverlauf löschen\n"
            "  [cyan]quit / exit[/]     — Beenden",
            title="Bundeshaushalt Q&A",
            border_style="blue",
        )
    )

    # Check if the query engine is available
    engine_available = True
    try:
        from src.query.engine import create_engine
    except ImportError:
        engine_available = False
        console.print(
            "[yellow]Hinweis: QueryEngine nicht verfügbar. "
            "Nur !sql und !search funktionieren.[/]\n"
        )

    # Conversation history — limited to the last 20 exchanges (40 msgs)
    _MAX_HISTORY_MESSAGES = 40
    conversation_history: list[dict[str, str]] = []

    while True:
        turn_count = len(conversation_history) // 2
        prompt = f"[bold blue][{turn_count}] Frage:[/] "
        try:
            question = console.input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Auf Wiedersehen![/]")
            break

        if not question:
            continue

        if question.lower() in ("quit", "exit", "q"):
            console.print("[dim]Auf Wiedersehen![/]")
            break

        # --- built-in commands (not added to history) ---

        if question.startswith("!sql "):
            _interactive_sql(question[5:].strip())
            continue

        if question.startswith("!search "):
            _display_search(question[8:].strip())
            continue

        if question.lower() == "!history":
            if not conversation_history:
                console.print("[dim]Noch kein Gesprächsverlauf.[/]")
            else:
                for i, msg in enumerate(conversation_history):
                    role_label = (
                        "[bold blue]Frage[/]" if msg["role"] == "user"
                        else "[bold green]Antwort[/]"
                    )
                    # Truncate long answers for display
                    content = msg["content"]
                    if len(content) > 200:
                        content = content[:200] + " …"
                    console.print(f"  {role_label}: {content}")
                console.print(
                    f"\n[dim]{turn_count} Austausch(e) im Verlauf.[/]"
                )
            continue

        if question.lower() == "!clear":
            conversation_history.clear()
            console.print("[dim]Gesprächsverlauf gelöscht.[/]")
            continue

        # --- regular question ---

        if not engine_available:
            console.print(
                "[yellow]QueryEngine nicht verfügbar. "
                "Verwende !sql oder !search.[/]"
            )
            continue

        try:
            result = _display_answer(
                question,
                conversation_history=conversation_history or None,
            )
            if result is not None:
                conversation_history.append(
                    {"role": "user", "content": question}
                )
                conversation_history.append(
                    {"role": "assistant", "content": result.answer}
                )
                # Trim to last N messages to stay within token limits
                if len(conversation_history) > _MAX_HISTORY_MESSAGES:
                    conversation_history[:] = conversation_history[
                        -_MAX_HISTORY_MESSAGES:
                    ]
        except SystemExit:
            # _display_answer raises SystemExit if engine is unavailable;
            # in interactive mode we continue instead of exiting.
            console.print("[yellow]Frage konnte nicht beantwortet werden.[/]")
        except Exception as exc:  # noqa: BLE001
            console.print(f"[bold red]Fehler:[/] {exc}")

        console.print()  # blank line between Q&A rounds


def _interactive_sql(sql: str) -> None:
    """Execute a raw SQL query and display results."""
    if not sql:
        console.print("[yellow]Bitte SQL angeben: !sql SELECT …[/]")
        return

    db_path = config.DB_PATH
    if not db_path.exists():
        console.print("[bold red]Datenbank nicht gefunden.[/] Bitte erst 'ingest' ausführen.")
        return

    try:
        from src.db.schema import get_connection
        conn = get_connection(db_path)
    except Exception as exc:
        console.print(f"[bold red]DB-Fehler:[/] {exc}")
        return

    try:
        cursor = conn.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        if not rows:
            console.print("[dim]Keine Ergebnisse.[/]")
        else:
            table = Table(border_style="blue")
            for col in columns:
                table.add_column(col, style="cyan")
            for row in rows[:100]:
                table.add_row(*(str(v) for v in row))
            console.print(table)
            if len(rows) > 100:
                console.print(f"[dim]… {len(rows) - 100} weitere Zeilen nicht angezeigt.[/]")
    except sqlite3.Error as exc:
        console.print(f"[bold red]SQL-Fehler:[/] {exc}")
        console.print(Syntax(sql, "sql", theme="monokai", line_numbers=False))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m src.cli",
        description="Bundeshaushalt Q&A — Fragen zum deutschen Bundeshaushalt beantworten",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Beispiele:
              python -m src.cli ingest docs/0350-25.pdf
              python -m src.cli query "Wie hoch sind die Ausgaben des Einzelplans 06?"
              python -m src.cli search "Verrechnungstitel"
              python -m src.cli status
              python -m src.cli interactive
        """),
    )

    subparsers = parser.add_subparsers(dest="command", help="Verfügbare Befehle")

    # ingest
    p_ingest = subparsers.add_parser(
        "ingest",
        help="PDF extrahieren, parsen und in Datenbank laden",
    )
    p_ingest.add_argument("pdf_path", help="Pfad zur Haushalts-PDF-Datei")
    p_ingest.add_argument(
        "--reset",
        action="store_true",
        help="Datenbank vor dem Laden zurücksetzen",
    )

    # query
    p_query = subparsers.add_parser(
        "query",
        help="Frage zum Bundeshaushalt stellen",
    )
    p_query.add_argument("question", nargs="+", help="Frage in natürlicher Sprache")

    # search
    p_search = subparsers.add_parser(
        "search",
        help="Wiki nach relevanten Seiten durchsuchen",
    )
    p_search.add_argument("term", nargs="+", help="Suchbegriff")
    p_search.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximale Anzahl Ergebnisse (Standard: 10)",
    )

    # lint
    subparsers.add_parser("lint", help="Wiki-Gesundheitsprüfung durchführen")

    # status
    subparsers.add_parser("status", help="Systemstatus anzeigen")

    # download
    p_download = subparsers.add_parser(
        "download",
        help="Budget-PDFs vom Downloadportal herunterladen",
    )
    p_download.add_argument(
        "-y",
        "--year",
        type=int,
        default=None,
        help="Nur ein bestimmtes Jahr herunterladen (2005–2026)",
    )
    p_download.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default="data/budgets",
        help="Ausgabeverzeichnis (Standard: data/budgets)",
    )
    p_download.add_argument(
        "-l",
        "--list-only",
        action="store_true",
        help="Nur verfügbare PDFs auflisten, nicht herunterladen",
    )
    p_download.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Vorhandene Dateien erneut herunterladen",
    )

    # ingest-all
    p_ingest_all = subparsers.add_parser(
        "ingest-all",
        help="Alle Budget-PDFs aus data/budgets/ einlesen",
    )
    p_ingest_all.add_argument(
        "--reset",
        action="store_true",
        help="Datenbank vorher zurücksetzen",
    )
    p_ingest_all.add_argument(
        "--year",
        type=int,
        help="Nur ein bestimmtes Jahr einlesen",
    )
    p_ingest_all.add_argument(
        "--skip-existing",
        action="store_true",
        help="Jahre mit vorhandenen Daten überspringen",
    )

    # interactive
    subparsers.add_parser("interactive", help="Interaktiven Fragemodus starten")

    return parser


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

_COMMANDS = {
    "ingest": cmd_ingest,
    "ingest-all": cmd_ingest_all,
    "query": cmd_query,
    "search": cmd_search,
    "lint": cmd_lint,
    "status": cmd_status,
    "download": cmd_download,
    "interactive": cmd_interactive,
}


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        raise SystemExit(0)

    handler = _COMMANDS.get(args.command)
    if handler is None:
        parser.print_help()
        raise SystemExit(1)

    try:
        handler(args)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        console.print("\n[dim]Abgebrochen.[/]")
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        console.print(f"[bold red]Unerwarteter Fehler:[/] {exc}")
        console.print_exception(show_locals=False)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
