#!/usr/bin/env python3
"""Download budget PDFs (and ZIPs) from the German federal budget website.

Navigates https://www.bundeshaushalt.de/DE/Download-Portal/download-portal.html
using Playwright to scrape PDF/ZIP links, then downloads them with requests.
ZIP files (used for years 2005-2011) are automatically extracted and removed.

Usage:
    python scripts/download_budgets.py
    python scripts/download_budgets.py --year 2024
    python scripts/download_budgets.py --list-only
    python scripts/download_budgets.py --force
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TransferSpeedColumn,
)
from rich.table import Table

console = Console()

PORTAL_URL = (
    "https://www.bundeshaushalt.de/DE/Download-Portal/download-portal.html"
)
DEFAULT_OUTPUT_DIR = Path("data/budgets")
FIRST_YEAR = 2005
LAST_YEAR = 2026
DOWNLOAD_CHUNK_SIZE = 128 * 1024  # 128 KB


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class DownloadLink:
    """A single downloadable file (PDF or ZIP) discovered on the portal."""

    year: int
    url: str
    label: str
    size_text: str = ""

    @property
    def is_zip(self) -> bool:
        return self.url.lower().endswith(".zip")

    @property
    def filename(self) -> str:
        """Derive a safe filename from the URL."""
        path = urlparse(self.url).path
        name = unquote(Path(path).name)
        expected_ext = ".zip" if self.is_zip else ".pdf"
        if not name or not name.endswith(expected_ext):
            safe = re.sub(r"[^\w\-.]", "_", self.label)[:120]
            name = f"{safe}{expected_ext}"
        return name


# Keep backward-compatible alias
PdfLink = DownloadLink


# ---------------------------------------------------------------------------
# Scraping with Playwright
# ---------------------------------------------------------------------------


def scrape_pdf_links(
    years: list[int] | None = None,
) -> list[DownloadLink]:
    """Use Playwright to navigate the download portal and collect PDF/ZIP links."""
    from playwright.sync_api import sync_playwright

    if years is None:
        years = list(range(FIRST_YEAR, LAST_YEAR + 1))

    all_links: list[DownloadLink] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        console.print(f"[dim]Navigiere zu {PORTAL_URL}[/]")
        page.goto(PORTAL_URL, wait_until="networkidle", timeout=60_000)

        # Dismiss cookie banner
        _dismiss_cookie_banner(page)

        for year in years:
            console.print(f"[bold cyan]Jahr {year}[/] — suche Dateien …")
            links = _scrape_year(page, year)
            all_links.extend(links)
            zips = sum(1 for l in links if l.is_zip)
            pdfs = len(links) - zips
            parts = []
            if pdfs:
                parts.append(f"{pdfs} PDF(s)")
            if zips:
                parts.append(f"{zips} ZIP(s)")
            console.print(f"  → {' + '.join(parts) or '0 Dateien'} gefunden")

        browser.close()

    return all_links


def _dismiss_cookie_banner(page) -> None:
    """Click the cookie-consent button if present."""
    try:
        btn = page.locator("button", has_text="Auswahl bestätigen").first
        btn.wait_for(state="visible", timeout=8_000)
        btn.click()
        page.wait_for_timeout(1_000)
        console.print("[dim]Cookie-Banner geschlossen.[/]")
    except Exception:
        console.print("[dim]Kein Cookie-Banner gefunden – weiter.[/]")


def _scrape_year(page, year: int) -> list[DownloadLink]:
    """Select a year in the portal dropdown, then collect PDF and ZIP links."""
    links: list[DownloadLink] = []

    try:
        # The portal has a year-selector widget. Try clicking the year button
        # directly — the years are typically rendered as buttons or links.
        year_btn = page.locator(
            f"button:has-text('{year}'), a:has-text('{year}')"
        )

        # Sometimes the dropdown needs to be opened first.  Look for the
        # currently-active year button / dropdown trigger and click it so the
        # year list becomes visible.
        dropdown_trigger = page.locator(
            "[class*='year'] button, [class*='dropdown'] button, "
            "button[class*='select'], button[aria-haspopup]"
        ).first
        try:
            dropdown_trigger.wait_for(state="visible", timeout=3_000)
            dropdown_trigger.click()
            page.wait_for_timeout(500)
        except Exception:
            pass  # dropdown might already be open or structured differently

        # Now click the specific year
        try:
            year_btn.first.wait_for(state="visible", timeout=5_000)
            year_btn.first.click()
        except Exception:
            # Fallback: try text-exact match
            page.get_by_role("button", name=str(year), exact=True).click()

        # Wait for the document list to populate
        page.wait_for_timeout(2_000)
        try:
            page.locator("a[href*='.pdf'], a[href*='.zip']").first.wait_for(
                state="visible", timeout=8_000
            )
        except Exception:
            pass

        # Collect all PDF and ZIP links
        anchors = page.locator("a[href*='.pdf'], a[href*='.zip']")
        count = anchors.count()

        for i in range(count):
            anchor = anchors.nth(i)
            href = anchor.get_attribute("href") or ""
            label = anchor.inner_text().strip()

            if not (href.endswith(".pdf") or href.endswith(".zip")):
                continue

            # Make absolute URL
            if href.startswith("/"):
                href = f"https://www.bundeshaushalt.de{href}"

            # Try to grab the size info shown near the link
            size_text = ""
            try:
                parent = anchor.locator("..")
                parent_text = parent.inner_text()
                size_match = re.search(
                    r"(\d[\d.,]*\s*(?:KB|MB|GB))", parent_text, re.IGNORECASE
                )
                if size_match:
                    size_text = size_match.group(1)
            except Exception:
                pass

            links.append(
                DownloadLink(year=year, url=href, label=label, size_text=size_text)
            )

    except Exception as exc:
        console.print(f"  [yellow]⚠ Fehler bei Jahr {year}: {exc}[/]")

    return links


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------


def download_pdfs(
    links: list[DownloadLink],
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    force: bool = False,
) -> tuple[int, int]:
    """Download PDF/ZIP files. ZIPs are extracted and removed. Returns (success, errors)."""
    ok = 0
    errors = 0

    with Progress(
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        DownloadColumn(),
        TransferSpeedColumn(),
        console=console,
    ) as progress:
        for link in links:
            year_dir = output_dir / str(link.year)
            year_dir.mkdir(parents=True, exist_ok=True)
            dest = year_dir / link.filename

            # For ZIPs: skip if already extracted (PDFs exist, no ZIP left)
            if link.is_zip and not force:
                existing_pdfs = list(year_dir.glob("*.pdf"))
                if existing_pdfs and not dest.exists():
                    console.print(
                        f"  [dim]⏭  {link.year}/ bereits entpackt "
                        f"({len(existing_pdfs)} PDFs)[/]"
                    )
                    ok += 1
                    continue

            if dest.exists() and not force:
                console.print(
                    f"  [dim]⏭  {link.filename} existiert bereits[/]"
                )
                ok += 1
                continue

            try:
                _download_file(link.url, dest, progress)

                # Extract ZIPs after download
                if link.is_zip:
                    _extract_zip(dest, year_dir)

                ok += 1
            except Exception as exc:
                console.print(
                    f"  [red]✗ {link.filename}: {exc}[/]"
                )
                errors += 1

    return ok, errors


def _extract_zip(zip_path: Path, dest_dir: Path) -> None:
    """Extract PDF files from a ZIP archive, then delete the ZIP."""
    extracted = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            if member.lower().endswith(".pdf"):
                # Flatten: extract into dest_dir regardless of ZIP internal paths
                member_name = Path(member).name
                target = dest_dir / member_name
                with zf.open(member) as src, open(target, "wb") as dst:
                    dst.write(src.read())
                extracted.append(member_name)

    if extracted:
        console.print(
            f"  [green]📦 {len(extracted)} PDF(s) entpackt aus {zip_path.name}[/]"
        )
        for name in sorted(extracted):
            console.print(f"     [dim]→ {name}[/]")
    else:
        console.print(
            f"  [yellow]⚠ Keine PDFs in {zip_path.name} gefunden[/]"
        )

    # Remove the ZIP to save space
    zip_path.unlink()
    console.print(f"  [dim]🗑  {zip_path.name} gelöscht[/]")


def _download_file(
    url: str, dest: Path, progress: Progress, max_retries: int = 3
) -> None:
    """Stream-download a single file with progress tracking and retries."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                url,
                stream=True,
                timeout=(30, 600),  # (connect, read) — 10 min read for large files
            )
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0))
            task = progress.add_task(
                "download",
                total=total or None,
                filename=dest.name,
            )

            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    fh.write(chunk)
                    progress.update(task, advance=len(chunk))

            progress.update(
                task, completed=total or progress.tasks[task].completed
            )
            return  # success
        except Exception as exc:
            # Remove partial file
            if dest.exists():
                dest.unlink()
            if attempt < max_retries:
                console.print(
                    f"  [yellow]⚠ Versuch {attempt}/{max_retries} fehlgeschlagen: "
                    f"{exc} — erneuter Versuch …[/]"
                )
                time.sleep(2 * attempt)
            else:
                raise


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def print_link_table(links: list[DownloadLink]) -> None:
    """Pretty-print discovered PDF/ZIP links."""
    table = Table(title="Verfügbare Dateien", show_lines=False)
    table.add_column("Jahr", style="cyan", width=6)
    table.add_column("Typ", width=5)
    table.add_column("Dateiname", style="bold")
    table.add_column("Größe", justify="right")
    table.add_column("URL", style="dim", max_width=60)

    for link in links:
        ftype = "[yellow]ZIP[/]" if link.is_zip else "PDF"
        table.add_row(
            str(link.year),
            ftype,
            link.filename,
            link.size_text or "—",
            link.url,
        )

    zips = sum(1 for l in links if l.is_zip)
    pdfs = len(links) - zips
    console.print(table)
    console.print(
        f"\n[bold]Gesamt:[/] {len(links)} Datei(en) — {pdfs} PDF(s), {zips} ZIP(s)"
    )


# ---------------------------------------------------------------------------
# Main logic (importable from CLI)
# ---------------------------------------------------------------------------


def run(
    years: list[int] | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    list_only: bool = False,
    force: bool = False,
) -> None:
    """Top-level entry point for the download workflow."""
    console.print(
        Panel(
            "[bold]Bundeshaushalt PDF/ZIP-Download[/]\n"
            f"Portal: {PORTAL_URL}",
            title="Download-Portal",
            border_style="blue",
        )
    )

    links = scrape_pdf_links(years)

    if not links:
        console.print("[yellow]Keine Dateien gefunden.[/]")
        return

    print_link_table(links)

    if list_only:
        return

    console.print()
    ok, errs = download_pdfs(links, output_dir=output_dir, force=force)
    console.print(
        f"\n[bold green]✓ {ok} heruntergeladen[/]"
        + (f", [bold red]{errs} Fehler[/]" if errs else "")
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_argparser() -> argparse.ArgumentParser:
    """Build the standalone argument parser."""
    parser = argparse.ArgumentParser(
        description="Budget-PDFs/ZIPs vom Bundeshaushalt-Downloadportal herunterladen",
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        default=None,
        help=f"Nur ein bestimmtes Jahr herunterladen ({FIRST_YEAR}–{LAST_YEAR})",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Ausgabeverzeichnis (Standard: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "-l",
        "--list-only",
        action="store_true",
        help="Nur verfügbare Dateien auflisten, nicht herunterladen",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Vorhandene Dateien erneut herunterladen",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Standalone CLI entry point."""
    parser = build_argparser()
    args = parser.parse_args(argv)

    years = [args.year] if args.year else None
    run(
        years=years,
        output_dir=args.output_dir,
        list_only=args.list_only,
        force=args.force,
    )


if __name__ == "__main__":
    main()
