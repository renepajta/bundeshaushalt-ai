"""Detect section types on budget PDF pages.

Classifies each page as one of:
- ueberblick: "Überblick zum Einzelplan/Kapitel" summary pages
- titel: Budget line item pages (Titel with amounts)
- personal: Personnel/Planstellen pages
- ve: Verpflichtungsermächtigungen pages
- vermerk: Haushaltsvermerke (budget notes/conditions)
- erlaeuterung: Erläuterungen (explanatory footnotes)
- gesamtplan: Gesamtplan overview/summary
- hg: Haushaltsgesetz (legal framework)
- vorspann: Table of contents, preamble
- other: Unclassified
"""

from __future__ import annotations

import re


def detect_section_type(text: str) -> str:
    """Classify a page's section type from its text content."""
    first_500 = text[:500] if text else ""
    first_200 = text[:200] if text else ""

    # Check patterns in priority order
    if re.search(r"Haushaltsgesetz", first_200):
        return "hg"
    if re.search(r"Gesamtplan", first_200):
        return "gesamtplan"
    if re.search(r"Inhaltsverzeichnis|Inhalt\s*$", first_200, re.MULTILINE):
        return "vorspann"
    if re.search(r"[ÜU]berblick\s+zum\s+(Einzelplan|Kapitel)", first_500):
        return "ueberblick"
    if re.search(r"Personalhaushalt|Planstellen|Stellen[üu]bersicht", first_500):
        return "personal"
    if re.search(r"Verpflichtungserm[äa]chtigung", first_500):
        return "ve"
    if re.search(r"Haushaltsvermerk", first_500):
        return "vermerk"
    if re.search(r"Erl[äa]uterung", first_500):
        return "erlaeuterung"
    if re.search(r"\d{3}\s+\d{2}", first_500):  # Titel pattern NNN NN
        return "titel"
    return "other"


def detect_einzelplan(text: str) -> str | None:
    """Extract Einzelplan number from page text."""
    m = re.search(r"Einzelplan\s+(\d{2})", text[:500])
    return m.group(1) if m else None


def detect_kapitel(text: str) -> str | None:
    """Extract Kapitel number from page text."""
    m = re.search(r"(?:Kapitel|Kap\.?)\s+(\d{4})", text[:500])
    if m:
        return m.group(1)
    # Footer pattern: 4-digit number alone on a line
    m = re.search(r"^(\d{4})\s*$", text, re.MULTILINE)
    return m.group(1) if m else None


def extract_heading(text: str) -> str:
    """Extract the first meaningful heading from page text."""
    lines = text.strip().split("\n")
    for line in lines[:10]:
        line = line.strip()
        if len(line) > 5 and not line.isdigit() and not line.startswith("-"):
            return line[:120]
    return ""
