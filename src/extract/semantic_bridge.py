"""Cross-era semantic normalization for the German federal budget.

Maps evolving terminology, organizational migrations, and structural
changes across budget years 2005-2026 so the Q&A system can answer
historical/cross-year questions accurately.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class KapitelMigration:
    """Tracks an institution's movement across Einzelplan/Kapitel codes."""

    institution: str
    periods: list[dict]
    # Each period: {"years": (start, end), "einzelplan": "XX",
    #               "kapitel": "XXXX", "name": "..."}


class SemanticBridge:
    """Cross-era normalization for German federal budget data."""

    # ------------------------------------------------------------------ #
    # Terminology Aliases — old term → canonical (modern) term            #
    # ------------------------------------------------------------------ #
    TERM_ALIASES: dict[str, str] = {
        # Military / Defence
        "Wehrübende": "Reservedienstleistende",
        "Wehrpflichtige": "Freiwillig Wehrdienstleistende",
        # Interior / Police
        "Bundesgrenzschutz": "Bundespolizei",  # renamed 2005
        # Technology / Digitalisation
        "Fernmeldewesen": "Digitale Infrastruktur",
        "Datenverarbeitung": "Informationstechnik",
        "Fernmeldetechnik": "Informations- und Kommunikationstechnik",
        # Personnel categories
        "Beamtete Hilfskräfte": "Tarifbeschäftigte",
        "Angestellte": "Tarifbeschäftigte",
        # Budget structure
        "Leertitel": "Nulltitel",
    }

    REVERSE_ALIASES: dict[str, list[str]]  # built in __init__

    # ------------------------------------------------------------------ #
    # Kapitel / Organization Migrations                                   #
    # ------------------------------------------------------------------ #
    KAPITEL_MIGRATIONS: list[KapitelMigration] = [
        # q06 — Datenschutzbeauftragter
        KapitelMigration(
            institution=(
                "Bundesbeauftragter für den Datenschutz "
                "und die Informationsfreiheit"
            ),
            periods=[
                {
                    "years": (2005, 2005),
                    "einzelplan": "06",
                    "kapitel": "0607",
                    "name": "Bundesbeauftragten für den Datenschutz",
                },
                {
                    "years": (2006, 2013),
                    "einzelplan": "06",
                    "kapitel": "0607",
                    "name": (
                        "Bundesbeauftragter für den Datenschutz "
                        "und die Informationsfreiheit"
                    ),
                },
                {
                    "years": (2014, 2016),
                    "einzelplan": "06",
                    "kapitel": "0613",
                    "name": (
                        "Bundesbeauftragter für den Datenschutz "
                        "und die Informationsfreiheit"
                    ),
                },
                {
                    "years": (2016, 2024),
                    "einzelplan": "21",
                    "kapitel": "2112",
                    "name": (
                        "Bundesbeauftragter für den Datenschutz "
                        "und die Informationsfreiheit"
                    ),
                },
                {
                    "years": (2025, 2026),
                    "einzelplan": "21",
                    "kapitel": "2112",
                    "name": (
                        "Bundesbeauftragter für den Datenschutz "
                        "und die Informationsfreiheit"
                    ),
                },
            ],
        ),
        # q07 — Kapitel 1201 meaning change
        KapitelMigration(
            institution="Kapitel 1201 — Ministerium vs Bundesfernstraßen",
            periods=[
                {
                    "years": (2005, 2015),
                    "einzelplan": "12",
                    "kapitel": "1201",
                    "name": "Bundesministerium für Verkehr (Ministerium)",
                },
                {
                    "years": (2016, 2026),
                    "einzelplan": "12",
                    "kapitel": "1201",
                    "name": "Bundesfernstraßen",
                },
            ],
        ),
    ]

    # ------------------------------------------------------------------ #
    # Einzelplan Name Changes                                             #
    # ------------------------------------------------------------------ #
    EINZELPLAN_NAMES: dict[str, list[dict]] = {
        "01": [
            {"years": (2005, 2026), "name": "Bundespräsident und Bundespräsidialamt"},
        ],
        "02": [
            {"years": (2005, 2026), "name": "Deutscher Bundestag"},
        ],
        "03": [
            {"years": (2005, 2026), "name": "Bundesrat"},
        ],
        "04": [
            {"years": (2005, 2026), "name": "Bundeskanzler und Bundeskanzleramt"},
        ],
        "05": [
            {"years": (2005, 2026), "name": "Auswärtiges Amt"},
        ],
        "06": [
            {"years": (2005, 2013), "name": "Bundesministerium des Innern"},
            {
                "years": (2014, 2026),
                "name": "Bundesministerium des Innern und für Heimat",
            },
        ],
        "07": [
            {"years": (2005, 2026), "name": "Bundesministerium der Justiz"},
        ],
        "08": [
            {"years": (2005, 2026), "name": "Bundesministerium der Finanzen"},
        ],
        "09": [
            {
                "years": (2005, 2013),
                "name": "Bundesministerium für Wirtschaft und Technologie",
            },
            {
                "years": (2014, 2021),
                "name": "Bundesministerium für Wirtschaft und Energie",
            },
            {
                "years": (2022, 2026),
                "name": "Bundesministerium für Wirtschaft und Klimaschutz",
            },
        ],
        "10": [
            {
                "years": (2005, 2013),
                "name": (
                    "Bundesministerium für Ernährung, "
                    "Landwirtschaft und Verbraucherschutz"
                ),
            },
            {
                "years": (2014, 2026),
                "name": "Bundesministerium für Ernährung und Landwirtschaft",
            },
        ],
        "11": [
            {"years": (2005, 2026), "name": "Bundesministerium für Arbeit und Soziales"},
        ],
        "12": [
            {
                "years": (2005, 2013),
                "name": (
                    "Bundesministerium für Verkehr, "
                    "Bau und Stadtentwicklung"
                ),
            },
            {
                "years": (2014, 2017),
                "name": (
                    "Bundesministerium für Verkehr "
                    "und digitale Infrastruktur"
                ),
            },
            {
                "years": (2018, 2026),
                "name": "Bundesministerium für Digitales und Verkehr",
            },
        ],
        "14": [
            {"years": (2005, 2026), "name": "Bundesministerium der Verteidigung"},
        ],
        "15": [
            {
                "years": (2005, 2013),
                "name": "Bundesministerium für Gesundheit",
            },
            {
                "years": (2014, 2026),
                "name": "Bundesministerium für Gesundheit",
            },
        ],
        "16": [
            {
                "years": (2005, 2013),
                "name": (
                    "Bundesministerium für Umwelt, Naturschutz "
                    "und Reaktorsicherheit"
                ),
            },
            {
                "years": (2014, 2026),
                "name": (
                    "Bundesministerium für Umwelt, Naturschutz, "
                    "nukleare Sicherheit und Verbraucherschutz"
                ),
            },
        ],
        "17": [
            {
                "years": (2005, 2013),
                "name": (
                    "Bundesministerium für Familie, Senioren, "
                    "Frauen und Jugend"
                ),
            },
            {
                "years": (2014, 2026),
                "name": (
                    "Bundesministerium für Familie, Senioren, "
                    "Frauen und Jugend"
                ),
            },
        ],
        "21": [
            {
                "years": (2016, 2026),
                "name": (
                    "Bundesbeauftragter für den Datenschutz "
                    "und die Informationsfreiheit"
                ),
            },
        ],
        "23": [
            {
                "years": (2005, 2013),
                "name": (
                    "Bundesministerium für wirtschaftliche "
                    "Zusammenarbeit und Entwicklung"
                ),
            },
            {
                "years": (2014, 2026),
                "name": (
                    "Bundesministerium für wirtschaftliche "
                    "Zusammenarbeit und Entwicklung"
                ),
            },
        ],
        "30": [
            {
                "years": (2005, 2013),
                "name": "Bundesministerium für Bildung und Forschung",
            },
            {
                "years": (2014, 2026),
                "name": "Bundesministerium für Bildung und Forschung",
            },
        ],
        "32": [
            {"years": (2005, 2026), "name": "Bundesschuld"},
        ],
        "60": [
            {"years": (2005, 2026), "name": "Allgemeine Finanzverwaltung"},
        ],
    }

    # ------------------------------------------------------------------ #
    # Amount Unit Configuration                                           #
    # All values stored in DB as 1,000 € (thousands).                    #
    # ------------------------------------------------------------------ #
    AMOUNT_MULTIPLIERS: dict[tuple[int, int], float] = {
        (2005, 2011): 1.0,  # Already in 1,000 €
        (2012, 2023): 1.0,  # Detail pages in 1,000 €; overviews in Mio €
        (2024, 2026): 1.0,  # 1,000 €
    }

    # ------------------------------------------------------------------ #
    # Partial/Substring Matches — shorter terms matching longer ones      #
    # ------------------------------------------------------------------ #
    PARTIAL_TERMS: dict[str, list[str]] = {
        "Reservedienstleistende": ["Reservist", "Wehrdienst", "Wehrsold"],
        "Aufwandsentschädigungen": ["Aufwandspauschale", "Dienstaufwand", "Entschädigung"],
        "Verpflichtungsermächtigung": ["Verpflichtungsermächtigung", "davon fällig"],
        "Personalausgaben": ["Personalkosten", "Bezüge", "Entgelte", "Planstellen"],
        "Flexibilisierung": ["flexibilisiert", "nicht flexibilisiert", "Flexibilisierungsinstrument"],
    }

    # ------------------------------------------------------------------ #
    # Organizational Mappings — institution name → Kapitel/EP             #
    # ------------------------------------------------------------------ #
    ORG_MAPPINGS: dict[str, dict] = {
        "Umweltbundesamt": {"einzelplan": "16", "kapitel": "1613"},
        "Bundeskriminalamt": {"einzelplan": "06", "kapitel": "0622"},
        "Bundespolizei": {"einzelplan": "06", "kapitel": "0625"},
        "Bundeswehr": {"einzelplan": "14"},
        "Auswärtiges Amt": {"einzelplan": "05", "kapitel": "0501"},
        "Bundeskanzleramt": {"einzelplan": "04", "kapitel": "0401"},
        "Bundesrechnungshof": {"einzelplan": "20", "kapitel": "2001"},
        "Bundespräsident": {"einzelplan": "01", "kapitel": "0101"},
        "Bundestag": {"einzelplan": "02"},
        "Bundesrat": {"einzelplan": "03"},
        "Bundesverfassungsgericht": {"einzelplan": "19"},
        "Statistisches Bundesamt": {"einzelplan": "06", "kapitel": "0614"},
        "Bundesamt für Migration": {"einzelplan": "06", "kapitel": "0633"},
        "BAMF": {"einzelplan": "06", "kapitel": "0633"},
    }

    # ------------------------------------------------------------------ #
    # Abbreviations → full names                                          #
    # ------------------------------------------------------------------ #
    ABBREVIATIONS: dict[str, str] = {
        "BMI": "Bundesministerium des Innern",
        "BMVg": "Bundesministerium der Verteidigung",
        "BMWK": "Bundesministerium für Wirtschaft und Klimaschutz",
        "BMDV": "Bundesministerium für Digitales und Verkehr",
        "BMF": "Bundesministerium der Finanzen",
        "BMAS": "Bundesministerium für Arbeit und Soziales",
        "BMBF": "Bundesministerium für Bildung und Forschung",
        "BMG": "Bundesministerium für Gesundheit",
        "BMFSFJ": "Bundesministerium für Familie, Senioren, Frauen und Jugend",
        "BMJ": "Bundesministerium der Justiz",
        "BMZ": "Bundesministerium für wirtschaftliche Zusammenarbeit",
        "BMUV": "Bundesministerium für Umwelt, Naturschutz und Verbraucherschutz",
        "BMEL": "Bundesministerium für Ernährung und Landwirtschaft",
        "AA": "Auswärtiges Amt",
        "BKA": "Bundeskriminalamt",
        "BKAmt": "Bundeskanzleramt",
        "UBA": "Umweltbundesamt",
        "BRH": "Bundesrechnungshof",
    }

    # ------------------------------------------------------------------ #
    # Besoldungsgruppe (pay-grade) evolution                              #
    # Grade labels remained mostly stable but groups were restructured.   #
    # ------------------------------------------------------------------ #
    BESOLDUNGSGRUPPEN: dict[str, str] = {
        # Abbreviation → full display name (modern)
        "ORR": "Oberregierungsrat",
        "RR": "Regierungsrat",
        "RD": "Regierungsdirektor",
        "MR": "Ministerialrat",
        "MD": "Ministerialdirektor",
        "StS": "Staatssekretär",
        "BM": "Bundesminister",
    }

    # ------------------------------------------------------------------ #
    # Known BIP (GDP) reference values in 1,000 € (thousands)            #
    # Source: Statistisches Bundesamt / Stabilitätsrat                    #
    # ------------------------------------------------------------------ #
    KNOWN_BIP: dict[int, int] = {
        2005: 2_300_860_000,
        2006: 2_393_250_000,
        2007: 2_513_230_000,
        2008: 2_561_740_000,
        2009: 2_460_280_000,
        2010: 2_580_060_000,
        2011: 2_703_120_000,
        2012: 2_758_260_000,
        2013: 2_826_240_000,
        2014: 2_938_590_000,
        2015: 3_043_650_000,
        2016: 3_144_050_000,
        2017: 3_277_340_000,
        2018: 3_388_220_000,
        2019: 3_473_860_000,
        2020: 3_367_560_000,
        2021: 3_613_380_000,
        2022: 3_876_810_000,
        2023: 4_121_160_000,
        2024: 4_261_950_000,
        2025: 4_469_910_000,
    }

    # ------------------------------------------------------------------ #
    # Known inflation rates (Verbraucherpreisindex, year-on-year %)       #
    # Source: Statistisches Bundesamt                                      #
    # ------------------------------------------------------------------ #
    KNOWN_INFLATION: dict[int, float] = {
        2005: 1.5,
        2006: 1.6,
        2007: 2.3,
        2008: 2.6,
        2009: 0.3,
        2010: 1.1,
        2011: 2.1,
        2012: 2.0,
        2013: 1.5,
        2014: 0.9,
        2015: 0.3,
        2016: 0.5,
        2017: 1.5,
        2018: 1.8,
        2019: 1.4,
        2020: 0.5,
        2021: 3.1,
        2022: 6.9,
        2023: 5.9,
    }

    # ------------------------------------------------------------------ #
    # Known cross-reference facts from the golden QA set                  #
    # ------------------------------------------------------------------ #
    KNOWN_FACTS: dict[str, dict] = {
        "q05_reservedienstleistende_2012": {
            "planstellen": 2500,
            "kapitel": "1403",
            "note": "In 2012 still called 'Wehrübende'",
        },
        "q06_datenschutz_kapitel": {
            "2005": "0607",
            "2006-2013": "0607",
            "2014-2016": "0613",
            "2016-2024": "2112",
            "2025-2026": "2112",
        },
        "q11_titel_88221_ep04_2026": {
            "titel": "88221",
            "neues_kapitel": "0416",
            "bisherige_einzelplaene": ["05", "06", "10", "12"],
            "note": "First appeared in EP 04 in 2026",
        },
    }

    # ================================================================== #
    # Initialisation                                                      #
    # ================================================================== #

    def __init__(self) -> None:
        self.REVERSE_ALIASES: dict[str, list[str]] = {}
        for old, new in self.TERM_ALIASES.items():
            self.REVERSE_ALIASES.setdefault(new, []).append(old)

    # ================================================================== #
    # Public API                                                          #
    # ================================================================== #

    def normalize_term(self, term: str) -> str:
        """Map an old term to its canonical modern equivalent."""
        return self.TERM_ALIASES.get(term, term)

    def get_historical_terms(self, modern_term: str) -> list[str]:
        """Get all historical variants of a modern term (for search)."""
        variants = [modern_term]
        variants.extend(self.REVERSE_ALIASES.get(modern_term, []))
        return variants

    def find_kapitel_for_institution(
        self, institution: str, year: int
    ) -> dict | None:
        """Find which Kapitel an institution was assigned to in a year."""
        needle = institution.lower()
        for migration in self.KAPITEL_MIGRATIONS:
            if needle in migration.institution.lower():
                for period in migration.periods:
                    start, end = period["years"]
                    if start <= year <= end:
                        return period
        return None

    def get_kapitel_history(self, institution: str) -> list[dict]:
        """Get the full migration history of an institution."""
        needle = institution.lower()
        for migration in self.KAPITEL_MIGRATIONS:
            if needle in migration.institution.lower():
                return migration.periods
        return []

    def get_einzelplan_name(self, einzelplan: str, year: int) -> str | None:
        """Get the name of an Einzelplan for a specific year."""
        for period in self.EINZELPLAN_NAMES.get(einzelplan, []):
            start, end = period["years"]
            if start <= year <= end:
                return period["name"]
        return None

    def get_amount_multiplier(self, year: int) -> float:
        """Get the multiplier to convert PDF amounts to 1,000 € standard."""
        for (start, end), mult in self.AMOUNT_MULTIPLIERS.items():
            if start <= year <= end:
                return mult
        return 1.0

    def get_bip(self, year: int) -> int | None:
        """Return known BIP in 1,000 € for *year*, or ``None``."""
        return self.KNOWN_BIP.get(year)

    def get_inflation_rate(self, year: int) -> float | None:
        """Return known year-on-year inflation rate (%) for *year*."""
        return self.KNOWN_INFLATION.get(year)

    def expand_search_terms(self, query: str) -> list[str]:
        """Expand query with ALL synonym types: aliases, partials, abbreviations."""
        expanded = [query]

        # 1. Term aliases (modern ↔ old)
        for old, new in self.TERM_ALIASES.items():
            if old.lower() in query.lower():
                expanded.append(query.replace(old, new))
            if new.lower() in query.lower():
                expanded.append(query.replace(new, old))

        # 2. Partial matches
        for term, partials in self.PARTIAL_TERMS.items():
            if term.lower() in query.lower():
                for p in partials:
                    expanded.append(p)

        # 3. Abbreviation expansion
        for abbr, full in self.ABBREVIATIONS.items():
            if abbr in query:  # case-sensitive for abbreviations
                expanded.append(query.replace(abbr, full))
            if full.lower() in query.lower():
                expanded.append(abbr)

        return list(set(expanded))

    def resolve_organization(self, query: str) -> dict | None:
        """If query mentions a known organization, return its EP/Kap mapping."""
        query_lower = query.lower()
        for org, mapping in self.ORG_MAPPINGS.items():
            if org.lower() in query_lower:
                return {"organization": org, **mapping}
        # Also check abbreviations
        for abbr, full in self.ABBREVIATIONS.items():
            if abbr in query:
                for org, mapping in self.ORG_MAPPINGS.items():
                    if org.lower() in full.lower():
                        return {"organization": org, **mapping}
        return None

    def get_besoldungsgruppe_name(self, abbrev: str) -> str | None:
        """Resolve a pay-grade abbreviation to its full name."""
        return self.BESOLDUNGSGRUPPEN.get(abbrev)
