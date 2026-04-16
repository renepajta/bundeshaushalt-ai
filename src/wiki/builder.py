"""Wiki page generator for the Bundeshaushalt Q&A system.

Builds Markdown wiki pages from structured database content and expert
knowledge, following the conventions defined in SCHEMA.md.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from src.config import config
from src.db.schema import get_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LLM helper — lazy-loaded so the module works without Azure credentials
# when only building concept pages.
# ---------------------------------------------------------------------------

_llm_client = None


def _get_llm():
    """Return a shared LLMClient instance, created on first call."""
    global _llm_client
    if _llm_client is None:
        from src.query.llm import LLMClient
        _llm_client = LLMClient()
    return _llm_client


# ---------------------------------------------------------------------------
# Frontmatter helper
# ---------------------------------------------------------------------------

def _frontmatter(fields: dict) -> str:
    """Render a YAML frontmatter block from a dict of fields."""
    lines = ["---"]
    for key, value in fields.items():
        if isinstance(value, list):
            items = ", ".join(str(v) for v in value)
            lines.append(f"{key}: [{items}]")
        elif isinstance(value, str):
            lines.append(f'{key}: "{value}"')
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    return "\n".join(lines)


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _slugify(text: str) -> str:
    """Create a kebab-case slug from German text (resolving umlauts)."""
    text = text.lower()
    for src, dst in [("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")]:
        text = text.replace(src, dst)
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


# ---------------------------------------------------------------------------
# WikiBuilder
# ---------------------------------------------------------------------------


class WikiBuilder:
    """Generate wiki pages from the Bundeshaushalt SQLite database."""

    def __init__(self, wiki_dir: Path | None = None, db_path: Path | None = None):
        self.wiki_dir = Path(wiki_dir or config.WIKI_DIR)
        self.db_path = Path(db_path or config.DB_PATH)
        # Ensure subdirectories exist
        for sub in ("entities", "concepts", "analyses"):
            (self.wiki_dir / sub).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def build_all(self, source_file: str = "") -> list[Path]:
        """Build / rebuild all wiki pages.  Returns list of created files."""
        created: list[Path] = []

        # Concept pages (no LLM, no DB needed)
        created.extend(self.build_concept_pages())

        # Database-backed pages
        try:
            conn = get_connection(self.db_path)
        except FileNotFoundError:
            logger.warning("Database not found at %s — skipping DB pages.", self.db_path)
            conn = None

        if conn is not None:
            try:
                created.extend(self._build_entity_pages(conn))
                overview = self.build_overview_page(conn)
                if overview:
                    created.append(overview)
            finally:
                conn.close()

        # Index
        from src.wiki.indexer import WikiIndexer
        indexer = WikiIndexer(self.wiki_dir)
        indexer.rebuild_index()
        indexer.append_log("build", f"build_all – {len(created)} pages"
                          + (f" (source: {source_file})" if source_file else ""))

        logger.info("Wiki build complete: %d pages created/updated.", len(created))
        return created

    # ------------------------------------------------------------------
    # Entity pages
    # ------------------------------------------------------------------

    def _build_entity_pages(self, conn: sqlite3.Connection) -> list[Path]:
        """Build entity pages for every Einzelplan found in the database."""
        rows = conn.execute(
            "SELECT DISTINCT einzelplan FROM einzelplan_meta ORDER BY einzelplan"
        ).fetchall()
        created = []
        for row in rows:
            ep = row["einzelplan"] if isinstance(row, sqlite3.Row) else row[0]
            path = self.build_einzelplan_page(ep, conn)
            if path:
                created.append(path)
        return created

    def build_einzelplan_page(self, einzelplan: str, conn: sqlite3.Connection) -> Path | None:
        """Generate a wiki page for a specific Einzelplan."""
        ep = einzelplan.zfill(2)

        # -- Gather metadata ------------------------------------------------
        meta_rows = conn.execute(
            "SELECT year, version, einzelplan_text FROM einzelplan_meta "
            "WHERE einzelplan = ? ORDER BY year, version",
            (ep,),
        ).fetchall()
        if not meta_rows:
            logger.debug("No metadata for Einzelplan %s – skipping.", ep)
            return None

        ep_name = meta_rows[0]["einzelplan_text"] or f"Einzelplan {ep}"
        years_set: set[int] = {r["year"] for r in meta_rows}
        years = sorted(years_set)

        # -- Aggregate Soll/Ist per year ------------------------------------
        agg_rows = conn.execute(
            "SELECT year, version, "
            "  SUM(ausgaben_soll) AS total_soll, "
            "  SUM(ausgaben_ist) AS total_ist "
            "FROM haushaltsdaten "
            "WHERE einzelplan = ? "
            "GROUP BY year, version ORDER BY year, version",
            (ep,),
        ).fetchall()

        eckwerte_lines = []
        for r in agg_rows:
            soll = f"{r['total_soll']:,.0f}" if r["total_soll"] else "–"
            ist = f"{r['total_ist']:,.0f}" if r["total_ist"] else "–"
            eckwerte_lines.append(f"| {r['year']} | {r['version']} | {soll} | {ist} |")

        # -- Key Kapitel ----------------------------------------------------
        kapitel_rows = conn.execute(
            "SELECT DISTINCT k.kapitel, k.kapitel_text "
            "FROM kapitel_meta k "
            "WHERE k.einzelplan = ? ORDER BY k.kapitel",
            (ep,),
        ).fetchall()
        kapitel_links = []
        for kr in kapitel_rows:
            kap = kr["kapitel"]
            ktext = kr["kapitel_text"] or kap
            kapitel_links.append(f"- [Kapitel {kap}](kapitel-{kap}.md) — {ktext}")

        # -- Use LLM for a rich overview if available -----------------------
        overview_text = ""
        try:
            data_summary = (
                f"Einzelplan {ep}: {ep_name}\n"
                f"Jahre: {', '.join(str(y) for y in years)}\n"
                f"Kapitel: {len(kapitel_rows)}\n"
            )
            for r in agg_rows:
                data_summary += (
                    f"  {r['year']} ({r['version']}): "
                    f"Soll={r['total_soll']} T€, Ist={r['total_ist']} T€\n"
                )
            llm = _get_llm()
            overview_text = llm.chat_with_system(
                "Du bist ein Experte für den deutschen Bundeshaushalt. "
                "Verfasse einen kurzen Überblick (3-5 Sätze, auf Deutsch) "
                "über den genannten Einzelplan. Nutze die bereitgestellten Zahlen.",
                data_summary,
                temperature=0.3,
            )
        except Exception as exc:
            logger.warning("LLM overview generation failed: %s", exc)
            overview_text = f"{ep_name} umfasst die Ausgaben und Einnahmen des zugehörigen Ressorts."

        # -- Compose page ---------------------------------------------------
        tags = ["einzelplan", _slugify(ep_name.split("—")[-1].strip() if "—" in ep_name else ep_name)]
        fm = _frontmatter({
            "title": f"Einzelplan {ep} — {ep_name}",
            "type": "entity",
            "einzelplan": ep,
            "tags": tags,
            "years": years,
            "last_updated": _today(),
        })

        body_parts = [fm, "", "## Überblick", "", overview_text, ""]

        if kapitel_links:
            body_parts += ["## Struktur", ""] + kapitel_links + [""]

        if eckwerte_lines:
            body_parts += [
                "## Eckwerte",
                "",
                "| Jahr | Version | Soll (T€) | Ist (T€) |",
                "|------|---------|-----------|----------|",
            ] + eckwerte_lines + [
                "",
                "> Hinweis: Angaben einschließlich Verrechnungstitel.",
                "",
            ]

        body_parts += [
            "## Besonderheiten",
            "",
            "- Siehe [Verrechnungstitel](../concepts/verrechnungstitel.md) "
            "für Hinweise zur Bereinigung interner Umbuchungen.",
            "- Siehe [Flexibilisierung](../concepts/flexibilisierung.md) "
            "für flexibilisierte Titel in diesem Einzelplan.",
            "- Siehe [Verpflichtungsermächtigungen](../concepts/verpflichtungsermachtigungen.md) "
            "für künftige Zahlungsverpflichtungen.",
            "",
        ]

        out_path = self.wiki_dir / "entities" / f"einzelplan-{ep}.md"
        out_path.write_text("\n".join(body_parts), encoding="utf-8")
        logger.info("Created %s", out_path)
        return out_path

    # ------------------------------------------------------------------
    # Overview page
    # ------------------------------------------------------------------

    def build_overview_page(self, conn: sqlite3.Connection) -> Path | None:
        """Generate the main overview/summary page."""
        rows = conn.execute(
            "SELECT year, version, "
            "  SUM(ausgaben_soll) AS total_soll, "
            "  SUM(ausgaben_ist)  AS total_ist, "
            "  COUNT(DISTINCT einzelplan) AS ep_count "
            "FROM haushaltsdaten GROUP BY year, version ORDER BY year, version"
        ).fetchall()

        if not rows:
            logger.debug("No data for overview page – skipping.")
            return None

        fm = _frontmatter({
            "title": "Überblick Bundeshaushalt",
            "type": "summary",
            "tags": ["ueberblick", "bundeshaushalt", "gesamtausgaben"],
            "years": sorted({r["year"] for r in rows}),
            "last_updated": _today(),
        })

        table = [
            "| Jahr | Version | Einzelpläne | Soll gesamt (T€) | Ist gesamt (T€) |",
            "|------|---------|-------------|-------------------|-----------------|",
        ]
        for r in rows:
            soll = f"{r['total_soll']:,.0f}" if r["total_soll"] else "–"
            ist = f"{r['total_ist']:,.0f}" if r["total_ist"] else "–"
            table.append(f"| {r['year']} | {r['version']} | {r['ep_count']} | {soll} | {ist} |")

        body = "\n".join([
            fm, "",
            "## Gesamtüberblick", "",
            "Diese Seite fasst die wichtigsten Kennzahlen aller im System "
            "erfassten Haushaltsjahre und -versionen zusammen.", "",
            "\n".join(table), "",
            "> Hinweis: Angaben einschließlich Verrechnungstitel.", "",
            "## Weiterführend", "",
            "- Einzelpläne: siehe [Index](index.md)",
            "- Konzepte: siehe Abschnitt *Konzepte* im [Index](index.md)",
            "",
        ])

        out_path = self.wiki_dir / "entities" / "ueberblick.md"
        out_path.write_text(body, encoding="utf-8")
        logger.info("Created %s", out_path)
        return out_path

    # ------------------------------------------------------------------
    # Concept pages (no LLM, hardcoded expert knowledge)
    # ------------------------------------------------------------------

    def build_concept_pages(self) -> list[Path]:
        """Generate concept pages with expert knowledge (no LLM needed)."""
        created: list[Path] = []
        concepts_dir = self.wiki_dir / "concepts"
        concepts_dir.mkdir(parents=True, exist_ok=True)

        for filename, content in _CONCEPT_PAGES.items():
            path = concepts_dir / filename
            path.write_text(content, encoding="utf-8")
            logger.info("Created concept page %s", path)
            created.append(path)
        return created

    # ------------------------------------------------------------------
    # Analysis filing
    # ------------------------------------------------------------------

    def file_analysis(self, question: str, answer: str) -> Path:
        """Save a Q&A analysis as a new wiki page under analyses/."""
        slug = _slugify(question)[:60]
        date_str = _today()
        filename = f"{date_str}-{slug}.md"

        fm = _frontmatter({
            "title": question[:120],
            "type": "analysis",
            "tags": ["analyse", "frage"],
            "last_updated": date_str,
        })

        body = "\n".join([
            fm, "",
            "## Fragestellung", "",
            question, "",
            "## Ergebnis", "",
            answer, "",
            "## Einschränkungen", "",
            "Diese Analyse wurde automatisch erstellt und sollte anhand der "
            "Primärquellen verifiziert werden.", "",
        ])

        out_path = self.wiki_dir / "analyses" / filename
        out_path.write_text(body, encoding="utf-8")
        logger.info("Filed analysis: %s", out_path)

        # Update index
        try:
            from src.wiki.indexer import WikiIndexer
            indexer = WikiIndexer(self.wiki_dir)
            indexer.rebuild_index()
            indexer.append_log("create", f"analyses/{filename}")
        except Exception as exc:
            logger.warning("Index update after filing analysis failed: %s", exc)

        return out_path


# ---------------------------------------------------------------------------
# Concept page content — expert-authored, no LLM
# ---------------------------------------------------------------------------

_CONCEPT_PAGES: dict[str, str] = {}

_CONCEPT_PAGES["verrechnungstitel.md"] = f"""\
{_frontmatter({
    "title": "Verrechnungstitel",
    "type": "concept",
    "tags": ["verrechnungstitel", "haushaltssystematik", "buchungstechnik"],
    "last_updated": _today(),
})}

## Definition

Verrechnungstitel sind haushaltsinterne Buchungsposten, die Leistungsbeziehungen
**zwischen** verschiedenen Kapiteln oder Einzelplänen des Bundeshaushalts abbilden.
Sie erfassen Erstattungen und Verrechnungen für Leistungen, die ein Ressort für
ein anderes erbringt — z.\\u202fB. die zentrale Beschaffung durch das BMI für andere
Ministerien.

## Identifikation

Verrechnungstitel sind in der Datenbank über das Feld `is_verrechnungstitel = 1`
in der Tabelle `haushaltsdaten` gekennzeichnet. Typische Titel-Nummern beginnen
häufig mit **381**, **382**, **527\\u202f55** oder gehören zu eigens ausgewiesenen
Verrechnungskapiteln (z.\\u202fB. Kapitel X\\u202f05).

## Bedeutung für Auswertungen

Bei der Berechnung der **Gesamtausgaben** eines Einzelplans oder des Bundes
müssen Verrechnungstitel **ausgeschlossen** werden, wenn man die *realen*
Ausgaben ermitteln will. Andernfalls werden interne Umbuchungen doppelt gezählt:

```
Gesamtausgaben (bereinigt) = SUM(ausgaben_soll) WHERE is_verrechnungstitel = 0
```

In der Wiki-Ausgabe wird bei jeder Summe angegeben, ob Verrechnungstitel
ein- oder ausgeschlossen sind.

## Beispiele

| Titel | Beschreibung | Einzelplan |
|-------|-------------|------------|
| 381\\u202f01 | Erstattung an EP 08 für IT-Leistungen | 14 |
| 527\\u202f55 | Verrechnungstitel Sächliche Verwaltungsausgaben | diverse |

## Verwandte Konzepte

- [Deckungsfähigkeit](deckungsfaehigkeit.md)
- [Flexibilisierung](flexibilisierung.md)
- [Haushaltsversionen](haushaltsversionen.md)
"""

_CONCEPT_PAGES["flexibilisierung.md"] = f"""\
{_frontmatter({
    "title": "Flexibilisierung",
    "type": "concept",
    "tags": ["flexibilisierung", "haushaltsfuehrung", "uebertragbarkeit"],
    "last_updated": _today(),
})}

## Definition

**Flexibilisierung** (auch *Flexibilisierte Ausgaben*) bezeichnet die
Möglichkeit, nicht verbrauchte Haushaltsmittel eines Titels in das Folgejahr zu
übertragen oder zwischen inhaltlich verwandten Titeln umzuschichten, ohne dass
ein Nachtragshaushalt erforderlich ist.

Seit der Haushaltsreform 1998 werden zahlreiche Verwaltungstitel als
*flexibilisiert* gekennzeichnet. Die Flexibilisierung soll den Ressorts mehr
Handlungsspielraum geben und wirtschaftliches Haushalten belohnen (sog.
„Dezember-Fieber" vermeiden).

## Kennzeichnung in der Datenbank

In der Tabelle `haushaltsdaten` zeigt das Feld `flexibilisiert = 1` an, dass der
Titel der Flexibilisierung unterliegt.

```sql
SELECT einzelplan, kapitel, titel, titel_text, ausgaben_soll
FROM haushaltsdaten
WHERE flexibilisiert = 1 AND year = 2024
ORDER BY ausgaben_soll DESC
LIMIT 20;
```

## Auswirkungen

1. **Übertragbarkeit**: Nicht verbrauchte Mittel können als Ausgaberest ins
   nächste Jahr mitgenommen werden.
2. **Deckungsfähigkeit**: Flexibilisierte Titel sind untereinander *gegenseitig
   deckungsfähig* innerhalb eines Kapitels.
3. **Planungssicherheit**: Ressorts können Beschaffungszyklen freier gestalten.

## Verwandte Konzepte

- [Deckungsfähigkeit](deckungsfaehigkeit.md)
- [Verrechnungstitel](verrechnungstitel.md)
- [Haushaltsversionen](haushaltsversionen.md)
"""

_CONCEPT_PAGES["deckungsfaehigkeit.md"] = f"""\
{_frontmatter({
    "title": "Deckungsfähigkeit",
    "type": "concept",
    "tags": ["deckungsfaehigkeit", "haushaltsfuehrung", "haushaltsrecht"],
    "last_updated": _today(),
})}

## Definition

**Deckungsfähigkeit** ist die haushaltsrechtliche Erlaubnis, Mehrausgaben bei
einem Titel durch Einsparungen bei einem anderen Titel zu kompensieren. Man
unterscheidet:

- **Einseitige Deckungsfähigkeit** (`deckungsfaehig = 1`): Titel A darf
  Einsparungen bereitstellen, um Mehrausgaben bei Titel B zu decken — aber
  nicht umgekehrt.
- **Gegenseitige Deckungsfähigkeit** (`gegenseitig_deckungsfaehig = 1`): Beide
  Titel können sich wechselseitig decken.

## Rechtsgrundlage

Gemäß § 20 Bundeshaushaltsordnung (BHO) können Ausgaben für gegenseitig oder
einseitig deckungsfähig erklärt werden. Die Festlegung erfolgt in den
Haushaltsvermerken des Bundeshaushaltsplans.

## Datenbankfelder

Die Tabelle `haushaltsdaten` enthält zwei relevante Spalten:

| Feld | Beschreibung |
|------|-------------|
| `deckungsfaehig` | Einseitige Deckungsfähigkeit (0/1) |
| `gegenseitig_deckungsfaehig` | Gegenseitige Deckungsfähigkeit (0/1) |

```sql
SELECT kapitel, titel, titel_text, ausgaben_soll
FROM haushaltsdaten
WHERE einzelplan = '14' AND year = 2024
  AND (deckungsfaehig = 1 OR gegenseitig_deckungsfaehig = 1)
ORDER BY kapitel, titel;
```

## Bedeutung für Auswertungen

Bei der Frage „Wie viel Geld steht *tatsächlich* zur Verfügung?" muss die
Deckungsfähigkeit berücksichtigt werden: Titel, die sich gegenseitig decken,
bilden ein *Budget-Cluster*, dessen Gesamtvolumen flexibel nutzbar ist.

## Verwandte Konzepte

- [Flexibilisierung](flexibilisierung.md)
- [Verrechnungstitel](verrechnungstitel.md)
"""

_CONCEPT_PAGES["verpflichtungsermachtigungen.md"] = f"""\
{_frontmatter({
    "title": "Verpflichtungsermächtigungen",
    "type": "concept",
    "tags": ["verpflichtungsermaechtigung", "ve", "haushaltsbindung", "zukunft"],
    "last_updated": _today(),
})}

## Definition

**Verpflichtungsermächtigungen (VE)** sind Ermächtigungen, die es der Verwaltung
gestatten, finanzielle Verpflichtungen einzugehen, die erst in **künftigen
Haushaltsjahren** zu Ausgaben führen. Sie sind das zentrale Instrument für
mehrjährige Beschaffungen, Bauvorhaben und langfristige Verträge.

Die VE wird im Haushaltsplan bewilligt, ohne dass im laufenden Jahr Barmittel
fließen — die tatsächlichen Zahlungen erfolgen über einen Fälligkeitsplan
(*Fälligkeitstranchen*).

## Rechtsgrundlage

§ 6 und § 38 Bundeshaushaltsordnung (BHO).

## Fälligkeitsstruktur

Jede VE hat einen **Fälligkeitsplan**, der angibt, wann welche Beträge
kassenwirksam werden:

| Fälligkeitsjahr | Betrag (T€) |
|-----------------|-------------|
| 2025 | 500.000 |
| 2026 | 300.000 |
| 2027 ff. | 200.000 |

## Datenbank

Die Tabelle `verpflichtungsermachtigungen` speichert VE mit Fälligkeiten:

```sql
SELECT einzelplan, kapitel, titel,
       betrag_gesamt, faellig_jahr, faellig_betrag
FROM verpflichtungsermachtigungen
WHERE einzelplan = '14' AND year = 2024
ORDER BY faellig_jahr;
```

## Bedeutung für Auswertungen

- VE zeigen die **implizite Vorbelastung** künftiger Haushalte.
- Hohe VE in einem Einzelplan signalisieren langfristige Investitions- oder
  Beschaffungsprogramme (z.\\u202fB. Rüstungsprojekte im EP 14).
- Bei Haushaltsanalysen sollten VE getrennt von den laufenden Ausgaben (Soll/Ist)
  betrachtet werden.

## Verwandte Konzepte

- [Haushaltsversionen](haushaltsversionen.md)
- [Deckungsfähigkeit](deckungsfaehigkeit.md)
"""

_CONCEPT_PAGES["planstellen.md"] = f"""\
{_frontmatter({
    "title": "Planstellen",
    "type": "concept",
    "tags": ["planstellen", "personal", "besoldung", "tariflich"],
    "last_updated": _today(),
})}

## Definition

**Planstellen** sind die im Bundeshaushaltsplan ausgewiesenen Stellen für
Beamtinnen und Beamte sowie Arbeitnehmerinnen und Arbeitnehmer des Bundes. Sie
bilden die personelle Grundlage jedes Einzelplans.

## Kategorien

| Kategorie | Beschreibung |
|-----------|-------------|
| **Tariflich** (`planstellen_tariflich`) | Stellen für Beschäftigte nach Tarifvertrag (TVöD). |
| **Außertariflich** (`planstellen_aussertariflich`) | Stellen für Beamte; Einstufung nach Besoldungsgruppen (A, B, R, W, C). |

## Besoldungsgruppen

Die Besoldungsgruppe (Feld `besoldungsgruppe` in `personalhaushalt`) gibt die
Einstufung an, z.\\u202fB.:

- **A 6 – A 9**: Mittlerer Dienst
- **A 9 – A 13**: Gehobener Dienst
- **A 13 – A 16**: Höherer Dienst (Eingangsamt: A 13)
- **B 1 – B 11**: Herausgehobene Führungspositionen
- **ORR, RD, MR, MD**: Laufbahnbezeichnungen im höheren Dienst

## Datenbank

```sql
SELECT einzelplan, kapitel, besoldungsgruppe,
       SUM(planstellen_gesamt) AS stellen
FROM personalhaushalt
WHERE year = 2024
GROUP BY einzelplan, kapitel, besoldungsgruppe
ORDER BY einzelplan, stellen DESC;
```

## Auswertungshinweise

- Die **Gesamtzahl der Planstellen** eines Einzelplans zeigt die personelle
  Ausstattung des Ressorts.
- Veränderungen gegenüber dem Vorjahr zeigen Stellenaufwuchs oder -abbau.
- Die Verteilung nach Besoldungsgruppen gibt Aufschluss über das
  Qualifikationsniveau der Belegschaft.

## Verwandte Konzepte

- [Haushaltsversionen](haushaltsversionen.md)
- [Flexibilisierung](flexibilisierung.md)
"""

_CONCEPT_PAGES["haushaltsversionen.md"] = f"""\
{_frontmatter({
    "title": "Haushaltsversionen",
    "type": "concept",
    "tags": ["version", "entwurf", "beschluss", "nachtrag", "gesetzgebung"],
    "last_updated": _today(),
})}

## Definition

Der Bundeshaushalt durchläuft mehrere **Versionen** im Verlauf des
Haushaltszyklus. Jede Version kann sich in Zahlen und Struktur von der
vorherigen unterscheiden.

## Versionstypen

| Version | Bedeutung |
|---------|-----------|
| **Entwurf** | Der Regierungsentwurf (RegE), vom Bundeskabinett beschlossen und dem Bundestag zugeleitet. |
| **Beschluss** | Der vom Bundestag verabschiedete und vom Bundesrat gebilligte Haushaltsplan — er hat Gesetzeskraft. |
| **Nachtrag** | Ein Nachtragshaushalt, der den Beschluss im laufenden Jahr ändert (z.\\u202fB. für Sondervermögen oder Krisenpakete). Kann nummeriert sein (1.\\u202fNachtrag, 2.\\u202fNachtrag). |
| **Ist** | Der Rechnungsabschluss — die tatsächlichen Ausgaben und Einnahmen nach Abschluss des Haushaltsjahres. |

## Legislativer Ablauf

1. **Aufstellung** durch das Bundesministerium der Finanzen (BMF) —
   Ressortverhandlungen, Eckwertebeschluss.
2. **Kabinettsbeschluss** → *Entwurf*.
3. **Parlamentarische Beratung** — 1.\\u202fLesung, Ausschussberatungen
   (Haushaltsausschuss), Bereinigungssitzung, 2./3.\\u202fLesung.
4. **Bundesratszustimmung** → *Beschluss*.
5. Ggf. **Nachtragshaushaltsgesetz** → *Nachtrag*.
6. **Rechnungslegung** nach § 80 BHO → *Ist*.

## Datenbankfeld

In allen Tabellen (`haushaltsdaten`, `einzelplan_meta`, etc.) enthält die Spalte
`version` einen der Werte: `entwurf`, `beschluss`, `nachtrag`, `ist`.

```sql
SELECT DISTINCT year, version
FROM haushaltsdaten
ORDER BY year, version;
```

## Bedeutung für Auswertungen

- Bei Vergleichen immer **dieselbe Version** verwenden (z.\\u202fB. Beschluss 2023
  vs. Beschluss 2024).
- Entwurf und Beschluss können signifikant voneinander abweichen —
  parlamentarische Änderungen beachten.
- Nachträge können große Beträge verschieben (z.\\u202fB. Sondervermögen
  Bundeswehr 2022).

## Verwandte Konzepte

- [Verpflichtungsermächtigungen](verpflichtungsermachtigungen.md)
- [Verrechnungstitel](verrechnungstitel.md)
"""


# ---------------------------------------------------------------------------
# Standalone smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    builder = WikiBuilder()
    # Build concept pages only (always works, no DB or LLM needed)
    paths = builder.build_concept_pages()
    print(f"Built {len(paths)} concept pages:")
    for p in paths:
        print(f"  {p}")
