#!/usr/bin/env python3
"""Generate wiki entity pages for all Einzelpläne from bundeshaushalt.db.

Reads structured budget data and produces one Markdown page per Einzelplan
under wiki/entities/, then rebuilds the wiki index.

No LLM calls — all content is derived directly from SQL aggregations.
"""

import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "bundeshaushalt.db"
WIKI_DIR = ROOT / "wiki"
ENTITIES_DIR = WIKI_DIR / "entities"

# ── Descriptions for each Einzelplan ──────────────────────────────────────
EP_DESCRIPTIONS: dict[str, str] = {
    "01": "Umfasst die Ausgaben des Bundespräsidenten und des Bundespräsidialamts. "
          "Der Einzelplan finanziert die Amtsführung des Staatsoberhaupts, das Büro "
          "des Bundespräsidenten sowie repräsentative Aufgaben.",
    "02": "Deckt die Ausgaben des Deutschen Bundestages ab, einschließlich der "
          "Abgeordnetenentschädigungen, der Fraktionsfinanzierung, der Bundestagsverwaltung "
          "und der wissenschaftlichen Dienste.",
    "03": "Finanziert den Bundesrat als Vertretung der Länder auf Bundesebene, "
          "einschließlich des Sekretariats und der laufenden Geschäftsführung.",
    "04": "Enthält die Ausgaben des Bundeskanzlers und des Bundeskanzleramts, "
          "einschließlich des Bundesnachrichtendienstes (BND), der Beauftragten der "
          "Bundesregierung für Kultur und Medien (BKM) sowie koordinierender Aufgaben.",
    "05": "Finanziert das Auswärtige Amt mit den Auslandsvertretungen, der Diplomatie, "
          "der Krisenprävention, der humanitären Hilfe und den Beiträgen zu "
          "internationalen Organisationen (u. a. Vereinte Nationen, NATO).",
    "06": "Umfasst das Bundesministerium des Innern mit den Geschäftsbereichen "
          "innere Sicherheit, Bundespolizei, Verfassungsschutz, IT-Sicherheit (BSI), "
          "Migration und öffentliche Verwaltung.",
    "07": "Deckt das Bundesministerium der Justiz ab, einschließlich der Bundesgerichte "
          "(BGH, BVerwG, BFH, BAG, BSG), des Generalbundesanwalts und der "
          "Rechtspflegeausgaben.",
    "08": "Finanziert das Bundesministerium der Finanzen mit der Steuerverwaltung, "
          "dem Zoll, der Bundesanstalt für Immobilienaufgaben (BImA) und der "
          "Bundesbeteiligungsverwaltung.",
    "09": "Enthält die Ausgaben des Bundesministeriums für Wirtschaft und Energie, "
          "darunter Wirtschaftsförderung, Energiepolitik, Mittelstandsförderung, "
          "Außenwirtschaft und Digitalisierung.",
    "10": "Umfasst das Bundesministerium für Landwirtschaft, Ernährung und Heimat "
          "mit Agrarförderung, Verbraucherschutz, ländlicher Entwicklung und "
          "Heimatpolitik.",
    "11": "Der größte Einzelplan: finanziert das Bundesministerium für Arbeit und "
          "Soziales, insbesondere den Bundeszuschuss zur Rentenversicherung, "
          "Arbeitsmarktpolitik, Grundsicherung und Teilhabe.",
    "12": "Deckt das Bundesministerium für Verkehr ab, einschließlich "
          "Bundesfernstraßen, Schieneninfrastruktur, Wasserstraßen, Luftverkehr "
          "und der Verkehrsinvestitionsplanung.",
    "14": "Finanziert das Bundesministerium der Verteidigung und die Bundeswehr. "
          "Umfasst Personalausgaben, Beschaffung von Rüstungsgütern, Betrieb, "
          "Auslandseinsätze und Infrastruktur.",
    "15": "Enthält die Ausgaben des Bundesministeriums für Gesundheit, darunter "
          "den Bundeszuschuss zum Gesundheitsfonds, Prävention, "
          "Arzneimittelregulierung und internationale Gesundheitspolitik.",
    "16": "Finanziert das Bundesministerium für Umwelt, Klimaschutz, Naturschutz "
          "und nukleare Sicherheit. Umfasst Klimaschutzprogramme, Naturschutz, "
          "Endlagersuche und Strahlenschutz.",
    "17": "Deckt das Bundesministerium für Bildung, Familie, Senioren, Frauen und "
          "Jugend ab, einschließlich BAföG, Forschungsförderung, Familienpolitik, "
          "Kinder- und Jugendplan sowie Seniorenpolitik.",
    "19": "Finanziert das Bundesverfassungsgericht in Karlsruhe als unabhängiges "
          "Verfassungsorgan.",
    "20": "Enthält die Ausgaben des Bundesrechnungshofs als oberste "
          "Rechnungsprüfungsbehörde des Bundes.",
    "21": "Finanziert die Bundesbeauftragte für den Datenschutz und die "
          "Informationsfreiheit (BfDI) als unabhängige Aufsichtsbehörde.",
    "22": "Finanziert den Unabhängigen Kontrollrat, der die nachrichtendienstliche "
          "Tätigkeit des Bundes überwacht.",
    "23": "Umfasst das Bundesministerium für wirtschaftliche Zusammenarbeit und "
          "Entwicklung (BMZ) mit der bilateralen und multilateralen "
          "Entwicklungszusammenarbeit, GIZ und KfW-Entwicklungsbank.",
    "25": "Finanziert das Bundesministerium für Wohnen, Stadtentwicklung und "
          "Bauwesen mit Wohnungsbauförderung, Stadtentwicklung und Bundesbauten.",
    "30": "Enthält die Ausgaben des Bundesministeriums für Forschung, Technologie "
          "und Raumfahrt, darunter Grundlagenforschung, Raumfahrtprogramme und "
          "Technologieförderung.",
    "32": "Der Einzelplan Bundesschuld umfasst die Zinsausgaben und "
          "Tilgungsleistungen für die Schulden des Bundes sowie die Kosten "
          "der Kreditaufnahme.",
    "60": "Die Allgemeine Finanzverwaltung ist ein Sammeleinzelplan für "
          "übergreifende Einnahmen und Ausgaben, darunter Steuereinnahmen, "
          "EU-Eigenmittel, Zuweisungen an Länder und Globaltitel.",
}

# Override names where PDF extraction truncated them
EP_NAME_OVERRIDES: dict[str, str] = {
    "16": "Bundesministerium für Umwelt, Klimaschutz, Naturschutz und nukleare Sicherheit",
}

EP_TAGS: dict[str, list[str]] = {
    "01": ["bundespraesidialamt", "verfassungsorgan"],
    "02": ["bundestag", "parlament", "verfassungsorgan"],
    "03": ["bundesrat", "laendervertretung", "verfassungsorgan"],
    "04": ["bundeskanzleramt", "bnd", "bkm", "kultur"],
    "05": ["auswaertiges-amt", "diplomatie", "aussenpolitik"],
    "06": ["inneres", "sicherheit", "bundespolizei", "bsi", "migration"],
    "07": ["justiz", "bundesgerichte", "rechtspflege"],
    "08": ["finanzen", "steuerverwaltung", "zoll", "bima"],
    "09": ["wirtschaft", "energie", "mittelstand", "digitalisierung"],
    "10": ["landwirtschaft", "ernaehrung", "heimat", "verbraucherschutz"],
    "11": ["arbeit", "soziales", "rente", "grundsicherung"],
    "12": ["verkehr", "infrastruktur", "schiene", "strasse"],
    "14": ["verteidigung", "bundeswehr", "ruestung"],
    "15": ["gesundheit", "gesundheitsfonds", "praevention"],
    "16": ["umwelt", "klimaschutz", "naturschutz", "nuklear"],
    "17": ["bildung", "familie", "senioren", "frauen", "jugend", "bafoeg"],
    "19": ["bundesverfassungsgericht", "verfassungsorgan"],
    "20": ["bundesrechnungshof", "rechnungspruefung"],
    "21": ["datenschutz", "informationsfreiheit", "bfdi"],
    "22": ["kontrollrat", "nachrichtendienste"],
    "23": ["entwicklung", "bmz", "giz", "entwicklungszusammenarbeit"],
    "25": ["wohnen", "stadtentwicklung", "bauwesen"],
    "30": ["forschung", "technologie", "raumfahrt"],
    "32": ["bundesschuld", "zinsen", "tilgung", "kreditaufnahme"],
    "60": ["allgemeine-finanzverwaltung", "steuern", "eu-eigenmittel"],
}


def format_de(n) -> str:
    """Format a number German-style: 16161139 → 16.161.139"""
    if n is None:
        return "—"
    return f"{int(n):,}".replace(",", ".")


def clean_text(text: str | None, max_len: int = 80) -> str:
    """Clean extracted PDF text: remove line-break hyphens, truncate."""
    if not text:
        return "—"
    t = text.replace("-\n", "").replace("\n", " ").strip()
    # Collapse multiple spaces
    while "  " in t:
        t = t.replace("  ", " ")
    if len(t) > max_len:
        t = t[:max_len].rsplit(" ", 1)[0] + " …"
    return t


def generate_page(conn: sqlite3.Connection, ep: str, ep_text: str) -> str:
    """Generate the Markdown content for one Einzelplan entity page."""

    # ── Aggregate figures ─────────────────────────────────────────────
    ausgaben_ohne_vt = conn.execute(
        "SELECT SUM(ausgaben_soll) FROM haushaltsdaten "
        "WHERE einzelplan=? AND is_verrechnungstitel=0",
        (ep,),
    ).fetchone()[0]

    ausgaben_mit_vt = conn.execute(
        "SELECT SUM(ausgaben_soll) FROM haushaltsdaten WHERE einzelplan=?",
        (ep,),
    ).fetchone()[0]

    einnahmen = conn.execute(
        "SELECT SUM(einnahmen_soll) FROM haushaltsdaten "
        "WHERE einzelplan=? AND is_verrechnungstitel=0",
        (ep,),
    ).fetchone()[0]

    planstellen = conn.execute(
        "SELECT SUM(planstellen_gesamt) FROM personalhaushalt WHERE einzelplan=?",
        (ep,),
    ).fetchone()[0]

    # ── Kapitel breakdown ─────────────────────────────────────────────
    kapitel_rows = conn.execute(
        "SELECT k.kapitel, k.kapitel_text, "
        "  COALESCE(SUM(h.ausgaben_soll), 0) AS ausgaben "
        "FROM kapitel_meta k "
        "LEFT JOIN haushaltsdaten h "
        "  ON k.einzelplan = h.einzelplan AND k.kapitel = h.kapitel "
        "  AND h.is_verrechnungstitel = 0 "
        "WHERE k.einzelplan = ? "
        "GROUP BY k.kapitel, k.kapitel_text "
        "ORDER BY k.kapitel",
        (ep,),
    ).fetchall()

    # ── Top 10 titles by expenditure ──────────────────────────────────
    top_titles = conn.execute(
        "SELECT kapitel, titel, titel_text, ausgaben_soll "
        "FROM haushaltsdaten "
        "WHERE einzelplan=? AND is_verrechnungstitel=0 "
        "  AND ausgaben_soll IS NOT NULL AND ausgaben_soll > 0 "
        "ORDER BY ausgaben_soll DESC LIMIT 10",
        (ep,),
    ).fetchall()

    # ── Personnel by Besoldungsgruppe ─────────────────────────────────
    personal_rows = conn.execute(
        "SELECT besoldungsgruppe, SUM(planstellen_gesamt) AS ps "
        "FROM personalhaushalt "
        "WHERE einzelplan=? AND planstellen_gesamt > 0 "
        "GROUP BY besoldungsgruppe "
        "ORDER BY ps DESC",
        (ep,),
    ).fetchall()

    # ── Build Markdown ────────────────────────────────────────────────
    today = date.today().isoformat()
    tags_list = EP_TAGS.get(ep, [])
    tags = '["einzelplan", ' + ", ".join(f'"{t}"' for t in tags_list) + "]"
    desc = EP_DESCRIPTIONS.get(ep, f"Einzelplan {ep} des Bundeshaushalts.")

    ep_text_clean = EP_NAME_OVERRIDES.get(ep, clean_text(ep_text, max_len=120))

    lines: list[str] = []
    # Frontmatter
    lines.append("---")
    lines.append(f'title: "Einzelplan {ep} — {ep_text_clean}"')
    lines.append("type: entity")
    lines.append(f"tags: {tags}")
    lines.append("years: [2026]")
    lines.append('sources: ["0350-25.pdf"]')
    lines.append(f'last_updated: "{today}"')
    lines.append("---")
    lines.append("")

    # Heading
    lines.append(f"# Einzelplan {ep} — {ep_text_clean}")
    lines.append("")

    # Overview
    lines.append("## Überblick")
    lines.append("")
    lines.append(desc)
    lines.append("")

    # Budget summary table
    lines.append("## Haushaltsdaten 2026 (Entwurf)")
    lines.append("")
    lines.append("| Kennzahl | Betrag (T€) |")
    lines.append("|----------|------------:|")
    lines.append(f"| Ausgaben Soll (ohne Verrechnungstitel) | {format_de(ausgaben_ohne_vt)} |")
    lines.append(f"| Ausgaben Soll (mit Verrechnungstitel) | {format_de(ausgaben_mit_vt)} |")
    lines.append(f"| Einnahmen Soll | {format_de(einnahmen)} |")
    lines.append(f"| Planstellen gesamt | {format_de(planstellen)} |")
    lines.append("")

    # Kapitel table
    lines.append("## Kapitel")
    lines.append("")
    lines.append("| Kapitel | Bezeichnung | Ausgaben Soll (T€) |")
    lines.append("|---------|-------------|--------------------:|")
    for kap, ktext, kausg in kapitel_rows:
        ktext_clean = clean_text(ktext, max_len=80)
        lines.append(f"| {kap} | {ktext_clean} | {format_de(kausg)} |")
    lines.append("")

    # Top 10 titles
    if top_titles:
        lines.append("## Top 10 Titel nach Ausgaben")
        lines.append("")
        lines.append("| Kapitel | Titel | Bezeichnung | Ausgaben Soll (T€) |")
        lines.append("|---------|-------|-------------|--------------------:|")
        for tkap, ttitel, ttext, tausg in top_titles:
            ttext_clean = clean_text(ttext, max_len=80)
            lines.append(f"| {tkap} | {ttitel} | {ttext_clean} | {format_de(tausg)} |")
        lines.append("")

    # Personnel
    if personal_rows:
        lines.append("## Personalhaushalt")
        lines.append("")
        lines.append("| Besoldungsgruppe | Planstellen |")
        lines.append("|------------------|------------:|")
        for bg, ps in personal_rows:
            lines.append(f"| {bg} | {format_de(ps)} |")
        lines.append("")

    # Cross-references
    lines.append("## Querverweise")
    lines.append("")
    lines.append("- [Verrechnungstitel](../concepts/verrechnungstitel.md)")
    lines.append("- [Planstellen](../concepts/planstellen.md)")
    lines.append("- [Flexibilisierung](../concepts/flexibilisierung.md)")
    lines.append("- [Verpflichtungsermächtigungen](../concepts/verpflichtungsermachtigungen.md)")
    lines.append("- [Deckungsfähigkeit](../concepts/deckungsfaehigkeit.md)")
    lines.append("- [Haushaltsversionen](../concepts/haushaltsversionen.md)")
    lines.append("")

    return "\n".join(lines)


def main():
    ENTITIES_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))

    einzelplaene = conn.execute(
        "SELECT einzelplan, einzelplan_text FROM einzelplan_meta ORDER BY einzelplan"
    ).fetchall()

    print(f"Generating entity pages for {len(einzelplaene)} Einzelpläne …")

    for ep, ep_text in einzelplaene:
        content = generate_page(conn, ep, ep_text)
        out_path = ENTITIES_DIR / f"einzelplan-{ep}.md"
        out_path.write_text(content, encoding="utf-8")
        print(f"  ✓ {out_path.name}")

    conn.close()

    # Rebuild wiki index
    from src.wiki.indexer import WikiIndexer

    indexer = WikiIndexer(WIKI_DIR)
    idx = indexer.rebuild_index()
    indexer.append_log(
        "build_entities",
        f"Generated entity pages for all {len(einzelplaene)} Einzelpläne from database",
    )
    print(f"\nIndex rebuilt: {idx}")

    # Verify
    entity_files = sorted(ENTITIES_DIR.glob("einzelplan-*.md"))
    print(f"\n{len(entity_files)} entity pages in wiki/entities/:")
    for f in entity_files:
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
