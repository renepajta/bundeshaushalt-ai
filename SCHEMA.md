# SCHEMA.md — Wiki-Strukturdefinition für das Bundeshaushalt-Q&A-System

> Dieses Dokument ist die verbindliche Referenz für den Aufbau, die Pflege und die
> Nutzung des Knowledge-Wiki. Es richtet sich an das LLM, das die Wiki-Seiten
> erstellt, aktualisiert und bei der Beantwortung von Fragen heranzieht.

---

## 1  Architekturüberblick

Das System besteht aus drei Schichten:

```
┌─────────────────────────────────────────────────────┐
│  Schicht 1 — Rohquellen (PDF)                       │
│  Bundeshaushaltspläne, BRH-Berichte, Nachträge      │
│  → data/                                            │
└────────────────────────┬────────────────────────────┘
                         │  Ingest
                         ▼
┌─────────────────────────────────────────────────────┐
│  Schicht 2 — Wissensschicht                         │
│                                                     │
│  ┌──────────────┐   ┌──────────────────────────┐    │
│  │  SQLite DB    │   │  Knowledge-Wiki (wiki/)   │   │
│  │  Strukturiert │   │  Kontextuell / Narrativ   │   │
│  │  Zahlen,      │   │  Erläuterungen, Begriffe, │   │
│  │  Titel, Soll/ │   │  Querverweise, Analysen   │   │
│  │  Ist-Werte    │   │                           │   │
│  └──────────────┘   └──────────────────────────┘    │
└────────────────────────┬────────────────────────────┘
                         │  Query
                         ▼
┌─────────────────────────────────────────────────────┐
│  Schicht 3 — Query-Engine                           │
│  LLM kombiniert Wiki-Kontext + SQL-Ergebnisse       │
│  → Präzise Antworten auf Deutsch mit Quellenangaben │
└─────────────────────────────────────────────────────┘
```

**Abgrenzung der Schichten:**

| Aspekt | SQLite-Datenbank | Knowledge-Wiki |
|---|---|---|
| Inhalt | Numerische Haushaltsdaten (Soll, Ist, VE) | Erläuterungen, Begriffe, Zusammenhänge |
| Format | Tabellen, Zeilen, Spalten | Markdown-Seiten mit YAML-Frontmatter |
| Zugriff | SQL-Abfragen | Datei lesen, Volltextsuche |
| Beispiel | `SELECT soll FROM titel WHERE einzelplan=14 AND jahr=2024` | „Einzelplan 14 umfasst die Ausgaben des BMVg …" |

---

## 2  Verzeichnisstruktur

```
wiki/
├── index.md              # Inhaltsverzeichnis aller Wiki-Seiten
├── log.md                # Chronologisches Aktivitätsprotokoll
├── entities/             # Entitäts-Seiten (Einzelpläne, Kapitel)
│   ├── einzelplan-01.md
│   ├── einzelplan-14.md
│   ├── kapitel-1403.md
│   └── …
├── concepts/             # Begriffs- und Konzeptseiten
│   ├── verrechnungstitel.md
│   ├── flexibilisierung.md
│   ├── verpflichtungsermaechtigung.md
│   └── …
└── analyses/             # Abgelegte Analysen und Vergleiche
    ├── 2026-04-14-verteidigungsausgaben-trend.md
    └── …
```

---

## 3  Seitentypen und Namenskonventionen

### 3.1  Entity-Seiten (`entities/`)

| Muster | Beispiel | Beschreibung |
|---|---|---|
| `einzelplan-{nn}.md` | `einzelplan-14.md` | Eine Seite pro Einzelplan. `nn` ist die zweistellige Nummer (mit führender Null). |
| `kapitel-{nnnn}.md` | `kapitel-1403.md` | Wichtige Kapitel. `nnnn` ist die vierstellige Kapitelnummer. |

**Regeln:**
- Jeder Einzelplan, der in mindestens einer Quelle vorkommt, erhält eine eigene Seite.
- Kapitel-Seiten werden nur für besonders relevante oder häufig abgefragte Kapitel angelegt.
- Die Dateinamen enthalten ausschließlich Ziffern und Bindestriche, keine Umlaute.

### 3.2  Concept-Seiten (`concepts/`)

| Muster | Beispiel |
|---|---|
| `{konzept-name}.md` | `verrechnungstitel.md` |

**Regeln:**
- Dateinamen in Kebab-Case (Kleinbuchstaben, Bindestriche statt Leerzeichen).
- Deutsche Begriffe verwenden. Umlaute werden aufgelöst: `ä→ae`, `ö→oe`, `ü→ue`, `ß→ss`.
- Beispiel: `deckungsfaehigkeit.md`, `verpflichtungsermaechtigung.md`.

### 3.3  Analysis-Seiten (`analyses/`)

| Muster | Beispiel |
|---|---|
| `{JJJJ-MM-TT}-{thema}.md` | `2026-04-14-verteidigungsausgaben-trend.md` |

**Regeln:**
- Dateiname beginnt immer mit einem ISO-Datum (`JJJJ-MM-TT`).
- Das Thema folgt in Kebab-Case (deutsch, Umlaute aufgelöst).
- Analysen sind unveränderlich (immutable). Bei Aktualisierung wird eine neue Datei mit neuem Datum erstellt.

### 3.4  Spezialseiten

| Datei | Zweck |
|---|---|
| `index.md` | Katalog aller Wiki-Seiten mit Kurzzusammenfassungen. |
| `log.md` | Chronologisches Protokoll aller Ingest- und Pflegeaktionen. |

---

## 4  Frontmatter-Format

Jede Wiki-Seite beginnt mit einem YAML-Frontmatter-Block:

```yaml
---
title: "Einzelplan 14 — Bundesministerium der Verteidigung"
type: entity          # entity | concept | analysis | summary
tags: [einzelplan, verteidigung, militaer]
years: [2020, 2021, 2022, 2023, 2024]
sources: ["0350-25.pdf", "bundeshaushaltsplan-2024.pdf"]
last_updated: "2026-04-14"
---
```

### Pflichtfelder

| Feld | Typ | Beschreibung |
|---|---|---|
| `title` | String | Lesbarer Titel der Seite. |
| `type` | Enum | Einer von: `entity`, `concept`, `analysis`, `summary`. |
| `last_updated` | Datum (ISO) | Letztes Änderungsdatum der Seite. |

### Optionale Felder

| Feld | Typ | Beschreibung |
|---|---|---|
| `tags` | Liste | Freitext-Schlagwörter für die Suche (Kleinbuchstaben, keine Umlaute). |
| `years` | Liste (int) | Haushaltsjahre, die auf dieser Seite behandelt werden. |
| `sources` | Liste | Dateinamen der Quell-PDFs in `data/`. |
| `einzelplan` | String | Zweistellige Einzelplan-Nummer (nur bei Entity-Seiten). |
| `kapitel` | String | Vierstellige Kapitelnummer (nur bei Kapitel-Seiten). |
| `supersedes` | String | Pfad einer älteren Analyse, die diese ersetzt. |

---

## 5  Querverweise (Cross-References)

### Interne Links

Für Verweise innerhalb des Wiki werden relative Markdown-Links verwendet:

```markdown
Siehe [Einzelplan 14](entities/einzelplan-14.md) für Details zum Verteidigungshaushalt.

Der Begriff [Verrechnungstitel](concepts/verrechnungstitel.md) wird in Kapitel 3 erläutert.
```

**Regeln:**
- Immer relative Pfade verwenden (von der Seite aus, die den Link enthält).
- Aus `entities/einzelplan-14.md` heraus: `[Kapitel 1403](kapitel-1403.md)` (gleicher Ordner).
- Aus `entities/einzelplan-14.md` heraus: `[Flexibilisierung](../concepts/flexibilisierung.md)` (anderer Ordner).
- Wiki-interne Kurzschreibweise `[[kapitel-1403]]` ist als Konvention erlaubt und wird beim Lint auf gültige Seiten geprüft.

### Externe Links

Verweise auf Quell-PDFs:

```markdown
Quelle: [Bundeshaushaltsplan 2024, Einzelplan 14](../data/0350-25.pdf)
```

---

## 6  Datenkonventionen

### 6.1  Geldbeträge

| Regel | Beispiel |
|---|---|
| Alle Beträge in **Tausend Euro (T€)** wie im Haushaltsplan. | `Soll 2024: 51.950.782 T€` |
| Deutsche Zahlenformatierung im Fließtext: Punkt als Tausendertrenner, Komma als Dezimaltrenner. | `16.161.139,00` |
| In Tabellen können reine Zahlen ohne Formatierung stehen, aber die Einheit muss im Spaltenkopf stehen. | Spaltenkopf: `Soll (T€)` |

### 6.2  Jahres- und Versionsangaben

Bei jeder Zahlenangabe **immer** angeben:
1. Das **Haushaltsjahr** (z. B. `2024`).
2. Die **Version** des Plans:
   - `Entwurf` (Regierungsentwurf)
   - `Beschluss` (vom Bundestag beschlossener Plan)
   - `Nachtrag` (Nachtragshaushalt, ggf. mit Nummer)
   - `Ist` (Rechnungsabschluss)

Beispiel:
> Die Soll-Ausgaben des Einzelplans 14 betragen im Beschluss 2024 insgesamt 51.950.782 T€.

### 6.3  Verrechnungstitel

Bei allen Summen und Vergleichen **explizit** angeben, ob Verrechnungstitel ein- oder ausgeschlossen sind:

```markdown
**Gesamtausgaben Einzelplan 14 (Beschluss 2024):**
- Einschließlich Verrechnungstitel: 52.123.456 T€
- Ohne Verrechnungstitel: 51.950.782 T€
```

### 6.4  Sprache

- Alle Wiki-Inhalte werden auf **Deutsch** verfasst.
- Fachbegriffe des Haushaltsrechts werden im deutschen Original verwendet.
- Englische Fachbegriffe (z. B. aus der Informatik) sind erlaubt, wenn es keine gebräuchliche deutsche Entsprechung gibt.

---

## 7  Inhaltsstruktur der Seitentypen

### 7.1  Einzelplan-Seite

```markdown
---
title: "Einzelplan 14 — Bundesministerium der Verteidigung"
type: entity
einzelplan: "14"
tags: [einzelplan, verteidigung, bmvg]
years: [2023, 2024, 2025]
sources: ["0350-25.pdf"]
last_updated: "2026-04-14"
---

## Überblick

Kurze Beschreibung des Ministeriums und seiner Zuständigkeit.

## Struktur

Auflistung der wichtigsten Kapitel mit Links:
- [Kapitel 1401](kapitel-1401.md) — Ministerium
- [Kapitel 1403](kapitel-1403.md) — Streitkräfte
- …

## Eckwerte

| Jahr | Soll (T€) | Ist (T€) | Veränderung |
|------|-----------|----------|-------------|
| 2023 | …         | …        | …           |
| 2024 | …         | …        | …           |

> Hinweis: Angaben ohne Verrechnungstitel.

## Besonderheiten

- Sondervermögen, Verpflichtungsermächtigungen, Planstellen etc.
- Verweis auf relevante [Konzeptseiten](../concepts/…).

## Quellen

- Bundeshaushaltsplan 2024, Einzelplan 14 (0350-25.pdf)
```

### 7.2  Konzept-Seite

```markdown
---
title: "Verrechnungstitel"
type: concept
tags: [verrechnungstitel, haushaltssystematik, buchungstechnik]
last_updated: "2026-04-14"
---

## Definition

Was ist ein Verrechnungstitel? Erklärung im haushaltsrechtlichen Kontext.

## Bedeutung für Auswertungen

Warum muss man Verrechnungstitel bei Summenbildungen beachten?

## Beispiele

Konkrete Titel-Nummern und typische Anwendungsfälle.

## Verwandte Konzepte

- [Deckungsfähigkeit](deckungsfaehigkeit.md)
- [Flexibilisierung](flexibilisierung.md)
```

### 7.3  Analyse-Seite

```markdown
---
title: "Trend der Verteidigungsausgaben 2020–2024"
type: analysis
tags: [verteidigung, trend, einzelplan-14]
years: [2020, 2021, 2022, 2023, 2024]
sources: ["0350-25.pdf", "0350-24.pdf"]
last_updated: "2026-04-14"
---

## Fragestellung

Welche Frage wurde beantwortet?

## Methodik

- Welche Daten wurden herangezogen?
- SQL-Abfrage(n) oder Berechnungslogik.

## Ergebnis

Zusammenfassung mit Tabellen und Erklärungen.

## Einschränkungen

Bekannte Lücken, Annahmen, Vorbehalte.
```

---

## 8  Spezialseiten

### 8.1  index.md

Die Datei `wiki/index.md` dient als Inhaltsverzeichnis. Aufbau:

```markdown
---
title: "Wiki-Index"
type: summary
last_updated: "2026-04-14"
---

## Einzelpläne

| Seite | Titel | Jahre |
|---|---|---|
| [einzelplan-01](entities/einzelplan-01.md) | Bundespräsident und Bundespräsidialamt | 2023, 2024 |
| [einzelplan-14](entities/einzelplan-14.md) | Bundesministerium der Verteidigung | 2023, 2024 |
| … | … | … |

## Konzepte

| Seite | Kurzbeschreibung |
|---|---|
| [verrechnungstitel](concepts/verrechnungstitel.md) | Interne Buchungstitel … |
| [flexibilisierung](concepts/flexibilisierung.md) | Übertragbarkeit von Mitteln … |
| … | … |

## Analysen

| Seite | Datum | Thema |
|---|---|---|
| [2026-04-14-verteidigungsausgaben-trend](analyses/2026-04-14-verteidigungsausgaben-trend.md) | 2026-04-14 | Trend der Verteidigungsausgaben |
| … | … | … |
```

**Regeln:**
- Jede Wiki-Seite **muss** im Index eingetragen sein.
- Der Index wird bei jedem Ingest und bei jeder Seitenerstellung aktualisiert.

### 8.2  log.md

Die Datei `wiki/log.md` protokolliert jede Aktion chronologisch:

```markdown
---
title: "Aktivitätsprotokoll"
type: summary
last_updated: "2026-04-14"
---

## Protokoll

| Zeitstempel | Aktion | Details |
|---|---|---|
| 2026-04-14 09:30 | ingest | 0350-25.pdf → Einzelplan 14, Beschluss 2024 |
| 2026-04-14 09:31 | create | entities/einzelplan-14.md |
| 2026-04-14 09:31 | create | concepts/verrechnungstitel.md |
| 2026-04-14 09:32 | update | index.md |
| 2026-04-14 10:15 | query  | „Wie hoch sind die Verteidigungsausgaben 2024?" |
| 2026-04-14 10:15 | create | analyses/2026-04-14-verteidigungsausgaben-trend.md |
```

**Aktionstypen:** `ingest`, `create`, `update`, `delete`, `query`, `lint`.

---

## 9  Workflows

### 9.1  Ingest-Workflow (Neue PDF-Quelle verarbeiten)

```
1. PDF-Text und Tabellen extrahieren
   └─ Werkzeug: PDF-Parser (z. B. pdfplumber, camelot)
   └─ Ergebnis: Rohtext + tabellarische Daten

2. Strukturierte Daten in SQLite laden
   └─ Titel, Kapitel, Einzelplan, Soll/Ist, VE, Planstellen
   └─ Deduplizierung: Prüfen, ob Datensatz bereits existiert

3. Entity-Seiten erstellen oder aktualisieren
   └─ Für jeden Einzelplan im Dokument:
      ├─ Existiert entities/einzelplan-{nn}.md?
      │   ├─ Ja → Aktualisieren (neue Jahre, Quellen, Eckwerte ergänzen)
      │   └─ Nein → Neu erstellen nach Vorlage (Abschnitt 7.1)
      └─ Für besonders relevante Kapitel: kapitel-{nnnn}.md analog

4. Konzept-Seiten erstellen
   └─ Neue Fachbegriffe identifizieren
   └─ Prüfen, ob concepts/{begriff}.md existiert
   └─ Falls nein: Seite anlegen nach Vorlage (Abschnitt 7.2)

5. index.md aktualisieren
   └─ Neue Seiten eintragen

6. log.md ergänzen
   └─ Ingest-Aktion mit Quelldatei und verarbeiteten Einzelplänen protokollieren
```

### 9.2  Query-Workflow (Frage beantworten)

```
1. index.md lesen
   └─ Relevante Wiki-Seiten für die Fragestellung identifizieren

2. Wiki-Seiten lesen
   └─ Kontextwissen sammeln (Erläuterungen, Begriffe, Besonderheiten)
   └─ Insbesondere Konzept-Seiten für Fachbegriffe in der Frage laden

3. SQL-Abfrage generieren und ausführen
   └─ Numerische Daten aus der SQLite-Datenbank abrufen
   └─ Verrechnungstitel beachten (ein-/ausschließen je nach Frage)
   └─ Ergebnis validieren (Plausibilitätsprüfung)

4. Antwort synthetisieren
   └─ Kontext (Wiki) + Daten (SQL) + Berechnung im LLM-Prompt kombinieren
   └─ Antwort auf Deutsch mit:
      ├─ Präzisen Zahlen (deutsche Formatierung)
      ├─ Quellenangabe (Jahr, Version, PDF)
      ├─ Hinweis auf Verrechnungstitel (ein-/ausgeschlossen)
      └─ Verweis auf weiterführende Wiki-Seiten

5. Optional: Analyse ablegen
   └─ Bei komplexen oder wiederverwertbaren Ergebnissen:
      ├─ analyses/{JJJJ-MM-TT}-{thema}.md erstellen
      ├─ index.md aktualisieren
      └─ log.md ergänzen
```

### 9.3  Lint-Workflow (Wiki-Gesundheitsprüfung)

Der Lint-Workflow prüft die Konsistenz und Vollständigkeit des Wiki:

| Prüfung | Beschreibung | Schweregrad |
|---|---|---|
| **Verwaiste Seiten** | Seiten, die von keiner anderen Seite und nicht vom Index verlinkt werden. | Warnung |
| **Fehlende Seiten** | Links, die auf nicht existierende Seiten zeigen. | Fehler |
| **Veraltete Daten** | Eckwerte im Wiki weichen von der SQLite-Datenbank ab. | Fehler |
| **Fehlende Querverweise** | Einzelplan-Seite erwähnt ein Kapitel, verlinkt es aber nicht. | Warnung |
| **Konzept ohne Seite** | Ein Fachbegriff wird im Wiki verwendet, hat aber keine eigene Konzeptseite. | Hinweis |
| **Frontmatter-Validierung** | Pflichtfelder fehlen oder haben ungültiges Format. | Fehler |
| **Index-Vollständigkeit** | Seite existiert auf der Festplatte, ist aber nicht in `index.md` eingetragen. | Fehler |
| **Quellen-Referenz** | `sources`-Feld verweist auf eine Datei, die nicht in `data/` liegt. | Warnung |

**Ablauf:**
```
1. Alle .md-Dateien in wiki/ rekursiv auflisten
2. Frontmatter jeder Seite parsen und validieren
3. Alle internen Links extrahieren und auf Existenz prüfen
4. Abgleich index.md ↔ tatsächliche Dateien
5. Eckwerte-Stichprobe: Zufällige Werte aus Entity-Seiten gegen SQL prüfen
6. Ergebnis als Report ausgeben und in log.md protokollieren
```

---

## 10  Begriffskatalog (Concept-Kandidaten)

Die folgende Liste enthält zentrale Haushaltstermini, für die Konzeptseiten in `concepts/` angelegt werden sollen, sobald sie in einer Quelle auftreten:

### 10.1  Haushaltssystematik

| Begriff | Dateiname | Kurzbeschreibung |
|---|---|---|
| Einzelplan | `einzelplan.md` | Oberste Gliederungsebene des Bundeshaushalts (ein Plan pro Ressort). |
| Kapitel | `kapitel.md` | Untergliederung eines Einzelplans. |
| Titel | `titel.md` | Einzelner Haushaltsposten innerhalb eines Kapitels. |
| Titelgruppe | `titelgruppe.md` | Zusammenfassung mehrerer Titel zu einer sachlichen Einheit. |

### 10.2  Plangrößen und Werte

| Begriff | Dateiname | Kurzbeschreibung |
|---|---|---|
| Soll | `soll.md` | Im Haushaltsplan veranschlagter Betrag (Plan-Wert). |
| Ist | `ist.md` | Tatsächlich geleisteter/vereinnahmter Betrag (Rechnungsergebnis). |
| Ausgaben | `ausgaben.md` | Auszahlungen des Bundes. |
| Einnahmen | `einnahmen.md` | Einzahlungen des Bundes. |

### 10.3  Haushaltsinstrumente

| Begriff | Dateiname | Kurzbeschreibung |
|---|---|---|
| Verrechnungstitel | `verrechnungstitel.md` | Interne Buchungstitel für Verrechnungen zwischen Einzelplänen. |
| Verpflichtungsermächtigung | `verpflichtungsermaechtigung.md` | Ermächtigung, Ausgaben für künftige Haushaltsjahre einzugehen. |
| Deckungsfähigkeit | `deckungsfaehigkeit.md` | Gegenseitige Deckung von Titeln (einseitig/gegenseitig). |
| Flexibilisierung | `flexibilisierung.md` | Übertragbarkeit und gegenseitige Deckungsfähigkeit bestimmter Titel. |
| Planstellen | `planstellen.md` | Im Haushaltsplan ausgebrachte Stellen für Beamte/Beschäftigte. |

### 10.4  Planversionen

| Begriff | Dateiname | Kurzbeschreibung |
|---|---|---|
| Entwurf | `entwurf.md` | Regierungsentwurf des Haushaltsplans. |
| Beschluss | `beschluss.md` | Vom Bundestag verabschiedeter Haushaltsplan. |
| Nachtrag | `nachtrag.md` | Nachtragshaushalt (Änderung des laufenden Plans). |

### 10.5  Volkswirtschaftlicher Kontext

| Begriff | Dateiname | Kurzbeschreibung |
|---|---|---|
| Bruttoinlandsprodukt / BIP | `bruttoinlandsprodukt.md` | Gesamtwert aller Waren und Dienstleistungen; Referenzgröße für Haushaltsquoten. |

---

## 11  Qualitätsregeln

1. **Keine Zahlen ohne Quelle.** Jede Zahl im Wiki muss einem konkreten Dokument (PDF), Jahr und Version zugeordnet werden können.

2. **Wiki ist kein Duplikat der Datenbank.** Das Wiki erklärt und kontextualisiert. Für exakte Summen und Vergleiche wird immer die SQLite-Datenbank per SQL abgefragt. Eckwerte in Entity-Seiten dienen der Orientierung und werden beim Lint gegen die Datenbank geprüft.

3. **Konsistenz vor Vollständigkeit.** Lieber eine korrekte Seite mit wenigen Angaben als eine umfangreiche Seite mit veralteten Werten.

4. **Atomar aktualisieren.** Änderungen an einer Entity-Seite, dem Index und dem Log werden immer gemeinsam durchgeführt — nicht einzeln.

5. **Unveränderlichkeit von Analysen.** Analyse-Seiten werden nach Erstellung nicht verändert. Neue Erkenntnisse führen zu einer neuen Analyse-Seite, die im Feld `supersedes` auf die ältere verweist.

---

## 12  Zusammenfassung der Dateinamen-Konventionen

```
wiki/index.md                                    # Immer vorhanden
wiki/log.md                                      # Immer vorhanden
wiki/entities/einzelplan-{nn}.md                  # nn = 01..60
wiki/entities/kapitel-{nnnn}.md                   # nnnn = 0101..6099
wiki/concepts/{konzept-name}.md                   # Kebab-Case, Umlaute aufgelöst
wiki/analyses/{JJJJ-MM-TT}-{thema}.md            # ISO-Datum + Kebab-Case
```

**Zeichenregeln für Dateinamen:**
- Nur Kleinbuchstaben `a-z`, Ziffern `0-9` und Bindestriche `-`.
- Keine Umlaute, kein `ß`, keine Leerzeichen, keine Unterstriche.
- Dateiendung immer `.md`.
