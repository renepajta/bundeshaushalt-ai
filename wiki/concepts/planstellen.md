---
title: "Planstellen"
type: "concept"
tags: [planstellen, personal, besoldung, tariflich]
last_updated: "2026-04-15"
---

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
Einstufung an, z.\u202fB.:

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
