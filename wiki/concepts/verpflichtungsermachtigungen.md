---
title: "Verpflichtungsermächtigungen"
type: "concept"
tags: [verpflichtungsermaechtigung, ve, haushaltsbindung, zukunft]
last_updated: "2026-04-15"
---

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
  Beschaffungsprogramme (z.\u202fB. Rüstungsprojekte im EP 14).
- Bei Haushaltsanalysen sollten VE getrennt von den laufenden Ausgaben (Soll/Ist)
  betrachtet werden.

## Verwandte Konzepte

- [Haushaltsversionen](haushaltsversionen.md)
- [Deckungsfähigkeit](deckungsfaehigkeit.md)
