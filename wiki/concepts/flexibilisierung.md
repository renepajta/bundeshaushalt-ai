---
title: "Flexibilisierung"
type: "concept"
tags: [flexibilisierung, haushaltsfuehrung, uebertragbarkeit]
last_updated: "2026-04-15"
---

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
