---
title: "Verrechnungstitel"
type: "concept"
tags: [verrechnungstitel, haushaltssystematik, buchungstechnik]
last_updated: "2026-04-15"
---

## Definition

Verrechnungstitel sind haushaltsinterne Buchungsposten, die Leistungsbeziehungen
**zwischen** verschiedenen Kapiteln oder Einzelplänen des Bundeshaushalts abbilden.
Sie erfassen Erstattungen und Verrechnungen für Leistungen, die ein Ressort für
ein anderes erbringt — z.\u202fB. die zentrale Beschaffung durch das BMI für andere
Ministerien.

## Identifikation

Verrechnungstitel sind in der Datenbank über das Feld `is_verrechnungstitel = 1`
in der Tabelle `haushaltsdaten` gekennzeichnet. Typische Titel-Nummern beginnen
häufig mit **381**, **382**, **527\u202f55** oder gehören zu eigens ausgewiesenen
Verrechnungskapiteln (z.\u202fB. Kapitel X\u202f05).

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
| 381\u202f01 | Erstattung an EP 08 für IT-Leistungen | 14 |
| 527\u202f55 | Verrechnungstitel Sächliche Verwaltungsausgaben | diverse |

## Verwandte Konzepte

- [Deckungsfähigkeit](deckungsfaehigkeit.md)
- [Flexibilisierung](flexibilisierung.md)
- [Haushaltsversionen](haushaltsversionen.md)
