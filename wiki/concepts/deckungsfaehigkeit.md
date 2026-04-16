---
title: "Deckungsfähigkeit"
type: "concept"
tags: [deckungsfaehigkeit, haushaltsfuehrung, haushaltsrecht]
last_updated: "2026-04-15"
---

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
