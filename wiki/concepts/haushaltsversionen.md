---
title: "Haushaltsversionen"
type: "concept"
tags: [version, entwurf, beschluss, nachtrag, gesetzgebung]
last_updated: "2026-04-15"
---

## Definition

Der Bundeshaushalt durchläuft mehrere **Versionen** im Verlauf des
Haushaltszyklus. Jede Version kann sich in Zahlen und Struktur von der
vorherigen unterscheiden.

## Versionstypen

| Version | Bedeutung |
|---------|-----------|
| **Entwurf** | Der Regierungsentwurf (RegE), vom Bundeskabinett beschlossen und dem Bundestag zugeleitet. |
| **Beschluss** | Der vom Bundestag verabschiedete und vom Bundesrat gebilligte Haushaltsplan — er hat Gesetzeskraft. |
| **Nachtrag** | Ein Nachtragshaushalt, der den Beschluss im laufenden Jahr ändert (z.\u202fB. für Sondervermögen oder Krisenpakete). Kann nummeriert sein (1.\u202fNachtrag, 2.\u202fNachtrag). |
| **Ist** | Der Rechnungsabschluss — die tatsächlichen Ausgaben und Einnahmen nach Abschluss des Haushaltsjahres. |

## Legislativer Ablauf

1. **Aufstellung** durch das Bundesministerium der Finanzen (BMF) —
   Ressortverhandlungen, Eckwertebeschluss.
2. **Kabinettsbeschluss** → *Entwurf*.
3. **Parlamentarische Beratung** — 1.\u202fLesung, Ausschussberatungen
   (Haushaltsausschuss), Bereinigungssitzung, 2./3.\u202fLesung.
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

- Bei Vergleichen immer **dieselbe Version** verwenden (z.\u202fB. Beschluss 2023
  vs. Beschluss 2024).
- Entwurf und Beschluss können signifikant voneinander abweichen —
  parlamentarische Änderungen beachten.
- Nachträge können große Beträge verschieben (z.\u202fB. Sondervermögen
  Bundeswehr 2022).

## Verwandte Konzepte

- [Verpflichtungsermächtigungen](verpflichtungsermachtigungen.md)
- [Verrechnungstitel](verrechnungstitel.md)
