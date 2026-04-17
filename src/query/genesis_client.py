"""GENESIS-Online REST API client for the Bundeshaushalt Q&A system.

Provides access to macroeconomic reference data from the official German
Federal Statistical Office (Statistisches Bundesamt / Destatis) database.

The GENESIS-Online API requires a registered account (free) for data access.
Set GENESIS_USERNAME and GENESIS_PASSWORD in .env, or provide an API token
via GENESIS_API_TOKEN. If no credentials are configured, all lookups return
None and the caller falls back to alternative sources.

API documentation:
    https://www-genesis.destatis.de/datenbank/online/docs/GENESIS-Webservices_Introduction.pdf
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
from typing import Any

import requests

from src.config import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GENESIS API configuration
# ---------------------------------------------------------------------------

_BASE_URL = "https://www-genesis.destatis.de/genesisWS/rest/2020/"
_TIMEOUT = 15  # seconds

# Mapping of our indicator names to GENESIS table codes and extraction hints.
# Each entry: (table_code, value_column_pattern, unit)
_INDICATOR_TABLES: dict[str, dict[str, Any]] = {
    "BIP": {
        "table": "81000-0001",
        "description": "Bruttoinlandsprodukt (nominal)",
        "unit": "Mrd. €",
        "value_hint": "jeweiligen Preisen",
    },
    "Bevoelkerung": {
        "table": "12411-0001",
        "description": "Bevölkerungsstand",
        "unit": "",
        "value_hint": "Bevölkerung",
    },
    "Inflationsrate": {
        "table": "61111-0001",
        "description": "Verbraucherpreisindex",
        "unit": "%",
        "value_hint": "Veränderung",
    },
}


class GenesisClient:
    """Lightweight client for the GENESIS-Online REST API (POST-only since July 2025)."""

    def __init__(self) -> None:
        # Credentials: prefer API token, fall back to username/password
        self._token = getattr(config, "GENESIS_API_TOKEN", "") or ""
        self._username = getattr(config, "GENESIS_USERNAME", "") or ""
        self._password = getattr(config, "GENESIS_PASSWORD", "") or ""
        self._available = bool(self._token or self._username)

        # Simple in-memory cache: (indicator, year) → formatted result string
        self._cache: dict[tuple[str, int | None], str | None] = {}

        if self._available:
            logger.info("GenesisClient initialised (token=%s, user=%s)",
                        "yes" if self._token else "no",
                        self._username or "(none)")
        else:
            logger.debug("GenesisClient: no credentials configured — GENESIS lookups disabled")

    @property
    def available(self) -> bool:
        """Whether GENESIS credentials are configured."""
        return self._available

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        """Build request headers with authentication."""
        h: dict[str, str] = {"Content-Type": "application/x-www-form-urlencoded"}
        if self._token:
            h["username"] = self._token
            h["password"] = ""
        elif self._username:
            h["username"] = self._username
            h["password"] = self._password
        return h

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(
        self,
        indicator: str,
        year: int | None = None,
        year_range: list[int] | None = None,
    ) -> str | None:
        """Look up a macroeconomic indicator from GENESIS-Online.

        Returns a formatted result string, or None if unavailable.
        """
        if not self._available:
            return None

        # Resolve year range
        if year_range and len(year_range) == 2:
            years = list(range(year_range[0], year_range[1] + 1))
        elif year:
            years = [year]
        else:
            years = None

        # Check cache
        cache_key = (indicator, year)
        if cache_key in self._cache:
            return self._cache[cache_key]

        table_info = _INDICATOR_TABLES.get(indicator)
        if not table_info:
            logger.debug("No GENESIS table mapping for indicator '%s'", indicator)
            return None

        try:
            result = self._fetch_table_data(
                table_code=table_info["table"],
                start_year=min(years) if years else None,
                end_year=max(years) if years else None,
            )
            if result is None:
                self._cache[cache_key] = None
                return None

            formatted = self._format_result(
                indicator, result, table_info, years
            )
            self._cache[cache_key] = formatted
            return formatted

        except Exception as exc:
            logger.warning("GENESIS lookup failed for %s: %s", indicator, exc)
            self._cache[cache_key] = None
            return None

    # ------------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------------

    def _fetch_table_data(
        self,
        table_code: str,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> str | None:
        """Fetch table data as flat CSV text from GENESIS-Online."""
        data: dict[str, Any] = {
            "name": table_code,
            "compress": "false",
            "format": "ffcsv",
            "language": "de",
        }
        if start_year:
            data["startyear"] = str(start_year)
        if end_year:
            data["endyear"] = str(end_year)

        try:
            response = requests.post(
                _BASE_URL + "data/tablefile",
                headers=self._build_headers(),
                data=data,
                timeout=_TIMEOUT,
            )
        except requests.RequestException as exc:
            logger.warning("GENESIS request failed: %s", exc)
            return None

        if response.status_code != 200:
            logger.warning(
                "GENESIS returned HTTP %d for table %s: %s",
                response.status_code, table_code,
                response.text[:200],
            )
            return None

        content_type = response.headers.get("content-type", "")

        # Handle ZIP-compressed response
        if "zip" in content_type or "octet-stream" in content_type:
            try:
                zf = zipfile.ZipFile(io.BytesIO(response.content))
                csv_name = zf.namelist()[0]
                return zf.read(csv_name).decode("utf-8", errors="replace")
            except Exception as exc:
                logger.warning("Failed to unzip GENESIS response: %s", exc)
                return None

        # Plain text/CSV response
        if response.text and not response.text.strip().startswith("{"):
            return response.text

        # JSON error response
        logger.warning("GENESIS returned unexpected response for %s", table_code)
        return None

    # ------------------------------------------------------------------
    # Result parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _format_result(
        indicator: str,
        csv_text: str,
        table_info: dict[str, Any],
        years: list[int] | None,
    ) -> str | None:
        """Extract relevant values from GENESIS ffcsv data."""
        lines = csv_text.strip().splitlines()
        if len(lines) < 2:
            return None

        # ffcsv format: semicolon-delimited, first row is header
        header = lines[0]

        # Find data rows matching the requested years
        results: list[str] = []
        unit = table_info.get("unit", "")

        for line in lines[1:]:
            fields = line.split(";")
            if len(fields) < 3:
                continue

            # Look for year in the fields (GENESIS uses Zeit_Label or similar)
            line_year = None
            for f in fields:
                f_clean = f.strip().strip('"')
                # Match 4-digit year
                m = re.match(r"^(\d{4})$", f_clean)
                if m:
                    line_year = int(m.group(1))
                    break

            if line_year is None:
                continue
            if years and line_year not in years:
                continue

            # Extract the last numeric field as the value
            value = None
            for f in reversed(fields):
                f_clean = f.strip().strip('"').replace(",", ".")
                try:
                    value = float(f_clean)
                    break
                except ValueError:
                    continue

            if value is not None:
                if unit:
                    results.append(f"{line_year}: {value:,.2f} {unit}")
                else:
                    results.append(f"{line_year}: {value:,.0f}")

        if not results:
            return None

        return (
            f"GENESIS-Online ({table_info['description']}, "
            f"Tabelle {table_info['table']}):\n"
            + "\n".join(results)
            + "\n(Quelle: Statistisches Bundesamt / GENESIS-Online)"
        )

    # ------------------------------------------------------------------
    # Convenience: check if GENESIS can find a table for a term
    # ------------------------------------------------------------------

    def search_tables(self, term: str, limit: int = 3) -> list[dict[str, str]]:
        """Search GENESIS for tables matching a term (works without full credentials)."""
        try:
            response = requests.post(
                _BASE_URL + "find/find",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "username": self._username or "guest",
                    "password": self._password or "",
                    "term": term,
                    "category": "tables",
                    "pagelength": str(limit),
                    "language": "de",
                },
                timeout=_TIMEOUT,
            )
            if response.status_code == 200:
                data = response.json()
                tables = data.get("Tables") or []
                return [
                    {"code": t.get("Code", ""), "content": t.get("Content", "")}
                    for t in tables[:limit]
                ]
        except Exception as exc:
            logger.debug("GENESIS search failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Standalone smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

    client = GenesisClient()
    print(f"Available: {client.available}")

    # Test search (works without credentials)
    results = client.search_tables("Bruttoinlandsprodukt")
    print(f"\nSearch results for 'Bruttoinlandsprodukt':")
    for r in results:
        print(f"  {r['code']}: {r['content'][:80]}")

    # Test data lookup (requires credentials)
    if client.available:
        result = client.lookup("BIP", year=2023)
        print(f"\nBIP 2023: {result}")
    else:
        print("\nNo GENESIS credentials configured — data lookup skipped")
