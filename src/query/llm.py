"""Azure OpenAI LLM client routed through Azure API Management (APIM).

The APIM gateway expects the subscription key in the ``api-key`` header, which
the ``openai`` library sends automatically when *api_key* is set on the
:class:`~openai.AzureOpenAI` client.
"""

from __future__ import annotations

import logging
from typing import Any

from openai import AzureOpenAI, APIConnectionError, APIStatusError, RateLimitError

from src.config import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System-prompt templates
# ---------------------------------------------------------------------------

_SQL_SYSTEM_PROMPT = """\
Du bist ein SQL-Experte für den deutschen Bundeshaushalt.

Datenbank-Schema (SQLite):
{schema_description}

Regeln:
- Erzeuge ausschließlich ein einzelnes SELECT-Statement.
- Gib **nur** den SQL-Code zurück – keine Erklärungen, kein Markdown.
- Verwende deutsche Spalten-/Tabellennamen exakt so, wie sie im Schema stehen.
- Nutze für Geldbeträge immer die korrekte Einheit (Tausend Euro, falls angegeben).
- Bei Textfiltern nutze LIKE mit %-Wildcards für unscharfe Suche.
- Begrenze Ergebnisse mit LIMIT, sofern die Frage keine vollständige Auflistung verlangt.
"""

_ANSWER_SYSTEM_PROMPT = """\
Du bist ein erfahrener Haushaltssachbearbeiter des Bundes — jemand, der seit
Jahrzehnten mit den Bundeshaushaltsplänen arbeitet und instinktiv weiß, wo
die relevanten Informationen zu finden sind.

Antworte stets auf Deutsch. Beachte dabei:
- Nenne präzise Zahlen mit korrekter Einheit (Euro, Tsd. Euro, Mio. Euro).
- Zeige Rechenwege, wenn du Werte aggregierst oder vergleichst.
- Zitiere die Quelle (z. B. Einzelplan, Kapitel, Titel), wenn vorhanden.
- Strukturiere längere Antworten mit Aufzählungen oder kurzen Absätzen.
- Wenn die vorhandenen Daten nicht vollständig ausreichen:
  • Liefere die BESTEN verfügbaren Informationen als Ausgangspunkt.
  • Erkläre kurz, welche Einschränkungen bestehen.
  • Schlage konkrete nächste Schritte vor (z.B. "Für die genaue Aufschlüsselung
    könnte man Kapitel 1401 im Detail betrachten.").
  • Sage NIEMALS nur "Daten nicht gefunden" — ein erfahrener Sachbearbeiter hat
    immer einen Hinweis oder Ansatzpunkt.
"""


class LLMClient:
    """Thin wrapper around Azure OpenAI chat completions via APIM."""

    def __init__(self) -> None:
        if not config.AZURE_OPENAI_ENDPOINT:
            raise ValueError(
                "AZURE_OPENAI_ENDPOINT ist nicht konfiguriert. "
                "Bitte in .env oder als Umgebungsvariable setzen."
            )
        if not config.AZURE_OPENAI_API_KEY:
            raise ValueError(
                "AZURE_OPENAI_API_KEY ist nicht konfiguriert. "
                "Bitte in .env oder als Umgebungsvariable setzen."
            )

        self._deployment = config.AZURE_OPENAI_DEPLOYMENT

        # The openai library sends *api_key* as the ``api-key`` header, which
        # is exactly what the APIM gateway expects as subscription key.
        self._client = AzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION,
        )

        logger.debug(
            "LLMClient initialised – endpoint=%s, deployment=%s, api_version=%s",
            config.AZURE_OPENAI_ENDPOINT,
            self._deployment,
            config.AZURE_OPENAI_API_VERSION,
        )

    # ------------------------------------------------------------------
    # Core chat method
    # ------------------------------------------------------------------

    def chat(
        self,
        messages: list[dict[str, Any]],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> str:
        """Send a chat-completion request and return the assistant text.

        Parameters
        ----------
        messages:
            OpenAI-style message list (role / content dicts).
        temperature:
            Sampling temperature (0.0 = deterministic).
        max_tokens:
            Upper bound on response tokens.

        Returns
        -------
        str
            The assistant's response content.
        """
        logger.debug(
            "chat request – %d message(s), temp=%.2f, max_tokens=%d",
            len(messages),
            temperature,
            max_tokens,
        )
        try:
            response = self._client.chat.completions.create(
                model=self._deployment,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        except RateLimitError as exc:
            logger.warning("Rate-limit erreicht: %s", exc)
            raise RuntimeError(
                "Azure OpenAI Rate-Limit erreicht. Bitte kurz warten und erneut versuchen."
            ) from exc
        except APIConnectionError as exc:
            logger.error("Verbindungsfehler: %s", exc)
            raise RuntimeError(
                "Verbindung zu Azure OpenAI (APIM) fehlgeschlagen. "
                "Endpoint und Netzwerkverbindung prüfen."
            ) from exc
        except APIStatusError as exc:
            if exc.status_code in (401, 403):
                logger.error("Authentifizierungsfehler: %s", exc)
                raise RuntimeError(
                    "Authentifizierung fehlgeschlagen (HTTP %d). "
                    "APIM-Subscription-Key prüfen." % exc.status_code
                ) from exc
            logger.error("API-Fehler (HTTP %d): %s", exc.status_code, exc)
            raise RuntimeError(
                "Azure OpenAI API-Fehler (HTTP %d): %s" % (exc.status_code, exc.message)
            ) from exc

        content: str = response.choices[0].message.content or ""
        logger.debug("chat response – %d chars, usage=%s", len(content), response.usage)
        return content

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def chat_with_system(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
    ) -> str:
        """Send a system + user message pair and return the response."""
        return self.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=temperature,
        )

    # ------------------------------------------------------------------
    # Domain-specific methods
    # ------------------------------------------------------------------

    def generate_sql(self, question: str, schema_description: str) -> str:
        """Generate a SQL query from a natural-language question.

        Parameters
        ----------
        question:
            User question in German about the federal budget.
        schema_description:
            Human-readable description of the SQLite schema.

        Returns
        -------
        str
            A raw SQL SELECT statement (no markdown fences).
        """
        system = _SQL_SYSTEM_PROMPT.format(schema_description=schema_description)
        sql = self.chat_with_system(system, question, temperature=0.0)

        # Strip accidental markdown code fences the model may add
        sql = sql.strip()
        if sql.startswith("```"):
            sql = sql.split("\n", 1)[-1]  # drop opening fence line
        if sql.endswith("```"):
            sql = sql.rsplit("```", 1)[0]
        return sql.strip()

    def synthesize_answer(
        self,
        question: str,
        context: str,
        sql_results: str = "",
    ) -> str:
        """Synthesize a final German-language answer.

        Parameters
        ----------
        question:
            The original user question.
        context:
            Relevant background text for context.
        sql_results:
            Formatted table of SQL query results (may be empty).

        Returns
        -------
        str
            A comprehensive answer in German.
        """
        user_parts = [f"Frage: {question}"]
        if context:
            user_parts.append(f"Hintergrund-Informationen:\n{context}")
        if sql_results:
            user_parts.append(f"Datenbank-Ergebnisse:\n{sql_results}")

        return self.chat_with_system(
            _ANSWER_SYSTEM_PROMPT,
            "\n\n".join(user_parts),
            temperature=0.2,
        )


# ---------------------------------------------------------------------------
# Standalone smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    client = LLMClient()
    response = client.chat_with_system(
        "Du bist ein Experte für den deutschen Bundeshaushalt.",
        "Was ist ein Einzelplan im Bundeshaushalt? Antworte in 2 Sätzen.",
    )
    print(response)
