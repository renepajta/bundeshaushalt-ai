"""Wiki search for the Bundeshaushalt Q&A system.

Provides keyword-based search over the wiki Markdown pages with German-aware
tokenisation, BM25-style scoring, and excerpt extraction.
"""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from pathlib import Path

from src.config import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Simple YAML frontmatter parser (shared with indexer)
# ---------------------------------------------------------------------------

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---", re.DOTALL)


def _parse_frontmatter(text: str) -> dict[str, str]:
    m = _FM_RE.match(text)
    if not m:
        return {}
    result: dict[str, str] = {}
    for line in m.group(1).splitlines():
        line = line.strip()
        if ":" not in line or line.startswith("#"):
            continue
        key, _, val = line.partition(":")
        result[key.strip()] = val.strip().strip('"').strip("'")
    return result


def _strip_frontmatter(text: str) -> str:
    """Return text with YAML frontmatter removed."""
    m = _FM_RE.match(text)
    return text[m.end():].lstrip() if m else text


# ---------------------------------------------------------------------------
# German-aware tokenisation and umlaut normalisation
# ---------------------------------------------------------------------------

_UMLAUT_MAP = str.maketrans({
    "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
    "Ä": "ae", "Ö": "oe", "Ü": "ue",
})

# Common German stop words (short list — keeps implementation simple)
_STOP_WORDS = frozenset(
    "der die das ein eine einer eines einem einen und oder aber auch "
    "ist sind war hat haben wird werden von zu zum zur mit für auf in "
    "im an am als ob so wie was wer den dem des noch nicht nach bei "
    "über aus vor kann durch ihre sein".split()
)


def _normalise(text: str) -> str:
    """Lowercase, resolve umlauts, keep only alphanumeric + spaces."""
    text = text.lower().translate(_UMLAUT_MAP)
    return re.sub(r"[^a-z0-9äöüß ]+", " ", text)


def _tokenize(text: str) -> list[str]:
    """German-aware tokenisation: normalise, split, drop stop words."""
    tokens = _normalise(text).split()
    return [t for t in tokens if t not in _STOP_WORDS and len(t) > 1]


# ---------------------------------------------------------------------------
# Search result data class
# ---------------------------------------------------------------------------


@dataclass
class SearchResult:
    """A single search hit."""
    page_path: Path
    title: str
    relevance_score: float
    excerpt: str


# ---------------------------------------------------------------------------
# WikiSearch
# ---------------------------------------------------------------------------


class WikiSearch:
    """Keyword-based search over the Bundeshaushalt wiki pages."""

    # Weights for different sections of a page
    _TITLE_WEIGHT = 4.0
    _TAG_WEIGHT = 3.0
    _HEADING_WEIGHT = 2.0
    _BODY_WEIGHT = 1.0

    def __init__(self, wiki_dir: Path | None = None):
        self.wiki_dir = Path(wiki_dir or config.WIKI_DIR)
        self._pages: list[dict] | None = None  # lazy cache

    # ------------------------------------------------------------------
    # Page loading
    # ------------------------------------------------------------------

    def _load_pages(self) -> list[dict]:
        """Read all wiki pages into memory (cached)."""
        if self._pages is not None:
            return self._pages

        pages: list[dict] = []
        for md_path in sorted(self.wiki_dir.rglob("*.md")):
            if md_path.name in ("index.md", "log.md"):
                continue
            try:
                raw = md_path.read_text(encoding="utf-8")
            except Exception:
                continue

            fm = _parse_frontmatter(raw)
            body = _strip_frontmatter(raw)

            pages.append({
                "path": md_path,
                "title": fm.get("title", md_path.stem),
                "tags": fm.get("tags", ""),
                "type": fm.get("type", ""),
                "raw": raw,
                "body": body,
            })

        self._pages = pages
        logger.debug("Loaded %d wiki pages for search.", len(pages))
        return pages

    def invalidate_cache(self) -> None:
        """Force re-reading pages on next search."""
        self._pages = None

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score_page(self, query_tokens: list[str], page: dict) -> float:
        """BM25-inspired relevance score for a page against query tokens."""
        title_tokens = _tokenize(page["title"])
        tag_tokens = _tokenize(page["tags"])
        body_text = _normalise(page["body"])
        body_tokens = body_text.split()

        # Extract headings from body
        headings = " ".join(
            line.lstrip("#").strip()
            for line in page["body"].splitlines()
            if line.startswith("#")
        )
        heading_tokens = _tokenize(headings)

        doc_len = len(body_tokens) or 1
        avg_dl = 500  # rough average document length
        k1 = 1.5
        b = 0.75

        score = 0.0
        for qt in query_tokens:
            # Term frequency in each zone
            tf_title = sum(1 for t in title_tokens if qt in t or t in qt)
            tf_tags = sum(1 for t in tag_tokens if qt in t or t in qt)
            tf_heading = sum(1 for t in heading_tokens if qt in t or t in qt)
            tf_body = sum(1 for t in body_tokens if qt in t or t in qt)

            # Weighted TF
            tf = (
                tf_title * self._TITLE_WEIGHT
                + tf_tags * self._TAG_WEIGHT
                + tf_heading * self._HEADING_WEIGHT
                + tf_body * self._BODY_WEIGHT
            )

            if tf == 0:
                # Also try substring match on the full body (compound words)
                if qt in body_text:
                    tf = self._BODY_WEIGHT
                else:
                    continue

            # BM25-style normalisation
            norm_tf = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avg_dl))
            score += norm_tf

        # Exact-match boost: if query token appears in the page filename,
        # this is very likely the authoritative page for that concept.
        stem = page["path"].stem.replace("-", "")
        for qt in query_tokens:
            if qt in stem:
                score *= 1.5

        return score

    # ------------------------------------------------------------------
    # Excerpt extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_excerpt(text: str, query_tokens: list[str], window: int = 200) -> str:
        """Find the best excerpt around the first matching keyword."""
        lower = _normalise(text)

        best_pos = -1
        for qt in query_tokens:
            pos = lower.find(qt)
            if pos != -1:
                best_pos = pos
                break

        if best_pos == -1:
            # No direct match — return start of body
            return text[:window].strip() + "…" if len(text) > window else text.strip()

        start = max(0, best_pos - window // 2)
        end = min(len(text), best_pos + window // 2)

        excerpt = text[start:end].strip()
        if start > 0:
            excerpt = "…" + excerpt
        if end < len(text):
            excerpt = excerpt + "…"

        return excerpt

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(self, query: str, max_results: int = 5) -> list[SearchResult]:
        """Search wiki pages for relevant content.

        Parameters
        ----------
        query:
            Natural-language query in German.
        max_results:
            Maximum number of results to return.

        Returns
        -------
        list[SearchResult]
            Ranked list of matching pages.
        """
        query_tokens = _tokenize(query)
        if not query_tokens:
            return []

        # Also add umlaut-resolved variants
        expanded: list[str] = list(query_tokens)
        for t in query_tokens:
            resolved = t.translate(_UMLAUT_MAP)
            if resolved != t and resolved not in expanded:
                expanded.append(resolved)
        query_tokens = expanded

        pages = self._load_pages()
        scored: list[tuple[float, dict]] = []
        for page in pages:
            score = self._score_page(query_tokens, page)
            if score > 0:
                scored.append((score, page))

        scored.sort(key=lambda x: x[0], reverse=True)

        results: list[SearchResult] = []
        for score, page in scored[:max_results]:
            excerpt = self._extract_excerpt(page["body"], query_tokens)
            results.append(SearchResult(
                page_path=page["path"],
                title=page["title"],
                relevance_score=round(score, 3),
                excerpt=excerpt,
            ))

        return results

    def get_context_for_query(self, query: str, max_chars: int = 4000) -> str:
        """Get relevant wiki context formatted for LLM consumption.

        Searches the wiki, reads the top pages, and concatenates them into a
        single context string that fits within *max_chars*.

        Parameters
        ----------
        query:
            User question in German.
        max_chars:
            Approximate character budget for the context window.

        Returns
        -------
        str
            Formatted context string (may be empty if nothing matches).
        """
        results = self.search(query, max_results=5)
        if not results:
            return ""

        parts: list[str] = []
        budget = max_chars

        for r in results:
            header = f"### {r.title} (Relevanz: {r.relevance_score})\n"
            try:
                body = r.page_path.read_text(encoding="utf-8")
                body = _strip_frontmatter(body)
            except Exception:
                body = r.excerpt

            chunk = header + body
            if len(chunk) > budget:
                # Truncate to fit budget
                chunk = chunk[:budget] + "\n[…gekürzt…]"
                parts.append(chunk)
                break
            parts.append(chunk)
            budget -= len(chunk)
            if budget <= 0:
                break

        return "\n\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# Standalone smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    ws = WikiSearch()

    test_queries = [
        "Verrechnungstitel",
        "Flexibilisierung Haushalt",
        "Verpflichtungsermächtigungen Verteidigung",
        "Planstellen Besoldungsgruppe",
        "Nachtrag Entwurf Beschluss",
        "Deckungsfähigkeit Titel",
    ]

    for q in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {q}")
        print(f"{'='*60}")
        results = ws.search(q, max_results=3)
        if not results:
            print("  (keine Treffer)")
        for r in results:
            print(f"  [{r.relevance_score:.2f}] {r.title}")
            print(f"         {r.excerpt[:100]}…")

    print(f"\n{'='*60}")
    print("Context for: 'Was sind Verrechnungstitel?'")
    print(f"{'='*60}")
    ctx = ws.get_context_for_query("Was sind Verrechnungstitel?", max_chars=1500)
    print(ctx[:500] + "…" if len(ctx) > 500 else ctx)
