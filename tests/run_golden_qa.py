"""Run all 16 golden Q&A pairs through the Bundeshaushalt Q&A engine.

Evaluates each question, distinguishing between:
- Answerable with current data (years 2005-2026, soll/beschluss, 153K entries)
- Partial: data exists but some aspects may be incomplete
- Data gap: requires data not in the database
"""

import json
import logging
import re
import sys
import time
from itertools import combinations
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
)
logger = logging.getLogger("golden_qa")

# Pre-classify which questions are answerable with current DB state
# DB contents: years 2005-2026, versions soll/beschluss, ~153K haushaltsdaten,
# ~110K personalhaushalt entries, kapitel_meta, referenzdaten
DATA_AVAILABILITY = {
    "q01": {"answerable": True, "reason": "2021+2022 EP06 data available in DB"},
    "q02": {"answerable": True, "reason": "Soll data available; documents cover Ist context"},
    "q03": {"answerable": True, "reason": "2025 Kap 1101 data + BIP in referenzdaten"},
    "q04": {"answerable": True, "reason": "2024 data available; documents provide inflation context"},
    "q05": {"answerable": True, "reason": "2012 data available; documents cover Reservedienstleistende"},
    "q06": {"answerable": True, "reason": "kapitel_meta 2005-2026 available + SemanticBridge"},
    "q07": {"answerable": True, "reason": "kapitel_meta 2005-2026 available + SemanticBridge"},
    "q08": {"answerable": "partial", "reason": "2024 beschluss data available; entwurf needs version_detail"},
    "q09": {"answerable": True, "reason": "2024 Kap 1403 data + documents for deckungsfaehig"},
    "q10": {"answerable": "partial", "reason": "2024 data available; version_detail tracking limited"},
    "q11": {"answerable": True, "reason": "2005-2026 haushaltsdaten available for cross-EP comparison"},
    "q12": {"answerable": True, "reason": "VE data in documents for 2020"},
    "q13": {"answerable": True, "reason": "2020 Kap 0455 personnel data available"},
    "q14": {"answerable": True, "reason": "Documents contain Sachverhalte/Erläuterungen"},
    "q15": {"answerable": True, "reason": "2024 Kap 1513 personnel data available"},
    "q16": {"answerable": "partial", "reason": "2024 entwurf+beschluss in DB; requires version comparison query"},
}


def _flatten_key_figures(figures, prefix=""):
    """Recursively flatten nested key_figures dict."""
    result = []
    for k, v in figures.items():
        name = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            result.extend(_flatten_key_figures(v, f"{name}."))
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    result.extend(_flatten_key_figures(item, f"{name}[{i}]."))
                else:
                    result.append((f"{name}[{i}]", item))
        else:
            result.append((name, v))
    return result


def _extract_numbers_from_text(text: str) -> list[float]:
    """Extract all numbers from text, handling German formatting."""
    numbers = []
    # Match German formatted numbers: 16.161.139 or 16.161.139,5 or 138.556
    for m in re.finditer(r'[\d]+(?:\.[\d]{3})*(?:,[\d]+)?', text):
        s = m.group().replace('.', '').replace(',', '.')
        try:
            numbers.append(float(s))
        except ValueError:
            pass
    # Also match plain numbers: 16161139, 138556, -15.12
    for m in re.finditer(r'-?[\d]+(?:\.[\d]+)?', text):
        try:
            numbers.append(float(m.group()))
        except ValueError:
            pass
    return list(set(numbers))


def _string_match_number(value, answer: str) -> bool:
    """Try various string representations of a number."""
    answer_clean = answer.replace(' ', '')

    if isinstance(value, float) and value == int(value):
        value = int(value)

    formats = []
    if isinstance(value, int):
        formats.append(str(value))
        # German format with dots
        formats.append(f'{value:,}'.replace(',', '.'))
        # Without separators
        formats.append(str(abs(value)))
    elif isinstance(value, float):
        formats.append(str(value))
        formats.append(f'{value:.2f}'.replace('.', ','))
        formats.append(f'{value:.1f}'.replace('.', ','))
        # Percentage format
        if abs(value) < 200:
            formats.append(f'{abs(value):.2f}'.replace('.', ','))
            formats.append(f'{abs(value):.1f}'.replace('.', ','))

    for fmt in formats:
        if fmt in answer_clean or fmt in answer:
            return True
    return False


def _check_component_sum(expected_value, numbers_in_answer, tolerance=0.01):
    """Check if any combination of found numbers sums to expected."""
    if not isinstance(expected_value, (int, float)) or expected_value == 0:
        return False
    for size in range(2, min(5, len(numbers_in_answer) + 1)):
        for combo in combinations(numbers_in_answer, size):
            if abs(sum(combo) - expected_value) / max(abs(expected_value), 1) < tolerance:
                return True
    return False


def _value_in_answer(value, answer: str) -> bool:
    """Check if a value appears in the answer text with fuzzy tolerance."""
    if isinstance(value, (int, float)):
        # Extract all numbers from the answer
        numbers_in_answer = _extract_numbers_from_text(answer)

        target = float(value)
        for num in numbers_in_answer:
            # Exact match
            if abs(num - target) < 0.01:
                return True
            # ±5% tolerance for large numbers
            if target != 0 and abs(num) > 100:
                if abs(num - target) / abs(target) < 0.05:
                    return True
            # Check if it's the same number in different units (thousands vs millions)
            if target != 0:
                for multiplier in [1, 1000, 0.001]:
                    if abs(num * multiplier - target) / abs(target) < 0.01:
                        return True

        # Also try string matching (German format)
        if _string_match_number(value, answer):
            return True

        # Check if components in the answer sum to the expected value
        if _check_component_sum(target, numbers_in_answer):
            return True

        return False

    elif isinstance(value, str):
        # Year-range matching with ±1 year tolerance on boundaries
        m = re.match(r'^(\d{4})-(\d{4})$', value)
        if m:
            start_y, end_y = int(m.group(1)), int(m.group(2))
            if value in answer:
                return True
            for s_off in [-1, 0, 1]:
                for e_off in [-1, 0, 1]:
                    variant = f"{start_y + s_off}-{end_y + e_off}"
                    if variant in answer:
                        return True
            # Also accept if both boundary years appear separately
            if str(start_y) in answer and str(end_y) in answer:
                return True
            return False

        # Kapitel code matching: strip leading zeros for comparison
        if re.match(r'^\d{4}$', value):
            if value in answer:
                return True
            stripped = value.lstrip('0') or '0'
            if stripped in answer:
                return True
            return False

        return value.lower() in answer.lower()
    return False


def score_answer(qa: dict, actual_answer: str) -> dict:
    """Score an answer against expected key figures.

    Returns: {"total_figures": N, "found": N, "missing": [...], "score": 0.0-1.0}
    """
    key_figures = qa.get("key_figures", {})
    if not key_figures:
        return {"total_figures": 0, "found": 0, "missing": [], "score": 1.0}

    total = 0
    found = 0
    missing = []

    # Flatten key_figures (may be nested)
    flat_values = _flatten_key_figures(key_figures)

    for name, value in flat_values:
        total += 1
        # Check if the value appears in the answer (with tolerance)
        if _value_in_answer(value, actual_answer):
            found += 1
        else:
            missing.append(f"{name}={value}")

    return {
        "total_figures": total,
        "found": found,
        "missing": missing,
        "score": found / total if total > 0 else 1.0,
    }


def run_golden_qa():
    """Run all golden Q&A pairs and evaluate results."""
    # Load golden Q&A pairs
    golden_path = Path(__file__).parent / "golden_qa.json"
    with open(golden_path, encoding="utf-8") as f:
        golden = json.load(f)

    logger.info("Loaded %d golden Q&A pairs", len(golden))

    # Create engine
    from src.query.engine import create_engine

    engine = create_engine()

    results = []
    for i, qa in enumerate(golden):
        qid = qa["id"]
        question = qa["question"]
        expected = qa["expected_answer"]
        availability = DATA_AVAILABILITY.get(qid, {})

        print(f"\n{'=' * 70}")
        print(f"[{i+1}/{len(golden)}] {qid}: {question[:80]}...")
        print(f"  Data available: {availability.get('answerable', 'unknown')}")
        print(f"  Reason: {availability.get('reason', 'N/A')}")
        print(f"  Expected: {expected[:100]}...")
        print("-" * 70)

        start_time = time.time()
        try:
            result = engine.ask(question)
            elapsed = time.time() - start_time

            print(f"  Answer ({elapsed:.1f}s): {result.answer[:200]}...")
            print(f"  Tools: {result.tools_used}")
            print(f"  Confidence: {result.confidence}")
            if result.sql_queries:
                for sq in result.sql_queries:
                    print(f"  SQL: {sq[:120]}...")

            # Determine status
            is_answerable = availability.get("answerable", False)
            if is_answerable is False:
                # Question needs data not in DB
                answer_lower = result.answer.lower()
                acknowledges_gap = any(
                    phrase in answer_lower
                    for phrase in [
                        "nicht verfügbar",
                        "keine daten",
                        "nicht vorhanden",
                        "nicht in der datenbank",
                        "nur 2026",
                        "nur das jahr 2026",
                        "keine ergebnisse",
                        "nicht enthalten",
                        "nicht gefunden",
                        "liegen nicht vor",
                        "stehen nicht zur verfügung",
                        "keine passenden",
                        "0 zeile",
                        "leer",
                    ]
                )
                status = "data_gap_acknowledged" if acknowledges_gap else "data_gap_not_acknowledged"
            elif is_answerable == "partial":
                status = "partial_data"
            else:
                status = "answerable"

            score = score_answer(qa, result.answer)

            results.append(
                {
                    "id": qid,
                    "question": question,
                    "category": qa.get("category", ""),
                    "difficulty": qa.get("difficulty", ""),
                    "expected": expected,
                    "actual_answer": result.answer,
                    "tools_used": result.tools_used,
                    "sql_queries": result.sql_queries,
                    "sources": result.sources,
                    "confidence": result.confidence,
                    "elapsed_seconds": round(elapsed, 1),
                    "data_available": availability.get("answerable", "unknown"),
                    "data_reason": availability.get("reason", ""),
                    "status": status,
                    "score": score,
                }
            )
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error("ERROR on %s: %s", qid, e)
            print(f"  ERROR ({elapsed:.1f}s): {e}")
            results.append(
                {
                    "id": qid,
                    "question": question,
                    "category": qa.get("category", ""),
                    "difficulty": qa.get("difficulty", ""),
                    "expected": expected,
                    "actual_answer": "",
                    "tools_used": [],
                    "sql_queries": [],
                    "sources": [],
                    "confidence": "none",
                    "elapsed_seconds": round(elapsed, 1),
                    "data_available": availability.get("answerable", "unknown"),
                    "data_reason": availability.get("reason", ""),
                    "status": "error",
                    "error": str(e),
                    "score": None,
                }
            )

        # Brief pause between questions to avoid rate limits
        if i < len(golden) - 1:
            time.sleep(2)

    # Save results
    output_path = Path(__file__).parent / "golden_qa_results.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info("Results saved to %s", output_path)

    # Print summary
    print_summary(results)


def print_summary(results: list[dict]):
    """Print a human-readable summary of the validation run."""
    print(f"\n{'=' * 70}")
    print("GOLDEN Q&A VALIDATION SUMMARY")
    print(f"{'=' * 70}")

    total = len(results)
    errors = [r for r in results if r["status"] == "error"]
    data_gap_ack = [r for r in results if r["status"] == "data_gap_acknowledged"]
    data_gap_not_ack = [r for r in results if r["status"] == "data_gap_not_acknowledged"]
    partial = [r for r in results if r["status"] == "partial_data"]
    answerable = [r for r in results if r["status"] == "answerable"]

    print(f"\nTotal questions: {total}")
    print(f"  Errors (engine failure):           {len(errors)}")
    print(f"  Data gap acknowledged:             {len(data_gap_ack)}")
    print(f"  Data gap NOT acknowledged:         {len(data_gap_not_ack)}")
    print(f"  Partial data:                      {len(partial)}")
    print(f"  Fully answerable:                  {len(answerable)}")

    # Tool usage stats
    all_tools = []
    for r in results:
        all_tools.extend(r.get("tools_used", []))
    tool_counts = {}
    for t in all_tools:
        tool_counts[t] = tool_counts.get(t, 0) + 1

    print(f"\nTool usage across all questions:")
    for tool, count in sorted(tool_counts.items(), key=lambda x: -x[1]):
        print(f"  {tool}: {count} calls")

    # Confidence distribution
    conf_counts = {}
    for r in results:
        c = r.get("confidence", "none")
        conf_counts[c] = conf_counts.get(c, 0) + 1
    print(f"\nConfidence distribution:")
    for c, count in sorted(conf_counts.items()):
        print(f"  {c}: {count}")

    # Timing
    times = [r["elapsed_seconds"] for r in results if r.get("elapsed_seconds")]
    if times:
        print(f"\nTiming:")
        print(f"  Average: {sum(times)/len(times):.1f}s")
        print(f"  Min:     {min(times):.1f}s")
        print(f"  Max:     {max(times):.1f}s")
        print(f"  Total:   {sum(times):.1f}s")

    # Answer quality scores
    print(f"\nAnswer Quality (key figure matching):")
    scored = [r for r in results if r.get("score") is not None]
    if scored:
        avg_score = sum(r["score"]["score"] for r in scored) / len(scored)
        perfect = sum(1 for r in scored if r["score"]["score"] == 1.0)
        partial_score = sum(1 for r in scored if 0 < r["score"]["score"] < 1.0)
        zero = sum(1 for r in scored if r["score"]["score"] == 0.0)
        print(f"  Average score: {avg_score:.1%}")
        print(f"  Perfect (100%): {perfect}")
        print(f"  Partial (>0%):  {partial_score}")
        print(f"  Zero (0%):      {zero}")

    # Details for each question
    print(f"\n{'=' * 70}")
    print("PER-QUESTION DETAILS")
    print(f"{'=' * 70}")
    for r in results:
        status_emoji = {
            "error": "❌",
            "data_gap_acknowledged": "✅",
            "data_gap_not_acknowledged": "⚠️",
            "partial_data": "🔶",
            "answerable": "🟢",
        }.get(r["status"], "❓")
        score_str = ""
        if r.get("score"):
            s = r["score"]
            score_str = f" score={s['score']:.0%} ({s['found']}/{s['total_figures']})"
            if s["missing"]:
                score_str += f" missing=[{', '.join(s['missing'][:3])}]"
        print(
            f"  {status_emoji} {r['id']}: {r['status']}{score_str} "
            f"(tools={r.get('tools_used', [])}, "
            f"conf={r.get('confidence', '?')}, "
            f"{r.get('elapsed_seconds', 0):.1f}s)"
        )
        if r["status"] == "error":
            print(f"      Error: {r.get('error', 'unknown')[:100]}")
        if r["status"] == "data_gap_not_acknowledged":
            ans = r.get("actual_answer", "")
            print(f"      Answer snippet: {ans[:150]}...")


if __name__ == "__main__":
    run_golden_qa()
