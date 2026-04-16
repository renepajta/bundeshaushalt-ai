#!/usr/bin/env python
import json

with open('tests/golden_qa_results.json', encoding='utf-8') as f:
    results = json.load(f)

print('DETAILED RESULTS BREAKDOWN')
print('=' * 100)
print()

# Group by score ranges
perfect = [r for r in results if r.get('score',{}).get('score',0) >= 1.0]
high = [r for r in results if 0.75 <= r.get('score',{}).get('score',0) < 1.0]
med = [r for r in results if 0.5 <= r.get('score',{}).get('score',0) < 0.75]
low = [r for r in results if 0 < r.get('score',{}).get('score',0) < 0.5]
zero = [r for r in results if r.get('score',{}).get('score',0) == 0]

print(f'✓ Perfect (100%): {len(perfect)} questions')
for r in perfect:
    print(f'  • {r["id"]}: {r.get("question","")[:70]}')

if high:
    print(f'\n+ High (75-99%): {len(high)} questions')
    for r in high:
        sc = r.get('score',{}).get('score',0)
        print(f'  • {r["id"]} ({sc:.0%}): {r.get("question","")[:60]}')

if med:
    print(f'\n◐ Medium (50-74%): {len(med)} questions')
    for r in med:
        sc = r.get('score',{}).get('score',0)
        print(f'  • {r["id"]} ({sc:.0%}): {r.get("question","")[:60]}')

if low:
    print(f'\n◑ Low (1-49%): {len(low)} questions')
    for r in low:
        sc = r.get('score',{}).get('score',0)
        print(f'  • {r["id"]} ({sc:.0%}): {r.get("question","")[:60]}')

if zero:
    print(f'\n✗ Zero (0%): {len(zero)} questions')
    for r in zero:
        print(f'  • {r["id"]}: {r.get("question","")[:70]}')

print()
print('STATISTICS')
print('=' * 100)
avg_score = sum(r.get('score',{}).get('score',0) for r in results) / len(results)
print(f'Average Score: {avg_score:.1%}')
print(f'Total Questions: {len(results)}')
