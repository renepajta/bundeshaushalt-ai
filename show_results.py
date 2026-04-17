import json
with open('tests/golden_qa_results.json', encoding='utf-8') as f:
    results = json.load(f)

perfect = sum(1 for r in results if r.get('score',{}).get('score',0) >= 1.0)
partial = sum(1 for r in results if 0 < r.get('score',{}).get('score',0) < 1.0)
zero = sum(1 for r in results if r.get('score',{}).get('score',0) == 0)
avg = sum(r.get('score',{}).get('score',0) for r in results) / len(results) * 100

print(f'Perfect: {perfect}/16, Partial: {partial}/16, Zero: {zero}/16, Avg: {avg:.0f}%')
print()
for r in results:
    sc = r.get('score',{}).get('score',0)
    tools = r.get('tools_used',[])
    elapsed = r.get('elapsed_seconds',0)
    missing = r.get('score',{}).get('missing',[])
    status = 'PERFECT' if sc >= 1.0 else f'{sc:.0%}' if sc > 0 else 'ZERO'
    miss_str = f' missing={missing[:2]}' if missing else ''
    print(f'  {r["id"]} {status:>8} {elapsed:>5.0f}s tools={tools}{miss_str}')
