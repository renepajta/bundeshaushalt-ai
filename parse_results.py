#!/usr/bin/env python
import json

with open('tests/golden_qa_results.json', encoding='utf-8') as f:
    results = json.load(f)

perfect = sum(1 for r in results if r.get('score',{}).get('score',0) >= 1.0)
partial = sum(1 for r in results if 0 < r.get('score',{}).get('score',0) < 1.0)
zero = sum(1 for r in results if r.get('score',{}).get('score',0) == 0)

print(f'GOLDEN QA TEST RESULTS')
print(f'=====================')
print(f'Perfect: {perfect}/16, Partial: {partial}/16, Zero: {zero}/16')
print()
print('PER-QUESTION DETAILS')
print('=' * 80)

total_time = 0
for r in results:
    sc = r.get('score',{}).get('score',0)
    tools = r.get('tools_used',[])
    elapsed = r.get('elapsed_seconds',0)
    total_time += elapsed
    question = r.get('question','')[:60]
    print(f"{r['id']} | score={sc:.0%} | tools={len(tools):2d} | time={elapsed:5.0f}s | {question}")

print()
print(f'Total Time: {total_time:.0f}s ({total_time/60:.1f}m)')
