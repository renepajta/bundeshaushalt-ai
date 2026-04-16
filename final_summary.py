#!/usr/bin/env python
import json

with open('tests/golden_qa_results.json', encoding='utf-8') as f:
    results = json.load(f)

print('GOLDEN QA TEST SUITE - FINAL SUMMARY')
print('=' * 100)
print()

perfect = sum(1 for r in results if r.get('score',{}).get('score',0) >= 1.0)
partial = sum(1 for r in results if 0 < r.get('score',{}).get('score',0) < 1.0)
zero = sum(1 for r in results if r.get('score',{}).get('score',0) == 0)
avg_score = sum(r.get('score',{}).get('score',0) for r in results) / len(results)

print('SUMMARY')
print('-' * 100)
print(f'Total Questions: 16')
print(f'Perfect Matches (100%): {perfect}')
print(f'Partial Matches (1-99%): {partial}')
print(f'Failed (0%): {zero}')
print(f'Average Score: {avg_score:.1%}')
print()

print('ANSWER QUALITY BREAKDOWN')
print('-' * 100)
score_dist = {}
for r in results:
    sc = int(r.get('score',{}).get('score',0) * 100)
    score_dist[sc] = score_dist.get(sc, 0) + 1

for score in sorted(score_dist.keys(), reverse=True):
    count = score_dist[score]
    bar = '█' * (count * 3)
    print(f'{score:3d}% | {bar} {count}')

print()
print('PER-QUESTION DETAILS')
print('-' * 100)
print('ID  | Score  | Tools | Time  | Question')
print('-' * 100)

for r in sorted(results, key=lambda x: x.get('score',{}).get('score',0), reverse=True):
    sc = r.get('score',{}).get('score',0)
    tools = len(r.get('tools_used',[]))
    elapsed = r.get('elapsed_seconds',0)
    question = r.get('question','')[:50]
    print(f'{r["id"]} | {sc:5.0%} | {tools:5d} | {elapsed:5.0f}s | {question}...')

print()
print('EXECUTION STATISTICS')
print('-' * 100)
total_time = sum(r.get('elapsed_seconds',0) for r in results)
total_tools = sum(len(r.get('tools_used',[])) for r in results)
avg_tools = total_tools / len(results)
print(f'Total Execution Time: {total_time:.0f}s ({total_time/60:.1f} minutes)')
print(f'Average Time per Question: {total_time/len(results):.1f}s')
print(f'Total Tool Invocations: {total_tools}')
print(f'Average Tools per Question: {avg_tools:.1f}')
