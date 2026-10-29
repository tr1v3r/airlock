#!/usr/bin/env python3
"""Summarize raw measured trials; retain per-trial sample counts and phase boundaries."""
import csv
import gzip
import json
import pathlib
import statistics
import sys
from collections import defaultdict

root = pathlib.Path(sys.argv[1])
rows = []
windows = []
reports = []
budgets = []

def quantiles(v):
    v = sorted(v)
    at = lambda p: v[min(int(len(v)*p),len(v)-1)]
    return [len(v),at(.5),at(.95) if len(v)>=20 else None,at(.99) if len(v)>=100 else None,max(v)]

for path in sorted((root/'raw').glob('*.jsonl*')):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        data = [json.loads(line) for line in stream]
    summary = data[-1]
    if summary.get('Type') != 'summary':
        raise ValueError(f'missing summary: {path}')
    reports.append(dict(File=path.name,**summary))
    for op,result in summary['Latencies'].items():
        rows.append([path.name,summary['Backend'],summary['Scenario'],summary['History'],summary['Capacity'],summary['Workers'],summary['Repeat'],op,result['N'],result['P50'],result['P95'],result['P99'],result['Max']])
    all_attempts = defaultdict(list)
    for sample in data[:-1]:
        if sample['Phase'] == 'measured':
            all_attempts[sample['Op']].append(sample['LatencyMS'])
    budget_values = defaultdict(list)
    for sample in data[:-1]:
        if sample['Phase'] != 'measured' or sample['Op'] != 'renew':
            continue
        result = 'error' if sample['Error'] else 'ok'
        if sample['PriorBudgetMS'] is not None:
            budget_values['old_at_completion/'+result].append(sample['PriorBudgetMS']-sample['LatencyMS'])
        if sample['BudgetMS'] is not None:
            budget_values['new_at_completion/'+result].append(sample['BudgetMS'])
    for kind, values in sorted(budget_values.items()):
        budgets.append([path.name,summary['Backend'],kind,min(values),*quantiles(values)])
    for op, values in sorted(all_attempts.items()):
        rows.append([path.name,summary['Backend'],summary['Scenario'],summary['History'],summary['Capacity'],summary['Workers'],summary['Repeat'],op+'/all',*quantiles(values)])
    if summary['Scenario']=='expiry-gc':
        bins = defaultdict(list)
        for s in data[:-1]:
            if s['Phase']!='measured':
                continue
            # Start-time cohort: the first operation whose evaluation triggers
            # maintenance may complete after the nominal boundary.
            bucket = int((s['StartMS']-summary['ExpiryBoundaryMS'])//500)
            bins[(bucket,s['Op'],'error' if s['Error'] else 'ok')].append(s['LatencyMS'])
        for (bucket,op,result),values in sorted(bins.items()):
            windows.append([path.name,summary['Backend'],summary['Repeat'],bucket*500,(bucket+1)*500,op,result,*quantiles(values)])
(root/'summary.json').write_text(json.dumps(reports,indent=2)+'\n')
for name,header,body in [('latency.csv',['trial','backend','scenario','history','capacity','workers','repeat','operation/result','n','p50_ms','p95_ms','p99_ms','max_ms'],rows),('renewal-budget.csv',['trial','backend','budget/result','min_ms','n','p50_ms','p95_ms','p99_ms','max_ms'],budgets),('expiry-windows.csv',['trial','backend','repeat','start_relative_ms','end_relative_ms','operation','result','n','p50_ms','p95_ms','p99_ms','max_ms'],windows)]:
    with (root/name).open('w',newline='') as f:
        writer=csv.writer(f,lineterminator="\n");writer.writerow(header);writer.writerows(body)
case=defaultdict(list)
for r in reports:
    case[(r['Backend'],r['Scenario'],r['History'],r['Capacity'],r['Workers'])].append(r)
for key,values in case.items():
    latency=[v['Latencies'].get('renew/ok',{}).get('P95') for v in values]
    print(key,'trials',len(values),'ops/s',[round(v['OperationsPerSecond'],1) for v in values],'admit/s',[round(v['AdmissionsPerSecond'],2) for v in values],'renew p95',latency,'errors',[v['Errors'] for v in values],'invariants',[v['InvariantErrors'] for v in values])
