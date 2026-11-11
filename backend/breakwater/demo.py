"""Deterministic synthetic fixtures, with an independently generated answer manifest."""
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
from decimal import Decimal

from breakwater.domain import INTERNAL,BROKER,INSTRUMENTS


def fixture(side,scenario='breaks',count=240,revision=1,business_date='2026-11-06',dataset='equities_accounts_01'):
    symbols=['AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','JPM','V']
    rows=[]
    for i in range(count):
        rows.append([f'T{1001+i}','ACC01' if i%3 else 'ACC02',symbols[i%len(symbols)],'BUY' if i%2==0 else 'SELL',
                     str(25+(i%12)*25),str(Decimal('100.00')+Decimal(i%97)*Decimal('1.25')),'USD',f'{business_date}T14:{i%60:02d}:00Z'])
    changes=[]
    if scenario=='breaks' and count>=60:
        if side=='INTERNAL':
            rows.append(rows[30].copy())
            changes.append({'ref':rows[30][0],'family':'DUPLICATE_INTERNAL'})
        else:
            for i in [2,8,17,33,56]:
                rows[i][5]=str(Decimal(rows[i][5])+Decimal('0.05'))
                changes.append({'ref':rows[i][0],'family':'FIELD_MISMATCH','field':'execution_price'})
            for i in [6,23]:
                rows[i][4]=str(int(rows[i][4])+25)
                changes.append({'ref':rows[i][0],'family':'FIELD_MISMATCH','field':'quantity'})
            for i,field,value in [(14,1,'ACC01'),(19,2,'MSFT'),(27,6,'EUR')]:
                if rows[i][field]==value:
                    value='ACC02' if field==1 else 'NVDA'
                rows[i][field]=value
                changes.append({'ref':rows[i][0],'family':'FIELD_MISMATCH','field':INTERNAL[field]})
            for i in [10,46]:
                changes.append({'ref':rows[i][0],'family':'MISSING_BROKER'})
            rows=[r for i,r in enumerate(rows) if i not in [10,46]]
            for i in [38,49]:
                rows.append(rows[i].copy())
                changes.append({'ref':rows[i][0],'family':'DUPLICATE_BROKER'})
            extra=rows[0].copy()
            extra[0]='B9001'
            rows.append(extra)
            changes.append({'ref':'B9001','family':'MISSING_INTERNAL'})
    if scenario=='empty' and side=='BROKER':
        rows=[]
    if scenario=='rejected' and side=='BROKER' and rows:
        rows[0][4]='NaN'
    out=io.StringIO(newline='')
    writer=csv.writer(out,lineterminator='\n')
    writer.writerow(INTERNAL if side=='INTERNAL' else BROKER)
    for row in rows:
        row=row.copy()
        if side=='BROKER':
            row[3]={'BUY':'B','SELL':'S'}[row[3]]
        writer.writerow(row)
    data=out.getvalue().encode()
    manifest={'schema_version':1,'side':side,'broker_id':'BRK01','business_date':business_date,'dataset_id':dataset,
      'revision':revision,'representation':'EXECUTION','account_ids':['ACC01','ACC02'],
      'expected_bytes':len(data),'expected_data_rows':len(rows),'sha256':hashlib.sha256(data).hexdigest(),'complete':True}
    if scenario=='corrupt' and side=='BROKER':
        data=data[:-40]
    return data,manifest,changes


def write_demo_sizes(root):
    """CSV-only demo pairs plus independent manifests for the advanced path."""
    for name,count in [('small',60),('medium',10000)]:
        target=root/name
        target.mkdir(parents=True,exist_ok=True)
        provenance={}
        for scenario in ['breaks','corrected']:
            provenance[scenario]={}
            for side in ['INTERNAL','BROKER']:
                data,manifest,changes=fixture(side,scenario,count,1 if scenario=='breaks' else 2)
                filename=('' if scenario=='breaks' else 'corrected-')+side.lower()
                (target/f'{filename}.csv').write_bytes(data)
                (target/f'{filename}.manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
                provenance[scenario][side]=changes
        answers={'baseline_trades_per_side':count,
            'first_run':{'internal_records':count+1,'broker_records':count+1,'groups':count+1,'matched':count-15,
                         'mismatch':10,'missing':3,'duplicate':3,'findings':16},
            'corrected_run':{'internal_records':count,'broker_records':count,'groups':count,'matched':count,'findings':0}}
        (target/'expected-results.json').write_text(json.dumps(answers,indent=2)+'\n')
        (target/'injected-changes.json').write_text(json.dumps(provenance,indent=2)+'\n')
        (target/'README.md').write_text(f'''# {name.title()} demo dataset

{count:,} baseline trades per side for BRK01 / 2026-11-06 / equities_accounts_01 (ACC01 and ACC02).

1. Choose `internal.csv` and `broker.csv` in Breakwater's **Upload files** form.
2. Confirm both files are complete snapshots, then press **Run workflow**.
3. Expect {count+1:,} comparison groups: {count-15:,} matched, 10 mismatches, 3 missing, and 3 duplicate groups, producing 16 cases.
4. Upload `corrected-internal.csv` and `corrected-broker.csv` and run again. Expect {count:,} matched groups and no new findings. Existing case history is preserved; the vanished broker-only reference needs review.

CSV-only uploads do not require the accompanying manifest files. The `.manifest.json` sidecars support the advanced upload dialog; update their revision if it has already been used in your workspace.

Exact counts are in `expected-results.json`; individual injected differences are listed in `injected-changes.json`.
''')


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--output',default='fixtures')
    p.add_argument('--rows',type=int,default=240)
    p.add_argument('--demo-sizes',action='store_true',help='Generate small (60) and medium (10,000) CSV pairs')
    args=p.parse_args()
    root=Path(args.output)
    root.mkdir(parents=True,exist_ok=True)
    if args.demo_sizes:
        write_demo_sizes(root)
        return
    provenance={}
    for scenario in ['breaks','corrected','empty','rejected','corrupt']:
        provenance[scenario]={}
        for side in ['INTERNAL','BROKER']:
            data,manifest,changes=fixture(side,scenario,args.rows,2 if scenario=='corrected' else 1)
            name=f'{scenario}-{side.lower()}'
            (root/f'{name}.csv').write_bytes(data)
            (root/f'{name}.manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
            provenance[scenario][side]=changes
    (root/'injected-changes.json').write_text(json.dumps(provenance,indent=2)+'\n')


if __name__=='__main__':
    main()

