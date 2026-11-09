"""Compare pipeline complexity with fixed 5 KiB events and per-run cleanup.
Each case changes only performance settings; queue durability, indexing semantics,
compression and refresh are preserved. Restores original API environment in finally.
"""
import argparse,datetime,json,os,subprocess,time
from pathlib import Path
from kafka_shard_matrix import ROOT,KEYS,COMPOSE,ready,profile

CASES=[
 ('baseline',{}),
 ('serial-output',{'OUTPUT_BUFFER_BYTES':'1'}),
 ('single-normalizer',{'INGEST_WORKERS':'1'}),
 ('single-bulk',{'BULK_WORKERS':'1'}),
 ('one-shard',{'OPENSEARCH_SHARDS':'1'}),
 ('minimal',{'INGEST_WORKERS':'1','BULK_WORKERS':'1','OUTPUT_BUFFER_BYTES':'1','OPENSEARCH_SHARDS':'1'}),
]
def stage_summary(logs):
 result={}
 for line in logs.splitlines():
  if 'ingest_stage_profile 'not in line:continue
  row=json.loads(line.split('ingest_stage_profile ',1)[1])
  if row.get('documents',0)<32:continue
  group=result.setdefault(row['stage'],{'calls':0,'documents':0,'budget_retries':0,'timing_sums_ms':{}})
  group['calls']+=1;group['documents']+=row['documents'];group['budget_retries']+=row.get('budget_retries',0)
  for key,value in row.items():
   if key=='ms'or key.endswith('_ms'):group['timing_sums_ms'][key]=group['timing_sums_ms'].get(key,0)+value
 return result

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--count',type=int,default=100000);p.add_argument('--bytes',type=int,default=5120);p.add_argument('--suite',choices=['primary','lean'],default='primary');args=p.parse_args()
 cases=CASES if args.suite=='primary'else [('baseline',{}),('lean',{'INGEST_WORKERS':'1','OUTPUT_BUFFER_BYTES':'0'})]
 raw=json.loads(subprocess.check_output(['docker','inspect','logister-api-1'],text=True))[0]
 keys=KEYS|{'OPENSEARCH_CODEC','OPENSEARCH_CODEC_LEVEL','OUTPUT_BUFFER_BYTES','OPENSEARCH_SHARDS','INGEST_PROFILE_STAGES'}
 original={k:v for k,v in (s.split('=',1)for s in raw['Config']['Env'])if k in keys}
 stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=ROOT/'graphify-out/configuration-audit'/stamp;out.mkdir(parents=True)
 result={'suite':args.suite,'count':args.count,'text_bytes':args.bytes,'original':original,'sequence':[name for name,_ in cases]+[name for name,_ in reversed(cases)],'runs':[]}
 try:
  for order,(name,overrides)in enumerate(cases+list(reversed(cases)),1):
   env={**original,**overrides,'INGEST_PROFILE_BULK':'true','INGEST_PROFILE_STAGES':'true'}
   subprocess.run(COMPOSE,cwd=ROOT,env={**os.environ,**env},check=True,capture_output=True);ready();time.sleep(5)
   actual_raw=json.loads(subprocess.check_output(['docker','inspect','logister-api-1'],text=True))[0]
   actual={k:v for k,v in (s.split('=',1)for s in actual_raw['Config']['Env'])if k in keys}
   assert all(actual.get(k)==v for k,v in env.items()),(env,actual)
   since=datetime.datetime.now(datetime.timezone.utc).isoformat();print('AUDIT CASE',order,name,flush=True)
   report=Path(profile(int(env['OPENSEARCH_SHARDS']),args.count,auto_index=True,text_bytes=args.bytes,extra_args=['--sample-interval','0.25']))
   d=json.loads(report.read_text());logs=subprocess.run(['docker','logs','--since',since,'logister-api-1'],capture_output=True,text=True,check=True).stderr
   before=next(iter(d['before']['opensearch']['nodes'].values()));after=next(iter(d['after']['opensearch']['nodes'].values()))
   row={'case':name,'order':order,'overrides':overrides,'actual_env':actual,'image':actual_raw['Image'],'report':str(report),'seconds':d['delivered_seconds'],'accepted_seconds':d['accepted_seconds'],'eps':d['delivery_eps'],'search_count':d['search_count'],'peak_pending':max(s['pending']for s in d['samples']),'os_cpu_seconds':(after['process']['cpu']['total_in_millis']-before['process']['cpu']['total_in_millis'])/1000,'bulk_failed':d['bulk_latency']['failed'],'stages':stage_summary(logs)}
   assert d['search_count']==args.count and row['bulk_failed']==0 and d['indexed_message_bytes']==[args.bytes]*min(3,args.count)
   result['runs'].append(row);(out/'report.json').write_text(json.dumps(result,indent=2));print('AUDIT RESULT',json.dumps({k:v for k,v in row.items()if k not in ['stages','actual_env','image']}),flush=True)
 finally:
  subprocess.run(COMPOSE,cwd=ROOT,env={**os.environ,**original},check=True,capture_output=True);ready();result['restored']=True;(out/'report.json').write_text(json.dumps(result,indent=2))
 print('AUDIT REPORT',out/'report.json',flush=True)
if __name__=='__main__':main()
