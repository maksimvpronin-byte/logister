"""Measure ingestion stages using dedicated indices, restoring runtime settings."""
import argparse,datetime,json,os,subprocess,sys,time
from pathlib import Path
from kafka_shard_matrix import ROOT,KEYS,COMPOSE,ready,profile
from ingest_diagnostics import distribution
def redis(*args):
 return subprocess.check_output(['docker','exec','logister-redis-1','redis-cli','--json',*args],text=True).strip()

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--label',required=True);parser.add_argument('--shards',nargs='+',type=int,choices=[1,2,4],default=[1,2]);parser.add_argument('--set',action='append',default=[]);parser.add_argument('--auto-index',action='store_true');parser.add_argument('--count',type=int,default=100000);parser.add_argument('--bytes',type=int,default=500);parser.add_argument('--sample-interval',type=float,choices=[0.25,0.5,1],default=1);args=parser.parse_args();assert 1<=args.count<=100000 and 64<=args.bytes<=16384
 keys=KEYS|{'INGEST_PROFILE_STAGES','OPENSEARCH_SHARDS','OUTPUT_BUFFER_BYTES','OPENSEARCH_CODEC','OPENSEARCH_CODEC_LEVEL'}
 raw=json.loads(subprocess.check_output(['docker','inspect','logister-api-1'],text=True))[0]
 original={v.split('=',1)[0]:v.split('=',1)[1]for v in raw['Config']['Env']if v.split('=',1)[0]in keys};original.setdefault('INGEST_PROFILE_STAGES','false');original.setdefault('INGEST_PROFILE_BULK','false')
 overrides={}
 for value in args.set:
  k,v=value.split('=',1);assert k in keys and not k.startswith('INGEST_PROFILE'),k;overrides[k]=v
 stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=ROOT/'graphify-out/ingest-research'/(stamp+'-'+args.label);out.mkdir(parents=True,exist_ok=True)
 result={'text_bytes':args.bytes,'label':args.label,'overrides':overrides,'runs':[]}
 monitor_original=json.loads(redis('CONFIG','GET','latency-monitor-threshold'))['latency-monitor-threshold']
 try:
  redis('CONFIG','SET','latency-monitor-threshold','5')
  subprocess.run(COMPOSE,cwd=ROOT,env={**os.environ,**original,**overrides,'INGEST_PROFILE_STAGES':'true','INGEST_PROFILE_BULK':'true'},check=True);ready()
  for shards in args.shards:
   time.sleep(5);since=datetime.datetime.now(datetime.timezone.utc).isoformat();p=Path(profile(shards,args.count,auto_index=args.auto_index,text_bytes=args.bytes,extra_args=['--sample-interval',str(args.sample_interval)]));d=json.loads(p.read_text());logs=subprocess.run(['docker','logs','--since',since,'logister-api-1'],capture_output=True,text=True,check=True).stderr
   stages=[]
   for line in logs.splitlines():
    if 'ingest_stage_profile 'in line:
     row=json.loads(line.split('ingest_stage_profile ',1)[1])
     if row.get('documents',0)>=32:stages.append(row)
   assert stages,'stage diagnostics missing'
   groups={}
   for row in stages:groups.setdefault(row['stage'],[]).append(row)
   summary={stage:{'calls':len(rows),'documents':sum(r['documents']for r in rows),'timings':{k:{'sum':sum(r[k]for r in rows),**distribution([r[k]for r in rows])}for k in rows[0]if k.endswith('_ms')or k=='ms'},'budget_retries':sum(r.get('budget_retries',0)for r in rows)}for stage,rows in groups.items()}
   run={'redis_latency_latest':json.loads(redis('LATENCY','LATEST')),'report':str(p),'shards':shards,'eps':d['delivery_eps'],'delivered_seconds':d['delivered_seconds'],'stages':summary};result['runs'].append(run)
   (out/(d['run']+'-stages.json')).write_text(json.dumps(stages,indent=2));(out/'report.json').write_text(json.dumps(result,indent=2))
   print('STAGE SUMMARY',json.dumps(run),flush=True)
 finally:
  redis('CONFIG','SET','latency-monitor-threshold',str(monitor_original))
  subprocess.run(COMPOSE,cwd=ROOT,env={**os.environ,**original},check=True);ready();(out/'report.json').write_text(json.dumps(result,indent=2))
 print('RESEARCH REPORT',out/'report.json',flush=True)
if __name__=='__main__':main()
