"""Compare earlier measurement modes with cleanup after each successful run."""
import datetime,json,os,subprocess,time
from pathlib import Path
from kafka_shard_matrix import ROOT,COMPOSE,ready,profile

def main():
 raw=json.loads(subprocess.check_output(['docker','inspect','logister-api-1'],text=True))[0]
 original={k:v for k,v in (s.split('=',1)for s in raw['Config']['Env'])if k in {'INGEST_PROFILE_STAGES','INGEST_PROFILE_BULK'}}
 out=ROOT/'graphify-out/clean-recheck'/datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out.mkdir(parents=True)
 result={'runs':[],'cleanup_after_each_run':True}
 try:
  subprocess.run(COMPOSE,cwd=ROOT,env={**os.environ,**original,'INGEST_PROFILE_STAGES':'true','INGEST_PROFILE_BULK':'true'},check=True);ready()
  for label,count,auto,interval in [('old-mode',100000,False,1),('automatic-index',100000,True,1),('frequent-poll',100000,True,.25),('short-recent-mode',50000,True,.25)]:
   time.sleep(5)
   p=Path(profile(2,count,auto_index=auto,extra_args=['--sample-interval',str(interval)]+([]if auto else ['--codec','zstd_no_dict','--compression-level','1'])))
   d=json.loads(p.read_text());result['runs'].append({'label':label,'report':str(p),'count':count,'auto_index':auto,'sample_interval':interval,'eps':d['delivery_eps'],'seconds':d['delivered_seconds'],'search_count':d['search_count']});(out/'report.json').write_text(json.dumps(result,indent=2));print('CASE',json.dumps(result['runs'][-1]),flush=True)
 finally:
  subprocess.run(COMPOSE,cwd=ROOT,env={**os.environ,**original},check=True);ready()
 print('CLEAN RECHECK',out/'report.json',flush=True)
if __name__=='__main__':main()
