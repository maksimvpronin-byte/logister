"""Compare 1/2/4 primary shards using the production Redis ingestion path.
Temporarily enables payload-free bulk timing; restores original API env/pause.
Only dedicated test indices have their shard count changed.
"""
import datetime,json,os,runpy,subprocess,sys,time,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
KEYS={'INGEST_WORKERS','KAFKA_BATCH_RECORDS','KAFKA_BATCH_BYTES','PUBLISH_BATCH_BYTES','INGEST_QUEUE_MAX_BYTES','BULK_MAX_BYTES','BULK_WORKERS','INGEST_PROFILE_BULK'}
COMPOSE=['docker','compose','--env-file','deploy/ldap/.env','-f','docker-compose.yml','-f','deploy/docker-compose.auth.yml','up','-d','--no-deps','api']
def ready():
 for _ in range(120):
  try:
   urllib.request.urlopen('http://127.0.0.1:8080/api/auth/status',timeout=2).close();return
  except Exception:time.sleep(.25)
 raise RuntimeError('API did not become ready')
def profile(shards,count,auto_index=False,extra_args=None,text_bytes=10240):
 process=subprocess.Popen([sys.executable,'deploy/tests/kafka_profile.py','--count',str(count),'--bytes',str(text_bytes),'--shards',str(shards)]+(['--auto-index']if auto_index else [])+(extra_args or []),cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
 report=None
 for line in process.stdout:
  print(line,end='',flush=True)
  if line.startswith('REPORT '):report=line.strip()[7:]
 assert process.wait()==0 and report,'profile failed'
 return report

def main():
 raw=json.loads(subprocess.check_output(['docker','inspect','logister-api-1'],text=True))[0]
 values={s.split('=',1)[0]:s.split('=',1)[1]for s in raw['Config']['Env']if s.split('=',1)[0]in KEYS};values.setdefault('INGEST_PROFILE_BULK','false')
 env={**os.environ,**values}
 stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=ROOT/'graphify-out/kafka-shards'/('matrix-'+stamp);out.mkdir(parents=True,exist_ok=True)
 manifest={'sequence':[1,2,4,4,2,1],'count':100000,'text_bytes':10240,'original_performance_env':values,'runs':[]}
 try:
  subprocess.run(COMPOSE,cwd=ROOT,env={**env,'INGEST_PROFILE_BULK':'true'},check=True);ready()
  print('SMOKE 3000 messages, 1 shard',flush=True);manifest['smoke']=profile(1,3000)
  for shard in manifest['sequence']:
   # Equal fixed cooldown. Record remaining merges rather than force-flushing data.
   time.sleep(10)
   print('MATRIX SHARDS',shard,flush=True)
   report=profile(shard,manifest['count']);manifest['runs'].append(report)
   (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
 finally:
  subprocess.run(COMPOSE,cwd=ROOT,env=env,check=True);ready()
  (out/'manifest.json').write_text(json.dumps(manifest,indent=2))
 print('MATRIX REPORT',out/'manifest.json',flush=True)
if __name__=='__main__':main()
