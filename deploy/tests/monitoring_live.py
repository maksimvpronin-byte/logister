"""Admin-only monitoring and blocked-output recovery on the local test stack."""
import json, runpy, subprocess, sys, time, uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'deploy/tests'))
from cleanup_loadtests import cleanup

def main():
 module=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))
 C=module['Client']; a=C();a.login('local-admin','local')
 C().call('/api/monitoring',expected=401)
 for name in ('operator','service'):
  c=C();c.login(name);c.call('/api/monitoring',expected=403)
 # The same authorization must hold through the UI reverse proxy.
 C.call.__globals__['BASE']='http://127.0.0.1:3000'
 a.call('/api/monitoring')
 C().call('/api/monitoring',expected=401)
 c.call('/api/monitoring',expected=403)
 C.call.__globals__['BASE']='http://127.0.0.1:8080'
 first=a.call('/api/monitoring')
 for _ in range(10):assert a.call('/api/monitoring')['latest']['at']==first['latest']['at'],'request triggered collection'
 stream='optimized-fault-monitor-'+uuid.uuid4().hex[:8]; count=2000
 a.call('/api/streams',{'name':stream,'title':'Проверка мониторинга','description':'Optimized ingestion acceptance','routing':{'enabled':False,'priority':1000,'match':'all','rules':[]},'storage':{'max_age_hours':1,'max_size_mb':64,'max_documents':1000000,'retention_days':1}},expected=201)
 paused=False; snapshots={'before':first}
 try:
  subprocess.run(['docker','pause','logister-opensearch-1'],check=True,capture_output=True);paused=True
  events=[{'id':stream+'-'+str(i),'message':'x'*5120,'stream':stream}for i in range(count)]
  for start in range(0,count,100):assert a.call('/api/streams/'+stream+'/ingest',events[start:start+100],expected=202)['count']==100
  time.sleep(10)
  blocked=a.call('/api/monitoring');snapshots['blocked']=blocked
  comp=blocked['latest']['components'];assert comp['opensearch']['status']=='down',comp['opensearch']
  assert comp['redis']['metrics']['pending']>=count,comp['redis']
  assert comp['redis']['metrics']['oldest_seconds']>0,comp['redis']
  assert comp['opensearch']['metrics']=={},'unavailable resources must not be zero'
  subprocess.run(['docker','unpause','logister-opensearch-1'],check=True,capture_output=True);paused=False
  for _ in range(90):
   row=next(x for x in a.call('/api/ingest/stats')['streams']if x['name']==stream)
   if row['published']==count and row['pending']==0:break
   time.sleep(.5)
  assert row['accepted']==row['published']==count and row['pending']==0,row
  time.sleep(6); snapshots['recovered']=a.call('/api/monitoring')
  assert snapshots['recovered']['latest']['components']['opensearch']['status']=='ok'
  assert snapshots['recovered']['latest']['process']['timings']['delivery']['calls']>0
  out=ROOT/'graphify-out/monitoring';out.mkdir(parents=True,exist_ok=True)
  (out/'live.json').write_text(json.dumps({'accepted':count,'text_bytes':5120,'final':row,'snapshots':snapshots},ensure_ascii=False,indent=2))
  print('PASS: admin/API/proxy authorization, cached samples, blocked output, queue age, recovery; 2000 × 5 KiB')
 finally:
  if paused:subprocess.run(['docker','unpause','logister-opensearch-1'],check=True,capture_output=True)
  cleanup([stream],apply=True)
if __name__=='__main__':main()
