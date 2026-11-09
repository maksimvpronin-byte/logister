"""Check retries and recovery with OpenSearch paused and a clean Redis restart."""
import argparse,hashlib,json,runpy,secrets,subprocess,time,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def docker(*args):return subprocess.run(['docker',*args],check=True,capture_output=True,text=True,timeout=45).stdout.strip()
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--count',type=int,default=3);args=parser.parse_args();COUNT=args.count;assert 1<=COUNT<=4000
 C=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))['Client'];a=C();a.login('local-admin','local')
 original=a.call('/api/ingest/kafka');stream='optimized-fault-'+uuid.uuid4().hex[:8]
 a.call('/api/streams',{'name':stream,'title':'Проверка восстановления','description':'Optimized ingestion acceptance','routing':{'enabled':False,'priority':1000,'match':'all','rules':[]},'storage':{'max_age_hours':1,'max_size_mb':64,'max_documents':1000000,'retention_days':1}},expected=201)
 events=[{'id':stream+'-'+str(i),'message':secrets.token_hex(5120),'stream':stream}for i in range(COUNT)]
 probes=[events[i]for i in sorted({0,COUNT//3,2*COUNT//3,COUNT-1})]
 keys=['logister:evt:logs-v2:'+hashlib.sha256(('logs-v2\0'+event['id']).encode()).hexdigest()for event in probes]
 def stats():return next(x for x in a.call('/api/ingest/stats')['streams']if x['name']==stream)
 paused=False
 try:
  a.call('/api/ingest/kafka',{'paused':True},method='PUT')
  for _ in range(60):
   if not a.call('/api/ingest/kafka')['running']:break
   time.sleep(.25)
  assert not a.call('/api/ingest/kafka')['running']
  docker('pause','logister-opensearch-1');paused=True;started=time.monotonic()
  intake_started=time.monotonic();accepted=0
  for start in range(0,COUNT,50):accepted+=a.call('/api/streams/'+stream+'/ingest',events[start:start+50],expected=202)['count']
  intake_seconds=time.monotonic()-intake_started;assert accepted==COUNT
  replay=a.call('/api/streams/'+stream+'/ingest',events[:3],expected=202);assert replay['duplicates']==min(3,COUNT)
  time.sleep(2);before=stats();assert before['accepted']==COUNT and before['published']==0 and before['pending']==COUNT,before
  bodies_before=[int(docker('exec','logister-redis-1','redis-cli','HSTRLEN',key,'body'))for key in keys];assert all(bodies_before)
  print('PENDING RETAINED',before,flush=True)
  docker('restart','logister-redis-1')
  for _ in range(80):
   try:
    if docker('exec','logister-redis-1','redis-cli','PING')=='PONG':break
   except Exception:pass
   time.sleep(.25)
  bodies_after=[int(docker('exec','logister-redis-1','redis-cli','HSTRLEN',key,'body'))for key in keys];assert bodies_after==bodies_before,(bodies_before,bodies_after)
  # Keep the publication blocked long enough to cover a lease renewal interval.
  time.sleep(max(0,12-(time.monotonic()-started)))
  docker('unpause','logister-opensearch-1');paused=False
  for _ in range(120):
   try:
    final=stats()
    if final['published']==COUNT and final['pending']==0:break
   except Exception:pass
   time.sleep(.5)
  assert final['accepted']==final['published']==COUNT and final['pending']==0,final
  for _ in range(65):
   result=a.call('/api/search?q=*&relative=1h&size=1&stream='+stream);count=result['total']
   if result.get('session_id'):a.call('/api/search/session',{'session_id':result['session_id']},method='DELETE',expected=204)
   if count==COUNT:break
   time.sleep(1)
  assert count==COUNT,count
  opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
  query=json.dumps({'query':{'ids':{'values':[hashlib.sha256(('logs-v2\0'+event['id']).encode()).hexdigest()for event in probes]}},'size':len(probes)}).encode()
  request=urllib.request.Request('http://127.0.0.1:9200/logs-v2-read/_search',data=query,headers={'Content-Type':'application/json'})
  with opener.open(request,timeout=10)as response:documents=json.load(response)['hits']['hits']
  recovered={json.loads(hit['_source']['payload_json'])['id']:json.loads(hit['_source']['payload_json'])['message']for hit in documents}
  assert recovered=={event['id']:event['message']for event in probes},'full payload changed'
  a.call('/api/streams/'+stream+'/ingest',{**events[0],'message':'changed payload'},expected=409)
  operator=C();operator.login('operator');operator.call('/api/streams/'+stream+'/ingest',events[0],expected=403)
  report={'stream':stream,'accepted':accepted,'intake_seconds_with_opensearch_paused':intake_seconds,'intake_completed_while_opensearch_paused':True,'sampled_payloads':len(probes),'retry_duplicates':replay['duplicates'],'text_bytes':10240,'full_payload_verified':True,'pending_before_restart':before,'body_bytes_before':bodies_before,'body_bytes_after_restart':bodies_after,'final':final,'search_count':count,'id_conflict_rejected':True,'operator_ingest_denied':True,'redis_restart':'clean','opensearch_failure':'paused, not power loss'}
  out=ROOT/'graphify-out/optimized';out.mkdir(parents=True,exist_ok=True);(out/('faults.json'if COUNT==3 else 'faults-'+str(COUNT)+'.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2));print('PASS FAULTS',json.dumps(report),flush=True)
  from cleanup_loadtests import cleanup
  cleanup([stream],apply=True)
 finally:
  if paused:docker('unpause','logister-opensearch-1')
  for attempt in range(20):
   try:a.call('/api/ingest/kafka',{'paused':original['pause_requested']},method='PUT');break
   except Exception:
    if attempt==19:raise
    time.sleep(.5)
if __name__=='__main__':main()
