"""Preload Kafka while consumption is paused, then measure actual drain.

Creates an isolated stream. Does not skip scheduled events: producer blocks on
its bounded buffer and requires broker acknowledgement for every message.
Use --count to change backlog; an existing administrator session is used only
for local API control. Restores the prior pause state in finally.
"""
import argparse,datetime,json,runpy,subprocess,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--count',type=int,default=250000);args=p.parse_args();assert 1<=args.count<=1000000
 C=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))['Client'];a=C();a.login('local-admin','local')
 run=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6];stream='kafka-load-'+run.lower()
 out=ROOT/'graphify-out/kafka-test'/run;out.mkdir(parents=True,exist_ok=True)
 original=a.call('/api/ingest/kafka');assert original['enabled'];samples=[];producer=None
 a.call('/api/streams',{'name':stream,'title':'Kafka: '+run,'description':'Buffered Kafka benchmark','routing':{'enabled':False,'priority':1000,'match':'all','rules':[]},'storage':{'max_age_hours':1,'max_size_mb':256,'max_documents':1000000,'retention_days':1}},expected=201)
 def get_stream():return next(s for s in a.call('/api/ingest/stats')['streams']if s['name']==stream)
 try:
  a.call('/api/ingest/kafka',{'paused':True},method='PUT')
  for _ in range(60):
   status=a.call('/api/ingest/kafka')
   if not status['running']:break
   time.sleep(.25)
  assert not status['running'],'consumer did not pause'
  print('PAUSED; filling Kafka with',args.count,'messages',flush=True)
  command=['docker','compose','--env-file','deploy/ldap/.env','-f','docker-compose.yml','-f','deploy/docker-compose.auth.yml','exec','-T','api','/app/logister-kafka-load','-brokers','kafka:9092','-stream',stream,'-count',str(args.count),'-bytes','512','-run',run]
  raw=subprocess.run(command,cwd=ROOT,text=True,capture_output=True,timeout=600,check=True);producer=json.loads(raw.stdout.strip().splitlines()[-1]);assert producer['acknowledged']==args.count and producer['failed']==0
  buffered=a.call('/api/ingest/kafka');before=get_stream();assert before['accepted']==0;assert buffered['lag']>=args.count
  print('BUFFERED',json.dumps({'lag':buffered['lag'],'producer':producer}),flush=True)
  # A restart must preserve the deliberate pause, without resetting offsets.
  subprocess.run(['docker','compose','--env-file','deploy/ldap/.env','-f','docker-compose.yml','-f','deploy/docker-compose.auth.yml','restart','api'],cwd=ROOT,capture_output=True,text=True,check=True,timeout=40)
  for _ in range(40):
   try:
    state=a.call('/api/ingest/kafka')
    if state['pause_requested'] and not state['running']:break
   except Exception:pass
   time.sleep(.5)
  assert state['pause_requested'] and not state['running'];assert get_stream()['accepted']==0
  started=time.monotonic();a.call('/api/ingest/kafka',{'paused':False},method='PUT')
  accepted_at=None;delivered_at=None
  while time.monotonic()-started<600:
   status=a.call('/api/ingest/kafka');s=get_stream();elapsed=time.monotonic()-started
   samples.append({'seconds':round(elapsed,3),'kafka_lag':status['lag'],'accepted':s['accepted'],'published':s['published'],'pending':s['pending'],'error':status.get('error','')})
   if s['accepted']==args.count and accepted_at is None:accepted_at=elapsed
   if s['published']==args.count and s['pending']==0 and status['lag']==0:delivered_at=elapsed;break
   if len(samples)%5==0:print('DRAIN',json.dumps(samples[-1]),flush=True)
   time.sleep(1)
  assert delivered_at is not None, samples[-1]
  # Exact search count is checked after refresh, separately from delivery ACK.
  count=None
  for _ in range(15):
   result=a.call('/api/search?q=*&relative=1h&size=1&stream='+stream);count=result['total']
   if result.get('session_id'):a.call('/api/search/session',{'session_id':result['session_id']},method='DELETE',expected=204)
   if count==args.count:break
   time.sleep(1)
  assert count==args.count,(count,args.count)
  report={'run':run,'stream':stream,'producer':producer,'buffered_lag':buffered['lag'],'pause_survived_restart':True,'accepted_seconds':accepted_at,'delivered_seconds':delivered_at,'end_to_end_eps':round(args.count/delivered_at,1),'search_count':count,'samples':samples}
  (out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print('PASS',json.dumps({k:v for k,v in report.items()if k!='samples'},ensure_ascii=False),flush=True);print('REPORT',out/'report.json',flush=True)
 finally:
  (out/'samples.json').write_text(json.dumps(samples,ensure_ascii=False,indent=2))
  for attempt in range(20):
   try:
    a.call('/api/ingest/kafka',{'paused':original['pause_requested']},method='PUT');break
   except Exception:
    if attempt==19:raise
    time.sleep(.5)
if __name__=='__main__':main()
