"""Profile Kafka -> Logister -> Redis -> OpenSearch with configurable log size.
Uses isolated test streams; optional shard settings affect only test indices.
"""
from ingest_diagnostics import disk_snapshot,disk_delta,bulk_profile
import argparse,datetime,json,runpy,subprocess,threading,time,urllib.request,urllib.parse,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
CONTAINERS=['logister-api-1','logister-redis-1','logister-mongo-1','logister-opensearch-1','logister-kafka-1']
def command(args):return subprocess.run(args,capture_output=True,text=True,check=True,timeout=30).stdout

def redis_info():
 raw=command(['docker','exec','logister-redis-1','redis-cli','INFO','all']);out={}
 for line in raw.splitlines():
  if ':'not in line or line.startswith('#'):continue
  k,v=line.split(':',1);out[k]=v
 out['_profile_pending_bytes']=command(['docker','exec','logister-redis-1','redis-cli','GET','logister:pending-bytes:logs-v2']).strip()
 return out

def os_json(path, body=None, method=None):
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 request=urllib.request.Request('http://127.0.0.1:9200'+path,data=json.dumps(body).encode()if body is not None else None,method=method,headers={'Content-Type':'application/json'})
 with opener.open(request,timeout=30)as response:return json.load(response)

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--count',type=int,default=30000);parser.add_argument('--bytes',type=int,default=500);parser.add_argument('--shards',type=int,choices=[1,2,4],default=None);parser.add_argument('--auto-index',action='store_true');parser.add_argument('--os-jfr',action='store_true');parser.add_argument('--codec',choices=['default','zstd','zstd_no_dict']);parser.add_argument('--compression-level',type=int,choices=range(1,7),default=1);parser.add_argument('--sample-interval',type=float,choices=[0.25,0.5,1],default=1);args=parser.parse_args()
 assert not args.codec or(args.shards and not args.auto_index)
 assert not args.auto_index or args.shards
 assert 1<=args.count<=100000 and 64<=args.bytes<=16384
 C=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))['Client'];a=C();a.login('local-admin','local')
 run=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6];stream='profile-'+run.lower();out=ROOT/('graphify-out/kafka-shards'if args.shards else 'graphify-out/kafka-profile')/run;out.mkdir(parents=True,exist_ok=True)
 original=a.call('/api/ingest/kafka');assert original['enabled'] and not original.get('error'),original
 created=a.call('/api/streams',{'name':stream,'title':str(args.bytes)+' байт: '+run,'description':'Performance profile, randomized log text','routing':{'enabled':False,'priority':1000,'match':'all','rules':[]},'storage':{'max_age_hours':1,'max_size_mb':1024,'max_documents':1000000,'retention_days':1}},expected=201)
 test_index=None
 if args.shards:
  test_index='logs-v2-s-'+created['stream']['id'].replace('-','')+'-000001'
  if not args.auto_index:
   mapping=os_json('/logs-v2/_mapping')['logs-v2']['mappings']
   settings={'number_of_shards':args.shards,'number_of_replicas':0,'refresh_interval':'30s','translog.durability':'async','translog.sync_interval':'5s'}
   if args.codec:
    settings['index.codec']=args.codec
    if args.codec!='default':settings['index.codec.compression_level']=args.compression_level
   prepared=os_json('/'+test_index,{'settings':settings,'mappings':mapping,'aliases':{'logs-v2-read':{}}},method='PUT')
   assert prepared['acknowledged'],prepared
   health=os_json('/_cluster/health/'+test_index+'?wait_for_status=green&timeout=30s');assert not health['timed_out']and health['status']=='green',health
 environment={'docker':json.loads(command(['docker','info','--format','{{json .}}'])),'images':json.loads(command(['docker','inspect',*CONTAINERS])),'cluster':os_json('/_cluster/stats?filter_path=indices.count,indices.shards,nodes.count,nodes.jvm.mem')}
 # Do not persist environment variables/credentials from docker inspect.
 environment['docker']={k:environment['docker'][k]for k in ['NCPU','MemTotal','Driver','Architecture']}
 environment['images']=[{'name':v['Name'],'image':v['Image'],'started':v['State']['StartedAt'],'performance_env':{line.split('=',1)[0]:line.split('=',1)[1]for line in v['Config']['Env']if line.split('=',1)[0]in ['INGEST_WORKERS','KAFKA_BATCH_RECORDS','KAFKA_BATCH_BYTES','PUBLISH_BATCH_BYTES','INGEST_QUEUE_MAX_BYTES','BULK_MAX_BYTES','BULK_WORKERS','INGEST_PROFILE_BULK','INGEST_PROFILE_STAGES','OPENSEARCH_SHARDS','OUTPUT_BUFFER_BYTES','OPENSEARCH_CODEC','OPENSEARCH_CODEC_LEVEL']}}for v in environment['images']]
 stop=threading.Event();samples=[];resources=[];searches=[];monitor_errors=[];phase=['preload'];t0=time.monotonic()
 def observe():
  while not stop.is_set():
   t=time.monotonic();phase_started=phase[0]
   try:
    raw=command(['docker','stats','--no-stream','--format','{{json .}}',*CONTAINERS]);r=redis_info();node=os_json('/_nodes/stats/indices,jvm,process,thread_pool,fs')
    resources.append({'started_seconds':t-t0,'seconds':time.monotonic()-t0,'phase_started':phase_started,'phase':phase[0],'docker':[json.loads(x)for x in raw.splitlines()if x],'redis':r,'opensearch':node,**({'disk':disk_snapshot()}if args.shards else {})})
    if args.os_jfr and phase_started=='drain'and phase[0]=='drain':
     opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
     with opener.open('http://127.0.0.1:9200/_nodes/hot_threads?threads=8&snapshots=5&interval=200ms&type=cpu',timeout=10)as response:stacks=response.read().decode()
     (out/('hot-threads-'+str(len(resources))+'.txt')).write_text(stacks)
   except Exception as e:monitor_errors.append(str(e))
   stop.wait(max(0,3-(time.monotonic()-t)))
 observer=threading.Thread(target=observe);observer.start()
 jfr_started=False
 def stream_stats():return next(v for v in a.call('/api/ingest/stats')['streams']if v['name']==stream)
 try:
  a.call('/api/ingest/kafka',{'paused':True},method='PUT')
  for _ in range(60):
   if not a.call('/api/ingest/kafka')['running']:break
   time.sleep(.25)
  assert not a.call('/api/ingest/kafka')['running']
  print('PRELOAD',stream,args.count,args.bytes,'bytes text',flush=True)
  raw=command(['docker','exec','logister-api-1','/app/logister-kafka-load','-brokers','kafka:9092','-stream',stream,'-count',str(args.count),'-bytes',str(args.bytes),'-run',run])
  producer=json.loads(raw.strip().splitlines()[-1]);assert producer['acknowledged']==args.count and not producer['failed'],producer
  buffered=a.call('/api/ingest/kafka');assert buffered['lag']>=args.count and stream_stats()['accepted']==0
  before={'redis':redis_info(),'opensearch':os_json('/_nodes/stats/indices,jvm,process,thread_pool,fs')}
  diagnostics_before=disk_snapshot()if args.shards else None
  index_before=os_json('/'+test_index+'/_stats')if args.shards and not args.auto_index else None
  trace_since=datetime.datetime.now(datetime.timezone.utc).isoformat()
  if args.os_jfr:
   command(['docker','exec','logister-opensearch-1','/usr/share/opensearch/jdk/bin/jcmd','1','JFR.start','name=logister-ingest','settings=profile','filename=/tmp/logister-ingest.jfr']);jfr_started=True
  phase[0]='drain';start=time.monotonic();a.call('/api/ingest/kafka',{'paused':False},method='PUT');accepted_at=None;delivered_at=None;last_search=-10
  while time.monotonic()-start<300:
   status=a.call('/api/ingest/kafka');s=stream_stats();elapsed=time.monotonic()-start
   sample={'seconds':elapsed,'accepted':s['accepted'],'published':s['published'],'pending':s['pending'],'kafka_lag':status['lag'],'error':status.get('error','')};samples.append(sample)
   assert not status.get('error'),status
   if s['accepted']==args.count and accepted_at is None:accepted_at=elapsed
   if s['published']==args.count and not s['pending']:delivered_at=elapsed;break
   if elapsed-last_search>=5:
    q=time.monotonic();result=a.call('/api/search?'+urllib.parse.urlencode({'q':'*','stream':stream,'relative':'1h','size':20}));searches.append({'seconds':elapsed,'latency_ms':(time.monotonic()-q)*1000,'total':result['total']})
    if result.get('session_id'):a.call('/api/search/session',{'session_id':result['session_id']},method='DELETE',expected=204)
    last_search=elapsed
   if len(samples)%5==0:print('DRAIN',json.dumps(sample),flush=True)
   time.sleep(args.sample_interval)
  assert delivered_at is not None,samples[-1]
  if jfr_started:
   command(['docker','exec','logister-opensearch-1','/usr/share/opensearch/jdk/bin/jcmd','1','JFR.stop','name=logister-ingest','filename=/tmp/logister-ingest.jfr']);jfr_started=False
   command(['docker','cp','logister-opensearch-1:/tmp/logister-ingest.jfr',str(out/'opensearch.jfr')])
  diagnostics_after=disk_snapshot()if args.shards else None
  index_after=os_json('/'+test_index+'/_stats')if args.shards else None
  phase[0]='visibility';after={'redis':redis_info(),'opensearch':os_json('/_nodes/stats/indices,jvm,process,thread_pool,fs')}
  for _ in range(65):
   result=a.call('/api/search?'+urllib.parse.urlencode({'q':'*','stream':stream,'relative':'1h','size':1}));total=result['total']
   if result.get('session_id'):a.call('/api/search/session',{'session_id':result['session_id']},method='DELETE',expected=204)
   if total==args.count:break
   time.sleep(1)
  visible_at=time.monotonic()-start;assert total==args.count,total
  indices=a.call('/api/streams/'+stream+'/indices')['indices']
  indexed_probe=os_json('/logs-v2-read/_search',{'size':3,'query':{'term':{'stream':stream}},'_source':['message']})['hits']['hits']
  indexed_message_bytes=[len(hit['_source']['message'].encode())for hit in indexed_probe]
  assert len(indexed_message_bytes)==min(3,args.count) and all(size==args.bytes for size in indexed_message_bytes),indexed_message_bytes
  settings={row['name']:os_json('/'+row['name']+'/_settings')for row in indices if not row.get('deleted_at')}
  report={'message_indexing':'full','indexed_message_bytes':indexed_message_bytes,'sample_interval':args.sample_interval,'run':run,'stream':stream,'count':args.count,'text_bytes':args.bytes,'producer':producer,'drain_start_global_seconds':start-t0,'mean_json_bytes':producer['bytes']/args.count,'accepted_seconds':accepted_at,'delivered_seconds':delivered_at,'search_visible_seconds':visible_at,'delivery_eps':args.count/delivered_at,'delivery_input_mib_s':producer['bytes']/delivered_at/1048576,'search_count':total,'samples':samples,'searches':searches,'monitor_errors':monitor_errors,'before':before,'after':after,'indices':indices,'index_settings':settings,'environment':environment}
  if args.shards:
   actual_settings=[definition['settings']['index']for row in settings.values()for definition in row.values()]
   actual_codecs={value.get('codec','default')for value in actual_settings};assert len(actual_codecs)==1,actual_codecs
   actual_codec=next(iter(actual_codecs))
   if args.codec:assert actual_codec==args.codec,(actual_codec,args.codec)
   for row in settings.values():
    for definition in row.values():assert int(definition['settings']['index']['number_of_shards'])==args.shards
   bulk_rows,latency=bulk_profile(trace_since,test_index)
   assert latency['requests']>0 and latency['document_total']==args.count and latency['failed']==0 and latency['bytes']['mean']>0,latency
   report.update(shards=args.shards,codec=actual_codec,compression_level=int(actual_settings[0].get("codec.compression_level",3))if actual_codec!="default"else None,auto_index=args.auto_index,bulk_latency=latency,disk_visible=disk_snapshot(),disk_before=diagnostics_before,disk_after=diagnostics_after,disk_delta=disk_delta(diagnostics_before,diagnostics_after),index_before=index_before,index_after=index_after)
   (out/'bulk.json').write_text(json.dumps(bulk_rows,indent=2))
  (out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2));print('PASS',json.dumps({k:report[k]for k in ['run','count','text_bytes','mean_json_bytes','accepted_seconds','delivered_seconds','search_visible_seconds','delivery_eps','delivery_input_mib_s']}),flush=True);print('REPORT',out/'report.json',flush=True)
 finally:
  if jfr_started:
   command(['docker','exec','logister-opensearch-1','/usr/share/opensearch/jdk/bin/jcmd','1','JFR.stop','name=logister-ingest','filename=/tmp/logister-ingest.jfr'])
   command(['docker','cp','logister-opensearch-1:/tmp/logister-ingest.jfr',str(out/'opensearch.jfr')])
  stop.set();observer.join();(out/'resources.json').write_text(json.dumps(resources,ensure_ascii=False,indent=2));(out/'samples.json').write_text(json.dumps(samples,ensure_ascii=False,indent=2))
  a.call('/api/ingest/kafka',{'paused':original['pause_requested']},method='PUT')
 # Successful runs retain artifacts, then remove only their synthetic data.
 from cleanup_loadtests import cleanup
 cleanup([stream],apply=True)
if __name__=='__main__':main()
