"""Remove completed synthetic stream data; preserve reports and normal streams.

Dry run by default. --apply removes exact managed indices and Redis event receipts
only for explicitly named, recognized benchmark streams. Kafka retention is unchanged.
"""
import argparse,datetime,json,re,socket,subprocess,time,urllib.error,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
PATTERN=re.compile(r'^(?:profile|directprofile|loadtest|kafka-load|kafka-minute|optimized-fault)-[a-z0-9-]+$')
class Redis:
 def __init__(self):
  self.s=socket.create_connection(('127.0.0.1',6379),timeout=60);self.f=self.s.makefile('rb')
 def read(self):
  line=self.f.readline();kind=line[:1];value=line[1:-2]
  if kind==b'+':return value.decode()
  if kind==b'-':raise RuntimeError(value.decode())
  if kind==b':':return int(value)
  if kind==b'$':
   n=int(value)
   if n<0:return None
   v=self.f.read(n);assert self.f.read(2)==b'\r\n';return v.decode()
  if kind==b'*':return [self.read()for _ in range(int(value))]
  raise RuntimeError('Invalid Redis response')
 def pipeline(self,commands):
  data=bytearray()
  for command in commands:
   data.extend(('*'+str(len(command))+'\r\n').encode())
   for arg in command:
    b=str(arg).encode();data.extend(('$'+str(len(b))+'\r\n').encode()+b+b'\r\n')
  self.s.sendall(data);return [self.read()for _ in commands]
 def call(self,*command):return self.pipeline([command])[0]
def mongo(script):
 return subprocess.check_output(['docker','exec','logister-mongo-1','mongosh','--quiet','logister','--eval',script],text=True).strip()
def cleanup(streams=None,apply=False):
 rows=json.loads(mongo('const rows=db.streams.aggregate([{$lookup:{from:"stream_indices",let:{id:"$_id"},pipeline:[{$match:{$expr:{$and:[{$eq:["$stream_id","$$id"]},{$in:["$namespace",["logs-v2","logs-v2-kafka"]]}]}}},{$project:{_id:1,kafka_pending:1}}],as:"idx"}},{$project:{id:"$_id",name:1,description:1,deleted_at:1,indices:"$idx._id",kafka_pending:{$sum:"$idx.kafka_pending"}}}]).toArray(); print(EJSON.stringify(rows,{relaxed:true}));'))
 known=set();direct_indices={}
 for p in (ROOT/'graphify-out').rglob('*.json'):
  try:d=json.loads(p.read_text())
  except (ValueError,OSError):continue
  if isinstance(d,dict)and isinstance(d.get('stream'),str):
   known.add(d['stream'])
   if d['stream'].startswith('directprofile-')and d.get('index')=='directbench-'+d['stream'][14:]:direct_indices[d['stream']]=d['index']
 # Exact report provenance, plus recognized synthetic names for runs that failed before reporting.
 targets=[r for r in rows if PATTERN.fullmatch(r['name'])and (streams is None or r['name']in streams)and (r['name']in known or r['description']in {'Performance profile, randomized log text','Optimized ingestion acceptance','Buffered Kafka benchmark','Experimental isolated transport; counters are in the test report'})]
 if streams is not None:assert {r['name']for r in targets}==set(streams),'Unrecognized stream requested'
 for row in targets:
  extra=direct_indices.get(row['name'])
  if extra and extra not in row['indices']:row['indices'].append(extra)
 r=Redis()
 def wait_empty_queue():
  deadline=time.monotonic()+10
  while int(r.call('LLEN','logister:pending:logs-v2'))!=0:
   assert time.monotonic()<deadline,'Pending publication; cleanup refused'
   time.sleep(.05)
 wait_empty_queue()
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 def osrequest(path,method='GET'):
  try:
   with opener.open(urllib.request.Request('http://127.0.0.1:9200'+path,method=method),timeout=60)as response:return json.load(response)
  except urllib.error.HTTPError as e:
   if e.code==404:return {'missing':True}
   raise
 stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=ROOT/'graphify-out/cleanup'/stamp;out.mkdir(parents=True,exist_ok=True)
 plan={'apply':apply,'streams':targets,'before':osrequest('/_cat/allocation?format=json'),'redis_before':r.call('INFO','memory'),'deleted_indices':[],'deleted_receipts':0,'deleted_kafka_placements':0}
 (out/'report.json').write_text(json.dumps(plan,indent=2,ensure_ascii=False))
 if apply and targets:
  assert all(row['kafka_pending']==0 for row in targets),'Pending direct Kafka publication; cleanup refused'
  # Archive first, preventing further ingestion into these synthetic stream names.
  ids=json.dumps([row['id'] for row in targets])
  mongo('db.streams.updateMany({_id:{$in:'+ids+'}},[{$set:{deleted_at:{$ifNull:["$deleted_at",new Date()]}}}])')
  names={row['name']for row in targets}
  for row in targets:
   assert re.fullmatch(r'[a-z0-9-]+',row['name'])
   for index in row['indices']:
    assert re.fullmatch(r'logs-v2(?:-kafka)?-s-'+row['id'].replace('-','')+r'-[0-9]{6}',index)or index==direct_indices.get(row['name']),index
    osrequest('/'+index,'DELETE');plan['deleted_indices'].append(index)
   r.call('UNLINK',*['logister:'+prefix+':logs-v2:'+row['name']for prefix in ['active','accepted','pending-count']])
  kafka_indices=json.dumps([i for i in plan['deleted_indices'] if i.startswith('logs-v2-kafka-s-')])
  plan['deleted_kafka_placements']=int(mongo('print(db.kafka_placements.deleteMany({index:{$in:'+kafka_indices+'},published:true}).deletedCount)'))
  mongo('db.stream_indices.updateMany({stream_id:{$in:'+ids+'},namespace:{$in:["logs-v2","logs-v2-kafka"]}},[{$set:{deleted_at:{$ifNull:["$deleted_at",new Date()]},sealed_at:{$ifNull:["$sealed_at",new Date()]}}}])')
  cursor='0'
  while True:
   cursor,keys=r.call('SCAN',cursor,'MATCH','logister:evt:logs-v2:*','COUNT',1000)
   if keys:
    values=r.pipeline([['HGET',key,'stream']for key in keys]);remove=[key for key,value in zip(keys,values)if value in names]
    if remove:plan['deleted_receipts']+=r.call('UNLINK',*remove)
   if cursor=='0':break
  plan['after']=osrequest('/_cat/allocation?format=json');plan['redis_after']=r.call('INFO','memory');wait_empty_queue()
 plan['completed']=True
 (out/'report.json').write_text(json.dumps(plan,indent=2,ensure_ascii=False));print('CLEANUP',len(targets),'streams',len(plan['deleted_indices']),'indices',plan['deleted_receipts'],'receipts; report',out/'report.json',flush=True)
 return out/'report.json'
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');p.add_argument('--stream',action='append');args=p.parse_args();cleanup(args.stream,args.apply)
