"""Rotate core local streams to the configured shard policy, preserving history."""
import argparse,datetime,json,runpy,time,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def os_json(path):
 with urllib.request.urlopen('http://127.0.0.1:9200'+path,timeout=20)as response:return json.load(response)
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--codec',choices=['default','zstd','zstd_no_dict']);args=parser.parse_args()
 C=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))['Client'];a=C();a.login('local-admin','local');original=a.call('/api/ingest/kafka');records=[]
 existing={s['name']for s in a.call('/api/streams')['streams']};names=sorted(existing&{'all','billing','payments','system','web-access'})
 try:
  a.call('/api/ingest/kafka',{'paused':True},method='PUT')
  for _ in range(120):
   state=a.call('/api/ingest/kafka');stats=a.call('/api/ingest/stats')
   if not state['running']and stats['totals']['pending']==0:break
   time.sleep(.5)
  assert not state['running']and stats['totals']['pending']==0
  for name in names:
   before=a.call('/api/streams/'+name+'/indices')['indices'];old={row['name']:os_json('/'+row['name']+'/_count')['count']for row in before if not row.get('deleted_at')}
   a.call('/api/streams/'+name+'/rotate',{},method='POST')
   stamp=datetime.datetime.now(datetime.timezone.utc).isoformat();event={'id':'shard-policy-'+stamp+'-'+name,'stream':name,'message':'Index shard policy verification','source':'deployment-check'}
   a.call('/api/streams/'+name+'/ingest',event,expected=202)
   for _ in range(120):
    row=next(x for x in a.call('/api/ingest/stats')['streams']if x['name']==name)
    if row['pending']==0:break
    time.sleep(.25)
   assert row['pending']==0
   after=a.call('/api/streams/'+name+'/indices')['indices'];fresh=[x['name']for x in after if x['name']not in old and not x.get('deleted_at')];assert len(fresh)==1,(name,fresh)
   settings=os_json('/'+fresh[0]+'/_settings')[fresh[0]]['settings']['index'];assert int(settings['number_of_shards'])==2,settings
   if args.codec:assert settings.get('codec','default')==args.codec,settings
   for index,count in old.items():assert os_json('/'+index+'/_count')['count']==count,(name,index)
   records.append({'stream':name,'new_index':fresh[0],'shards':2,'codec':settings.get('codec','default'),'previous_indices_documents':old,'old_counts_unchanged':True,'probe_delivered':True})
 finally:a.call('/api/ingest/kafka',{'paused':original['pause_requested']},method='PUT')
 out=ROOT/'graphify-out/ingest-research';out.mkdir(parents=True,exist_ok=True);(out/('codec-deployment.json'if args.codec else 'deployment.json')).write_text(json.dumps(records,indent=2));print('PASS DEPLOYMENT',json.dumps(records),flush=True)
if __name__=='__main__':main()
