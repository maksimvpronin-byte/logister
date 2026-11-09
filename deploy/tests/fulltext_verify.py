"""Verify full text, typed fields and deduplication on the deployed API."""
import json,runpy,time,urllib.parse
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def main():
 C=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))['Client'];a=C();a.login('local-admin','local');p=json.loads((ROOT/'graphify-out/fulltext-probe.json').read_text());stream=p['stream']
 def search(query,expected):
  for _ in range(70):
   r=a.call('/api/search?'+urllib.parse.urlencode({'q':query,'stream':stream,'relative':'24h','size':10}))
   if r.get('session_id'):a.call('/api/search/session',{'session_id':r['session_id']},method='DELETE',expected=204)
   if r['total']==expected:return r
   time.sleep(.5)
  raise AssertionError((query,r['total'],expected))
 migrated=search(p['tail'],1);assert migrated['messages'][0]['id']==p['document_id'];assert migrated['messages'][0]['message']==p['event']['message']
 assert migrated['messages'][0]['payload']['message']==p['event']['message']
 repeat=a.call('/api/streams/'+stream+'/ingest',p['event'],expected=202);assert repeat['duplicates']==1
 search('service:billing AND request_id:"req-fulltext" AND trace_id:"trace-fulltext" AND status:[500 TO 599] AND ip:"192.0.2.0/24"',1)
 event={**p['event'],'id':stream+'-new','message':'newprefix '*1500+' newtailsearchmarker','request_id':'req-new'}
 a.call('/api/streams/'+stream+'/ingest',event,expected=202);new=search('newtailsearchmarker',1);assert new['messages'][0]['message']==event['message'];assert new['messages'][0]['status']==503
 a.call('/api/streams/'+stream+'/ingest',{**event,'message':event['message']+' changed'},expected=409)
 op=C();op.login('operator');r=op.call('/api/search?'+urllib.parse.urlencode({'q':p['tail'],'relative':'24h','size':1}));assert r['total']==0
 if r.get('session_id'):op.call('/api/search/session',{'session_id':r['session_id']},method='DELETE',expected=204)
 report={'stream':stream,'old_searchable_bytes':p['old_searchable_bytes'],'migrated_searchable_bytes':len(migrated['messages'][0]['message'].encode()),'old_tail_search':True,'new_tail_search':True,'full_payload_preserved':True,'typed_exact_range_cidr_search':True,'same_document_id':True,'old_retry_duplicates':repeat['duplicates'],'changed_tail_conflict_409':True,'operator_cannot_see_probe':True}
 (ROOT/'graphify-out/fulltext-verification.json').write_text(json.dumps(report,indent=2));print('PASS FULLTEXT',json.dumps(report),flush=True)
 from cleanup_loadtests import cleanup
 cleanup([stream],apply=True)
if __name__=='__main__':main()
