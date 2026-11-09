"""Restore full searchable messages from preserved payloads, without changing IDs.
Dry-run by default; --apply updates the existing documents in place, preserving
payload_json, timestamps, stream membership, aliases, codecs and index generations.
"""
import argparse,datetime,ipaddress,json,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OPENER=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def request(path,body=None,method=None,ndjson=False):
 data=body.encode()if ndjson else json.dumps(body).encode()if body is not None else None
 with OPENER.open(urllib.request.Request('http://127.0.0.1:9200'+path,data=data,method=method,headers={'Content-Type':'application/x-ndjson'if ndjson else 'application/json'}),timeout=120)as response:return json.load(response)
def projection(source):
 raw=source.get('payload_json')
 if not raw:return None
 root=json.loads(raw)
 if not isinstance(root,dict):return None
 metadata=root;message=root.get('message');fields=root.get('fields')
 if not isinstance(message,str)or not message.strip():
  log=root.get('log')
  if isinstance(log,str)and log.strip():
   try:nested=json.loads(log)
   except ValueError:nested=None
   if isinstance(nested,dict):
    metadata={**root,**nested};message=nested.get('message');fields=nested.get('fields')
   else:message=log
 message=message.strip()if isinstance(message,str)and message.strip()else raw
 doc={'message':message};fields=fields if isinstance(fields,dict)else {}
 def value(key):return metadata[key]if key in metadata else fields.get(key)
 for key in ['service','request_id','trace_id']:
  v=value(key)
  if isinstance(v,str)and v and len(v.encode())<=8000:doc[key]=v
 v=value('status')
 if isinstance(v,(str,int))and not isinstance(v,bool):
  try:
   n=int(v)
   if -(2**63)<=n<2**63:doc['status']=n
  except ValueError:pass
 v=value('ip')
 if isinstance(v,str):
  try:ipaddress.ip_address(v);doc['ip']=v
  except ValueError:pass
 return doc

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--apply',action='store_true');args=p.parse_args()
 out=ROOT/'graphify-out/fulltext-migration'/datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out.mkdir(parents=True)
 report={'apply':args.apply,'before_count':request('/logs-v2-read/_count')['count'],'scanned':0,'without_payload':0,'changed_candidates':0,'updated':0,'noops':0,'indices':{}}
 scroll=None
 try:
  page=request('/logs-v2-read/_search?scroll=2m',{'size':500,'sort':['_doc'],'query':{'match_all':{}},'_source':['message','payload_json','service','request_id','trace_id','status','ip']})
  while True:
   scroll=page['_scroll_id'];hits=page['hits']['hits']
   if not hits:break
   lines=[]
   for hit in hits:
    report['scanned']+=1;report['indices'][hit['_index']]=report['indices'].get(hit['_index'],0)+1
    doc=projection(hit['_source'])
    if doc is None:report['without_payload']+=1;continue
    if all(hit['_source'].get(k)==v for k,v in doc.items()):continue
    report['changed_candidates']+=1
    lines.extend([json.dumps({'update':{'_index':hit['_index'],'_id':hit['_id'],'retry_on_conflict':3}}),json.dumps({'doc':doc},ensure_ascii=False)])
   if args.apply and lines:
    result=request('/_bulk','\n'.join(lines)+'\n',method='POST',ndjson=True)
    assert not result['errors'],[(x['update']['status'],x['update'].get('error',{}).get('type'))for x in result['items']if x['update']['status']>=300]
    for item in result['items']:report['noops'if item['update']['result']=='noop'else 'updated']+=1
   page=request('/_search/scroll',{'scroll':'2m','scroll_id':scroll})
  if args.apply:request('/logs-v2-read/_refresh',method='POST')
  report['after_count']=request('/logs-v2-read/_count')['count'];assert report['after_count']>=report['before_count'],'documents disappeared'
 finally:
  if scroll:request('/_search/scroll',{'scroll_id':[scroll]},method='DELETE')
  (out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
 print('MIGRATION',json.dumps({k:v for k,v in report.items()if k!='indices'}),out/'report.json')
if __name__=='__main__':main()
