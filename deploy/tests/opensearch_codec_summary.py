"""Summarize codec benchmarks; save post-visibility sizes for reproducibility."""
import argparse,collections,json,urllib.request
from pathlib import Path

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('manifest',type=Path);parser.add_argument('--offline',action='store_true');args=parser.parse_args()
 m=json.loads(args.manifest.read_text());snapshot=args.manifest.parent/'post-visibility-stats.json';saved=json.loads(snapshot.read_text())if snapshot.exists()else{};groups=collections.defaultdict(list)
 for path in m['runs']:
  d=json.loads(Path(path).read_text());index=d['indices'][0]['name']
  if not args.offline:
   opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
   with opener.open('http://127.0.0.1:9200/'+index+'/_stats/docs,store',timeout=15)as response:saved[index]=json.load(response)['_all']['primaries']
  stats=saved[index];assert stats['docs']['count']==d['count']
  a=next(iter(d['after']['opensearch']['nodes'].values()));b=next(iter(d['before']['opensearch']['nodes'].values()))
  groups[d['codec']].append({'run':d['run'],'count':d['count'],'seconds':d['delivered_seconds'],'eps':d['delivery_eps'],'cpu_seconds':(a['process']['cpu']['total_in_millis']-b['process']['cpu']['total_in_millis'])/1000,'index_ms':d['index_after']['_all']['primaries']['indexing']['index_time_in_millis'],'store_bytes':stats['store']['size_in_bytes'],'bulk_ms':d['bulk_latency']['request_ms']})
 result={k:{'runs':v,'pooled_eps':sum(x['count']for x in v)/sum(x['seconds']for x in v),'mean_cpu_seconds':sum(x['cpu_seconds']for x in v)/len(v),'mean_store_mib':sum(x['store_bytes']for x in v)/len(v)/1048576}for k,v in groups.items()}
 snapshot.write_text(json.dumps(saved,indent=2));(args.manifest.parent/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps({k:{a:b for a,b in v.items()if a!='runs'}for k,v in result.items()},indent=2))
if __name__=='__main__':main()
