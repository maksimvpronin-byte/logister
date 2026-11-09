"""Summarize an existing shard matrix without running or changing services."""
import argparse,json,statistics
from pathlib import Path
from ingest_diagnostics import distribution

def summarize(path):
 manifest=json.loads(path.read_text());groups={}
 for name in manifest['runs']:
  p=Path(name);d=json.loads(p.read_text());after=next(iter(d['after']['opensearch']['nodes'].values()));before=next(iter(d['before']['opensearch']['nodes'].values()))
  bulk=json.loads((p.parent/'bulk.json').read_text());large=[x['request_ms']for x in bulk if x['bytes']>=4<<20]
  row={'run':d['run'],'shards':d['shards'],'delivery_seconds':d['delivered_seconds'],'eps':d['delivery_eps'],'search_count':d['search_count'],'bulk_requests':len(bulk),'bulk_request_ms':d['bulk_latency']['request_ms'],'large_bulk_request_ms':distribution(large),'peak_pending':max(x['pending']for x in d['samples']),'disk':d['disk_delta']['disks']['vdb'],'psi':d['disk_delta']['psi_stall_percent'],'opensearch_cpu_cores':(after['process']['cpu']['total_in_millis']-before['process']['cpu']['total_in_millis'])/(after['process']['timestamp']-before['process']['timestamp']),'young_gc_ms':after['jvm']['gc']['collectors']['young']['collection_time_in_millis']-before['jvm']['gc']['collectors']['young']['collection_time_in_millis'],'old_gc_ms':after['jvm']['gc']['collectors']['old']['collection_time_in_millis']-before['jvm']['gc']['collectors']['old']['collection_time_in_millis'],'aof_rewrites':int(d['after']['redis']['aof_rewrites'])-int(d['before']['redis']['aof_rewrites']),'index_failed':d['index_after']['_all']['primaries']['indexing']['index_failed'],'merge_throttle_ms':d['index_after']['_all']['primaries']['merges']['total_throttled_time_in_millis']-d['index_before']['_all']['primaries']['merges']['total_throttled_time_in_millis']}
  groups.setdefault(d['shards'],[]).append(row)
 out={'manifest':str(path),'groups':{}}
 for shards,rows in sorted(groups.items()):
  total=sum(x['search_count']for x in rows);duration=sum(x['delivery_seconds']for x in rows)
  out['groups'][shards]={'runs':rows,'count':total,'delivery_seconds_sum':duration,'pooled_eps':total/duration,'mean_delivery_seconds':duration/len(rows),'eps_min':min(x['eps']for x in rows),'eps_max':max(x['eps']for x in rows)}
 (path.parent/'summary.json').write_text(json.dumps(out,indent=2))
 for shards,g in out['groups'].items():
  print(shards,'shards',round(g['pooled_eps']),'EPS, mean',round(g['mean_delivery_seconds'],3),'seconds, range',round(g['eps_min']),round(g['eps_max']))
 return out
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('manifest',type=Path);args=parser.parse_args();summarize(args.manifest)
