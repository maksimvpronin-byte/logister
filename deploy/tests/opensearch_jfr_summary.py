"""Summarize JFR CPU stack samples separately from blocking/native samples."""
import argparse,collections,json,re
from pathlib import Path

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('directory',type=Path);args=parser.parse_args();p=args.directory
 events=json.loads((p/'jfr-cpu.json').read_text())['recording']['events'];cpu=[e for e in events if e['type']=='jdk.ExecutionSample']
 thread=collections.Counter();leaf=collections.Counter();inclusive=collections.Counter();categories=collections.Counter()
 def frames(v):return [f['method']['type']['name']+'.'+f['method']['name']for f in (v.get('stackTrace')or{}).get('frames',[])]
 for event in cpu:
  v=event['values'];thread[v['sampledThread']['javaName']]+=1;names=frames(v)
  if names:leaf[names[0]]+=1
  inclusive.update(set(names))
  for cat,pat in [('LZ4','LZ4'),('Zstd','Zstd'),('StoredFields','StoredFields'),('JSON','jackson'),('DocumentParser','DocumentParser'),('Translog','translog'),('BKD','BKD'),('DocValues','DocValues'),('Flush','flush'),('IndexingChain','IndexingChain')]:
   if any(pat.lower()in name.lower()for name in names):categories[cat]+=1
 groups={}
 for event in json.loads((p/'jfr-waits.json').read_text())['recording']['events']:
  v=event['values'];duration=v.get('duration','PT0S');seconds=sum(float(n)*{'H':3600,'M':60,'S':1}[unit]for n,unit in re.findall(r'([\d.]+)([HMS])',duration));names=frames(v)
  if event['type']=='jdk.ThreadPark'and not any(any(p in n for p in ['lucene','opensearch.index','opensearch.action.bulk'])for n in names):continue
  row=groups.setdefault(event['type'],{'count':0,'seconds':0,'examples':[]});row['count']+=1;row['seconds']+=seconds
  if len(row['examples'])<4:row['examples'].append({'duration':duration,'path':v.get('path'),'thread':(v.get('eventThread')or{}).get('javaName'),'stack':names[:5]})
 result={'execution_samples':len(cpu),'native_samples':sum(e['type']=='jdk.NativeMethodSample'for e in events),'threads':thread.most_common(),'leaf':leaf.most_common(),'inclusive':inclusive.most_common(),'categories':dict(categories),'waits':groups}
 (p/'jfr-summary.json').write_text(json.dumps(result,indent=2));print(json.dumps({**result,'leaf':result['leaf'][:18],'inclusive':result['inclusive'][:15]},indent=2))
if __name__=='__main__':main()
