"""Compare compression on dedicated indices; no node or production settings change."""
import argparse,datetime,json,time
from pathlib import Path
from kafka_shard_matrix import ROOT,profile

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--sequence',nargs='+',choices=['default','zstd','zstd_no_dict'],default=['default','zstd_no_dict','zstd','zstd','zstd_no_dict','default']);args=parser.parse_args()
 out=ROOT/'graphify-out/opensearch-codecs'/datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out.mkdir(parents=True,exist_ok=True)
 result={'sequence':args.sequence,'runs':[],'count':100000,'text_bytes':10240,'shards':2,'compression_level':1}
 for codec in result['sequence']:
  time.sleep(5);print('CODEC',codec,flush=True)
  path=Path(profile(2,100000,extra_args=['--codec',codec,'--compression-level','1']));d=json.loads(path.read_text());assert d['codec']==codec
  result['runs'].append(str(path));(out/'manifest.json').write_text(json.dumps(result,indent=2))
 print('CODEC REPORT',out/'manifest.json',flush=True)
if __name__=='__main__':main()
