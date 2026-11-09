"""Isolated Kafka -> OpenSearch experiment, with request-durable bulk ACKs.
Uses a dedicated topic/index and common Logister validation/normalization.
Production group, queue and index settings are not changed.
"""
import argparse,datetime,json,runpy,subprocess,time,threading,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def shell(args,timeout=180):return subprocess.run(args,cwd=ROOT,text=True,capture_output=True,check=True,timeout=timeout).stdout

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--count',type=int,default=100000);parser.add_argument('--bytes',type=int,default=10240);args=parser.parse_args();assert 1<=args.count<=100000 and 64<=args.bytes<=16384
 C=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))['Client'];admin=C();admin.login('local-admin','local')
 run=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ').lower()+'-'+uuid.uuid4().hex[:6];name='directbench-'+run;stream='directprofile-'+run
 out=ROOT/'graphify-out/kafka-direct'/run;out.mkdir(parents=True,exist_ok=True)
 admin.call('/api/streams',{'name':stream,'title':'Прямой Kafka: '+run,'description':'Experimental isolated transport; counters are in the test report','routing':{'enabled':False,'priority':1000,'match':'all','rules':[]},'storage':{'max_age_hours':1,'max_size_mb':1024,'max_documents':1000000,'retention_days':1}},expected=201)
 shell(['docker','exec','logister-kafka-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:9092','--create','--topic',name,'--partitions','6','--replication-factor','1','--config','retention.ms=86400000','--config','retention.bytes=357913941'])
 print('PRELOAD DIRECT',name,flush=True)
 producer=json.loads(shell(['docker','exec','logister-api-1','/app/logister-kafka-load','-brokers','kafka:9092','-topic',name,'-stream',stream,'-count',str(args.count),'-bytes',str(args.bytes),'-run',run]).strip().splitlines()[-1]);assert producer['acknowledged']==args.count and not producer['failed']
 before=shell(['docker','exec','logister-redis-1','redis-cli','INFO','stats'])
 print('DRAIN DIRECT',flush=True)
 raw=shell(['docker','exec','logister-api-1','/app/logister-kafka-direct-bench','-topic',name,'-index',name,'-count',str(args.count)],300)
 result=json.loads(raw.strip().splitlines()[-1]);assert result['processed']==result['search_count']==args.count
 after=shell(['docker','exec','logister-redis-1','redis-cli','INFO','stats'])
 result.update(run=run,stream=stream,producer=producer,redis_before=before,redis_after=after)
 (out/'report.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));print('PASS DIRECT',json.dumps({k:v for k,v in result.items()if k not in ['redis_before','redis_after']}),flush=True);print('REPORT',out/'report.json',flush=True)
 from cleanup_loadtests import cleanup
 cleanup([stream],apply=True)
if __name__=='__main__':main()
