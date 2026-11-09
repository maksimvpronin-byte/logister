"""Bounded local ingestion benchmark; creates and cleans up an isolated test stream.

Uses the Fluent Bit HTTP endpoint, batches of 100, max eight in-flight requests.
Runs search concurrently, saves raw samples and stops escalating on overload.
No automatic retries: failures and generator saturation are reported explicitly.
"""
import argparse
import concurrent.futures
import datetime
import http.cookiejar
import json
from pathlib import Path
import runpy
import statistics
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
import uuid

ROOT = Path(__file__).resolve().parents[2]

def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered)-1, int((len(ordered)-1)*p))] * 1000, 2)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration',type=int,default=30)
    parser.add_argument('--rates',type=int,nargs='+',default=[100,500,1000,2000])
    parser.add_argument('--out',default='graphify-out/load-test')
    args=parser.parse_args()
    assert 5<=args.duration<=300 and all(0<r<=5000 for r in args.rates)
    helper=runpy.run_path(str(ROOT/'deploy/tests/ldap_auth.py'))
    Client=helper['Client'];base=helper['BASE']
    admin=Client();admin.login('local-admin','local')
    run= datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6]
    stream='loadtest-'+run.lower()
    out=ROOT/args.out/run;out.mkdir(parents=True,exist_ok=True)
    admin.call('/api/streams',{'name':stream,'title':'Нагрузочный тест '+run,'description':'Isolated synthetic benchmark. Retained for inspection.','routing':{'enabled':False,'priority':1000,'match':'all','rules':[]},'storage':{'max_age_hours':1,'max_size_mb':256,'max_documents':1000000,'retention_days':1}},expected=201)
    monitor=Client();monitor.login('local-admin','local')
    search=Client();search.login('local-admin','local')
    stop=threading.Event();overload=threading.Event();lock=threading.Lock()
    samples=[];queries=[];query_errors=[];phase=['baseline'];start=time.monotonic()
    def metrics():
        while not stop.is_set():
            try:
                data=monitor.call('/api/ingest/stats')
                row=next(s for s in data['streams'] if s['name']==stream)
                with lock:samples.append({'t':round(time.monotonic()-start,3),'phase':phase[0],**row})
                if row['pending']>5000:overload.set()
            except Exception as e:
                with lock:samples.append({'t':round(time.monotonic()-start,3),'phase':phase[0],'error':str(e)})
                overload.set()
            stop.wait(2)
    def searching():
        while not stop.is_set():
            t=time.monotonic()
            try:
                data=search.call('/api/search?'+urllib.parse.urlencode({'q':'*','stream':stream,'relative':'1h','size':20}))
                with lock:queries.append({'t':round(t-start,3),'phase':phase[0],'seconds':time.monotonic()-t,'total':data['total']})
                if data.get('session_id'):search.call('/api/search/session',{'session_id':data['session_id']},method='DELETE',expected=204)
            except Exception as e:
                with lock:query_errors.append({'phase':phase[0],'error':str(e)})
            stop.wait(2)
    mt=threading.Thread(target=metrics);qt=threading.Thread(target=searching);mt.start();qt.start()
    pool=concurrent.futures.ThreadPoolExecutor(max_workers=8)
    gate=threading.BoundedSemaphore(8);results=[];all_requests=[];seq=0;payload_sizes=[]
    def send(events,stage):
        body=json.dumps(events,separators=(',',':')).encode();t=time.monotonic()
        result={'phase':stage,'count':len(events),'bytes':len(body)}
        try:
            req=urllib.request.Request(base+'/api/ingest/fluent-bit',data=body,headers={'Content-Type':'application/json'},method='POST')
            opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
            try:response=opener.open(req,timeout=15)
            except urllib.error.HTTPError as e:response=e
            with response:
                data=json.load(response);result.update(status=response.status,response=data)
                result['ok']=response.status==202 and data.get('count')==len(events) and data.get('duplicates')==0
        except Exception as e:result.update(ok=False,error=str(e))
        finally:
            result['seconds']=time.monotonic()-t
            with lock:all_requests.append(result)
            gate.release()
        return result
    def current():
        return next(s for s in admin.call('/api/ingest/stats')['streams'] if s['name']==stream)
    def drain(limit=45):
        deadline=time.monotonic()+limit
        while time.monotonic()<deadline:
            row=current()
            if row['pending']==0:return row
            time.sleep(.5)
        return current()
    try:
        time.sleep(2)
        for rate in args.rates:
            label=str(rate);phase[0]=label;before=current();futures=[];missed=0;scheduled=0
            t0=time.monotonic();ticks=(rate*args.duration+99)//100
            print('START',rate,'logs/s',flush=True)
            for tick in range(ticks):
                due=t0+tick*100/rate
                if stop.wait(max(0,due-time.monotonic())) or overload.is_set():break
                n=min(100,rate*args.duration-scheduled);scheduled+=n
                if not gate.acquire(blocking=False):missed+=n;continue
                events=[]
                for _ in range(n):
                    seq+=1
                    events.append({'id':run+'-'+str(seq),'message':'synthetic load test '+('0123456789abcdef'*20),'source':'loadtest','host':'benchmark','level':'info','stream':stream,'fields':{'sequence':seq,'run':run,'status':200}})
                futures.append(pool.submit(send,events,label))
            stop.wait(max(0,t0+args.duration-time.monotonic()))
            at_end=current();ingest_elapsed=time.monotonic()-t0
            requests=[f.result() for f in futures];completed_elapsed=time.monotonic()-t0
            phase[0]=label+'-drain';drain_start=time.monotonic();after=drain();drain_seconds=time.monotonic()-drain_start
            good=[r for r in requests if r['ok']];bad=[r for r in requests if not r['ok']]
            with lock:
                stage_samples=[s for s in samples if s['phase']==label and 'pending'in s]
                stage_queries=[q['seconds'] for q in queries if q['phase']==label]
            accepted=after['accepted']-before['accepted'];published=at_end['published']-before['published']
            result={'target_eps':rate,'duration_seconds':args.duration,'scheduled':scheduled,'generator_skipped':missed,'submitted':sum(r['count']for r in requests),'acknowledged_unique':sum(r['count']for r in good),'failed_requests':len(bad),'error_examples':bad[:3],'accepted':accepted,'delivered_during_stage':published,'accepted_eps':round((at_end['accepted']-before['accepted'])/ingest_elapsed,1),'delivered_eps':round(published/ingest_elapsed,1),'end_pending':at_end['pending'],'max_observed_pending':max([s['pending']for s in stage_samples]+[at_end['pending']]),'pending_after_drain':after['pending'],'drain_seconds':round(drain_seconds,2),'all_requests_seconds':round(completed_elapsed,2),'request_p50_ms':percentile([r['seconds']for r in good],.5),'request_p95_ms':percentile([r['seconds']for r in good],.95),'request_max_ms':percentile([r['seconds']for r in good],1),'search_p95_ms':percentile(stage_queries,.95),'mean_event_bytes':round(sum(r['bytes']for r in requests)/max(1,sum(r['count']for r in requests)),1)}
            result['passed']=not bad and not missed and accepted==scheduled==rate*args.duration and at_end['pending']<=rate*2 and after['pending']==0 and not overload.is_set()
            results.append(result);print(json.dumps(result,ensure_ascii=False),flush=True)
            if not result['passed']:print('STOP escalation: overload or target not sustained',flush=True);break
            time.sleep(2)
        phase[0]='final-drain';final=drain(60)
        # Search refresh can lag behind acknowledged indexing; verify exact count.
        final_search=None
        for _ in range(12):
            data=admin.call('/api/search?'+urllib.parse.urlencode({'q':'*','stream':stream,'relative':'1h','size':1}))
            final_search=data['total']
            if data.get('session_id'):admin.call('/api/search/session',{'session_id':data['session_id']},method='DELETE',expected=204)
            if final_search==final['accepted']:break
            time.sleep(1)
        report={'run':run,'stream':stream,'started_utc':run.split('-')[0],'batch_size':100,'concurrency_limit':8,'search_interval_seconds':2,'results':results,'final_stats':final,'final_search_count':final_search,'integrity_ok':final_search==final['accepted']==final['published'] and final['pending']==0,'search_errors':query_errors}
    finally:
        stop.set();pool.shutdown(wait=True);mt.join();qt.join()
        (out/'samples.json').write_text(json.dumps({'metrics':samples,'searches':queries,'requests':all_requests},ensure_ascii=False,indent=2))
    (out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print('REPORT',out/'report.json',flush=True)
    print('INTEGRITY',report['integrity_ok'],'count',final_search,flush=True)
    if report['integrity_ok']:
        from cleanup_loadtests import cleanup
        cleanup([stream],apply=True)

if __name__=='__main__':main()
