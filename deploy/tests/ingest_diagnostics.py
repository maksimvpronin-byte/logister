"""Diagnostic snapshots for the local Linux VM; no payloads or credentials."""
import datetime,json,statistics,subprocess,time

def disk_snapshot():
 started=time.monotonic()
 raw=subprocess.run(['docker','exec','logister-opensearch-1','cat','/proc/diskstats','/proc/pressure/io','/sys/fs/cgroup/io.stat'],text=True,capture_output=True,check=True,timeout=10).stdout
 result={'monotonic':time.monotonic(),'capture_seconds':time.monotonic()-started,'disks':{},'psi':{},'opensearch_cgroup':{}}
 for line in raw.splitlines():
  values=line.split()
  if not values:continue
  if values[0]in ['some','full']:
   result['psi'][values[0]]={k:float(v)for k,v in (item.split('=')for item in values[1:])}
  elif ':'in values[0]:
   result['opensearch_cgroup'][values[0]]={k:int(v)for k,v in (item.split('=')for item in values[1:])}
  elif len(values)>=14 and values[2]in ['vda','vdb','vdc']:
   result['disks'][values[2]]=[int(x)for x in values[3:]]
 return result

def disk_delta(before,after):
 seconds=after['monotonic']-before['monotonic'];out={'seconds':seconds,'disks':{},'opensearch_cgroup':{}}
 for name,a in after['disks'].items():
  b=before['disks'][name];d=[x-y for x,y in zip(a,b)];ios=d[0]+d[4]
  out['disks'][name]={'read_mib_s':d[2]*512/1048576/seconds,'write_mib_s':d[6]*512/1048576/seconds,'rw_await_ms':(d[3]+d[7])/ios if ios else 0,'busy_percent':d[9]/seconds/10,'average_queue':d[10]/seconds/1000,'read_ios':d[0],'write_ios':d[4]}
 out['psi_stall_percent']={key:(after['psi'][key]['total']-before['psi'][key]['total'])/seconds/10000 for key in ['some','full']}
 for device,a in after['opensearch_cgroup'].items():
  b=before['opensearch_cgroup'].get(device,{})
  out['opensearch_cgroup'][device]={k:a[k]-b.get(k,0)for k in a}
 return out

def distribution(values):
 values=sorted(values)
 if not values:return {}
 def percentile(p):
  at=(len(values)-1)*p;lo=int(at);hi=min(lo+1,len(values)-1)
  return values[lo]+(values[hi]-values[lo])*(at-lo)
 return {'mean':statistics.mean(values),'p50':percentile(.5),'p95':percentile(.95),'p99':percentile(.99),'max':max(values)}

def bulk_profile(since,index):
 process=subprocess.run(['docker','logs','--since',since,'logister-api-1'],capture_output=True,text=True,check=True,timeout=15)
 rows=[]
 for line in (process.stdout+'\n'+process.stderr).splitlines():
  if 'ingest_bulk_profile 'not in line:continue
  item=json.loads(line.split('ingest_bulk_profile ',1)[1])
  if item['index']==index:rows.append(item)
 return rows,{'requests':len(rows),'failed':sum(not x['success']for x in rows),**{field:distribution([x[field]for x in rows])for field in ['build_ms','request_ms','total_ms','bytes','documents']},'document_total':sum(x['documents']for x in rows)}
