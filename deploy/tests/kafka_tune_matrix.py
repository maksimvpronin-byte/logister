"""Compare byte-bounded bulk variants, restoring the current API environment."""
import json,os,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
KEYS=['INGEST_WORKERS','KAFKA_BATCH_RECORDS','KAFKA_BATCH_BYTES','PUBLISH_BATCH_BYTES','INGEST_QUEUE_MAX_BYTES','BULK_MAX_BYTES','BULK_WORKERS']
compose=['docker','compose','--env-file','deploy/ldap/.env','-f','docker-compose.yml','-f','deploy/docker-compose.auth.yml','up','-d','--no-deps','api']
original=json.loads(subprocess.check_output(['docker','inspect','logister-api-1'],text=True))[0]
values={line.split('=',1)[0]:line.split('=',1)[1]for line in original['Config']['Env']if line.split('=',1)[0]in KEYS}
try:
 for workers,size in [(1,5<<20),(4,2<<20)]:
  env={**os.environ,**values,'BULK_WORKERS':str(workers),'BULK_MAX_BYTES':str(size)}
  print('VARIANT',workers,size,flush=True)
  subprocess.run(compose,cwd=ROOT,env=env,check=True)
  # Wait for the HTTP listener after recreation.
  import time,urllib.request
  for _ in range(40):
   try:
    urllib.request.urlopen('http://127.0.0.1:8080/api/auth/status',timeout=2).close();break
   except Exception:time.sleep(.25)
  subprocess.run([sys.executable,'deploy/tests/kafka_profile.py','--count','100000','--bytes','10240'],cwd=ROOT,check=True)
finally:
 subprocess.run(compose,cwd=ROOT,env={**os.environ,**values},check=True)
