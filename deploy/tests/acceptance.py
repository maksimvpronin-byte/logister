"""End-to-end acceptance and failure drills. Run in WSL after docker compose build.

Each run creates a unique Compose project, private volumes, ports, and an isolated
quota filesystem. Production containers and indexes are never stopped or filled.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class Stack:
    def __init__(self, keep=False, resume=None):
        self.root = Path(__file__).resolve().parents[2]
        self.keep = keep
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.results = []
        if resume:
            self.out = Path(resume).resolve()
            state = json.loads((self.out / 'state.json').read_text())
            self.project, self.env = state['project'], dict(os.environ, **state['env'])
            assert self.project.startswith('logister-check-')
            if (self.out / 'results.json').exists(): self.results = json.loads((self.out / 'results.json').read_text())
        else:
            self.project = 'logister-check-' + uuid.uuid4().hex[:10]
            self.out = self.root / 'graphify-out/acceptance' / self.project
            self.out.mkdir(parents=True)
            allocated = {}
            for key in ['POSTGRES_PORT', 'OPENSEARCH_PORT', 'API_PORT', 'UI_PORT', 'FLUENT_BIT_PORT', 'REDIS_PORT']:
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0)); allocated[key] = str(sock.getsockname()[1])
            allocated.update(LOG_INTERVAL_MS='1', LOG_ROTATE_EVERY='50', LOG_MAX_MESSAGES='300', SEED_SAMPLE_LOGS='false')
            self.env = dict(os.environ, **allocated)
            (self.out / 'state.json').write_text(json.dumps({'project': self.project, 'env': allocated}, indent=2))
            (self.out / 'override.json').write_text(json.dumps({'services': {
                'loggen': {'restart': 'no'},
                'postgres': {'tmpfs': ['/quota:size=64m,mode=1777']},
            }}))
        self.api = 'http://127.0.0.1:' + self.env['API_PORT']
        self.os = 'http://127.0.0.1:' + self.env['OPENSEARCH_PORT']
        self.ui = 'http://127.0.0.1:' + self.env['UI_PORT']
        self.index = 'logs-v2-read'

    def docker(self, *args, input=None, check=True):
        return subprocess.run(['docker', *args], input=input, capture_output=True, check=check)

    def compose(self, *args, check=True):
        return subprocess.run(['docker', 'compose', '-p', self.project, '-f', str(self.root / 'docker-compose.yml'),
                               '-f', str(self.out / 'override.json'), *args], env=self.env,
                              capture_output=True, text=True, check=check)

    def container(self, service):
        value = self.compose('ps', '-a', '-q', service).stdout.strip()
        assert value, service
        return value

    def request(self, path, body=None, key=None, method=None, base=None, timeout=35):
        data = body if isinstance(body, bytes) else None if body is None else json.dumps(body).encode()
        headers = {'Content-Type': 'application/json'}
        if key: headers['Idempotency-Key'] = key
        req = urllib.request.Request((base or self.api) + path, data=data, method=method, headers=headers)
        try: response = self.opener.open(req, timeout=timeout)
        except urllib.error.HTTPError as error: response = error
        with response:
            raw = response.read()
            try: parsed = json.loads(raw) if raw else None
            except json.JSONDecodeError: parsed = raw.decode()
            return response.status, parsed

    def wait(self, predicate, label, timeout=90):
        until = time.monotonic() + timeout
        last = None
        while time.monotonic() < until:
            try:
                if predicate(): return
            except (urllib.error.URLError, ConnectionError, KeyError, json.JSONDecodeError) as error: last = error
            time.sleep(.3)
        raise AssertionError(f'Timed out: {label}; last={last}')

    def count(self, source=None):
        query = {'match_all': {}} if source is None else {'term': {'source': source}}
        status, body = self.request('/' + self.index + '/_count', {'query': query}, base=self.os)
        assert status == 200, body
        return body['count']

    def expect_count(self, source, count):
        self.wait(lambda: self.count(source) >= count, f'{source}: {count} indexed')
        actual = self.count(source)
        assert actual == count, (source, actual, count)

    def redis(self, *args):
        result = self.docker('exec', '-i', self.container('redis'), 'redis-cli', *args)
        return result.stdout.decode().strip()

    def sql(self, query):
        result = self.docker('exec', '-i', self.container('postgres'), 'psql', '-U', 'logister', '-d', 'logister', '-At', '-v', 'ON_ERROR_STOP=1', input=query.encode())
        return result.stdout.decode().strip()

    def events(self, source, count, prefix=None, size=0):
        prefix = prefix or uuid.uuid4().hex
        return [{'id': f'{prefix}-{i}', 'message': f'{source} {i} ' + 'x' * size, 'source': source,
                 'fields': {'seq': i}} for i in range(count)]

    def accept(self, events, key=None):
        status, body = self.request('/api/ingest/json', events, key=key)
        assert status == 202, (status, body)
        return body

    def search(self, query=None, cursor=None, size=200):
        params = {'cursor': cursor} if cursor else {'q': query or '*', 'relative': '24h', 'size': size}
        status, result = self.request('/api/search?' + urllib.parse.urlencode(params))
        assert status == 200, (status, result)
        return result

    def close(self, result):
        if result.get('session_id'): self.request('/api/search/session', {'session_id': result['session_id']}, method='DELETE')

    def all_messages(self, query):
        result = self.search(query)
        messages = list(result['messages'])
        try:
            while result.get('next_cursor'):
                result = self.search(cursor=result['next_cursor']); messages.extend(result['messages'])
            return messages
        finally: self.close(result)

    def run(self, name, action):
        started = time.monotonic()
        extra = action() or {}
        record = {'test': name, 'seconds': round(time.monotonic() - started, 3), 'passed': True, **extra}
        self.results.append(record)
        (self.out / 'results.json').write_text(json.dumps(self.results, indent=2))
        print('PASS:', name, json.dumps(extra), flush=True)

    def start(self):
        result = self.compose('up', '-d', '--no-build')
        print(result.stdout + result.stderr, flush=True)
        self.wait(lambda: self.request('/api/health')[0] == 200, 'isolated stack healthy', timeout=150)
        self.wait(lambda: self.docker('inspect', self.container('loggen')).stdout and
                  json.loads(self.docker('inspect', self.container('loggen')).stdout)[0]['State']['Status'] == 'exited', 'bounded generator finished')
        print('Isolated project:', self.project, 'UI:', self.ui, 'artifacts:', self.out, flush=True)

    def logs(self):
        (self.out / 'compose.log').write_text(self.compose('logs', '--no-color', check=False).stdout)

    def cleanup(self):
        self.logs()
        if not self.keep:
            assert self.project.startswith('logister-check-')
            self.compose('down', '-v', '--remove-orphans')

    def generated_pipeline(self):
        self.wait(lambda: self.count() >= 300, 'all rotated generator records indexed')
        assert self.count() == 300, self.count()
        messages = self.all_messages('*')
        assert len(messages) == 300 and len({m['id'] for m in messages}) == 300
        assert {m['fields']['seq'] for m in messages} == set(range(1, 301))
        for message in messages:
            expected = 'web-access' if message['source'] == 'nginx' else 'system' if message['source'] == 'cron' else 'all'
            assert message['stream'] == expected, message
        status, _ = self.request('/', base=self.ui)
        assert status == 200
        assert self.request('/api/health', base=self.ui)[0] == 200
        return {'records': 300, 'rotations': 6, 'stream_rules_applied_by_API': True}

    def mixed_types(self):
        values = ['text', 7, True, {'nested': 3}, ['a', 7], None]
        events = self.events('mixed', len(values))
        for event, value in zip(events, values): event['fields'] = {'mixed': value}
        self.accept(events); self.expect_count('mixed', 6)
        found = sorted(self.all_messages('source:mixed'), key=lambda m: int(m['message'].split()[1]))
        assert [m['fields']['mixed'] for m in found] == values
        return {'types': len(values)}

    def legacy_migration(self):
        base = 'legacy-check-' + uuid.uuid4().hex[:12]
        target = base + '-v2'
        binary = self.out / 'migration-api'
        donor = self.project + '-api-donor'
        self.docker('create', '--name', donor, 'logister-api:local')
        try: self.docker('cp', donor + ':/app/logister-api', str(binary))
        finally: self.docker('rm', donor)
        binary.chmod(0o755)
        status, _ = self.request('/'+base, {'settings': {'number_of_replicas': 0}, 'mappings': {
            'properties': {'@timestamp': {'type':'date'}, 'fields': {'type':'object'}, 'source': {'type':'keyword'}}}},
            method='PUT', base=self.os)
        assert status == 200
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        fixture = ''.join(json.dumps({'index': {'_id':f'legacy-{i}'}})+'\n'+json.dumps({
            '@timestamp':timestamp,'source':'legacy','message':f'legacy {i}','fields':{'seq':i}})+'\n' for i in range(200)).encode()
        assert self.request('/'+base+'/_bulk?refresh=true',fixture,base=self.os)[1]['errors'] is False
        with socket.socket() as sock: sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        address=f'http://127.0.0.1:{port}'
        log=(self.out/'migration-api.log').open('wb')
        env=dict(os.environ,HTTP_ADDR=f'127.0.0.1:{port}',
                 DATABASE_URL=f'postgres://logister:logister@127.0.0.1:{self.env["POSTGRES_PORT"]}/logister?sslmode=disable',
                 REDIS_URL=f'redis://127.0.0.1:{self.env["REDIS_PORT"]}',
                 OPENSEARCH_URL=self.os,OPENSEARCH_INDEX=base,SEED_SAMPLE_LOGS='false')
        process=subprocess.Popen([str(binary)],env=env,stdout=log,stderr=log)
        try:
            self.wait(lambda:self.request('/api/health',base=address)[0]==200,'legacy migration ready')
            status,result=self.request('/api/search?q=source:legacy&size=200',base=address)
            assert status==200 and result['total']==200
            assert {m['id'] for m in result['messages']}=={f'legacy-{i}' for i in range(200)}
            self.request('/api/search/session',{'session_id':result['session_id']},method='DELETE',base=address)
            events=[{'id':'new-string','message':'string','fields':{'seq':'text'}},
                    {'id':'new-object','message':'object','fields':{'seq':{'nested':True}}}]
            assert self.request('/api/ingest/json',events,base=address)[0]==202
            self.wait(lambda:self.request('/'+target+'-read/_count',base=self.os)[1]['count']==202,'new schema accepts mixed fields')
            assert self.request('/'+base+'/_count',base=self.os)[1]['count']==200
            return {'migrated_records':200,'original_backup_records':200,'new_mixed_records':2}
        finally:
            process.terminate()
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:process.kill();process.wait()
            log.close()
            for owned in [target+'-s-*',target,base]:self.request('/'+owned,method='DELETE',base=self.os)

    def deep_snapshot(self):
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        events = self.events('paging', 10050, prefix='paging')
        for event in events: event['timestamp'] = timestamp
        for offset in range(0, len(events), 500): self.accept(events[offset:offset+500])
        self.expect_count('paging', 10050)
        first = self.search('source:paging')
        assert first['total'] == 10050
        second = self.search(cursor=first['next_cursor'])
        previous = self.search(cursor=second['previous_cursor'])
        assert [m['id'] for m in first['messages']] == [m['id'] for m in previous['messages']]
        late = self.events('paging', 1, prefix='late'); late[0]['timestamp'] = timestamp;late[0]['message']='arrived-after-snapshot'
        self.accept(late); self.expect_count('paging', 10051)
        result, ids = first, []
        try:
            while True:
                assert all(m['message']!='arrived-after-snapshot' for m in result['messages'])
                ids.extend(m['id'] for m in result['messages'])
                if not result.get('next_cursor'): break
                result = self.search(cursor=result['next_cursor'])
            assert len(ids) == 10050 and len(set(ids)) == 10050
            assert result['offset'] == 10000 and len(result['messages']) == 50
        finally: self.close(result)
        assert self.request('/api/search?cursor=tampered')[0] == 410
        return {'snapshot_records': len(ids), 'last_offset': 10000, 'late_records_excluded': 1}

    def lost_acknowledgement(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        events = self.events('lost-ack', 10, prefix='lost-ack')
        payload = json.dumps(events).encode(); target = urllib.parse.urlparse(self.api)
        owner = self
        class DropResponse(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                connection = http.client.HTTPConnection(target.hostname, target.port, timeout=20)
                connection.request('POST', '/api/ingest/json', body, {'Content-Type': 'application/json', 'Idempotency-Key': 'ack-lost'})
                response = connection.getresponse(); response.read(); assert response.status == 202
                connection.close(); self.close_connection = True
        proxy = ThreadingHTTPServer(('127.0.0.1', 0), DropResponse)
        thread = threading.Thread(target=proxy.serve_forever, daemon=True); thread.start()
        try:
            try: self.request('/api/ingest/json', payload, base=f'http://127.0.0.1:{proxy.server_port}')
            except (ConnectionError, http.client.RemoteDisconnected): pass
        finally: proxy.shutdown(); proxy.server_close(); thread.join()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            receipts = list(pool.map(lambda _: owner.accept(events, 'ack-lost'), range(8)))
        self.expect_count('lost-ack', 10)
        assert all(receipt['duplicates'] == 10 for receipt in receipts)
        changed = [dict(event, message='different') for event in events]
        assert self.request('/api/ingest/json', changed, key='ack-lost')[0] == 409
        return {'acknowledged_records': 10, 'concurrent_retries': 8, 'duplicates_in_index': 0}

    def load(self):
        batches = [self.events('load', 100, prefix=f'load-{i}') for i in range(50)]
        latencies = []
        def send(batch):
            start = time.monotonic(); self.accept(batch, batch[0]['id']); return time.monotonic() - start
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool: latencies = list(pool.map(send, batches))
        self.expect_count('load', 5000)
        return {'records': 5000, 'workers': 8, 'accept_p95_ms': round(sorted(latencies)[47]*1000),
                'end_to_end_seconds': round(time.monotonic()-started, 3)}

    def reconcile(self):
        self.wait(lambda:json.loads(self.docker('inspect',self.container('loggen')).stdout)[0]['State']['Status']=='exited',
                  'generator settled after recovery')
        def equal():
            status, stats = self.request('/api/ingest/stats')
            return status == 200 and stats['totals']['pending'] == 0 and stats['totals']['published'] == self.count()
        self.wait(equal,'all acknowledged records reconciled with search index')
        time.sleep(1)
        assert equal()
        published = self.request('/api/ingest/stats')[1]['totals']['published']
        return {'durable_records':published,'indexed_records':self.count(),'pending':0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep', action='store_true')
    parser.add_argument('--resume', type=Path)
    parser.add_argument('--phase', choices=['basic', 'streams', 'faults', 'all'], default='all')
    parser.add_argument('--only', help='Run one named failure drill in an existing owned test project')
    args = parser.parse_args()
    stack = Stack(args.keep, args.resume)
    try:
        if not args.resume: stack.start()
        if args.phase in ('basic', 'all'):
            for name in ['generated_pipeline', 'mixed_types', 'legacy_migration', 'deep_snapshot', 'lost_acknowledgement', 'load']:
                stack.run(name, getattr(stack, name))
        if args.phase in ('faults', 'all'):
            from acceptance_faults import run_faults
            run_faults(stack, args.only)
        if args.phase in ('streams', 'all'):
            from stream_acceptance import run_stream_checks
            run_stream_checks(stack)
        if args.phase=='all': stack.run('reconcile',stack.reconcile)
    finally:
        stack.cleanup()


if __name__ == '__main__': main()
