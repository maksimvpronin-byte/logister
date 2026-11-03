"""Failure drills for the unique test project created by acceptance.py."""
import datetime
import json
from pathlib import Path
import subprocess
import shutil
import os
import time
import uuid


def write_logs(stack, events):
    volume = stack.project + '_loggen_data'
    payload = ''.join(json.dumps(event) + '\n' for event in events).encode()
    stack.docker('run', '--rm', '-i', '--mount', f'type=volume,source={volume},target=/logs',
                 'busybox:1.36', 'sh', '-c', 'cat >> /logs/app.log', input=payload)


def ready(stack):
    stack.wait(lambda: stack.request('/api/health')[0] == 200, 'stack recovered', timeout=150)


def os_and_api_crash(stack):
    stack.compose('stop', 'opensearch')
    events = stack.events('os-offline', 400, prefix='os-offline')
    stack.accept(events, 'os-offline')
    assert stack.request('/api/health')[1]['pending_messages'] >= 400
    stack.compose('kill', '-s', 'SIGKILL', 'api')
    stack.compose('start', 'api')
    stack.wait(lambda: stack.request('/api/ingest/json', events, key='os-offline')[0] == 202,
               'durable intake after API kill while OpenSearch is down')
    receipt = stack.accept(events, 'os-offline')
    assert receipt['duplicates'] == 400
    stack.compose('start', 'opensearch'); ready(stack)
    stack.expect_count('os-offline', 400)
    return {'accepted_while_search_offline': 400, 'duplicates_after_restart': 0}


def postgres_crash(stack):
    stack.accept(stack.events('postgres-crash', 100, prefix='pg-before'), 'pg-before')
    stack.expect_count('postgres-crash', 100)
    stack.compose('kill', '-s', 'SIGKILL', 'postgres')
    events = stack.events('postgres-crash', 50, prefix='pg-after')
    status, body = stack.request('/api/ingest/json', events, key='pg-after')
    assert status == 503 and body['count'] == 0, (status, body)
    stack.compose('start', 'postgres'); ready(stack)
    stack.accept(events, 'pg-after'); stack.expect_count('postgres-crash', 150)
    return {'previously_acknowledged': 100, 'retried_after_recovery': 50}


def shipper_crash(stack):
    stack.compose('stop', 'api')
    since = datetime.datetime.now(datetime.timezone.utc).isoformat()
    write_logs(stack, stack.events('shipper-crash', 200, prefix='shipper-crash'))
    def queued():
        logs = stack.docker('logs', '--since', since, stack.container('fluent-bit'))
        return b'failed to flush chunk' in logs.stdout + logs.stderr
    stack.wait(queued, 'shipper queued messages during outage')
    stack.compose('kill', '-s', 'SIGKILL', 'fluent-bit')
    stack.compose('start', 'fluent-bit', 'api'); ready(stack)
    stack.expect_count('shipper-crash', 200)
    stack.compose('stop', 'fluent-bit')
    volume = stack.project + '_fluent_bit_state'
    stack.docker('run', '--rm', '--mount', f'type=volume,source={volume},target=/state', 'busybox:1.36',
                 'rm', '-f', '/state/tail.db', '/state/tail.db-wal', '/state/tail.db-shm')
    stack.compose('start', 'fluent-bit')
    # Resetting the checkpoint deliberately replays the retained source files.
    time.sleep(3)
    stack.expect_count('shipper-crash', 200)
    assert stack.count('paging') == 10051
    return {'queued_records_survived_SIGKILL': 200, 'logical_duplicates_after_checkpoint_reset': 0}


def rotation(stack):
    volume = stack.project + '_loggen_data'
    write_logs(stack, stack.events('rotation', 200, prefix='rename-a'))
    stack.docker('run', '--rm', '--mount', f'type=volume,source={volume},target=/logs', 'busybox:1.36', 'sh', '-c',
                 'mv /logs/app.log /logs/app.log.manual-rename; touch /logs/app.log')
    write_logs(stack, stack.events('rotation', 200, prefix='rename-b'))
    stack.expect_count('rotation', 400)
    stack.docker('run', '--rm', '--mount', f'type=volume,source={volume},target=/logs', 'busybox:1.36', 'sh', '-c',
                 'cp /logs/app.log /logs/app.log.copy-truncate; truncate -s 0 /logs/app.log')
    # copytruncate is tested with a quiesced writer; concurrent copytruncate can
    # delete bytes before a collector sees them and is not a lossless protocol.
    time.sleep(1.5)
    write_logs(stack, stack.events('rotation', 200, prefix='copy-c'))
    stack.expect_count('rotation', 600)
    return {'records': 600, 'rename_and_create': True, 'quiesced_copytruncate': True}


def shipper_burst(stack):
    started = time.monotonic()
    write_logs(stack, stack.events('shipper-burst', 5000, prefix='burst', size=200))
    stack.expect_count('shipper-burst', 5000)
    return {'records': 5000, 'seconds': round(time.monotonic()-started, 3)}


def postgres_disk_full(stack):
    # Intake no longer writes log bodies to PostgreSQL. An unavailable Redis
    # journal must reject the batch outright, then accept it after recovery.
    stack.compose('stop', 'redis')
    events = stack.events('disk-full', 20, prefix='journal-down')
    status, body = stack.request('/api/ingest/json', events, key='journal-down')
    assert status == 503 and body['count'] == 0, (status, body)
    stack.compose('start', 'redis')
    ready(stack)
    stack.accept(events, 'journal-down')
    stack.expect_count('disk-full', 20)
    return {'journal_rejects_while_redis_down': True, 'recovered_records': 20}


def source_disk_full(stack):
    name = stack.project + '-source-quota'
    stack.docker('run', '-d', '--name', name, '--tmpfs', '/logs:size=2m', '--entrypoint', 'sh',
                 'logister-loggen:local', '-c', 'sleep 300')
    try:
        result = stack.docker('exec', name, 'dd', 'if=/dev/zero', 'of=/logs/test-fill', 'bs=1M', 'count=3', check=False)
        assert result.returncode != 0 and b'No space left' in result.stderr
        stack.docker('exec', '-d', '-e', 'MAX_MESSAGES=300', '-e', 'INTERVAL_MS=1', '-e', 'ROTATE_EVERY=50',
                     name, 'sh', '/usr/local/bin/logister-generate')
        time.sleep(2)
        stack.docker('exec', name, 'rm', '/logs/test-fill')
        def written():
            result = stack.docker('exec', name, 'sh', '-c', 'cat /logs/app.log*', check=False)
            try: records = [json.loads(line) for line in result.stdout.splitlines() if line]
            except json.JSONDecodeError: return False
            return len(records) == 300
        stack.wait(written, 'generator resumed after source filesystem was freed')
        records = [json.loads(line) for line in stack.docker('exec', name, 'sh', '-c', 'cat /logs/app.log*').stdout.splitlines() if line]
        assert len({row['id'] for row in records}) == 300
        assert {row['fields']['seq'] for row in records} == set(range(1, 301))
        return {'valid_source_records_after_ENOSPC': 300, 'unique_ids': 300}
    finally: stack.docker('rm', '-f', name, check=False)


def shipper_disk_full(stack):
    source = 'shipper-disk-' + uuid.uuid4().hex[:8]
    name = stack.project + '-shipper-quota'
    helper = stack.out / 'disk-helper'
    compiler = shutil.which('go') or str(Path.home() / 'sdk/go/bin/go')
    subprocess.run([compiler,'build','-o',str(helper),str(stack.root/'deploy/tests/disk-helper/main.go')],
                   env=dict(os.environ,CGO_ENABLED='0'),check=True)
    stack.compose('stop', 'fluent-bit')
    stack.docker('run', '-d', '--name', name, '--network', stack.project + '_default',
                 '--tmpfs', '/var/lib/fluent-bit:size=8m', '--mount',
                 f'type=volume,source={stack.project}_loggen_data,target=/logs,readonly',
                 '-e', 'INGEST_HOST=api', '-e', 'INGEST_PORT=8080', 'logister-fluent-bit:local')
    try:
        stack.docker('cp', str(helper), name + ':/disk-helper')
        time.sleep(3)
        stack.compose('stop', 'api')
        result = stack.docker('exec', name, '/disk-helper', 'fill')
        assert b'ENOSPC' in result.stdout
        write_logs(stack, stack.events(source, 100, prefix=source))
        time.sleep(3)
        stack.docker('exec', name, '/disk-helper', 'free')
        stack.compose('start', 'api'); ready(stack)
        stack.expect_count(source, 100)
        return {'source':source,'records_after_shipper_ENOSPC': 100, 'automatic_recovery_after_freeing_space': True}
    finally:
        logs = stack.docker('logs', name, check=False)
        (stack.out / 'shipper-quota.log').write_bytes(logs.stdout + logs.stderr)
        stack.docker('rm', '-f', name, check=False)
        stack.compose('start', 'api', 'fluent-bit', check=False)


def ui_proxy_recovery(stack):
    # compose start can restart the bounded generator through dependencies.
    # Wait for it and the asynchronous projection before comparing snapshots.
    stack.wait(lambda: json.loads(stack.docker('inspect', stack.container('loggen')).stdout)[0]['State']['Status'] == 'exited',
               'bounded generator settled before proxy check')
    stack.wait(lambda: stack.request('/api/health')[1].get('pending_messages') == 0,
               'publisher settled before proxy check')
    assert stack.request('/'+stack.index+'/_refresh', {}, base=stack.os)[0] == 200
    before = stack.count()
    baseline = stack.search('*', size=200)
    baseline_ids = {message['id'] for message in baseline['messages']}
    stack.close(baseline)
    stack.wait(lambda: stack.request('/api/health', base=stack.ui)[0] == 200,
               'UI proxy healthy before address change')
    network = stack.project + '_default'
    info = json.loads(stack.docker('inspect', stack.container('api')).stdout)[0]
    old_ip = info['NetworkSettings']['Networks'][network]['IPAddress']
    holder = stack.project + '-old-api-address'
    stack.compose('rm', '-s', '-f', 'api')
    try:
        stack.docker('run', '-d', '--name', holder, '--network', network, '--ip', old_ip,
                     'busybox:1.36', 'sleep', '120')
        stack.compose('up', '-d', '--no-deps', 'api')
        ready(stack)
        info = json.loads(stack.docker('inspect', stack.container('api')).stdout)[0]
        assert info['NetworkSettings']['Networks'][network]['IPAddress'] != old_ip
        stack.wait(lambda: stack.request('/api/health', base=stack.ui)[0] == 200,
                   'UI proxy follows changed API address')
        result = stack.request('/api/search?size=1', base=stack.ui)
        # A tail flush can still arrive after the generator exits. New records
        # are allowed; verify the pre-restart records rather than requiring an
        # unchanged total while the sender remains active.
        assert result[0] == 200 and result[1]['total'] >= before, result
        stack.close(result[1])
        retained = stack.all_messages('event_id:(' + ' OR '.join(sorted(baseline_ids)) + ')')
        assert {message['id'] for message in retained} == baseline_ids
        return {'UI_recovers_after_API_address_change': True, 'preserved_records': before}
    finally:
        stack.docker('rm', '-f', holder, check=False)
        stack.compose('up', '-d', '--no-deps', 'api', check=False)


def run_faults(stack, only=None):
    actions=[os_and_api_crash, postgres_crash, shipper_crash, rotation, shipper_burst,
             postgres_disk_full, source_disk_full, shipper_disk_full, ui_proxy_recovery]
    if only: actions=[action for action in actions if action.__name__==only];assert actions, 'Unknown drill'
    for action in actions:
        stack.run(action.__name__, lambda action=action: action(stack))
