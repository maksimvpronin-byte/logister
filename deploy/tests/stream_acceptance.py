"""Real routing, rollover, retention and retry checks on an owned test stack."""
import json
import hashlib
import os
import time
import urllib.parse
import uuid


def create(stack, name, *, documents=1000000, size=1024, retention=0, routing=None):
    draft = {'name': name, 'title': name.title(), 'description': 'Isolated stream acceptance fixture',
             'routing': routing or {'enabled': False, 'priority': 100, 'match': 'all', 'rules': []},
             'storage': {'max_age_hours': 24, 'max_size_mb': size, 'max_documents': documents, 'retention_days': retention}}
    status, body = stack.request('/api/streams', draft)
    assert status == 201, (status, body)
    return body['stream']


def send(stack, name, count, prefix=None, **kwargs):
    events = stack.events('stream-' + name, count, prefix=prefix)
    status, body = stack.request('/api/streams/' + name + '/ingest', events, **kwargs)
    assert status == 202, (status, body)
    return events


def indices(stack, name):
    return stack.sql("SELECT index_name FROM stream_indices i JOIN streams s ON s.id=i.stream_id WHERE s.name='" + name + "' AND i.namespace='logs-v2' ORDER BY generation").splitlines()


def stream_routing(stack):
    billing = create(stack, 'route-billing', routing={'enabled': True, 'priority': 10, 'match': 'all', 'rules': [
        {'field': 'source', 'operator': 'starts_with', 'value': 'routed-'},
        {'field': 'fields.status', 'operator': 'equals', 'value': '500'}]})
    create(stack, 'route-errors', routing={'enabled': True, 'priority': 20, 'match': 'any', 'rules': [
        {'field': 'level', 'operator': 'equals', 'value': 'error'}]})
    events = stack.events('routed-fixture', 3, prefix='routing')
    for event in events: event.update(level='error', fields={'status': 500})
    events[1]['stream'] = 'route-errors'
    events[2]['fields']['status'] = 200
    stack.accept(events)
    stack.expect_count('routed-fixture', 3)
    found = {m['message'].split()[1]: m['stream'] for m in stack.all_messages('source:routed-fixture')}
    assert found == {'0': 'route-billing', '1': 'route-errors', '2': 'route-errors'}, found
    billing['routing']['enabled'] = False
    assert stack.request('/api/streams/route-billing', billing, method='PUT')[0] == 200
    assert stack.accept(events)['duplicates'] == 3
    replayed = {m['message'].split()[1]: m['stream'] for m in stack.all_messages('source:routed-fixture')}
    assert replayed == found, 'rule changes moved or duplicated previously accepted events'
    bad = stack.events('rejected-stream', 2); bad[1]['stream'] = 'missing-stream'
    assert stack.request('/api/ingest/json', bad)[0] == 400
    assert stack.count('rejected-stream') == 0
    return {'priority_and_explicit_destination': True, 'unknown_stream_batch_atomic': True, 'rule_changes_preserve_retry_destination': True}


def stream_rollover_and_retry(stack):
    create(stack, 'small-indices', documents=2)
    events = send(stack, 'small-indices', 5, prefix='small')
    stack.expect_count('stream-small-indices', 5)
    targets = indices(stack, 'small-indices'); assert len(targets) == 3, targets
    counts = [stack.request('/'+target+'/_count', base=stack.os)[1]['count'] for target in targets]
    assert counts == [2, 2, 1], counts
    snapshot = stack.search('stream:small-indices', size=2)
    assert stack.request('/api/streams/small-indices/rotate', {}, method='POST')[1]['rotated'] is True
    status, receipt = stack.request('/api/streams/small-indices/ingest', events)
    assert status == 202 and receipt['duplicates'] == 5
    assert indices(stack, 'small-indices') == targets
    send(stack, 'small-indices', 1, prefix='small-new')
    stack.expect_count('stream-small-indices', 6)
    rest = stack.search(cursor=snapshot['next_cursor'])
    assert rest['total'] == 5
    stack.close(rest)
    assert len(indices(stack, 'small-indices')) == 4
    return {'generation_counts': counts, 'duplicates_after_rollover': 0, 'snapshot_survives_rollover': True}


def stream_age_rotation(stack):
    stream = create(stack, 'age-rotation')
    stream['storage']['max_age_hours'] = 1
    assert stack.request('/api/streams/age-rotation', stream, method='PUT')[0] == 200
    send(stack, 'age-rotation', 1, prefix='age-first'); stack.expect_count('stream-age-rotation', 1)
    old = indices(stack, 'age-rotation')[0]
    stack.sql("UPDATE stream_indices SET created_at=now()-interval '2 hours' WHERE index_name='"+old+"'")
    stack.wait(lambda: stack.sql("SELECT sealed_at IS NOT NULL FROM stream_indices WHERE index_name='"+old+"'") == 't', 'quiet stream age rotation', timeout=35)
    assert len(indices(stack, 'age-rotation')) == 1
    send(stack, 'age-rotation', 1, prefix='age-next'); stack.expect_count('stream-age-rotation', 2)
    assert len(indices(stack, 'age-rotation')) == 2
    return {'age_threshold_hours': 1, 'quiet_stream_rotated': True, 'empty_successor_avoided': True}


def stream_size_rotation(stack):
    create(stack, 'size-rotation', size=1, documents=0)
    events = stack.events('stream-size-rotation', 64, prefix='size')
    for event in events:
        # Random, bounded-length tokens prevent compression from making this a
        # false size test and avoid oversized Lucene terms.
        event['message'] = ' '.join(os.urandom(50).hex() for _ in range(600))
    for offset in range(0, len(events), 8):
        assert stack.request('/api/streams/size-rotation/ingest', events[offset:offset+8])[0] == 202
    stack.expect_count('stream-size-rotation', 64)
    first = indices(stack, 'size-rotation')[0]
    stats = stack.request('/'+first+'/_stats/store', base=stack.os)[1]['_all']['primaries']['store']['size_in_bytes']
    assert stats >= 1048576, stats
    stack.wait(lambda: stack.sql("SELECT sealed_at IS NOT NULL FROM stream_indices WHERE index_name='"+first+"'") == 't', 'real primary-store size rollover', timeout=35)
    send(stack, 'size-rotation', 1, prefix='size-next'); stack.expect_count('stream-size-rotation', 65)
    assert len(indices(stack, 'size-rotation')) >= 2
    return {'threshold_bytes': 1048576, 'observed_primary_bytes': stats, 'size_rotation': True}


def stream_retention(stack):
    create(stack, 'retention-fixture', documents=1, retention=1)
    events = send(stack, 'retention-fixture', 2, prefix='expire')
    stack.expect_count('stream-retention-fixture', 2)
    old, active = indices(stack, 'retention-fixture')
    stack.sql("UPDATE stream_indices SET sealed_at=now()-interval '2 days' WHERE index_name='"+old+"'")
    stack.wait(lambda: stack.sql("SELECT deleted_at IS NOT NULL FROM stream_indices WHERE index_name='"+old+"'") == 't', 'sealed index expired', timeout=35)
    assert stack.request('/'+old, method='HEAD', base=stack.os)[0] == 404
    assert stack.request('/'+active, method='HEAD', base=stack.os)[0] == 200
    stack.wait(lambda: stack.count('stream-retention-fixture') == 1, 'expired data absent from search')
    identity = hashlib.sha256(('logs-v2\x00' + events[0]['id']).encode()).hexdigest()
    assert stack.redis('HGET', 'logister:evt:logs-v2:' + identity, 'body') in ('', '(nil)')
    assert stack.redis('HGET', 'logister:evt:logs-v2:' + identity, 'hash') not in ('', '(nil)')
    status, receipt = stack.request('/api/streams/retention-fixture/ingest', events[:1])
    assert status == 202 and receipt['duplicates'] == 1
    time.sleep(.5)
    assert stack.count('stream-retention-fixture') == 1
    return {'expired_index_deleted': True, 'active_index_preserved': True, 'published_payload_cleared': True, 'retry_does_not_resurrect': True}


def stream_pending_retention(stack):
    create(stack, 'pending-retention', documents=1, retention=1)
    stack.compose('stop', 'opensearch')
    try:
        send(stack, 'pending-retention', 2, prefix='pending-expire')
        old = indices(stack, 'pending-retention')[0]
        stack.sql("UPDATE stream_indices SET sealed_at=now()-interval '2 days' WHERE index_name='"+old+"'")
        time.sleep(12)
        assert stack.sql("SELECT deleted_at IS NULL FROM stream_indices WHERE index_name='"+old+"'") == 't'
        pending = stack.redis('GET', 'logister:target-pending:logs-v2:' + old)
        assert pending not in ('', '(nil)', '0')
    finally:
        stack.compose('start', 'opensearch')
    stack.wait(lambda: stack.request('/api/health')[0] == 200, 'search restored', timeout=150)
    stack.wait(lambda: stack.sql("SELECT deleted_at IS NOT NULL FROM stream_indices WHERE index_name='"+old+"'") == 't', 'expired index removed only after durable publication', timeout=60)
    stack.wait(lambda: stack.count('stream-pending-retention') == 1, 'active record retained')
    return {'pending_event_protected_during_outage': True, 'deletion_waits_for_publication': True}


def stream_settings_restart(stack):
    stream = create(stack, 'persistent-settings')
    stream.update(title='Saved title', description='Saved after API restart')
    stream['storage']['max_documents'] = 123
    assert stack.request('/api/streams/persistent-settings', stream, method='PUT')[0] == 200
    assert stack.request('/api/streams', stream)[0] == 409
    stack.compose('restart', 'api')
    stack.wait(lambda: stack.request('/api/health')[0] == 200, 'API restarted', timeout=60)
    saved = next(s for s in stack.request('/api/streams')[1]['streams'] if s['name'] == stream['name'])
    assert saved['title'] == 'Saved title' and saved['storage']['max_documents'] == 123
    return {'settings_persist_after_restart': True, 'duplicate_name_rejected': True}


def stream_search_errors(stack):
    status, body = stack.request('/api/search?' + urllib.parse.urlencode({'q': '(', 'stream': 'web-access'}))
    assert status == 400 and body['error'] == 'Invalid search query. Check the syntax and field values.', (status, body)
    assert stack.request('/api/search?size=201')[0] == 400
    return {'invalid_query_across_multiple_indices': 400, 'invalid_parameters': 400}


def stream_preview_and_fallback_search(stack):
    baseline = stack.count()
    sample = {'source': 'nginx', 'message': 'routing-preview-only', 'level': 'info'}
    status, result = stack.request('/api/streams/preview-routing', {'message': sample})
    assert status == 200 and result['stream'] == 'web-access' and result['reason'] == 'rule', (status, result)
    selected = next(candidate for candidate in result['candidates'] if candidate['selected'])
    assert selected['rules'][0]['actual'] == 'nginx' and selected['rules'][0]['matched']
    saved = next(s for s in stack.request('/api/streams')[1]['streams'] if s['name'] == 'web-access')
    draft = json.loads(json.dumps(saved)); draft['routing']['rules'][0]['value'] = 'apache'
    status, result = stack.request('/api/streams/preview-routing', {'message': sample, 'draft': draft})
    assert status == 200 and result['stream'] == 'all' and result['reason'] == 'fallback'
    after = next(s for s in stack.request('/api/streams')[1]['streams'] if s['name'] == 'web-access')
    assert after == saved and stack.count() == baseline, 'preview changed saved rules or ingested its sample'
    status, result = stack.request('/api/search?' + urllib.parse.urlencode({'stream': '@default', 'size': 200}))
    assert status == 200 and result['total'] > 0 and all(m['stream'] == 'all' for m in result['messages']), (status, result)
    stack.close(result)
    return {'preview_matches_real_rules': True, 'unsaved_draft_evaluated': True, 'preview_read_only': True, 'fallback_separate_from_all_search': True}


def run_stream_checks(stack):
    for check in [stream_routing, stream_rollover_and_retry, stream_age_rotation, stream_size_rotation,
                  stream_retention, stream_pending_retention, stream_settings_restart, stream_search_errors,
                  stream_preview_and_fallback_search]:
        stack.run(check.__name__, lambda check=check: check(stack))
    for check in [arbitrary_json_fields, arbitrary_json_types, legacy_json_retry]:
        stack.run(check.__name__, lambda check=check: check(stack))


def arbitrary_json_fields(stack):
    create(stack, 'arbitrary-json', routing={'enabled': True, 'priority': 1, 'match': 'all', 'rules': [
        {'field': 'tenant.id', 'operator': 'equals', 'value': 'acme'},
        {'field': 'events[].code', 'operator': 'equals', 'value': '503'},
        {'field': '/random ~1 key/a.b', 'operator': 'exists', 'value': ''}]})
    event={'id':'arbitrary-complete','source':'arbitrary-fixture','tenant':{'id':'acme'},
           'events':[{'code':200},{'code':503}],'random / key':{'a.b':None},'host':{'name':'node'},
           'timestamp':{'random':'shape'},'tags':['blue','production'],'ключ с пробелом':True}
    preview=stack.request('/api/streams/preview-routing',{'message':event})
    assert preview[0]==200 and preview[1]['stream']=='arbitrary-json',preview
    assert stack.request('/api/ingest/json',event)[0]==202
    stack.expect_count('arbitrary-fixture',1)
    message=stack.all_messages('source:arbitrary-fixture')[0]
    assert message['stream']=='arbitrary-json' and message['payload']==event
    assert json.loads(message['payload_json'])==event
    assert stack.accept([event])['duplicates']==1
    changed=dict(event);changed['tags']=['changed']
    assert stack.request('/api/ingest/json',changed)[0]==409
    return {'no_message_required':True,'all_original_fields_preserved':True,'arrays_and_JSON_pointer_route':True,'extra_field_change_conflicts':True}


def arbitrary_json_types(stack):
    events=[{'id':f'arbitrary-type-{i}','source':'arbitrary-types','varied':value,'fields':{'large':900719925474099312345,'long':'z'*40000,'nullchar':'a\x00b'}}
            for i,value in enumerate(['text',7,True,{'inner':[1,None]},['a',2],None])]
    for event in events:
        raw=json.dumps(dict(event,huge='REPLACE_EXPONENT'),ensure_ascii=False).replace('"REPLACE_EXPONENT"','1e999999').encode()
        assert stack.request('/api/ingest/json',raw)[0]==202
    stack.expect_count('arbitrary-types',6)
    found={m['payload']['id']:m for m in stack.all_messages('source:arbitrary-types')}
    for event in events:
        message=found[event['id']]
        assert message['payload']['varied']==event['varied']
        assert message['fields']['large']==900719925474099312345
        assert message['fields']['long']=='z'*40000
        assert message['fields']['nullchar']=='a\x00b'
        assert '1e999999' in message['payload_json']
    return {'mixed_types':6,'large_integer_preserved':True,'extreme_exponent_stored_safely':True,'long_values_and_NUL_preserved':True}


def legacy_json_retry(stack):
    event={'id':'pre-json-upgrade','message':'legacy retry','source':'legacy-json','fields':{'seq':1}}
    canonical={'message':event['message'],'source':event['source'],'level':'','host':'','stream':'','facility':'','timestamp':None,'fields':event['fields'],'log':''}
    previous_hash=hashlib.sha256(json.dumps(canonical,separators=(',',':')).encode()).hexdigest()
    identity=hashlib.sha256(('logs-v2\x00'+event['id']).encode()).hexdigest()
    stack.redis('HSET', 'logister:evt:logs-v2:' + identity, 'hash', previous_hash, 'ver', '1')
    receipt=stack.accept([event])
    assert receipt['duplicates']==1
    assert stack.redis('HGET', 'logister:evt:logs-v2:' + identity, 'ver') == '1'
    return {'pre_upgrade_identity_retry_accepted':True,'logical_records':1}
