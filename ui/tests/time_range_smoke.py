"""Read-only real-API verification of absolute search bounds in Edge/Moscow."""
import argparse
import datetime as dt
import json
from pathlib import Path
import urllib.parse
import urllib.request
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:3000')
parser.add_argument('--artifacts', type=Path, default=Path('graphify-out/time-range-ui'))
args = parser.parse_args()
args.artifacts.mkdir(parents=True, exist_ok=True)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def api(path, body=None, method=None):
    req = urllib.request.Request('http://localhost:8080' + path, method=method,
        data=None if body is None else json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
    with opener.open(req, timeout=15) as response:
        raw = response.read()
        return json.loads(raw) if raw else None

def timestamp(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))

def close(result):
    if result.get('session_id'):
        api('/api/search/session', {'session_id': result['session_id']}, 'DELETE')

fixture = api('/api/search?' + urllib.parse.urlencode({'q': 'source:demo-app-123-*', 'stream': 'new-stream', 'relative': '7d', 'size': 100}))
close(fixture)
assert fixture['total'] == 50
start = min(timestamp(m['timestamp']) for m in fixture['messages']).replace(microsecond=0) + dt.timedelta(seconds=10)
end = start + dt.timedelta(seconds=10)
expected = {m['id'] for m in fixture['messages'] if start <= timestamp(m['timestamp']) <= end}
assert len(expected) == 10
moscow = dt.timezone(dt.timedelta(hours=3))
local = lambda value: value.astimezone(moscow).strftime('%d.%m.%Y %H:%M:%S')

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, channel='msedge')
    context = browser.new_context(viewport={'width': 1440, 'height': 1050}, timezone_id='Europe/Moscow')
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(args.url)
    expect(page.get_by_text('postgres доступен', exact=True)).to_be_visible()
    page.get_by_label('Поток', exact=True).select_option('new-stream')
    page.get_by_role('textbox', name='Поисковый запрос').fill('source:demo-app-123-*')
    page.get_by_label('Период', exact=True).select_option('custom')
    page.get_by_label('С', exact=True).fill(local(start))
    page.get_by_label('По', exact=True).fill(local(end))
    expect(page.get_by_text('Times in Europe/Moscow.', exact=False)).to_be_visible()
    # Reproduce accidental wheel/arrow use while the date field has focus.
    # Values and applied results must stay unchanged until an explicit Search.
    wheel_requests = []
    page.on('request', lambda req: wheel_requests.append(req.url) if '/api/search?' in req.url else None)
    for label in ['From', 'To']:
        control = page.get_by_label(label, exact=True)
        assert control.get_attribute('type') == 'text'
        control.click()
        control.hover()
        page.mouse.wheel(0, 480)
        control.press('ArrowUp')
        control.press('ArrowDown')
    expect(page.get_by_label('С', exact=True)).to_have_value(local(start))
    expect(page.get_by_label('По', exact=True)).to_have_value(local(end))
    assert not wheel_requests
    print('PASS: focused date fields ignore wheel and arrow changes; no search is triggered', flush=True)
    with page.expect_response(lambda response: '/api/search?' in response.url) as response:
        page.get_by_role('button', name='Найти', exact=True).click()
    query = urllib.parse.parse_qs(urllib.parse.urlparse(response.value.url).query)
    assert 'relative' not in query
    assert timestamp(query['from'][0]) == start and timestamp(query['to'][0]) == end
    result = response.value.json()
    assert result['total'] == 10 and {m['id'] for m in result['messages']} == expected, {
        'query': query, 'total': result['total'], 'expected_ids': sorted(expected),
        'actual': [(m['timestamp'], m['id']) for m in result['messages']],
    }
    expect(page.locator('.message-row')).to_have_count(10)
    expect(page.get_by_label('Выбранный период')).to_be_visible()
    page.screenshot(path=str(args.artifacts/'absolute-time-range.png'), full_page=False)
    print('PASS: Moscow local times become exact UTC bounds; precisely 10 expected real records returned', flush=True)

    requests = []
    page.on('request', lambda req: requests.append(req.url) if '/api/search?' in req.url else None)
    page.get_by_label('С', exact=True).fill(local(end))
    page.get_by_role('button', name='Найти', exact=True).click()
    expect(page.get_by_role('alert')).to_contain_text('Start must be earlier than end.')
    assert not requests
    page.get_by_label('По', exact=True).fill('')
    page.get_by_role('button', name='Найти', exact=True).click()
    expect(page.get_by_role('alert')).to_contain_text('Choose the end date and time.')
    assert not requests
    print('PASS: missing/equal bounds are rejected locally; prior results stay intact', flush=True)

    page.get_by_label('Поток', exact=True).select_option('all')
    page.get_by_role('textbox', name='Поисковый запрос').fill('*')
    page.get_by_label('С', exact=True).fill('2026-11-02T03:00')
    page.get_by_label('По', exact=True).fill('2026-11-04T03:00')
    with page.expect_response(lambda response: '/api/search?' in response.url) as initial_response:
        page.get_by_role('button', name='Найти', exact=True).click()
    original = initial_response.value.json()
    assert original['total'] > 100
    page.get_by_label('С', exact=True).fill('2026-11-05T03:00')
    with page.expect_response(lambda response: '/api/search?' in response.url) as next_response:
        page.get_by_role('button', name='Следующая страница', exact=True).click()
    cursor_query = urllib.parse.parse_qs(urllib.parse.urlparse(next_response.value.url).query)
    assert list(cursor_query) == ['cursor']
    next_page = next_response.value.json()
    assert next_page['from'] == original['from'] and next_page['to'] == original['to'] and next_page['offset'] == 100
    print('PASS: pagination keeps the applied absolute interval despite invalid draft edits', flush=True)
    page.get_by_label('Период', exact=True).select_option('1h')
    with page.expect_response(lambda response: '/api/search?' in response.url) as relative_response:
        page.get_by_role('button', name='Найти', exact=True).click()
    quick = urllib.parse.parse_qs(urllib.parse.urlparse(relative_response.value.url).query)
    assert quick['relative'] == ['1h'] and 'from' not in quick and 'to' not in quick
    page.set_viewport_size({'width': 390, 'height': 844})
    page.get_by_label('Период', exact=True).select_option('custom')
    expect(page.get_by_label('С', exact=True)).to_have_value('2026-11-05T03:00')
    expect(page.get_by_label('По', exact=True)).to_have_value('2026-11-04T03:00')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    assert not errors, errors
    print('PASS: relative presets remain available; custom inputs fit a mobile viewport', flush=True)
    browser.close()

# Exact inclusive boundaries are also checked against real whole-second cron events.
cron = api('/api/search?' + urllib.parse.urlencode({'q': 'source:cron', 'stream': 'system', 'relative': '24h', 'size': 100}))
close(cron)
assert len(cron['messages']) >= 25
upper = cron['messages'][5]['timestamp']
lower = cron['messages'][20]['timestamp']
inside = {m['id'] for m in cron['messages'] if timestamp(lower) <= timestamp(m['timestamp']) <= timestamp(upper)}
bounded = api('/api/search?' + urllib.parse.urlencode({'q': 'source:cron', 'stream': 'system', 'from': lower, 'to': upper, 'size': 100}))
close(bounded)
assert {m['id'] for m in bounded['messages']} == inside
assert any(m['timestamp'] == lower for m in bounded['messages']) and any(m['timestamp'] == upper for m in bounded['messages'])
print('PASS: both exact endpoint timestamps are included in real OpenSearch results', flush=True)
