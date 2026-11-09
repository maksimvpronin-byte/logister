"""Browser regression checks for the running local UI (Windows + installed Edge).

Run: py ui/tests/browser_smoke.py
Requires the Python playwright package. Test data is read from the running app;
the only mocked response is the isolated page's partial-outage health response.
"""

import argparse
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import expect, sync_playwright


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:3000')
parser.add_argument('--deep-query', help='Optional fixture query with >10000 matches for a real UI traversal')
parser.add_argument('--artifacts', type=Path,
                    default=Path(__file__).resolve().parents[2] / 'graphify-out/search-ui')
args = parser.parse_args()
args.artifacts.mkdir(parents=True, exist_ok=True)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, channel='msedge')
    page = browser.new_page(viewport={'width': 1440, 'height': 900})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(args.url)
    expect(page.get_by_text('mongo доступен', exact=True)).to_be_visible()
    expect(page.get_by_text('opensearch доступен', exact=True)).to_be_visible()
    expect(page.locator('tbody tr')).to_have_count(100)
    print('PASS: real UI, metadata and first page loaded', flush=True)

    with page.expect_response(lambda response: '/api/search?' in response.url) as response:
        page.get_by_role('button', name='Следующая страница').click()
    next_query = parse_qs(urlparse(response.value.url).query)
    assert list(next_query) == ['cursor']
    expect(page.get_by_text('Показаны 101', exact=False)).to_be_visible()
    with page.expect_response(lambda response: '/api/search?' in response.url) as response:
        page.get_by_role('button', name='Предыдущая страница').click()
    assert list(parse_qs(urlparse(response.value.url).query)) == ['cursor']
    expect(page.get_by_role('button', name='Предыдущая страница')).to_be_disabled()
    print('PASS: next and previous pages use snapshot cursors', flush=True)

    if args.deep_query:
        page.get_by_role('textbox', name='Поисковый запрос').fill(args.deep_query)
        with page.expect_response(lambda response: '/api/search?' in response.url):
            page.get_by_role('button', name='Найти', exact=True).click()
        expect(page.get_by_text('Показаны 1',exact=False)).to_be_visible()
        steps=0
        while page.get_by_role('button',name='Следующая страница').is_enabled():
            with page.expect_response(lambda response: '/api/search?' in response.url):
                page.get_by_role('button',name='Следующая страница').click()
            steps+=1
            assert steps<200, 'Fixture unexpectedly requires more than 200 pages'
            first_row = f'{steps * 100 + 1:,}'.replace(',', ' ')
            expect(page.get_by_text(f'Показаны {first_row}', exact=False).first).to_be_visible()
        expect(page.get_by_text('Показаны 10 001', exact=False).first).to_be_visible()
        page.screenshot(path=str(args.artifacts/'deep-pagination-fixed.png'),full_page=False)
        print('PASS: browser traversed',steps+1,'snapshot pages beyond 10000 results',flush=True)

    page.get_by_role('textbox', name='Поисковый запрос').fill('level:error')
    page.get_by_label('Поток', exact=True).select_option('web-access')
    page.get_by_label('Период', exact=True).select_option('15m')
    with page.expect_response(lambda response: '/api/search?' in response.url):
        page.get_by_role('button', name='Найти', exact=True).click()
    expect(page.locator('tbody tr').first).to_be_visible()
    levels = page.locator('tbody .level').all_text_contents()
    assert levels and all(level == 'error' for level in levels), levels
    assert page.locator('.level.error').first.evaluate('(el) => getComputedStyle(el).padding') == '0px'
    page.locator('tbody tr').first.click()
    expect(page.locator('.details .message-stream')).to_contain_text('web-access')
    page.screenshot(path=str(args.artifacts / 'search-pages-fixed.png'), full_page=False)
    print('PASS: query, stream/time filters, details and ERROR styling', flush=True)

    page.get_by_role('textbox', name='Поисковый запрос').fill('(')
    with page.expect_response(lambda response: '/api/search?' in response.url) as response:
        page.get_by_role('button', name='Найти', exact=True).click()
    assert response.value.status == 400
    expect(page.get_by_role('alert')).to_have_text('Invalid search query. Check the syntax and field values.')
    expect(page.locator('tbody tr')).to_have_count(0)
    expect(page.locator('.meta')).to_contain_text('0 hits')
    page.screenshot(path=str(args.artifacts / 'search-error-fixed.png'),
                    clip={'x': 80, 'y': 20, 'width': 1280, 'height': 430})
    print('PASS: invalid query shows a clear error and clears stale rows/counters', flush=True)

    page.get_by_role('textbox', name='Поисковый запрос').fill('source:no-matches-browser-smoke')
    with page.expect_response(lambda response: '/api/search?' in response.url):
        page.get_by_role('button', name='Найти', exact=True).click()
    expect(page.get_by_text('За выбранный период сообщений нет.', exact=True)).to_be_visible()
    expect(page.get_by_role('alert')).to_have_count(0)
    print('PASS: no-match search is separate from a failed search', flush=True)

    page.route('**/api/health', lambda route: route.fulfill(
        status=503, content_type='application/json', body=json.dumps({
            'status': 'degraded', 'mongo': True, 'opensearch': False,
            'time': '2026-11-03T00:00:00Z',
        })))
    page.reload()
    expect(page.get_by_text('mongo доступен', exact=True)).to_be_visible()
    expect(page.get_by_text('opensearch недоступен', exact=True)).to_be_visible()
    expect(page.get_by_label('Поток', exact=True)).to_contain_text('Web access')
    print('PASS: mocked partial outage preserves PostgreSQL and stream metadata', flush=True)
    assert not errors, errors
    browser.close()
