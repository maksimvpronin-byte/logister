"""Verify stream ID generation and native browser validation without saving data."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:3000')
parser.add_argument('--artifacts', type=Path, default=Path('graphify-out/stream-name-ui'))
args = parser.parse_args()
args.artifacts.mkdir(parents=True, exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, channel='msedge')
    page = browser.new_page(viewport={'width': 1440, 'height': 1000})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.on('console', lambda message: errors.append(message.text) if 'Invalid regular expression' in message.text else None)
    page.goto(args.url)
    expect(page.get_by_text('postgres доступен', exact=True)).to_be_visible()
    page.get_by_role('button', name='Потоки и хранение', exact=True).click()
    page.get_by_role('button', name='Создать поток', exact=True).click()
    title = page.get_by_label('Название', exact=True)
    name = page.get_by_label('Идентификатор', exact=True)
    title.fill('New stream')
    expect(name).to_have_value('new-stream')
    name.fill('New stream')
    expect(name).to_have_value('new-stream')
    title.fill('New stream title')
    expect(name).to_have_value('new-stream')
    assert name.evaluate('(input) => input.checkValidity()')
    print('PASS: title generates the ID; manual New stream becomes new-stream and survives title edits', flush=True)

    captured = []
    def intercept(route):
        if route.request.method != 'POST':
            route.continue_()
            return
        captured.append(route.request.post_data_json)
        route.fulfill(status=503, content_type='application/json', body=json.dumps({'error': 'Browser check: save intercepted'}))
    page.route('**/api/streams', intercept)
    name.fill('-invalid')
    assert not name.evaluate('(input) => input.checkValidity()')
    page.get_by_role('button', name='Создать поток с настройками', exact=True).click()
    assert not captured
    print('PASS: invalid ID is blocked before any API write request', flush=True)
    name.fill('with_underscore-1')
    assert name.evaluate('(input) => input.checkValidity()')
    name.fill('New stream')
    name.scroll_into_view_if_needed()
    page.screenshot(path=str(args.artifacts/'stream-name.png'), full_page=False)
    page.get_by_role('button', name='Создать поток с настройками', exact=True).click()
    expect(page.get_by_role('alert')).to_contain_text('Browser check: save intercepted')
    assert len(captured) == 1 and captured[0]['name'] == 'new-stream'
    print('PASS: valid ID reaches save with normalized payload (intercepted, no settings written)', flush=True)
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    assert not errors, errors
    browser.close()
