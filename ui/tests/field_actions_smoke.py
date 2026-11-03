"""Read-only field-menu checks against real indexed logs in Edge."""
import argparse
from pathlib import Path
import urllib.parse
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:3000')
parser.add_argument('--artifacts', type=Path, default=Path('graphify-out/field-actions-ui'))
args = parser.parse_args()
args.artifacts.mkdir(parents=True, exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, channel='msedge')
    page = browser.new_page(viewport={'width': 1440, 'height': 1050})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(args.url)
    expect(page.get_by_text('postgres доступен', exact=True)).to_be_visible()
    query = page.get_by_role('textbox', name='Поисковый запрос')
    base = 'source:gateway OR source:nginx'
    query.fill(base)
    with page.expect_response(lambda response: '/api/search?' in response.url and urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query).get('q') == [base]) as response:
        page.get_by_role('button', name='Найти', exact=True).click()
    sample = response.value.json()['messages'][0]
    client = sample['fields']['client']
    page.get_by_role('button', name='Показать поля сообщения', exact=False).first.click()
    panel = page.get_by_role('region', name='Подробности сообщения', exact=True)
    page.get_by_role('searchbox', name='Фильтр полей').fill('fields.client')
    trigger = page.get_by_role('button', name='Действия с полем fields.client', exact=True)
    trigger.click()
    expect(page.get_by_role('menu')).to_be_visible()
    expect(trigger).to_have_attribute('aria-expanded', 'true')
    page.get_by_role('menuitem', name='Добавить в поиск', exact=False).press('ArrowDown')
    expect(page.get_by_role('menuitem', name='Исключить из поиска', exact=False)).to_be_focused()
    page.keyboard.press('Escape')
    expect(page.get_by_role('menu')).to_have_count(0)
    expect(trigger).to_be_focused()
    trigger.click()
    page.screenshot(path=str(args.artifacts/'field-actions.png'), full_page=False)
    page.get_by_role('menuitem', name='Добавить в поиск', exact=False).click()
    expect(query).to_have_value(f'({base}) AND (fields.client:"{client}")')
    expect(panel).to_be_visible()
    with page.expect_response(lambda response: '/api/search?' in response.url) as included:
        page.get_by_role('button', name='Найти', exact=True).click()
    result = included.value.json()
    assert included.value.status == 200 and result['total'] > 0
    assert all(m['fields']['client'] == client and m['source'] in ['gateway', 'nginx'] for m in result['messages'])
    print('PASS: include adds a nested field condition and preserves existing OR semantics in real search', flush=True)

    page.get_by_role('button', name='Показать поля сообщения', exact=False).first.click()
    page.get_by_role('searchbox', name='Фильтр полей').fill('fields.path')
    path = result['messages'][0]['fields']['path']
    query.fill(base)
    page.get_by_role('button', name='Действия с полем fields.path', exact=True).click()
    page.get_by_role('menuitem', name='Исключить из поиска', exact=False).click()
    assert 'AND NOT (fields.path:' in query.input_value() and '\\/' in query.input_value()
    with page.expect_response(lambda response: '/api/search?' in response.url) as excluded:
        page.get_by_role('button', name='Найти', exact=True).click()
    result = excluded.value.json()
    assert excluded.value.status == 200 and result['total'] > 0
    assert all(m['fields']['path'] != path and m['source'] in ['gateway', 'nginx'] for m in result['messages'])
    print('PASS: exclusion escapes a URL path and removes matching values in real search', flush=True)

    page.get_by_role('button', name='Показать поля сообщения', exact=False).first.click()
    page.get_by_role('searchbox', name='Фильтр полей').fill('host')
    host = result['messages'][0]['host']
    query.fill('*')
    trigger = page.get_by_role('button', name='Действия с полем host', exact=True)
    trigger.click()
    page.get_by_role('menuitem', name='Добавить в поиск', exact=False).click()
    with page.expect_response(lambda response: '/api/search?' in response.url) as host_search:
        page.get_by_role('button', name='Найти', exact=True).click()
    host_result = host_search.value.json()
    assert host_search.value.status == 200 and host_result['total'] > 0
    assert all(m['host'] == host for m in host_result['messages'])
    print('PASS: escaped hyphens in metadata values match the exact host', flush=True)

    page.get_by_role('button', name='Показать поля сообщения', exact=False).first.click()
    page.get_by_role('searchbox', name='Фильтр полей').fill('delivery_id')
    trigger = page.get_by_role('button', name='Действия с полем delivery_id', exact=True)
    trigger.click()
    expect(page.get_by_role('menuitem', name='Добавить в поиск', exact=False)).to_be_disabled()
    expect(page.get_by_role('menu')).to_contain_text('не входит в поисковый индекс')
    page.keyboard.press('Escape')
    page.get_by_role('searchbox', name='Фильтр полей').fill('host')
    page.set_viewport_size({'width': 390, 'height': 844})
    trigger = page.get_by_role('button', name='Действия с полем host', exact=True)
    trigger.click()
    menu = page.get_by_role('menu')
    expect(menu).to_be_visible()
    box = menu.bounding_box()
    assert box['x'] >= 0 and box['x'] + box['width'] <= 390 and box['y'] >= 0 and box['y'] + box['height'] <= 844
    page.keyboard.press('Escape')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    assert not errors, errors
    print('PASS: unindexed values are explained; menu stays inside the mobile viewport', flush=True)
    browser.close()
