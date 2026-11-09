"""Read-only Edge check of the field viewer using the user's new-stream demo logs."""
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:3000')
parser.add_argument('--artifacts', type=Path, default=Path('graphify-out/message-fields-ui'))
args = parser.parse_args()
args.artifacts.mkdir(parents=True, exist_ok=True)
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, channel='msedge')
    page = browser.new_page(viewport={'width': 1440, 'height': 1100})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(args.url)
    expect(page.get_by_text('mongo доступен', exact=True)).to_be_visible()
    page.get_by_label('Поток', exact=True).select_option('new-stream')
    page.get_by_label('Период', exact=True).select_option('24h')
    page.get_by_role('textbox', name='Поисковый запрос').fill('source:demo-app-123-*')
    with page.expect_response(lambda response: '/api/search?' in response.url):
        page.get_by_role('button', name='Найти', exact=True).click()
    expect(page.locator('.message-row')).to_have_count(50)
    page.get_by_role('button', name='Показать поля сообщения', exact=False).first.click()
    panel = page.get_by_role('region', name='Подробности сообщения', exact=True)
    expect(panel).to_be_visible()
    expect(panel.locator('.field-key').get_by_text('app', exact=True)).to_be_visible()
    expect(panel.locator('.field-value').get_by_text('123', exact=True)).to_be_visible()
    expect(panel.locator('.field-key').get_by_text('response', exact=True)).to_be_attached()
    expect(page.locator('.result-detail')).to_contain_text('Поля сообщения')
    panel.scroll_into_view_if_needed()
    panel.screenshot(path=str(args.artifacts/'message-fields.png'))
    print('PASS: real JSON is displayed as typed fields beside the result list', flush=True)

    field_filter = page.get_by_role('searchbox', name='Фильтр полей')
    field_filter.fill('response.status')
    expect(panel.locator('.field-key').get_by_text('status', exact=True)).to_be_visible()
    expect(panel.locator('.field-value').get_by_text('503', exact=True)).to_be_visible()
    expect(panel.locator('.field-key').get_by_text('app', exact=True)).to_have_count(0)
    field_filter.fill('no-such-field')
    expect(panel.get_by_text('Подходящие поля не найдены.', exact=True)).to_be_visible()
    field_filter.fill('')
    print('PASS: nested paths can be filtered; empty matches are explained', flush=True)

    page.get_by_role('tab', name='JSON', exact=True).click()
    expect(panel.locator('pre')).to_contain_text('"app": "123"')
    expect(panel.locator('pre')).to_contain_text('"status": 503')
    assert '\n  "app"' in panel.locator('pre').inner_text()
    page.get_by_role('tab', name='Поля', exact=True).click()
    tags = panel.locator('.field-group').filter(has=page.locator('summary .field-key').get_by_text('tags', exact=True))
    expect(tags.locator('.field-key').get_by_text('[0]', exact=True)).to_be_visible()
    print('PASS: JSON is formatted separately; arrays have individual indexed fields', flush=True)
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    page.get_by_role('button', name='Закрыть подробности сообщения').click()
    expect(panel).to_have_count(0)
    assert not errors, errors
    browser.close()
