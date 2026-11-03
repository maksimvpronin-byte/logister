"""Real Edge check of visible rules and read-only routing preview."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url',default='http://localhost:3000')
parser.add_argument('--artifacts',type=Path,default=Path('graphify-out/routing-ui'))
args=parser.parse_args();args.artifacts.mkdir(parents=True,exist_ok=True)
with sync_playwright() as p:
    browser=p.chromium.launch(headless=True,channel='msedge')
    page=browser.new_page(viewport={'width':1440,'height':1050})
    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    page.goto(args.url)
    expect(page.get_by_text('postgres доступен',exact=True)).to_be_visible()
    select=page.get_by_label('Поток',exact=True)
    expect(select).to_contain_text('Все сообщения');expect(select).to_contain_text('Сообщения без совпавших правил')
    page.get_by_role('button',name='Потоки и хранение',exact=True).click()
    expect(page.get_by_role('region',name='Сохранённые правила маршрутизации')).to_contain_text('source = "nginx"')
    expect(page.get_by_role('region',name='Сохранённые правила маршрутизации')).to_contain_text('source = "cron"')
    page.get_by_role('button',name='Web access web-access',exact=True).click()
    expect(page.get_by_label('Включить правила маршрутизации',exact=True)).to_be_checked()
    expect(page.get_by_label('Условие 1 значение',exact=True)).to_have_value('nginx')
    sample={'source':'nginx','message':'GET /preview-only','level':'info'}
    page.get_by_label('Пример JSON-сообщения',exact=True).fill(json.dumps(sample))
    with page.expect_response(lambda response:response.url.endswith('/api/streams/preview-routing')) as response:
        page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    assert response.value.status==200
    expect(page.get_by_text('web-access: выбран',exact=True)).to_be_visible()
    page.screenshot(path=str(args.artifacts/'routing-rules.png'),full_page=True)
    page.get_by_role('heading',name='Проверить маршрутизацию',exact=True).scroll_into_view_if_needed()
    page.screenshot(path=str(args.artifacts/'routing-preview.png'),full_page=False)
    print('PASS: active saved rules are visible and preview explains the matched field',flush=True)
    page.get_by_label('Условие 1 значение',exact=True).fill('apache')
    page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    expect(page.get_by_text('Ни одно включённое правило не подошло. Сообщение попадает в поток по умолчанию.',exact=True)).to_be_visible()
    page.reload()
    page.get_by_role('button',name='Потоки и хранение',exact=True).click()
    page.get_by_role('button',name='Web access web-access',exact=True).click()
    expect(page.get_by_label('Условие 1 значение',exact=True)).to_have_value('nginx')
    print('PASS: unsaved changes affect preview but do not modify saved routing',flush=True)
    sample['stream']='system'
    page.get_by_label('Пример JSON-сообщения',exact=True).fill(json.dumps(sample))
    page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    expect(page.get_by_text('Поток указан в сообщении явно. Автоматические правила не применяются.',exact=True)).to_be_visible()
    print('PASS: explicit destination is explained',flush=True)
    page.get_by_label('Пример JSON-сообщения',exact=True).fill('invalid')
    page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    expect(page.get_by_role('alert')).to_have_text('Введите корректное JSON-сообщение.')
    print('PASS: invalid preview JSON receives a clear error',flush=True)
    page.set_viewport_size({'width':390,'height':844})
    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    assert not errors,errors
    browser.close()
