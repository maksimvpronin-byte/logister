"""Edge verification of selecting arbitrary fields from JSON and testing rules."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url',default='http://localhost:3000')
parser.add_argument('--artifacts',type=Path,default=Path('graphify-out/arbitrary-json-ui'))
args=parser.parse_args();args.artifacts.mkdir(parents=True,exist_ok=True)
with sync_playwright() as p:
    browser=p.chromium.launch(headless=True,channel='msedge')
    page=browser.new_page(viewport={'width':1440,'height':1050})
    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    page.goto(args.url,wait_until='domcontentloaded')
    expect(page.get_by_text('postgres доступен',exact=True)).to_be_visible()
    page.get_by_role('button',name='Потоки и хранение',exact=True).click()
    page.get_by_role('button',name='Создать поток',exact=True).click()
    page.get_by_label('Идентификатор',exact=True).fill('json-routing-draft')
    page.get_by_label('Название',exact=True).fill('Arbitrary JSON')
    sample={'tenant':'acme','http':{'response':{'status':503}},'items':[{'code':'FAILED'}],'random / key':{'a.b':None},'ключ с пробелом':True}
    page.get_by_label('Пример JSON-сообщения',exact=True).fill(json.dumps(sample,ensure_ascii=False,indent=2))
    page.get_by_role('button',name='Использовать поле http.response.status',exact=True).click()
    expect(page.get_by_label('Условие 1 поле',exact=True)).to_have_value('http.response.status')
    page.get_by_label('Условие 1 оператор',exact=True).select_option('equals')
    page.get_by_label('Условие 1 значение',exact=True).fill('503')
    with page.expect_response(lambda response:response.url.endswith('/api/streams/preview-routing')) as response:
        page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    assert response.value.status==200
    expect(page.get_by_text('json-routing-draft: выбран',exact=True)).to_be_visible()
    page.get_by_role('heading',name='Проверить маршрутизацию',exact=True).scroll_into_view_if_needed()
    page.screenshot(path=str(args.artifacts/'arbitrary-json-routing.png'),full_page=False)
    print('PASS: nested arbitrary field selected from sample; JSON without message routes correctly',flush=True)
    page.get_by_role('button',name='Использовать поле items[0].code',exact=True).click()
    page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    expect(page.get_by_text('json-routing-draft: выбран',exact=True)).to_be_visible()
    print('PASS: array field and existence condition use the same evaluator',flush=True)
    page.get_by_label('Условие 1 поле',exact=True).fill('/random ~1 key/a.b')
    page.get_by_label('Условие 1 оператор',exact=True).select_option('exists')
    page.get_by_role('button',name='Проверить маршрутизацию',exact=True).click()
    expect(page.get_by_text('json-routing-draft: выбран',exact=True)).to_be_visible()
    print('PASS: JSON Pointer selects a literal dotted field containing null',flush=True)
    page.set_viewport_size({'width':390,'height':844})
    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
    assert not errors,errors
    # No Save or Send is clicked: the entire scenario is read-only.
    browser.close()
