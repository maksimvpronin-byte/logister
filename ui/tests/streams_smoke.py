"""Real Edge checks for stream settings, scoped ingestion and rotation."""
import argparse
import json
from pathlib import Path
import time
import urllib.parse
import uuid

from playwright.sync_api import expect, sync_playwright


parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url',default='http://localhost:3000')
parser.add_argument('--artifacts',type=Path,default=Path('graphify-out/streams-ui'))
args=parser.parse_args();args.artifacts.mkdir(parents=True,exist_ok=True)

with sync_playwright() as p:
    browser=p.chromium.launch(headless=True,channel='msedge')
    page=browser.new_page(viewport={'width':1440,'height':1100})
    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    page.goto(args.url)
    expect(page.get_by_text('postgres up',exact=True)).to_be_visible()
    page.get_by_role('button',name='Streams & storage',exact=True).click()
    expect(page.get_by_label('Name',exact=True)).to_be_visible()
    page.get_by_role('button',name='Create stream',exact=True).click()
    name='browser-stream-'+uuid.uuid4().hex[:8]
    page.get_by_label('Name',exact=True).fill(name)
    page.get_by_label('Title',exact=True).fill('Browser stream')
    page.get_by_label('Description',exact=True).fill('Created and verified in Edge')
    page.get_by_label('Enable routing rules',exact=True).check()
    page.get_by_role('button',name='Add rule',exact=True).click()
    page.get_by_label('Rule 1 field',exact=True).fill('source')
    page.get_by_label('Rule 1 value',exact=True).fill(name)
    page.get_by_label('Maximum messages',exact=True).fill('2')
    page.get_by_label('Retention after rotation (days)',exact=True).fill('0')
    with page.expect_response(lambda response:response.request.method=='POST' and response.url.endswith('/api/streams')) as saved:
        page.get_by_role('button',name='Create stream settings',exact=True).click()
    assert saved.value.status==201
    expect(page.get_by_text('Stream settings saved. New messages use these settings.',exact=True)).to_be_visible()
    expect(page.get_by_label('Name',exact=True)).to_be_disabled()
    print('PASS: create stream with routing and safe storage defaults',flush=True)

    page.get_by_label('Maximum size (MiB)',exact=True).fill('8')
    with page.expect_response(lambda response:response.request.method=='PUT' and response.url.endswith('/api/streams/'+name)) as updated:
        page.get_by_role('button',name='Save settings',exact=True).click()
    assert updated.value.status==200
    page.reload()
    page.get_by_role('button',name='Streams & storage',exact=True).click()
    page.get_by_role('button',name='Browser stream '+name,exact=True).click()
    expect(page.get_by_label('Maximum size (MiB)',exact=True)).to_have_value('8')
    print('PASS: persisted settings survive reload',flush=True)

    token='browser-message-'+uuid.uuid4().hex
    page.get_by_label('Message',exact=True).fill(token)
    with page.expect_response(lambda response:response.url.endswith('/api/streams/'+name+'/ingest')) as sent:
        page.get_by_role('button',name='Send message',exact=True).click()
    assert sent.value.status==202
    expect(page.get_by_text('Message accepted. It will appear in search after indexing.',exact=True)).to_be_visible()
    print('PASS: UI writes a message to the selected stream',flush=True)

    page.get_by_role('button',name='Refresh indices',exact=True).click()
    expect(page.get_by_role('cell',name='Active',exact=True)).to_be_visible()
    page.screenshot(path=str(args.artifacts/'stream-settings.png'),full_page=True)
    page.get_by_role('group',name='Rotation & retention',exact=True).scroll_into_view_if_needed()
    page.screenshot(path=str(args.artifacts/'stream-storage.png'),full_page=False)
    with page.expect_response(lambda response:response.url.endswith('/api/streams/'+name+'/rotate')) as rotated:
        page.get_by_role('button',name='Rotate now',exact=True).click()
    assert rotated.value.status==200
    expect(page.get_by_role('cell',name='Sealed',exact=True)).to_be_visible()
    print('PASS: manual rotation seals the managed index',flush=True)

    page.get_by_role('button',name='Messages',exact=True).click()
    page.get_by_label('Search query',exact=True).fill(token)
    page.get_by_label('Stream',exact=True).select_option(name)
    # 202 acknowledges PostgreSQL; poll through the actual UI until projection.
    for attempt in range(15):
        with page.expect_response(lambda response:'/api/search?' in response.url):
            page.get_by_role('button',name='Search',exact=True).click()
        if page.get_by_role('cell',name=token,exact=True).count():break
        time.sleep(.3)
    expect(page.get_by_role('cell',name=token,exact=True)).to_be_visible()
    page.screenshot(path=str(args.artifacts/'stream-message.png'),full_page=False)
    print('PASS: saved stream appears in search and finds the ingested message',flush=True)

    page.set_viewport_size({'width':390,'height':844})
    page.get_by_role('button',name='Streams & storage',exact=True).click()
    page.get_by_role('button',name='Browser stream '+name,exact=True).click()
    expect(page.get_by_label('Title',exact=True)).to_be_visible()
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth'), 'mobile page overflows'
    page.screenshot(path=str(args.artifacts/'stream-settings-mobile.png'),full_page=True)
    print('PASS: stream settings fit a mobile viewport',flush=True)
    assert not errors,errors
    browser.close()
