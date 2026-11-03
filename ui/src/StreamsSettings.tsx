import { apiError, streamDisplayTitle } from "./locale";
import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { sampleFields } from "./jsonFields";
import { deleteStream, restoreStream, fetchDeletedStreams, fetchStreams, fetchStreamIndices, saveStream, rotateStream, sendStreamMessage, previewRouting, type Stream, type StreamDraft, type StreamIndex, type RoutingRule, type RoutingPreview } from "./api";

function emptyDraft(): StreamDraft {
  return { name: "", title: "", description: "", routing: { enabled: false, priority: 100, match: "all", rules: [] }, storage: { max_age_hours: 24, max_size_mb: 1024, max_documents: 1000000, retention_days: 0 } };
}
function editDraft(stream: Stream): StreamDraft {
  const defaults = emptyDraft();
  return { name: stream.name, title: stream.title, description: stream.description, routing: stream.routing ?? defaults.routing, storage: stream.storage ?? defaults.storage };
}
function errorText(error: unknown) { return error instanceof Error ? error.message : "Не удалось выполнить запрос. Повторите попытку."; }
const streamTitle = streamDisplayTitle;
function ruleText(rule: RoutingRule) { return rule.operator === "exists" || rule.operator === "not_exists" ? `${rule.field} ${rule.operator === "exists" ? "существует" : "отсутствует"}` : `${rule.field} ${rule.operator === "equals" ? "=" : rule.operator === "starts_with" ? "начинается с" : "содержит"} ${JSON.stringify(rule.value)}`; }
function suggestedName(title: string) {
  return title.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-").replace(/^[-_]+|[-_]+$/g, "").slice(0, 63);
}
const validName = /^[a-z0-9][a-z0-9_-]{0,62}$/;
const nameHelp = "Технический идентификатор: 1–63 строчные латинские буквы, цифры, дефисы или подчёркивания; начало — буква или цифра. Используется в адресах API и не меняется после создания.";

export default function StreamsSettings({ onSaved }: { onSaved: (streams: Stream[]) => void }) {
  const [deletedStreams,setDeletedStreams]=useState<Stream[]>([]);
  const [deleteOpen,setDeleteOpen]=useState(false);
  const [deleteName,setDeleteName]=useState("");
  const [copyStatus,setCopyStatus]=useState<""|"copied"|"manual">("");
  const copySource=useRef<HTMLInputElement>(null);
  const [deletedOpen,setDeletedOpen]=useState(false);
  const [streams, setStreams] = useState<Stream[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [draft, setDraft] = useState<StreamDraft>(emptyDraft);
  const [nameEdited, setNameEdited] = useState(false);
  const [indices, setIndices] = useState<StreamIndex[]>([]);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [indexLoading, setIndexLoading] = useState(false);
  const [error, setError] = useState("");
  const [indexError, setIndexError] = useState("");
  const [notice, setNotice] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [message, setMessage] = useState("");
  const [source, setSource] = useState("manual");
  const [sample, setSample] = useState('{\n  "source": "nginx",\n  "message": "GET /api/health",\n  "level": "info"\n}');
  const [preview, setPreview] = useState<RoutingPreview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [previewBusy, setPreviewBusy] = useState(false);
  const previewRequest = useRef(0);
  const availableFields = useMemo(() => sampleFields(sample), [sample]);
  const retry = useRef<{ id: string; message: string; source: string } | null>(null);
  const alive = useRef(true);

  useEffect(() => {
    alive.current = true;
    const controller = new AbortController();
    fetchStreams(controller.signal).then(result => {
      if (controller.signal.aborted) return;
      setStreams(result.streams);
      if (result.streams[0]) { setSelected(result.streams[0].name); setDraft(editDraft(result.streams[0])); }
    }).catch(error => { if (!controller.signal.aborted) setError(errorText(error)); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => { alive.current = false; controller.abort(); };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setIndices([]); setIndexError("");
    if (!selected) return () => controller.abort();
    setIndexLoading(true);
    fetchStreamIndices(selected, controller.signal).then(result => {
      if (!controller.signal.aborted) setIndices(result.indices);
    }).catch(error => { if (!controller.signal.aborted) setIndexError(errorText(error)); })
      .finally(() => { if (!controller.signal.aborted) setIndexLoading(false); });
    return () => controller.abort();
  }, [selected, refresh]);

  useEffect(() => { previewRequest.current++; setPreview(null); setPreviewError(""); setPreviewBusy(false); }, [draft, sample]);

  function choose(stream?: Stream) {
    if (busy) return;
    setSelected(stream?.name ?? null); setDraft(stream ? editDraft(stream) : emptyDraft());
    setNameEdited(false);
    setError(""); setNotice(""); setMessage(""); retry.current = null;
  }
  async function copyDeleteIdentifier() {
    if(!selected)return;
    try { await navigator.clipboard.writeText(selected);setCopyStatus("copied"); }
    catch { copySource.current?.focus();copySource.current?.select();setCopyStatus("manual"); }
  }
  async function showDeleted() {
    setBusy(true);setError("");try{const result=await fetchDeletedStreams();setDeletedStreams(result.streams);setDeletedOpen(true);}catch(e){setError(errorText(e));}finally{setBusy(false);}
  }
  async function changeDeletion(name:string,remove:boolean) {
    setBusy(true);setError("");setNotice("");
    try {
      if(remove) await deleteStream(name);else await restoreStream(name);
      const result=await fetchStreams();if(!alive.current)return;
      setStreams(result.streams);onSaved(result.streams);setDeleteOpen(false);setDeleteName("");
      const next=result.streams.find(s=>s.name===name)??result.streams[0];
      setSelected(next?.name??null);setDraft(next?editDraft(next):emptyDraft());
      setMessage("");retry.current=null;setRefresh(v=>v+1);
      if(deletedOpen){const hidden=await fetchDeletedStreams();if(alive.current)setDeletedStreams(hidden.streams);}
      if(alive.current)setNotice(remove?"Поток удалён. Его можно восстановить в списке удалённых потоков.":"Поток восстановлен. Приём сообщений снова доступен.");
    }catch(e){if(alive.current)setError(errorText(e));}finally{if(alive.current)setBusy(false);}
  }
  function updateRule(index: number, patch: Partial<RoutingRule>) {
    setDraft(current => ({ ...current, routing: { ...current.routing, rules: current.routing.rules.map((rule, i) => i === index ? { ...rule, ...patch } : rule) } }));
  }
  async function testRouting() {
    const request = ++previewRequest.current;
    setPreview(null); setPreviewError("");
    if (!validName.test(draft.name)) { setPreviewError("Укажите корректный технический идентификатор потока. " + nameHelp); return; }
    let parsed: unknown;
    try { parsed = JSON.parse(sample); } catch { setPreviewError("Введите корректное JSON-сообщение."); return; }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) { setPreviewError("Введите одно сообщение в виде JSON-объекта."); return; }
    setPreviewBusy(true);
    try {
      const result = await previewRouting(sample, draft);
      if (alive.current && previewRequest.current === request) setPreview(result);
    } catch (error) { if (alive.current && previewRequest.current === request) setPreviewError(errorText(error)); }
    finally { if (alive.current && previewRequest.current === request) setPreviewBusy(false); }
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    if (!validName.test(draft.name)) { setError("Укажите корректный технический идентификатор потока. " + nameHelp); return; }
    setBusy(true); setError(""); setNotice("");
    try {
      const result = await saveStream(draft, !selected);
      const next = [...streams.filter(stream => stream.name !== result.stream.name), result.stream].sort((a, b) => a.name.localeCompare(b.name));
      if (!alive.current) return;
      setStreams(next); onSaved(next); setSelected(result.stream.name); setDraft(editDraft(result.stream));
      setNotice("Настройки потока сохранены и применяются к новым сообщениям."); setRefresh(value => value + 1);
    } catch (error) { if (alive.current) setError(errorText(error)); }
    finally { if (alive.current) setBusy(false); }
  }
  async function rotate() {
    if (!selected) return;
    setBusy(true); setError(""); setNotice("");
    try {
      const result = await rotateStream(selected);
      if (alive.current) { setNotice(result.rotated ? "Текущий индекс закрыт. Следующее сообщение создаст новый индекс." : "Активного индекса пока нет. Следующее сообщение создаст его."); setRefresh(value => value + 1); }
    } catch (error) { if (alive.current) setError(errorText(error)); }
    finally { if (alive.current) setBusy(false); }
  }
  async function send(event: FormEvent) {
    event.preventDefault(); if (!selected) return;
    setBusy(true); setError(""); setNotice("");
    if (!retry.current || retry.current.message !== message || retry.current.source !== source) retry.current = { id: crypto.randomUUID(), message, source };
    try {
      await sendStreamMessage(selected, retry.current);
      if (alive.current) { setMessage(""); retry.current = null; setNotice("Сообщение принято и появится в поиске после индексации."); setRefresh(value => value + 1); }
    } catch (error) { if (alive.current) setError(errorText(error)); }
    finally { if (alive.current) setBusy(false); }
  }

  if (loading) return <div className="empty" role="status">Загрузка настроек потоков…</div>;
  return <section className="streams-settings" aria-label="Настройки потоков">
    <div className="settings-heading"><div><h2>Потоки и хранение</h2><p>Настройте маршрутизацию входящих сообщений и хранение каждого потока.</p></div><button type="button" disabled={busy} onClick={() => choose()}>Создать поток</button></div>
    <div className="stream-delete-tools"><button type="button" disabled={busy} onClick={showDeleted}>Удалённые потоки</button>{selected && selected!=="all" && <button className="danger" type="button" disabled={busy} onClick={()=>{setError("");setDeleteName("");setCopyStatus("");setDeleteOpen(true);}}>Удалить поток</button>}{selected==="all" && <small className="settings-help">Поток по умолчанию нельзя удалить.</small>}</div>
    {deletedOpen && <section className="settings-card" aria-label="Удалённые потоки"><div className="settings-heading"><h3>Удалённые потоки</h3><button type="button" onClick={()=>setDeletedOpen(false)}>Закрыть список</button></div>{deletedStreams.length===0?<p>Нет удалённых потоков.</p>:deletedStreams.map(stream=><div className="deleted-stream-row" key={stream.id}><span>{stream.title} <small>{stream.name}</small></span><button type="button" disabled={busy} onClick={()=>changeDeletion(stream.name,false)}>Восстановить {stream.title}</button></div>)}</section>}
    {deleteOpen && <div className="modal-backdrop"><section className="settings-card stream-delete-dialog" role="dialog" aria-modal="true" aria-labelledby="delete-stream-title" onKeyDown={event=>{
 if(event.key==="Escape"&&!busy){event.preventDefault();setDeleteOpen(false);}
 if(event.key==="Tab"){const controls=event.currentTarget.querySelectorAll<HTMLElement>("button:not(:disabled),input:not(:disabled)");const first=controls[0],last=controls[controls.length-1];if(event.shiftKey&&document.activeElement===first){event.preventDefault();last?.focus();}else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first?.focus();}}
 }}><h3 id="delete-stream-title">Удалить поток «{streams.find(s=>s.name===selected)?.title}»?</h3><p>Поток исчезнет из поиска, списка и автоматической маршрутизации. Новые сообщения с его идентификатором не будут приниматься. Сообщения сохранятся до истечения срока хранения. Поток можно восстановить вместе с прежними назначениями доступа.</p>{error&&<p role="alert" className="request-error">{error}</p>}<div className="delete-copy-source"><label>Идентификатор для копирования<input ref={copySource} readOnly value={selected??""} onFocus={event=>event.currentTarget.select()} /></label><button type="button" disabled={busy} onClick={copyDeleteIdentifier}>{copyStatus==="copied"?"Скопировано":"Скопировать"}</button></div>
 <p className="settings-help" role="status">{copyStatus==="manual"?"Идентификатор выделен. Скопируйте его через Ctrl+C / ⌘C и вставьте в поле ниже.":"Скопируйте идентификатор и вставьте его в поле ниже для подтверждения удаления."}</p>
 <label>Введите идентификатор потока<input autoFocus value={deleteName} onChange={e=>setDeleteName(e.target.value)} placeholder={selected??""} /></label><div className="stream-delete-tools"><button type="button" disabled={busy} onClick={()=>setDeleteOpen(false)}>Отмена</button><button className="danger" type="button" disabled={busy||deleteName!==selected} onClick={()=>selected&&changeDeletion(selected,true)}>Подтвердить удаление</button></div></section></div>}
    {error && !deleteOpen && <div className="request-error" role="alert">{error}</div>}
    {notice && <div className="settings-success" role="status">{notice}</div>}
    <section className="routing-overview" aria-label="Сохранённые правила маршрутизации">
      <h3>Как сообщения попадают в потоки</h3>
      <p className="settings-help">Правила выполняются в порядке приоритета. Сообщение получает первый подходящий поток. Если поток указан в сообщении явно, оно отправляется напрямую в него.</p>
      <div className="routing-map">{[...streams.filter(stream => stream.name !== "all").sort((a, b) => (a.routing?.priority ?? 100) - (b.routing?.priority ?? 100) || a.name.localeCompare(b.name)), ...streams.filter(stream => stream.name === "all")].map(stream => <div key={stream.name}>
        <strong>{streamTitle(stream)}</strong><code>{stream.name}</code>
        {stream.name === "all" ? <span>По умолчанию: ни одно правило не совпало</span> : stream.routing?.enabled ? <><span>{stream.routing.rules.map(ruleText).join(stream.routing.match === "all" ? " AND " : " OR ")}</span><small>Приоритет {stream.routing.priority}</small></> : <span>Правила отключены · только явное указание потока</span>}
      </div>)}</div>
    </section>
    <div className="settings-layout"><aside className="stream-list" aria-label="Настроенные потоки">
      {streams.map(stream => <button type="button" key={stream.name} disabled={busy} aria-pressed={selected === stream.name} onClick={() => choose(stream)}><strong>{streamTitle(stream)}</strong><span>{stream.name}</span></button>)}
      {streams.length === 0 && <p>Потоков пока нет. Создайте первый поток.</p>}
    </aside><div className="stream-editor">
      <form onSubmit={save} aria-label="Редактирование потока">
        <fieldset disabled={busy}><legend>{selected ? "Параметры потока" : "Новый поток"}</legend>
          <div className="settings-grid"><label>Название<input aria-label="Название" required maxLength={160} value={draft.title} onChange={event => {
            const title = event.target.value;
            setDraft({ ...draft, title, name: !selected && !nameEdited ? suggestedName(title) : draft.name }); setError("");
          }} placeholder="Платежи" /><small className="settings-help">Отображаемое название. Можно использовать пробелы, заглавные буквы и любой язык.</small></label>
          <label>Идентификатор<input aria-label="Идентификатор" required pattern={"[a-z0-9][a-z0-9_\\-]{0,62}"} maxLength={63} disabled={Boolean(selected)} value={draft.name} aria-describedby="stream-name-help" aria-invalid={Boolean(draft.name && !validName.test(draft.name))} onChange={event => {
            const name = event.target.value.toLowerCase().replace(/\s+/g, "-");
            setNameEdited(Boolean(name)); setDraft({ ...draft, name }); setError("");
          }} placeholder="payments" /><small className="settings-help" id="stream-name-help">{nameHelp} Пробелы заменяются дефисами, заглавные буквы — строчными.</small></label></div>
          <label>Описание<textarea maxLength={2000} rows={2} value={draft.description} onChange={event => setDraft({ ...draft, description: event.target.value })} /></label>
        </fieldset>
        <fieldset disabled={busy}><legend>Автоматическая маршрутизация</legend>
          {draft.name === "all" ? <p className="settings-help">Это поток по умолчанию: он получает сообщения, не подошедшие под включённые правила. Пункт «Все сообщения» в поиске объединяет все доступные потоки.</p> : <>
          <label className="check-label"><input type="checkbox" disabled={draft.name === "all"} checked={draft.routing.enabled} onChange={event => setDraft({ ...draft, routing: { ...draft.routing, enabled: event.target.checked } })} />Включить правила маршрутизации</label>
          <p className="settings-help">Явно указанный поток имеет преимущество. Иначе выбирается первый подходящий поток: меньший номер приоритета проверяется раньше. Сообщения без совпадений попадают в поток по умолчанию.</p>
          <div className="settings-grid"><label>Приоритет<input type="number" min="0" max="1000" required value={draft.routing.priority} onChange={event => setDraft({ ...draft, routing: { ...draft.routing, priority: Number(event.target.value) } })} /></label>
          <label>Совпадение<select value={draft.routing.match} onChange={event => setDraft({ ...draft, routing: { ...draft.routing, match: event.target.value as "all" | "any" } })}><option value="all">Все условия</option><option value="any">Любое условие</option></select></label></div>
          {draft.routing.rules.map((rule, index) => <div className="routing-rule" key={index}>
            <label>Поле {index + 1}<input required maxLength={1024} aria-label={`Условие ${index + 1} поле`} list="routing-fields" value={rule.field} onChange={event => updateRule(index, { field: event.target.value })} placeholder="service, event.code, items[0].status" /></label>
            <label>Оператор<select aria-label={`Условие ${index + 1} оператор`} value={rule.operator} onChange={event => updateRule(index, { operator: event.target.value as RoutingRule["operator"] })}><option value="equals">Равно</option><option value="contains">Содержит</option><option value="starts_with">Начинается с</option><option value="exists">Существует</option><option value="not_exists">Отсутствует</option></select></label>
            <label>Значение<input maxLength={4096} disabled={rule.operator === "exists" || rule.operator === "not_exists"} aria-label={`Условие ${index + 1} значение`} value={rule.value} onChange={event => updateRule(index, { value: event.target.value })} /></label>
            <button type="button" aria-label={`Удалить условие ${index + 1}`} onClick={() => setDraft({ ...draft, routing: { ...draft.routing, rules: draft.routing.rules.filter((_, i) => i !== index) } })}>Удалить</button>
          </div>)}
          <datalist id="routing-fields">{[...new Set(["source", "host", "level", "facility", "message", ...availableFields.map(field => field.path)])].map(field => <option key={field} value={field} />)}</datalist>
          <p className="settings-help">Можно указать путь к полю JSON, индекс массива или JSON Pointer. Для массива подходит любой совпавший элемент. Вставьте событие ниже, чтобы увидеть доступные поля.</p>
          <button type="button" disabled={draft.routing.rules.length >= 20 || draft.name === "all"} onClick={() => setDraft({ ...draft, routing: { ...draft.routing, rules: [...draft.routing.rules, { field: "source", operator: "equals", value: "" }] } })}>Добавить условие</button>
          </>}
          <div className="routing-test">
            <h3>Проверить маршрутизацию</h3><p className="settings-help">Вставьте JSON-сообщение для проверки настроек перед сохранением. Пример не сохраняется. Введённые настройки проверяются вместе с остальными сохранёнными потоками.</p>
            <label>Пример JSON-сообщения<textarea aria-label="Пример JSON-сообщения" rows={6} spellCheck={false} value={sample} onChange={event => setSample(event.target.value)} /></label>
            {draft.name !== "all" && availableFields.length > 0 && <div className="sample-fields" aria-label="Поля из примера">
              <p className="settings-help">Добавить условие для поля из этого JSON:</p>
              {availableFields.slice(0, 40).map(field => <button type="button" key={field.path} disabled={draft.routing.rules.length >= 20} aria-label={`Использовать поле ${field.path}`} onClick={() => setDraft({ ...draft, routing: { ...draft.routing, enabled: true, rules: [...draft.routing.rules, { field: field.path, operator: "exists", value: "" }] } })}>{field.path}</button>)}
            </div>}
            <button type="button" disabled={previewBusy || !draft.name || !draft.title} onClick={testRouting}>{previewBusy ? "Проверка…" : "Проверить маршрутизацию"}</button>
            {previewError && <div className="request-error" role="alert">{previewError}</div>}
            {preview && <div className="routing-result" role="status">
              <strong>Поток назначения: {streams.find(stream => stream.name === preview.stream)?.title ?? (draft.name === preview.stream ? draft.title : preview.stream)} <code>({preview.stream})</code></strong>
              <p>{preview.reason === "explicit" ? "Поток указан в сообщении явно. Автоматические правила не применяются." : preview.reason === "fallback" ? "Ни одно включённое правило не подошло. Сообщение попадает в поток по умолчанию." : "Этот поток первым подошёл по порядку приоритетов."}</p>
              {preview.candidates.map(candidate => <div className="routing-candidate" key={candidate.name}>
                <strong>{candidate.name}: {candidate.selected ? "выбран" : !candidate.enabled ? "отключён" : candidate.matched ? "совпало, но приоритет ниже" : "не совпало"}</strong>
                {candidate.rules.map((rule, index) => <div key={index}><span className={rule.matched ? "rule-pass" : "rule-fail"}>{rule.matched ? "Совпало" : "Не совпало"}</span> · {ruleText(rule)} · фактическое значение: <code>{rule.exists ? JSON.stringify(rule.actual) : "отсутствует"}</code></div>)}
              </div>)}
            </div>}
          </div>
        </fieldset>
        <fieldset disabled={busy}><legend>Ротация и срок хранения</legend>
          <p className="settings-help">Индекс меняется при достижении любого включённого порога. Значение 0 отключает порог. Возраст и число сообщений проверяются при приёме, размер — в фоновом режиме.</p>
          <div className="settings-grid">
            <label>Максимальный возраст (часы)<input type="number" min="0" max="87600" required value={draft.storage.max_age_hours} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, max_age_hours: Number(event.target.value) } })} /></label>
            <label>Максимальный размер (МиБ)<input type="number" min="0" max="1048576" required value={draft.storage.max_size_mb} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, max_size_mb: Number(event.target.value) } })} /></label>
            <label>Максимальное число сообщений<input type="number" min="0" max="1000000000" required value={draft.storage.max_documents} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, max_documents: Number(event.target.value) } })} /></label>
            <label>Хранить после ротации (дни)<input type="number" min="0" max="3650" required value={draft.storage.retention_days} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, retention_days: Number(event.target.value) } })} /></label>
          </div>
          <p className="settings-help">Срок хранения 0 означает бессрочное хранение. При положительном значении закрытые индексы удаляются через указанное число дней. Тело сообщения хранится в Redis до подтверждения OpenSearch, затем остаётся только идентификатор. Неопубликованные сообщения защищены.</p>
          <p className="settings-help">Эти правила действуют для новых индексов потоков. Ранее сохранённые логи доступны в поиске и хранятся отдельно.</p>
        </fieldset>
        <button className="primary" type="submit" disabled={busy}>{busy ? "Выполняется…" : selected ? "Сохранить настройки" : "Создать поток с настройками"}</button>
      </form>
      {selected && <>
        <section className="settings-card" aria-label="Индексы потока"><div className="settings-heading"><h3>Индексы</h3><div><button type="button" disabled={busy || indexLoading} onClick={() => setRefresh(value => value + 1)}>Обновить индексы</button><button type="button" disabled={busy} onClick={rotate}>Выполнить ротацию</button></div></div>
          {indexError && <div className="request-error" role="alert">{indexError}</div>}
          {indexLoading ? <p role="status">Загрузка индексов…</p> : indices.length === 0 ? <p className="settings-help">Индексов пока нет. Первый индекс появится при получении сообщения.</p> : <div className="table-wrap"><table><thead><tr><th>Индекс</th><th>Состояние</th><th>Сообщения</th><th>Размер</th></tr></thead><tbody>{indices.map(index => <tr key={index.name}><td className="index-name">{index.name}<small>{new Date(index.created_at).toLocaleString("ru-RU")}</small></td><td>{index.deleted_at ? "Удалён" : index.sealed_at ? "Закрыт" : "Активен"}</td><td>{index.documents ?? `${index.accepted_count} принято`}</td><td>{index.size_bytes === undefined ? "—" : `${(index.size_bytes / 1048576).toFixed(2)} МиБ`}{index.stats_error && <small>{apiError(index.stats_error, 503).message}</small>}</td></tr>)}</tbody></table></div>}
        </section>
        <section className="settings-card"><h3>Отправка в этот поток</h3><p className="settings-help">Отправьте JSON на <code>/api/streams/{selected}/ingest</code>или добавьте <code>"stream": "{selected}"</code> в сообщение, отправленное на общий адрес приёма.</p>
          <form onSubmit={send} aria-label="Отправить сообщение"><fieldset disabled={busy}><label>Сообщение<textarea required rows={3} value={message} onChange={event => setMessage(event.target.value)} /></label><label>Источник<input required maxLength={200} value={source} onChange={event => setSource(event.target.value)} /></label><button type="submit" disabled={!message.trim()}>Отправить сообщение</button></fieldset></form>
        </section>
      </>}
    </div></div>
  </section>;
}
