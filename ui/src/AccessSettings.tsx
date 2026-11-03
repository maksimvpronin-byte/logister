import { streamDisplayTitle } from "./locale";
import { FormEvent, useEffect, useRef, useState } from "react";
import { fetchDirectory, fetchGrants, fetchStreams, saveGrants, type DirectoryEntry, type Grant, type GrantCatalog, type Stream } from "./api";

const roles: { code: Grant["role"]; title: string; description: string }[] = [
  { code: "operator", title: "Оператор", description: "Поиск и чтение сообщений" },
  { code: "service", title: "Сервисная роль", description: "Отправка сообщений через API с авторизацией" },
  { code: "administrator", title: "Администратор", description: "Полный доступ, настройки и управление пользователями" },
];
const principalKey = (kind: string, id: string) => `${kind}:${id}`;
const sameGrant = (a: Grant, b: Grant) => a.principal_kind === b.principal_kind && a.principal_id === b.principal_id && a.role === b.role && a.scope === b.scope && (a.stream_id ?? "") === (b.stream_id ?? "");
type Editor = { mode: "create" | "edit" | "delete"; index: number; grant: Grant };

export default function AccessSettings() {
  const [catalog, setCatalog] = useState<GrantCatalog | null>(null);
  const [entries, setEntries] = useState<DirectoryEntry[]>([]);
  const [results, setResults] = useState<DirectoryEntry[]>([]);
  const [streams, setStreams] = useState<Stream[]>([]);
  const [filter, setFilter] = useState("");
  const [kind, setKind] = useState("all");
  const [query, setQuery] = useState("");
  const [editor, setEditor] = useState<Editor | null>(null);
  const [error, setError] = useState("");
  const [editorError, setEditorError] = useState("");
  const [directoryError, setDirectoryError] = useState("");
  const [notice, setNotice] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [searching, setSearching] = useState(false);
  const dialog = useRef<HTMLDialogElement>(null);

  function remember(list: DirectoryEntry[]) {
    setEntries(previous => [...new Map([...previous, ...list].map(entry => [principalKey(entry.kind, entry.id), entry])).values()]);
    setResults(list);
  }
  async function load() {
    setLoading(true); setError("");
    try {
      const [grants, list] = await Promise.all([fetchGrants(), fetchStreams()]);
      setCatalog(grants); setStreams(list.streams);
    } catch (e) { setError(e instanceof Error ? e.message : "Не удалось загрузить назначения доступа."); }
    finally { setLoading(false); }
    try { remember((await fetchDirectory()).entries); setDirectoryError(""); }
    catch { setDirectoryError("Каталог недоступен. Существующие назначения можно редактировать. Повторите поиск после восстановления соединения."); }
  }
  useEffect(() => { void load(); }, []);
  useEffect(() => {
    if (editor && dialog.current && !dialog.current.open) dialog.current.showModal();
  }, [editor]);

  function name(grant: Grant) {
    if (grant.principal_id === "local-admin") return "Локальный аварийный администратор";
    return entries.find(e => e.id === grant.principal_id && e.kind === grant.principal_kind)?.name ?? grant.principal_id;
  }
  function scopeName(grant: Grant) {
    if (grant.scope === "system") return "Все потоки и вся система";
    const stream = streams.find(s => s.id === grant.stream_id);
    return stream ? streamDisplayTitle(stream) : grant.stream_id;
  }

  function open(mode: Editor["mode"], index = -1) {
    setEditorError(""); setQuery("");
    setEditor({ mode, index, grant: index >= 0 && catalog ? { ...catalog.grants[index] } : { principal_kind: "group", principal_id: "", role: "operator", scope: "stream", stream_id: streams.find(s => s.name !== "all")?.id ?? streams[0]?.id ?? "" } });
  }
  function close() { if (!saving) { dialog.current?.close(); setEditor(null); setEditorError(""); } }
  function update(change: Partial<Grant>) { setEditor(previous => previous && { ...previous, grant: { ...previous.grant, ...change } }); setEditorError(""); }
  async function searchDirectory(e: FormEvent) {
    e.preventDefault(); setSearching(true); setDirectoryError("");
    try { remember((await fetchDirectory(query)).entries); }
    catch { setDirectoryError("Не удалось выполнить поиск в каталоге. Проверьте соединение и повторите попытку."); }
    finally { setSearching(false); }
  }
  const grant = editor?.grant;
  const duplicate = Boolean(grant && catalog?.grants.some((other, i) => i !== editor.index && sameGrant(other, grant)));
  const changed = Boolean(editor && catalog && (editor.mode !== "edit" || !sameGrant(editor.grant, catalog.grants[editor.index])));
  const valid = Boolean(grant?.principal_id && (grant.scope === "system" || grant.stream_id));
  async function commit() {
    if (!catalog || !editor || saving) return;
    if (editor.grant.principal_id === "local-admin") return;
    if (editor.mode !== "delete" && (!valid || duplicate || !changed)) return;
    const { mode, index, grant: next } = editor;
    const grants = mode === "delete" ? catalog.grants.filter((_, i) => i !== index) : mode === "edit" ? catalog.grants.map((existing, i) => i === index ? next : existing) : [...catalog.grants, next];
    setSaving(true); setEditorError(""); setNotice("");
    try {
      await saveGrants({ revision: catalog.revision, grants });
      // The API atomically increments this revision; no second request is needed
      // to establish whether the write succeeded.
      setCatalog({ grants, revision: catalog.revision + 1 });
      setNotice(`${mode === "delete" ? "Доступ удалён" : mode === "edit" ? "Доступ изменён" : "Доступ предоставлен"} для ${name(next)}.`);
      dialog.current?.close(); setEditor(null);
    } catch (e) { setEditorError(e instanceof Error ? e.message : "Не удалось сохранить доступ. Введённые изменения сохранены в редакторе."); }
    finally { setSaving(false); }
  }
  const visible = catalog?.grants.map((g, index) => ({ grant: g, index })).filter(({ grant: g }) => (kind === "all" || g.principal_kind === kind) && `${name(g)} ${g.role} ${roles.find(r => r.code === g.role)?.title ?? ""} ${scopeName(g)}`.toLowerCase().includes(filter.toLowerCase())) ?? [];
  const choices = [...results];
  if (grant?.principal_id && !choices.some(e => e.id === grant.principal_id && e.kind === grant.principal_kind)) {
    const selected = entries.find(e => e.id === grant.principal_id && e.kind === grant.principal_kind);
    if (selected) choices.push(selected);
  }

  return <section className="access-settings" aria-label="Настройки доступа">
    <header className="access-heading"><div><h1>Пользователи и роли</h1><p>Управляйте доступом к чтению сообщений, отправке данных и администрированию Logister.</p></div><button className="access-primary" disabled={loading || !catalog} onClick={() => open("create")}>+ Предоставить доступ</button></header>
    {error && <p className="error" role="alert">{error}</p>}
    {notice && <p className="access-notice" role="status">{notice}</p>}
    {directoryError && !editor && <p className="access-warning" role="status">{directoryError}</p>}
    <div className="access-list-tools"><label>Поиск назначений<input type="search" placeholder="Пользователь, группа, роль или поток…" value={filter} onChange={e => setFilter(e.target.value)} /></label><div className="access-kind-filter" role="group" aria-label="Тип назначения">{[["all", "Все"], ["group", "Группы"], ["user", "Пользователи"]].map(([value, label]) => <button key={value} aria-pressed={kind === value} onClick={() => setKind(value)}>{label}</button>)}</div><button disabled={loading || saving} onClick={() => void load()}>Обновить</button></div>
    <div className="access-table-wrap"><table className="access-table"><thead><tr><th>Пользователь / группа</th><th>Роль</th><th>Область доступа</th><th>Действия</th></tr></thead><tbody>
      {visible.map(({ grant: g, index }) => <tr key={index}><td><strong>{name(g)}</strong><small>{g.principal_id === "local-admin" ? "Локальная учётная запись · аварийный доступ" : g.principal_kind === "group" ? "Группа LDAP · все участники" : "Пользователь LDAP · прямое назначение"}</small></td><td><span className={`access-role access-role-${g.role}`}>{roles.find(r => r.code === g.role)?.title ?? g.role}</span></td><td>{scopeName(g)}</td><td>{g.principal_id === "local-admin" ? <span className="access-protected" title="Аварийный доступ администратора обязателен и не может быть изменён.">Защищено</span> : <div className="access-row-actions"><button aria-label={`Изменить доступ: ${name(g)} → ${scopeName(g)}`} onClick={() => open("edit", index)}>Изменить</button><button className="access-remove" aria-label={`Удалить доступ: ${name(g)} → ${scopeName(g)}`} onClick={() => open("delete", index)}>Удалить</button></div>}</td></tr>)}
      {visible.length === 0 && <tr><td colSpan={4} className="access-empty">{loading ? "Загрузка назначений…" : "Назначения по вашему запросу не найдены."}</td></tr>}
    </tbody></table></div>
    <p className="access-footnote">{visible.length} назначений · Права пользователей и групп складываются. Изменение одного назначения не отменяет остальные.</p>

    {editor && grant && <dialog ref={dialog} className="access-dialog" aria-labelledby="access-editor-title" onCancel={e => { e.preventDefault(); close(); }}>
      <header className="access-dialog-heading"><div><p className="access-eyebrow">{editor.mode === "delete" ? "Отзыв доступа" : editor.mode === "edit" ? "Существующее назначение" : "Новое назначение"}</p><h2 id="access-editor-title">{editor.mode === "delete" ? "Удалить это назначение?" : editor.mode === "edit" ? "Изменить доступ" : "Предоставить доступ"}</h2></div><button aria-label="Закрыть редактор" disabled={saving} onClick={close}>×</button></header>
      <div className="access-dialog-body">
        {editor.mode === "create" ? <><form className="access-directory-search" onSubmit={e => void searchDirectory(e)}><label>Найти пользователя или группу<input autoFocus value={query} placeholder="Поиск в каталоге LDAP…" onChange={e => setQuery(e.target.value)} /></label><button disabled={searching || saving}>{searching ? "Поиск…" : "Найти в каталоге"}</button></form>{directoryError && <p className="access-warning" role="status">{directoryError}</p>}<label className="access-field">Пользователь или группа<select value={principalKey(grant.principal_kind, grant.principal_id)} disabled={saving} onChange={e => { const selected = choices.find(entry => principalKey(entry.kind, entry.id) === e.target.value); if (selected) update({ principal_id: selected.id, principal_kind: selected.kind }); }}><option value="group:">Выберите пользователя или группу</option>{choices.map(e => <option key={principalKey(e.kind, e.id)} value={principalKey(e.kind, e.id)}>{e.kind === "group" ? "Группа" : "Пользователь"}: {e.name}</option>)}</select></label></> : <div className="access-recipient"><strong>{name(grant)}</strong><small>{grant.principal_kind === "group" ? "Группа LDAP · права для всех участников" : "Пользователь LDAP · прямое назначение"}</small></div>}
        {editor.mode === "delete" ? <><p><strong>{roles.find(r => r.code === grant.role)?.title}</strong> доступ к <strong>{scopeName(grant)}</strong> будет удалён.</p><p className="access-footnote">Другие назначения пользователей и групп сохранятся. При необходимости вы сможете выдать этот доступ снова.</p></> : <>
          <fieldset className="access-role-options" disabled={saving}><legend>Роль</legend>{roles.map(role => <label key={role.code} className={grant.role === role.code ? "is-selected" : ""}><input type="radio" name="access-role" value={role.code} checked={grant.role === role.code} onChange={() => update({ role: role.code, ...(role.code === "administrator" ? { scope: "system", stream_id: undefined } : {}) })} /><span><strong>{role.title}</strong><small>{role.description}</small></span></label>)}</fieldset>
          <fieldset className="access-scope-options" disabled={saving || grant.role === "administrator"}><legend>Область доступа</legend><label><input type="radio" name="access-scope" checked={grant.scope === "stream"} onChange={() => update({ scope: "stream", stream_id: streams.find(s => s.name !== "all")?.id ?? streams[0]?.id ?? "" })} />Один поток</label><label><input type="radio" name="access-scope" checked={grant.scope === "system"} onChange={() => update({ scope: "system", stream_id: undefined })} />Все потоки / вся система</label></fieldset>
          {grant.scope === "stream" && <label className="access-field">Поток<select disabled={saving} value={grant.stream_id ?? ""} onChange={e => update({ stream_id: e.target.value })}>{!streams.some(s => s.id === grant.stream_id) && <option value={grant.stream_id ?? ""}>{grant.stream_id ? "Поток недоступен" : "Выберите поток"}</option>}{streams.map(s => <option key={s.id} value={s.id}>{streamDisplayTitle(s)}</option>)}</select></label>}
          {grant.role === "administrator" && <p className="access-footnote">Администраторы всегда имеют полный доступ ко всей системе.</p>}
          {grant.principal_id && <div className="access-preview"><span>Результат</span><p><strong>{name(grant)}</strong> → <strong>{roles.find(r => r.code === grant.role)?.title}</strong> → <strong>{scopeName(grant)}</strong></p><small>{grant.principal_kind === "group" ? "Этот доступ получит каждый участник группы." : "Это прямое назначение получит только выбранный пользователь."} Остальные назначения продолжат действовать.</small></div>}
          {duplicate && <p className="access-warning" role="status">Такое назначение уже существует. Измените существующую строку.</p>}
        </>}
        {editorError && <div className="error" role="alert"><p>{editorError}</p><button disabled={saving} onClick={() => { close(); void load(); }}>Отменить изменения и загрузить заново</button></div>}
      </div>
      <footer className="access-dialog-footer"><span>{editor.mode === "delete" ? "Применяется сразу после удаления" : "Применяется сразу после сохранения"}</span><button disabled={saving} onClick={close}>Отмена</button><button className={editor.mode === "delete" ? "access-danger" : "access-primary"} disabled={saving || (editor.mode !== "delete" && (!valid || duplicate || !changed))} onClick={() => void commit()}>{saving ? "Сохранение…" : editor.mode === "delete" ? "Удалить доступ" : editor.mode === "edit" ? "Сохранить изменения" : "Предоставить доступ"}</button></footer>
    </dialog>}
  </section>;
}
