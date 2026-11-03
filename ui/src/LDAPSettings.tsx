import { FormEvent, useEffect, useState } from "react";
import { fetchLDAPSettings, saveLDAPSettings, testLDAPSettings, type LDAPCheck, type LDAPSettings as Settings, type LDAPSettingsDraft } from "./api";

export default function LDAPSettings() {
  const [saved, setSaved] = useState<Settings | null>(null);
  const [draft, setDraft] = useState<LDAPSettingsDraft | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<"test" | "save" | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [check, setCheck] = useState<LDAPCheck | null>(null);
  function accept(settings: Settings) {
    setSaved(settings);
    setDraft({ url: settings.url, bind_dn: settings.bind_dn, users_dn: settings.users_dn, groups_dn: settings.groups_dn, bind_password: "", revision: settings.revision });
  }
  async function load() {
    setLoading(true); setError(""); setNotice(""); setCheck(null);
    try { accept(await fetchLDAPSettings()); }
    catch (e) { setError(e instanceof Error ? e.message : "Не удалось загрузить настройки LDAP."); }
    finally { setLoading(false); }
  }
  useEffect(() => { void load(); }, []);
  function change(field: keyof LDAPSettingsDraft, value: string) {
    setDraft(previous => previous && { ...previous, [field]: value }); setCheck(null); setNotice(""); setError("");
  }
  const dirty = Boolean(saved && draft && (draft.bind_password || draft.url !== saved.url || draft.bind_dn !== saved.bind_dn || draft.users_dn !== saved.users_dn || draft.groups_dn !== saved.groups_dn));
  const valid = Boolean(draft?.url.trim() && draft.bind_dn.trim() && draft.users_dn.trim() && draft.groups_dn.trim() && (saved?.password_set || draft.bind_password));
  async function run(action: "test" | "save") {
    if (!draft || busy || !valid) return;
    setBusy(action); setError(""); setNotice(""); setCheck(null);
    try {
      if (action === "test") { setCheck(await testLDAPSettings(draft)); }
      else { accept(await saveLDAPSettings(draft)); setNotice("Настройки LDAP сохранены и уже действуют. Перезапуск не требуется."); }
    } catch (e) { setError(e instanceof Error ? e.message : "Не удалось подключиться к LDAP. Сохранённые настройки не изменены."); }
    finally { setBusy(null); }
  }
  function submit(e: FormEvent) { e.preventDefault(); void run("save"); }
  return <section className="ldap-settings" aria-label="Настройки LDAP">
    <header className="access-heading"><div><h1>Подключение к LDAP</h1><p>Настройте каталог для входа пользователей и назначения прав группам. Доступно только администраторам.</p></div></header>
    {error && <div className="error" role="alert"><p>{error}</p>{draft && <p>Сохранённые настройки продолжают действовать. Введённые изменения остаются в форме.</p>}</div>}
    {notice && <p className="access-notice" role="status">{notice}</p>}
    {loading ? <p>Загрузка настроек LDAP…</p> : draft && saved ? <form onSubmit={submit}>
      <fieldset disabled={Boolean(busy)} className="ldap-connection-card"><legend>Сервер и техническая учётная запись</legend>
        <label>Адрес сервера<input aria-label="Адрес сервера" required type="url" value={draft.url} placeholder="ldaps://directory.example.com:636" spellCheck={false} onChange={e => change("url", e.target.value)} /><small>Укажите ldap:// или ldaps://. Сервер должен быть доступен из Logister. При LDAPS проверяется сертификат сервера.</small></label>
        <label>Техническая учётная запись (Bind DN)<input aria-label="Техническая учётная запись (Bind DN)" required value={draft.bind_dn} placeholder="uid=ldap-reader,ou=services,dc=example,dc=com" spellCheck={false} onChange={e => change("bind_dn", e.target.value)} /><small>Полный LDAP-логин технической учётной записи с правом чтения пользователей и групп.</small></label>
        <label>Пароль технической учётной записи<input aria-label="Пароль технической учётной записи" type="password" autoComplete="new-password" value={draft.bind_password ?? ""} placeholder={saved.password_set ? "Пароль сохранён — оставьте пустым, чтобы не менять" : "Введите пароль технической учётной записи"} onChange={e => change("bind_password", e.target.value)} /><small>{saved.password_set ? "Пароль сохранён и не передаётся обратно в браузер. Заполните поле, только если хотите заменить его." : "Пароль шифруется перед сохранением."}</small></label>
      </fieldset>
      <fieldset disabled={Boolean(busy)} className="ldap-connection-card"><legend>Базы поиска</legend>
        <label>База поиска пользователей (Users DN)<input aria-label="База поиска пользователей (Users DN)" required value={draft.users_dn} placeholder="ou=people,dc=example,dc=com" spellCheck={false} onChange={e => change("users_dn", e.target.value)} /><small>Пользователи ищутся по атрибуту uid среди записей класса inetOrgPerson.</small></label>
        <label>База поиска групп (Groups DN)<input aria-label="База поиска групп (Groups DN)" required value={draft.groups_dn} placeholder="ou=groups,dc=example,dc=com" spellCheck={false} onChange={e => change("groups_dn", e.target.value)} /><small>Группы — записи groupOfNames с DN участников в member. У пользователей и групп должен быть entryUUID. Поддерживается прямое членство.</small></label>
      </fieldset>
      {check && <div className="access-notice" role="status"><strong>Подключение проверено</strong><p>Вход технической учётной записи успешен, обе базы поиска доступны для чтения.</p><p>{check.users_found ? "Найдены подходящие записи пользователей." : "В базе поиска пока нет пользователей."} {check.groups_found ? "Найдены подходящие записи групп." : "В базе поиска пока нет групп."}</p><small>Проверены введённые значения. Сохраните настройки, чтобы применить изменения.</small></div>}
      <div className="ldap-settings-actions"><button type="button" disabled={Boolean(busy) || !valid} onClick={() => void run("test")}>{busy === "test" ? "Проверка…" : "Проверить подключение"}</button><button className="access-primary" disabled={Boolean(busy) || !valid || !dirty} type="submit">{busy === "save" ? "Проверка и сохранение…" : "Сохранить настройки"}</button>{dirty && <span>Есть несохранённые изменения</span>}</div>
      <p className="access-footnote">Перед сохранением проверяется подключение. Назначения ролей сохраняются. Если каталог недоступен, можно войти под локальным аварийным администратором.</p>
      <button type="button" className="ldap-reload" disabled={Boolean(busy)} onClick={() => void load()}>{dirty ? "Отменить изменения и загрузить заново" : "Загрузить сохранённые настройки"}</button>
    </form> : <button disabled={loading} onClick={() => void load()}>Повторить</button>}
  </section>;
}
