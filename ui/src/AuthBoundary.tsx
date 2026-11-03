import { FormEvent, useEffect, useState } from "react";
import App from "./App";
import { authStatus, fetchIdentity, login, logout, type Identity } from "./api";

export default function AuthBoundary() {
  const [loading, setLoading] = useState(true);
  const [enabled, setEnabled] = useState(true);
  const [identity, setIdentity] = useState<Identity | null>(null);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [source, setSource] = useState("ldap");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function refresh() {
    setLoading(true); setError("");
    try {
      const status = await authStatus();
      if (typeof status.enabled !== "boolean") throw new Error("Не удалось загрузить настройки входа.");
      setEnabled(status.enabled);
      if (status.enabled) {
        try { setIdentity(await fetchIdentity()); }
        catch (e) { setIdentity(null); if (!(e instanceof Error) || !("status" in e && e.status === 401) && !/Sign in|expired session|Войдите|сессия истекла/i.test(e.message)) setError(e instanceof Error ? e.message : "Вход временно недоступен."); }
      }
    } catch (e) { setError(e instanceof Error ? e.message : "Не удалось подключиться к Logister."); }
    finally { setLoading(false); }
  }
  useEffect(() => { void refresh(); }, []);
  useEffect(() => {
    const expired = () => setIdentity(null);
    window.addEventListener("logister:session-expired", expired);
    return () => window.removeEventListener("logister:session-expired", expired);
  }, []);
  // Refresh permissions without preserving data in a view the user can no
  // longer open. The API independently refreshes LDAP membership per request.
  useEffect(() => {
    if (!identity) return;
    let active = true;
    const timer = window.setInterval(() => { void fetchIdentity().then(next => { if (active) setIdentity(next); }).catch(e => { if (active) { setIdentity(null); setError(e instanceof Error ? e.message : "Не удалось обновить сессию."); } }); }, 30000);
    return () => { active = false; window.clearInterval(timer); };
  }, [Boolean(identity)]);

  async function submit(e: FormEvent) {
    e.preventDefault(); setBusy(true); setError("");
    try { await login(username, password, source); setPassword(""); setIdentity(await fetchIdentity()); }
    catch (e) { setError(e instanceof Error ? e.message : "Не удалось войти."); }
    finally { setBusy(false); }
  }
  async function signOut() {
    try { await logout(); setIdentity(null); setPassword(""); setError(""); }
    catch (e) { setError(e instanceof Error ? e.message : "Не удалось выйти."); }
  }
  if (loading) return <main className="auth-shell"><p>Подключение к Logister…</p></main>;
  if (!enabled) return <App />;
  if (identity) {
    if (identity.permissions.includes("messages:search")) return <App key={identity.user_id + identity.access_version + identity.permissions.join(",")} identity={identity} sessionError={error} onLogout={() => void signOut()} />;
    return <main className="auth-shell"><section className="auth-card"><p className="brand-name">Logister</p><h1>{identity.username}</h1><p>{identity.permissions.includes("ingest:write") ? "Эта учётная запись может отправлять сообщения через API. Доступ к поиску и настройкам не предоставлен." : "Вход выполнен, но права доступа не назначены. Обратитесь к администратору."}</p>{error && <p role="alert">{error}</p>}<button onClick={() => void signOut()}>Выйти</button></section></main>;
  }
  return <main className="auth-shell"><form className="auth-card" onSubmit={e => void submit(e)}><p className="brand-name">Logister</p><h1>Войти</h1><p>Войдите с учётной записью каталога.</p>
    <label>Тип учётной записи<select value={source} onChange={e => { setSource(e.target.value); setUsername(e.target.value === "local" ? "local-admin" : ""); }}><option value="ldap">Учётная запись LDAP</option><option value="local">Локальный аварийный администратор</option></select></label>
    <label>Логин<input autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} required /></label>
    <label>Пароль<input type="password" autoComplete="current-password" value={password} onChange={e => setPassword(e.target.value)} required /></label>
    {error && <p role="alert">{error}</p>}
    <button type="submit" disabled={busy}>{busy ? "Вход…" : "Войти"}</button>
    <button type="button" className="subtle-button" onClick={() => void refresh()} disabled={busy}>Подключиться снова</button>
  </form></main>;
}
