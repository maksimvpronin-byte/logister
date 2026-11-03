import { streamDisplayTitle } from "./locale";
import { FormEvent, useEffect, useRef, useState } from "react";
import StreamsSettings from "./StreamsSettings";
import MessageDetails from "./MessageDetails";
import AccessSettings from "./AccessSettings";
import LDAPSettings from "./LDAPSettings";
import IngestIndicator from "./IngestIndicator";
import KafkaIndicator from "./KafkaIndicator";
import type { Identity } from "./api";
import { absoluteTimeRange, formatRangeTime, localDateTime } from "./timeRange";
import { appendFieldSearch } from "./fieldSearch";
import {
  fetchHealth,
  fetchStreams,
  searchLogs,
  closeSearchSession,
  type Health,
  type Message,
  type Stream,
} from "./api";

const RELATIVE_OPTIONS = [
  { value: "15m", label: "За последние 15 минут" },
  { value: "1h", label: "За последний час" },
  { value: "6h", label: "За последние 6 часов" },
  { value: "24h", label: "За последние сутки" },
  { value: "7d", label: "За последние 7 дней" },
];

const PAGE_SIZE = 100;
type View = "search" | "streams" | "access" | "ldap";
const viewPaths: Record<View, string> = { search: "#/messages", streams: "#/streams", access: "#/access", ldap: "#/ldap" };

function viewFromLocation(administrator: boolean, canManageAccess: boolean): View {
  if (window.location.hash === viewPaths.streams && administrator) return "streams";
  if (window.location.hash === viewPaths.access && canManageAccess) return "access";
  if (window.location.hash === viewPaths.ldap && canManageAccess) return "ldap";
  return "search";
}

function formatTime(value: string) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString("ru-RU");
}

export default function App({ identity, onLogout, sessionError }: { identity?: Identity; onLogout?: () => void; sessionError?: string }) {
  const administrator = !identity || identity.administrator;
  const canManageAccess = Boolean(identity?.administrator);
  const [currentView, setCurrentView] = useState<View>(() => viewFromLocation(administrator, canManageAccess));
  const view = (currentView === "access" || currentView === "ldap") && !canManageAccess || currentView === "streams" && !administrator ? "search" : currentView;

  useEffect(() => {
    function restoreView() {
      const next = viewFromLocation(administrator, canManageAccess);
      setCurrentView(next);
      if (window.location.hash !== viewPaths[next]) {
        const url = new URL(window.location.href);
        url.hash = viewPaths[next];
        window.history.replaceState(null, "", url);
      }
    }
    restoreView();
    window.addEventListener("hashchange", restoreView);
    return () => window.removeEventListener("hashchange", restoreView);
  }, [administrator, canManageAccess]);

  function setView(next: View) {
    setCurrentView(next);
    window.location.hash = viewPaths[next];
  }
  const [health, setHealth] = useState<Health | null>(null);
  const [healthLoading, setHealthLoading] = useState(true);
  const [healthError, setHealthError] = useState<string | null>(null);
  const [streamsError, setStreamsError] = useState<string | null>(null);
  const [streams, setStreams] = useState<Stream[]>([]);
  const [query, setQuery] = useState("*");
  const [relative, setRelative] = useState("24h");
  const [absoluteFrom, setAbsoluteFrom] = useState(() => localDateTime(new Date(Date.now() - 86400000)));
  const [absoluteTo, setAbsoluteTo] = useState(() => localDateTime(new Date()));
  const [rangeError, setRangeError] = useState("");
  const [appliedRange, setAppliedRange] = useState<{ from: string; to: string } | null>(null);
  const customRangeInitialized = useRef(false);
  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const [stream, setStream] = useState("all");
  const [messages, setMessages] = useState<Message[]>([]);
  const [total, setTotal] = useState(0);
  const [totalRelation, setTotalRelation] = useState<"eq" | "gte">("eq");
  const [offset, setOffset] = useState(0);
  const [nextCursor, setNextCursor] = useState<string | undefined>();
  const [previousCursor, setPreviousCursor] = useState<string | undefined>();
  const session = useRef("");
  const [tookMs, setTookMs] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const searchRequest = useRef(0);
  const searchController = useRef<AbortController | null>(null);

  async function runSearch(nextOffset = 0, paging = false) {
    const cursor = paging ? nextOffset > offset ? nextCursor : previousCursor : undefined;
    let timeParams: { relative: string } | { from: string; to: string } | undefined;
    if (!cursor) {
      try { timeParams = relative === "custom" ? absoluteTimeRange(absoluteFrom, absoluteTo) : { relative }; }
      catch (error) { setRangeError(error instanceof Error ? error.message : "Укажите корректный период."); return; }
    }
    setRangeError("");
    const requestId = ++searchRequest.current;
    searchController.current?.abort();
    const controller = new AbortController();
    searchController.current = controller;
    if (!paging && session.current) {
      void closeSearchSession(session.current).catch(() => {});
      session.current = "";
    }
    setLoading(true);
    setError(null);
    setMessages([]);
    setTotal(0);
    setTotalRelation("eq");
    setTookMs(0);
    setSelected(null);
    setAppliedRange(null);
    try {
      const result = await searchLogs(cursor
        ? { cursor, signal: controller.signal }
        : { q: query, ...timeParams, stream, size: PAGE_SIZE, offset: 0, signal: controller.signal });
      if (requestId !== searchRequest.current || controller.signal.aborted) {
        if (result.session_id && result.session_id !== session.current) void closeSearchSession(result.session_id).catch(() => {});
        return;
      }
      session.current = result.session_id ?? "";
      setMessages(result.messages);
      setTotal(result.total);
      setTotalRelation(result.total_relation ?? "eq");
      setOffset(result.offset ?? nextOffset);
      setNextCursor(result.next_cursor);
      setPreviousCursor(result.previous_cursor);
      setTookMs(result.took_ms);
      setAppliedRange({ from: result.from, to: result.to });
    } catch (e) {
      if (requestId !== searchRequest.current || controller.signal.aborted) return;
      setError(e instanceof Error ? e.message : "Не удалось выполнить поиск");
    } finally {
      if (requestId === searchRequest.current) setLoading(false);
    }
  }

  useEffect(() => {
    let active = true;
    let healthPending = false;
    let streamsPending = false;
    const controller = new AbortController();
    async function refreshHealth() {
      if (healthPending) return;
      healthPending = true;
      try {
        const result = await fetchHealth(controller.signal);
        if (active) { setHealth(result); setHealthError(null); }
      } catch (e) {
        if (active) {
          setHealth(null);
          setHealthError(e instanceof Error ? e.message : "Состояние сервисов недоступно");
        }
      } finally {
        healthPending = false;
        if (active) setHealthLoading(false);
      }
    }
    async function refreshStreams() {
      if (streamsPending) return;
      streamsPending = true;
      try {
        const result = await fetchStreams(controller.signal);
        if (active) { setStreams(result.streams); setStreamsError(null); }
      } catch (e) {
        if (active) setStreamsError(e instanceof Error ? e.message : "Не удалось загрузить потоки");
      } finally { streamsPending = false; }
    }
    void refreshHealth();
    void refreshStreams();
    void runSearch();
    const timer = window.setInterval(() => { void refreshHealth(); void refreshStreams(); }, 30000);
    return () => {
      active = false;
      controller.abort();
      window.clearInterval(timer);
      searchRequest.current++;
      searchController.current?.abort();
      if (session.current) void closeSearchSession(session.current).catch(() => {});
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    void runSearch();
  }

  function serviceStatus(up: boolean | undefined) {
    if (healthLoading) return "проверка";
    return up === undefined ? "неизвестно" : up ? "доступен" : "недоступен";
  }

  const canGoNext = messages.length > 0 && Boolean(nextCursor);

  const selectedMessage = messages.find((m) => m.id === selected) ?? null;

  return (
    <div className="app">
      <header className="top">
        <p className="brand-name">Logister</p>
        <nav className="section-nav" aria-label="Разделы приложения">
          <button type="button" aria-pressed={view === "search"} onClick={() => setView("search")}>Сообщения</button>
          {administrator && <button type="button" aria-pressed={view === "streams"} onClick={() => setView("streams")}>Потоки и хранение</button>}
          {identity?.administrator && <button type="button" aria-pressed={view === "access"} onClick={() => setView("access")}>Пользователи и роли</button>}
          {identity?.administrator && <button type="button" aria-pressed={view === "ldap"} onClick={() => setView("ldap")}>Настройки LDAP</button>}
        </nav>
        <div className="health">
          {identity && <><span>{identity.username}</span><button type="button" onClick={onLogout}>Выйти</button></>}
          <span className={`pill ${health ? health.postgres ? "ok" : "bad" : ""}`}>
            postgres {serviceStatus(health?.postgres)}
          </span>
          <span className={`pill ${health ? health.redis ? "ok" : "bad" : ""}`}>
            redis {serviceStatus(health?.redis)}
          </span>
          <span className={`pill ${health ? health.opensearch ? "ok" : "bad" : ""}`}>
            opensearch {serviceStatus(health?.opensearch)}
          </span>
        </div>
      </header>

      <IngestIndicator />
      {administrator && <KafkaIndicator />}

      {sessionError && <p className="error" role="alert">{sessionError}</p>}

      {view === "streams" && administrator && <StreamsSettings onSaved={setStreams} />}
      {view === "access" && identity?.administrator && <AccessSettings />}
      {view === "ldap" && identity?.administrator && <LDAPSettings />}
      <section hidden={view !== "search"} aria-label="Поиск сообщений">

      <form className="searchbar" onSubmit={onSubmit} noValidate>
        <input
          aria-label="Поисковый запрос"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Запрос, например level:error OR source:nginx"
          spellCheck={false}
        />
        <select
          value={relative}
          onChange={(e) => {
            const value = e.target.value;
            if (value === "custom" && !customRangeInitialized.current) {
              if (appliedRange) { setAbsoluteFrom(localDateTime(appliedRange.from)); setAbsoluteTo(localDateTime(appliedRange.to)); }
              customRangeInitialized.current = true;
            }
            setRelative(value); setRangeError("");
          }}
          aria-label="Период"
        >
          {RELATIVE_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
          <option value="custom">Произвольный период</option>
        </select>
        <select
          value={stream}
          onChange={(e) => setStream(e.target.value)}
          aria-label="Поток"
        >
          <option value="all">Все сообщения</option>
          {streams.map((s) => (
              <option key={s.id} value={s.name === "all" ? "@default" : s.name}>
                {streamDisplayTitle(s)}
              </option>
            ))}
        </select>
        <button type="submit" disabled={loading}>
          {loading ? "Поиск…" : "Найти"}
        </button>
        {relative === "custom" && <div className="absolute-range">
          <label>С<input type="text" aria-label="С" placeholder="ДД.ММ.ГГГГ ЧЧ:мм:сс" autoComplete="off" spellCheck={false} required value={absoluteFrom} aria-invalid={Boolean(rangeError)} aria-describedby="time-range-help" onChange={event => { setAbsoluteFrom(event.target.value); setRangeError(""); }} /></label>
          <label>По<input type="text" aria-label="По" placeholder="ДД.ММ.ГГГГ ЧЧ:мм:сс" autoComplete="off" spellCheck={false} required value={absoluteTo} aria-invalid={Boolean(rangeError)} aria-describedby="time-range-help" onChange={event => { setAbsoluteTo(event.target.value); setRangeError(""); }} /></label>
          <p id="time-range-help">Формат: ДД.ММ.ГГГГ ЧЧ:мм:сс (секунды необязательны). Часовой пояс: {timeZone}. Обе границы включены. Нажмите «Найти», чтобы применить.</p>
        </div>}
        {rangeError && <div className="request-error time-range-error" role="alert">{rangeError}</div>}
      </form>

      {appliedRange && <div className="applied-range" aria-label="Выбранный период">
        Выбранный период: <time dateTime={appliedRange.from}>{formatRangeTime(appliedRange.from)}</time>
        <span aria-hidden="true"> — </span><time dateTime={appliedRange.to}>{formatRangeTime(appliedRange.to)}</time>
      </div>}

      <div className="meta">
        <span>
          {total}{totalRelation === "gte" ? "+" : ""} найдено · {tookMs} мс
        </span>
      </div>

      {healthError ? <div className="request-error" role="alert">{healthError}</div> : null}
      {streamsError ? <div className="request-error" role="alert">{streamsError}</div> : null}
      {error ? <div className="request-error" role="alert">{error}</div> : null}

      <div className="results">
        <div className="result-list">
          {messages.length > 0 && !error ? (
            <nav className="pagination" aria-label="Страницы результатов поиска">
              <span>Показаны {offset + 1}–{offset + messages.length} из {total}{totalRelation === "gte" ? "+" : ""}</span>
              <div>
                <button type="button" aria-label="Предыдущая страница" disabled={loading || !previousCursor}
                  onClick={() => { void runSearch(Math.max(0, offset - PAGE_SIZE), true); }}>Назад</button>
                <button type="button" aria-label="Следующая страница" disabled={loading || !canGoNext}
                  onClick={() => { void runSearch(offset + PAGE_SIZE, true); }}>Далее</button>
              </div>
            </nav>
          ) : null}
          <div className="table-wrap" aria-busy={loading}>
            {loading ? <div className="empty" role="status">Поиск…</div> : error ? null : messages.length === 0 ? (
              <div className="empty">За выбранный период сообщений нет.</div>
            ) : (
              <table className="result-table">
                <thead>
                  <tr>
                    <th>Время</th>
                    <th>Уровень</th>
                    <th>Источник</th>
                    <th>Сообщение</th>
                  </tr>
                </thead>
                <tbody>
                  {messages.map((m) => (
                    <tr key={m.id} className="message-row" data-selected={selected === m.id} onClick={() => setSelected(selected === m.id ? null : m.id)}>
                      <td>{formatTime(m.timestamp)}</td>
                      <td>
                        <span className={`level ${m.level || "info"}`}>
                          {m.level || "info"}
                        </span>
                      </td>
                      <td>{m.source}</td>
                      <td className="msg">
                        <button className="message-toggle" type="button" aria-label={`${selected === m.id ? "Скрыть" : "Показать"} поля сообщения ${m.id}`} aria-expanded={selected === m.id} onClick={event => { event.stopPropagation(); setSelected(selected === m.id ? null : m.id); }}><span aria-hidden="true">{selected === m.id ? "▾" : "▸"}</span></button>
                        <span className="msg-text">{m.message}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
        <aside className="result-detail" aria-label="Выбранное сообщение">
          {selectedMessage ? (
            <MessageDetails message={selectedMessage} onClose={() => setSelected(null)} onSearch={(clause, exclude) => setQuery(current => appendFieldSearch(current, clause, exclude))} />
          ) : messages.length > 0 && !loading && !error ? (
            <p className="detail-empty">Выберите сообщение, чтобы посмотреть его поля.</p>
          ) : null}
        </aside>
      </div>
      </section>
    </div>
  );
}
