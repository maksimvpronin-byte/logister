import { streamDisplayTitle } from "./locale";
import { FormEvent, useEffect, useRef, useState } from "react";
import StreamsSettings from "./StreamsSettings";
import MessageDetails from "./MessageDetails";
import AccessSettings from "./AccessSettings";
import LDAPSettings from "./LDAPSettings";
import Monitoring from "./Monitoring";
import IngestIndicator from "./IngestIndicator";
import KafkaIndicator from "./KafkaIndicator";
import Icon, { type IconName } from "./Icon";
import ThemePicker from "./ThemePicker";
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
  { value: "15m", label: "15 минут" },
  { value: "1h", label: "1 час" },
  { value: "6h", label: "6 часов" },
  { value: "24h", label: "24 часа" },
  { value: "7d", label: "7 дней" },
];

const PAGE_SIZE = 100;
const countFormat = new Intl.NumberFormat("ru-RU");
type View = "search" | "streams" | "access" | "monitoring";
type AccessTab = "ldap" | "grants";
const navigation: Record<View, { label: string; icon: IconName }> = {
  search: { label: "Сообщения", icon: "search" },
  streams: { label: "Потоки и хранение", icon: "streams" },
  monitoring: { label: "Мониторинг", icon: "activity" },
  access: { label: "Доступ", icon: "access" },
};
const hashes = { search: "#/messages", streams: "#/streams", grants: "#/access", ldap: "#/ldap", monitoring: "#/monitoring" };

function readLocation(administrator: boolean, canManageAccess: boolean): { view: View; accessTab: AccessTab } {
  const hash = window.location.hash;
  if (hash === hashes.streams && administrator) return { view: "streams", accessTab: "grants" };
  if (hash === hashes.monitoring && canManageAccess) return { view: "monitoring", accessTab: "grants" };
  if (hash === hashes.ldap && canManageAccess) return { view: "access", accessTab: "ldap" };
  if (hash === hashes.grants && canManageAccess) return { view: "access", accessTab: "grants" };
  return { view: "search", accessTab: "ldap" };
}

function hashFor(view: View, accessTab: AccessTab) {
  if (view === "access") return accessTab === "ldap" ? hashes.ldap : hashes.grants;
  return hashes[view];
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
  const initialLocation = readLocation(administrator, canManageAccess);
  const [currentView, setCurrentView] = useState<View>(initialLocation.view);
  const [accessTab, setAccessTab] = useState<AccessTab>(initialLocation.accessTab);
  const view = ((currentView === "access" || currentView === "monitoring") && !canManageAccess) || (currentView === "streams" && !administrator) ? "search" : currentView;

  useEffect(() => {
    if (window.scrollY > 0) window.scrollTo(0, 0);
  }, [view]);

  useEffect(() => {
    function restoreView() {
      const next = readLocation(administrator, canManageAccess);
      setCurrentView(next.view);
      setAccessTab(next.accessTab);
      const hash = hashFor(next.view, next.accessTab);
      if (window.location.hash !== hash) {
        const url = new URL(window.location.href);
        url.hash = hash;
        window.history.replaceState(null, "", url);
      }
    }
    restoreView();
    window.addEventListener("hashchange", restoreView);
    return () => window.removeEventListener("hashchange", restoreView);
  }, [administrator, canManageAccess]);

  function setView(next: View) {
    if (next === "access" && view === "access") return;
    const tab: AccessTab = next === "access" ? "ldap" : accessTab;
    setCurrentView(next);
    if (next === "access") setAccessTab("ldap");
    window.location.hash = hashFor(next, tab);
  }
  function openAccessTab(tab: AccessTab) {
    setCurrentView("access");
    setAccessTab(tab);
    window.location.hash = hashFor("access", tab);
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
  const [stream, setStream] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [total, setTotal] = useState(0);
  const [totalRelation, setTotalRelation] = useState<"eq" | "gte">("eq");
  const [offset, setOffset] = useState(0);
  const [nextCursor, setNextCursor] = useState<string | undefined>();
  const [previousCursor, setPreviousCursor] = useState<string | undefined>();
  const session = useRef("");
  const [tookMs, setTookMs] = useState(0);
  const [appliedQuery, setAppliedQuery] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const searchRequest = useRef(0);
  const searchController = useRef<AbortController | null>(null);

  async function runSearch(nextOffset = 0, paging = false, overrides: { query?: string; relative?: string; stream?: string; absoluteFrom?: string; absoluteTo?: string } = {}) {
    const nextQuery = overrides.query ?? query;
    const nextRelative = overrides.relative ?? relative;
    const nextStream = overrides.stream ?? stream;
    const nextFrom = overrides.absoluteFrom ?? absoluteFrom;
    const nextTo = overrides.absoluteTo ?? absoluteTo;
    const cursor = paging ? nextOffset > offset ? nextCursor : previousCursor : undefined;
    let timeParams: { relative: string } | { from: string; to: string } | undefined;
    if (!cursor) {
      try { timeParams = nextRelative === "custom" ? absoluteTimeRange(nextFrom, nextTo) : { relative: nextRelative }; }
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
        : { q: nextQuery, ...timeParams, stream: nextStream, size: PAGE_SIZE, offset: 0, signal: controller.signal });
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
      if (!paging) setAppliedQuery(nextQuery);
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
  const queryPending = appliedQuery !== null && query !== appliedQuery && !loading;
  const views: View[] = ["search"];
  if (administrator) views.push("streams");
  if (canManageAccess) views.push("monitoring", "access");
  const pages = (label: string) => messages.length > 0 && !error ? (
    <nav className={`pagination${label.startsWith("Конец") ? " pagination-bottom" : ""}`} aria-label={label}>
      <span>Показаны {countFormat.format(offset + 1)}–{countFormat.format(offset + messages.length)} из {countFormat.format(total)}{totalRelation === "gte" ? "+" : ""}</span>
      <div>
        <button type="button" aria-label="Предыдущая страница" disabled={loading || !previousCursor}
          onClick={() => { void runSearch(Math.max(0, offset - PAGE_SIZE), true); }}>Назад</button>
        <button type="button" aria-label="Следующая страница" disabled={loading || !canGoNext}
          onClick={() => { void runSearch(offset + PAGE_SIZE, true); }}>Далее</button>
      </div>
    </nav>
  ) : null;

  return (
    <div className="app">
      <aside className="app-sidebar">
        <div className="app-brand"><span className="brand-mark"><Icon name="logo" /></span><div><p className="brand-name">Logister</p><small>Журналы событий</small></div></div>
        <p className="nav-label">Разделы</p>
        <nav className="section-nav" aria-label="Разделы приложения">
          {views.map(key => <button key={key} type="button" aria-pressed={view === key} onClick={() => setView(key)}><Icon name={navigation[key].icon} />{navigation[key].label}</button>)}
        </nav>
        {identity && <div className="account"><span className="account-icon"><Icon name="account" /></span><span className="account-name" title={identity.username}>{identity.username}</span><button type="button" title="Выйти" aria-label="Выйти" onClick={onLogout}><Icon name="logout" /></button></div>}
      </aside>
      <main className="app-main">
        <header className="workspace-bar">
          <div className="workspace-path"><span>Рабочая область</span><Icon name="arrow" /><strong>{navigation[view].label}</strong></div>
          <ThemePicker />
          <div className="health">
            <div className="health-path" aria-label="Путь лога">
              {health?.redis !== undefined && <span className={`pill ${health.redis ? "ok" : "bad"}`}>
                redis {serviceStatus(health?.redis)}
              </span>}
              {health?.kafka !== undefined && <span className={`pill ${health.kafka ? "ok" : "bad"}`}>
                kafka {serviceStatus(health.kafka)}
              </span>}
              <span className={`pill ${health ? health.opensearch ? "ok" : "bad" : ""}`}>
                opensearch {serviceStatus(health?.opensearch)}
              </span>
            </div>
            <div className="health-config" aria-label="Конфигурация">
              <span className={`pill ${health ? health.mongo ? "ok" : "bad" : ""}`}>
                mongo {serviceStatus(health?.mongo)}
              </span>
            </div>
          </div>
        </header>

        {healthError && <p className="error" role="alert">{healthError}</p>}

        {sessionError && <p className="error" role="alert">{sessionError}</p>}

        {view === "streams" && administrator && <StreamsSettings onSaved={setStreams} kafkaOnly={health?.kafka !== undefined} />}
        {view === "access" && identity?.administrator && <section className="access-section" aria-label="Доступ">
          <div className="access-tabs" role="tablist" aria-label="Разделы доступа">
            <button type="button" role="tab" aria-selected={accessTab === "ldap"} onClick={() => openAccessTab("ldap")}>Каталог</button>
            <button type="button" role="tab" aria-selected={accessTab === "grants"} onClick={() => openAccessTab("grants")}>Назначения</button>
          </div>
          {accessTab === "ldap" ? <LDAPSettings /> : <AccessSettings />}
        </section>}
        {view === "monitoring" && identity?.administrator && <Monitoring kafkaOnlyHint={health ? health.kafka !== undefined : undefined} />}
        <section className="search-page" hidden={view !== "search"} aria-label="Поиск сообщений">
          <header className="page-heading page-hero">
            <div className="heading-copy">
              <p className="page-kicker"><Icon name="streams" /> Журнал событий</p>
              <h1>Сообщения</h1>
              <p>Поиск событий по содержимому, периоду и потоку.</p>
            </div>
            {view === "search" && <div className="pipeline-status" aria-label="Краткое состояние очередей">
              <IngestIndicator compact />
              {administrator && <KafkaIndicator variant="compact" />}
            </div>}
          </header>
          <form className="searchbar" onSubmit={onSubmit} noValidate>
            <label className="search-query"><span>Поисковый запрос</span><span className="query-input"><Icon name="search" />
            <input
              aria-label="Поисковый запрос"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Запрос, например level:error OR source:nginx"
              spellCheck={false}
            />
            </span></label>
            <label><span>Период</span>
            <select
              value={relative}
              onChange={(e) => {
                const value = e.target.value;
                if (value === "custom" && !customRangeInitialized.current) {
                  if (appliedRange) { setAbsoluteFrom(localDateTime(appliedRange.from)); setAbsoluteTo(localDateTime(appliedRange.to)); }
                  customRangeInitialized.current = true;
                }
                setRelative(value); setRangeError("");
                if (value !== "custom") void runSearch(0, false, { relative: value });
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
            </label>
            <label><span>Поток</span>
            <select
              value={stream}
              onChange={(e) => { const value = e.target.value; setStream(value); void runSearch(0, false, { stream: value }); }}
              aria-label="Поток"
            >
              <option value="">Все сообщения</option>
              {streams.map((s) => (
                  <option key={s.id} value={s.name === "all" ? "@default" : s.name}>
                    {streamDisplayTitle(s)}
                  </option>
                ))}
            </select>
            </label>
            <button type="submit" disabled={loading}>
              {loading ? "Поиск…" : "Найти"}
            </button>
            {relative === "custom" && <div className="absolute-range">
              <label>С<input type="text" aria-label="С" placeholder="ДД.ММ.ГГГГ ЧЧ:мм:сс" autoComplete="off" spellCheck={false} required value={absoluteFrom} aria-invalid={Boolean(rangeError)} aria-describedby="time-range-help" onChange={event => { setAbsoluteFrom(event.target.value); setRangeError(""); }} /></label>
              <label>По<input type="text" aria-label="По" placeholder="ДД.ММ.ГГГГ ЧЧ:мм:сс" autoComplete="off" spellCheck={false} required value={absoluteTo} aria-invalid={Boolean(rangeError)} aria-describedby="time-range-help" onChange={event => { setAbsoluteTo(event.target.value); setRangeError(""); }} /></label>
              <p id="time-range-help">Формат: ДД.ММ.ГГГГ ЧЧ:мм:сс (секунды необязательны). Часовой пояс: {timeZone}. Обе границы включены. Нажмите «Найти», чтобы применить.</p>
            </div>}
            {rangeError && <div className="request-error time-range-error" role="alert">{rangeError}</div>}
          {appliedRange && <div className="applied-range" aria-label="Выбранный период">
            Выбранный период: <time dateTime={appliedRange.from}>{formatRangeTime(appliedRange.from)}</time>
            <span aria-hidden="true"> — </span><time dateTime={appliedRange.to}>{formatRangeTime(appliedRange.to)}</time>
          </div>}
          </form>

          {streamsError ? <div className="request-error" role="alert">{streamsError}</div> : null}
          {error ? <div className="request-error" role="alert">{error}</div> : null}

          <div className="results">
            <div className="result-list">
              <div className="result-toolbar">
                <div className="meta">
                  <div><h2>Результаты поиска</h2><span>{countFormat.format(total)}{totalRelation === "gte" ? "+" : ""} найдено · {tookMs} мс</span></div>
                  {queryPending && <span className="query-pending">Запрос изменён. Нажмите «Найти», чтобы применить.</span>}
                </div>
                {pages("Страницы результатов поиска")}
              </div>
              <div className="table-wrap" aria-busy={loading}>
                {loading ? <div className="empty" role="status"><Icon name="search" />Поиск…</div> : error ? null : messages.length === 0 ? (
                  <div className="empty"><Icon name="inbox" /><span>За выбранный период сообщений нет.</span></div>
                ) : (
                  <table className="result-table">
                    <thead>
                      <tr>
                        <th scope="col">Время</th>
                        <th scope="col">Уровень</th>
                        <th scope="col">Источник</th>
                        <th scope="col">Сообщение</th>
                      </tr>
                    </thead>
                    <tbody>
                      {messages.map((m) => (
                        <tr key={m.id} className="message-row" data-selected={selected === m.id} onClick={() => setSelected(selected === m.id ? null : m.id)}>
                          <td><time dateTime={m.timestamp}>{formatTime(m.timestamp)}</time></td>
                          <td>
                            <span className={`level ${m.level || "info"}`}>
                              {m.level || "info"}
                            </span>
                          </td>
                          <td className="message-source" title={m.source}>{m.source || "—"}</td>
                          <td><div className="msg">
                            <div className="msg-copy"><span className="message-source-mobile">{m.source || "—"}</span><span className="msg-text">{m.message.length > 500 ? m.message.slice(0, 500) + "…" : m.message}</span></div>
                            <button className="message-toggle" type="button" title="Поля сообщения" aria-label={`${selected === m.id ? "Скрыть" : "Показать"} поля сообщения ${m.id}`} aria-expanded={selected === m.id} onClick={event => { event.stopPropagation(); setSelected(selected === m.id ? null : m.id); }}><Icon name="arrow" /></button>
                          </div></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
              {pages("Конец списка результатов")}
            </div>
            {selectedMessage && <aside className="result-detail" aria-label="Выбранное сообщение">
              <MessageDetails message={selectedMessage} onClose={() => setSelected(null)} onSearch={(clause, exclude) => setQuery(current => appendFieldSearch(current, clause, exclude))} />
            </aside>}
          </div>
        </section>
      </main>
    </div>
  );
}
