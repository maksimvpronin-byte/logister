import { FormEvent, useEffect, useRef, useState } from "react";
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
  { value: "15m", label: "Last 15 minutes" },
  { value: "1h", label: "Last 1 hour" },
  { value: "6h", label: "Last 6 hours" },
  { value: "24h", label: "Last 24 hours" },
  { value: "7d", label: "Last 7 days" },
];

const PAGE_SIZE = 100;

function formatTime(value: string) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [healthLoading, setHealthLoading] = useState(true);
  const [healthError, setHealthError] = useState<string | null>(null);
  const [streamsError, setStreamsError] = useState<string | null>(null);
  const [streams, setStreams] = useState<Stream[]>([]);
  const [query, setQuery] = useState("*");
  const [relative, setRelative] = useState("24h");
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
    const requestId = ++searchRequest.current;
    searchController.current?.abort();
    const controller = new AbortController();
    searchController.current = controller;
    const cursor = paging ? nextOffset > offset ? nextCursor : previousCursor : undefined;
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
    try {
      const result = await searchLogs(cursor
        ? { cursor, signal: controller.signal }
        : { q: query, relative, stream, size: PAGE_SIZE, offset: 0, signal: controller.signal });
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
    } catch (e) {
      if (requestId !== searchRequest.current || controller.signal.aborted) return;
      setError(e instanceof Error ? e.message : "Search failed");
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
          setHealthError(e instanceof Error ? e.message : "Service status is unavailable");
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
        if (active) setStreamsError(e instanceof Error ? e.message : "Failed to load streams");
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
    if (healthLoading) return "checking";
    return up === undefined ? "unknown" : up ? "up" : "down";
  }

  const canGoNext = messages.length > 0 && Boolean(nextCursor);

  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <h1>
            Log<span>ister</span>
          </h1>
          <p>Open log search — OpenSearch + Postgres</p>
        </div>
        <div className="health">
          <span className={`pill ${health ? health.postgres ? "ok" : "bad" : ""}`}>
            postgres {serviceStatus(health?.postgres)}
          </span>
          <span className={`pill ${health ? health.opensearch ? "ok" : "bad" : ""}`}>
            opensearch {serviceStatus(health?.opensearch)}
          </span>
        </div>
      </header>

      <form className="searchbar" onSubmit={onSubmit}>
        <input
          aria-label="Search query"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder='query, e.g. level:error OR source:nginx'
          spellCheck={false}
        />
        <select
          value={relative}
          onChange={(e) => setRelative(e.target.value)}
          aria-label="Time range"
        >
          {RELATIVE_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <select
          value={stream}
          onChange={(e) => setStream(e.target.value)}
          aria-label="Stream"
        >
          {streams.length === 0 ? (
            <option value="all">all</option>
          ) : (
            streams.map((s) => (
              <option key={s.id} value={s.name}>
                {s.title}
              </option>
            ))
          )}
        </select>
        <button type="submit" disabled={loading}>
          {loading ? "Searching…" : "Search"}
        </button>
      </form>

      <div className="meta">
        <span>
          {total}{totalRelation === "gte" ? "+" : ""} hits · {tookMs} ms
        </span>
        <span>Lucene-like query · stream filter from Postgres</span>
      </div>

      {healthError ? <div className="request-error" role="alert">{healthError}</div> : null}
      {streamsError ? <div className="request-error" role="alert">{streamsError}</div> : null}
      {error ? <div className="request-error" role="alert">{error}</div> : null}

      {messages.length > 0 && !error ? (
        <nav className="pagination" aria-label="Search result pages">
          <span>Showing {offset + 1}–{offset + messages.length} of {total}{totalRelation === "gte" ? "+" : ""}</span>
          <div>
            <button type="button" aria-label="Previous page" disabled={loading || !previousCursor}
              onClick={() => { void runSearch(Math.max(0, offset - PAGE_SIZE), true); }}>Previous</button>
            <button type="button" aria-label="Next page" disabled={loading || !canGoNext}
              onClick={() => { void runSearch(offset + PAGE_SIZE, true); }}>Next</button>
          </div>
        </nav>
      ) : null}

      <div className="table-wrap" aria-busy={loading}>
        {loading ? <div className="empty" role="status">Searching…</div> : error ? null : messages.length === 0 ? (
          <div className="empty">No messages in this range.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Level</th>
                <th>Source</th>
                <th>Host</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {messages.map((m) => (
                <tr key={m.id} onClick={() => setSelected(m.id)}>
                  <td>{formatTime(m.timestamp)}</td>
                  <td>
                    <span className={`level ${m.level || "info"}`}>
                      {m.level || "info"}
                    </span>
                  </td>
                  <td>{m.source}</td>
                  <td>{m.host}</td>
                  <td className="msg">
                    {m.message}
                    {selected === m.id && m.fields ? (
                      <div className="details">
                        stream={m.stream} · fields=
                        {JSON.stringify(m.fields)}
                      </div>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
