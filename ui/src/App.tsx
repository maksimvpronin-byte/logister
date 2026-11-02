import { FormEvent, useEffect, useState } from "react";
import {
  fetchHealth,
  fetchStreams,
  searchLogs,
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

function formatTime(value: string) {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

export default function App() {
  const [health, setHealth] = useState<Health | null>(null);
  const [streams, setStreams] = useState<Stream[]>([]);
  const [query, setQuery] = useState("*");
  const [relative, setRelative] = useState("24h");
  const [stream, setStream] = useState("all");
  const [messages, setMessages] = useState<Message[]>([]);
  const [total, setTotal] = useState(0);
  const [tookMs, setTookMs] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  async function loadMeta() {
    try {
      const [h, s] = await Promise.all([fetchHealth(), fetchStreams()]);
      setHealth(h);
      setStreams(s.streams);
    } catch (e) {
      setHealth(null);
      setError(e instanceof Error ? e.message : "Failed to load metadata");
    }
  }

  async function runSearch(next?: {
    q?: string;
    relative?: string;
    stream?: string;
  }) {
    setLoading(true);
    setError(null);
    try {
      const result = await searchLogs({
        q: next?.q ?? query,
        relative: next?.relative ?? relative,
        stream: next?.stream ?? stream,
      });
      setMessages(result.messages);
      setTotal(result.total);
      setTookMs(result.took_ms);
      setSelected(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Search failed");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadMeta();
    void runSearch();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function onSubmit(e: FormEvent) {
    e.preventDefault();
    void runSearch();
  }

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
          <span className={`pill ${health?.postgres ? "ok" : "bad"}`}>
            postgres {health?.postgres ? "up" : "down"}
          </span>
          <span className={`pill ${health?.opensearch ? "ok" : "bad"}`}>
            opensearch {health?.opensearch ? "up" : "down"}
          </span>
        </div>
      </header>

      <form className="searchbar" onSubmit={onSubmit}>
        <input
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
          {total} hits · {tookMs} ms
        </span>
        <span>Lucene-like query · stream filter from Postgres</span>
      </div>

      {error ? <div className="error">{error}</div> : null}

      <div className="table-wrap">
        {messages.length === 0 && !loading ? (
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
