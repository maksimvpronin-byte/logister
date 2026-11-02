const API_URL = import.meta.env.VITE_API_URL || "";

export type Health = {
  status: string;
  postgres: boolean;
  opensearch: boolean;
  time: string;
};

export type Stream = {
  id: string;
  name: string;
  title: string;
  description: string;
};

export type Message = {
  id: string;
  timestamp: string;
  message: string;
  source: string;
  level: string;
  host: string;
  stream: string;
  facility?: string;
  fields?: Record<string, unknown>;
};

export type SearchResult = {
  total: number;
  total_relation: "eq" | "gte";
  took_ms: number;
  messages: Message[];
  from: string;
  to: string;
  result_window: number;
  next_cursor?: string;
  previous_cursor?: string;
  session_id?: string;
  offset?: number;
};

async function getJSON<T>(path: string, signal?: AbortSignal, acceptedStatus?: number): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, { signal });
  if (!res.ok && res.status !== acceptedStatus) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function fetchHealth(signal?: AbortSignal) {
  const health = await getJSON<Health>("/api/health", signal, 503);
  if (!health || typeof health.postgres !== "boolean" || typeof health.opensearch !== "boolean") {
    throw new Error("Service status is unavailable");
  }
  return health;
}

export function fetchStreams(signal?: AbortSignal) {
  return getJSON<{ streams: Stream[] }>("/api/streams", signal);
}

export function searchLogs(params: {
  q?: string;
  relative?: string;
  stream?: string;
  from?: string;
  to?: string;
  size?: number;
  offset?: number;
  signal?: AbortSignal;
  cursor?: string;
}) {
  if (params.cursor) return getJSON<SearchResult>(`/api/search?${new URLSearchParams({ cursor: params.cursor })}`, params.signal);
  const qs = new URLSearchParams({
    q: params.q || "*",
    stream: params.stream ?? "all",
    size: String(params.size ?? 100),
    offset: String(params.offset ?? 0),
  });
  if (params.relative !== undefined) qs.set("relative", params.relative);
  if (params.from !== undefined) qs.set("from", params.from);
  if (params.to !== undefined) qs.set("to", params.to);
  return getJSON<SearchResult>(`/api/search?${qs}`, params.signal);
}

export async function closeSearchSession(session_id: string) {
  if (!session_id) return;
  await fetch(`${API_URL}/api/search/session`, { method: "DELETE", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ session_id }), keepalive: true });
}
