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
  routing?: RoutingConfig;
  storage?: StorageConfig;
};

export type RoutingRule = { field: string; operator: "equals" | "contains" | "starts_with" | "exists" | "not_exists"; value: string };
export type RoutingConfig = { enabled: boolean; priority: number; match: "all" | "any"; rules: RoutingRule[] };
export type RoutingPreview = { stream: string; reason: "explicit" | "rule" | "fallback"; candidates: { name: string; title: string; enabled: boolean; priority: number; match: "all" | "any"; matched: boolean; selected: boolean; rules: (RoutingRule & { actual: string; exists: boolean; matched: boolean })[] }[] };
export function previewRouting(message: unknown, draft: StreamDraft) {
  return writeJSON<RoutingPreview>("/api/streams/preview-routing", "POST", typeof message === "string" ? { message_json: message, draft } : { message, draft });
}
export type StorageConfig = { max_age_hours: number; max_size_mb: number; max_documents: number; retention_days: number };
export type StreamDraft = Omit<Stream, "id" | "routing" | "storage"> & { routing: RoutingConfig; storage: StorageConfig };
export type StreamIndex = { name: string; generation: number; created_at: string; sealed_at: string | null; deleted_at: string | null; accepted_count: number; documents?: number; size_bytes?: number; stats_error?: string; preparing?: boolean };

async function writeJSON<T>(path: string, method: string, body?: unknown): Promise<T> {
  const response = await fetch(`${API_URL}${path}`, { method, headers: { "Content-Type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}

export function saveStream(stream: StreamDraft, create: boolean) {
  return writeJSON<{ stream: Stream }>(create ? "/api/streams" : `/api/streams/${encodeURIComponent(stream.name)}`, create ? "POST" : "PUT", stream);
}
export function fetchStreamIndices(name: string, signal?: AbortSignal) {
  return getJSON<{ indices: StreamIndex[] }>(`/api/streams/${encodeURIComponent(name)}/indices`, signal);
}
export function rotateStream(name: string) {
  return writeJSON<{ rotated: boolean }>(`/api/streams/${encodeURIComponent(name)}/rotate`, "POST");
}
export function sendStreamMessage(name: string, event: { id: string; message: string; source: string }) {
  return writeJSON<{ count: number; duplicates: number }>(`/api/streams/${encodeURIComponent(name)}/ingest`, "POST", event);
}

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
  payload?: Record<string, unknown>;
  payload_json?: string;
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
