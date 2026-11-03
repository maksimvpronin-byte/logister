import { apiError } from "./locale";

async function requestFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  try { return await fetch(input, init); }
  catch (error) {
    if (error instanceof Error && error.name === "AbortError") throw error;
    throw new Error("Не удалось связаться с сервером. Проверьте подключение и повторите попытку.");
  }
}

const API_URL = import.meta.env.VITE_API_URL || "";

export type Identity = { enabled: boolean; username: string; user_id: string; administrator: boolean; permissions: string[]; access_version?: string };
export type DirectoryEntry = { id: string; kind: "user" | "group"; name: string; dn: string };
export type Grant = { principal_kind: "user" | "group"; principal_id: string; role: "operator" | "service" | "administrator"; scope: "system" | "stream"; stream_id?: string };
export type GrantCatalog = { grants: Grant[]; revision: number };

function sessionExpired(status: number) { if (status === 401) window.dispatchEvent(new Event("logister:session-expired")); }
export const authStatus = () => getJSON<{ enabled: boolean }>("/api/auth/status");
export const fetchIdentity = () => getJSON<Identity>("/api/auth/me");
export const login = (username: string, password: string, source: string) => writeJSON<{ username: string }>("/api/auth/login", "POST", { username, password, source });
export const logout = async () => {
  const res = await requestFetch(`${API_URL}/api/auth/logout`, { method: "POST", credentials: "include" });
  if (!res.ok && res.status !== 401) throw new Error("Не удалось выйти. Повторите попытку.");
};
export const fetchDirectory = (query = "") => getJSON<{ entries: DirectoryEntry[] }>(`/api/auth/directory?q=${encodeURIComponent(query)}`);
export const fetchGrants = () => getJSON<GrantCatalog>("/api/auth/grants");
export const saveGrants = (catalog: GrantCatalog) => writeJSON("/api/auth/grants", "PUT", catalog);

export type LDAPSettings = { url: string; bind_dn: string; users_dn: string; groups_dn: string; password_set: boolean; revision: number };
export type LDAPSettingsDraft = Omit<LDAPSettings, "password_set"> & { bind_password?: string };
export type LDAPCheck = { users_found: boolean; groups_found: boolean };
export const fetchLDAPSettings = () => getJSON<LDAPSettings>("/api/auth/ldap");
export const testLDAPSettings = (draft: LDAPSettingsDraft) => writeJSON<LDAPCheck>("/api/auth/ldap/test", "POST", draft);
export const saveLDAPSettings = (draft: LDAPSettingsDraft) => writeJSON<LDAPSettings>("/api/auth/ldap", "PUT", draft);

export type IngestCounts = { accepted: number; published: number; pending: number };
export type IngestStats = { captured_at: string; totals: IngestCounts; streams: (IngestCounts & { id: string; name: string; title: string })[] };
export const fetchIngestStats = (signal?: AbortSignal) => getJSON<IngestStats>("/api/ingest/stats", signal);

export type Health = {
  status: string;
  postgres: boolean;
  redis?: boolean;
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
  const response = await requestFetch(`${API_URL}${path}`, { method, credentials: "include", headers: { "Content-Type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) { sessionExpired(response.status); throw apiError(data.error, response.status); }
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
  const res = await requestFetch(`${API_URL}${path}`, { signal, credentials: "include" });
  if (!res.ok && res.status !== acceptedStatus) {
    const body = await res.json().catch(() => ({}));
    sessionExpired(res.status);
    throw apiError(body.error, res.status);
  }
  return res.json();
}

export async function fetchHealth(signal?: AbortSignal) {
  const health = await getJSON<Health>("/api/health", signal, 503);
  if (!health || typeof health.postgres !== "boolean" || typeof health.opensearch !== "boolean") {
    throw new Error("Состояние сервисов недоступно");
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
  await requestFetch(`${API_URL}/api/search/session`, { method: "DELETE", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ session_id }), keepalive: true });
}

export const deleteStream = (name: string) => writeJSON<{deleted:boolean}>(`/api/streams/${encodeURIComponent(name)}`, "DELETE", {});
export const restoreStream = (name: string) => writeJSON<{deleted:boolean}>(`/api/streams/${encodeURIComponent(name)}/restore`, "POST", {});
export const fetchDeletedStreams = () => getJSON<{streams:Stream[]}>("/api/streams/deleted");
export type KafkaStatus={enabled:boolean;topic:string;group:string;pause_requested:boolean;running:boolean;available:number;committed:number;lag:number;error?:string;retention_loss:boolean;captured_at:string};
export const fetchKafkaStatus=(signal?:AbortSignal)=>getJSON<KafkaStatus>("/api/ingest/kafka",signal);
export const setKafkaPaused=(paused:boolean)=>writeJSON<KafkaStatus>("/api/ingest/kafka","PUT",{paused});
