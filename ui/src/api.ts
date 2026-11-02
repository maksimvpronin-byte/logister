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
  took_ms: number;
  messages: Message[];
};

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export function fetchHealth() {
  return getJSON<Health>("/api/health");
}

export function fetchStreams() {
  return getJSON<{ streams: Stream[] }>("/api/streams");
}

export function searchLogs(params: {
  q: string;
  relative: string;
  stream: string;
  size?: number;
}) {
  const qs = new URLSearchParams({
    q: params.q || "*",
    relative: params.relative,
    stream: params.stream,
    size: String(params.size ?? 100),
  });
  return getJSON<SearchResult>(`/api/search?${qs}`);
}
