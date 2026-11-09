import type { Message } from "./api";
import type { FieldNode } from "./messageFields";

export type FieldSearch = { clause: string; reason?: never } | { clause?: never; reason: string };

// These characters are literal values here, never Lucene operators.
function escapeLiteral(value: string) { return value.replace(/[+\-=!(){}\[\]^"~*?:\\/&|]/g, "\\$&"); }

export function fieldSearch(node: FieldNode, message: Message): FieldSearch {
  if (node.children || node.type === "null") return { reason: "Для объектов, массивов и null поиск по значению недоступен." };
  if (!node.text || node.text.length > 4096 || /[\u0000-\u001f<>]/.test(node.text)) {
    return { reason: "Это значение нельзя безопасно добавить в текстовый поисковый запрос." };
  }
  let field: string;
  if (["source", "host", "level", "message", "facility", "stream", "service", "request_id", "trace_id", "ip"].includes(node.path)) {
    if (node.type !== "string" || node.text !== message[node.path as keyof Message]) {
      return { reason: "Исходное значение отличается от поискового представления этого поля." };
    }
    field = node.path;
  } else if (node.path === "status" && message.status !== undefined && node.text === String(message.status)) {
    field = "status";
  } else if (node.path === "timestamp" && node.type === "string"
    && Number.isFinite(Date.parse(node.text)) && Date.parse(node.text) === Date.parse(message.timestamp)) {
    field = "@timestamp";
  } else if (/^fields(?:\.[A-Za-z_][A-Za-z0-9_-]*)+$/.test(node.path) && node.path.length <= 1024 && node.path.split(".").length <= 33) {
    field = node.path;
  } else {
    return { reason: "Поле сохранено в исходном JSON, но не входит в поисковый индекс. Для поиска по нему нужна индексация." };
  }
  return { clause: `${escapeLiteral(field)}:"${escapeLiteral(node.text)}"` };
}

export function appendFieldSearch(current: string, clause: string, exclude: boolean): string {
  const query = current.trim();
  if (!query || query === "*" || query === "*:*") return exclude ? `*:* AND NOT (${clause})` : clause;
  return `(${query}) AND ${exclude ? "NOT " : ""}(${clause})`;
}
