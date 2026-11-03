import { describe, expect, it } from "vitest";
import { appendFieldSearch, fieldSearch } from "./fieldSearch";
import { parseMessageFields } from "./messageFields";
import type { Message } from "./api";

const message: Message = { id: "id", timestamp: "2026-11-03T00:00:00Z", message: "hello", source: "app", host: "server", level: "info", stream: "all" };

describe("field search actions", () => {
  it("groups existing OR expressions when including or excluding a value", () => {
    expect(appendFieldSearch('level:error OR level:warn', 'host:"server"', false)).toBe('(level:error OR level:warn) AND (host:"server")');
    expect(appendFieldSearch('level:error OR level:warn', 'host:"server"', true)).toBe('(level:error OR level:warn) AND NOT (host:"server")');
    expect(appendFieldSearch('*', 'host:"server"', true)).toBe('*:* AND NOT (host:"server")');
    expect(appendFieldSearch('  ', 'host:"server"', false)).toBe('host:"server"');
  });
  it("escapes query operators, quotes and backslashes in values", () => {
    const root = parseMessageFields(JSON.stringify({ fields: { path: '/a?b="x"\\file OR *:*' } }));
    const node = root.children![0].children![0];
    expect(fieldSearch(node, message)).toEqual({ clause: 'fields.path:"\\/a\\?b\\=\\"x\\"\\\\file OR \\*\\:\\*"' });
  });
  it("uses indexed metadata and nested fields while explaining unindexed raw properties", () => {
    const root = parseMessageFields('{"source":"app","app":"123","fields":{"seq":900719925474099312345},"nil":null,"timestamp":"2026-11-03T00:00:00Z"}');
    const [source, app, fields, nil, timestamp] = root.children!;
    expect(fieldSearch(source, message)).toEqual({ clause: 'source:"app"' });
    expect(fieldSearch(fields.children![0], message)).toEqual({ clause: 'fields.seq:"900719925474099312345"' });
    expect(fieldSearch(app, message).reason).toContain("не входит в поисковый индекс");
    expect(fieldSearch(nil, message).clause).toBeUndefined();
    expect(fieldSearch(timestamp, message).clause).toMatch(/^@timestamp:/);
    expect(fieldSearch(source, { ...message, source: "different" }).clause).toBeUndefined();
  });
});
