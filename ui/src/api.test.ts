import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchHealth, searchLogs } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("HTTP client", () => {
  it("uses only the signed cursor for a snapshot page", async () => {
    const fetch = vi.fn().mockResolvedValue(new Response('{}'));
    vi.stubGlobal("fetch", fetch);
    await searchLogs({ cursor: 'signed-page', q: 'unsaved draft', relative: '15m' });
    const url = new URL(fetch.mock.calls[0][0], 'http://localhost');
    expect([...url.searchParams.entries()]).toEqual([['cursor', 'signed-page']]);
  });
  it("preserves individual service states on HTTP 503", async () => {
    const body = { status: "degraded", mongo: true, opensearch: false, time: "2026-11-03T00:00:00Z" };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(body), { status: 503 })));
    await expect(fetchHealth()).resolves.toEqual(body);
  });

  it("does not treat a malformed health response as a service status", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('{"error":"unavailable"}', { status: 503 })));
    await expect(fetchHealth()).rejects.toThrow();
  });

  it("sends an absolute range and offset when paging", async () => {
    const fetch = vi.fn().mockResolvedValue(new Response('{}'));
    vi.stubGlobal("fetch", fetch);
    await searchLogs({ q: 'source:nginx', stream: 'all', from: '2026-11-03T00:00:00Z', to: '2026-11-03T01:00:00Z', offset: 100 });
    const url = new URL(fetch.mock.calls[0][0], 'http://localhost');
    expect(url.searchParams.get('from')).toBe('2026-11-03T00:00:00Z');
    expect(url.searchParams.get('to')).toBe('2026-11-03T01:00:00Z');
    expect(url.searchParams.get('offset')).toBe('100');
    expect(url.searchParams.has('relative')).toBe(false);
  });
});
