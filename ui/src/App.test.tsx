import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { fetchHealth, fetchStreams, searchLogs, closeSearchSession } from "./api";

vi.mock("./api", () => ({ fetchHealth: vi.fn(), fetchStreams: vi.fn(), searchLogs: vi.fn(), closeSearchSession: vi.fn() }));

const range = { from: '2026-11-03T00:00:00Z', to: '2026-11-03T01:00:00Z' };
function result(offset = 0, total = 250) {
  return {
    total, total_relation: 'eq' as const, took_ms: 3, result_window: 0, ...range, offset, session_id: 'session',
    next_cursor: offset + 100 < total ? `next-${offset}` : undefined,
    previous_cursor: offset > 0 ? `previous-${offset}` : undefined,
    messages: Array.from({ length: Math.min(100, total - offset) }, (_, i) => ({
      id: String(offset + i), timestamp: '2026-11-03T00:30:00Z', message: `message-${offset + i}`,
      source: 'nginx', level: 'info', host: 'web-01', stream: 'web-access', fields: { seq: offset + i },
    })),
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(closeSearchSession).mockResolvedValue(undefined);
  vi.mocked(fetchHealth).mockResolvedValue({ status: 'ok', postgres: true, opensearch: true, time: range.to });
  vi.mocked(fetchStreams).mockResolvedValue({ streams: [
    { id: 'all', name: 'all', title: 'All messages', description: '' },
    { id: 'web', name: 'web-access', title: 'Web access', description: '' },
  ] });
  vi.mocked(searchLogs).mockResolvedValue(result());
});

afterEach(() => vi.useRealTimers());

describe('existing search UI', () => {
  it('keeps PostgreSQL up and streams available when only OpenSearch is down', async () => {
    vi.mocked(fetchHealth).mockResolvedValue({ status: 'degraded', postgres: true, opensearch: false, time: range.to });
    render(<App />);
    expect(await screen.findByText('postgres up')).toBeInTheDocument();
    expect(screen.getByText('opensearch down')).toBeInTheDocument();
    expect(await screen.findByRole('option', { name: 'Web access' })).toBeInTheDocument();
  });

  it('keeps metadata failures separate from search results', async () => {
    vi.mocked(fetchStreams).mockRejectedValue(new Error('Cannot load streams'));
    render(<App />);
    expect(await screen.findByText('postgres up')).toBeInTheDocument();
    expect(await screen.findByText('Cannot load streams')).toBeInTheDocument();
    expect(await screen.findByText('message-0', { exact: true })).toBeInTheDocument();
  });

  it('shows unknown service states on a transport error without discarding streams', async () => {
    vi.mocked(fetchHealth).mockRejectedValue(new Error('Network error'));
    render(<App />);
    expect(await screen.findByText('postgres unknown')).toBeInTheDocument();
    expect(screen.getByText('opensearch unknown')).toBeInTheDocument();
    expect(await screen.findByRole('option', { name: 'Web access' })).toBeInTheDocument();
  });

  it('clears stale rows and counters after an invalid query', async () => {
    const user = userEvent.setup();
    vi.mocked(searchLogs).mockResolvedValueOnce(result()).mockRejectedValueOnce(new Error('Invalid search query.'));
    render(<App />);
    await screen.findByText('message-0', { exact: true });
    const input = screen.getByRole('textbox');
    await user.clear(input);
    await user.type(input, '(');
    await user.click(screen.getByRole('button', { name: 'Search' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Invalid search query.');
    expect(screen.queryByText('message-0', { exact: true })).not.toBeInTheDocument();
    expect(screen.getByText(/0 hits/)).toBeInTheDocument();
    expect(screen.queryByText('No messages in this range.')).not.toBeInTheDocument();
  });

  it('pages with the applied query and fixed range, then resets paging for a new search', async () => {
    const user = userEvent.setup();
    vi.mocked(searchLogs).mockResolvedValueOnce(result()).mockResolvedValueOnce(result(100)).mockResolvedValueOnce(result(0, 1));
    render(<App />);
    await screen.findByText('message-0', { exact: true });
    await user.clear(screen.getByRole('textbox'));
    await user.type(screen.getByRole('textbox'), 'level:warn');
    await user.click(screen.getByRole('button', { name: 'Next page' }));
    expect(await screen.findByText('message-100', { exact: true })).toBeInTheDocument();
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ cursor: 'next-0' }));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.not.objectContaining({ relative: expect.anything() }));
    await user.click(screen.getByRole('button', { name: 'Search' }));
    await screen.findByText('message-0', { exact: true });
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ q: 'level:warn', relative: '24h', offset: 0 }));
    expect(closeSearchSession).toHaveBeenCalledWith('session');
    expect(screen.getByRole('button', { name: 'Previous page' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();
  });

  it('shows a lower-bound marker without an artificial browsing limit', async () => {
    vi.mocked(searchLogs).mockResolvedValue({ ...result(0, 12000), total_relation: 'gte' });
    render(<App />);
    expect(await screen.findByText(/12000\+ hits/)).toBeInTheDocument();
    expect(screen.queryByText(/first 10000/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeEnabled();
  });

  it('continues browsing beyond the old 10000-result window', async () => {
    vi.mocked(searchLogs).mockResolvedValue(result(10000, 12000));
    render(<App />);
    expect(await screen.findByText('message-10000', { exact: true })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next page' })).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Previous page' })).toBeEnabled();
  });

  it('ignores an older search response that arrives after the latest search', async () => {
    let resolveOld!: (value: ReturnType<typeof result>) => void;
    vi.mocked(searchLogs).mockReturnValueOnce(new Promise(resolve => { resolveOld = resolve; })).mockResolvedValueOnce(result(100));
    render(<App />);
    fireEvent.submit(screen.getByRole('textbox').closest('form')!);
    await screen.findByText('message-100', { exact: true });
    await act(async () => { resolveOld(result()); });
    expect(screen.queryByText('message-0', { exact: true })).not.toBeInTheDocument();
    expect(screen.getByText('message-100', { exact: true })).toBeInTheDocument();
  });

  it('refreshes service states and stops polling after unmount', async () => {
    vi.useFakeTimers();
    vi.mocked(fetchHealth).mockResolvedValueOnce({ status: 'degraded', postgres: true, opensearch: false, time: range.to })
      .mockResolvedValue({ status: 'ok', postgres: true, opensearch: true, time: range.to });
    let view!: ReturnType<typeof render>;
    await act(async () => { view = render(<App />); });
    expect(screen.getByText('opensearch down')).toBeInTheDocument();
    await act(async () => { vi.advanceTimersByTime(30000); });
    expect(screen.getByText('opensearch up')).toBeInTheDocument();
    view.unmount();
    await act(async () => { vi.advanceTimersByTime(30000); });
    expect(fetchHealth).toHaveBeenCalledTimes(2);
    expect(fetchStreams).toHaveBeenCalledTimes(2);
  });

  it('recovers the stream list after an initial metadata failure', async () => {
    vi.useFakeTimers();
    vi.mocked(fetchStreams).mockRejectedValueOnce(new Error('Cannot load streams'));
    await act(async () => { render(<App />); });
    expect(screen.getByText('Cannot load streams')).toBeInTheDocument();
    await act(async () => { vi.advanceTimersByTime(30000); });
    expect(screen.getByRole('option', { name: 'Web access' })).toBeInTheDocument();
    expect(screen.queryByText('Cannot load streams')).not.toBeInTheDocument();
  });

  it('shows an empty result without a request error', async () => {
    vi.mocked(searchLogs).mockResolvedValue(result(0, 0));
    render(<App />);
    expect(await screen.findByText('No messages in this range.')).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument());
  });
});
