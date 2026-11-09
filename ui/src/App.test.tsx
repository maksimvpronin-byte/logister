vi.mock("./KafkaIndicator", () => ({ default: () => <div /> }));
vi.mock("./IngestIndicator", () => ({ default: () => <div /> }));
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
  window.history.replaceState(null, "", "/");
  vi.resetAllMocks();
  vi.mocked(closeSearchSession).mockResolvedValue(undefined);
  vi.mocked(fetchHealth).mockResolvedValue({ status: 'ok', mongo: true, redis: true, opensearch: true, time: range.to });
  vi.mocked(fetchStreams).mockResolvedValue({ streams: [
    { id: 'all', name: 'all', title: "Все сообщения", description: '' },
    { id: 'web', name: 'web-access', title: 'Web access', description: '' },
  ] });
  vi.mocked(searchLogs).mockResolvedValue(result());
});

afterEach(() => vi.useRealTimers());

describe('existing search UI', () => {
  it('previews a long message but expands its full tail', async () => {
    const user = userEvent.setup();
    const page = result(0, 1);
    const full = 'start '.repeat(200) + 'tailsearchmarker';
    page.messages[0].message = full;
    vi.mocked(searchLogs).mockResolvedValue(page);
    render(<App />);
    expect(await screen.findByText(full.slice(0, 500) + '…', { exact: true })).toBeInTheDocument();
    expect(screen.queryByText(full, { exact: true })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Показать поля сообщения 0' }));
    expect(screen.getAllByText(full, { exact: true }).length).toBeGreaterThan(0);
  });
  it('adds field-menu conditions to the query draft and applies them on Search', async () => {
    const user = userEvent.setup(); render(<App />);
    await screen.findByText('message-0', { exact: true });
    await user.clear(screen.getByLabelText("Поисковый запрос")); await user.type(screen.getByLabelText("Поисковый запрос"), 'level:error OR level:warn');
    await user.click(screen.getByRole('button', { name: "Показать поля сообщения 0" }));
    const trigger = screen.getByRole('button', { name: "Действия с полем source" });
    vi.spyOn(trigger, 'getBoundingClientRect').mockReturnValue(new DOMRect(20, 50, 24, 24));
    await user.click(trigger);
    await user.click(screen.getByRole('menuitem', { name: /Добавить в поиск/ }));
    expect(screen.getByLabelText("Поисковый запрос")).toHaveValue('(level:error OR level:warn) AND (source:"nginx")');
    expect(searchLogs).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('button', { name: "Найти" }));
    await waitFor(() => expect(searchLogs).toHaveBeenCalledTimes(2));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ q: '(level:error OR level:warn) AND (source:"nginx")', relative: '24h' }));
  });
  it('applies custom local bounds and keeps cursor paging independent of draft edits', async () => {
    const user = userEvent.setup();
    vi.mocked(searchLogs).mockResolvedValueOnce(result()).mockResolvedValueOnce(result()).mockResolvedValueOnce(result(100));
    render(<App />);
    await screen.findByText('message-0', { exact: true });
    await user.selectOptions(screen.getByLabelText("Период"), 'custom');
    fireEvent.change(screen.getByLabelText("С", { exact: true }), { target: { value: '2026-11-03T03:00:00' } });
    fireEvent.change(screen.getByLabelText("По", { exact: true }), { target: { value: '2026-11-03T04:00:00' } });
    await user.click(screen.getByRole('button', { name: "Найти" }));
    await screen.findByText('message-0', { exact: true });
    const request = vi.mocked(searchLogs).mock.calls[1][0];
    expect(request.from).toBe(new Date(2026, 10, 3, 3).toISOString());
    expect(request.to).toBe(new Date(2026, 10, 3, 4).toISOString());
    expect(request).not.toHaveProperty('relative');
    expect(screen.getByLabelText("Выбранный период").querySelector('time')).toHaveAttribute('datetime', range.from);
    fireEvent.change(screen.getByLabelText("С", { exact: true }), { target: { value: '2026-11-04T03:00:00' } });
    await user.click(screen.getAllByRole('button', { name: "Следующая страница" })[0]);
    await screen.findByText('message-100', { exact: true });
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ cursor: 'next-0' }));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.not.objectContaining({ from: expect.anything() }));
    await user.click(screen.getByRole('button', { name: "Найти" }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Начало периода должно быть раньше окончания.');
    expect(searchLogs).toHaveBeenCalledTimes(3);
    expect(screen.getByText('message-100', { exact: true })).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Период"), '1h');
    await waitFor(() => expect(searchLogs).toHaveBeenCalledTimes(4));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ relative: '1h' }));
    await user.selectOptions(screen.getByLabelText("Период"), 'custom');
    expect(screen.getByLabelText("С", { exact: true })).toHaveValue('2026-11-04T03:00:00');
    expect(searchLogs).toHaveBeenCalledTimes(4);
    await user.selectOptions(screen.getByLabelText("Период"), '1h');
    await waitFor(() => expect(searchLogs).toHaveBeenCalledTimes(5));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ relative: '1h' }));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.not.objectContaining({ from: expect.anything() }));
  });
  it('separates the all-streams view from the unmatched destination', async () => {
    const user = userEvent.setup(); render(<App />);
    await screen.findByText('message-0', { exact: true });
    expect(screen.getByRole('option', { name: "Все сообщения" })).toHaveValue('');
    expect(screen.getByRole('option', { name: "Сообщения без совпавших правил" })).toHaveValue('@default');
    await user.selectOptions(screen.getByLabelText("Поток", { exact: true }), '@default');
    await user.click(screen.getByRole('button', { name: "Найти" }));
    await waitFor(() => expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ stream: '@default' })));
  });
  it('keeps MongoDB up and streams available when only OpenSearch is down', async () => {
    vi.mocked(fetchHealth).mockResolvedValue({ status: 'degraded', mongo: true, redis: true, opensearch: false, time: range.to });
    render(<App />);
    expect(await screen.findByText('mongo доступен')).toBeInTheDocument();
    expect(screen.getByText('redis доступен')).toBeInTheDocument();
    expect(screen.getByText('opensearch недоступен')).toBeInTheDocument();
    expect(await screen.findByRole('option', { name: 'Web access' })).toBeInTheDocument();
  });

  it('keeps metadata failures separate from search results', async () => {
    vi.mocked(fetchStreams).mockRejectedValue(new Error('Cannot load streams'));
    render(<App />);
    expect(await screen.findByText('mongo доступен')).toBeInTheDocument();
    expect(await screen.findByText('Cannot load streams')).toBeInTheDocument();
    expect(await screen.findByText('message-0', { exact: true })).toBeInTheDocument();
  });

  it('shows unknown service states on a transport error without discarding streams', async () => {
    vi.mocked(fetchHealth).mockRejectedValue(new Error('Network error'));
    render(<App />);
    expect(await screen.findByText('mongo неизвестно')).toBeInTheDocument();
    expect(screen.getByText('opensearch неизвестно')).toBeInTheDocument();
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
    await user.click(screen.getByRole('button', { name: "Найти" }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Invalid search query.');
    expect(screen.queryByText('message-0', { exact: true })).not.toBeInTheDocument();
    expect(screen.getByText(/0 найдено/)).toBeInTheDocument();
    expect(screen.queryByText("За выбранный период сообщений нет.")).not.toBeInTheDocument();
  });

  it('pages with the applied query and fixed range, then resets paging for a new search', async () => {
    const user = userEvent.setup();
    vi.mocked(searchLogs).mockResolvedValueOnce(result()).mockResolvedValueOnce(result(100)).mockResolvedValueOnce(result(0, 1));
    render(<App />);
    await screen.findByText('message-0', { exact: true });
    await user.clear(screen.getByRole('textbox'));
    await user.type(screen.getByRole('textbox'), 'level:warn');
    await user.click(screen.getAllByRole('button', { name: "Следующая страница" })[0]);
    expect(await screen.findByText('message-100', { exact: true })).toBeInTheDocument();
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ cursor: 'next-0' }));
    expect(searchLogs).toHaveBeenLastCalledWith(expect.not.objectContaining({ relative: expect.anything() }));
    await user.click(screen.getByRole('button', { name: "Найти" }));
    await screen.findByText('message-0', { exact: true });
    expect(searchLogs).toHaveBeenLastCalledWith(expect.objectContaining({ q: 'level:warn', relative: '24h', offset: 0 }));
    expect(closeSearchSession).toHaveBeenCalledWith('session');
    expect(screen.getAllByRole('button', { name: "Предыдущая страница" })[0]).toBeDisabled();
    expect(screen.getAllByRole('button', { name: "Следующая страница" })[0]).toBeDisabled();
  });

  it('shows a lower-bound marker without an artificial browsing limit', async () => {
    vi.mocked(searchLogs).mockResolvedValue({ ...result(0, 12000), total_relation: 'gte' });
    render(<App />);
    expect(await screen.findByText(/12\s000\+ найдено/)).toBeInTheDocument();
    expect(screen.queryByText(/first 10000/)).not.toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: "Следующая страница" })[0]).toBeEnabled();
  });

  it('continues browsing beyond the old 10000-result window', async () => {
    vi.mocked(searchLogs).mockResolvedValue(result(10000, 12000));
    render(<App />);
    expect(await screen.findByText('message-10000', { exact: true })).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: "Следующая страница" })[0]).toBeEnabled();
    expect(screen.getAllByRole('button', { name: "Предыдущая страница" })[0]).toBeEnabled();
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
    vi.mocked(fetchHealth).mockResolvedValueOnce({ status: 'degraded', mongo: true, redis: true, opensearch: false, time: range.to })
      .mockResolvedValue({ status: 'ok', mongo: true, redis: true, opensearch: true, time: range.to });
    let view!: ReturnType<typeof render>;
    await act(async () => { view = render(<App />); });
    expect(screen.getByText('opensearch недоступен')).toBeInTheDocument();
    await act(async () => { vi.advanceTimersByTime(30000); });
    expect(screen.getByText('opensearch доступен')).toBeInTheDocument();
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
    expect(await screen.findByText("За выбранный период сообщений нет.")).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument());
  });
});
