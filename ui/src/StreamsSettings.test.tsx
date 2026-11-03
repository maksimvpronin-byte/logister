import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import StreamsSettings from "./StreamsSettings";
import { deleteStream, restoreStream, fetchDeletedStreams, fetchStreams, fetchStreamIndices, saveStream, rotateStream, sendStreamMessage, previewRouting, type Stream } from "./api";

vi.mock("./api", () => ({ deleteStream:vi.fn(),restoreStream:vi.fn(),fetchDeletedStreams:vi.fn(),fetchStreams: vi.fn(), fetchStreamIndices: vi.fn(), saveStream: vi.fn(), rotateStream: vi.fn(), sendStreamMessage: vi.fn(), previewRouting: vi.fn() }));
const stream: Stream = { id: "payments-id", name: "payments", title: "Payments", description: "Payment events", routing: { enabled: false, priority: 100, match: "all", rules: [] }, storage: { max_age_hours: 24, max_size_mb: 1024, max_documents: 1000000, retention_days: 0 } };
beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(fetchStreams).mockResolvedValue({ streams: [stream] });
  vi.mocked(fetchStreamIndices).mockResolvedValue({ indices: [] });
  vi.mocked(saveStream).mockImplementation(async draft => ({ stream: { ...draft, id: "saved-id" } }));
  vi.mocked(rotateStream).mockResolvedValue({ rotated: true });
  vi.mocked(sendStreamMessage).mockResolvedValue({ count: 1, duplicates: 0 });
  vi.mocked(previewRouting).mockResolvedValue({ stream: "payments", reason: "rule", candidates: [{ name: "payments", title: "Payments", enabled: true, priority: 100, match: "all", matched: true, selected: true, rules: [{ field: "source", operator: "equals", value: "nginx", actual: "nginx", exists: true, matched: true }] }] });
});

describe("stream settings", () => {
  it("creates a technical ID from the display title and preserves a manual override", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Идентификатор");
    await user.click(screen.getByRole("button", { name: "Создать поток" }));
    await user.type(screen.getByLabelText("Название"), "New stream");
    expect(screen.getByLabelText("Идентификатор")).toHaveValue("new-stream");
    await user.clear(screen.getByLabelText("Идентификатор"));
    await user.type(screen.getByLabelText("Идентификатор"), "Custom Stream");
    expect(screen.getByLabelText("Идентификатор")).toHaveValue("custom-stream");
    await user.type(screen.getByLabelText("Название"), " title");
    expect(screen.getByLabelText("Идентификатор")).toHaveValue("custom-stream");
    await user.click(screen.getByRole("button", { name: "Создать поток с настройками" }));
    await screen.findByText(/Настройки потока сохранены/);
    expect(saveStream).toHaveBeenCalledWith(expect.objectContaining({ name: "custom-stream", title: "New stream title" }), true);
  });
  it("adds a condition from any field found in the sample", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Идентификатор");
    await user.clear(screen.getByLabelText("Пример JSON-сообщения"));
    await user.paste('{"custom":{"items":[{"code":503}]},"tenant":"acme"}');
    await user.click(screen.getByRole("button", { name: "Использовать поле custom.items[0].code" }));
    expect(screen.getByLabelText("Условие 1 поле")).toHaveValue("custom.items[0].code");
    expect(screen.getByLabelText("Условие 1 оператор")).toHaveValue("exists");
    expect(screen.getByLabelText("Условие 1 значение")).toBeDisabled();
    expect(screen.getByLabelText("Включить правила маршрутизации")).toBeChecked();
  });
  it("explains routing against the unsaved draft without saving or ingesting", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Идентификатор");
    await user.click(screen.getByLabelText("Включить правила маршрутизации"));
    await user.click(screen.getByRole("button", { name: "Добавить условие" }));
    await user.type(screen.getByLabelText("Условие 1 значение"), "nginx");
    await user.click(screen.getByRole("button", { name: "Проверить маршрутизацию" }));
    expect(await screen.findByText(/Поток назначения: Payments/)).toBeInTheDocument();
    expect(screen.getByText("payments: выбран")).toBeInTheDocument();
    expect(previewRouting).toHaveBeenCalledWith(expect.stringContaining('"source": "nginx"'), expect.objectContaining({ routing: expect.objectContaining({ enabled: true }) }));
    expect(saveStream).not.toHaveBeenCalled(); expect(sendStreamMessage).not.toHaveBeenCalled();
  });
  it("rejects invalid sample JSON locally and keeps saved conditions visible", async () => {
    const user = userEvent.setup();
    vi.mocked(fetchStreams).mockResolvedValue({ streams: [{ ...stream, routing: { enabled: true, priority: 20, match: "all", rules: [{ field: "source", operator: "equals", value: "billing" }] } }] });
    render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Идентификатор");
    expect(screen.getByText('source = "billing"')).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Пример JSON-сообщения"));
    await user.type(screen.getByLabelText("Пример JSON-сообщения"), "invalid");
    await user.click(screen.getByRole("button", { name: "Проверить маршрутизацию" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Введите корректное JSON-сообщение.");
    expect(previewRouting).not.toHaveBeenCalled();
  });
  it("loads saved settings and clearly leaves deletion disabled", async () => {
    render(<StreamsSettings onSaved={vi.fn()} />);
    expect(await screen.findByLabelText("Идентификатор")).toBeDisabled();
    expect(screen.getByLabelText("Идентификатор")).toHaveValue("payments");
    expect(screen.getByLabelText("Хранить после ротации (дни)")).toHaveValue(0);
    expect(screen.getByText(/Ранее сохранённые логи доступны в поиске/)).toBeInTheDocument();
    await screen.findByText(/Индексов пока нет/);
  });
  it("creates a stream with routing and storage settings and updates the search list", async () => {
    const user = userEvent.setup(); const onSaved = vi.fn();
    render(<StreamsSettings onSaved={onSaved} />);
    await screen.findByLabelText("Идентификатор");
    await user.click(screen.getByRole("button", { name: "Создать поток" }));
    await user.type(screen.getByLabelText("Идентификатор"), "auth");
    await user.type(screen.getByLabelText("Название"), "Authentication");
    await user.click(screen.getByLabelText("Включить правила маршрутизации"));
    await user.click(screen.getByRole("button", { name: "Добавить условие" }));
    await user.type(screen.getByLabelText("Условие 1 значение"), "auth-service");
    await user.clear(screen.getByLabelText("Максимальное число сообщений"));
    await user.type(screen.getByLabelText("Максимальное число сообщений"), "500");
    await user.type(screen.getByLabelText("Хранить после ротации (дни)"), "7");
    await user.click(screen.getByRole("button", { name: "Создать поток с настройками" }));
    await screen.findByText(/Настройки потока сохранены/);
    expect(saveStream).toHaveBeenCalledWith(expect.objectContaining({ name: "auth", routing: expect.objectContaining({ enabled: true, rules: [{ field: "source", operator: "equals", value: "auth-service" }] }), storage: expect.objectContaining({ max_documents: 500, retention_days: 7 }) }), true);
    expect(onSaved).toHaveBeenCalledWith(expect.arrayContaining([expect.objectContaining({ name: "auth" })]));
  });
  it("preserves the draft when the server rejects settings", async () => {
    const user = userEvent.setup();
    vi.mocked(saveStream).mockRejectedValue(new Error("Enable at least one rotation threshold."));
    render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Название");
    await user.clear(screen.getByLabelText("Название")); await user.type(screen.getByLabelText("Название"), "Updated payments");
    await user.click(screen.getByRole("button", { name: "Сохранить настройки" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Enable at least one rotation threshold.");
    expect(screen.getByLabelText("Название")).toHaveValue("Updated payments");
  });
  it("rotates the selected stream without creating an empty successor", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByRole("button", { name: "Выполнить ротацию" });
    await user.click(screen.getByRole("button", { name: "Выполнить ротацию" }));
    expect(await screen.findByText(/Следующее сообщение создаст новый индекс/)).toBeInTheDocument();
    expect(rotateStream).toHaveBeenCalledWith("payments");
  });
  it("reuses the event identity when retrying a message after a missing response", async () => {
    const user = userEvent.setup();
    vi.mocked(sendStreamMessage).mockRejectedValueOnce(new Error("Connection lost"));
    render(<StreamsSettings onSaved={vi.fn()} />);
    const form = await screen.findByRole("form", { name: "Отправить сообщение" });
    await user.type(within(form).getByLabelText("Сообщение"), "payment accepted");
    await user.click(within(form).getByRole("button", { name: "Отправить сообщение" }));
    await screen.findByRole("alert");
    await user.click(within(form).getByRole("button", { name: "Отправить сообщение" }));
    await screen.findByText(/Сообщение принято/);
    expect(sendStreamMessage).toHaveBeenCalledTimes(2);
    expect(vi.mocked(sendStreamMessage).mock.calls[0]).toEqual(vi.mocked(sendStreamMessage).mock.calls[1]);
    expect(vi.mocked(sendStreamMessage).mock.calls[0][0]).toBe("payments");
  });
  it("ignores index responses from a stream that is no longer selected", async () => {
    const user = userEvent.setup();
    vi.mocked(fetchStreams).mockResolvedValue({ streams: [stream, { ...stream, id: "auth-id", name: "auth", title: "Authentication" }] });
    let resolveOld!: (result: { indices: [] }) => void;
    vi.mocked(fetchStreamIndices).mockReturnValueOnce(new Promise(resolve => { resolveOld = resolve; }));
    render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Идентификатор");
    await user.click(screen.getByRole("button", { name: "Authentication auth" }));
    await screen.findByText(/Индексов пока нет/);
    await act(async () => resolveOld({ indices: [] }));
    expect(screen.getByLabelText("Идентификатор")).toHaveValue("auth");
    await waitFor(() => expect(fetchStreamIndices).toHaveBeenLastCalledWith("auth", expect.any(AbortSignal)));
  });
});

it("requires the stream identifier before deleting and exposes restoration",async()=>{
 const user=userEvent.setup();const saved=vi.fn();render(<StreamsSettings onSaved={saved}/>);await screen.findByLabelText("Идентификатор");
 await user.click(screen.getByRole("button",{name:"Удалить поток"}));
 const dialog=screen.getByRole("dialog");expect(within(dialog).getByRole("button",{name:"Подтвердить удаление"})).toBeDisabled();
 await user.type(within(dialog).getByLabelText("Введите идентификатор потока"),"payments");
 vi.mocked(deleteStream).mockResolvedValue({deleted:true});vi.mocked(fetchStreams).mockResolvedValue({streams:[]});
 await user.click(within(dialog).getByRole("button",{name:"Подтвердить удаление"}));
 await waitFor(()=>expect(deleteStream).toHaveBeenCalledWith("payments"));await waitFor(()=>expect(saved).toHaveBeenCalledWith([]));
 expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
 vi.mocked(fetchDeletedStreams).mockResolvedValue({streams:[stream]});await user.click(screen.getByRole("button",{name:"Удалённые потоки"}));
 vi.mocked(restoreStream).mockResolvedValue({deleted:false});vi.mocked(fetchStreams).mockResolvedValue({streams:[stream]});
 await user.click(await screen.findByRole("button",{name:"Восстановить Payments"}));
 await waitFor(()=>expect(restoreStream).toHaveBeenCalledWith("payments"));await waitFor(()=>expect(saved).toHaveBeenLastCalledWith([stream]));
});
it("protects the default stream from deletion",async()=>{
 vi.mocked(fetchStreams).mockResolvedValue({streams:[{...stream,name:"all"}]});render(<StreamsSettings onSaved={vi.fn()}/>);await screen.findByLabelText("Идентификатор");
 expect(screen.queryByRole("button",{name:"Удалить поток"})).not.toBeInTheDocument();expect(screen.getByText("Поток по умолчанию нельзя удалить.")).toBeInTheDocument();
});
it("copies the exact identifier and accepts pasting it without deleting automatically",async()=>{
 const user=userEvent.setup();render(<StreamsSettings onSaved={vi.fn()}/>);await screen.findByLabelText("Идентификатор");
 await user.click(screen.getByRole("button",{name:"Удалить поток"}));
 const dialog=screen.getByRole("dialog");
 expect(within(dialog).getByLabelText("Идентификатор для копирования")).toHaveValue("payments");
 await user.click(within(dialog).getByRole("button",{name:"Скопировать"}));
 expect(await within(dialog).findByRole("button",{name:"Скопировано"})).toBeInTheDocument();
 expect(within(dialog).getByRole("button",{name:"Подтвердить удаление"})).toBeDisabled();
 await user.click(within(dialog).getByLabelText("Введите идентификатор потока"));await user.paste();
 expect(within(dialog).getByLabelText("Введите идентификатор потока")).toHaveValue("payments");
 expect(within(dialog).getByRole("button",{name:"Подтвердить удаление"})).toBeEnabled();
 expect(deleteStream).not.toHaveBeenCalled();
});
