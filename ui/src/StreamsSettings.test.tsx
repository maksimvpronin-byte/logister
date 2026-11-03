import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import StreamsSettings from "./StreamsSettings";
import { fetchStreams, fetchStreamIndices, saveStream, rotateStream, sendStreamMessage, previewRouting, type Stream } from "./api";

vi.mock("./api", () => ({ fetchStreams: vi.fn(), fetchStreamIndices: vi.fn(), saveStream: vi.fn(), rotateStream: vi.fn(), sendStreamMessage: vi.fn(), previewRouting: vi.fn() }));
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
    await screen.findByLabelText("Name");
    await user.click(screen.getByRole("button", { name: "Create stream" }));
    await user.type(screen.getByLabelText("Title"), "New stream");
    expect(screen.getByLabelText("Name")).toHaveValue("new-stream");
    await user.clear(screen.getByLabelText("Name"));
    await user.type(screen.getByLabelText("Name"), "Custom Stream");
    expect(screen.getByLabelText("Name")).toHaveValue("custom-stream");
    await user.type(screen.getByLabelText("Title"), " title");
    expect(screen.getByLabelText("Name")).toHaveValue("custom-stream");
    await user.click(screen.getByRole("button", { name: "Create stream settings" }));
    await screen.findByText(/Stream settings saved/);
    expect(saveStream).toHaveBeenCalledWith(expect.objectContaining({ name: "custom-stream", title: "New stream title" }), true);
  });
  it("adds a condition from any field found in the sample", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Name");
    await user.clear(screen.getByLabelText("Sample JSON message"));
    await user.paste('{"custom":{"items":[{"code":503}]},"tenant":"acme"}');
    await user.click(screen.getByRole("button", { name: "Use field custom.items[0].code" }));
    expect(screen.getByLabelText("Rule 1 field")).toHaveValue("custom.items[0].code");
    expect(screen.getByLabelText("Rule 1 operator")).toHaveValue("exists");
    expect(screen.getByLabelText("Rule 1 value")).toBeDisabled();
    expect(screen.getByLabelText("Enable routing rules")).toBeChecked();
  });
  it("explains routing against the unsaved draft without saving or ingesting", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Name");
    await user.click(screen.getByLabelText("Enable routing rules"));
    await user.click(screen.getByRole("button", { name: "Add rule" }));
    await user.type(screen.getByLabelText("Rule 1 value"), "nginx");
    await user.click(screen.getByRole("button", { name: "Test routing" }));
    expect(await screen.findByText(/Destination: Payments/)).toBeInTheDocument();
    expect(screen.getByText("payments: selected")).toBeInTheDocument();
    expect(previewRouting).toHaveBeenCalledWith(expect.stringContaining('"source": "nginx"'), expect.objectContaining({ routing: expect.objectContaining({ enabled: true }) }));
    expect(saveStream).not.toHaveBeenCalled(); expect(sendStreamMessage).not.toHaveBeenCalled();
  });
  it("rejects invalid sample JSON locally and keeps saved conditions visible", async () => {
    const user = userEvent.setup();
    vi.mocked(fetchStreams).mockResolvedValue({ streams: [{ ...stream, routing: { enabled: true, priority: 20, match: "all", rules: [{ field: "source", operator: "equals", value: "billing" }] } }] });
    render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Name");
    expect(screen.getByText('source = "billing"')).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Sample JSON message"));
    await user.type(screen.getByLabelText("Sample JSON message"), "invalid");
    await user.click(screen.getByRole("button", { name: "Test routing" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Enter a valid JSON message.");
    expect(previewRouting).not.toHaveBeenCalled();
  });
  it("loads saved settings and clearly leaves deletion disabled", async () => {
    render(<StreamsSettings onSaved={vi.fn()} />);
    expect(await screen.findByLabelText("Name")).toBeDisabled();
    expect(screen.getByLabelText("Name")).toHaveValue("payments");
    expect(screen.getByLabelText("Retention after rotation (days)")).toHaveValue(0);
    expect(screen.getByText(/Logs stored before this feature remain searchable/)).toBeInTheDocument();
    await screen.findByText(/No managed indices yet/);
  });
  it("creates a stream with routing and storage settings and updates the search list", async () => {
    const user = userEvent.setup(); const onSaved = vi.fn();
    render(<StreamsSettings onSaved={onSaved} />);
    await screen.findByLabelText("Name");
    await user.click(screen.getByRole("button", { name: "Create stream" }));
    await user.type(screen.getByLabelText("Name"), "auth");
    await user.type(screen.getByLabelText("Title"), "Authentication");
    await user.click(screen.getByLabelText("Enable routing rules"));
    await user.click(screen.getByRole("button", { name: "Add rule" }));
    await user.type(screen.getByLabelText("Rule 1 value"), "auth-service");
    await user.clear(screen.getByLabelText("Maximum messages"));
    await user.type(screen.getByLabelText("Maximum messages"), "500");
    await user.type(screen.getByLabelText("Retention after rotation (days)"), "7");
    await user.click(screen.getByRole("button", { name: "Create stream settings" }));
    await screen.findByText(/Stream settings saved/);
    expect(saveStream).toHaveBeenCalledWith(expect.objectContaining({ name: "auth", routing: expect.objectContaining({ enabled: true, rules: [{ field: "source", operator: "equals", value: "auth-service" }] }), storage: expect.objectContaining({ max_documents: 500, retention_days: 7 }) }), true);
    expect(onSaved).toHaveBeenCalledWith(expect.arrayContaining([expect.objectContaining({ name: "auth" })]));
  });
  it("preserves the draft when the server rejects settings", async () => {
    const user = userEvent.setup();
    vi.mocked(saveStream).mockRejectedValue(new Error("Enable at least one rotation threshold."));
    render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByLabelText("Title");
    await user.clear(screen.getByLabelText("Title")); await user.type(screen.getByLabelText("Title"), "Updated payments");
    await user.click(screen.getByRole("button", { name: "Save settings" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Enable at least one rotation threshold.");
    expect(screen.getByLabelText("Title")).toHaveValue("Updated payments");
  });
  it("rotates the selected stream without creating an empty successor", async () => {
    const user = userEvent.setup(); render(<StreamsSettings onSaved={vi.fn()} />);
    await screen.findByRole("button", { name: "Rotate now" });
    await user.click(screen.getByRole("button", { name: "Rotate now" }));
    expect(await screen.findByText(/The next message starts a new index/)).toBeInTheDocument();
    expect(rotateStream).toHaveBeenCalledWith("payments");
  });
  it("reuses the event identity when retrying a message after a missing response", async () => {
    const user = userEvent.setup();
    vi.mocked(sendStreamMessage).mockRejectedValueOnce(new Error("Connection lost"));
    render(<StreamsSettings onSaved={vi.fn()} />);
    const form = await screen.findByRole("form", { name: "Send message" });
    await user.type(within(form).getByLabelText("Message"), "payment accepted");
    await user.click(within(form).getByRole("button", { name: "Send message" }));
    await screen.findByRole("alert");
    await user.click(within(form).getByRole("button", { name: "Send message" }));
    await screen.findByText(/Message accepted/);
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
    await screen.findByLabelText("Name");
    await user.click(screen.getByRole("button", { name: "Authentication auth" }));
    await screen.findByText(/No managed indices yet/);
    await act(async () => resolveOld({ indices: [] }));
    expect(screen.getByLabelText("Name")).toHaveValue("auth");
    await waitFor(() => expect(fetchStreamIndices).toHaveBeenLastCalledWith("auth", expect.any(AbortSignal)));
  });
});
