import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import MessageDetails from "./MessageDetails";
import { parseMessageFields, prettyMessageJSON } from "./messageFields";
import type { Message } from "./api";

const message: Message = {
  id: "test", timestamp: "2026-11-03T00:00:00Z", message: "Request failed",
  source: "demo", host: "test-host", level: "error", stream: "new-stream",
  payload_json: '{"app":"123","env":"test1","request":{"id":"req-1"},"response":{"status":503},"tags":["demo","test1"],"nil":null,"enabled":false,"large":900719925474099312345,"exponent":1e999999}',
};

describe("message field viewer", () => {
  it("opens field actions by keyboard, closes on Escape and adds an indexed value", async () => {
    const user = userEvent.setup(); const onSearch = vi.fn();
    render(<MessageDetails message={{ ...message, payload_json: undefined }} onClose={vi.fn()} onSearch={onSearch} />);
    const trigger = screen.getByRole("button", { name: "Actions for field host" });
    vi.spyOn(trigger, "getBoundingClientRect").mockReturnValue(new DOMRect(20, 50, 24, 24));
    await user.click(trigger);
    expect(screen.getByRole("menu")).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: /Добавить в поиск/ })).toHaveFocus();
    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("menuitem", { name: /Исключить из поиска/ })).toHaveFocus();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument(); expect(trigger).toHaveFocus();
    await user.click(trigger);
    await user.click(screen.getByRole("menuitem", { name: /Исключить из поиска/ }));
    expect(onSearch).toHaveBeenCalledWith('host:"test\\-host"', true);
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Нажмите Search");
  });
  it("explains why a raw unindexed field cannot be added instead of producing a false filter", async () => {
    const user = userEvent.setup(); render(<MessageDetails message={message} onClose={vi.fn()} onSearch={vi.fn()} />);
    const trigger = screen.getByRole("button", { name: "Actions for field app" });
    vi.spyOn(trigger, "getBoundingClientRect").mockReturnValue(new DOMRect(20, 50, 24, 24));
    await user.click(trigger);
    expect(screen.getByRole("menuitem", { name: /Добавить в поиск/ })).toBeDisabled();
    expect(screen.getByRole("menu")).toHaveTextContent("не входит в поисковый индекс");
    await user.click(screen.getByRole("searchbox", { name: "Filter fields" }));
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });
  it("preserves numeric literals and distinguishes literal dotted keys from nested paths", () => {
    const root = parseMessageFields('{"a.b":900719925474099312345,"a":{"b":1e999999},"text":"brace } and \\\"quote\\\"","empty":[],"nil":null}');
    expect(root.children?.[0].path).toBe('["a.b"]');
    expect(root.children?.[1].children?.[0].path).toBe("a.b");
    const pretty = prettyMessageJSON(root);
    expect(pretty).toContain("900719925474099312345");
    expect(pretty).toContain("1e999999");
    expect(prettyMessageJSON(parseMessageFields(pretty))).toBe(pretty);
    expect(() => parseMessageFields('{"trailing":1,}')).toThrow();
  });
  it("shows typed nested fields, filters them and keeps exact numbers in JSON", async () => {
    const user = userEvent.setup(); render(<MessageDetails message={message} onClose={vi.fn()} />);
    const fields = screen.getByRole("tabpanel");
    expect(within(fields).getByText("app", { exact: true })).toBeInTheDocument();
    expect(within(fields).getByText("503", { exact: true })).toBeInTheDocument();
    expect(within(fields).getByText("null", { selector: ".field-value" })).toBeInTheDocument();
    expect(within(fields).getByText("false")).toBeInTheDocument();
    await user.type(screen.getByRole("searchbox", { name: "Filter fields" }), "response.status");
    expect(within(fields).getByText("503")).toBeInTheDocument();
    expect(within(fields).queryByText("app")).not.toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "JSON" }));
    expect(screen.getByRole("tabpanel")).toHaveTextContent("900719925474099312345");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("1e999999");
    await user.keyboard("{ArrowLeft}");
    expect(screen.getByRole("tab", { name: "Fields" })).toHaveFocus();
  });
  it("shows old records without original JSON using their saved fields", async () => {
    const close = vi.fn();
    render(<MessageDetails message={{ ...message, payload_json: undefined, fields: { legacy: "old value" } }} onClose={close} />);
    expect(screen.getByText("old value")).toBeInTheDocument();
    expect(screen.getByText("test-host", { exact: true })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Close message details" }));
    expect(close).toHaveBeenCalledOnce();
  });
});
