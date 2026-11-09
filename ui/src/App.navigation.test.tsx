vi.mock("./KafkaIndicator", () => ({ default: () => <div /> }));
vi.mock("./IngestIndicator", () => ({ default: () => <div /> }));
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import App from "./App";
import type { Identity } from "./api";
vi.mock("./StreamsSettings", () => ({ default: () => <div>Stream settings page</div> }));
vi.mock("./Monitoring", () => ({ default: () => <div>Monitoring page</div> }));
vi.mock("./LDAPSettings", () => ({ default: () => <div>LDAP settings page</div> }));
vi.mock("./AccessSettings", () => ({ default: () => <div>Access settings page</div> }));
vi.mock("./api", () => ({ fetchHealth: vi.fn(async () => ({ mongo: true, opensearch: true })), fetchStreams: vi.fn(async () => ({ streams: [] })), searchLogs: vi.fn(async () => ({ messages: [], total: 0, took_ms: 0 })), closeSearchSession: vi.fn(async () => {}) }));
const admin: Identity = { enabled: true, username: "administrator", user_id: "admin-uuid", administrator: true, permissions: ["messages:search"] };
beforeEach(() => window.history.replaceState(null, "", "/"));
it("restores the current section after remounting and records navigation in the URL", async () => {
 const user = userEvent.setup();
 const app = render(<App identity={admin} />);
 await user.click(screen.getByRole("button", { name: "Доступ" }));
 expect(window.location.hash).toBe("#/ldap");
 expect(screen.getByText("LDAP settings page")).toBeInTheDocument();
 await user.click(screen.getByRole("tab", { name: "Назначения" }));
 expect(window.location.hash).toBe("#/access");
 app.unmount();
 render(<App identity={admin} />);
 expect(screen.getByText("Access settings page")).toBeInTheDocument();
 expect(screen.getByRole("button", { name: "Доступ" })).toHaveAttribute("aria-pressed", "true");
 expect(screen.getByRole("tab", { name: "Назначения" })).toHaveAttribute("aria-selected", "true");
 await user.click(screen.getByRole("button", { name: "Потоки и хранение" }));
 expect(window.location.hash).toBe("#/streams");
});
it("opens stream settings directly and responds to browser navigation", async () => {
 window.history.replaceState(null, "", "/#/streams");render(<App identity={admin} />);
 expect(screen.getByText("Stream settings page")).toBeInTheDocument();
 act(() => { window.location.hash = "#/access"; window.dispatchEvent(new HashChangeEvent("hashchange")); });
 await waitFor(() => expect(screen.getByText("Access settings page")).toBeInTheDocument());
});
it("falls back to messages for unauthorized and unknown sections", () => {
 window.history.replaceState(null, "", "/#/access");
 const app = render(<App identity={{ ...admin, administrator: false }} />);
 expect(screen.queryByText("Access settings page")).not.toBeInTheDocument();
 expect(window.location.hash).toBe("#/messages");
 app.unmount();window.history.replaceState(null, "", "/#/unknown");
 render(<App identity={admin} />);
 expect(window.location.hash).toBe("#/messages");
});
it("leaves an admin section when the current account loses its permissions", () => {
 window.history.replaceState(null, "", "/#/access");
 const app = render(<App identity={admin} />);
 app.rerender(<App identity={{ ...admin, administrator: false }} />);
 expect(screen.queryByText("Access settings page")).not.toBeInTheDocument();
 expect(window.location.hash).toBe("#/messages");
});

it("restores LDAP settings only for an administrator", () => {
 window.history.replaceState(null,"","/#/ldap");
 const app=render(<App identity={admin} />);
 expect(screen.getByText("LDAP settings page")).toBeInTheDocument();
 expect(screen.getByRole("button", { name: "Доступ" })).toHaveAttribute("aria-pressed", "true");
 expect(screen.getByRole("tab", { name: "Каталог" })).toHaveAttribute("aria-selected", "true");
 app.rerender(<App identity={{...admin,administrator:false}} />);
 expect(screen.queryByText("LDAP settings page")).not.toBeInTheDocument();
 expect(screen.queryByRole("button",{name:"Доступ"})).not.toBeInTheDocument();
 expect(window.location.hash).toBe("#/messages");
});

it("restricts monitoring navigation and direct hashes to administrators", async () => {
 window.history.replaceState(null,"","/#/monitoring");
 const app=render(<App identity={admin} />);
 expect(screen.getByText("Monitoring page")).toBeInTheDocument();
 expect(screen.getByRole("button",{name:"Мониторинг"})).toHaveAttribute("aria-pressed","true");
 app.rerender(<App identity={{...admin,administrator:false}} />);
 expect(screen.queryByText("Monitoring page")).not.toBeInTheDocument();
 expect(screen.queryByRole("button",{name:"Мониторинг"})).not.toBeInTheDocument();
 expect(window.location.hash).toBe("#/messages");
});
