import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import AuthBoundary from "./AuthBoundary";
import { authStatus, fetchIdentity, login, logout, type Identity } from "./api";

vi.mock("./api", () => ({ authStatus: vi.fn(), fetchIdentity: vi.fn(), login: vi.fn(), logout: vi.fn() }));
vi.mock("./App", () => ({ default: ({ identity, onLogout }: { identity?: Identity; onLogout?: () => void }) => <div>Workspace {identity?.username}<button onClick={onLogout}>Выйти</button></div> }));
const identity: Identity = { enabled: true, username: "operator", user_id: "operator-uuid", administrator: false, permissions: ["messages:search", "streams:read"] };
beforeEach(() => { vi.resetAllMocks(); vi.mocked(authStatus).mockResolvedValue({ enabled: true }); vi.mocked(fetchIdentity).mockRejectedValue(new Error("Sign in to continue.")); vi.mocked(login).mockResolvedValue({ username: "operator" }); vi.mocked(logout).mockResolvedValue(undefined); });

it("requires sign-in, submits a directory login and revokes the UI session on logout", async () => {
  const user = userEvent.setup(); render(<AuthBoundary />);
  await screen.findByRole("heading", { name: "Войти" });
  await user.type(screen.getByLabelText("Логин"), "operator"); await user.type(screen.getByLabelText("Пароль"), "test-password");
  vi.mocked(fetchIdentity).mockResolvedValue(identity);
  await user.click(screen.getByRole("button", { name: "Войти" }));
  expect(login).toHaveBeenCalledWith("operator", "test-password", "ldap");
  await screen.findByText("Workspace operator");
  await user.click(screen.getByRole("button", { name: "Выйти" }));
  await screen.findByRole("heading", { name: "Войти" }); expect(logout).toHaveBeenCalledOnce();
  expect(screen.getByLabelText("Пароль")).toHaveValue("");
});
it("keeps service and unassigned accounts out of the search workspace", async () => {
  vi.mocked(fetchIdentity).mockResolvedValue({ ...identity, permissions: [] }); render(<AuthBoundary />);
  expect(await screen.findByText(/права доступа не назначены/)).toBeInTheDocument();
  expect(screen.queryByText(/Workspace/)).not.toBeInTheDocument();
});
it("does not open an anonymous workspace when the status endpoint fails", async () => {
  vi.mocked(authStatus).mockRejectedValue(new Error("Connection failed")); render(<AuthBoundary />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Connection failed");
  expect(screen.queryByText(/Workspace/)).not.toBeInTheDocument();
});
it("supports explicit development mode without authentication", async () => {
  vi.mocked(authStatus).mockResolvedValue({ enabled: false }); render(<AuthBoundary />);
  await waitFor(() => expect(screen.getByText("Workspace")).toBeInTheDocument()); expect(fetchIdentity).not.toHaveBeenCalled();
});
