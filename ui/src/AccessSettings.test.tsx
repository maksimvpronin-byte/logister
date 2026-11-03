import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import AccessSettings from "./AccessSettings";
import { fetchDirectory, fetchGrants, fetchStreams, saveGrants, type GrantCatalog } from "./api";
vi.mock("./api", () => ({ fetchDirectory: vi.fn(), fetchGrants: vi.fn(), fetchStreams: vi.fn(), saveGrants: vi.fn() }));
const local = { principal_kind: "user", principal_id: "local-admin", role: "administrator", scope: "system" } as const;
const mapping = { principal_kind: "group", principal_id: "group-uuid", role: "operator", scope: "stream", stream_id: "payments-uuid" } as const;
const catalog: GrantCatalog = { revision: 4, grants: [local, mapping] };
beforeEach(() => {
 vi.resetAllMocks();
 HTMLDialogElement.prototype.showModal = function () { this.setAttribute("open", ""); };
 HTMLDialogElement.prototype.close = function () { this.removeAttribute("open"); };
 vi.mocked(fetchGrants).mockResolvedValue(catalog);
 vi.mocked(fetchStreams).mockResolvedValue({ streams: [{ id: "payments-uuid", name: "payments", title: "Payments", description: "" }, { id: "billing-uuid", name: "billing", title: "Billing", description: "" }] });
 vi.mocked(fetchDirectory).mockResolvedValue({ entries: [{ id: "group-uuid", kind: "group", name: "Operators", dn: "cn=Operators,dc=test" }, { id: "user-uuid", kind: "user", name: "Test user", dn: "uid=test,dc=test" }] });
 vi.mocked(saveGrants).mockResolvedValue({});
});
it("edits a row in place and increments the revision for subsequent saves", async () => {
 const user=userEvent.setup(); render(<AccessSettings />);
 await user.click(await screen.findByRole("button",{name:"Изменить доступ: Operators → Payments"}));
 expect(screen.getByLabelText("Поток")).toHaveValue("payments-uuid");
 expect(screen.getByRole("button",{name:"Сохранить изменения"})).toBeDisabled();
 await user.selectOptions(screen.getByLabelText("Поток"),"billing-uuid");
 await user.click(screen.getByRole("button",{name:"Сохранить изменения"}));
 await waitFor(()=>expect(saveGrants).toHaveBeenCalledWith({revision:4,grants:[local,{...mapping,stream_id:"billing-uuid"}]}));
 await user.click(await screen.findByRole("button",{name:"Изменить доступ: Operators → Billing"}));
 await user.selectOptions(screen.getByLabelText("Поток"),"payments-uuid");
 await user.click(screen.getByRole("button",{name:"Сохранить изменения"}));
 await waitFor(()=>expect(saveGrants).toHaveBeenLastCalledWith({revision:5,grants:[local,mapping]}));
});
it("cancels edits without saving", async () => {
 const user=userEvent.setup();render(<AccessSettings />);
 await user.click(await screen.findByRole("button",{name:"Изменить доступ: Operators → Payments"}));
 await user.selectOptions(screen.getByLabelText("Поток"),"billing-uuid");
 await user.click(screen.getByRole("button",{name:"Отмена"}));
 expect(saveGrants).not.toHaveBeenCalled();expect(screen.getByRole("button",{name:"Изменить доступ: Operators → Payments"})).toBeInTheDocument();
});
it("blocks duplicates and creates a single assignment with stable IDs", async () => {
 const user=userEvent.setup();render(<AccessSettings />);
 await user.click(await screen.findByRole("button",{name:"+ Предоставить доступ"}));
 await screen.findByRole("option",{name:"Группа: Operators"});
 await user.selectOptions(screen.getByLabelText("Пользователь или группа"),"group:group-uuid");
 expect(await screen.findByText(/Такое назначение уже существует/)).toBeInTheDocument();
 const editor=screen.getByRole("dialog",{name:"Предоставить доступ"});
 expect(within(editor).getByRole("button",{name:"Предоставить доступ"})).toBeDisabled();
 await user.selectOptions(screen.getByLabelText("Пользователь или группа"),"user:user-uuid");
 await user.click(within(editor).getByRole("button",{name:"Предоставить доступ"}));
 await waitFor(()=>expect(saveGrants).toHaveBeenCalledWith({revision:4,grants:[local,mapping,{...mapping,principal_kind:"user",principal_id:"user-uuid"}]}));
});
it("confirms removal and protects the emergency administrator", async () => {
 const user=userEvent.setup();render(<AccessSettings />);
 await screen.findByText("Защищено");expect(screen.queryByRole("button",{name:/Изменить доступ: Local/})).not.toBeInTheDocument();
 await user.click(await screen.findByRole("button",{name:"Удалить доступ: Operators → Payments"}));expect(saveGrants).not.toHaveBeenCalled();
 await user.click(screen.getByRole("button",{name:"Удалить доступ"}));
 await waitFor(()=>expect(saveGrants).toHaveBeenCalledWith({revision:4,grants:[local]}));
});
it("retains the draft on save conflict without changing the list", async () => {
 vi.mocked(saveGrants).mockRejectedValue(new Error("Role mappings changed; reload before saving"));
 const user=userEvent.setup();render(<AccessSettings />);
 await user.click(await screen.findByRole("button",{name:"Изменить доступ: Operators → Payments"}));
 await user.selectOptions(screen.getByLabelText("Поток"),"billing-uuid");await user.click(screen.getByRole("button",{name:"Сохранить изменения"}));
 expect(await screen.findByRole("alert")).toHaveTextContent("reload before saving");expect(screen.getByLabelText("Поток")).toHaveValue("billing-uuid");
 await user.click(screen.getByRole("button",{name:"Отмена"}));expect(screen.getByRole("button",{name:"Изменить доступ: Operators → Payments"})).toBeInTheDocument();
});
it("preserves selected names across directory searches and scopes administrators to the system", async () => {
 const user=userEvent.setup();render(<AccessSettings />);
 await user.click(await screen.findByRole("button",{name:"+ Предоставить доступ"}));await screen.findByRole("option",{name:"Группа: Operators"});
 await user.selectOptions(screen.getByLabelText("Пользователь или группа"),"group:group-uuid");vi.mocked(fetchDirectory).mockResolvedValue({entries:[]});
 await user.type(screen.getByLabelText("Найти пользователя или группу"),"other");await user.click(screen.getByRole("button",{name:"Найти в каталоге"}));
 await waitFor(()=>expect(fetchDirectory).toHaveBeenCalledWith("other"));expect(screen.getByLabelText("Пользователь или группа")).toHaveValue("group:group-uuid");
 await user.click(screen.getByRole("radio",{name:/Администратор Полный доступ/}));expect(screen.getByRole("radio",{name:"Один поток"})).toBeDisabled();expect(screen.queryByLabelText("Поток")).not.toBeInTheDocument();
 await user.click(within(screen.getByRole("dialog")).getByRole("button",{name:"Предоставить доступ"}));
 await waitFor(()=>expect(saveGrants).toHaveBeenCalledWith({revision:4,grants:[local,mapping,{principal_kind:"group",principal_id:"group-uuid",role:"administrator",scope:"system",stream_id:undefined}]}));
});
it("keeps saved assignments editable during an LDAP outage", async () => {
 vi.mocked(fetchDirectory).mockRejectedValue(new Error("offline"));render(<AccessSettings />);
 await screen.findByText(/Каталог недоступен/);expect(screen.getByRole("button",{name:"Изменить доступ: group-uuid → Payments"})).toBeEnabled();
});
