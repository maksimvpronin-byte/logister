import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, it, vi } from "vitest";
import LDAPSettings from "./LDAPSettings";
import { fetchLDAPSettings, testLDAPSettings, saveLDAPSettings } from "./api";
vi.mock("./api", () => ({ fetchLDAPSettings: vi.fn(), testLDAPSettings: vi.fn(), saveLDAPSettings: vi.fn() }));
const settings = { url: "ldap://ldap:389", bind_dn: "uid=reader,dc=test", users_dn: "ou=people,dc=test", groups_dn: "ou=groups,dc=test", password_set: true, revision: 3 };
beforeEach(() => { vi.resetAllMocks(); vi.mocked(fetchLDAPSettings).mockResolvedValue(settings); vi.mocked(testLDAPSettings).mockResolvedValue({users_found:true,groups_found:true}); vi.mocked(saveLDAPSettings).mockResolvedValue({...settings,revision:4}); });
it("loads saved fields without retrieving or showing the password", async () => {
 render(<LDAPSettings />);
 expect(await screen.findByLabelText("Адрес сервера")).toHaveValue(settings.url);
 expect(screen.getByLabelText("Пароль технической учётной записи")).toHaveValue("");
 expect(screen.getByRole("button",{name:"Сохранить настройки"})).toBeDisabled();
 expect(screen.getByPlaceholderText(/Пароль сохранён/)).toBeInTheDocument();
});
it("tests the draft without applying it and clears a stale test result after editing", async () => {
 const user=userEvent.setup();render(<LDAPSettings />);await screen.findByLabelText("Адрес сервера");
 await user.clear(screen.getByLabelText("База поиска пользователей (Users DN)"));await user.type(screen.getByLabelText("База поиска пользователей (Users DN)"),"ou=newpeople,dc=test");
 await user.click(screen.getByRole("button",{name:"Проверить подключение"}));
 expect(await screen.findByText("Подключение проверено")).toBeInTheDocument();expect(saveLDAPSettings).not.toHaveBeenCalled();
 expect(testLDAPSettings).toHaveBeenCalledWith(expect.objectContaining({users_dn:"ou=newpeople,dc=test",bind_password:"",revision:3}));
 await user.type(screen.getByLabelText("База поиска пользователей (Users DN)"),"x");expect(screen.queryByText("Подключение проверено")).not.toBeInTheDocument();
});
it("saves once with the current revision, preserving a blank password, then clears dirty state", async () => {
 const user=userEvent.setup();render(<LDAPSettings />);await screen.findByLabelText("Адрес сервера");
 await user.clear(screen.getByLabelText("База поиска пользователей (Users DN)"));await user.type(screen.getByLabelText("База поиска пользователей (Users DN)"),"ou=newpeople,dc=test");
 await user.click(screen.getByRole("button",{name:"Сохранить настройки"}));
 await waitFor(()=>expect(saveLDAPSettings).toHaveBeenCalledWith(expect.objectContaining({users_dn:"ou=newpeople,dc=test",bind_password:"",revision:3})));
 expect(await screen.findByText(/Настройки LDAP сохранены/)).toBeInTheDocument();expect(screen.getByRole("button",{name:"Сохранить настройки"})).toBeDisabled();
});
it("retains the draft and hides no errors on a failed or conflicting save", async () => {
 vi.mocked(saveLDAPSettings).mockRejectedValue(new Error("LDAP settings changed; reload before saving"));
 const user=userEvent.setup();render(<LDAPSettings />);await screen.findByLabelText("Адрес сервера");
 await user.type(screen.getByLabelText("Пароль технической учётной записи"),"new-secret");await user.click(screen.getByRole("button",{name:"Сохранить настройки"}));
 expect(await screen.findByRole("alert")).toHaveTextContent("reload before saving");expect(screen.getByLabelText("Пароль технической учётной записи")).toHaveValue("new-secret");
 expect(screen.getByRole("button",{name:"Отменить изменения и загрузить заново"})).toBeInTheDocument();
});
it("requires a service password for initial setup", async () => {
 vi.mocked(fetchLDAPSettings).mockResolvedValue({...settings,password_set:false});render(<LDAPSettings />);await screen.findByLabelText("Адрес сервера");
 expect(screen.getByRole("button",{name:"Проверить подключение"})).toBeDisabled();
});
