import { expect, it } from "vitest";
import { apiError } from "./locale";
it("translates login, permission and LDAP errors while retaining HTTP status", () => {
 expect(apiError("Sign in to continue.",401)).toMatchObject({message:"Войдите, чтобы продолжить.",status:401});
 expect(apiError("Your role does not permit this action.",403).message).toContain("не разрешает");
 expect(apiError("Cannot read the user search base. Check the DN and reader permissions.",422).message).toContain("База поиска пользователей");
});
it("provides a readable fallback without displaying unrecognized server diagnostics", () => {
 expect(apiError("internal-private-stack-trace",503).message).toBe("Не удалось выполнить запрос (HTTP 503). Повторите попытку.");
 expect(apiError("Уже русское сообщение",400).message).toBe("Уже русское сообщение");
});

it("localizes the built-in fallback label without changing user-provided titles or IDs", async () => {
 const {streamDisplayTitle}=await import("./locale");
 const builtin={name:"all",title:"Unmatched messages"};
 expect(streamDisplayTitle(builtin)).toBe("Сообщения без совпавших правил");
 expect(builtin).toEqual({name:"all",title:"Unmatched messages"});
 expect(streamDisplayTitle({name:"billing",title:"Billing"})).toBe("Billing");
});
