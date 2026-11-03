import { describe, expect, it } from "vitest";
import { sampleFields } from "./jsonFields";
describe("arbitrary field suggestions", () => {
  it("discovers nested arrays, Unicode and literal dotted keys", () => {
    const paths = sampleFields(JSON.stringify({ tenant: "acme", items: [{ code: 503 }], "a.b": true, "ключ с пробелом": null })).map(field => field.path);
    expect(paths).toEqual(expect.arrayContaining(["tenant", "items[0].code", '["a.b"]', '["ключ с пробелом"]']));
  });
  it("ignores incomplete JSON while typing and bounds large samples", () => {
    expect(sampleFields("{")).toEqual([]);
    expect(sampleFields(JSON.stringify(Object.fromEntries(Array.from({ length: 1000 }, (_, i) => [String(i), i])))).length).toBeLessThanOrEqual(256);
  });
});
