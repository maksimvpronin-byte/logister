import { describe, expect, it } from "vitest";
import { absoluteTimeRange, localDateTime } from "./timeRange";

describe("absolute search bounds", () => {
  it("converts local calendar values to UTC without interpreting them as UTC", () => {
    const start = new Date(2026, 10, 3, 3, 15, 20);
    const end = new Date(2026, 10, 3, 4, 20, 30);
    expect(localDateTime(start)).toBe("03.11.2026 03:15:20");
    expect(absoluteTimeRange(localDateTime(start), localDateTime(end))).toEqual({ from: start.toISOString(), to: end.toISOString() });
  });
  it("accepts readable dates, optional seconds and pasted local ISO timestamps", () => {
    const expected = { from: new Date(2026, 10, 3, 3, 15).toISOString(), to: new Date(2026, 10, 3, 4, 20, 30).toISOString() };
    expect(absoluteTimeRange(" 03.11.2026 03:15 ", "03.11.2026 04:20:30")).toEqual(expected);
    expect(absoluteTimeRange("2026-11-03T03:15", "2026-11-03T04:20:30")).toEqual(expected);
  });
  it.each([
    ["", "2026-11-03T04:00", "Укажите дату и время начала"],
    ["2026-11-03T03:00", "", "Укажите дату и время окончания"],
    ["2026-02-30T03:00", "2026-03-03T04:00", "корректные дату и время начала"],
    ["30.02.2026 03:00", "03.03.2026 04:00", "корректные дату и время начала"],
    ["   ", "03.11.2026 04:00", "Укажите дату и время начала"],
    ["invalid", "2026-11-03T04:00", "корректные дату и время начала"],
    ["2026-11-03T04:00", "2026-11-03T03:00", "раньше окончания"],
    ["2026-11-03T03:00", "2026-11-03T03:00", "раньше окончания"],
  ])("rejects invalid range %s to %s", (from, to, error) => {
    expect(() => absoluteTimeRange(from, to)).toThrow(error);
  });
});
