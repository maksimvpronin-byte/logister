function pad(value: number) { return String(value).padStart(2, "0"); }

export function localDateTime(value: Date | string): string {
  const date = typeof value === "string" ? new Date(value) : value;
  return `${pad(date.getDate())}.${pad(date.getMonth() + 1)}.${String(date.getFullYear()).padStart(4, "0")} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function parseLocal(value: string, label: string): Date {
  value = value.trim();
  if (!value) throw new Error(`Choose the ${label} date and time.`);
  const display = /^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
  if (display) value = `${display[3]}-${display[2]}-${display[1]}T${display[4]}:${display[5]}:${display[6] ?? "00"}`;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
  const date = new Date(value);
  // Date can normalize invalid calendar dates or nonexistent local DST times.
  if (!match || !Number.isFinite(date.getTime())
    || date.getFullYear() !== Number(match[1]) || date.getMonth() + 1 !== Number(match[2])
    || date.getDate() !== Number(match[3]) || date.getHours() !== Number(match[4])
    || date.getMinutes() !== Number(match[5]) || date.getSeconds() !== Number(match[6] ?? 0)) {
    throw new Error(`Enter a valid ${label} date and time (DD.MM.YYYY HH:mm:ss) in your local time zone.`);
  }
  return date;
}

export function absoluteTimeRange(from: string, to: string): { from: string; to: string } {
  const start = parseLocal(from, "start");
  const end = parseLocal(to, "end");
  if (start >= end) throw new Error("Start must be earlier than end.");
  return { from: start.toISOString(), to: end.toISOString() };
}

export function formatRangeTime(value: string): string {
  return new Date(value).toLocaleString(undefined, {
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit",
    minute: "2-digit", second: "2-digit", timeZoneName: "short",
  });
}
