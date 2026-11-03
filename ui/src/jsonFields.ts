export function sampleFields(sample: string): { path: string; value: unknown }[] {
  let root: unknown;
  try { root = JSON.parse(sample); } catch { return []; }
  const out: { path: string; value: unknown }[] = [];
  function visit(value: unknown, path: string, depth: number) {
    if (out.length >= 256 || depth > 32) return;
    if (path) out.push({ path, value });
    if (Array.isArray(value)) {
      for (let i = 0; i < Math.min(value.length, 8); i++) visit(value[i], `${path}[${i}]`, depth + 1);
    } else if (value && typeof value === "object") {
      for (const [key, child] of Object.entries(value)) {
        const normal = /^[A-Za-z_][A-Za-z0-9_-]*$/.test(key);
        visit(child, normal ? (path ? `${path}.${key}` : key) : `${path}[${JSON.stringify(key)}]`, depth + 1);
      }
    }
  }
  visit(root, "", 0); return out;
}
