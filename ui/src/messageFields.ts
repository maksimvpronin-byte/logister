// Keep JSON numbers as their original text: parsing them as JS numbers can round IDs.
export type FieldNode = {
  key: string;
  path: string;
  type: "object" | "array" | "string" | "number" | "boolean" | "null";
  text: string;
  children?: FieldNode[];
};

export function parseMessageFields(raw: string): FieldNode {
  const tokens: string[] = [];
  const lexer = /"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}\[\]:,]/g;
  let end = 0;
  for (const match of raw.matchAll(lexer)) {
    if (raw.slice(end, match.index).trim() || tokens.length >= 100000) throw new Error("Invalid or oversized JSON");
    tokens.push(match[0]); end = match.index! + match[0].length;
  }
  if (raw.slice(end).trim()) throw new Error("Invalid JSON");
  let index = 0;
  function read(key: string, path: string, depth: number): FieldNode {
    if (depth > 128) throw new Error("JSON is too deeply nested");
    const token = tokens[index++];
    if (token === "{" || token === "[") {
      const object = token === "{";
      const close = object ? "}" : "]";
      const children: FieldNode[] = [];
      if (tokens[index] !== close) {
        while (true) {
          let childKey = `[${children.length}]`;
          if (object) {
            const keyToken = tokens[index++];
            if (!keyToken?.startsWith('"')) throw new Error("Invalid object key");
            childKey = JSON.parse(keyToken) as string;
            if (tokens[index++] !== ":") throw new Error("Missing colon");
          }
          const childPath = object
            ? /^[A-Za-z_][A-Za-z0-9_-]*$/.test(childKey) ? (path ? `${path}.${childKey}` : childKey) : `${path}[${JSON.stringify(childKey)}]`
            : `${path}${childKey}`;
          children.push(read(childKey, childPath, depth + 1));
          if (tokens[index] !== ",") break;
          index++;
        }
      }
      if (tokens[index++] !== close) throw new Error("Unclosed JSON container");
      return { key, path, type: object ? "object" : "array", text: "", children };
    }
    if (token?.startsWith('"')) return { key, path, type: "string", text: JSON.parse(token) as string };
    if (token === "null") return { key, path, type: "null", text: token };
    if (token === "true" || token === "false") return { key, path, type: "boolean", text: token };
    if (token && /^-?\d/.test(token)) return { key, path, type: "number", text: token };
    throw new Error("Invalid JSON value");
  }
  const root = read("", "", 0);
  if (index !== tokens.length) throw new Error("Unexpected JSON tokens");
  return root;
}

export function prettyMessageJSON(node: FieldNode, depth = 0): string {
  if (!node.children) return node.type === "string" ? JSON.stringify(node.text) : node.text;
  const object = node.type === "object";
  const [open, close] = object ? ["{", "}"] : ["[", "]"];
  if (!node.children.length) return open + close;
  return open + "\n" + node.children.map(child => "  ".repeat(depth + 1)
    + (object ? JSON.stringify(child.key) + ": " : "") + prettyMessageJSON(child, depth + 1)).join(",\n")
    + "\n" + "  ".repeat(depth) + close;
}

export function filterMessageFields(nodes: FieldNode[], query: string): FieldNode[] {
  if (!query) return nodes;
  return nodes.flatMap(node => {
    if (`${node.path} ${node.text}`.toLowerCase().includes(query)) return [node];
    const children = node.children && filterMessageFields(node.children, query);
    return children?.length ? [{ ...node, children }] : [];
  });
}
