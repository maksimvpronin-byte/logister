import { useCallback, useId, useMemo, useState } from "react";
import type { Message } from "./api";
import { filterMessageFields, parseMessageFields, prettyMessageJSON, type FieldNode } from "./messageFields";
import { fieldSearch } from "./fieldSearch";
import FieldActionsMenu from "./FieldActionsMenu";

function fieldLabel(node: FieldNode) {
  return !node.path ? "$" : node.key === "" ? '""' : node.key.replace(/\u0000/g, "\\u0000");
}

type OpenFieldMenu = (node: FieldNode, anchor: HTMLButtonElement) => void;
function FieldRows({ nodes, depth = 0, filtered, onOpenMenu, activePath }: { nodes: FieldNode[]; depth?: number; filtered: boolean; onOpenMenu?: OpenFieldMenu; activePath?: string }) {
  // Large objects stay usable; more fields are available on demand, never silently dropped.
  const [limit, setLimit] = useState(200);
  return <>{nodes.slice(0, limit).map((node, index) => node.children ? (
    <details className="field-group" key={`${node.path}-${index}`} open={filtered || depth < 1}>
      <summary className="field-row" title={node.path}>
        <span className="field-key">{fieldLabel(node)}</span>
        <span className="field-type">{node.type}</span>
        <span className="field-summary">{node.children.length} {node.type === "array" ? "items" : "fields"}</span>
      </summary>
      <div className="field-children"><FieldRows nodes={node.children} depth={depth + 1} filtered={filtered} onOpenMenu={onOpenMenu} activePath={activePath} /></div>
    </details>
  ) : <div className="field-row" key={`${node.path}-${index}`} title={node.path}>
    <span className="field-key">{fieldLabel(node)}</span>
    <span className="field-type">{node.type}</span>
    <div className="field-value-wrap"><span className={`field-value field-${node.type}`}>{node.text === "" ? '""' : node.text.replace(/\u0000/g, "\\u0000")}</span>
      {onOpenMenu && <button type="button" className="field-action-button" aria-label={`Actions for field ${node.path}`} aria-haspopup="menu" aria-expanded={activePath === node.path} onClick={event => onOpenMenu(node, event.currentTarget)}><span aria-hidden="true">▾</span></button>}
    </div>
  </div>)}
    {nodes.length > limit && <button className="more-fields" type="button" onClick={() => setLimit(limit + 200)}>Show more fields ({nodes.length - limit} remaining)</button>}
  </>;
}

export default function MessageDetails({ message, onClose, onSearch }: { message: Message; onClose: () => void; onSearch?: (clause: string, exclude: boolean) => void }) {
  const [tab, setTab] = useState<"fields" | "json">("fields");
  const [filter, setFilter] = useState("");
  const [copyStatus, setCopyStatus] = useState("");
  const [fieldMenu, setFieldMenu] = useState<{ node: FieldNode; anchor: HTMLButtonElement } | null>(null);
  const [queryNotice, setQueryNotice] = useState("");
  const closeFieldMenu = useCallback(() => setFieldMenu(null), []);
  const openFieldMenu: OpenFieldMenu = (node, anchor) => setFieldMenu(current => current?.anchor === anchor ? null : { node, anchor });
  const id = useId();
  const raw = useMemo(() => message.payload_json ?? JSON.stringify(message.payload ?? {
    timestamp: message.timestamp, level: message.level, source: message.source,
    host: message.host, message: message.message, ...(message.facility ? { facility: message.facility } : {}),
    ...(message.fields ? { fields: message.fields } : {}),
  }), [message]);
  const parsed = useMemo(() => {
    try { const root = parseMessageFields(raw); return { root, pretty: prettyMessageJSON(root) }; }
    catch { return null; }
  }, [raw]);
  const nodes = useMemo(() => parsed ? filterMessageFields(parsed.root.children ?? [parsed.root], filter.trim().toLowerCase()) : [], [parsed, filter]);
  async function copy() {
    try { await navigator.clipboard.writeText(raw); setCopyStatus("JSON copied"); }
    catch { setCopyStatus("Could not copy. Select the text in the JSON tab to copy it."); }
  }
  return <section className="details message-details" aria-label="Message details">
    <div className="message-details-header">
      <div><strong>Message fields</strong><span className="message-stream">Stream <code>{message.stream}</code></span></div>
      <div className="message-details-actions"><button type="button" onClick={copy}>Copy JSON</button><button type="button" aria-label="Close message details" onClick={onClose}>Close</button></div>
    </div>
    <div className="message-details-toolbar">
      <div role="tablist" aria-label="Message format" onKeyDown={event => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault(); const next = tab === "fields" ? "json" : "fields"; setTab(next);
        document.getElementById(`${id}-${next}-tab`)?.focus();
      }}>
        {(["fields", "json"] as const).map(value => <button type="button" role="tab" key={value} id={`${id}-${value}-tab`} aria-selected={tab === value} aria-controls={`${id}-${value}`} tabIndex={tab === value ? 0 : -1} onClick={() => { setTab(value); closeFieldMenu(); }}>{value === "fields" ? "Fields" : "JSON"}</button>)}
      </div>
      {tab === "fields" && parsed && <input type="search" aria-label="Filter fields" placeholder="Find a field or value…" value={filter} onChange={event => setFilter(event.target.value)} />}
      {copyStatus && <span className="copy-status" role="status">{copyStatus}</span>}
    </div>
    <div role="tabpanel" id={`${id}-fields`} aria-labelledby={`${id}-fields-tab`} hidden={tab !== "fields"}>
      {!parsed ? <p className="fields-empty">This record is available in the JSON tab.</p> : <>
        <div className="field-row field-heading" aria-hidden="true"><span>Field</span><span>Type</span><span>Value</span></div>
        {queryNotice && <p className="field-query-notice" role="status">{queryNotice}</p>}
        <div className="fields-scroll">{nodes.length ? <FieldRows key={filter} nodes={nodes} filtered={Boolean(filter.trim())} onOpenMenu={onSearch ? openFieldMenu : undefined} activePath={fieldMenu?.node.path} /> : <p className="fields-empty">{filter ? "No matching fields." : "Empty JSON object."}</p>}</div>
      </>}
    </div>
    <div role="tabpanel" id={`${id}-json`} aria-labelledby={`${id}-json-tab`} hidden={tab !== "json"}>
      <pre className="message-json">{parsed?.pretty ?? raw}</pre>
    </div>
    {fieldMenu && <FieldActionsMenu anchor={fieldMenu.anchor} path={fieldMenu.node.path} search={fieldSearch(fieldMenu.node, message)} onClose={closeFieldMenu} onSelect={exclude => {
      const search = fieldSearch(fieldMenu.node, message);
      if (!search.clause) return;
      onSearch?.(search.clause, exclude); closeFieldMenu();
      setQueryNotice("Условие добавлено в строку поиска. Нажмите Search, чтобы применить.");
    }} />}
  </section>;
}
