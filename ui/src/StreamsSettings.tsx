import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { sampleFields } from "./jsonFields";
import { fetchStreams, fetchStreamIndices, saveStream, rotateStream, sendStreamMessage, previewRouting, type Stream, type StreamDraft, type StreamIndex, type RoutingRule, type RoutingPreview } from "./api";

function emptyDraft(): StreamDraft {
  return { name: "", title: "", description: "", routing: { enabled: false, priority: 100, match: "all", rules: [] }, storage: { max_age_hours: 24, max_size_mb: 1024, max_documents: 1000000, retention_days: 0 } };
}
function editDraft(stream: Stream): StreamDraft {
  const defaults = emptyDraft();
  return { name: stream.name, title: stream.title, description: stream.description, routing: stream.routing ?? defaults.routing, storage: stream.storage ?? defaults.storage };
}
function errorText(error: unknown) { return error instanceof Error ? error.message : "The request failed. Please retry."; }
function streamTitle(stream: Stream) { return stream.name === "all" && stream.title === "All messages" ? "Unmatched messages" : stream.title; }
function ruleText(rule: RoutingRule) { return rule.operator === "exists" || rule.operator === "not_exists" ? `${rule.field} ${rule.operator === "exists" ? "exists" : "is missing"}` : `${rule.field} ${rule.operator === "equals" ? "=" : rule.operator === "starts_with" ? "starts with" : "contains"} ${JSON.stringify(rule.value)}`; }
function suggestedName(title: string) {
  return title.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-").replace(/^[-_]+|[-_]+$/g, "").slice(0, 63);
}
const validName = /^[a-z0-9][a-z0-9_-]{0,62}$/;
const nameHelp = "Technical ID: 1–63 lowercase Latin letters, digits, hyphens or underscores; start with a letter or digit. Used in API URLs and cannot be changed after creation.";

export default function StreamsSettings({ onSaved }: { onSaved: (streams: Stream[]) => void }) {
  const [streams, setStreams] = useState<Stream[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [draft, setDraft] = useState<StreamDraft>(emptyDraft);
  const [nameEdited, setNameEdited] = useState(false);
  const [indices, setIndices] = useState<StreamIndex[]>([]);
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [indexLoading, setIndexLoading] = useState(false);
  const [error, setError] = useState("");
  const [indexError, setIndexError] = useState("");
  const [notice, setNotice] = useState("");
  const [refresh, setRefresh] = useState(0);
  const [message, setMessage] = useState("");
  const [source, setSource] = useState("manual");
  const [sample, setSample] = useState('{\n  "source": "nginx",\n  "message": "GET /api/health",\n  "level": "info"\n}');
  const [preview, setPreview] = useState<RoutingPreview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [previewBusy, setPreviewBusy] = useState(false);
  const previewRequest = useRef(0);
  const availableFields = useMemo(() => sampleFields(sample), [sample]);
  const retry = useRef<{ id: string; message: string; source: string } | null>(null);
  const alive = useRef(true);

  useEffect(() => {
    alive.current = true;
    const controller = new AbortController();
    fetchStreams(controller.signal).then(result => {
      if (controller.signal.aborted) return;
      setStreams(result.streams);
      if (result.streams[0]) { setSelected(result.streams[0].name); setDraft(editDraft(result.streams[0])); }
    }).catch(error => { if (!controller.signal.aborted) setError(errorText(error)); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => { alive.current = false; controller.abort(); };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setIndices([]); setIndexError("");
    if (!selected) return () => controller.abort();
    setIndexLoading(true);
    fetchStreamIndices(selected, controller.signal).then(result => {
      if (!controller.signal.aborted) setIndices(result.indices);
    }).catch(error => { if (!controller.signal.aborted) setIndexError(errorText(error)); })
      .finally(() => { if (!controller.signal.aborted) setIndexLoading(false); });
    return () => controller.abort();
  }, [selected, refresh]);

  useEffect(() => { previewRequest.current++; setPreview(null); setPreviewError(""); setPreviewBusy(false); }, [draft, sample]);

  function choose(stream?: Stream) {
    if (busy) return;
    setSelected(stream?.name ?? null); setDraft(stream ? editDraft(stream) : emptyDraft());
    setNameEdited(false);
    setError(""); setNotice(""); setMessage(""); retry.current = null;
  }
  function updateRule(index: number, patch: Partial<RoutingRule>) {
    setDraft(current => ({ ...current, routing: { ...current.routing, rules: current.routing.rules.map((rule, i) => i === index ? { ...rule, ...patch } : rule) } }));
  }
  async function testRouting() {
    const request = ++previewRequest.current;
    setPreview(null); setPreviewError("");
    if (!validName.test(draft.name)) { setPreviewError("Enter a valid technical ID for the stream. " + nameHelp); return; }
    let parsed: unknown;
    try { parsed = JSON.parse(sample); } catch { setPreviewError("Enter a valid JSON message."); return; }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) { setPreviewError("Enter one JSON message object."); return; }
    setPreviewBusy(true);
    try {
      const result = await previewRouting(sample, draft);
      if (alive.current && previewRequest.current === request) setPreview(result);
    } catch (error) { if (alive.current && previewRequest.current === request) setPreviewError(errorText(error)); }
    finally { if (alive.current && previewRequest.current === request) setPreviewBusy(false); }
  }
  async function save(event: FormEvent) {
    event.preventDefault();
    if (!validName.test(draft.name)) { setError("Enter a valid technical ID for the stream. " + nameHelp); return; }
    setBusy(true); setError(""); setNotice("");
    try {
      const result = await saveStream(draft, !selected);
      const next = [...streams.filter(stream => stream.name !== result.stream.name), result.stream].sort((a, b) => a.name.localeCompare(b.name));
      if (!alive.current) return;
      setStreams(next); onSaved(next); setSelected(result.stream.name); setDraft(editDraft(result.stream));
      setNotice("Stream settings saved. New messages use these settings."); setRefresh(value => value + 1);
    } catch (error) { if (alive.current) setError(errorText(error)); }
    finally { if (alive.current) setBusy(false); }
  }
  async function rotate() {
    if (!selected) return;
    setBusy(true); setError(""); setNotice("");
    try {
      const result = await rotateStream(selected);
      if (alive.current) { setNotice(result.rotated ? "Current index sealed. The next message starts a new index." : "No active index yet. The next message creates one."); setRefresh(value => value + 1); }
    } catch (error) { if (alive.current) setError(errorText(error)); }
    finally { if (alive.current) setBusy(false); }
  }
  async function send(event: FormEvent) {
    event.preventDefault(); if (!selected) return;
    setBusy(true); setError(""); setNotice("");
    if (!retry.current || retry.current.message !== message || retry.current.source !== source) retry.current = { id: crypto.randomUUID(), message, source };
    try {
      await sendStreamMessage(selected, retry.current);
      if (alive.current) { setMessage(""); retry.current = null; setNotice("Message accepted. It will appear in search after indexing."); setRefresh(value => value + 1); }
    } catch (error) { if (alive.current) setError(errorText(error)); }
    finally { if (alive.current) setBusy(false); }
  }

  if (loading) return <div className="empty" role="status">Loading stream settings…</div>;
  return <section className="streams-settings" aria-label="Stream settings">
    <div className="settings-heading"><div><h2>Streams &amp; storage</h2><p>Route incoming messages and control how each stream is stored.</p></div><button type="button" disabled={busy} onClick={() => choose()}>Create stream</button></div>
    {error && <div className="request-error" role="alert">{error}</div>}
    {notice && <div className="settings-success" role="status">{notice}</div>}
    <section className="routing-overview" aria-label="Saved routing rules">
      <h3>How messages reach streams</h3>
      <p className="settings-help">Saved rules run in priority order. The first matching stream receives the message. A message with an explicit stream goes directly to that destination.</p>
      <div className="routing-map">{[...streams.filter(stream => stream.name !== "all").sort((a, b) => (a.routing?.priority ?? 100) - (b.routing?.priority ?? 100) || a.name.localeCompare(b.name)), ...streams.filter(stream => stream.name === "all")].map(stream => <div key={stream.name}>
        <strong>{streamTitle(stream)}</strong><code>{stream.name}</code>
        {stream.name === "all" ? <span>Fallback: no rule matched</span> : stream.routing?.enabled ? <><span>{stream.routing.rules.map(ruleText).join(stream.routing.match === "all" ? " AND " : " OR ")}</span><small>Priority {stream.routing.priority}</small></> : <span>Rules disabled · explicit destination only</span>}
      </div>)}</div>
    </section>
    <div className="settings-layout"><aside className="stream-list" aria-label="Configured streams">
      {streams.map(stream => <button type="button" key={stream.name} disabled={busy} aria-pressed={selected === stream.name} onClick={() => choose(stream)}><strong>{streamTitle(stream)}</strong><span>{stream.name}</span></button>)}
      {streams.length === 0 && <p>No streams yet. Create your first stream.</p>}
    </aside><div className="stream-editor">
      <form onSubmit={save} aria-label="Edit stream">
        <fieldset disabled={busy}><legend>{selected ? "Stream details" : "New stream"}</legend>
          <div className="settings-grid"><label>Title<input aria-label="Title" required maxLength={160} value={draft.title} onChange={event => {
            const title = event.target.value;
            setDraft({ ...draft, title, name: !selected && !nameEdited ? suggestedName(title) : draft.name }); setError("");
          }} placeholder="Payments" /><small className="settings-help">Display name. Spaces, uppercase letters and any language are allowed.</small></label>
          <label>Name<input aria-label="Name" required pattern={"[a-z0-9][a-z0-9_\\-]{0,62}"} maxLength={63} disabled={Boolean(selected)} value={draft.name} aria-describedby="stream-name-help" aria-invalid={Boolean(draft.name && !validName.test(draft.name))} onChange={event => {
            const name = event.target.value.toLowerCase().replace(/\s+/g, "-");
            setNameEdited(Boolean(name)); setDraft({ ...draft, name }); setError("");
          }} placeholder="payments" /><small className="settings-help" id="stream-name-help">{nameHelp} Spaces become hyphens; uppercase letters become lowercase.</small></label></div>
          <label>Description<textarea maxLength={2000} rows={2} value={draft.description} onChange={event => setDraft({ ...draft, description: event.target.value })} /></label>
        </fieldset>
        <fieldset disabled={busy}><legend>Automatic routing</legend>
          {draft.name === "all" ? <p className="settings-help">This is the fallback stream: it receives messages that do not match any enabled rule. The separate All messages search option includes every stream.</p> : <>
          <label className="check-label"><input type="checkbox" disabled={draft.name === "all"} checked={draft.routing.enabled} onChange={event => setDraft({ ...draft, routing: { ...draft.routing, enabled: event.target.checked } })} />Enable routing rules</label>
          <p className="settings-help">An explicit destination stream takes priority. Otherwise, the first matching stream wins; lower priority numbers run first. Unmatched messages go to the default stream.</p>
          <div className="settings-grid"><label>Priority<input type="number" min="0" max="1000" required value={draft.routing.priority} onChange={event => setDraft({ ...draft, routing: { ...draft.routing, priority: Number(event.target.value) } })} /></label>
          <label>Match<select value={draft.routing.match} onChange={event => setDraft({ ...draft, routing: { ...draft.routing, match: event.target.value as "all" | "any" } })}><option value="all">All rules</option><option value="any">Any rule</option></select></label></div>
          {draft.routing.rules.map((rule, index) => <div className="routing-rule" key={index}>
            <label>Field {index + 1}<input required maxLength={1024} aria-label={`Rule ${index + 1} field`} list="routing-fields" value={rule.field} onChange={event => updateRule(index, { field: event.target.value })} placeholder="service, event.code, items[0].status" /></label>
            <label>Operator<select aria-label={`Rule ${index + 1} operator`} value={rule.operator} onChange={event => updateRule(index, { operator: event.target.value as RoutingRule["operator"] })}><option value="equals">Equals</option><option value="contains">Contains</option><option value="starts_with">Starts with</option><option value="exists">Exists</option><option value="not_exists">Is missing</option></select></label>
            <label>Value<input maxLength={4096} disabled={rule.operator === "exists" || rule.operator === "not_exists"} aria-label={`Rule ${index + 1} value`} value={rule.value} onChange={event => updateRule(index, { value: event.target.value })} /></label>
            <button type="button" aria-label={`Remove rule ${index + 1}`} onClick={() => setDraft({ ...draft, routing: { ...draft.routing, rules: draft.routing.rules.filter((_, i) => i !== index) } })}>Remove</button>
          </div>)}
          <datalist id="routing-fields">{[...new Set(["source", "host", "level", "facility", "message", ...availableFields.map(field => field.path)])].map(field => <option key={field} value={field} />)}</datalist>
          <p className="settings-help">Use any JSON field path, array index or JSON Pointer. Array conditions match any element. Paste a real event below to discover its fields.</p>
          <button type="button" disabled={draft.routing.rules.length >= 20 || draft.name === "all"} onClick={() => setDraft({ ...draft, routing: { ...draft.routing, rules: [...draft.routing.rules, { field: "source", operator: "equals", value: "" }] } })}>Add rule</button>
          </>}
          <div className="routing-test">
            <h3>Test routing</h3><p className="settings-help">Paste a JSON message to check these settings before saving. The sample is not stored. This draft is evaluated alongside the other saved streams.</p>
            <label>Sample JSON message<textarea aria-label="Sample JSON message" rows={6} spellCheck={false} value={sample} onChange={event => setSample(event.target.value)} /></label>
            {draft.name !== "all" && availableFields.length > 0 && <div className="sample-fields" aria-label="Fields from sample">
              <p className="settings-help">Add a condition for a field from this JSON:</p>
              {availableFields.slice(0, 40).map(field => <button type="button" key={field.path} disabled={draft.routing.rules.length >= 20} aria-label={`Use field ${field.path}`} onClick={() => setDraft({ ...draft, routing: { ...draft.routing, enabled: true, rules: [...draft.routing.rules, { field: field.path, operator: "exists", value: "" }] } })}>{field.path}</button>)}
            </div>}
            <button type="button" disabled={previewBusy || !draft.name || !draft.title} onClick={testRouting}>{previewBusy ? "Checking…" : "Test routing"}</button>
            {previewError && <div className="request-error" role="alert">{previewError}</div>}
            {preview && <div className="routing-result" role="status">
              <strong>Destination: {streams.find(stream => stream.name === preview.stream)?.title ?? (draft.name === preview.stream ? draft.title : preview.stream)} <code>({preview.stream})</code></strong>
              <p>{preview.reason === "explicit" ? "The message declares its stream. Automatic rules are bypassed." : preview.reason === "fallback" ? "No enabled rule matched. The message goes to the fallback stream." : "This stream is the first match in priority order."}</p>
              {preview.candidates.map(candidate => <div className="routing-candidate" key={candidate.name}>
                <strong>{candidate.name}: {candidate.selected ? "selected" : !candidate.enabled ? "disabled" : candidate.matched ? "matched, lower priority" : "did not match"}</strong>
                {candidate.rules.map((rule, index) => <div key={index}><span className={rule.matched ? "rule-pass" : "rule-fail"}>{rule.matched ? "Match" : "No match"}</span> · {ruleText(rule)} · actual: <code>{rule.exists ? JSON.stringify(rule.actual) : "missing"}</code></div>)}
              </div>)}
            </div>}
          </div>
        </fieldset>
        <fieldset disabled={busy}><legend>Rotation &amp; retention</legend>
          <p className="settings-help">Rotate when any enabled threshold is reached. Set a threshold to 0 to disable it. Age and document limits are checked on intake; disk size is checked in the background.</p>
          <div className="settings-grid">
            <label>Maximum age (hours)<input type="number" min="0" max="87600" required value={draft.storage.max_age_hours} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, max_age_hours: Number(event.target.value) } })} /></label>
            <label>Maximum size (MiB)<input type="number" min="0" max="1048576" required value={draft.storage.max_size_mb} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, max_size_mb: Number(event.target.value) } })} /></label>
            <label>Maximum messages<input type="number" min="0" max="1000000000" required value={draft.storage.max_documents} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, max_documents: Number(event.target.value) } })} /></label>
            <label>Retention after rotation (days)<input type="number" min="0" max="3650" required value={draft.storage.retention_days} onChange={event => setDraft({ ...draft, storage: { ...draft.storage, retention_days: Number(event.target.value) } })} /></label>
          </div>
          <p className="settings-help">Retention 0 keeps data indefinitely. A positive value permanently deletes sealed indices after this many days and clears their published payloads from PostgreSQL. Retry identities are retained. Unpublished messages are protected.</p>
          <p className="settings-help">These policies apply to new stream indices. Logs stored before this feature remain searchable and are preserved separately.</p>
        </fieldset>
        <button className="primary" type="submit" disabled={busy}>{busy ? "Working…" : selected ? "Save settings" : "Create stream settings"}</button>
      </form>
      {selected && <>
        <section className="settings-card" aria-label="Stream indices"><div className="settings-heading"><h3>Indices</h3><div><button type="button" disabled={busy || indexLoading} onClick={() => setRefresh(value => value + 1)}>Refresh indices</button><button type="button" disabled={busy} onClick={rotate}>Rotate now</button></div></div>
          {indexError && <div className="request-error" role="alert">{indexError}</div>}
          {indexLoading ? <p role="status">Loading indices…</p> : indices.length === 0 ? <p className="settings-help">No managed indices yet. The first message creates one.</p> : <div className="table-wrap"><table><thead><tr><th>Index</th><th>State</th><th>Messages</th><th>Size</th></tr></thead><tbody>{indices.map(index => <tr key={index.name}><td className="index-name">{index.name}<small>{new Date(index.created_at).toLocaleString()}</small></td><td>{index.deleted_at ? "Deleted" : index.sealed_at ? "Sealed" : "Active"}</td><td>{index.documents ?? `${index.accepted_count} accepted`}</td><td>{index.size_bytes === undefined ? "—" : `${(index.size_bytes / 1048576).toFixed(2)} MiB`}{index.stats_error && <small>{index.stats_error}</small>}</td></tr>)}</tbody></table></div>}
        </section>
        <section className="settings-card"><h3>Write to this stream</h3><p className="settings-help">Send JSON to <code>/api/streams/{selected}/ingest</code>, or include <code>"stream": "{selected}"</code> in a message sent to the general ingestion endpoint.</p>
          <form onSubmit={send} aria-label="Send message"><fieldset disabled={busy}><label>Message<textarea required rows={3} value={message} onChange={event => setMessage(event.target.value)} /></label><label>Source<input required maxLength={200} value={source} onChange={event => setSource(event.target.value)} /></label><button type="submit" disabled={!message.trim()}>Send message</button></fieldset></form>
        </section>
      </>}
    </div></div>
  </section>;
}
