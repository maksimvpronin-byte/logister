import { useEffect, useRef, useState } from "react";
import { fetchMonitoring, type MonitorComponent, type MonitorNode, type MonitorPoint, type MonitorSnapshot } from "./api";
import IngestIndicator from "./IngestIndicator";
import KafkaIndicator from "./KafkaIndicator";
import Icon, { type IconName } from "./Icon";

const integer = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 });
const decimal = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 });
const format = (value: number | null | undefined) => value == null ? "—" : decimal.format(value);
const bytes = (value: number | undefined) => value == null ? "—" : value >= 1 << 30 ? `${format(value / (1 << 30))} ГиБ` : `${format(value / (1 << 20))} МиБ`;
const number = (value: number | undefined) => value == null ? "—" : integer.format(value);
const age = (value: number | undefined) => value == null ? "—" : value >= 60 ? `${format(value / 60)} мин` : `${format(value)} с`;
const statusLabels: Record<string, string> = { ok: "Доступен", warning: "Внимание", down: "Недоступен", disabled: "Не настроен" };
const names: Record<string, string> = { kafka: "Kafka", api: "Logister API", redis: "Redis", opensearch: "OpenSearch", mongo: "MongoDB" };
const colors = ["var(--accent)", "var(--chart-alt)"];

function HistoryChart({ title, points, series, unit, primary = false }: { title: string; points: MonitorPoint[]; series: { key: keyof Omit<MonitorPoint, "at">; label: string }[]; unit: string; primary?: boolean }) {
  const [hover, setHover] = useState<number | null>(null);
  const end = points.length ? Date.parse(points[points.length - 1].at) : Date.now();
  const start = Math.min(points.length ? Date.parse(points[0].at) : end, end - 60000);
  const max = Math.max(1, ...points.flatMap(p => series.map(s => p[s.key] ?? 0)));
  const width = primary ? 620 : 360;
  const height = primary ? 254 : 104;
  const plotLeft = 60;
  const plotRight = width - 12;
  const plotBottom = height - 24;
  const x = (point: MonitorPoint) => plotLeft + (Date.parse(point.at) - start) / Math.max(1, end - start) * (plotRight - plotLeft);
  const y = (value: number) => plotBottom - value / max * (plotBottom - 12);
  const paths = series.map(s => {
    let path = "", connected = false, last = 0;
    for (const point of points) {
      const value = point[s.key];
      if (value == null) { connected = false; continue; }
      const at = Date.parse(point.at);
      path += `${connected && at - last < 15000 ? "L" : "M"}${x(point)},${y(value)} `;
      connected = true; last = at;
    }
    return path;
  });
  const chosen = hover == null ? points[points.length - 1] : points[hover];
  const hasSeries = points.filter(p => series.some(s => p[s.key] != null)).length >= 2;
  return <article className={`monitor-chart${primary ? " monitor-chart-primary" : ""}`}>
    <div className="monitor-chart-title"><h3>{title}</h3><span className="chart-unit">{unit}</span></div>
    <div className="monitor-legend">{series.map((s, i) => <span key={s.key}><i style={{ background: colors[i] }} />{s.label}: <strong>{format(chosen?.[s.key])}</strong></span>)}</div>
    <div className="monitor-plot">{!hasSeries ? <div className="monitor-chart-empty">Накапливаются данные для графика…</div> : <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title} onMouseLeave={() => setHover(null)} onMouseMove={event => {
      const rect = event.currentTarget.getBoundingClientRect();
      const ratio = ((event.clientX - rect.left) / rect.width) * width;
      const target = start + Math.max(0, Math.min(1, (ratio - plotLeft) / (plotRight - plotLeft))) * (end - start);
      let nearest = 0; points.forEach((p, i) => { if (Math.abs(Date.parse(p.at) - target) < Math.abs(Date.parse(points[nearest].at) - target)) nearest = i; }); setHover(nearest);
    }}>
      {[0, .5, 1].map(v => <g key={v}><line x1={plotLeft} x2={plotRight} y1={y(v * max)} y2={y(v * max)} /><text x={plotLeft - 10} y={y(v * max) + 4} textAnchor="end">{format(v * max)}</text></g>)}
      {paths.map((path, i) => <path key={series[i].key} d={path} fill="none" strokeWidth="2" vectorEffect="non-scaling-stroke" style={{ stroke: colors[i] }} />)}
      {hover != null && chosen && <line className="monitor-chart-cursor" x1={x(chosen)} x2={x(chosen)} y1="12" y2={plotBottom} />}
      <text x={plotLeft} y={height - 5}>{new Date(start).toLocaleTimeString("ru-RU")}</text><text x={plotRight} y={height - 5} textAnchor="end">{new Date(end).toLocaleTimeString("ru-RU")}</text>
    </svg>}</div>
    <small className="monitor-chart-time">{hasSeries && hover != null && chosen ? new Date(chosen.at).toLocaleTimeString("ru-RU") : ""}</small>
  </article>;
}
function Value({ label, value, hint, icon }: { label: string; value: string; hint?: string; icon: IconName }) {
  return <div className="monitor-value"><div><span className="metric-icon"><Icon name={icon} /></span><span>{label}</span></div><strong>{value}</strong>{hint && <small>{hint}</small>}</div>;
}
function ComponentStatus({ status }: { status?: string }) {
  return <span className={`monitor-status ${status ?? "disabled"}`}>{status ? statusLabels[status] ?? status : "Нет данных"}</span>;
}
const clusterDependencies = ["kafka", "redis", "opensearch", "mongo"];

function ClusterOverview({ nodes, instance, stale, kafkaOnly }: { nodes: MonitorNode[]; instance?: string; stale: boolean; kafkaOnly: boolean }) {
  const dependencies = clusterDependencies.filter(key => {
    if (key === "redis" && kafkaOnly) {
      return nodes.some(node => node.online && node.components[key] && node.components[key] !== "disabled");
    }
    return true;
  });
  const active = nodes.filter(node => node.online).length;
  return <section className="monitor-cluster" aria-label="Инстансы Logister">
    <div className="monitor-cluster-heading">
      <h2>Инстансы Logister</h2>
      <span>{active ? `Активных инстансов: ${active}` : "Ожидание снимков"}</span>
    </div>
    <p className="monitor-caption">{stale ? "Показан последний доступный список нод." : "Статусы зависимостей показаны с точки зрения каждого инстанса."}</p>
    <div className="monitor-cluster-table">
      <table>
        <thead><tr><th scope="col">Инстанс</th><th scope="col">Logister</th>{dependencies.map(key => <th scope="col" key={key}>{names[key]}</th>)}</tr></thead>
        <tbody>{nodes.map(node => <tr key={node.id}>
          <th scope="row"><span title={node.id}>{node.id}</span><small>{node.id === instance ? "Этот инстанс" : ""}</small></th>
          <td><ComponentStatus status={node.online ? "ok" : "down"} /></td>
          {dependencies.map(key => <td key={key}><ComponentStatus status={node.online ? node.components[key] : undefined} /></td>)}
        </tr>)}</tbody>
      </table>
    </div>
  </section>;
}

function ComponentMetrics({ name, component }: { name: string; component?: MonitorComponent }) {
  const c = component, m = c?.status === "down" ? {} : c?.metrics ?? {};
  const rows: [string, string][] = [];
  if (name === "api") rows.push(["CPU, ядер", format(m.cpu_cores)], ["Память контейнера", bytes(m.memory_bytes)], ["Go heap", bytes(m.heap_bytes)], ["Goroutine", number(m.goroutines)]);
  if (name === "redis") rows.push(["CPU процесса, ядер", format(m.cpu_cores)], ["Данные в памяти", bytes(m.memory_bytes)], ["RSS процесса", bytes(m.rss_bytes)], ["Ключи, всего", number(m.keys)], ["Размер AOF", bytes(m.aof_bytes)], ["Задержки fsync, всего", number(m.aof_delayed_fsync)]);
  if (name === "opensearch") rows.push(["CPU процессов, ядер", format(m.cpu_cores)], ["JVM heap", `${bytes(m.heap_bytes)} / ${bytes(m.heap_limit_bytes)}`], ["Свободно на дисках узлов", bytes(m.disk_free_bytes)], ["Основные / все шарды", `${number(m.primary_shards)} / ${number(m.shards)}`], ["Очередь записи", number(m.write_queue)], ["Отклонено записей, всего", number(m.write_rejected)]);
  if (name === "mongo") rows.push(["Размер данных", bytes(m.database_bytes)], ["Подключения", number(m.connections)], ["Участники", number(m.members)]);
  if (name === "kafka") rows.push(["Ожидает обработки", number(m.lag)], ["Хранится в топике", number(m.available)], ["Подтверждено в диапазоне", number(m.committed)], ["CPU / память процесса", "Не экспортируются"]);
  return <article className="monitor-component"><div className="monitor-component-heading"><h3>{names[name]}</h3><ComponentStatus status={c?.status} /></div>
    <dl>{rows.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>
    <small className="monitor-component-note">{c?.status === "down" && <span className="monitor-unavailable">Данные недоступны. Значения не заменяются нулями.</span>}{c?.note && <span>{c.note}</span>}</small>
  </article>;
}
export default function Monitoring({ kafkaOnlyHint }: { kafkaOnlyHint?: boolean }) {
  const [data, setData] = useState<MonitorSnapshot | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [now, setNow] = useState(Date.now());
  const refreshRef = useRef<() => void>(() => {});
  useEffect(() => {
    let active = true, pending = false; let controller: AbortController | null = null;
    async function refresh() {
      setNow(Date.now()); if (pending) return; pending = true; controller = new AbortController(); const timeout = window.setTimeout(() => controller?.abort(), 4000);
      try { const next = await fetchMonitoring(controller.signal); if (active) { setData(next); setError(""); } }
      catch (e) { if (active) { if (e instanceof Error && "status" in e && (e.status === 401 || e.status === 403)) setData(null); setError(e instanceof Error && e.name !== "AbortError" ? e.message : "Сервер не ответил вовремя."); } }
      finally { window.clearTimeout(timeout); pending = false; if (active) { setLoading(false); setNow(Date.now()); } }
    }
    refreshRef.current = () => void refresh(); void refresh(); const timer = window.setInterval(() => void refresh(), 1000);
    return () => { active = false; controller?.abort(); window.clearInterval(timer); };
  }, []);
  const sample = data?.latest, c = sample?.components ?? {}, p = sample?.process;
  const redis = c.redis?.metrics ?? {}, kafka = c.kafka?.metrics ?? {}, os = c.opensearch?.metrics ?? {};
  const config = data?.config ?? {};
  const kafkaOnly = data ? config.ingest_mode === "kafka-only" : kafkaOnlyHint !== false;
  const stale = sample ? now - Date.parse(sample.at) > 15000 : false;
  const limit = Number(config.queue_max_bytes ?? 0), fill = limit > 0 && redis.pending_bytes != null ? Math.min(100, redis.pending_bytes / limit * 100) : null;
  const timings = p?.timings ?? {};
  const prefetchOn = !kafkaOnly && Number(config.output_buffer_bytes) !== 0;
  const apiStages = [
    { name: "Подготовка", value: format(sample?.rates.accepted), detail: "принято лог/с" },
    ...(prefetchOn ? [{ name: "RAM-предвыборка", value: number(p?.buffer_windows), detail: `${bytes(p?.buffer_bytes)} готовых тел` }] : []),
    { name: "Bulk writer", value: format(sample?.rates.published), detail: "подтверждено лог/с" },
  ];
  const services = [
    { key: "kafka", name: "Kafka", role: "Вход", value: number(kafka.lag), detail: "сообщений в очереди" },
    { key: "api", name: "Logister API", role: "Обработка" },
    ...(!kafkaOnly ? [{ key: "redis", name: "Журнал Redis", role: "Буфер", value: number(redis.pending), detail: "ожидает записи" }] : []),
    { key: "opensearch", name: "OpenSearch", role: "Запись", value: number(os.indexed), detail: "операций индексации" },
  ];
  const configRows: [string, string][] = [["Обработчики подготовки", String(config.ingest_workers ?? "—")], ["Bulk-отправители", String(config.bulk_workers ?? "—")], ["Размер bulk", bytes(Number(config.bulk_bytes) || undefined)]];
  if (!kafkaOnly) configRows.push(
    ["RAM-предвыборка", Number(config.output_buffer_bytes) === 0 ? "Отключена" : bytes(Number(config.output_buffer_bytes) || undefined)],
    ["Порция публикации", bytes(Number(config.publish_batch_bytes) || undefined)],
    ["Бюджет журнала", bytes(Number(config.queue_max_bytes) || undefined)],
  );
  configRows.push(["Kafka: записей в порции", String(config.kafka_batch_records ?? "—")], ["Kafka: размер порции", bytes(Number(config.kafka_batch_bytes) || undefined)], ["Шарды новых индексов", String(config.index_shards ?? "—")], ["Сжатие новых индексов", config.codec ? `${config.codec} · уровень ${config.codec_level}` : "—"]);
  const queueSeries: { key: "kafka_lag" | "pending"; label: string }[] = [{ key: "kafka_lag", label: "Kafka" }];
  const timingRows = [["decode", "Разбор Kafka JSON"], ["prepare", "Подготовка и маршрутизация"]];
  if (kafkaOnly) {
    timingRows.push(["kafka_reserve", "Резервирование в MongoDB"], ["bulk", "Запрос bulk"], ["kafka_complete", "Подтверждение в MongoDB"]);
  } else {
    queueSeries.push({ key: "pending", label: "Redis" });
    timingRows.push(["journal", "Приём в журнал"], ["bulk", "Запрос bulk"], ["delivery", "Приём → ACK записи"]);
  }
  return <section className="monitoring" aria-label="Мониторинг обработки" aria-busy={loading}>
    <header className="monitor-heading page-heading page-hero">
      <div className="heading-copy">
        <p className="page-kicker"><Icon name="activity" /> Состояние системы</p>
        <h1>Мониторинг обработки</h1>
        <p>Скорость записи, очереди и состояние узлов Logister.</p>
      </div>
      <button type="button" className="secondary-button" onClick={() => refreshRef.current()}><Icon name="refresh" />Обновить показатели</button>
    </header>
    <div className="monitor-meta"><span className={stale || error ? "monitor-dot bad" : sample ? "monitor-dot" : "monitor-dot pending"} /><span>{sample ? `Снимок: ${new Date(sample.at).toLocaleTimeString("ru-RU")}` : "Ожидание данных"}</span><span>Опрос каждые 1 с</span><span>API: <code>{data?.instance ?? "—"}</code></span></div>
    <div className={`monitor-notices${error || stale || sample?.warnings.length ? " has-warnings" : ""}`} aria-label="Предупреждения системы">
      <strong>Предупреждения{sample?.warnings.length ? ` · ${sample.warnings.length}` : ""}</strong>
      <div className="monitor-notices-body" aria-live="polite" tabIndex={0}>
        {(error || stale) && <div role="alert">
          {error && <p>{error} {data ? "Показан последний полученный снимок." : "Показатели ещё не получены."}</p>}
          {stale && <p>Показатели устарели: последнему снимку больше 15 секунд.</p>}
        </div>}
        {sample?.warnings.map(w => <p key={w}>{w}</p>)}
        {!error && !stale && !sample?.warnings.length && <p>{sample ? "Нет активных предупреждений" : "Ожидание показателей"}</p>}
      </div>
    </div>
    <div className="monitor-summary">
      <Value icon="input" label="Принято этим API" value={`${format(sample?.rates.accepted)} лог/с`} hint={`${format(sample?.rates.input_mib)} МиБ/с исходного JSON`} />
      <Value icon="write" label="Подтверждена запись" value={`${format(sample?.rates.published)} лог/с`} hint={`${format(sample?.rates.bulk_mib)} МиБ/с bulk с метаданными`} />
      {kafkaOnly ? <Value icon="inbox" label="Ожидает в Kafka" value={number(kafka.lag)} /> : <Value icon="inbox" label="Возраст старейшего в Redis" value={age(redis.oldest_seconds)} hint="От системного времени приёма, не из timestamp лога" />}
      <Value icon="clock" label="Задержка bulk · P95" value={timings.bulk ? `${format(timings.bulk.p95_ms)} мс` : "—"} hint="Последние 128 bulk-запросов" />
    </div>
    <div className="monitor-charts"><HistoryChart primary title="Скорость обработки" points={data?.history ?? []} unit="лог/с" series={[{ key: "accepted", label: "Принято" }, { key: "published", label: "Записано" }]} /><HistoryChart title="Очереди" points={data?.history ?? []} unit="сообщений" series={queueSeries} /><HistoryChart title="Объём принятого JSON" points={data?.history ?? []} unit="МиБ/с" series={[{ key: "input_mib", label: "Принято" }]} /></div>
    <p className="monitor-caption">История за 15 минут сбрасывается при перезапуске API. Скорости относятся к этому API. {kafkaOnly ? "Отставание Kafka относится ко всей группе потребителей." : "Очереди Kafka и Redis общие."}</p>
    <ClusterOverview nodes={data?.nodes ?? []} instance={data?.instance} stale={Boolean(error || stale || data?.cluster_stale)} kafkaOnly={kafkaOnly} />
    <section className="monitor-topology" aria-label="Связи компонентов">
      <div className="monitor-topology-heading">
        <div><h2 className="monitor-topology-title">Связи компонентов</h2><p className="monitor-caption">От входной очереди до индекса OpenSearch</p></div>
        <span className="topology-mode">{kafkaOnly ? "Kafka Direct" : "Журнал Redis"}</span>
      </div>
      <ol className={`monitor-flow${kafkaOnly ? " monitor-flow-direct" : ""}`} aria-label="Схема обработки логов" tabIndex={0}>
        {services.map(node => {
          const status = c[node.key]?.status;
          return <li key={node.key} className={`monitor-node monitor-${node.key} ${status ?? "disabled"}`}>
            <div className="monitor-node-top"><span className="monitor-node-label">{node.role}</span><ComponentStatus status={status} /></div>
            <strong>{node.name}</strong>
            {node.key === "api" ? (
              <div className="monitor-api-stages">
                {apiStages.map(stage => <div key={stage.name} className="monitor-api-stage"><span>{stage.name}</span><b title={stage.value}>{stage.value}</b><small>{stage.detail}</small></div>)}
              </div>
            ) : <><b title={node.value}>{node.value}</b><small>{node.detail}</small></>}
            {node.key === "kafka" && <KafkaIndicator variant="pause" />}
          </li>;
        })}
      </ol>
      <p className="monitor-caption">{kafkaOnly ? "Logister подтверждает смещения Kafka после записи в OpenSearch и обновления состояния в MongoDB." : "Logister сохраняет принятые события в журнале Redis до подтверждения записи в OpenSearch. Приём по HTTP использует тот же журнал."}</p>
      <dl className="monitor-links">
        {!kafkaOnly && <div><dt>Приём по HTTP</dt><dd>HTTP-клиент → Logister API → журнал Redis</dd></div>}
        <div><dt>Поиск</dt><dd>Браузер ↔ nginx ↔ Logister API ↔ OpenSearch</dd></div>
        <div><dt>Метаданные <ComponentStatus status={c.mongo?.status} /></dt><dd>Logister API ↔ MongoDB</dd></div>
        <div><dt>Вход и группы</dt><dd>Logister API ↔ LDAP (если настроен)</dd></div>
      </dl>
    </section>
    {!kafkaOnly && <div className="monitor-queue"><div><strong>Заполнение журнала Redis</strong><span>{bytes(redis.pending_bytes)} / {bytes(limit || undefined)}{fill != null ? ` · ${format(fill)}%` : ""}</span></div><progress aria-label="Заполнение журнала Redis" max={100} value={fill ?? undefined} /><small>При заполнении журнала приём ожидает освобождения места. Полные тела остаются в Redis до подтверждения записи.</small></div>}
    <IngestIndicator />
    <div className="monitor-section-heading"><h2>Ресурсы и компоненты</h2><span>CPU в ядрах: 1,0 = одно полностью занятое ядро</span></div>
    <div className="monitor-components">{["api", ...(!kafkaOnly ? ["redis"] : []), "opensearch", "mongo", "kafka"].map(name => <ComponentMetrics key={name} name={name} component={c[name]} />)}</div>
    <div className="monitor-bottom"><article className="monitor-component"><h2>Время прохождения</h2><p className="monitor-caption">Последние 128 операций каждого этапа.{!kafkaOnly && " Для доставки измеряется время старейшего сообщения в подтверждённой порции."}</p><div className="table-wrap"><table><thead><tr><th>Этап</th><th>Последний, мс</th><th>P95, мс</th></tr></thead><tbody>{timingRows.map(([key, label]) => <tr key={key}><td>{label}</td><td>{format(timings[key]?.last_ms)}</td><td>{format(timings[key]?.p95_ms)}</td></tr>)}</tbody></table></div><p className="monitor-caption">Bulk ACK подтверждает приём записи OpenSearch. Видимость в поиске обновляется отдельно при refresh индекса.</p><p>Ошибки bulk: <strong>{number(p?.bulk_errors)}</strong> · Отклонения 429: <strong>{number(p?.bulk_rejected)}</strong> · Сбои публикации: <strong>{number(timings.publish?.errors)}</strong> · Дубли: <strong>{number(p?.duplicates)}</strong></p></article>
    <article className="monitor-component"><h2>Действующая конфигурация API</h2><dl>{configRows.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl><p className="monitor-caption">Это текущие параметры экземпляра. Число шардов и codec применяются к новым индексам; существующие показаны в карточке OpenSearch.</p></article></div>
    <p className="monitor-caption">Хост первого узла OpenSearch: CPU {os.host_cpu_percent == null ? "—" : `${format(os.host_cpu_percent)}%`}, свободная RAM {bytes(os.host_memory_free_bytes)} из {bytes(os.host_memory_total_bytes)}.</p>
  </section>;
}
