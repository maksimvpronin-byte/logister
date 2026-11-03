import { useEffect, useState } from "react";
import { fetchIngestStats, type IngestStats } from "./api";
import { streamDisplayTitle } from "./locale";

const integer = new Intl.NumberFormat("ru-RU");
const decimal = new Intl.NumberFormat("ru-RU", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
type Rates = { accepted: number; published: number };
function sampleRates(previous: IngestStats | null, next: IngestStats): Rates | null {
  if (!previous || previous.streams.map(s => s.id).join() !== next.streams.map(s => s.id).join()) return null;
  const seconds = (Date.parse(next.captured_at) - Date.parse(previous.captured_at)) / 1000;
  const accepted = next.totals.accepted - previous.totals.accepted;
  const published = next.totals.published - previous.totals.published;
  if (!Number.isFinite(seconds) || seconds <= 0 || seconds > 30 || accepted < 0 || published < 0) return null;
  return { accepted: accepted / seconds, published: published / seconds };
}

export default function IngestIndicator() {
  const [stats, setStats] = useState<IngestStats | null>(null);
  const [rates, setRates] = useState<Rates | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let active = true;
    let pending = false;
    let previous: IngestStats | null = null;
    const controller = new AbortController();
    async function refresh() {
      if (pending) return;
      pending = true;
      try {
        const next = await fetchIngestStats(controller.signal);
        if (!active) return;
        setRates(sampleRates(previous, next)); previous = next;
        setStats(next); setError("");
      } catch (e) {
        if (active) { previous = null; setRates(null); setError(e instanceof Error ? e.message : "Показатели обработки недоступны."); }
      } finally { pending = false; if (active) setLoading(false); }
    }
    void refresh();
    const timer = window.setInterval(() => void refresh(), 1000);
    return () => { active = false; controller.abort(); window.clearInterval(timer); };
  }, []);
  const value = (key: keyof IngestStats["totals"]) => stats ? integer.format(stats.totals[key]) : "—";
  return <section className="ingest-indicator" aria-label="Обработка логов" aria-busy={loading}>
    <div className="ingest-indicator-heading"><strong>Обработка логов</strong><span>{loading ? "Загрузка показателей…" : error ? "Данные недоступны или устарели" : stats?.totals.pending ? "Есть сообщения в очереди" : "Очередь пуста"}</span><small>Все доступные вам потоки · обновление каждые 1 с</small></div>
    <div className="ingest-counters">
      <div className="ingest-counter"><span>Принято</span><strong aria-label="Принято логов">{value("accepted")}</strong><small>{rates ? `${decimal.format(rates.accepted)} лог/с` : "Скорость появится после следующего обновления"}</small></div>
      <div className="ingest-counter"><span>Доставлено в потоки</span><strong aria-label="Доставлено логов">{value("published")}</strong><small>{rates ? `${decimal.format(rates.published)} лог/с` : "Подтверждена запись в индексы потоков"}</small></div>
      <div className={`ingest-counter ${stats?.totals.pending ? "ingest-pending" : ""}`}><span>Ожидает отправки</span><strong aria-label="Логов в очереди">{value("pending")}</strong><small>Принято, но отправка ещё не подтверждена</small></div>
    </div>
    {error && <p className="ingest-stats-error" role="status">{error} {stats ? "Показаны последние полученные значения." : "Значения не подменяются нулями."}</p>}
    <details className="ingest-streams"><summary>По потокам{stats ? ` · ${stats.streams.length}` : ""}</summary>
      <p>Уникальные сообщения с момента включения учёта потоков. Повторные доставки не увеличивают счётчики. Удаление старых логов по сроку хранения не обнуляет показатели.</p>
      {stats && <div className="table-wrap"><table><thead><tr><th>Поток</th><th>Принято</th><th>Доставлено</th><th>В очереди</th></tr></thead><tbody>{stats.streams.map(stream => <tr key={stream.id}><td>{streamDisplayTitle(stream)}<small>{stream.name}</small></td><td>{integer.format(stream.accepted)}</td><td>{integer.format(stream.published)}</td><td>{integer.format(stream.pending)}</td></tr>)}</tbody></table>{stats.streams.length === 0 && <p>Нет доступных потоков.</p>}</div>}
      {stats && <small>Получено в {new Date(stats.captured_at).toLocaleTimeString("ru-RU")}. Скорость — средняя за последний интервал обновления.</small>}
    </details>
  </section>;
}
