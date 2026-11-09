import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import Monitoring from "./Monitoring";
import { fetchMonitoring, type MonitorSnapshot } from "./api";
vi.mock("./api", () => ({ fetchMonitoring: vi.fn() }));
function snapshot(): MonitorSnapshot {
 const at = new Date().toISOString();
 return { instance: "api-test", started_at: at, interval_seconds: 1, history_seconds: 900, config: { ingest_workers: 4, bulk_workers: 2, output_buffer_bytes: 33554432, queue_max_bytes: 268435456, index_shards: 2 }, history: [], latest: { at, duration_ms: 10, rates: { accepted: 1234, published: 1200, input_mib: 6, bulk_mib: 12 }, process: { accepted: 10000, published: 9000, duplicates: 3, input_bytes: 1000, bulk_bytes: 2000, bulk_requests: 12, bulk_errors: 0, bulk_rejected: 0, buffer_windows: 1, buffer_bytes: 1024, timings: { bulk: { calls: 12, errors: 0, p95_ms: 42, last_ms: 20 } } }, warnings: [], components: { redis: { status: "ok", metrics: { pending: 1000, pending_bytes: 10240, oldest_seconds: 6 } }, opensearch: { status: "down", metrics: {} }, api: { status: "ok", metrics: { heap_bytes: 10485760 } } } } };
}
beforeEach(() => vi.resetAllMocks());
it("shows the pipeline and preserves missing component metrics as unavailable", async () => {
 vi.mocked(fetchMonitoring).mockResolvedValue(snapshot());render(<Monitoring />);
 expect(await screen.findByText("API:", { exact: false })).toBeInTheDocument();
 const flow = screen.getByLabelText("Схема обработки логов");
 expect(flow.querySelectorAll(".monitor-node")).toHaveLength(4);
 expect(flow.querySelectorAll(".monitor-api")).toHaveLength(1);
 expect(flow).toHaveTextContent("Подготовка");
 expect(flow).toHaveTextContent("RAM-предвыборка");
 expect(flow).toHaveTextContent("Bulk writer");
 expect(screen.getByRole("button", { name: "Приостановить чтение Kafka" })).toBeInTheDocument();
 expect(screen.getByText("Данные недоступны. Значения не заменяются нулями.")).toBeInTheDocument();
 expect(screen.getByText("42 мс")).toBeInTheDocument();
 expect(screen.getByLabelText("Заполнение журнала Redis")).toHaveAttribute("max", "100");
});
it("hides the prefetch stage when the buffer is off", async () => {
 const data = snapshot();
 data.config.output_buffer_bytes = 0;
 vi.mocked(fetchMonitoring).mockResolvedValue(data);
 render(<Monitoring />);
 const flow = await screen.findByLabelText("Схема обработки логов");
 expect(flow).not.toHaveTextContent("RAM-предвыборка");
 expect(flow).toHaveTextContent("Подготовка");
 expect(flow).toHaveTextContent("Bulk writer");
});
it("shows Kafka Direct without a Redis stage or dependency", async () => {
 const data = snapshot();
 data.config.ingest_mode = "kafka-only";
 delete data.latest!.components.redis;
 data.latest!.components.fluentbit = { status: "ok", metrics: { output_proc_records: 120 } };
 data.nodes = [{ id: "peer", seen_at: data.latest!.at, online: true, components: { fluentbit: "ok" } }];
 vi.mocked(fetchMonitoring).mockResolvedValue(data);
 render(<Monitoring />);
 expect(await screen.findByText("Резервирование в MongoDB")).toBeInTheDocument();
 expect(screen.getByLabelText("Схема обработки логов").querySelectorAll(".monitor-node")).toHaveLength(3);
 expect(screen.queryByText(/Redis|RAM-предвыборка|Fluent Bit/)).not.toBeInTheDocument();
});
it("marks stale snapshots instead of presenting them as live", async () => {
 const data=snapshot();data.latest!.at=new Date(Date.now()-60000).toISOString();vi.mocked(fetchMonitoring).mockResolvedValue(data);render(<Monitoring />);
 expect(await screen.findByRole("alert")).toHaveTextContent("Показатели устарели");
});
it("clears cached monitoring data when a subsequent request is forbidden", async () => {
 vi.mocked(fetchMonitoring).mockResolvedValueOnce(snapshot()).mockRejectedValueOnce(Object.assign(new Error("Доступ запрещён"),{status:403}));render(<Monitoring />);
 await screen.findByText("api-test");fireEvent.click(screen.getByRole("button",{name:"Обновить показатели"}));
 expect(await screen.findByRole("alert")).toHaveTextContent("Доступ запрещён");
 expect(screen.queryByText("api-test")).not.toBeInTheDocument();
});
it("keeps component metrics and the warning panel mounted during an outage", async () => {
 const healthy = snapshot();
 healthy.latest!.components.opensearch = { status: "ok", metrics: { write_queue: 42 } };
 const down = snapshot();
 down.latest!.warnings = ["Очередь Redis растёт"];
 vi.mocked(fetchMonitoring).mockResolvedValueOnce(healthy).mockResolvedValue(down);
 render(<Monitoring />);
 const metric = await screen.findByText("Очередь записи");
 const warnings = screen.getByLabelText("Предупреждения системы");
 expect(warnings).toHaveTextContent("Нет активных предупреждений");

 fireEvent.click(screen.getByRole("button", { name: "Обновить показатели" }));

 expect(await screen.findByText("Очередь Redis растёт")).toBeInTheDocument();
 expect(screen.getByLabelText("Предупреждения системы")).toBe(warnings);
 expect(screen.getByText("Очередь записи")).toBe(metric);
 expect(metric.nextElementSibling).toHaveTextContent("—");
});
