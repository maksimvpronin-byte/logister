import { act, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import IngestIndicator from "./IngestIndicator";
import { fetchIngestStats, type IngestStats } from "./api";
vi.mock("./api", () => ({ fetchIngestStats: vi.fn() }));
const sample = (seconds: number, accepted: number, published: number): IngestStats => ({captured_at:`2026-11-03T10:00:${String(seconds).padStart(2,"0")}Z`,totals:{accepted,published,pending:accepted-published},streams:[{id:"billing",name:"billing",title:"Billing",accepted,published,pending:accepted-published}]});
afterEach(()=>{vi.useRealTimers();vi.resetAllMocks();});
it("computes rates and preserves stale counts after failure",async()=>{
 vi.useFakeTimers();vi.mocked(fetchIngestStats).mockResolvedValueOnce(sample(0,10,8)).mockResolvedValueOnce(sample(5,20,13)).mockRejectedValueOnce(new Error("Нет подключения"));
 const view=render(<IngestIndicator />);await act(async()=>{});
 expect(screen.getByLabelText("Принято логов")).toHaveTextContent("10");expect(screen.getByLabelText("Логов в очереди")).toHaveTextContent("2");
 await act(async()=>{await vi.advanceTimersByTimeAsync(1000);});
 expect(screen.getByText("2,0 лог/с")).toBeInTheDocument();expect(screen.getByText("1,0 лог/с")).toBeInTheDocument();
 expect(screen.getByLabelText("Логов в очереди")).toHaveTextContent("7");
 await act(async()=>{await vi.advanceTimersByTimeAsync(1000);});
 expect(screen.getByRole("status")).toHaveTextContent("Показаны последние полученные значения");expect(screen.getByLabelText("Принято логов")).toHaveTextContent("20");
 expect(screen.queryByText("2,0 лог/с")).not.toBeInTheDocument();view.unmount();
 await act(async()=>{await vi.advanceTimersByTimeAsync(10000);});expect(fetchIngestStats).toHaveBeenCalledTimes(3);
});
it("does not invent zero counts or overlap requests and aborts on unmount",async()=>{
 vi.useFakeTimers();vi.mocked(fetchIngestStats).mockReturnValue(new Promise(()=>{}));
 const view=render(<IngestIndicator />);expect(screen.getByLabelText("Принято логов")).toHaveTextContent("—");
 await act(async()=>{await vi.advanceTimersByTimeAsync(10000);});expect(fetchIngestStats).toHaveBeenCalledTimes(1);
 const signal=vi.mocked(fetchIngestStats).mock.calls[0][0];view.unmount();expect(signal?.aborted).toBe(true);
});
