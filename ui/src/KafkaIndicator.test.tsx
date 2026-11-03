import {act,render,screen,waitFor} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import {afterEach,expect,it,vi} from "vitest";
import KafkaIndicator from "./KafkaIndicator";
import {fetchKafkaStatus,setKafkaPaused,type KafkaStatus}from "./api";
vi.mock("./api",()=>({fetchKafkaStatus:vi.fn(),setKafkaPaused:vi.fn()}));
const status:KafkaStatus={enabled:true,topic:"logs",group:"logister",pause_requested:false,running:true,available:100,committed:60,lag:40,retention_loss:false,captured_at:"2026-11-03T10:00:00Z"};
afterEach(()=>{vi.resetAllMocks();vi.useRealTimers();});
it("shows backlog and distinguishes requested pause from completed pause",async()=>{
 vi.mocked(fetchKafkaStatus).mockResolvedValue(status);vi.mocked(setKafkaPaused).mockResolvedValue({...status,pause_requested:true});
 const user=userEvent.setup();render(<KafkaIndicator/>);await waitFor(()=>expect(screen.getByLabelText("Осталось в Kafka")).toHaveTextContent("40"));
 await user.click(screen.getByRole("button",{name:"Приостановить чтение Kafka"}));
 await waitFor(()=>expect(setKafkaPaused).toHaveBeenCalledWith(true));expect(await screen.findByText(/Завершается текущий пакет/)).toBeInTheDocument();
 expect(screen.getByRole("button",{name:"Возобновить чтение Kafka"})).toBeInTheDocument();
});
it("retains last backlog with an explicit stale warning on monitoring error",async()=>{
 vi.useFakeTimers();vi.mocked(fetchKafkaStatus).mockResolvedValueOnce(status).mockRejectedValueOnce(new Error("Сеть недоступна"));
 const view=render(<KafkaIndicator/>);await act(async()=>{});await act(async()=>{await vi.advanceTimersByTimeAsync(1000);});
 expect(screen.getByLabelText("Осталось в Kafka")).toHaveTextContent("40");expect(screen.getByRole("status")).toHaveTextContent("устаревшими");view.unmount();
});
