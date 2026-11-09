import {useEffect,useState} from "react";
import {fetchKafkaStatus,setKafkaPaused,type KafkaStatus} from "./api";
const number=new Intl.NumberFormat("ru-RU");
export default function KafkaIndicator({controls=false,variant="board"}:{controls?:boolean;variant?:"board"|"compact"|"pause"}){
 const [status,setStatus]=useState<KafkaStatus|null>(null),[error,setError]=useState(""),[busy,setBusy]=useState(false);
 useEffect(()=>{let active=true,pending=false;const controller=new AbortController();
 async function refresh(){if(pending)return;pending=true;try{const next=await fetchKafkaStatus(controller.signal);if(active){setStatus(next);setError("");}}catch(e){if(active)setError(e instanceof Error?e.message:"Kafka недоступна");}finally{pending=false;}}
 void refresh();const timer=window.setInterval(()=>void refresh(),1000);return()=>{active=false;controller.abort();window.clearInterval(timer);};},[]);
 async function toggle(){if(!status)return;setBusy(true);try{setStatus(await setKafkaPaused(!status.pause_requested));setError("");}catch(e){setError(e instanceof Error?e.message:"Не удалось изменить чтение Kafka");}finally{setBusy(false);}}
 if(status&&!status.enabled)return null;
 const lag=<strong aria-label="Осталось в Kafka">{status?number.format(status.lag):"—"}</strong>;
 const pauseLabel=status?.pause_requested?"Возобновить чтение Kafka":"Приостановить чтение Kafka";
 const pauseButton=<button type="button" disabled={busy||!status} onClick={toggle}>{pauseLabel}</button>;
 const alerts=<>
  {error&&<p className="request-error" role="status">{error} Показатели могут быть устаревшими.</p>}
  {status?.error&&<p className="request-error" role="status">Чтение повторяется после ошибки: {status.error}</p>}
  {status?.retention_loss&&<p className="request-error" role="alert">Kafka удалила непрочитанные данные или смещения были изменены. Проверьте срок хранения и состояние группы.</p>}
 </>;
 if(variant==="compact")return <span className="pipeline-compact">Отставание Kafka {lag}</span>;
 if(variant==="pause")return <div className="kafka-pause">{pauseButton}{status?.pause_requested&&<small>{status.running?"Завершается текущий пакет…":"Чтение на паузе — сообщения накапливаются в Kafka"}</small>}{alerts}</div>;
 return <section className="kafka-indicator" aria-label="Очередь Kafka"><div className="settings-heading"><div><strong>Kafka → Logister</strong><p className="settings-help">{status?.topic} · {status?.group} · {status?.pause_requested?(status.running?"Завершается текущий пакет…":"Чтение на паузе — сообщения накапливаются в Kafka"):status?.running?"Чтение включено":"Подключение…"}</p></div>{controls&&pauseButton}</div>
 <div className="ingest-counters"><div className="ingest-counter"><span>Ожидает в Kafka</span>{lag}<small>Ещё ожидает подтверждения Logister</small></div><div className="ingest-counter"><span>Подтверждено Logister</span><strong>{status?number.format(status.committed):"—"}</strong><small>Смещения подтверждаются после успешной обработки</small></div><div className="ingest-counter"><span>Хранится в Kafka</span><strong>{status?number.format(status.available):"—"}</strong><small>Включая уже прочитанные сообщения · ограничено сроком хранения</small></div></div>
 {alerts}
 <small className="settings-help">{controls?"Общий показатель входящего топика. Скорость доставки в потоки показана выше. Для теста накопите сообщения на паузе, затем возобновите чтение.":"Общий показатель входящего топика. Пауза чтения находится в разделе «Мониторинг»."}</small></section>;
}
