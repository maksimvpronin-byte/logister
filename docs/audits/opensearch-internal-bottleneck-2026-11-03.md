# Внутреннее ограничение записи OpenSearch: сжатие stored fields

Дата: 3 ноября 2026. Исследован **один узел OpenSearch 2.17.1 / Lucene 9.11.1**,
ARM64, heap 512 МиБ. VM: 8 vCPU, около 6 ГиБ RAM. Число узлов, реплик и ресурсы
не увеличивались. Индексы — 2 primary shard, replicas=0.

## Что найдено

Главное найденное CPU-горячее место внутри OpenSearch — **LZ4-сжатие сохраняемых
полей Lucene с preset dictionary**, прежде всего сохранённого полного JSON.
В первом JVM-профиле 616 из 1257 Java execution samples (около **49%**)
содержали путь LZ4. Stored fields присутствовали в 629 samples; эти группы
пересекаются и не суммируются.

Конкретный стек:

```text
LZ4$HighCompressionHashTable.get
LZ4.compressWithDictionary
LZ4WithPresetDictCompressor.doCompress / compress
Lucene90CompressingStoredFieldsWriter.flush / finishDocument
StoredFieldsConsumer.finishDocument
IndexingChain.finishStoredFields / processDocument
DocumentsWriterPerThread.updateDocuments
IndexWriter.addDocument
InternalEngine.indexIntoLucene
IndexShard.applyIndexOperationOnPrimary
```

Только два верхних leaf-метода get и compressWithDictionary встретились в
261 и 248 samples соответственно. Это работа CPU над сжатием данных во время
индексации, а не просто ожидание bulk на стороне нашего клиента.

Полное сообщение 10 КиБ остаётся в payload_json и сохраняется внутри _source.
Настройка index:false у payload_json исключает его из текстового индекса, но
не устраняет стоимость хранения и сжатия исходного JSON. Текстовый message
по-прежнему содержит только первые 500 байт с маркером «…».

Официальная [документация codec для нашей версии 2.17](https://docs.opensearch.org/2.17/im-plugin/index-codecs/)
подтверждает: codec задаёт сжатие stored fields, default использует LZ4 с preset
словарём, zstd_no_dict — Zstandard без словарной стадии. Это настройка новых
сегментов индекса; в приложении она применяется при создании нового поколения.

## Как проверили внутри JVM

Сняты JFR settings=profile и CPU hot_threads во время разбора накопленной Kafka.
Использовались JDK-инструменты внутри контейнера OpenSearch, без изменения JVM,
heap и числа узлов. Запись JFR включалась после наполнения Kafka и останавливалась
после доставки тестового потока. Сохранены:

- Java ExecutionSample отдельно от NativeMethodSample;
- JavaMonitorEnter, ThreadPark, FileWrite, FileForce, GarbageCollection;
- CPU/heap/GC, thread_pool, indexing/flush/refresh/merge, diskstats/PSI;
- время каждого bulk, точные счётчики и настройки физического индекса.

Профилирование добавляет нагрузку. Первый профиль дал 6564 лог/с, поэтому его EPS
**не использован** как baseline для сравнения codec. Перцентили и число сэмплов
не являются точным интегралом CPU времени. Native samples могут включать ожидания,
поэтому они не смешивались с Java execution samples в доле 49%.

Первый профиль:

- 100 000 документов, index_time суммарно по primary shard — 24,91 с;
- flush — 2 операции, 0,704 с;
- refresh — 4,066 с; часть работы включает подготовку и сжатие stored fields;
- merge — 0, throttle — 0;
- write queue в выборках — 0, active максимум 4, rejected=0;
- young GC по node stats — 346 мс, old GC — 0;
- зафиксированы отдельные fsync и файловые задержки. JFR пишет события с порогом,
  поэтому их сумма не равна всему времени I/O. Постоянное насыщение диска
  этими данными не подтверждено.

Внутренний buffer/flush виден в стеке, но основной CPU-потребитель в нём —
сжатие сохраняемых данных. Поэтому увеличение heap или порога translog не
выбрано первым исправлением: они не устраняют найденную стоимость сжатия.

## Контролируемый эксперимент с codec

Создавались отдельные индексы с прежним mapping, 2 шардами и прежними режимами
надёжности. Менялся только codec; для Zstandard compression_level=1.
Порядок: **default → zstd_no_dict → zstd → zstd → zstd_no_dict → default**.
По 100 000 сообщений на прогон, текст ровно 10 240 байт случайных hex-данных,
средний входной JSON 10 415,78 байта. Полностью наполняли Kafka перед чтением.

JFR и hot_threads в этой серии выключены. Число потоков API, размер bulk,
выходной буфер, refresh=30s, translog async / sync 5s, AOF everysec и replicas=0
одинаковы. Фоновый генератор продолжал работать. Это короткие backlog-тесты,
с редким поиском; окончание доставки фиксируется примерно раз в секунду.

| Codec | Прогон 1, лог/с | Прогон 2, лог/с | Суммарная скорость | Среднее CPU-время узла / 100 000 | Средний размер индекса |
|---|---:|---:|---:|---:|---:|
| default / LZ4 | 8 676 | 12 110 | **10 109 лог/с** | 25,17 CPU-с | 1036 МиБ |
| zstd, level 1 | 10 706 | 11 970 | **11 303 лог/с** | 19,17 CPU-с | 598 МиБ |
| zstd_no_dict, level 1 | 13 756 | 12 041 | **12 841 лог/с** | 16,55 CPU-с | 611 МиБ |

Суммарная скорость = сумма count / сумма delivery_seconds двух прогонов.
CPU-с — разность накопительного process CPU node stats; она включает фоновую
работу узла и не равна wall-clock или только CPU конкретного индекса.
Размер снимался после поисковой видимости всех 100 000 документов, без force merge;
размер translog в него не входит.

В данной серии zstd_no_dict дал примерно **+27% EPS**, **−34% CPU-времени** и
**−41% размера индекса** относительно default. Разброс значительный: второй
прогон default почти совпал по EPS с одним из zstd_no_dict. Поэтому результат
не является гарантией +27% при любой нагрузке. Отдельно суммарное index_time
снизилось с 15,2–18,4 с у default до 10,0–10,4 с у zstd_no_dict.

Эффект зависит от данных. Здесь синтетический случайный hex-текст 10 КиБ,
полный JSON сохраняется, но не индексируется целиком. На реальных JSON-логах
с другим составом и повторяемостью нужна отдельная проверка.

Во всех шести прогонах точно сверены **600 000 / 600 000** документов. Из каждого
индекса проверены три payload: длина полного текста 10 240 байт, индексируемый
префикс и SHA-256 идентификатора соответствуют прежней схеме.

## Повторный профиль после замены codec

Отдельный JFR-прогон zstd_no_dict: 100 000 / 100 000, index_time 11,03 с,
flush 0,455 с, refresh 0,823 с, merge=0. В выборках write queue=0, rejected=0.
Старый LZ4-путь встречался в **2 из 355 Java execution samples**.

Zstandard выполняет часть работы в native-коде, поэтому уменьшение числа Java
samples нельзя выдавать за такое же уменьшение всей CPU-стоимости. Снижение CPU
подтверждается отдельно накопительными node stats в серии без профилировщика.
После устранения старого горячего пути в Java samples заметнее разбор JSON,
индексирование полей и транспортная обработка.

## Установленное изменение

В API и docker-compose добавлены параметры:

```text
OPENSEARCH_CODEC=zstd_no_dict
OPENSEARCH_CODEC_LEVEL=1
```

Поддерживаются default, zstd и zstd_no_dict. Для Zstandard уровень 1–6;
некорректный codec отвергается при запуске. Параметры передаются только при
создании нового managed-индекса; существующие индексы не перенастраиваются.

Штатной ротацией пять рабочих потоков all, billing, payments, system, web-access
переведены на поколение 000003 с двумя шардами и zstd_no_dict. Старые поколения
сохранены, их счётчики документов проверены до и после операции. UUID потоков,
права, alias, маршрутизация и ID не менялись. Реплики и второй узел не добавлены.

Отдельный auto-index контроль подтвердил, что приложение само создаёт индекс с
нужным codec, сохраняет и находит 100 000 документов. Этот прогон дал **7712 лог/с**,
медленнее основной серии: время decode/prepare/приёма Redis также выросло.
Его нельзя скрывать или заменять лучшим результатом. Из одного такого прогона
нельзя приписать падение конкретно codec или считать найденную причину единственным
ограничением всей системы. После него выполнена дополнительная парная проверка
старого и нового codec при текущем состоянии стенда; её результаты лежат в
graphify-out/opensearch-codecs/20261103T202028Z. В этой паре default дал
8429 лог/с, zstd_no_dict — 13 799 лог/с; по 100 000 / 100 000 документов в каждом
индексе. Это подтвердило направление эффекта, но не устраняет вариативность
абсолютного EPS и влияние фоновой работы. Настройка установлена как более
дешёвый по CPU codec для текущей формы логов, а не как гарантия постоянной скорости.

## Корректность и восстановление

- Go-тесты с локальной PostgreSQL, go vet и race для search/httpapi/redisingest;
- Docker build со всеми Go-тестами;
- создание индекса с codec/level и двумя шардами; существующий индекс не получает
  изменения codec; недопустимые codec/level отклоняются;
- проверка чтения полных payload и прежних ID во всех вариантах;
- после установки OpenSearch приостановлен, приняты 4000 × 10 КиБ за 0,990 с,
  published=0, pending=4000; повтор трёх ID распознан как дубли;
- чистый restart Redis сохранил проверенные тела; после восстановления OpenSearch
  найдены ровно 4000 документов, исходный текст четырёх выбранных совпал точно;
- конфликт ID даёт 409, ingest оператору запрещён с 403.

Проверка clean restart не моделирует отключение питания. Прежние окна
несинхронизированной записи AOF/translog не устранялись этим изменением.

## Артефакты и повторение

- graphify-out/kafka-shards/20261103T195900Z-410d51 — первый JFR, hot_threads,
  jfr-cpu.json, jfr-waits.json, jfr-summary.json и node/index stats;
- graphify-out/opensearch-codecs/20261103T200159Z — manifest, summary,
  post-visibility-stats и проверки payload основной серии;
- graphify-out/kafka-shards/20261103T200906Z-7f96ba — второй JFR;
- graphify-out/ingest-research/codec-deployment.json — ротация рабочих потоков;
- graphify-out/ingest-research/20261103T201548Z-codec-deployed — auto-index контроль;
- graphify-out/optimized/faults-4000.json — последний живой fault-тест.

```sh
python3 deploy/tests/kafka_profile.py --count 100000 --bytes 10240 --shards 2 --os-jfr
python3 deploy/tests/opensearch_codec_matrix.py
python3 deploy/tests/opensearch_codec_summary.py graphify-out/opensearch-codecs/20261103T200159Z/manifest.json --offline
python3 deploy/tests/opensearch_jfr_summary.py graphify-out/kafka-shards/20261103T195900Z-410d51
```

Для profile/matrix требуется временно включённый INGEST_PROFILE_BULK=true.
Сценарии создают отдельные тестовые индексы. В финальном рабочем режиме
диагностические флаги выключены, JFR остановлен и Kafka возобновлена.

Первичные источники: [codec OpenSearch 2.17](https://docs.opensearch.org/2.17/im-plugin/index-codecs/),
[Nodes Hot Threads API](https://docs.opensearch.org/latest/api-reference/nodes-apis/nodes-hot-threads/),
[методика настройки индексации](https://docs.opensearch.org/latest/tuning-your-cluster/performance-tuning/indexing-speed/).
Чужие опубликованные benchmark не переносились на наш стенд.
