# Система централизованного сбора и анализа логов

## Введение

### Описание проекта

Проект представляет собой систему сбора, хранения и анализа логов из распределенных источников. Система построена на основе Kappa-архитектуры с потоковой обработкой данных.

### Стек технологий
- Python 3.11+ (FastAPI, AIOKafka, Pydantic)
- Apache Kafka (брокер сообщений, 3 узла)
- ClickHouse (OLAP-хранилище, 2 шарда + 3 Keeper)
- Apache Airflow (ETL-пайплайны, обработка DLQ)
- Apache Superset (аналитические дашборды)
- Nginx (load balancer для ClickHouse)
- Prometheus (сбор метрик)
- Grafana (технический мониторинг)
- Docker & Docker Compose (контейнеризация)

## Запуск

### Требования

- Docker версии 20.10+
- Docker Compose версии 2.0+
- Минимум 8 GB RAM (лучше 16 и больше...)
- Минимум 10 GB свободного места на диске

### Быстрый старт

```bash
# Клонировать репозиторий
git clone https://github.com/KurochkinDaniil/logcelot.git
cd logcelot

# Запустить всю инфраструктуру (все зависимости в Docker, venv не нужен)
make up

# Дождаться готовности всех сервисов (30-60 секунд)
docker ps | grep healthy

# Запустить генератор логов (опционально, для демонстрации)
make up-generator

# Проверить работу API
curl http://localhost:8000/health
```

### Доступные эндпоинты

- **API (Swagger UI)**: http://localhost:8000/docs - основной API для отправки логов
- **Superset** (аналитика): http://localhost:8088 (admin/admin) - дашборды с логами
- **Grafana** (мониторинг): http://localhost:3000 (admin/admin) - технические метрики
- **Kafka UI**: http://localhost:8080 - мониторинг Kafka топиков и DLQ
- **Airflow UI**: http://localhost:8081 (admin/admin) - ETL пайплайн для DLQ
- Prometheus: http://localhost:9090
- Генератор логов API: http://localhost:8082 (если запущен)


### Демонстрация работы

```bash
# 1. Отправить тестовый лог через API
curl -X POST http://localhost:8000/logs \
  -H "Content-Type: application/json" \
  -d '{"source":"test","level":"INFO","message":"Hello from API"}'

# 2. Загрузить файл с логами
curl -X POST http://localhost:8000/logs/file/raw \
  -F "file=@examples/test_logs.json" \
  -F "format=json" \
  -F "source=file"

# 3. Заполнить DLQ для демонстрации Airflow DAG
make demo-dlq

# 4. Проверить DLQ в Kafka UI
# http://localhost:8080 Topics logs.dead_letter

# 5. Запустить DAG в Airflow для обработки DLQ
# http://localhost:8081  DAGs dlq_triage_replay Trigger DAG

# 6. Импортировать Postman коллекцию для полного тестирования
# Logcelot_API.postman_collection.json
```

### Архитектура компонентов

**Kafka Cluster:**
- kafka-1: порт 9092
- kafka-2: порт 9093  
- kafka-3: порт 9094
- KRaft mode (без Zookeeper)
- 3 партиции, replication factor = 3

**ClickHouse Cluster:**
- clickhouse-01: порты 8123 (HTTP), 9000 (Native)
- clickhouse-02: порты 8124 (HTTP), 9001 (Native)
- clickhouse-lb: порт 8125 (Nginx load balancer)
- keeper-01, keeper-02, keeper-03: координация кластера

**Воркеры:**
- worker-1, worker-2, worker-3: консьюмеры Kafka (по одному на партицию)

## Основная часть

### Анализ предметной области

#### Обоснование архитектуры

Выбрана Kappa-архитектура по следующим причинам:

1. **Real-time обработка:** Все данные обрабатываются как поток, что обеспечивает минимальную задержку.
2. **Единый источник правды:** Kafka выступает единым источником данных, упрощая replay и восстановление.
3. **Масштабируемость:** Горизонтальное масштабирование через партиции Kafka и реплики консьюмеров.

#### Стек технологий для компонентов

**API Layer (FastAPI):**
- Асинхронная обработка запросов
- Автогенерация OpenAPI документации Swagger
- Валидация данных через Pydantic
- Поддержка Prometheus-метрик

**Message Broker (Kafka):**
- At-least-once delivery гарантии
- Партиционирование для параллельной обработки
- Replication для отказоустойчивости
- Retention для возможности replay

**Storage Layer (ClickHouse):**
- Колоночное хранение для эффективной компрессии
- Партиционирование по дате для быстрых запросов
- ReplicatedMergeTree для репликации данных
- MergeTree движок для автоматической агрегации

### Проектирование

#### Архитектура приложения

**Расчет нагрузки:**

Целевые показатели:
- RPS: 1000 запросов/сек
- Средний размер лога: 500 байт
- Throughput: ~500 KB/s = ~43 GB/день

Требуемые ресурсы:

**Kafka (3 узла):**
- CPU: 2 cores на узел
- RAM: 2 GB на узел
- Disk: 100 GB на узел (7 дней retention)

**ClickHouse (2 шарда):**
- CPU: 4 cores на узел
- RAM: 4 GB на узел  
- Disk: 200 GB на узел (30 дней хранения с компрессией 10:1)

**API + Workers:**
- CPU: 2 cores
- RAM: 1 GB на инстанс
- Количество воркеров: 3 (по количеству партиций)

**Итого минимальные требования:**
- CPU: 24 cores
- RAM: 20 GB
- Disk: 600 GB

Подробные архитектурные диаграммы см. в `docs/ARCHITECTURE_DIAGRAMS.md`

**Основной поток обработки:**

1. Получение лога через API/File/Generator
2. Валидация структуры (Pydantic)
3. Парсинг формата (JSON/Syslog/CLF)
4. Отправка в Kafka topic `logs.raw`
5. Консьюмер читает batch из Kafka
6. Буферизация в памяти (до 1000 записей или 5 сек)
7. Bulk INSERT в ClickHouse
8. При ошибке: retry с exponential backoff
9. При критической ошибке: отправка в DLQ (`logs.dead_letter`)
10. Commit offset в Kafka после успешного INSERT

#### Схемы баз данных

**ClickHouse schema:**

```sql
CREATE TABLE logs (
    id UUID DEFAULT generateUUIDv4(),
    created_at DateTime DEFAULT now(),
    ingested_at DateTime DEFAULT now(),
    level String,
    source String,
    source_service String,
    host String,
    environment String DEFAULT 'production',
    message String,
    
    -- HTTP fields
    http_method Nullable(String),
    http_path Nullable(String),
    http_status Nullable(UInt16),
    http_response_time_ms Nullable(UInt32),
    
    -- Error tracking
    error_type Nullable(String),
    error_stack Nullable(String),
    
    -- Metadata
    metadata String,  -- JSON
    
    -- Data quality
    is_parsed Bool DEFAULT true,
    parse_errors Nullable(String),
    event_time_missing Bool DEFAULT false,
    
    -- Indexing
    INDEX idx_source source TYPE bloom_filter GRANULARITY 1,
    INDEX idx_level level TYPE bloom_filter GRANULARITY 1,
    INDEX idx_message message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
    
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs', '{replica}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (created_at, source, level)
TTL created_at + INTERVAL 30 DAY;
```

**Materialized Views для аналитики:**

```sql
-- Агрегация по часам и источникам
CREATE MATERIALIZED VIEW logs_hourly_by_source
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, source, level)
AS SELECT
    toStartOfHour(created_at) AS hour,
    source,
    level,
    count() AS log_count
FROM logs
GROUP BY hour, source, level;

-- Топ ошибок
CREATE MATERIALIZED VIEW logs_errors_top
ENGINE = SummingMergeTree()
ORDER BY (error_type, source)
AS SELECT
    error_type,
    source,
    count() AS error_count,
    max(created_at) AS last_seen
FROM logs
WHERE level = 'ERROR' AND error_type IS NOT NULL
GROUP BY error_type, source;
```

#### Описание API

**Базовый URL:** `http://localhost:8000`

**Эндпоинты:**

1. `GET /` - Информация о системе
2. `GET /health` - Health check (статус Kafka, ClickHouse)
3. `POST /logs` - Прием одного лога (JSON)
4. `POST /logs/batch` - Прием batch логов (до 1000 штук)
5. `POST /logs/raw` - Прием raw-логов (Syslog/CLF)
6. `POST /logs/file/raw` - Загрузка файла с raw-логами
7. `POST /logs/file/jsonl` - Загрузка JSONL файла
8. `GET /logs/search` - Поиск логов с фильтрами

**Параметры поиска:**
- `level`: фильтр по уровню (INFO, WARN, ERROR, FATAL)
- `source`: фильтр по источнику
- `source_service`: фильтр по сервису
- `query`: полнотекстовый поиск по message
- `from_time`, `to_time`: временной диапазон
- `limit`, `offset`: пагинация

**Пример запроса:**
```bash
curl -X POST "http://localhost:8000/logs" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "api",
    "source_service": "payment-service",
    "level": "ERROR",
    "message": "Payment timeout",
    "payload": {
      "user_id": "12345",
      "duration_ms": 5000
    }
  }'
```

**Пример ответа:**
```json
{
  "status": "queued",
  "count": 1,
  "kafka_topic": "logs.raw",
  "kafka_offset": 12345,
  "kafka_partition": 2
}
```

### Тестирование

#### Покрытие тестами

**Unit-тесты:**
- Модели и валидация (Pydantic)
- Парсеры логов (Syslog, CLF)
- Kafka Producer
- ClickHouse Client
- Consumer Service (с mock зависимостями)

**Integration-тесты:**
- API эндпоинты (с mock Kafka/ClickHouse)
- E2E тесты (testcontainers!)

**Запуск тестов:**
```bash
make test       # Unit-тесты
make test-all   # Все тесты
```
- Покрытие кода: 77%


#### Нагрузочное тестирование

**Chaos test (отказоустойчивость):**
```bash
./scripts/chaos_test.sh
```

Тест проверяет:
- Работу при отказе ClickHouse узла (20 сек downtime)
- Работу при отказе Kafka broker (Min ISR=2 из 3)
- Отсутствие потери данных (At-Least-Once delivery)
- Автоматическое восстановление

**Результаты нагрузочного тестирования (Locust):**
- RPS: 1000+ req/s (одиночные запросы)
- Batch RPS: 10000+ logs/s (batch по 100 логов)
- Latency P95: (45ms усредненно, 24ms для single-запросов)
- Latency P99: (180ms усредненно, 42ms для single-запросв)

#### Тестовые данные

**Файлы с примерами:**
- `examples/test_logs.json` - JSON формат
- `examples/test_logs.jsonl` - JSON Lines формат
- `examples/test_logs.log` - Syslog RFC5424
- `examples/test_logs.txt` - Common Log Format (CLF)

**Postman коллекция для тестирования всего, включая поиск**
- `Logcelot_API.postman_collection.json`

Содержит примеры всех API запросов с разными форматами и сценариями.

## Заключение

### Краткие выводы

Реализована полнофункциональная система сбора и анализа логов на базе Kappa-архитектуры:

1. **Производительность:** 10000+ логов/сек при batch-обработке, latency P95 < 50ms
2. **Отказоустойчивость:** Выдерживает отказ любого узла Kafka или ClickHouse без потери данных
3. **Масштабируемость:** Горизонтальное масштабирование через партиции и воркеры
4. **Качество кода:** 77% покрытие тестами, type hints и проие pythonic-практики, в тестах попробовал test-containers и в целом разделил архитектурно код на слои 



### Результаты

1. Реализованы:
   - 3 источника данных (файлы, HTTP, Kafka)
   - 3 формата логов (JSON, Syslog RFC5424, CLF)
   - REST API с поиском и фильтрацией
   - ETL-пайплайн в Airflow (DLQ triage & replay)
   - Аналитические дашборды в Apache Superset (7 чартов)
   - Технический мониторинг через Prometheus + Grafana

2. Генерация тестовых данных:
   - Реализован генератор логов с различными сценариями
   - Поддержка >200000 логов (ограничено только ресурсами)

3. Отказоустойчивость:
   - Kafka Cluster с RF=3, Min ISR=2
   - ClickHouse Cluster с репликацией
   - Dead Letter Queue для проблемных сообщений
   - Retry logic с exponential backoff


### Перспективы развития

**Оптимизация производительности:**
- Асинхронный batch INSERT в ClickHouse
- Компрессия данных каким-нибудь способом, чтобы в Kafka больше помещалось исторических данных

**Безопасность:**
- Аутентификация и ролевая модель со скрытием логов по источнику
- Шифрование данных в Kafka (TLS)
- Маскирование sensitive данных (PII)

**Функциональность:**
- Machine Learning для автоматического обнаружения аномалий
- Alert manager для уведомлений о критических событиях
