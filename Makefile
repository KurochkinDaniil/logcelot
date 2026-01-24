.PHONY: up down up-generator down-generator test test-all demo-dlq help

help: ## Показать эту справку
	@echo "Доступные команды:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

COMPOSE ?= docker compose


up: ## Запустить всю инфраструктуру
	@echo "Запуск Logcelot"
	$(COMPOSE) up -d kafka-1 kafka-2 kafka-3 \
		keeper-01 keeper-02 keeper-03 \
		clickhouse-01 clickhouse-02 clickhouse-lb \
		api worker-1 worker-2 \
		prometheus grafana kafka-ui \
		kafka-exporter clickhouse-exporter blackbox-exporter \
		postgres-airflow airflow-webserver airflow-scheduler \
		postgres-superset superset
	@echo ""
	@echo "Инфраструктура запущена!"
	@echo ""
	@echo "Доступные сервисы:"
	@echo "  API (Swagger):     http://localhost:8000/docs"
	@echo "  Superset:          http://localhost:8088 (admin/admin) - Аналитика"
	@echo "  Grafana:           http://localhost:3000 (admin/admin) - Технический мониторинг"
	@echo "  Kafka UI:          http://localhost:8080"
	@echo "  Airflow:           http://localhost:8081 (admin/admin)"
	@echo ""
	@echo "Демонстрация:"
	@echo "  DLQ + DAG demo:    make demo-dlq"
	@echo "  Запуск теста:      make load-test"
	@echo ""
	@echo "Superset (аналитика):"
	@echo "  1. Откройте http://localhost:8088 (admin/admin)"
	@echo "  2. Добавьте ClickHouse: Settings → Database Connections → + Database"
	@echo "     URI: clickhousedb://logcelot_user:logcelot_pass@clickhouse-lb:8123/logcelot"
	@echo "  3. Создайте дашборды: infrastructure/superset/DASHBOARDS.md"
	@echo ""

down: ## Остановить всю инфраструктуру
	@echo "Остановка Logcelot"
	$(COMPOSE) down
	@echo "Все сервисы остановлены"

up-generator: ## Запустить генератор логов
	@echo "Запуск генератора логов"
	$(COMPOSE) up -d log-generator
	@echo "Генератор запущен"
	@echo "   API генератора: http://localhost:8082"
	@echo "   Статистика:     curl http://localhost:8082/stats | jq"

down-generator: ## Остановить генератор логов
	$(COMPOSE) stop log-generator
	@echo "Генератор остановлен"


test: ## Запустить unit-тесты
	@echo "Запуск unit-тестов"
	@if [ -f venv/bin/activate ]; then \
		. venv/bin/activate && PYTHONPATH=. pytest tests/unit/ -v --tb=short; \
	else \
		PYTHONPATH=. python3 -m pytest tests/unit/ -v --tb=short; \
	fi
	@echo " Unit-тесты завершены"

test-all: ## Запустить все тесты (unit + integration)
	@echo "Запуск всех тестов"
	@if [ -f venv/bin/activate ]; then \
		. venv/bin/activate && PYTHONPATH=. pytest tests/ -v --tb=short; \
	else \
		PYTHONPATH=. python3 -m pytest tests/ -v --tb=short; \
	fi
	@echo "Все тесты завершены"

load-test: ## Запустить нагрузочное тестирование (Locust)
	@echo "⚡ Запуск Locust..."
	@echo "   Web UI: http://localhost:8089"
	@echo "   Рекомендуемые настройки: 100 users, spawn rate 10"
	@echo ""
	@if [ -f venv/bin/python3 ]; then \
		venv/bin/python3 -m locust -f locustfile.py --host http://localhost:8000; \
	else \
		python3 -m locust -f locustfile.py --host http://localhost:8000; \
	fi

demo-dlq: ## Заполнить DLQ тестовыми сообщениями для демонстрации DAG
	@echo "Заполнение DLQ тестовыми данными для демонстрации DAG..."
	@if [ -f venv/bin/python3 ]; then \
		PYTHONPATH=. venv/bin/python3 scripts/populate_dlq.py; \
	else \
		PYTHONPATH=. python3 scripts/populate_dlq.py; \
	fi
