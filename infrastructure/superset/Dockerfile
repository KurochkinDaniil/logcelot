FROM apache/superset:4.1.4

# Устанавливаем зависимости
USER root

# Установка PostgreSQL client для проверки готовности БД
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Установка драйвера ClickHouse
RUN pip install --no-cache-dir clickhouse-connect

# Копируем скрипт инициализации
COPY init.sh /app/superset-init.sh
RUN chmod +x /app/superset-init.sh

# Переключаемся на пользователя superset
USER superset

# Точка входа
ENTRYPOINT ["/bin/bash", "/app/superset-init.sh"]