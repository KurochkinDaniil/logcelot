#!/bin/bash
set -e

# Скрипт инициализации Superset с поддержкой ClickHouse
# Выполняется ВНУТРИ контейнера при запуске

echo "=== Инициализация Superset ==="

# Ждём готовности PostgreSQL
echo "Ожидание готовности PostgreSQL..."
until PGPASSWORD=${DATABASE_PASSWORD} psql -h "${DATABASE_HOST}" -U "${DATABASE_USER}" -d "${DATABASE_DB}" -c '\q' 2>/dev/null; do
  echo "PostgreSQL недоступен - ждём..."
  sleep 2
done
echo "✓ PostgreSQL готов"

# Проверка, нужна ли инициализация БД
echo "Проверка инициализации БД..."
if ! superset db check 2>/dev/null; then
    echo "Первый запуск - инициализация БД..."
    
    # Обновление БД
    echo "Обновление базы данных..."
    superset db upgrade
    
    # Создание администратора
    echo "Создание администратора..."
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@logcelot.local \
        --password admin || echo "⚠️  Администратор уже существует"
    
    # Инициализация Superset
    echo "Инициализация Superset..."
    superset init
    
    echo "=== Инициализация завершена ==="
else
    echo "БД уже инициализирована"
fi

echo ""
echo "🚀 Запуск Superset..."
echo "   URL: http://localhost:8088"
echo "   Логин: admin / Пароль: admin"
echo ""
echo "📊 Для подключения к ClickHouse используйте:"
echo "   URI: clickhousedb://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/${CLICKHOUSE_DB}"
echo ""

# Запуск сервера
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger