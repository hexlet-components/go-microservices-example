# Учебное приложение аудита

Пример запускается на Go с локальным PostgreSQL. Приложение проверяет только локальный учебный сайт на `http://127.0.0.1:8090`. Обход ограничен пятью страницами. Правила `MissingTitle` и `MissingDescription` и загрузчик уже реализованы.

В первом терминале создаётся база и запускается учебный сайт.

```bash
createdb go_microservices_demo
go run ./cmd/audit -site
```

Во втором терминале из каталога приложения запускается API.

```bash
go run ./cmd/setup
. ./local.env
go run ./cmd/audit
```

Команда `go run ./cmd/setup` создаёт _local.env_ по шаблону _local.env.example_ и генерирует ключ подписи токена для локального примера. При отличающемся подключении измените `DATABASE_URL`.

В третьем терминале из того же каталога создаётся аудит.

```bash
. ./local.env
token=$(go run ./cmd/audit -token)
curl -sS -X POST http://127.0.0.1:8080/audits \
  -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: first' \
  -d '{"url":"http://127.0.0.1:8090/"}'
```

Поле `audit_id` из ответа подставляется в маршрут `GET /audits/{audit_id}/report`. Запрос чтения использует тот же заголовок `Authorization`. Ответ содержит `ready=true`, `missing-title` для _/about_ и `http-404` для _/missing_.

Тесты загрузчика выполняются без БД. Для интеграционных проверок создаётся отдельная тестовая база.

```bash
createdb go_microservices_test
AUDIT_TEST_DATABASE_URL='postgres://localhost/go_microservices_test?sslmode=disable' go test ./...
```

Без адреса тестовой БД интеграционные тесты пропускаются с сообщением. Каждый тест PostgreSQL создаёт свою случайно именованную схему с префиксом `audit_test_`. Проверки не очищают таблицы приложения. После окончания работы всю отдельную тестовую базу можно удалить вручную.
