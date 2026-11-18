# Микросервисы на Go

В репозитории находятся запускаемые примеры системы аудита сайтов для курса «Разработка микросервисов на Go». Каждый каталог в examples соответствует самостоятельному учебному снимку. Они используют общие предметные имена, но запускаются независимо. Внутри каждого снимка находятся его зависимости, команды и тесты.

Готовые архивы уроков находятся в [релизе курса](https://github.com/hexlet-components/go-microservices-example/releases/tag/course-2026-11-17). После распаковки команды выполняются из каталога, названного в уроке. Примеры с локальными токенами и сертификатами подготавливаются командой go run ./cmd/setup из своего каталога. Команда создаёт настройки для запуска на localhost. Сгенерированные ключи и local.env не входят в репозиторий и архивы.

## Проверка исходников

Для проверки нужны Go и Docker Compose. Общая конфигурация поднимает три PostgreSQL и Kafka на локальных портах 55441, 55442, 55443 и 29092.

```bash
docker compose up -d --wait
bash scripts/check.sh
```

Скрипт задаёт адреса зависимостей всем снимкам. Каждый интеграционный тест создаёт свои схемы, топики и группы, затем удаляет только их. Проверяются реальные gRPC и TLS, повторная доставка Kafka, SQL-транзакции, остановка процессов и выпуск одной версии при прежних соседях. Диагностический пример использует явно учебные данные, а не измерение производительности всего приложения.

## Снимки

| Пример | Каталог |
| --- | --- |
| asynchronous-communication | _examples/asynchronous-communication_ |
| basic-application | _examples/basic-application_ |
| contract-evolution | _examples/contract-evolution_ |
| data-and-events | _examples/data-and-events_ |
| database-evolution | _examples/database-evolution_ |
| distributed-tracing | _examples/distributed-tracing_ |
| fault-isolation | _examples/fault-isolation_ |
| idempotent-consumer | _examples/idempotent-consumer_ |
| independent-release | _examples/independent-release_ |
| message-failures | _examples/message-failures_ |
| message-processing | _examples/message-processing_ |
| microservice-testing | _examples/microservice-testing_ |
| partial-failures | _examples/partial-failures_ |
| retries | _examples/retries_ |
| service-communication | _examples/service-communication_ |
| service-configuration | _examples/service-configuration_ |
| service-extraction | _examples/service-extraction_ |
| service-lifecycle | _examples/service-lifecycle_ |
| service-security | _examples/service-security_ |
| service-telemetry | _examples/service-telemetry_ |
| synchronous-communication | _examples/synchronous-communication_ |

В microservice-testing находятся управляющие файлы объединённого набора. Его архив собирается из исходников соответствующих снимков, чтобы проверки использовали те же файлы.

## Архивы

Манифест archives.json задаёт соответствие уроков, исходников и корневого каталога архива. Скрипт использует только стандартную библиотеку Python и создаёт файлы в dist вместе с SHA256SUMS.

```bash
python3 scripts/package.py
```
