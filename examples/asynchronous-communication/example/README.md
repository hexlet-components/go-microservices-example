# Асинхронный аудит

Из каталога example команда bash setup.sh поднимает PostgreSQL и Kafka и создаёт топики. В отдельных терминалах команды bash run.sh site, bash run.sh api, bash run.sh checks и bash run.sh reports запускают процессы. Команда bash run.sh token выдаёт учебный JWT. Роль api слушает8080, reports8082, site8090. Для независимого опыта используется пустое локальное окружение; схема снимка содержит последовательную запись и публикацию, outbox здесь ещё не добавлен.

Интеграционный TestRealPipeline использует AUDIT_TEST_BROKERS, AUDIT_TEST_REQUESTS_DSN, AUDIT_TEST_CHECKS_DSN и AUDIT_TEST_REPORTS_DSN. Каждый запуск создаёт собственные схемы, топики и группы.
