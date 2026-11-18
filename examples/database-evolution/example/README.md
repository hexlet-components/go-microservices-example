# Совместная работа версий

Локальная БД запускается командой docker compose up -d --wait. Из каталога example тесты выполняются командой AUDIT_TEST_DATABASE_URL='postgres://migration_demo:migration-local@localhost:55439/migration_demo?sslmode=disable' go test -race -v ./.... Каждый тест создаёт случайную собственную схему и удаляет её после завершения. TestMixedVersionsAndRollback запускает два HTTP-сервера на разных портах, печатает адреса, моделирует старую запись после новой, повторное заполнение и откат. TestContractClosesRollbackWindow проверяет границу удаления прежнего столбца.
