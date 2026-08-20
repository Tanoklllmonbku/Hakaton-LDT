<<<<<<< Updated upstream
=======
# 15.08.2026
Changes:
1. Added new documents for architecture
2. Added helpful commands
3. Reworked readme
Plans for 16.08.2026:
1. Start writing arch for backend, services and frontend
2. Start development (API)
# 19.08.2026
Changes:
1. Final arch of system
2. Frontend-features
3. Backend architecture
Plans for 20-21.08.2026:
Чек-лист архитектурных правок (Architecture Refinement)
1.1 Бэкенд: Ядро и DDD (High Priority)

    Добавить таблицу idempotency_keys: 
        Описать схему: key (PK), task_id, created_at, expires_at.
        Указать, что проверка ключа происходит в Middleware/API Gateway до начала бизнес-транзакции.
    Уточнить Multi-tenancy / Data Isolation:
        Добавить поле tenant_id (или user_id) во все агрегаты (Task, Document, VoiceNote).
        Прописать правило: все репозитории обязаны фильтровать данные по этому полю на уровне SQL-запроса.
    Асинхронная компенсация в Saga:
        Уточнить, что очистка временных файлов в MinIO при ошибке (FailTask) происходит через асинхронное событие task.compensation.requested, чтобы не блокировать основную транзакцию статуса задачи.

1.2 Инфраструктура и Оркестрация (Medium Priority)

    Разделение процессов (Long-term Note):
        В раздел "API Gateway" добавить пометку: "Для MVP компоненты объединены в один под. В Roadmap v2.0 запланировано выделение Saga Orchestrator, Notification Relay и Outbox Relay в отдельные Deployment'ы для независимого масштабирования через KEDA."
    Масштабирование Outbox Relay:
        Указать, что для outbox-relay используется либо статическое количество реплик (HA), либо KEDA PostgreSQL scaler (по количеству записей со статусом pending), так как Kafka-лаг здесь не применим.
    Resource Limits для ML-воркеров:
        В требованиях к инфраструктуре явно прописать необходимость установки requests/limits по памяти для Python-воркеров (OCR, Voice) для предотвращения OOMKill.

1.3 Работа с событиями и Kafka (Medium Priority)

    Correlation ID:
        Добавить требование передавать correlation_id (равный task_id или UUID запроса) в заголовках всех Kafka-сообщений для сквозной трассировки и фильтрации логов.
    Операционализация DLQ:
        Добавить подраздел "Operational DLQ Handling":
            Настройка алертов в Slack/PagerDuty через Prometheus (kafka_dlq_messages_total > 0).
            Описание плана по созданию CLI-инструмента или админ-эндпоинта для Replay сообщений из DLQ.
    Уточнение триггера K9:
        Разъяснить роль file.upload.completed: он используется для фоновой проверки целостности (Audit), а запуск OCR инициируется синхронно Gateway после confirm_upload.

1.4 Frontend (FSD) (Low Priority)

    WebSocket Reconnection Strategy:
        В shared/lib/websocket описать стратегию exponential backoff при обрыве связи.
        Добавить логику синхронизации состояния: при восстановлении соединения фронтенд должен делать GET /api/v1/tasks/{task_id}, чтобы получить актуальный статус, если пропустил события.
    Presigned URL Expiration:
        В фиче upload-file предусмотреть обработку истечения срока жизни URL (запрос нового URL, если загрузка не началась или прервалась).

1.5 Стилистика и Детали (Low Priority)

    Индексация Outbox:
        Проверить порядок колонок в индексе outbox_pending_idx. Рекомендуется (status, available_at, created_at) для оптимальной работы с SKIP LOCKED.
    Identity Context:
        Добавить пометку, что UserRepository реализован как порт, что позволит в будущем заменить in-process реализацию на gRPC-клиент без изменения Domain-слоя.
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======

# 20.08.2026
Changes:
1. Corrections on backend by 19.08.2026
2. Event bus declaration

plans for 21-23.08.2026:
1. Complete arch docs (MVP, changes in process of developing)
2. Start writing API gateway
3. Adapting of service code from previos project to EDA patterns. Adding K8s-compatability.
>>>>>>> Stashed changes
