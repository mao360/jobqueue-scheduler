# jobqueue-scheduler

Приём задач от клиентов, доклад воркерам, стрим прогресса. Хранилище — in-memory
(перезапуск = потеря очереди). Транспорт — connect-go поверх `net/http`,
один порт говорит на gRPC / gRPC-Web / Connect.

## Слои

- `domain` - модели
- `usecase` - бизнес-логика, интерфейсы зависимостей
- `repository/memory` - хранилище под
- `eventbus` - pub/sub для WatchJob
- `gateway` - bidi-стрим с воркерами
- `transport/connectrpc` - хендлеры + мапперы
- `app` - wiring, h2c-сервер, graceful shutdown

## Запуск

- Локально: `make run`
- Docker: `docker compose up`
- Тесты: `make test, make race`

## Пример (Connect, JSON)

    curl -X POST http://localhost:8080/jobqueue.v1.SchedulerService/CreateJob \
      -H "Content-Type: application/json" \
      -d '{"taskType":"echo","payload":"aGVsbG8=","timeoutSeconds":30}'