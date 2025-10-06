# RabbitMQ Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` so it can publish normalized market data events to RabbitMQ exchanges.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Support routing to multiple exchanges/routing keys based on payload type while sharing a base configuration.
- Permit operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Reject misconfigured RabbitMQ routing entries where payload case names do not match supported Bronze payloads.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow brokers do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient RabbitMQ errors with bounded backoff.
- Support static headers so operators can stamp deployment metadata on every record.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept a `rabbitmq` configuration object.
- RabbitMQ implementation should use `amqplib` library for Node.js.
- Expose url, exchange, exchangeType via CLI env/flags (e.g. `--rabbitmq-url`).
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management

- Reuse existing Buf-generated TypeScript codecs for encoding.
- Generated TypeScript code should live under `src/generated` (git committed) and feed publisher encoding.

## Testing Strategy

- Add integration test that spins up a RabbitMQ server (Testcontainers) and asserts published records decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `RabbitMQEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Exchange routing allows mapping Bronze payload cases to dedicated RabbitMQ exchanges while keeping a base fallback exchange.
- Routing key templates can be used for dynamic routing key generation based on placeholders for exchange, symbol, payload case, and normalized meta entries.
- Static headers configured via `--rabbitmq-static-headers` become constant headers on every published record.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--rabbitmq-*` options (url, exchange, exchange type, routing key template) to enable publishing.
- Publishing is optional; when RabbitMQ options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--rabbitmq-include-payloads` drop unlisted cases before they reach batching logic, minimizing broker load.
