# Apache ActiveMQ Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` to publish normalized market data events to Apache ActiveMQ.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Permit routing to multiple queues/topics based on payload type while sharing a base configuration.
- Support operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Reject misconfigured ActiveMQ routing entries where payload case names do not match supported Bronze payloads.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow brokers do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient ActiveMQ errors with bounded backoff.
- Support static JMS headers so operators can stamp deployment metadata on every message.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept an `eventBus` configuration object for ActiveMQ.
- Introduce an internal publisher abstraction compatible with existing event bus interfaces.
- ActiveMQ implementation should use `amqplib` for Node.js with configurable connection options.
- Expose tuning via CLI env/flags (e.g. `--activemq-url`, `--activemq-exchange`).
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management

- Use existing Buf CLI workflows and TypeScript generation via `@bufbuild/protoc-gen-es`.
- Generated TypeScript code under `src/generated` feeds publisher encoding.

## Testing Strategy

- Add integration test that spins up an ActiveMQ broker (Testcontainers) and asserts published messages decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `ActiveMQEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Queue/topic routing allows mapping Bronze payload cases to dedicated destinations while keeping a base fallback.
- Static header maps configured via `--activemq-static-headers` become constant JMS headers on every published message.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--activemq-*` options (url, exchange, routing, etc.) to enable publishing.
- Publishing is optional; when ActiveMQ options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--activemq-include-payloads` drop unlisted cases before they reach batching logic, minimizing broker load.
