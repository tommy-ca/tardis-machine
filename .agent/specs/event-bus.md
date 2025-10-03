# Event Bus Publishing Requirements

## Goal
- Extend `tardis-machine` so it can publish normalized market data events to external event buses (starting with Apache Kafka).
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements
- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Permit routing to multiple topics based on payload type while sharing a base configuration.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow brokers do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient Kafka errors with bounded backoff.

## Non-Functional Requirements
- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces
- Extend `TardisMachine` constructor options to accept an `eventBus` configuration object.
- Introduce an internal publisher abstraction so additional backends (e.g., AWS Kinesis) can be added later.
- Kafka implementation should use `kafkajs` for Node.js with configurable client/batch options.
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management
- Introduce Buf CLI workflows (format, lint, generate) and TypeScript generation via `@bufbuild/protoc-gen-es` to avoid runtime reflection encoding.
- Generated TypeScript code should live under `src/generated` (git committed) and feed publisher encoding.
- Maintain compatibility tests under `test/proto` to ensure schema and generated code fidelity.

## Testing Strategy
- Add integration test that spins up a Kafka broker (Testcontainers) and asserts published records decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes (2025-10-03)
- `KafkaEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Topic routing allows mapping Bronze payload cases to dedicated Kafka topics while keeping a base fallback topic.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--kafka-*` options (brokers, topic, topic routing, client id, SSL, SASL) to enable publishing.
- Publishing is optional; when Kafka options are missing, the server behaves exactly as before.
