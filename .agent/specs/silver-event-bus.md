# Silver Layer Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` to publish Silver layer records to external event buses (starting with Apache Kafka).
- Silver records provide analytics-ready data with fixed scales and strong typing, complementing the existing Bronze layer.

## Functional Requirements

- Allow operators to enable Silver publishing through server configuration/CLI and environment variables.
- Support Silver layer records (`lakehouse.silver.v1.*Record`) for outgoing messages.
- Emit records for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Permit routing to multiple topics based on record type while sharing a base configuration.
- Support operator-defined allow lists so only selected Silver record types are published when desired.
- Capture ingest metadata (timestamps, origin) required by Silver schemas.
- Provide backpressure handling so slow brokers do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient Kafka errors with bounded backoff.
- Allow specifying Kafka compression codecs to balance throughput vs CPU usage.
- Support static Kafka headers so operators can stamp deployment metadata on every record.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept Silver event bus configuration.
- Introduce Silver publisher abstraction compatible with existing event bus interfaces.
- Kafka implementation should use `kafkajs` for Node.js with configurable client/batch options.
- Expose compression tuning via CLI env/flags (e.g. `--kafka-silver-compression`).
- Publish WS control errors as appropriate Silver control payloads if applicable.

## Schema Management

- Use existing Buf CLI workflows and TypeScript generation via `@bufbuild/protoc-gen-es`.
- Generated TypeScript code under `src/generated` feeds Silver publisher encoding.
- Maintain compatibility tests under `test/proto` to ensure schema and generated code fidelity.
- Support Confluent Schema Registry for Kafka publishing, registering Protobuf schemas and encoding messages with schema IDs for better schema evolution.

## Testing Strategy

- Add integration test that spins up a Kafka broker (Testcontainers) and asserts published Silver records decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping to Silver scales.
- Add regression test ensuring Silver publishing can be toggled off without side effects.

## Implementation Notes

- `SilverNormalizedEventEncoder` converts normalized messages to Silver records with fixed scales (e.g., price_e8).
- Topic routing allows mapping Silver record types to dedicated Kafka topics while keeping a base fallback topic.
- Kafka key templates let operators control partition keys by using placeholders for exchange, symbol, record type, and metadata.
- Static header maps configured via `--kafka-silver-static-headers` become constant Kafka headers on every published record.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the Silver publisher with request/session metadata.
- CLI exposes `--kafka-silver-*` options (brokers, topic, topic routing, client id, SSL, SASL) to enable publishing.
- Publishing is optional; when Silver Kafka options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--kafka-silver-include-records` drop unlisted record types before they reach batching logic, minimizing broker load.
- Schema Registry support allows registering Protobuf schemas and encoding messages with schema IDs for compatibility and evolution (optional via `--kafka-silver-schema-registry-url`).
