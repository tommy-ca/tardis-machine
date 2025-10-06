# Apache Pulsar Event Bus

## Goal

- Extend `tardis-machine` to publish normalized market data events to Apache Pulsar.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.
- Use Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Support topic routing based on payload type while sharing a base configuration.
- Support operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow brokers do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient Pulsar errors with bounded backoff.
- Support configurable Pulsar compression types to balance throughput vs CPU usage.
- Support static Pulsar properties so operators can stamp deployment metadata on every message.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept Pulsar configuration.
- Pulsar implementation should use `pulsar-client` for Node.js with configurable client/producer options.
- Expose compression tuning via CLI env/flags (e.g. `--pulsar-compression`).
- Publish WS control errors as Bronze `ControlError` payloads.

## Schema Management

- Use existing Buf-generated TypeScript code for encoding.
- Maintain compatibility with existing schema tests.

## Testing Strategy

- Add integration test that spins up a Pulsar broker (Testcontainers) and asserts published messages decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `PulsarEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Topic routing allows mapping Bronze payload cases to dedicated Pulsar topics while keeping a base fallback topic.
- Static property maps configured via `--pulsar-static-properties` become constant Pulsar properties on every published message.
- Publishing is optional; when Pulsar options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--pulsar-include-payloads` drop unlisted cases before they reach batching logic, minimizing broker load.
