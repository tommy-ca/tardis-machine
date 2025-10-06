# Redis Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` so it can publish normalized market data events to Redis streams.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Support routing to multiple streams based on payload type while sharing a base configuration.
- Permit operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Reject misconfigured Redis stream routing entries where payload case names do not match supported Bronze payloads.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow Redis do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient Redis errors with bounded backoff.
- Support static headers so operators can stamp deployment metadata on every record.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept a `redis` configuration object.
- Redis implementation should use `redis` library for Node.js.
- Expose host, port, password via CLI env/flags (e.g. `--redis-host`).
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management

- Reuse existing Buf-generated TypeScript codecs for encoding.
- Generated TypeScript code should live under `src/generated` (git committed) and feed publisher encoding.

## Testing Strategy

- Add integration test that spins up a Redis server (Testcontainers) and asserts published records decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `RedisEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Stream routing allows mapping Bronze payload cases to dedicated Redis streams while keeping a base fallback stream.
- Stream key templates can be used for dynamic stream key generation based on placeholders for exchange, symbol, payload case, and normalized meta entries.
- Static headers configured via `--redis-static-headers` become constant metadata on every published record.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--redis-*` options (host, port, stream, stream routing, password) to enable publishing.
- Publishing is optional; when Redis options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--redis-include-payloads` drop unlisted cases before they reach batching logic, minimizing Redis load.
