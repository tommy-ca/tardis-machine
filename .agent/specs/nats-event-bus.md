# Nats Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` so it can publish normalized market data events to NATS servers.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Support routing to multiple subjects based on payload type while sharing a base configuration.
- Permit operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Reject misconfigured NATS subject routing entries where payload case names do not match supported Bronze payloads.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow servers do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient NATS errors with bounded backoff.
- Support static headers so operators can stamp deployment metadata on every record.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept a `nats` configuration object.
- NATS implementation should use `nats` library for Node.js.
- Expose servers, user, pass via CLI env/flags (e.g. `--nats-servers`).
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management

- Reuse existing Buf-generated TypeScript codecs for encoding.
- Generated TypeScript code should live under `src/generated` (git committed) and feed publisher encoding.

## Testing Strategy

- Add integration test that spins up a NATS server (Testcontainers) and asserts published records decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `NatsEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Subject routing allows mapping Bronze payload cases to dedicated NATS subjects while keeping a base fallback subject.
- Subject templates can be used for dynamic subject generation based on placeholders for exchange, symbol, payload case, and normalized meta entries.
- Static headers configured via `--nats-static-headers` become constant headers on every published record.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--nats-*` options (servers, subject, subject routing, user, pass) to enable publishing.
- Publishing is optional; when NATS options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--nats-include-payloads` drop unlisted cases before they reach batching logic, minimizing server load.
