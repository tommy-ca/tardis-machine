# Google Cloud Pub/Sub Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` so it can publish normalized market data events to Google Cloud Pub/Sub.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Support routing to multiple topics based on payload type while sharing a base configuration.
- Permit operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Reject misconfigured Pub/Sub routing entries where payload case names do not match supported Bronze payloads.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow topics do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient Pub/Sub errors with bounded backoff.
- Support static attributes so operators can stamp deployment metadata on every message.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept a `pubsub` configuration object.
- Google Cloud Pub/Sub implementation should use @google-cloud/pubsub for Node.js.
- Expose project id, topic name via CLI env/flags (e.g. `--pubsub-project-id`, `--pubsub-topic`).
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management

- Reuse existing Buf-generated TypeScript codecs for encoding.
- Generated TypeScript code should live under `src/generated` (git committed) and feed publisher encoding.

## Testing Strategy

- Add integration test that uses Pub/Sub emulator and asserts published messages decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `PubSubEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Topic routing allows mapping Bronze payload cases to dedicated Pub/Sub topics while keeping a base fallback topic.
- Ordering key templates let operators control ordering keys by using placeholders for exchange, symbol, payload case, and normalized meta entries.
- Static attributes configured via `--pubsub-static-attributes` become constant Pub/Sub attributes on every published message.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--pubsub-*` options to enable publishing.
- Publishing is optional; when Pub/Sub options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--pubsub-include-payloads` drop unlisted cases before they reach batching logic, minimizing topic load.
