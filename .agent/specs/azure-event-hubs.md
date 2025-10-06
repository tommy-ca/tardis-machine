# Azure Event Hubs Event Bus Publishing Requirements

## Goal

- Extend `tardis-machine` so it can publish normalized market data events to Azure Event Hubs.
- Ensure published payloads follow the Buf-managed Protobuf schemas located under `schemas/proto`.

## Functional Requirements

- Allow operators to enable publishing through server configuration/CLI and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Support routing to multiple event hubs based on payload type while sharing a base configuration.
- Permit operator-defined allow lists so only selected Bronze payload cases are published when desired.
- Reject misconfigured Event Hubs routing entries where payload case names do not match supported Bronze payloads.
- Capture ingest metadata (producer id, timestamps) required by schemas.
- Provide backpressure handling so slow hubs do not block core HTTP/WS responses indefinitely.
- Deliver at-least-once publishing semantics; retry transient Event Hubs errors with bounded backoff.
- Support static properties so operators can stamp deployment metadata on every event.

## Non-Functional Requirements

- Preserve existing API performance characteristics; publishing must be asynchronous relative to response streaming.
- TDD emphasis with integration-level coverage (prefer fixtures over mocks).
- Configuration defaults should keep publishing disabled unless explicitly turned on.
- Provide operational logging for publish status and error scenarios.
- Ensure graceful shutdown drains publisher queues before process exit.

## Interfaces

- Extend `TardisMachine` constructor options to accept an `eventHubs` configuration object.
- Azure Event Hubs implementation should use @azure/event-hubs for Node.js.
- Expose connection string, event hub name via CLI env/flags (e.g. `--event-hubs-connection-string`).
- Publish WS control errors as Bronze `ControlError` payloads so downstream systems observe retry behavior.

## Schema Management

- Reuse existing Buf-generated TypeScript codecs for encoding.
- Generated TypeScript code should live under `src/generated` (git committed) and feed publisher encoding.

## Testing Strategy

- Add integration test that spins up an Azure Event Hubs emulator or mock and asserts published events decode via generated types.
- Reuse existing normalized data fixtures to assert correct payload mapping.
- Add regression test ensuring publishing can be toggled off without side effects.

## Implementation Notes

- `AzureEventHubsEventBus` batches bronze events using Buf-generated codecs, retrying transient send failures with exponential backoff.
- Event Hub routing allows mapping Bronze payload cases to dedicated Event Hubs while keeping a base fallback hub.
- Partition key templates let operators control partition keys by using placeholders for exchange, symbol, payload case, and normalized meta entries.
- Static properties configured via `--event-hubs-static-properties` become constant Event Hubs properties on every published event.
- HTTP `/replay-normalized` and WS `/ws-replay-normalized`, `/ws-stream-normalized` pipe normalized messages into the publisher with request/session metadata.
- CLI exposes `--event-hubs-*` options to enable publishing.
- Publishing is optional; when Event Hubs options are missing, the server behaves exactly as before.
- Payload filtering allow-lists via `--event-hubs-include-payloads` drop unlisted cases before they reach batching logic, minimizing hub load.</content>
  </xai:function_call">Azure Event Hubs spec created
