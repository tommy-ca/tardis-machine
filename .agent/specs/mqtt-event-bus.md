# MQTT Event Bus Publishing

## Goal

- Extend `tardis-machine` to publish normalized market data to MQTT brokers using Buf-managed Protobuf schemas.
- Support both Bronze and Silver layer publishing.

## Functional Requirements

- Allow operators to enable MQTT publishing through CLI flags and environment variables.
- Support Bronze layer envelope (`lakehouse.bronze.v1.NormalizedEvent`) for outgoing messages.
- Emit events for normalized HTTP replay, normalized WebSocket replay, and normalized real-time streaming flows.
- Permit routing to multiple topics based on payload type.
- Support operator-defined allow lists for selected payload cases.
- Capture ingest metadata required by schemas.
- Provide backpressure handling for slow brokers.
- Deliver at-least-once publishing semantics with retries.
- Support MQTT QoS levels and retain flags.

## Non-Functional Requirements

- Preserve existing API performance; publishing asynchronous relative to responses.
- TDD emphasis with integration-level coverage.
- Configuration defaults keep publishing disabled unless explicitly enabled.
- Provide operational logging for publish status and errors.
- Ensure graceful shutdown drains publisher queues.

## Interfaces

- Extend `TardisMachine` constructor for MQTT configuration.
- Introduce MQTT implementation using a Node.js MQTT client library.
- Expose CLI options like `--mqtt-url`, `--mqtt-topic`, etc.
- Support topic templates with placeholders.

## Schema Management

- Use existing Buf-generated codecs for encoding.
- Maintain compatibility with Bronze and Silver schemas.

## Testing Strategy

- Add integration test with MQTT broker using Testcontainers.
- Reuse existing fixtures for payload mapping.
- Add regression test for toggling off publishing.
