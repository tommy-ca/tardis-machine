# TODO

- [x] Implement Kafka publishing for normalized events.
- [x] Generate Buf-based TypeScript codecs under src/generated.
- [x] Wire CLI and config for Kafka options with defaults disabled.
- [x] Add integration test verifying Kafka publish using Testcontainers.
- [x] Update documentation for event bus capabilities.
- [x] Add Kafka close-drain regression coverage.
- [x] Document Kafka maintenance checklist (brokers, batching, headers).
- [x] Add configurable Kafka key template support for partition keys.
- [x] Expose Kafka ack/idempotent configuration knobs.
- [x] Document Kafka ack/idempotent tuning guidance.
- [x] Harden Kafka config parsing for boolean env values.
- [x] Add regression tests covering blank topic inputs.
- [x] Add Kafka static header publishing capability.
