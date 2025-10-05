# Event Bus Publishing

- Publish normalized market data to external buses (Kafka first) using Buf-managed protobuf schemas.
- Encode every outbound message as `lakehouse.bronze.v1.NormalizedEvent` with binary payload.
- Emphasize at-least-once semantics; retry batches on transient failures and requeue on hard failures.
- Support topic overrides per payload case and pass through source metadata (request/session identifiers, transport).
- Provide tunables for batching (`maxBatchSize`, `maxBatchDelayMs`) and secure connection options (SSL + SASL).
- Enable configurable Kafka compression codecs to optimize transport costs.
- Allow operators to define static Kafka headers for consistent deployment metadata.
