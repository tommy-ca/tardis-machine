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
- [x] Implement Kinesis publishing for normalized events.
- [x] Wire Kinesis into TardisMachine event bus creation.
- [x] Add integration test verifying Kinesis publish using LocalStack.
- [x] Add regression test ensuring Kinesis publishing can be toggled off.
- [x] Implement Nats publishing for normalized events.
- [x] Wire Nats into TardisMachine event bus creation.
- [x] Add integration test verifying Nats publish using Testcontainers.
- [x] Add regression test ensuring Nats publishing can be toggled off.
- [x] Implement RabbitMQ publishing for normalized events.
- [x] Wire RabbitMQ into TardisMachine event bus creation.
- [x] Add integration test verifying RabbitMQ publish using Testcontainers.
- [x] Add regression test ensuring RabbitMQ publishing can be toggled off.
- [x] Implement Silver Kafka publishing for normalized events.
- [x] Wire Silver Kafka into TardisMachine event bus creation.
- [x] Add integration test verifying Silver Kafka publish using Testcontainers.
- [x] Add CLI support for Silver Kafka event bus options.
- [x] Add unit tests for Silver Kafka config parsing.
- [x] Implement Redis publishing for normalized events.
- [x] Wire Redis into TardisMachine event bus creation.
- [x] Add integration test verifying Redis publish using Testcontainers.
- [x] Add regression test ensuring Redis publishing can be toggled off.
- [x] Add CLI support for Redis event bus options.
- [x] Add unit tests for Redis config parsing.
- [x] Implement Silver Redis publishing for normalized events.
- [x] Wire Silver Redis into TardisMachine event bus creation.
- [x] Add integration test verifying Silver Redis publish using Testcontainers.
- [x] Add CLI support for Silver Redis event bus options.
- [x] Add unit tests for Silver Redis config parsing.

## New Feature: Schema Registry Support for Kafka

- [x] Research Schema Registry client libraries for Node.js
- [x] Add schema registry configuration to KafkaEventBusConfig
- [x] Implement schema registration in KafkaEventBus.start()
- [x] Modify publish to use schema ID in message value
- [x] Add E2E test with schema registry using Testcontainers
- [x] Update documentation for schema registry usage

## New Feature: Pulsar Event Bus

- [x] Implement Pulsar publishing for normalized events.
- [x] Wire Pulsar into TardisMachine event bus creation.
- [x] Add integration test verifying Pulsar publish using Testcontainers.
- [x] Add CLI support for Pulsar event bus options.
- [x] Add unit tests for Pulsar config parsing.
- [x] Add regression test ensuring Pulsar publishing can be toggled off.
- [x] Implement Silver Pulsar publishing for normalized events.
- [x] Wire Silver Pulsar into TardisMachine event bus creation.
- [x] Add integration test verifying Silver Pulsar publish using Testcontainers.
- [x] Add CLI support for Silver Pulsar event bus options.
- [x] Add unit tests for Silver Pulsar config parsing.

## New Feature: SQS Event Bus

- [x] Implement SQS publishing for normalized events.
- [x] Wire SQS into TardisMachine event bus creation.
- [x] Add integration test verifying SQS publish using LocalStack.
- [x] Add CLI support for SQS event bus options.
- [x] Add unit tests for SQS config parsing.
- [x] Add regression test ensuring SQS publishing can be toggled off.
- [x] Implement Silver SQS publishing for normalized events.
- [x] Wire Silver SQS into TardisMachine event bus creation.
- [x] Add integration test verifying Silver SQS publish using LocalStack.
- [x] Add CLI support for Silver SQS event bus options.
- [x] Add unit tests for Silver SQS config parsing.

## New Feature: Azure Event Hubs Event Bus

- [x] Implement Azure Event Hubs publishing for normalized events.
- [x] Wire Azure Event Hubs into TardisMachine event bus creation.
- [x] Add integration test verifying Azure Event Hubs publish using mocked producer.
- [x] Add CLI support for Azure Event Hubs event bus options.
- [x] Add unit tests for Azure Event Hubs config parsing.
- [x] Add regression test ensuring Azure Event Hubs publishing can be toggled off.
- [x] Implement Silver Azure Event Hubs publishing for normalized events.
- [x] Wire Silver Azure Event Hubs into TardisMachine event bus creation.
- [x] Add integration test verifying Silver Azure Event Hubs publish using mocked producer.
- [x] Add CLI support for Silver Azure Event Hubs event bus options.
- [x] Add unit tests for Silver Azure Event Hubs config parsing.
