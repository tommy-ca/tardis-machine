# Tardis Machine Server

[![Version](https://img.shields.io/npm/v/tardis-machine.svg)](https://www.npmjs.org/package/tardis-machine)

[Tardis-machine](https://docs.tardis.dev/api/tardis-machine) is a locally runnable server with built-in data caching that uses [Tardis.dev HTTP API](https://docs.tardis.dev/api/http) under the hood. It provides both **tick-level historical** and **consolidated real-time cryptocurrency market data** via HTTP and WebSocket APIs. Available via [npm](https://docs.tardis.dev/api/tardis-machine#npm) and [Docker](https://docs.tardis.dev/api/tardis-machine#docker).
<br/>
<br/>
![overview](<https://gblobscdn.gitbook.com/assets%2F-LihqQrMLN4ia7KgxAzi%2F-M2YHT2t5D3zrOL7TEyt%2F-M2YHurMxtHTW9ak0V9I%2Fexcalidraw-2020316131859%20(1).png?alt=media&token=11f81814-6b3e-4254-8047-cb03c433bcde>)
<br/>

## Features

- efficient data replay API endpoints returning historical market data for whole time periods \(in contrast to [Tardis.dev HTTP API](https://docs.tardis.dev/api/http) where single call returns data for single minute time period\)

- [exchange-native market data APIs](https://docs.tardis.dev/api/tardis-machine#exchange-native-market-data-apis)
  - tick-by-tick historical market data replay in [exchange-native format](https://docs.tardis.dev/faq/data#what-is-a-difference-between-exchange-native-and-normalized-data-format)

  - [HTTP](https://docs.tardis.dev/api/tardis-machine#http-get-replay-options-options) and [WebSocket](https://docs.tardis.dev/api/tardis-machine#websocket-ws-replay-exchange-exchange-and-from-fromdate-and-to-todate) endpoints

  - [WebSocket API](https://docs.tardis.dev/api/tardis-machine#websocket-ws-replay-exchange-exchange-and-from-fromdate-and-to-todate) providing historical market data replay from any given past point in time with the same data format and 'subscribe' logic as real-time exchanges' APIs - in many cases **existing exchanges' WebSocket clients can be used to connect to this endpoint**

- [normalized market data APIs](https://docs.tardis.dev/api/tardis-machine#normalized-market-data-apis)
  <br/>
  - consistent format for accessing market data across multiple exchanges

  - [HTTP](https://docs.tardis.dev/api/tardis-machine#http-get-replay-normalized-options-options) and [WebSocket](https://docs.tardis.dev/api/tardis-machine#websocket-ws-replay-normalized-options-options) endpoints

  - synchronized [historical market data replay across multiple exchanges](https://docs.tardis.dev/api/tardis-machine#http-get-replay-normalized-options-options)

  - [consolidated real-time data streaming](https://docs.tardis.dev/api/tardis-machine#websocket-ws-stream-normalized-options-options) connecting directly to exchanges' WebSocket APIs

  - customizable [order book snapshots](https://docs.tardis.dev/api/tardis-machine#book_snapshot_-number_of_levels-_-snapshot_interval-time_unit) and [trade bars](https://docs.tardis.dev/api/tardis-machine#trade_bar_-aggregation_interval-suffix) data types
  - seamless [switching between real-time data streaming and historical data replay](https://docs.tardis.dev/api/tardis-machine#normalized-market-data-apis)
    <br/>

- transparent historical local data caching \(cached data is stored on disk in compressed GZIP format and decompressed
  on demand when reading the data\)
  <br/>

- support for top cryptocurrency exchanges: BitMEX, Deribit, Binance, Binance Futures, FTX, OKEx, Huobi Global, Huobi DM, bitFlyer, Bitstamp, Coinbase Pro, Crypto Facilities, Gemini, Kraken, Bitfinex, Bybit, OKCoin, CoinFLEX and more
  <br/>
  <br/>
  <br/>

## Documentation

### [See official docs](https://docs.tardis.dev/api/tardis-machine).

<br/>
<br/>

## Event Bus Publishing

- Publish normalized market data encoded with Buf-managed Protobufs to Kafka, RabbitMQ, or AWS Kinesis by supplying the respective flags.
- Use `--kafka-topic-routing` to route specific payload cases (e.g. `trade`, `bookChange`) to dedicated topics via a comma separated `payloadCase:topic` list. Payload case names must match the normalized Bronze cases (`trade`, `bookChange`, `bookSnapshot`, `groupedBookSnapshot`, `quote`, `derivativeTicker`, `liquidation`, `optionSummary`, `bookTicker`, `tradeBar`, `error`, `disconnect`).
- Include real-time `quote` payloads alongside trades, book snapshots, and other normalized events.
- Reduce downstream load by specifying `--kafka-include-payloads` with a comma separated payload case allow-list (others are dropped before batching).
- Additional flags like `--kafka-client-id`, `--kafka-ssl`, and SASL options remain available for secure deployments.
- Shape Kafka partitioning keys with `--kafka-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{payloadCase}}`, or `{{meta.request_id}}`.
- Dial delivery guarantees with `--kafka-acks` (`all`, `leader`, `none`) and enable idempotent producers via `--kafka-idempotent` when coordinating with transactional sinks.
- Tune publishing throughput via `--kafka-max-batch-size` (events per batch) and `--kafka-max-batch-delay-ms` (max milliseconds to wait before flushing).
- Select compression with `--kafka-compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`) to balance throughput and broker resource usage.
- Attach deployment metadata with `--kafka-static-headers`, supplying comma separated `key:value` pairs that become constant Kafka headers on every record.

### RabbitMQ Publishing

- Publish normalized market data to RabbitMQ by supplying `--rabbitmq-url` and `--rabbitmq-exchange` flags.
- Use `--rabbitmq-exchange-type` to set the exchange type (`direct`, `topic`, `headers`, `fanout`).
- Shape routing keys with `--rabbitmq-routing-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{payloadCase}}`, or `{{meta.request_id}}`.
- Reduce downstream load by specifying `--rabbitmq-include-payloads` with a comma separated payload case allow-list (others are dropped).
- Attach deployment metadata with `--rabbitmq-static-headers`, supplying comma separated `key:value` pairs that become constant RabbitMQ headers on every message.

### Kinesis Publishing

- Publish normalized market data to AWS Kinesis by supplying `--kinesis-stream-name` and `--kinesis-region` flags.
- Use `--kinesis-stream-routing` to route specific payload cases (e.g. `trade`, `bookChange`) to dedicated streams via a comma separated `payloadCase:streamName` list. Payload case names must match the normalized Bronze cases (`trade`, `bookChange`, `bookSnapshot`, `groupedBookSnapshot`, `quote`, `derivativeTicker`, `liquidation`, `optionSummary`, `bookTicker`, `tradeBar`, `error`, `disconnect`).
- Reduce downstream load by specifying `--kinesis-include-payloads` with a comma separated payload case allow-list (others are dropped before batching).
- Provide AWS credentials via `--kinesis-access-key-id` and `--kinesis-secret-access-key`, or rely on IAM roles/instance profiles.
- Shape partition keys with `--kinesis-partition-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{payloadCase}}`, or `{{meta.request_id}}`.
- Tune publishing throughput via `--kinesis-max-batch-size` (events per batch) and `--kinesis-max-batch-delay-ms` (max milliseconds to wait before flushing).
- Attach deployment metadata with `--kinesis-static-headers`, supplying comma separated `key:value` pairs that become constant metadata on every record.

### NATS Publishing

- Publish normalized market data to NATS by supplying `--nats-servers` and `--nats-subject` flags.
- Use `--nats-subject-routing` to route specific payload cases (e.g. `trade`, `bookChange`) to dedicated subjects via a comma separated `payloadCase:subject` list. Payload case names must match the normalized Bronze cases (`trade`, `bookChange`, `bookSnapshot`, `groupedBookSnapshot`, `quote`, `derivativeTicker`, `liquidation`, `optionSummary`, `bookTicker`, `tradeBar`, `error`, `disconnect`).
- Reduce downstream load by specifying `--nats-include-payloads` with a comma separated payload case allow-list (others are dropped).
- Shape subjects with `--nats-subject-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{payloadCase}}`, or `{{meta.request_id}}`.
- Attach deployment metadata with `--nats-static-headers`, supplying comma separated `key:value` pairs that become constant NATS headers on every message.

### Redis Publishing

- Publish normalized market data to Redis streams by supplying `--redis-url` and `--redis-stream` flags.
- Use `--redis-stream-routing` to route specific payload cases (e.g. `trade`, `bookChange`) to dedicated streams via a comma separated `payloadCase:stream` list. Payload case names must match the normalized Bronze cases (`trade`, `bookChange`, `bookSnapshot`, `groupedBookSnapshot`, `quote`, `derivativeTicker`, `liquidation`, `optionSummary`, `bookTicker`, `tradeBar`, `error`, `disconnect`).
- Reduce downstream load by specifying `--redis-include-payloads` with a comma separated payload case allow-list (others are dropped before batching).
- Shape stream keys with `--redis-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{payloadCase}}`, or `{{meta.request_id}}`.
- Tune publishing throughput via `--redis-max-batch-size` (events per batch) and `--redis-max-batch-delay-ms` (max milliseconds to wait before flushing).
- Attach deployment metadata with `--redis-static-headers`, supplying comma separated `key:value` pairs that become constant Redis metadata on every record.

### Silver Layer Publishing

The Silver layer provides analytics-ready data with fixed scales and strong typing, complementing the existing Bronze layer. Silver records are published to event buses using the same infrastructure but with dedicated configuration flags.

#### Silver Kafka Publishing

- Publish Silver layer records to Kafka by supplying `--kafka-silver-brokers` and `--kafka-silver-topic` flags.
- Use `--kafka-silver-topic-routing` to route specific record types (e.g. `trade`, `book_change`) to dedicated topics via a comma separated `recordType:topic` list. Record type names must match the Silver record types (`trade`, `book_change`, `book_snapshot`, `grouped_book_snapshot`, `quote`, `derivative_ticker`, `liquidation`, `option_summary`, `book_ticker`, `trade_bar`).
- Reduce downstream load by specifying `--kafka-silver-include-records` with a comma separated record type allow-list (others are dropped before batching).
- Additional flags like `--kafka-silver-client-id`, `--kafka-silver-ssl`, and SASL options remain available for secure deployments.
- Shape Kafka partitioning keys with `--kafka-silver-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{recordType}}`.
- Dial delivery guarantees with `--kafka-silver-acks` (`all`, `leader`, `none`) and enable idempotent producers via `--kafka-silver-idempotent`.
- Tune publishing throughput via `--kafka-silver-max-batch-size` (events per batch) and `--kafka-silver-max-batch-delay-ms` (max milliseconds to wait before flushing).
- Select compression with `--kafka-silver-compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`).
- Use Schema Registry for schema evolution by supplying `--kafka-silver-schema-registry-url` and optional auth flags (`--kafka-silver-schema-registry-auth-username`, `--kafka-silver-schema-registry-auth-password`).
- Attach deployment metadata with `--kafka-silver-static-headers`, supplying comma separated `key:value` pairs that become constant Kafka headers on every record.

#### Silver RabbitMQ Publishing

- Publish Silver layer records to RabbitMQ by supplying `--rabbitmq-silver-url` and `--rabbitmq-silver-exchange` flags.
- Use `--rabbitmq-silver-exchange-type` to set the exchange type (`direct`, `topic`, `headers`, `fanout`).
- Shape routing keys with `--rabbitmq-silver-routing-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{recordType}}`.
- Reduce downstream load by specifying `--rabbitmq-silver-include-records` with a comma separated record type allow-list (others are dropped).
- Attach deployment metadata with `--rabbitmq-silver-static-headers`, supplying comma separated `key:value` pairs that become constant RabbitMQ headers on every message.

#### Silver Kinesis Publishing

- Publish Silver layer records to AWS Kinesis by supplying `--kinesis-silver-stream-name` and `--kinesis-silver-region` flags.
- Use `--kinesis-silver-stream-routing` to route specific record types (e.g. `trade`, `book_change`) to dedicated streams via a comma separated `recordType:streamName` list. Record type names must match the Silver record types.
- Reduce downstream load by specifying `--kinesis-silver-include-records` with a comma separated record type allow-list (others are dropped before batching).
- Provide AWS credentials via `--kinesis-silver-access-key-id` and `--kinesis-silver-secret-access-key`, or rely on IAM roles/instance profiles.
- Shape partition keys with `--kinesis-silver-partition-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{recordType}}`.
- Tune publishing throughput via `--kinesis-silver-max-batch-size` (events per batch) and `--kinesis-silver-max-batch-delay-ms` (max milliseconds to wait before flushing).
- Attach deployment metadata with `--kinesis-silver-static-headers`, supplying comma separated `key:value` pairs that become constant metadata on every record.

#### Silver NATS Publishing

- Publish Silver layer records to NATS by supplying `--nats-silver-servers` and `--nats-silver-subject` flags.
- Use `--nats-silver-subject-routing` to route specific record types (e.g. `trade`, `book_change`) to dedicated subjects via a comma separated `recordType:subject` list. Record type names must match the Silver record types.
- Reduce downstream load by specifying `--nats-silver-include-records` with a comma separated record type allow-list (others are dropped).
- Shape subjects with `--nats-silver-subject-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{recordType}}`.
- Attach deployment metadata with `--nats-silver-static-headers`, supplying comma separated `key:value` pairs that become constant NATS headers on every message.

#### Silver Redis Publishing

- Publish Silver layer records to Redis streams by supplying `--redis-silver-url` and `--redis-silver-stream` flags.
- Use `--redis-silver-stream-routing` to route specific record types (e.g. `trade`, `book_change`) to dedicated streams via a comma separated `recordType:stream` list. Record type names must match the Silver record types.
- Reduce downstream load by specifying `--redis-silver-include-records` with a comma separated record type allow-list (others are dropped before batching).
- Shape stream keys with `--redis-silver-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{recordType}}`.
- Tune publishing throughput via `--redis-silver-max-batch-size` (events per batch) and `--redis-silver-max-batch-delay-ms` (max milliseconds to wait before flushing).
- Attach deployment metadata with `--redis-silver-static-headers`, supplying comma separated `key:value` pairs that become constant Redis metadata on every record.

### Keeping Schemas and Builds in Sync

Normalized event schemas live under `schemas/proto`, and generated TypeScript bindings are emitted into `src/generated`. Whenever schemas change, run `npm run buf:generate` to refresh the Buf-generated sources and `npm run build` to update the compiled `dist/` artifacts that power the CLI entry point.

### Event Bus Integration Test Prerequisites

Event bus publishing is covered by integration tests in `test/eventbus`. These rely on Testcontainers and require a local Docker daemon with at least 2 CPU cores, 4 GB of memory, and the ability to pull the `confluentinc/cp-kafka:7.5.3`, `rabbitmq:3-management-alpine`, `localstack/localstack:3.0`, `nats:2.10`, and `redis:7-alpine` images. Ensure Docker is running before invoking `npm test`; otherwise event bus suites will be skipped after a timeout.

### Event Bus Maintenance Checklist

- For Kafka: Verify `--kafka-brokers` reflects the current cluster endpoints before each deployment; stale broker URIs are the most common cause of failed connects.
- For Kafka: Confirm `--kafka-acks` and `--kafka-idempotent` match broker durability targets (idempotence requires Kafka >= 0.11 with `acks=all`).
- For Kafka: Review batching knobs regularly: `--kafka-max-batch-size` should stay below broker `message.max.bytes`, and `--kafka-max-batch-delay-ms` must align with downstream latency budgets.
- For RabbitMQ: Ensure `--rabbitmq-url` points to a healthy RabbitMQ cluster and `--rabbitmq-exchange` exists or can be auto-created.
- For Kinesis: Confirm `--kinesis-region` and stream name are correct; ensure IAM permissions allow `PutRecords` on the specified stream.
- For Kinesis: Review batching knobs regularly: `--kinesis-max-batch-size` should stay below Kinesis `PutRecords` limits (500 records), and `--kinesis-max-batch-delay-ms` must align with downstream latency budgets.
- For NATS: Ensure `--nats-servers` points to healthy NATS servers and subjects are appropriately configured for your routing needs.
- For Redis: Ensure `--redis-url` points to a healthy Redis instance and streams are appropriately configured for your routing needs.
- For Redis: Review batching knobs regularly: `--redis-max-batch-size` and `--redis-max-batch-delay-ms` should align with throughput requirements.
- For Silver Kafka: Verify `--kafka-silver-brokers` reflects the current cluster endpoints; apply same broker maintenance as Bronze layer.
- For Silver Kafka: Confirm `--kafka-silver-acks` and `--kafka-silver-idempotent` match broker durability targets.
- For Silver Kafka: Review batching knobs regularly: `--kafka-silver-max-batch-size` and `--kafka-silver-max-batch-delay-ms` should align with throughput requirements.
- For Silver RabbitMQ: Ensure `--rabbitmq-silver-url` points to a healthy RabbitMQ cluster and `--rabbitmq-silver-exchange` exists or can be auto-created.
- For Silver Kinesis: Confirm `--kinesis-silver-region` and stream name are correct; ensure IAM permissions allow `PutRecords` on the specified stream.
- For Silver Kinesis: Review batching knobs regularly: `--kinesis-silver-max-batch-size` should stay below Kinesis `PutRecords` limits (500 records).
- For Silver NATS: Ensure `--nats-silver-servers` points to healthy NATS servers and subjects are appropriately configured.
- For Silver Redis: Ensure `--redis-silver-url` points to a healthy Redis instance and streams are appropriately configured for your routing needs.
- For Silver Redis: Review batching knobs regularly: `--redis-silver-max-batch-size` and `--redis-silver-max-batch-delay-ms` should align with throughput requirements.
- Confirm header contracts after schema updates by consuming a sample message and validating Buf-decoded payloads alongside the emitted `recordType` and `dataType` headers for Silver layer.
- Track retries via application logs; repeated send attempt warnings indicate sustained pressure and should trigger broker-side health checks.
- Re-run `npm run buf:generate` and rebuild whenever `schemas/proto` changes to keep binary payloads matching the deployed Buf schema version.
