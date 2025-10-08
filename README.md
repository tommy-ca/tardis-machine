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

Tardis Machine now focuses exclusively on Kafka for streaming normalized (Bronze) and analytics-ready (Silver) datasets encoded with Buf-managed Protobuf schemas.

### Kafka Bronze Publishing

- Use `--kafka-brokers` and `--kafka-topic` to enable Bronze publishing to Kafka.
- Route specific payload cases (e.g. `trade`, `bookChange`) with `--kafka-topic-routing=payloadCase:topic` values.
- Drop unwanted payloads via `--kafka-include-payloads` (comma separated allow-list).
- Shape record keys with `--kafka-key-template`, using placeholders like `{{exchange}}`, `{{symbol}}`, `{{payloadCase}}`, or `{{meta.request_id}}`.
- Control durability with `--kafka-acks` (`all`, `leader`, `none`) and enable idempotent producers via `--kafka-idempotent`.
- Tune batching using `--kafka-max-batch-size` and `--kafka-max-batch-delay-ms`.
- Select compression using `--kafka-compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`).
- Attach deployment metadata with `--kafka-static-headers` and emit meta fields as headers with `--kafka-meta-headers-prefix`.
- Integrate Schema Registry via `--kafka-schema-registry-url` and optional basic-auth credentials.

### Operational Tips

- Keep `--kafka-brokers` aligned with the current cluster endpoints; refresh configs during broker maintenance.
- Monitor send retries in logs; sustained retry warnings indicate downstream pressure that may require Kafka tuning.
- After updating protobuf definitions under `schemas/proto`, run `npm run buf:generate` and `npm run build` so the published payloads match the latest Buf schemas.
