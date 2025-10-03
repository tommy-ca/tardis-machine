Crypto Lakehouse Schemas

This directory contains the Protobuf schemas and design notes for the crypto lakehouse Bronze and Silver layers.

Principles
- Bronze: Preserve normalized source semantics with minimal transformation; include control signals (error/disconnect); favor evolvability.
- Silver: Strongly typed, analytics‑ready records; consistent units/scales; dedupe keys; stable schemas.
- Timestamps: Use google.protobuf.Timestamp for event/ingest/local times.
- Decimals: Bronze as strings (no precision loss). Silver as scaled integers (e.g., price_e8 = price × 1e8) with fixed, documented scales.

Packages
- lakehouse.bronze.v1: Envelope NormalizedEvent with oneof payload for each type (trade, book_change, book_snapshot, quote, derivative_ticker, liquidation, option_summary, book_ticker) and control messages.
- lakehouse.silver.v1: Per‑type record messages for curated tables with fixed scales.

Evolution
- Prefer additive changes. Do not remove/repurpose tags; reserve removed tags.
- New payloads: add new oneof cases in Bronze and new Record messages in Silver.
- Breaking changes require a new package version (e.g., v2).

Scales (Silver)
- Prices/quantities: *_e8 (int64).
- Funding rate: *_e9 (int64).
- Option Greeks: iv_e6, delta_e9, gamma_e9, theta_e9, vega_e9.

Kafka Mapping
- Bronze Kafka value: NormalizedEvent (protobuf). Key: "exchange|symbol|payload_type".
- S3 Bronze: NDJSON.gz lines mirroring Bronze message JSON form; partition path: exchange/type/symbol/ds/hour.

Testing
- Jest tests perform encode/decode round‑trips for Bronze and Silver messages.
- Fixtures illustrate common payloads across venues.

Buf CLI
- Lint: `npm run buf:lint`
- Build: `npm run buf:build`
- Format: `npm run buf:format`
- Breaking check (requires `main` branch available locally): `npm run buf:breaking`
- Module root: `schemas/proto` (see `schemas/proto/buf.yaml`).

Notes
- Enum values follow Buf STANDARD lint (prefixed with enum name, zero value *_UNSPECIFIED).
- Prefer additive evolution; for breaking changes, bump package version to `v2` and update `buf.yaml` accordingly.
