# TODO

## Kafka Buf Publishing

- [x] Verify Bronze schema registry path with Buf-coded payloads end-to-end.
- [x] Ensure CLI/README guidance reflects Kafka-only publishing flags.
- [x] Capture schema registry smoke-test coverage for Bronze payloads.

## Repository Maintenance

- [x] Prune or archive outdated specs for non-Kafka buses under `.agent/specs`.
- [x] Remove Silver event bus references from developer notes and docs.
- [ ] Run `npm run buf:generate` and `npm test` before release candidates. (Buf regenerate completed 2025-10-08; `npm test` blocked by Kafka schema registry container timeout, rerun when Docker networking is available.)
