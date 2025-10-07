# TODO

## Kafka Buf Publishing

- [ ] Verify Bronze schema registry path with Buf-coded payloads end-to-end.
- [ ] Add Silver schema registry coverage ensuring Buf payload + schema ID framing.
- [ ] Align Silver Kafka schema registry encoding with Bronze logic.

## Repository Maintenance

- [ ] Refresh event bus specification to reflect Kafka-only scope.
- [ ] Prune or archive outdated specs for non-Kafka buses.
- [ ] Document schema registry workflow updates in README.
- [ ] Run `npm run buf:generate` and `npm test` before release candidates.
