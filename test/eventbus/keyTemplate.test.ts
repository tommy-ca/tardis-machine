import { compileKeyBuilder, compileSilverKeyBuilder } from '../../src/eventbus/keyTemplate'
import { create } from '@bufbuild/protobuf'
import { NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

describe('compileKeyBuilder', () => {
  test('compiles simple template with exchange and symbol', () => {
    const builder = compileKeyBuilder('{{exchange}}.{{symbol}}')

    const event = create(NormalizedEventSchema, { exchange: 'binance', symbol: 'btcusdt', source: 'test', origin: Origin.REPLAY })
    const result = builder(event, 'trade', 'normalized')

    expect(result).toBe('binance.btcusdt')
  })

  test('compiles template with payloadCase and dataType', () => {
    const builder = compileKeyBuilder('{{payloadCase}}-{{dataType}}')

    const event = create(NormalizedEventSchema, { exchange: 'binance', symbol: 'btcusdt', source: 'test', origin: Origin.REPLAY })
    const result = builder(event, 'bookChange', 'normalized')

    expect(result).toBe('bookChange-normalized')
  })

  test('compiles template with meta placeholders', () => {
    const builder = compileKeyBuilder('{{meta.request_id}}-{{exchange}}')

    const event = create(NormalizedEventSchema, {
      exchange: 'binance',
      symbol: 'btcusdt',
      source: 'test',
      origin: Origin.REPLAY,
      meta: { request_id: 'req-123' }
    })
    const result = builder(event, 'trade', 'normalized')

    expect(result).toBe('req-123-binance')
  })

  test('compiles template with origin', () => {
    const builder = compileKeyBuilder('{{origin}}-{{symbol}}')

    const event = create(NormalizedEventSchema, { exchange: 'binance', symbol: 'btcusdt', source: 'test', origin: Origin.REALTIME })
    const result = builder(event, 'trade', 'normalized')

    expect(result).toBe('realtime-btcusdt')
  })

  test('throws on empty template', () => {
    expect(() => compileKeyBuilder('')).toThrow('kafka-key-template must be a non-empty string.')
  })

  test('throws on unknown placeholder', () => {
    expect(() => compileKeyBuilder('{{unknown}}')).toThrow('Unknown kafka-key-template placeholder "{{unknown}}"')
  })

  test('throws on invalid meta placeholder', () => {
    expect(() => compileKeyBuilder('{{meta.invalid-key}}')).toThrow('Invalid kafka-key-template meta placeholder')
  })
})

describe('compileSilverKeyBuilder', () => {
  test('compiles simple template with exchange and symbol', () => {
    const builder = compileSilverKeyBuilder('{{exchange}}.{{symbol}}')

    const record = { exchange: 'binance', symbol: 'btcusdt', origin: Origin.REPLAY }
    const result = builder(record, 'trade', 'silver')

    expect(result).toBe('binance.btcusdt')
  })

  test('compiles template with recordType and dataType', () => {
    const builder = compileSilverKeyBuilder('{{recordType}}-{{dataType}}')

    const record = { exchange: 'binance', symbol: 'btcusdt', origin: Origin.REPLAY }
    const result = builder(record, 'book_change', 'silver')

    expect(result).toBe('book_change-silver')
  })

  test('compiles template with meta placeholders', () => {
    const builder = compileSilverKeyBuilder('{{meta.request_id}}-{{exchange}}')

    const record = { exchange: 'binance', symbol: 'btcusdt', origin: Origin.REPLAY, meta: { request_id: 'req-123' } }
    const result = builder(record, 'trade', 'silver')

    expect(result).toBe('req-123-binance')
  })

  test('compiles template with origin', () => {
    const builder = compileSilverKeyBuilder('{{origin}}-{{symbol}}')

    const record = { exchange: 'binance', symbol: 'btcusdt', origin: Origin.REALTIME }
    const result = builder(record, 'trade', 'silver')

    expect(result).toBe('realtime-btcusdt')
  })

  test('throws on empty template', () => {
    expect(() => compileSilverKeyBuilder('')).toThrow('kafka-silver-key-template must be a non-empty string.')
  })

  test('throws on unknown placeholder', () => {
    expect(() => compileSilverKeyBuilder('{{unknown}}')).toThrow('Unknown kafka-silver-key-template placeholder "{{unknown}}"')
  })

  test('throws on invalid meta placeholder', () => {
    expect(() => compileSilverKeyBuilder('{{meta.invalid-key}}')).toThrow('Invalid kafka-silver-key-template meta placeholder')
  })
})
