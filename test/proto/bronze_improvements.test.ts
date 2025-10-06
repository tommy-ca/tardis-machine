import path from 'path'
import protobuf from 'protobufjs'

const bronzeProto = path.join(__dirname, '../../schemas/proto/lakehouse/bronze/v1/normalized_event.proto')

const load = () => protobuf.load(bronzeProto)
const ts = (seconds: number, nanos = 0) => ({ seconds, nanos })

describe('Bronze schema improvements (spec-driven)', () => {
  test('NormalizedEvent supports origin enum and control codes', async () => {
    const root = await load()
    const NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent') as protobuf.Type

    const errMsg: any = {
      source: 'tardis-machine',
      exchange: 'okx',
      symbol: 'btc-usdt-swap',
      localTs: ts(1700000001),
      origin: 1, // ORIGIN_REALTIME
      error: { details: 'dial timeout', code: 1, subsequentErrors: 2 }
    }
    const err = NormalizedEvent.verify(errMsg)
    expect(err || null).toBeNull()

    const buf = NormalizedEvent.encode(NormalizedEvent.create(errMsg)).finish()
    const dec: any = NormalizedEvent.decode(buf)
    expect(dec.origin).toBe(1) // ORIGIN_REALTIME
    expect(dec.error.code).toBe(1) // ERROR_CODE_WS_CONNECT
    expect(dec.error.subsequentErrors).toBe(2)
  })

  test('BookSnapshot carries optional grouping and interval metadata', async () => {
    const root = await load()
    const NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent') as protobuf.Type

    const snap: any = {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: ts(1700000100),
      origin: 2, // ORIGIN_REPLAY
      bookSnapshot: {
        depth: 5,
        bids: [{ priceStr: '100.0', qtyStr: '1.0' }],
        asks: [{ priceStr: '100.1', qtyStr: '1.1' }],
        eventTs: ts(1700000100),
        grouping: 10,
        intervalMs: 1000,
        removeCrossedLevels: true,
        sequence: 42
      }
    }
    const e = NormalizedEvent.verify(snap)
    expect(e || null).toBeNull()
    const buf = NormalizedEvent.encode(NormalizedEvent.create(snap)).finish()
    const dec = NormalizedEvent.toObject(NormalizedEvent.decode(buf), { longs: Number }) as any
    expect(dec.bookSnapshot.grouping).toBe(10)
    expect(dec.bookSnapshot.intervalMs).toBe(1000)
    expect(dec.bookSnapshot.removeCrossedLevels).toBe(true)
    expect(dec.bookSnapshot.sequence).toBe(42)
  })
})
