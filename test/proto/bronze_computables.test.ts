import path from 'path'
import protobuf from 'protobufjs'

const bronzeProto = path.join(
  __dirname,
  '../../schemas/proto/lakehouse/bronze/v1/normalized_event.proto'
)

const load = () => protobuf.load(bronzeProto)
const ts = (seconds: number, nanos = 0) => ({ seconds, nanos })

describe('Bronze computables: TradeBar & GroupedBookSnapshot', () => {
  test('NormalizedEvent supports tradeBar payload', async () => {
    const root = await load()
    const NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent') as protobuf.Type

    const msg: any = {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: ts(1700001000),
      tradeBar: {
        kind: 2, // BAR_KIND_TIME
        interval: 1,
        intervalMs: 1000,
        openStr: '100.0',
        highStr: '101.0',
        lowStr: '99.5',
        closeStr: '100.5',
        volumeStr: '12.34',
        tradeCount: 42,
        eventTs: ts(1700001000),
        endTs: ts(1700001001)
      }
    }

    const err = NormalizedEvent.verify(msg)
    expect(err || null).toBeNull()

    const buf = NormalizedEvent.encode(NormalizedEvent.create(msg)).finish()
    const dec = NormalizedEvent.toObject(NormalizedEvent.decode(buf), { longs: Number }) as any
    expect(dec.tradeBar.kind).toBe(2)
    expect(dec.tradeBar.interval).toBe(1)
    expect(dec.tradeBar.intervalMs).toBe(1000)
    expect(dec.tradeBar.openStr).toBe('100.0')
    expect(dec.tradeBar.tradeCount).toBe(42)
  })

  test('NormalizedEvent supports groupedBookSnapshot payload', async () => {
    const root = await load()
    const NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent') as protobuf.Type

    const msg: any = {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: ts(1700002000),
      groupedBookSnapshot: {
        depth: 2,
        bids: [{ priceStr: '100.0', qtyStr: '1.0' }],
        asks: [{ priceStr: '100.1', qtyStr: '1.1' }],
        eventTs: ts(1700002000),
        grouping: 10,
        intervalMs: 1000,
        removeCrossedLevels: true,
        sequence: 7
      }
    }

    const err = NormalizedEvent.verify(msg)
    expect(err || null).toBeNull()
    const buf = NormalizedEvent.encode(NormalizedEvent.create(msg)).finish()
    const dec = NormalizedEvent.toObject(NormalizedEvent.decode(buf), { longs: Number }) as any
    expect(dec.groupedBookSnapshot.grouping).toBe(10)
    expect(dec.groupedBookSnapshot.intervalMs).toBe(1000)
    expect(dec.groupedBookSnapshot.removeCrossedLevels).toBe(true)
    expect(dec.groupedBookSnapshot.sequence).toBe(7)
  })
})

