import path from 'path'
import protobuf from 'protobufjs'

const silverProto = path.join(
  __dirname,
  '../../schemas/proto/lakehouse/silver/v1/records.proto'
)

const load = () => protobuf.load(silverProto)
const ts = (seconds: number, nanos = 0) => ({ seconds, nanos })

describe('Silver computables: TradeBarRecord & GroupedBookSnapshotRecord', () => {
  test('TradeBarRecord encode/decode', async () => {
    const root = await load()
    const TradeBarRecord = root.lookupType('lakehouse.silver.v1.TradeBarRecord') as protobuf.Type
    const msg: any = {
      exchange: 'binance', symbol: 'btcusdt',
      eventTs: ts(1700001000), endTs: ts(1700001001), ingestTs: ts(1700001002),
      kind: 2, // BAR_KIND_TIME
      interval: 1, intervalMs: 1000,
      openE8: 100_00000000, highE8: 101_00000000, lowE8: 99_50000000, closeE8: 100_50000000,
      volumeE8: 12_34000000, tradeCount: 42
    }
    const err = TradeBarRecord.verify(msg)
    expect(err || null).toBeNull()
    const buf = TradeBarRecord.encode(TradeBarRecord.create(msg)).finish()
    const dec = TradeBarRecord.toObject(TradeBarRecord.decode(buf), { longs: Number }) as any
    expect(dec.kind).toBe(2)
    expect(dec.intervalMs).toBe(1000)
    expect(dec.tradeCount).toBe(42)
    expect(dec.openE8).toBe(100_00000000)
  })

  test('GroupedBookSnapshotRecord encode/decode', async () => {
    const root = await load()
    const GroupedBookSnapshotRecord = root.lookupType('lakehouse.silver.v1.GroupedBookSnapshotRecord') as protobuf.Type
    const msg: any = {
      exchange: 'binance', symbol: 'btcusdt',
      eventTs: ts(1700002000), ingestTs: ts(1700002001),
      depth: 2,
      bids: [{ priceE8: 100_00000000, qtyE8: 1_00000000 }],
      asks: [{ priceE8: 100_10000000, qtyE8: 1_10000000 }],
      grouping: 10, intervalMs: 1000, removeCrossedLevels: true, sequence: 7
    }
    const err = GroupedBookSnapshotRecord.verify(msg)
    expect(err || null).toBeNull()
    const buf = GroupedBookSnapshotRecord.encode(GroupedBookSnapshotRecord.create(msg)).finish()
    const dec = GroupedBookSnapshotRecord.toObject(GroupedBookSnapshotRecord.decode(buf), { longs: Number }) as any
    expect(dec.grouping).toBe(10)
    expect(dec.intervalMs).toBe(1000)
    expect(dec.removeCrossedLevels).toBe(true)
    expect(dec.sequence).toBe(7)
  })
})

