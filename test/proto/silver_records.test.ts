import path from 'path'
import protobuf from 'protobufjs'

const silverProto = path.join(__dirname, '../../schemas/proto/lakehouse/silver/v1/records.proto')

function ts(seconds: number, nanos = 0) {
  return { seconds, nanos }
}

describe('Silver record protos', () => {
  let root: protobuf.Root
  const get = (name: string) => root.lookupType(name) as protobuf.Type

  beforeAll(async () => {
    root = await protobuf.load(silverProto)
  })

  test('TradeRecord encode/decode', () => {
    const TradeRecord = get('lakehouse.silver.v1.TradeRecord')
    const msg: any = {
      exchange: 'binance',
      symbol: 'btcusdt',
      eventTs: ts(1561939201, 81000000),
      ingestTs: ts(1561939202, 0),
      tradeId: '145918416',
      side: 1, // BUY
      priceE8: 1085644, // small ints to avoid precision edge cases in test
      qtyE8: 60501
    }
    const err = TradeRecord.verify(msg)
    expect(err || null).toBeNull()
    const buf = TradeRecord.encode(TradeRecord.create(msg)).finish()
    const decoded = TradeRecord.toObject(TradeRecord.decode(buf), { longs: Number, enums: String }) as any
    expect(decoded.priceE8).toBe(msg.priceE8)
    expect(decoded.qtyE8).toBe(msg.qtyE8)
  })

  test('BookSnapshotRecord encode/decode', () => {
    const BookSnapshotRecord = get('lakehouse.silver.v1.BookSnapshotRecord')
    const msg: any = {
      exchange: 'binance',
      symbol: 'btcusdt',
      eventTs: ts(1700000000),
      ingestTs: ts(1700000001),
      depth: 2,
      bids: [
        { priceE8: 1085600, qtyE8: 1000000 },
        { priceE8: 1085590, qtyE8: 500000 }
      ],
      asks: [
        { priceE8: 1085700, qtyE8: 1200000 },
        { priceE8: 1085710, qtyE8: 800000 }
      ]
    }
    const err = BookSnapshotRecord.verify(msg)
    expect(err || null).toBeNull()
    const buf = BookSnapshotRecord.encode(BookSnapshotRecord.create(msg)).finish()
    const decoded = BookSnapshotRecord.toObject(BookSnapshotRecord.decode(buf), { longs: Number }) as any
    expect(decoded.depth).toBe(2)
    expect(decoded.bids.length).toBe(2)
    expect(decoded.asks[0].priceE8).toBe(msg.asks[0].priceE8)
  })
})
