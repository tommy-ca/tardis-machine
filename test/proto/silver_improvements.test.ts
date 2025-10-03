import path from 'path'
import protobuf from 'protobufjs'

const silverProto = path.join(
  __dirname,
  '../../schemas/proto/lakehouse/silver/v1/records.proto'
)

const load = () => protobuf.load(silverProto)
const ts = (seconds: number, nanos = 0) => ({ seconds, nanos })

describe('Silver schema improvements (spec-driven)', () => {
  test('TradeRecord optionally carries origin', async () => {
    const root = await load()
    const TradeRecord = root.lookupType('lakehouse.silver.v1.TradeRecord') as protobuf.Type
    const msg: any = {
      exchange: 'binance',
      symbol: 'btcusdt',
      eventTs: ts(1700000500),
      ingestTs: ts(1700000501),
      tradeId: 't1',
      side: 1,
      priceE8: 100_00000000,
      qtyE8: 1_00000000,
      origin: 2 // ORIGIN_REPLAY
    }
    const err = TradeRecord.verify(msg)
    expect(err || null).toBeNull()
    const buf = TradeRecord.encode(TradeRecord.create(msg)).finish()
    const dec: any = TradeRecord.decode(buf)
    expect(dec.origin).toBe(2) // ORIGIN_REPLAY
  })
})
