import path from 'path'
import protobuf from 'protobufjs'

const bronzeProto = path.join(__dirname, '../../schemas/proto/lakehouse/bronze/v1/normalized_event.proto')

function ts(seconds: number, nanos = 0) {
  return { seconds, nanos }
}

describe('Bronze NormalizedEvent proto', () => {
  let root: protobuf.Root
  let NormalizedEvent: protobuf.Type

  beforeAll(async () => {
    root = await protobuf.load(bronzeProto)
    NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent') as protobuf.Type
  })

  test('encodes/decodes trade payload', () => {
    const msg: any = {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: ts(1561939201, 5000000),
      trade: {
        trade_id: '145918416',
        priceStr: '10856.44',
        qtyStr: '0.060501',
        side: 1, // BUY
        eventTs: ts(1561939201, 81000000)
      }
    }

    const err = NormalizedEvent.verify(msg)
    expect(err || null).toBeNull()

    const buf = NormalizedEvent.encode(NormalizedEvent.create(msg)).finish()
    const decoded: any = NormalizedEvent.decode(buf)

    expect(decoded.source).toBe('tardis-machine')
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.trade.priceStr).toBe('10856.44')
    expect(decoded.trade.qtyStr).toBe('0.060501')
    expect(decoded.trade.side).toBe(1)
  })

  test('encodes/decodes book_change and control error', () => {
    const bc: any = {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: ts(1700000000, 1_000),
      bookChange: {
        side: 2, // SELL
        action: 1, // UPSERT
        priceStr: '10860.00',
        qtyStr: '0.03273',
        level: 0,
        sequence: 123,
        eventTs: ts(1700000000, 2_000)
      }
    }
    expect(NormalizedEvent.verify(bc) || null).toBeNull()
    const bcBuf = NormalizedEvent.encode(NormalizedEvent.create(bc)).finish()
    const bcDecoded: any = NormalizedEvent.decode(bcBuf)
    expect(bcDecoded.bookChange.action).toBe(1) // UPSERT

    const errMsg: any = {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: ts(1700000001),
      error: { details: 'WS error', subsequentErrors: 3 }
    }
    expect(NormalizedEvent.verify(errMsg) || null).toBeNull()
    const ebuf = NormalizedEvent.encode(NormalizedEvent.create(errMsg)).finish()
    const edec: any = NormalizedEvent.decode(ebuf)
    expect(edec.error.details).toBe('WS error')
    expect(edec.error.subsequentErrors).toBe(3)
  })
})
