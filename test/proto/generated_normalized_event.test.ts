import path from 'path'
import protobuf from 'protobufjs'
import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import { NormalizedEventSchema, TradeSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

const bronzeProto = path.join(
  __dirname,
  '../../schemas/proto/lakehouse/bronze/v1/normalized_event.proto'
)

const timestampBuf = (seconds: number, nanos = 0) => ({ seconds: BigInt(seconds), nanos })
const timestampProto = (seconds: number, nanos = 0) => ({ seconds, nanos })

describe('Generated NormalizedEvent codec', () => {
  test('encodes via bufbuild and decodes with protobufjs', async () => {
    const trade = create(TradeSchema, {
      tradeId: '123',
      priceStr: '100.50',
      qtyStr: '0.75',
      side: 1,
      sequence: BigInt(5),
      eventTs: timestampBuf(10)
    })

    const message = create(NormalizedEventSchema, {
      source: 'tardis-machine',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTs: timestampBuf(15, 1_000),
      ingestTs: timestampBuf(16),
      origin: Origin.REPLAY,
      meta: { shard: 'a' },
      payload: {
        case: 'trade',
        value: trade
      }
    })

    const binary = toBinary(NormalizedEventSchema, message)

    const root = await protobuf.load(bronzeProto)
    const NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent')
    const decoded: any = NormalizedEvent.decode(binary)

    expect(decoded.source).toBe('tardis-machine')
    expect(decoded.origin).toBe(Origin.REPLAY)
    expect(decoded.meta.shard).toBe('a')
    expect(decoded.trade.priceStr).toBe('100.50')
  })

  test('decodes bufbuild binary produced by protobufjs', async () => {
    const root = await protobuf.load(bronzeProto)
    const NormalizedEvent = root.lookupType('lakehouse.bronze.v1.NormalizedEvent')

    const payload = {
      source: 'tardis-machine',
      exchange: 'okx',
      symbol: 'ethusdt',
      localTs: timestampProto(1700000000, 2),
      origin: Origin.REALTIME,
      trade: {
        tradeId: '234',
        priceStr: '2500.12',
        qtyStr: '1.25',
        side: 2,
        eventTs: timestampProto(1700000000, 3)
      }
    }

    const err = NormalizedEvent.verify(payload)
    expect(err || null).toBeNull()

    const binary = NormalizedEvent.encode(NormalizedEvent.create(payload)).finish()
    const decoded = fromBinary(NormalizedEventSchema, binary)

    expect(decoded.exchange).toBe('okx')
    expect(decoded.payload.case).toBe('trade')
    if (decoded.payload.case !== 'trade') {
      throw new Error('expected trade payload')
    }
    expect(decoded.payload.value.priceStr).toBe('2500.12')
  })
})
