import { fromBinary } from '@bufbuild/protobuf'
import { BronzeNormalizedEventEncoder } from '../../src/eventbus/bronzeMapper'
import { compileKeyBuilder } from '../../src/eventbus/keyTemplate'
import {
  NormalizedEventSchema,
  Origin,
  Side,
  Action,
  BarKind,
  ErrorCode
} from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'
import type {
  Trade as NormalizedTrade,
  BookChange as NormalizedBookChange,
  BookSnapshot as NormalizedBookSnapshot,
  TradeBar as NormalizedTradeBar,
  BookTicker as NormalizedBookTicker,
  DerivativeTicker as NormalizedDerivativeTicker,
  Liquidation as NormalizedLiquidation,
  Disconnect,
  NormalizedData
} from 'tardis-dev'
import type { ControlErrorMessage } from '../../src/eventbus/types'

const encoder = new BronzeNormalizedEventEncoder()
const ingest = new Date('2024-01-01T00:00:00.000Z')

const baseMeta = {
  source: 'unit-test',
  origin: Origin.REPLAY,
  ingestTimestamp: ingest
}

type NormalizedQuote = NormalizedData & {
  type: 'quote'
  bidPrice?: number
  bidAmount?: number
  askPrice?: number
  askAmount?: number
}

type NormalizedGroupedBookSnapshot = NormalizedData & {
  type: 'grouped_book_snapshot'
  depth: number
  grouping: number
  intervalMs?: number
  interval?: number
  removeCrossedLevels?: boolean
  sequence?: number
  bids: { price: number; amount: number }[]
  asks: { price: number; amount: number }[]
}

describe('BronzeNormalizedEventEncoder', () => {
  test('encodes trade payload', () => {
    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      id: 'abc',
      price: 31250.25,
      amount: 0.75,
      side: 'buy',
      timestamp: new Date('2024-01-01T00:00:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:01.500Z')
    }

    const [event] = encoder.encode(trade, baseMeta)
    const decoded = fromBinary(NormalizedEventSchema, event.binary)

    expect(event.payloadCase).toBe('trade')
    expect(event.key).toBe('bitmex|BTCUSD|trade')
    expect(decoded.payload.case).toBe('trade')
    if (decoded.payload.case !== 'trade') {
      throw new Error('expected trade payload')
    }
    const payload = decoded.payload.value
    expect(payload.priceStr).toBe('31250.25')
    expect(payload.side).toBe(Side.BUY)
    expect(decoded.meta.data_type).toBe('trade')
  })

  test('breaks book_change into per-level events', () => {
    const change: NormalizedBookChange = {
      type: 'book_change',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      isSnapshot: false,
      bids: [
        { price: 2500, amount: 10 },
        { price: 2499.5, amount: 0 }
      ],
      asks: [{ price: 2500.5, amount: 8 }],
      timestamp: new Date('2024-01-01T00:00:05.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:05.010Z')
    }

    const events = encoder.encode(change, baseMeta)
    expect(events).toHaveLength(3)

    const upsert = fromBinary(NormalizedEventSchema, events[0].binary)
    expect(upsert.payload.case).toBe('bookChange')
    if (upsert.payload.case !== 'bookChange') {
      throw new Error('expected bookChange payload')
    }
    const upsertPayload = upsert.payload.value
    expect(upsertPayload.side).toBe(Side.BUY)
    expect(upsertPayload.action).toBe(Action.UPSERT)

    const del = fromBinary(NormalizedEventSchema, events[1].binary)
    expect(del.payload.case).toBe('bookChange')
    if (del.payload.case !== 'bookChange') {
      throw new Error('expected bookChange payload')
    }
    expect(del.payload.value.action).toBe(Action.DELETE)

    const ask = fromBinary(NormalizedEventSchema, events[2].binary)
    expect(ask.payload.case).toBe('bookChange')
    if (ask.payload.case !== 'bookChange') {
      throw new Error('expected bookChange payload')
    }
    expect(ask.payload.value.side).toBe(Side.SELL)
  })

  test('encodes book snapshot', () => {
    const snapshot: NormalizedBookSnapshot = {
      type: 'book_snapshot',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      name: 'book_snapshot_5_1s',
      depth: 5,
      interval: 1000,
      bids: [
        { price: 2500, amount: 10 },
        { price: 2499.5, amount: 9 }
      ],
      asks: [
        { price: 2500.5, amount: 8 },
        { price: 2501, amount: 7 }
      ],
      timestamp: new Date('2024-01-01T00:00:10.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:10.005Z')
    }

    const [event] = encoder.encode(snapshot, baseMeta)
    expect(event.payloadCase).toBe('bookSnapshot')
    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(decoded.payload.case).toBe('bookSnapshot')
    if (decoded.payload.case !== 'bookSnapshot') {
      throw new Error('expected bookSnapshot payload')
    }
    const snapshotPayload = decoded.payload.value
    expect(snapshotPayload.depth).toBe(5)
    expect(snapshotPayload.bids.length).toBe(2)
    expect(decoded.meta.data_name).toBe('book_snapshot_5_1s')
  })

  test('encodes grouped book snapshot payload', () => {
    const snapshot: NormalizedGroupedBookSnapshot = {
      type: 'grouped_book_snapshot',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      depth: 3,
      grouping: 25,
      intervalMs: 1000,
      removeCrossedLevels: false,
      sequence: 12345,
      bids: [
        { price: 25000, amount: 1.5 },
        { price: 24999.5, amount: 0.75 }
      ],
      asks: [{ price: 25000.5, amount: 2 }],
      timestamp: new Date('2024-01-01T00:00:15.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:15.005Z')
    }

    const events = encoder.encode(snapshot, baseMeta)
    expect(events).toHaveLength(1)

    const decoded = fromBinary(NormalizedEventSchema, events[0].binary)
    expect(decoded.payload.case).toBe('groupedBookSnapshot')
    if (decoded.payload.case !== 'groupedBookSnapshot') {
      throw new Error('expected groupedBookSnapshot payload')
    }

    const payload = decoded.payload.value
    expect(payload.grouping).toBe(25)
    expect(payload.intervalMs).toBe(1000)
    expect(payload.removeCrossedLevels).toBe(false)
    expect(payload.sequence).toBe(BigInt(12345))
    expect(payload.bids[0]?.priceStr).toBe('25000')
    expect(decoded.meta.data_type).toBe('grouped_book_snapshot')
  })

  test('encodes trade bar with timestamps', () => {
    const bar: NormalizedTradeBar = {
      type: 'trade_bar',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      name: 'trade_bar_1s',
      interval: 1000,
      kind: 'time',
      open: 100,
      high: 101,
      low: 99,
      close: 100.5,
      volume: 12,
      buyVolume: 7,
      sellVolume: 5,
      trades: 42,
      vwap: 100.2,
      openTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      closeTimestamp: new Date('2024-01-01T00:00:01.000Z'),
      timestamp: new Date('2024-01-01T00:00:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:01.005Z')
    }

    const [event] = encoder.encode(bar, baseMeta)
    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(decoded.payload.case).toBe('tradeBar')
    if (decoded.payload.case !== 'tradeBar') {
      throw new Error('expected tradeBar payload')
    }
    const tradeBar = decoded.payload.value
    expect(tradeBar.kind).toBe(BarKind.TIME)
    expect(tradeBar.tradeCount).toBe(BigInt(42))
  })

  test('encodes book ticker', () => {
    const ticker: NormalizedBookTicker = {
      type: 'book_ticker',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      askAmount: 10,
      askPrice: 2500.5,
      bidPrice: 2500,
      bidAmount: 8,
      timestamp: new Date('2024-01-01T00:00:02.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:02.100Z')
    }

    const [event] = encoder.encode(ticker, baseMeta)
    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(decoded.payload.case).toBe('bookTicker')
    if (decoded.payload.case !== 'bookTicker') {
      throw new Error('expected bookTicker payload')
    }
    expect(decoded.payload.value.bestAskPriceStr).toBe('2500.5')
  })

  test('encodes quote payload', () => {
    const quote: NormalizedQuote = {
      type: 'quote',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      bidPrice: 2500,
      bidAmount: 9,
      askPrice: 2500.5,
      askAmount: 5,
      timestamp: new Date('2024-01-01T00:00:02.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:02.050Z')
    }

    const [event] = encoder.encode(quote, baseMeta)
    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(event.payloadCase).toBe('quote')
    expect(decoded.payload.case).toBe('quote')
    if (decoded.payload.case !== 'quote') {
      throw new Error('expected quote payload')
    }
    expect(decoded.payload.value.bidPriceStr).toBe('2500')
    expect(decoded.payload.value.askQtyStr).toBe('5')
  })

  test('encodes derivative ticker', () => {
    const ticker: NormalizedDerivativeTicker = {
      type: 'derivative_ticker',
      symbol: 'BTC-PERP',
      exchange: 'deribit',
      lastPrice: 25000,
      openInterest: 1000,
      fundingRate: 0.0001,
      fundingTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      predictedFundingRate: 0.0002,
      indexPrice: 24990,
      markPrice: 25010,
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.050Z')
    }

    const [event] = encoder.encode(ticker, baseMeta)
    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(decoded.payload.case).toBe('derivativeTicker')
    if (decoded.payload.case !== 'derivativeTicker') {
      throw new Error('expected derivativeTicker payload')
    }
    expect(decoded.payload.value.markPriceStr).toBe('25010')
  })

  test('encodes liquidation', () => {
    const liq: NormalizedLiquidation = {
      type: 'liquidation',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      id: 'liq-1',
      price: 25000,
      amount: 5,
      side: 'sell',
      timestamp: new Date('2024-01-01T00:00:03.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:03.010Z')
    }

    const [event] = encoder.encode(liq, baseMeta)
    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(decoded.payload.case).toBe('liquidation')
    if (decoded.payload.case !== 'liquidation') {
      throw new Error('expected liquidation payload')
    }
    const payload = decoded.payload.value
    expect(payload.orderId).toBe('liq-1')
    expect(payload.side).toBe(Side.SELL)
  })

  test('encodes disconnect control message', () => {
    const disconnect: Disconnect = {
      type: 'disconnect',
      exchange: 'binance',
      localTimestamp: new Date('2024-01-01T00:00:05.000Z'),
      symbols: ['BTCUSDT']
    }

    const [event] = encoder.encode(disconnect, {
      ...baseMeta,
      origin: Origin.REALTIME,
      sessionId: 'ws-1'
    })

    const decoded = fromBinary(NormalizedEventSchema, event.binary)
    expect(decoded.payload.case).toBe('disconnect')
    expect(decoded.meta.session_id).toBe('ws-1')
    expect(event.payloadCase).toBe('disconnect')
  })

  test('encodes control error payload', () => {
    const errorMessage: ControlErrorMessage = {
      type: 'error',
      exchange: 'bitmex',
      symbol: 'BTCUSD',
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      details: 'WS connection failed',
      subsequentErrors: 3,
      code: 'ws_connect'
    }

    const [event] = encoder.encode(errorMessage, baseMeta)
    expect(event.payloadCase).toBe('error')

    const decoded = fromBinary(NormalizedEventSchema, event.binary)

    expect(decoded.payload.case).toBe('error')
    if (decoded.payload.case !== 'error') {
      throw new Error('expected control error payload')
    }

    expect(decoded.payload.value.details).toBe('WS connection failed')
    expect(decoded.payload.value.subsequentErrors).toBe(3)
    expect(decoded.payload.value.code).toBe(ErrorCode.WS_CONNECT)
    expect(decoded.meta.data_type).toBe('error')
  })

  test('supports custom key templates', () => {
    const keyBuilder = compileKeyBuilder('{{exchange}}/{{payloadCase}}/{{meta.request_id}}')
    const customEncoder = new BronzeNormalizedEventEncoder(keyBuilder)

    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      id: 'k-1',
      price: 40250,
      amount: 0.5,
      side: 'sell',
      timestamp: new Date('2024-01-01T00:00:02.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:02.250Z')
    }

    const [event] = customEncoder.encode(trade, {
      ...baseMeta,
      requestId: 'req-key'
    })

    expect(event.key).toBe('bitmex/trade/req-key')
  })
})
