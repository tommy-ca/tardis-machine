import { fromBinary } from '@bufbuild/protobuf'
import { SilverNormalizedEventEncoder } from '../../src/eventbus/silverMapper'
import { compileSilverKeyBuilder } from '../../src/eventbus/keyTemplate'
import {
  TradeRecordSchema,
  BookChangeRecordSchema,
  BookSnapshotRecordSchema,
  GroupedBookSnapshotRecordSchema,
  TradeBarRecordSchema,
  QuoteRecordSchema,
  DerivativeTickerRecordSchema,
  LiquidationRecordSchema,
  OptionSummaryRecordSchema,
  BookTickerRecordSchema,
  Origin,
  Side,
  Action,
  BarKind
} from '../../src/generated/lakehouse/silver/v1/records_pb'
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

const encoder = new SilverNormalizedEventEncoder()
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

type NormalizedOptionSummary = NormalizedData & {
  type: 'option_summary'
  markIV?: number
  bestBidIV?: number
  bestAskIV?: number
  delta?: number
  gamma?: number
  theta?: number
  vega?: number
  openInterest?: number
}

describe('SilverNormalizedEventEncoder', () => {
  it('encodes trade', () => {
    const message: NormalizedTrade = {
      type: 'trade',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      id: '123',
      price: 50000,
      amount: 1,
      side: 'buy'
    }

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('trade')
    expect(event.dataType).toBe('trade')
    expect(event.key).toBe('binance|btcusdt|trade')

    const decoded = fromBinary(TradeRecordSchema, event.binary)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.tradeId).toBe('123')
    expect(decoded.priceE8).toBe(5000000000000n)
    expect(decoded.qtyE8).toBe(100000000n)
    expect(decoded.side).toBe(Side.BUY)
    expect(decoded.origin).toBe(Origin.REPLAY)
  })

  it('encodes book_change', () => {
    const message = {
      type: 'book_change',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      bids: [{ price: 50000, amount: 1 }],
      asks: [{ price: 50001, amount: 0.5 }],
      isSnapshot: false
    } as NormalizedBookChange

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(2)

    const bidEvent = events.find((e) => {
      const decoded = fromBinary(BookChangeRecordSchema, e.binary)
      return decoded.side === Side.BUY
    })
    const askEvent = events.find((e) => {
      const decoded = fromBinary(BookChangeRecordSchema, e.binary)
      return decoded.side === Side.SELL
    })

    expect(bidEvent).toBeDefined()
    expect(askEvent).toBeDefined()

    if (bidEvent) {
      expect(bidEvent.recordType).toBe('book_change')
      const decoded = fromBinary(BookChangeRecordSchema, bidEvent.binary)
      expect(decoded.side).toBe(Side.BUY)
      expect(decoded.action).toBe(Action.UPSERT)
      expect(decoded.priceE8).toBe(5000000000000n)
      expect(decoded.qtyE8).toBe(100000000n)
    }

    if (askEvent) {
      expect(askEvent.recordType).toBe('book_change')
      const decoded = fromBinary(BookChangeRecordSchema, askEvent.binary)
      expect(decoded.side).toBe(Side.SELL)
      expect(decoded.action).toBe(Action.UPSERT)
      expect(decoded.priceE8).toBe(5000100000000n)
      expect(decoded.qtyE8).toBe(50000000n)
    }

    if (askEvent) {
      expect(askEvent.recordType).toBe('book_change')
      const decoded = fromBinary(BookChangeRecordSchema, askEvent.binary)
      expect(decoded.side).toBe(Side.SELL)
      expect(decoded.action).toBe(Action.UPSERT)
      expect(decoded.priceE8).toBe(5000100000000n)
      expect(decoded.qtyE8).toBe(50000000n)
    }
  })

  it('encodes book_snapshot', () => {
    const message = {
      type: 'book_snapshot',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      bids: [
        { price: 50000, amount: 1 },
        { price: 49999, amount: 2 }
      ],
      asks: [
        { price: 50001, amount: 0.5 },
        { price: 50002, amount: 1.5 }
      ],
      depth: 10
    } as NormalizedBookSnapshot

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('book_snapshot')

    const decoded = fromBinary(BookSnapshotRecordSchema, event.binary)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.bids).toHaveLength(2)
    expect(decoded.asks).toHaveLength(2)
    expect(decoded.bids[0].priceE8).toBe(5000000000000n)
    expect(decoded.bids[0].qtyE8).toBe(100000000n)
  })

  it('encodes grouped_book_snapshot', () => {
    const message: NormalizedGroupedBookSnapshot = {
      type: 'grouped_book_snapshot',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      depth: 2,
      grouping: 1,
      bids: [
        { price: 50000, amount: 1 },
        { price: 49999, amount: 2 }
      ],
      asks: [
        { price: 50001, amount: 0.5 },
        { price: 50002, amount: 1.5 }
      ]
    }

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('grouped_book_snapshot')

    const decoded = fromBinary(GroupedBookSnapshotRecordSchema, event.binary)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.depth).toBe(2)
    expect(decoded.grouping).toBe(1)
    expect(decoded.bids).toHaveLength(2)
    expect(decoded.asks).toHaveLength(2)
  })

  it('encodes quote', () => {
    const message: NormalizedQuote = {
      type: 'quote',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      bidPrice: 50000,
      bidAmount: 1,
      askPrice: 50001,
      askAmount: 0.5
    }

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('quote')

    const decoded = fromBinary(QuoteRecordSchema, event.binary)
    expect(decoded.bidPriceE8).toBe(5000000000000n)
    expect(decoded.bidQtyE8).toBe(100000000n)
    expect(decoded.askPriceE8).toBe(5000100000000n)
    expect(decoded.askQtyE8).toBe(50000000n)
  })

  it('encodes derivative_ticker', () => {
    const message = {
      type: 'derivative_ticker',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      markPrice: 50000,
      indexPrice: 49999,
      fundingRate: 0.0001,
      lastPrice: 50000,
      openInterest: 100,
      fundingTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      predictedFundingRate: 0.0002
    } as NormalizedDerivativeTicker

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('derivative_ticker')

    const decoded = fromBinary(DerivativeTickerRecordSchema, event.binary)
    expect(decoded.markPriceE8).toBe(5000000000000n)
    expect(decoded.indexPriceE8).toBe(4999900000000n)
    expect(decoded.fundingRateE9).toBe(100000n)
  })

  it('encodes liquidation', () => {
    const message: NormalizedLiquidation = {
      type: 'liquidation',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      id: '123',
      price: 50000,
      amount: 1,
      side: 'sell'
    }

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('liquidation')

    const decoded = fromBinary(LiquidationRecordSchema, event.binary)
    expect(decoded.priceE8).toBe(5000000000000n)
    expect(decoded.qtyE8).toBe(100000000n)
    expect(decoded.side).toBe(Side.SELL)
  })

  it('encodes option_summary', () => {
    const message: NormalizedOptionSummary = {
      type: 'option_summary',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      markIV: 0.25,
      delta: 0.5,
      gamma: 0.1,
      theta: -0.05,
      vega: 0.2,
      openInterest: 100
    }

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('option_summary')

    const decoded = fromBinary(OptionSummaryRecordSchema, event.binary)
    expect(decoded.ivE6).toBe(250000n)
    expect(decoded.deltaE9).toBe(500000000n)
    expect(decoded.gammaE9).toBe(100000000n)
    expect(decoded.thetaE9).toBe(-50000000n)
    expect(decoded.vegaE9).toBe(200000000n)
    expect(decoded.oi).toBe(100n)
  })

  it('encodes book_ticker', () => {
    const message: NormalizedBookTicker = {
      type: 'book_ticker',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      bidPrice: 50000,
      bidAmount: 1,
      askPrice: 50001,
      askAmount: 0.5
    }

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('book_ticker')

    const decoded = fromBinary(BookTickerRecordSchema, event.binary)
    expect(decoded.bestBidPriceE8).toBe(5000000000000n)
    expect(decoded.bestBidQtyE8).toBe(100000000n)
    expect(decoded.bestAskPriceE8).toBe(5000100000000n)
    expect(decoded.bestAskQtyE8).toBe(50000000n)
  })

  it('encodes trade_bar', () => {
    const message = {
      type: 'trade_bar',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      openTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      closeTimestamp: new Date('2024-01-01T00:01:00.000Z'),
      open: 50000,
      high: 50100,
      low: 49900,
      close: 50050,
      volume: 10,
      trades: 100,
      kind: 'time',
      interval: 60,
      name: 'btcusdt',
      buyVolume: 5,
      sellVolume: 5,
      vwap: 50025
    } as NormalizedTradeBar

    const events = encoder.encode(message, baseMeta)

    expect(events).toHaveLength(1)
    const event = events[0]
    expect(event.recordType).toBe('trade_bar')

    const decoded = fromBinary(TradeBarRecordSchema, event.binary)
    expect(decoded.kind).toBe(BarKind.TIME)
    expect(decoded.interval).toBe(60n)
    expect(decoded.intervalMs).toBe(60000)
    expect(decoded.openE8).toBe(5000000000000n)
    expect(decoded.highE8).toBe(5010000000000n)
    expect(decoded.lowE8).toBe(4990000000000n)
    expect(decoded.closeE8).toBe(5005000000000n)
    expect(decoded.volumeE8).toBe(1000000000n)
    expect(decoded.tradeCount).toBe(100n)
  })

  it('ignores disconnect messages', () => {
    const message: Disconnect = {
      type: 'disconnect',
      exchange: 'binance',
      symbols: ['btcusdt'],
      localTimestamp: new Date('2024-01-01T00:00:00.000Z')
    }

    const events = encoder.encode(message, baseMeta)
    expect(events).toHaveLength(0)
  })

  it('ignores control error messages', () => {
    const message: ControlErrorMessage = {
      type: 'error',
      exchange: 'binance',
      symbol: 'btcusdt',
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      details: 'test error',
      code: 'ws_connect'
    }

    const events = encoder.encode(message, baseMeta)
    expect(events).toHaveLength(0)
  })

  it('uses custom key builder', () => {
    const customEncoder = new SilverNormalizedEventEncoder((record, recordType) => `${record.exchange}-${record.symbol}-${recordType}`)
    const message: NormalizedTrade = {
      type: 'trade',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      id: '123',
      price: 50000,
      amount: 1,
      side: 'buy'
    }

    const events = customEncoder.encode(message, baseMeta)
    expect(events[0].key).toBe('binance-btcusdt-trade')
  })
})
