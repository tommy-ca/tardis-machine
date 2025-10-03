import { create, toBinary } from '@bufbuild/protobuf'
import {
  Action,
  BarKind,
  ControlDisconnect,
  ControlDisconnectSchema,
  DerivativeTicker,
  DerivativeTickerSchema,
  Liquidation,
  LiquidationSchema,
  NormalizedEvent,
  NormalizedEventSchema,
  OptionSummary,
  OptionSummarySchema,
  Origin,
  Side,
  Trade,
  TradeBar,
  TradeBarSchema,
  TradeSchema,
  BookChange,
  BookChangeSchema,
  Quote,
  QuoteSchema,
  BookSnapshot,
  BookSnapshotSchema,
  GroupedBookSnapshot,
  GroupedBookSnapshotSchema,
  BookTicker,
  BookTickerSchema
} from '../generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Optional, BookChange as NormalizedBookChange, BookSnapshot as NormalizedBookSnapshot, BookTicker as NormalizedBookTicker, DerivativeTicker as NormalizedDerivativeTicker, Disconnect, Liquidation as NormalizedLiquidation, NormalizedData, OptionSummary as NormalizedOptionSummary, Trade as NormalizedTrade, TradeBar as NormalizedTradeBar } from 'tardis-dev'
import type { BronzeEvent, BronzePayloadCase, NormalizedEventEncoder, NormalizedMessage, PublishMeta } from './types'

type EventRecord = {
  event: NormalizedEvent
  payloadCase: BronzePayloadCase
  dataType: string
}

export type KeyBuilder = (event: NormalizedEvent, payloadCase: BronzePayloadCase) => string

const defaultKeyBuilder: KeyBuilder = (event, payloadCase) => {
  const symbol = event.symbol ?? ''
  return `${event.exchange}|${symbol}|${payloadCase}`
}

export class BronzeNormalizedEventEncoder implements NormalizedEventEncoder {
  constructor(private readonly keyBuilder: KeyBuilder = defaultKeyBuilder) {}

  encode(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[] {
    const records = buildEvents(message, meta)
    return records.map(({ event, payloadCase, dataType }) => ({
      key: this.keyBuilder(event, payloadCase),
      payloadCase,
      dataType,
      binary: toBinary(NormalizedEventSchema, event)
    }))
  }
}

function buildEvents(message: NormalizedMessage, meta: PublishMeta): EventRecord[] {
  if (isDisconnect(message)) {
    return [buildDisconnectEvent(message, meta)]
  }

  const typed = message as NormalizedData
  switch (typed.type) {
    case 'trade':
      return [buildTradeEvent(typed as NormalizedTrade, meta)]
    case 'book_change':
      return buildBookChangeEvents(typed as NormalizedBookChange, meta)
    case 'book_snapshot':
      return [buildBookSnapshotEvent(typed as NormalizedBookSnapshot, meta)]
    case 'derivative_ticker':
      return [buildDerivativeTickerEvent(typed as NormalizedDerivativeTicker, meta)]
    case 'liquidation':
      return [buildLiquidationEvent(typed as NormalizedLiquidation, meta)]
    case 'option_summary':
      return [buildOptionSummaryEvent(typed as NormalizedOptionSummary, meta)]
    case 'book_ticker':
      return [buildBookTickerEvent(typed as NormalizedBookTicker, meta)]
    case 'trade_bar':
      return [buildTradeBarEvent(typed as NormalizedTradeBar, meta)]
    default:
      return []
  }
}

function buildTradeEvent(message: NormalizedTrade, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'trade',
    value: create(TradeSchema, {
      tradeId: message.id ?? '',
      priceStr: decimalToString(message.price),
      qtyStr: decimalToString(message.amount),
      side: toSide(message.side),
      eventTs: dateToTimestamp(message.timestamp)
    })
  }

  return {
    event,
    payloadCase: 'trade',
    dataType: message.type
  }
}

function buildBookChangeEvents(message: NormalizedBookChange, meta: PublishMeta): EventRecord[] {
  const events: EventRecord[] = []

  const { bids, asks } = message
  for (const level of bids) {
    const event = buildBookChange(message, meta, level, Side.BUY)
    if (event) {
      events.push(event)
    }
  }
  for (const level of asks) {
    const event = buildBookChange(message, meta, level, Side.SELL)
    if (event) {
      events.push(event)
    }
  }

  if (events.length === 0) {
    const base = createBaseEvent(message, meta)
    base.payload = {
      case: 'bookChange',
      value: create(BookChangeSchema, {
        side: Side.UNSPECIFIED,
        action: Action.UNSPECIFIED,
        priceStr: '0',
        qtyStr: '0'
      })
    }
    events.push({ event: base, payloadCase: 'bookChange', dataType: message.type })
  }

  return events
}

function buildBookChange(
  message: NormalizedBookChange,
  meta: PublishMeta,
  level: Optional<{ price: number; amount: number }> | undefined,
  side: Side
): EventRecord | undefined {
  if (!level || level.price === undefined || level.amount === undefined) {
    return undefined
  }

  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'bookChange',
    value: create(BookChangeSchema, {
      side,
      action: level.amount === 0 ? Action.DELETE : Action.UPSERT,
      priceStr: decimalToString(level.price),
      qtyStr: decimalToString(Math.abs(level.amount)),
      eventTs: dateToTimestamp(message.timestamp)
    })
  }

  return {
    event,
    payloadCase: 'bookChange',
    dataType: message.type
  }
}

function buildBookSnapshotEvent(message: NormalizedBookSnapshot, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  const bids = message.bids
    .filter((level): level is Optional<{ price: number; amount: number }> => !!level)
    .map((level) => ({
      priceStr: level.price !== undefined ? decimalToString(level.price) : '',
      qtyStr: level.amount !== undefined ? decimalToString(level.amount) : ''
    }))
  const asks = message.asks
    .filter((level): level is Optional<{ price: number; amount: number }> => !!level)
    .map((level) => ({
      priceStr: level.price !== undefined ? decimalToString(level.price) : '',
      qtyStr: level.amount !== undefined ? decimalToString(level.amount) : ''
    }))

  const snapshot = create(message.grouping ? GroupedBookSnapshotSchema : BookSnapshotSchema, {
    depth: message.depth,
    bids,
    asks,
    eventTs: dateToTimestamp(message.timestamp),
    grouping: message.grouping,
    intervalMs: message.interval,
    removeCrossedLevels: true
  }) as BookSnapshot | GroupedBookSnapshot

  event.payload = message.grouping
    ? { case: 'groupedBookSnapshot', value: snapshot as GroupedBookSnapshot }
    : { case: 'bookSnapshot', value: snapshot as BookSnapshot }

  return {
    event,
    payloadCase: message.grouping ? 'groupedBookSnapshot' : 'bookSnapshot',
    dataType: message.type
  }
}

function buildDerivativeTickerEvent(message: NormalizedDerivativeTicker, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'derivativeTicker',
    value: create(DerivativeTickerSchema, {
      markPriceStr: numberToOptionalString(message.markPrice),
      indexPriceStr: numberToOptionalString(message.indexPrice),
      fundingRateStr: numberToOptionalString(message.fundingRate),
      eventTs: dateToTimestamp(message.timestamp)
    }) as DerivativeTicker
  }

  return {
    event,
    payloadCase: 'derivativeTicker',
    dataType: message.type
  }
}

function buildLiquidationEvent(message: NormalizedLiquidation, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'liquidation',
    value: create(LiquidationSchema, {
      priceStr: decimalToString(message.price),
      qtyStr: decimalToString(message.amount),
      side: toSide(message.side),
      orderId: message.id ?? '',
      eventTs: dateToTimestamp(message.timestamp)
    }) as Liquidation
  }

  return {
    event,
    payloadCase: 'liquidation',
    dataType: message.type
  }
}

function buildOptionSummaryEvent(message: NormalizedOptionSummary, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'optionSummary',
    value: create(OptionSummarySchema, {
      ivStr: numberToOptionalString(message.markIV ?? message.bestBidIV ?? message.bestAskIV),
      deltaStr: numberToOptionalString(message.delta),
      gammaStr: numberToOptionalString(message.gamma),
      thetaStr: numberToOptionalString(message.theta),
      vegaStr: numberToOptionalString(message.vega),
      oiStr: numberToOptionalString(message.openInterest),
      eventTs: dateToTimestamp(message.timestamp)
    }) as OptionSummary
  }

  return {
    event,
    payloadCase: 'optionSummary',
    dataType: message.type
  }
}

function buildBookTickerEvent(message: NormalizedBookTicker, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'bookTicker',
    value: create(BookTickerSchema, {
      bestBidPriceStr: numberToOptionalString(message.bidPrice),
      bestBidQtyStr: numberToOptionalString(message.bidAmount),
      bestAskPriceStr: numberToOptionalString(message.askPrice),
      bestAskQtyStr: numberToOptionalString(message.askAmount),
      eventTs: dateToTimestamp(message.timestamp)
    }) as BookTicker
  }

  return {
    event,
    payloadCase: 'bookTicker',
    dataType: message.type
  }
}

function buildTradeBarEvent(message: NormalizedTradeBar, meta: PublishMeta): EventRecord {
  const event = createBaseEvent(message, meta)
  event.payload = {
    case: 'tradeBar',
    value: create(TradeBarSchema, {
      kind: toBarKind(message.kind),
      interval: BigInt(message.interval),
      intervalMs: message.kind === 'time' ? message.interval : 0,
      openStr: decimalToString(message.open),
      highStr: decimalToString(message.high),
      lowStr: decimalToString(message.low),
      closeStr: decimalToString(message.close),
      volumeStr: decimalToString(message.volume),
      tradeCount: BigInt(message.trades),
      eventTs: dateToTimestamp(message.openTimestamp ?? message.timestamp),
      endTs: dateToTimestamp(message.closeTimestamp ?? message.timestamp)
    }) as TradeBar
  }

  return {
    event,
    payloadCase: 'tradeBar',
    dataType: message.type
  }
}

function buildDisconnectEvent(message: Disconnect, meta: PublishMeta): EventRecord {
  const event = createBaseEventForDisconnect(message, meta)
  event.payload = {
    case: 'disconnect',
    value: create(ControlDisconnectSchema, {
      reason: 'stream_disconnected'
    }) as ControlDisconnect
  }

  return {
    event,
    payloadCase: 'disconnect',
    dataType: 'disconnect'
  }
}

function createBaseEvent(message: NormalizedData, meta: PublishMeta): NormalizedEvent {
  return create(NormalizedEventSchema, {
    source: meta.source,
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    localTs: dateToTimestamp(message.localTimestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    origin: meta.origin,
    meta: buildMeta(message, meta),
    payload: { case: undefined }
  })
}

function createBaseEventForDisconnect(message: Disconnect, meta: PublishMeta): NormalizedEvent {
  return create(NormalizedEventSchema, {
    source: meta.source,
    exchange: message.exchange,
    symbol: message.symbols && message.symbols.length > 0 ? message.symbols[0] : '',
    localTs: dateToTimestamp(message.localTimestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    origin: meta.origin,
    meta: buildMeta(message, meta),
    payload: { case: undefined }
  })
}

function buildMeta(message: Partial<NormalizedData> | Disconnect, meta: PublishMeta): Record<string, string> {
  const result: Record<string, string> = {}

  if ('type' in message && message.type) {
    result['data_type'] = message.type
  }
  if ('name' in message && message.name) {
    result['data_name'] = message.name
  }
  if ('isSnapshot' in message && typeof message.isSnapshot === 'boolean') {
    result['is_snapshot'] = String(message.isSnapshot)
  }
  if ('symbols' in message && message.symbols) {
    result['symbols'] = message.symbols.join(',')
  }
  if (meta.requestId) {
    result['request_id'] = meta.requestId
  }
  if (meta.sessionId) {
    result['session_id'] = meta.sessionId
  }
  if (meta.extraMeta) {
    for (const [key, value] of Object.entries(meta.extraMeta)) {
      if (value !== undefined) {
        result[key] = value
      }
    }
  }

  return result
}

function dateToTimestamp(date: Date | undefined) {
  if (!date) {
    return undefined
  }
  const seconds = BigInt(Math.floor(date.getTime() / 1000))
  const millis = date.getTime() % 1000
  const micros = (date as any).μs ?? 0
  const nanos = millis * 1_000_000 + micros * 1_000
  return { seconds, nanos }
}

function decimalToString(value: number): string {
  if (!Number.isFinite(value)) {
    return '0'
  }
  return value.toString()
}

function numberToOptionalString(value: number | undefined): string {
  if (value === undefined || Number.isNaN(value)) {
    return ''
  }
  return value.toString()
}

function toSide(side: NormalizedTrade['side'] | NormalizedLiquidation['side']): Side {
  switch (side) {
    case 'buy':
      return Side.BUY
    case 'sell':
      return Side.SELL
    default:
      return Side.UNSPECIFIED
  }
}

function toBarKind(kind: NormalizedTradeBar['kind']): BarKind {
  switch (kind) {
    case 'tick':
      return BarKind.TICK
    case 'volume':
      return BarKind.VOLUME
    case 'time':
      return BarKind.TIME
    default:
      return BarKind.UNSPECIFIED
  }
}

function isDisconnect(message: NormalizedMessage): message is Disconnect {
  return (message as any).type === 'disconnect' && !(message as any).timestamp
}
