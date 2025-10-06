import { create, toBinary } from '@bufbuild/protobuf'
import {
  Action,
  BarKind,
  Origin,
  Side,
  TradeRecord,
  TradeRecordSchema,
  BookChangeRecord,
  BookChangeRecordSchema,
  BookSnapshotRecord,
  BookSnapshotRecordSchema,
  BookSnapshotRecord_LevelSchema,
  GroupedBookSnapshotRecord,
  GroupedBookSnapshotRecordSchema,
  GroupedBookSnapshotRecord_LevelSchema,
  TradeBarRecord,
  TradeBarRecordSchema,
  QuoteRecord,
  QuoteRecordSchema,
  DerivativeTickerRecord,
  DerivativeTickerRecordSchema,
  LiquidationRecord,
  LiquidationRecordSchema,
  OptionSummaryRecord,
  OptionSummaryRecordSchema,
  BookTickerRecord,
  BookTickerRecordSchema
} from '../generated/lakehouse/silver/v1/records_pb'
import type {
  Optional,
  BookChange as NormalizedBookChange,
  BookSnapshot as NormalizedBookSnapshot,
  BookTicker as NormalizedBookTicker,
  DerivativeTicker as NormalizedDerivativeTicker,
  Disconnect,
  Liquidation as NormalizedLiquidation,
  NormalizedData,
  OptionSummary as NormalizedOptionSummary,
  Trade as NormalizedTrade,
  TradeBar as NormalizedTradeBar
} from 'tardis-dev'
import type { SilverEvent, SilverEventEncoder, SilverRecordType, NormalizedMessage, PublishMeta } from './types'

type SilverRecord = {
  record: any
  recordType: SilverRecordType
  dataType: string
}

const defaultKeyBuilder: SilverKeyBuilder = (record, recordType, _dataType) => {
  const symbol = record.symbol ?? ''
  return `${record.exchange}|${symbol}|${recordType}`
}

export type SilverKeyBuilder = (record: any, recordType: SilverRecordType, dataType: string) => string

type NormalizedQuote = NormalizedData & {
  type: 'quote'
  bidPrice?: number
  bidAmount?: number
  askPrice?: number
  askAmount?: number
}

type SnapshotMetadata = {
  grouping?: number
  interval?: number
  intervalMs?: number
  removeCrossedLevels?: boolean
  sequence?: number | bigint | string | null
}

type NormalizedBookSnapshotWithMetadata = NormalizedBookSnapshot & SnapshotMetadata

type NormalizedGroupedBookSnapshot = NormalizedData &
  SnapshotMetadata & {
    type: 'grouped_book_snapshot'
    depth: number
    bids: Array<Optional<{ price: number; amount: number }>>
    asks: Array<Optional<{ price: number; amount: number }>>
  }

export class SilverNormalizedEventEncoder implements SilverEventEncoder {
  constructor(private readonly keyBuilder: SilverKeyBuilder = defaultKeyBuilder) {}

  encode(message: NormalizedMessage, meta: PublishMeta): SilverEvent[] {
    const records = buildRecords(message, meta)
    return records.map(({ record, recordType, dataType }) => ({
      key: this.keyBuilder(record, recordType, dataType),
      recordType,
      dataType,
      meta: {}, // Silver records don't have meta like Bronze
      binary: toBinary(getSchema(recordType), record)
    }))
  }
}

function buildRecords(message: NormalizedMessage, meta: PublishMeta): SilverRecord[] {
  if (isDisconnect(message)) {
    return [] // Silver doesn't have disconnect records
  }

  if (isControlError(message)) {
    return [] // Silver doesn't have error records
  }

  const typed = message as NormalizedData
  switch (typed.type) {
    case 'trade':
      return [buildTradeRecord(typed as NormalizedTrade, meta)]
    case 'book_change':
      return buildBookChangeRecords(typed as NormalizedBookChange, meta)
    case 'book_snapshot':
      return [buildBookSnapshotRecord(typed as NormalizedBookSnapshot, meta)]
    case 'grouped_book_snapshot':
      return [buildGroupedBookSnapshotRecord(typed as NormalizedGroupedBookSnapshot, meta)]
    case 'quote':
      return [buildQuoteRecord(typed as NormalizedQuote, meta)]
    case 'derivative_ticker':
      return [buildDerivativeTickerRecord(typed as NormalizedDerivativeTicker, meta)]
    case 'liquidation':
      return [buildLiquidationRecord(typed as NormalizedLiquidation, meta)]
    case 'option_summary':
      return [buildOptionSummaryRecord(typed as NormalizedOptionSummary, meta)]
    case 'book_ticker':
      return [buildBookTickerRecord(typed as NormalizedBookTicker, meta)]
    case 'trade_bar':
      return [buildTradeBarRecord(typed as NormalizedTradeBar, meta)]
    default:
      return []
  }
}

function buildTradeRecord(message: NormalizedTrade, meta: PublishMeta): SilverRecord {
  const record = create(TradeRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    tradeId: message.id ?? '',
    side: toSide(message.side),
    priceE8: BigInt(decimalToE8(message.price)),
    qtyE8: BigInt(decimalToE8(message.amount)),
    sequence: optionalBigInt(message.id),
    origin: meta.origin
  })

  return {
    record,
    recordType: 'trade',
    dataType: message.type
  }
}

function buildBookChangeRecords(message: NormalizedBookChange, meta: PublishMeta): SilverRecord[] {
  const records: SilverRecord[] = []

  const { bids, asks } = message
  for (const level of bids) {
    const record = buildBookChangeRecord(message, meta, level, Side.BUY)
    if (record) {
      records.push(record)
    }
  }
  for (const level of asks) {
    const record = buildBookChangeRecord(message, meta, level, Side.SELL)
    if (record) {
      records.push(record)
    }
  }

  if (records.length === 0) {
    const record = create(BookChangeRecordSchema, {
      exchange: message.exchange,
      symbol: message.symbol ?? '',
      eventTs: dateToTimestamp(message.timestamp),
      ingestTs: dateToTimestamp(meta.ingestTimestamp),
      side: Side.UNSPECIFIED,
      action: Action.UNSPECIFIED,
      priceE8: BigInt(0),
      qtyE8: BigInt(0),
      level: 0,
      sequence: undefined,
      origin: meta.origin
    })
    records.push({ record, recordType: 'book_change', dataType: message.type })
  }

  return records
}

function buildBookChangeRecord(
  message: NormalizedBookChange,
  meta: PublishMeta,
  level: Optional<{ price: number; amount: number }> | undefined,
  side: Side
): SilverRecord | undefined {
  if (!level || level.price === undefined || level.amount === undefined) {
    return undefined
  }

  const record = create(BookChangeRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    side,
    action: level.amount === 0 ? Action.DELETE : Action.UPSERT,
    priceE8: BigInt(decimalToE8(level.price)),
    qtyE8: BigInt(decimalToE8(Math.abs(level.amount))),
    level: 0, // TODO: if level info available
    sequence: undefined,
    origin: meta.origin
  })

  return {
    record,
    recordType: 'book_change',
    dataType: message.type
  }
}

function buildBookSnapshotRecord(message: NormalizedBookSnapshotWithMetadata, meta: PublishMeta): SilverRecord {
  const recordType = message.grouping ? 'grouped_book_snapshot' : 'book_snapshot'
  return buildSnapshotRecord(message, meta, recordType)
}

function buildGroupedBookSnapshotRecord(message: NormalizedGroupedBookSnapshot, meta: PublishMeta): SilverRecord {
  return buildSnapshotRecord(message, meta, 'grouped_book_snapshot')
}

function buildSnapshotRecord(
  message: NormalizedBookSnapshotWithMetadata | NormalizedGroupedBookSnapshot,
  meta: PublishMeta,
  recordType: 'book_snapshot' | 'grouped_book_snapshot'
): SilverRecord {
  const schema = recordType === 'grouped_book_snapshot' ? GroupedBookSnapshotRecordSchema : BookSnapshotRecordSchema

  const record = create(schema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    depth: message.depth,
    bids: mapSnapshotLevels(message.bids),
    asks: mapSnapshotLevels(message.asks),
    grouping: message.grouping,
    intervalMs: resolveIntervalMs(message),
    removeCrossedLevels: message.removeCrossedLevels ?? true,
    sequence: optionalBigInt(message.sequence),
    origin: meta.origin
  })

  return {
    record,
    recordType,
    dataType: message.type
  }
}

function mapSnapshotLevels(levels: Array<Optional<{ price: number; amount: number }> | undefined>): Array<any> {
  return levels
    .filter((level): level is Optional<{ price: number; amount: number }> => !!level)
    .map((level) =>
      create(BookSnapshotRecord_LevelSchema, {
        priceE8: BigInt(level.price !== undefined ? decimalToE8(level.price) : 0),
        qtyE8: BigInt(level.amount !== undefined ? decimalToE8(level.amount) : 0)
      })
    )
}

function resolveIntervalMs(message: SnapshotMetadata): number | undefined {
  if (typeof message.intervalMs === 'number') {
    return message.intervalMs
  }
  if (typeof message.interval === 'number') {
    return message.interval
  }
  return undefined
}

function buildDerivativeTickerRecord(message: NormalizedDerivativeTicker, meta: PublishMeta): SilverRecord {
  const record = create(DerivativeTickerRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    markPriceE8: BigInt(numberToE8(message.markPrice)),
    indexPriceE8: BigInt(numberToE8(message.indexPrice)),
    fundingRateE9: BigInt(numberToE9(message.fundingRate)),
    origin: meta.origin
  })

  return {
    record,
    recordType: 'derivative_ticker',
    dataType: message.type
  }
}

function buildLiquidationRecord(message: NormalizedLiquidation, meta: PublishMeta): SilverRecord {
  const record = create(LiquidationRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    side: toSide(message.side),
    priceE8: BigInt(decimalToE8(message.price)),
    qtyE8: BigInt(decimalToE8(message.amount)),
    orderId: message.id ?? '',
    origin: meta.origin
  })

  return {
    record,
    recordType: 'liquidation',
    dataType: message.type
  }
}

function buildOptionSummaryRecord(message: NormalizedOptionSummary, meta: PublishMeta): SilverRecord {
  const record = create(OptionSummaryRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    ivE6: BigInt(numberToE6(message.markIV ?? message.bestBidIV ?? message.bestAskIV)),
    deltaE9: BigInt(numberToE9(message.delta)),
    gammaE9: BigInt(numberToE9(message.gamma)),
    thetaE9: BigInt(numberToE9(message.theta)),
    vegaE9: BigInt(numberToE9(message.vega)),
    oi: BigInt(message.openInterest ?? 0),
    origin: meta.origin
  })

  return {
    record,
    recordType: 'option_summary',
    dataType: message.type
  }
}

function buildBookTickerRecord(message: NormalizedBookTicker, meta: PublishMeta): SilverRecord {
  const record = create(BookTickerRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    bestBidPriceE8: BigInt(numberToE8(message.bidPrice)),
    bestBidQtyE8: BigInt(numberToE8(message.bidAmount)),
    bestAskPriceE8: BigInt(numberToE8(message.askPrice)),
    bestAskQtyE8: BigInt(numberToE8(message.askAmount)),
    origin: meta.origin
  })

  return {
    record,
    recordType: 'book_ticker',
    dataType: message.type
  }
}

function buildQuoteRecord(message: NormalizedQuote, meta: PublishMeta): SilverRecord {
  const record = create(QuoteRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    bidPriceE8: BigInt(numberToE8(message.bidPrice)),
    bidQtyE8: BigInt(numberToE8(message.bidAmount)),
    askPriceE8: BigInt(numberToE8(message.askPrice)),
    askQtyE8: BigInt(numberToE8(message.askAmount)),
    origin: meta.origin
  })

  return {
    record,
    recordType: 'quote',
    dataType: message.type
  }
}

function buildTradeBarRecord(message: NormalizedTradeBar, meta: PublishMeta): SilverRecord {
  const record = create(TradeBarRecordSchema, {
    exchange: message.exchange,
    symbol: message.symbol ?? '',
    eventTs: dateToTimestamp(message.openTimestamp ?? message.timestamp),
    endTs: dateToTimestamp(message.closeTimestamp ?? message.timestamp),
    ingestTs: dateToTimestamp(meta.ingestTimestamp),
    kind: toBarKind(message.kind),
    interval: BigInt(message.interval),
    intervalMs: message.kind === 'time' ? message.interval * 1000 : 0,
    openE8: BigInt(decimalToE8(message.open)),
    highE8: BigInt(decimalToE8(message.high)),
    lowE8: BigInt(decimalToE8(message.low)),
    closeE8: BigInt(decimalToE8(message.close)),
    volumeE8: BigInt(decimalToE8(message.volume)),
    tradeCount: BigInt(message.trades),
    origin: meta.origin
  })

  return {
    record,
    recordType: 'trade_bar',
    dataType: message.type
  }
}

function optionalBigInt(value: number | bigint | string | null | undefined): bigint | undefined {
  if (value === undefined || value === null) {
    return undefined
  }

  if (typeof value === 'bigint') {
    return value
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return undefined
    }
    return BigInt(Math.trunc(value))
  }

  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (trimmed === '') {
      return undefined
    }
    try {
      return BigInt(trimmed)
    } catch (error) {
      return undefined
    }
  }

  return undefined
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

function decimalToE8(value: number): number {
  if (!Number.isFinite(value)) {
    return 0
  }
  return Math.round(value * 100000000)
}

function decimalToE9(value: number): number {
  if (!Number.isFinite(value)) {
    return 0
  }
  return Math.round(value * 1000000000)
}

function numberToE8(value: number | undefined): number {
  if (value === undefined || Number.isNaN(value)) {
    return 0
  }
  return Math.round(value * 100_000_000)
}

function numberToE9(value: number | undefined): number {
  if (value === undefined || Number.isNaN(value)) {
    return 0
  }
  return Math.round(value * 1_000_000_000)
}

function numberToE6(value: number | undefined): number {
  if (value === undefined || Number.isNaN(value)) {
    return 0
  }
  return Math.round(value * 1_000_000)
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

function isControlError(message: NormalizedMessage): message is any {
  return (message as any).type === 'error' && typeof (message as any).details === 'string'
}

function getSchema(recordType: SilverRecordType): any {
  switch (recordType) {
    case 'trade':
      return TradeRecordSchema
    case 'book_change':
      return BookChangeRecordSchema
    case 'book_snapshot':
      return BookSnapshotRecordSchema
    case 'grouped_book_snapshot':
      return GroupedBookSnapshotRecordSchema
    case 'trade_bar':
      return TradeBarRecordSchema
    case 'quote':
      return QuoteRecordSchema
    case 'derivative_ticker':
      return DerivativeTickerRecordSchema
    case 'liquidation':
      return LiquidationRecordSchema
    case 'option_summary':
      return OptionSummaryRecordSchema
    case 'book_ticker':
      return BookTickerRecordSchema
    default:
      throw new Error(`Unknown record type: ${recordType}`)
  }
}
