import { getNormalizers, constructDataTypeFilter, getComputables } from '../src/helpers'
import {
  normalizeTrades,
  normalizeBookChanges,
  normalizeDerivativeTickers,
  normalizeLiquidations,
  normalizeOptionsSummary,
  normalizeBookTickers
} from 'tardis-dev'

describe('getNormalizers', () => {
  test('yields normalizeTrades for trade dataType', () => {
    const normalizers = Array.from(getNormalizers(['trade']))
    expect(normalizers).toContain(normalizeTrades)
  })

  test('yields normalizeTrades for trade_bar dataType', () => {
    const normalizers = Array.from(getNormalizers(['trade_bar_1m']))
    expect(normalizers).toContain(normalizeTrades)
  })

  test('yields normalizeBookChanges for book_change dataType', () => {
    const normalizers = Array.from(getNormalizers(['book_change']))
    expect(normalizers).toContain(normalizeBookChanges)
  })

  test('yields normalizeBookChanges for book_snapshot dataType', () => {
    const normalizers = Array.from(getNormalizers(['book_snapshot_10_100ms']))
    expect(normalizers).toContain(normalizeBookChanges)
  })

  test('yields normalizeBookChanges for quote dataType', () => {
    const normalizers = Array.from(getNormalizers(['quote']))
    expect(normalizers).toContain(normalizeBookChanges)
  })

  test('yields normalizeDerivativeTickers for derivative_ticker dataType', () => {
    const normalizers = Array.from(getNormalizers(['derivative_ticker']))
    expect(normalizers).toContain(normalizeDerivativeTickers)
  })

  test('yields normalizeLiquidations for liquidation dataType', () => {
    const normalizers = Array.from(getNormalizers(['liquidation']))
    expect(normalizers).toContain(normalizeLiquidations)
  })

  test('yields normalizeOptionsSummary for option_summary dataType', () => {
    const normalizers = Array.from(getNormalizers(['option_summary']))
    expect(normalizers).toContain(normalizeOptionsSummary)
  })

  test('yields normalizeBookTickers for book_ticker dataType', () => {
    const normalizers = Array.from(getNormalizers(['book_ticker']))
    expect(normalizers).toContain(normalizeBookTickers)
  })

  test('yields multiple normalizers for multiple dataTypes', () => {
    const normalizers = Array.from(getNormalizers(['trade', 'book_change', 'derivative_ticker']))
    expect(normalizers).toContain(normalizeTrades)
    expect(normalizers).toContain(normalizeBookChanges)
    expect(normalizers).toContain(normalizeDerivativeTickers)
  })
})

describe('constructDataTypeFilter', () => {
  test('returns filter that accepts messages matching requested dataTypes', () => {
    const options = [
      { exchange: 'binance', dataTypes: ['trade'], withDisconnectMessages: false },
      { exchange: 'bitmex', dataTypes: ['book_change'], withDisconnectMessages: false }
    ]
    const filter = constructDataTypeFilter(options)

    expect(filter({ type: 'trade', exchange: 'binance' } as any)).toBe(true)
    expect(filter({ type: 'book_change', exchange: 'bitmex' } as any)).toBe(true)
    expect(filter({ type: 'trade', exchange: 'bitmex' } as any)).toBe(false)
  })

  test('returns filter that accepts disconnect messages when withDisconnectMessages is true', () => {
    const options = [{ exchange: 'binance', dataTypes: ['trade'], withDisconnectMessages: true }]
    const filter = constructDataTypeFilter(options)

    expect(filter({ type: 'disconnect', exchange: 'binance' } as any)).toBe(true)
  })

  test('returns filter that rejects disconnect messages when withDisconnectMessages is false', () => {
    const options = [{ exchange: 'binance', dataTypes: ['trade'], withDisconnectMessages: false }]
    const filter = constructDataTypeFilter(options)

    expect(filter({ type: 'disconnect', exchange: 'binance' } as any)).toBe(false)
  })

  test('combines dataTypes for same exchange', () => {
    const options = [
      { exchange: 'binance', dataTypes: ['trade'], withDisconnectMessages: false },
      { exchange: 'binance', dataTypes: ['book_change'], withDisconnectMessages: false }
    ]
    const filter = constructDataTypeFilter(options)

    expect(filter({ type: 'trade', exchange: 'binance' } as any)).toBe(true)
    expect(filter({ type: 'book_change', exchange: 'binance' } as any)).toBe(true)
  })
})

describe('getComputables', () => {
  test('returns empty array for no computable dataTypes', () => {
    const computables = getComputables(['trade'])
    expect(computables).toEqual([])
  })

  test('returns trade bar computable for trade_bar dataType', () => {
    const computables = getComputables(['trade_bar_1m'])
    expect(computables).toHaveLength(1)
    // Since computeTradeBars returns a function, we can check if it's called with correct params
    // But for simplicity, just check length and that it's a function
    expect(typeof computables[0]).toBe('function')
  })

  test('returns book snapshot computable for book_snapshot dataType', () => {
    const computables = getComputables(['book_snapshot_10_100ms'])
    expect(computables).toHaveLength(1)
    expect(typeof computables[0]).toBe('function')
  })

  test('returns quote computable for quote dataType', () => {
    const computables = getComputables(['quote'])
    expect(computables).toHaveLength(1)
    expect(typeof computables[0]).toBe('function')
  })

  test('returns multiple computables for multiple dataTypes', () => {
    const computables = getComputables(['trade_bar_1m', 'book_snapshot_10_100ms'])
    expect(computables).toHaveLength(2)
  })
})

describe('getComputables', () => {
  test('throws error for invalid trade bar dataType', () => {
    expect(() => getComputables(['trade_bar_invalid_m'])).toThrow('invalid interval: invalid')
  })

  test('throws error for invalid book snapshot dataType', () => {
    expect(() => getComputables(['book_snapshot_invalid_100ms'])).toThrow('invalid depth: invalid')
  })

  test('throws error for invalid quote dataType', () => {
    expect(() => getComputables(['quote_invalidms'])).toThrow('invalid interval: invalid')
  })
})
