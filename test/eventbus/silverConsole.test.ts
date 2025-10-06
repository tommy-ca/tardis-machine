import { SilverConsoleEventBus } from '../../src/eventbus/silverConsole'
import { Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'
import type { Trade as NormalizedTrade } from 'tardis-dev'

const baseMeta = {
  source: 'unit-test',
  origin: Origin.REPLAY,
  ingestTimestamp: new Date('2024-01-01T00:00:00.000Z'),
  requestId: 'req-1'
}

describe('SilverConsoleEventBus', () => {
  let consoleSpy: jest.SpyInstance

  beforeEach(() => {
    consoleSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(() => {
    consoleSpy.mockRestore()
  })

  it('should log silver events to console', async () => {
    const eventBus = new SilverConsoleEventBus({ prefix: '[TEST]' })

    const tradeMessage: NormalizedTrade = {
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

    await eventBus.start()
    await eventBus.publish(tradeMessage, baseMeta)
    await eventBus.flush()
    await eventBus.close()

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('[TEST] Key:'))
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('RecordType: trade'))
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('DataType: trade'))
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('BinaryLength:'))
  })

  it('should use default prefix when not specified', async () => {
    const eventBus = new SilverConsoleEventBus({})

    const tradeMessage: NormalizedTrade = {
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

    await eventBus.start()
    await eventBus.publish(tradeMessage, baseMeta)
    await eventBus.flush()
    await eventBus.close()

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('[SILVER_EVENTBUS]'))
  })
})
