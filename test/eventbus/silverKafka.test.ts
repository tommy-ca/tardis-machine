import { Kafka, type IHeaders } from 'kafkajs'
import { KafkaContainer, StartedKafkaContainer } from '@testcontainers/kafka'
import { fromBinary } from '@bufbuild/protobuf'
import { SilverKafkaEventBus } from '../../src/eventbus/silverKafka'
import { Origin, TradeRecordSchema, BookChangeRecordSchema } from '../../src/generated/lakehouse/silver/v1/records_pb'
import type { Trade as NormalizedTrade, BookChange as NormalizedBookChange } from 'tardis-dev'

jest.setTimeout(240000)

const baseTopic = 'silver.records.test'
const routingTopics = {
  trade: `${baseTopic}.trades`,
  book_change: `${baseTopic}.books`
}
const metaHeadersTopic = `${baseTopic}.meta`
const staticHeadersTopic = `${baseTopic}.static`
const allTopics = [baseTopic, routingTopics.trade, routingTopics.book_change, metaHeadersTopic, staticHeadersTopic]
const startTimeoutMs = 180000

const baseMeta = {
  source: 'unit-test',
  origin: Origin.REPLAY,
  ingestTimestamp: new Date('2024-01-01T00:00:00.000Z'),
  requestId: 'req-1'
}

let shouldSkip = false

describe('SilverKafkaEventBus', () => {
  let container: StartedKafkaContainer
  let brokers: string[]
  let kafka: Kafka

  beforeAll(async () => {
    try {
      container = await startKafkaContainer()
      brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`]
      kafka = new Kafka({ clientId: 'silver-test-admin', brokers })

      const admin = kafka.admin()
      await admin.connect()
      await waitForKafkaController(admin)
      await admin.createTopics({ topics: allTopics.map((topic) => ({ topic, numPartitions: 1 })) })
      await admin.disconnect()
    } catch (error) {
      shouldSkip = true
      console.warn('Skipping SilverKafkaEventBus integration test:', error)
    }
  })

  afterAll(async () => {
    if (container && !shouldSkip) {
      await container.stop()
    }
  })

  beforeEach(() => {
    if (shouldSkip) {
      return
    }
  })

  it('publishes trade records', async () => {
    const config = {
      brokers,
      topic: baseTopic,
      topicByRecordType: routingTopics
    }
    const bus = new SilverKafkaEventBus(config)
    await bus.start()

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

    await bus.publish(message, baseMeta)
    await bus.flush()
    await bus.close()

    const consumer = kafka.consumer({ groupId: 'silver-test-trade' })
    await consumer.connect()
    await consumer.subscribe({ topic: routingTopics.trade, fromBeginning: true })

    const messages: any[] = []
    await new Promise<void>((resolve) => {
      consumer.run({
        eachMessage: async ({ message: kafkaMessage }) => {
          messages.push(kafkaMessage)
          resolve()
        }
      })
    })

    await consumer.disconnect()

    expect(messages).toHaveLength(1)
    const kafkaMessage = messages[0]
    expect(kafkaMessage.key?.toString()).toBe('binance|btcusdt|trade')

    const decoded = fromBinary(TradeRecordSchema, kafkaMessage.value!)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.tradeId).toBe('123')
    expect(decoded.priceE8).toBe(5000000000000n)
    expect(decoded.qtyE8).toBe(100000000n)
  })

  it('publishes book change records', async () => {
    const config = {
      brokers,
      topic: baseTopic,
      topicByRecordType: routingTopics
    }
    const bus = new SilverKafkaEventBus(config)
    await bus.start()

    const message: NormalizedBookChange = {
      type: 'book_change',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      bids: [{ price: 50000, amount: 1 }],
      asks: [{ price: 50001, amount: 0.5 }],
      isSnapshot: false
    }

    await bus.publish(message, baseMeta)
    await bus.flush()
    await bus.close()

    const consumer = kafka.consumer({ groupId: 'silver-test-book' })
    await consumer.connect()
    await consumer.subscribe({ topic: routingTopics.book_change, fromBeginning: true })

    const messages: any[] = []
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Timeout waiting for messages')), 10000)
      let count = 0
      consumer.run({
        eachMessage: async ({ message: kafkaMessage }) => {
          messages.push(kafkaMessage)
          count++
          if (count === 2) {
            clearTimeout(timeout)
            resolve()
          }
        }
      })
    })

    await consumer.disconnect()

    expect(messages).toHaveLength(2)

    const bidMessage = messages.find((m) => {
      const decoded = fromBinary(BookChangeRecordSchema, m.value!)
      return decoded.side === 1 // BUY
    })
    const askMessage = messages.find((m) => {
      const decoded = fromBinary(BookChangeRecordSchema, m.value!)
      return decoded.side === 2 // SELL
    })

    expect(bidMessage).toBeDefined()
    expect(askMessage).toBeDefined()

    if (bidMessage) {
      const decoded = fromBinary(BookChangeRecordSchema, bidMessage.value!)
      expect(decoded.priceE8).toBe(5000000000000n)
      expect(decoded.qtyE8).toBe(100000000n)
    }

    if (askMessage) {
      const decoded = fromBinary(BookChangeRecordSchema, askMessage.value!)
      expect(decoded.priceE8).toBe(5000100000000n)
      expect(decoded.qtyE8).toBe(50000000n)
    }
  })

  it('applies static headers', async () => {
    const config = {
      brokers,
      topic: staticHeadersTopic,
      staticHeaders: {
        deployment: 'test',
        version: '1.0'
      }
    }
    const bus = new SilverKafkaEventBus(config)
    await bus.start()

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

    await bus.publish(message, baseMeta)
    await bus.flush()
    await bus.close()

    const consumer = kafka.consumer({ groupId: 'silver-test-static' })
    await consumer.connect()
    await consumer.subscribe({ topic: staticHeadersTopic, fromBeginning: true })

    const messages: any[] = []
    await new Promise<void>((resolve) => {
      consumer.run({
        eachMessage: async ({ message: kafkaMessage }) => {
          messages.push(kafkaMessage)
          resolve()
        }
      })
    })

    await consumer.disconnect()

    expect(messages).toHaveLength(1)
    const kafkaMessage = messages[0]
    const headers = kafkaMessage.headers as IHeaders
    expect(headers.deployment?.toString()).toBe('test')
    expect(headers.version?.toString()).toBe('1.0')
    expect(headers.recordType?.toString()).toBe('trade')
    expect(headers.dataType?.toString()).toBe('trade')
  })

  it('filters record types', async () => {
    const config = {
      brokers,
      topic: baseTopic,
      includeRecordTypes: ['trade' as const]
    }
    const bus = new SilverKafkaEventBus(config)
    await bus.start()

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

    const bookMessage: NormalizedBookChange = {
      type: 'book_change',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:00.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:00.000Z'),
      bids: [{ price: 50000, amount: 1 }],
      asks: [],
      isSnapshot: false
    }

    await bus.publish(tradeMessage, baseMeta)
    await bus.publish(bookMessage, baseMeta)
    await bus.flush()
    await bus.close()

    const consumer = kafka.consumer({ groupId: 'silver-test-filter' })
    await consumer.connect()
    await consumer.subscribe({ topic: baseTopic, fromBeginning: true })

    const messages: any[] = []
    await new Promise<void>((resolve) => {
      consumer.run({
        eachMessage: async ({ message: kafkaMessage }) => {
          messages.push(kafkaMessage)
          resolve()
        }
      })
    })

    await consumer.disconnect()

    expect(messages).toHaveLength(1)
    const decoded = fromBinary(TradeRecordSchema, messages[0].value!)
    expect(decoded.exchange).toBe('binance')
  })
})

async function startKafkaContainer(): Promise<StartedKafkaContainer> {
  const container = await new KafkaContainer('confluentinc/cp-kafka:7.4.0').withExposedPorts(9093).start()
  return container
}

async function waitForKafkaController(admin: any): Promise<void> {
  let attempts = 0
  while (attempts < 30) {
    try {
      await admin.describeCluster()
      return
    } catch (error) {
      attempts++
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }
  throw new Error('Kafka controller not ready')
}
