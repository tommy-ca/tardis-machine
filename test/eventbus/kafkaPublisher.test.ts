import { Kafka, type IHeaders } from 'kafkajs'
import { KafkaContainer, StartedKafkaContainer } from '@testcontainers/kafka'
import { fromBinary } from '@bufbuild/protobuf'
import { KafkaEventBus } from '../../src/eventbus/kafka'
import { Origin, NormalizedEventSchema, NormalizedEvent } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Trade as NormalizedTrade, BookChange as NormalizedBookChange } from 'tardis-dev'

jest.setTimeout(240000)

const baseTopic = 'bronze.events.test'
const routingTopics = {
  trade: `${baseTopic}.trades`,
  bookChange: `${baseTopic}.books`
}
const metaHeadersTopic = `${baseTopic}.meta`
const allTopics = [baseTopic, routingTopics.trade, routingTopics.bookChange, metaHeadersTopic]
const startTimeoutMs = 180000

const baseMeta = {
  source: 'unit-test',
  origin: Origin.REPLAY,
  ingestTimestamp: new Date('2024-01-01T00:00:00.000Z'),
  requestId: 'req-1'
}

describe('KafkaEventBus', () => {
  let container: StartedKafkaContainer
  let brokers: string[]
  let kafka: Kafka

  beforeAll(async () => {
    try {
      container = await startKafkaContainer()
      brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`]
      kafka = new Kafka({ clientId: 'bronze-test-admin', brokers })

      const admin = kafka.admin()
      await admin.connect()
      await waitForKafkaController(admin)
      await admin.createTopics({ topics: allTopics.map((topic) => ({ topic, numPartitions: 1 })) })
      await admin.disconnect()
    } catch (error) {
      shouldSkip = true
      console.warn('Skipping KafkaEventBus integration test:', error)
    }
  })

  afterAll(async () => {
    if (container && !shouldSkip) {
      await container.stop()
    }
  })

  test('publishes bronze normalized events to kafka', async () => {
    if (shouldSkip) {
      return
    }
    const bus = new KafkaEventBus({
      brokers,
      topic: baseTopic,
      clientId: 'bronze-producer',
      maxBatchSize: 2,
      maxBatchDelayMs: 20,
      compression: 'gzip'
    })

    await bus.start()

    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      id: 't-1',
      price: 32000.5,
      amount: 0.25,
      side: 'buy',
      timestamp: new Date('2024-01-01T00:00:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:01.100Z')
    }

    const bookChange: NormalizedBookChange = {
      type: 'book_change',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      isSnapshot: false,
      bids: [
        { price: 31999.5, amount: 1.5 },
        { price: 31999, amount: 0 }
      ],
      asks: [{ price: 32001, amount: 2 }],
      timestamp: new Date('2024-01-01T00:00:01.500Z'),
      localTimestamp: new Date('2024-01-01T00:00:01.600Z')
    }

    await bus.publish(trade, baseMeta)
    await bus.publish(bookChange, baseMeta)
    await bus.flush()

    let received: NormalizedEvent[] = []
    try {
      received = await consumeEvents(kafka, baseTopic, 4)
    } finally {
      await bus.close().catch(() => undefined)
    }

    expect(received).toHaveLength(4)
    const tradeEvent = received.find((evt) => evt.payload.case === 'trade')
    expect(tradeEvent?.meta.request_id).toBe('req-1')
    const bookChanges = received.filter((evt) => evt.payload.case === 'bookChange')
    expect(bookChanges.length).toBe(3)
    expect(bookChanges[0]?.payload.case).toBe('bookChange')
  })

  test('routes bronze normalized events by payload case', async () => {
    if (shouldSkip) {
      return
    }

    const bus = new KafkaEventBus({
      brokers,
      topic: metaHeadersTopic,
      topicByPayloadCase: {
        trade: routingTopics.trade,
        bookChange: routingTopics.bookChange
      },
      clientId: 'bronze-producer-routing',
      maxBatchSize: 2,
      maxBatchDelayMs: 20
    })

    await bus.start()

    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      id: 't-2',
      price: 2200.75,
      amount: 0.5,
      side: 'sell',
      timestamp: new Date('2024-01-01T00:01:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:01:01.100Z')
    }

    const bookChange: NormalizedBookChange = {
      type: 'book_change',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      isSnapshot: false,
      bids: [{ price: 2199.5, amount: 1.2 }],
      asks: [
        { price: 2201.25, amount: 1.1 },
        { price: 2201.5, amount: 0 }
      ],
      timestamp: new Date('2024-01-01T00:01:01.500Z'),
      localTimestamp: new Date('2024-01-01T00:01:01.600Z')
    }

    await bus.publish(trade, baseMeta)
    await bus.publish(bookChange, baseMeta)
    await bus.flush()

    const [tradeEvents, bookChangeEvents] = await Promise.all([
      consumeEvents(kafka, routingTopics.trade, 1),
      consumeEvents(kafka, routingTopics.bookChange, 3)
    ])

    await bus.close().catch(() => undefined)

    expect(tradeEvents).toHaveLength(1)
    expect(tradeEvents[0]?.payload.case).toBe('trade')
    expect(tradeEvents[0]?.meta.request_id).toBe('req-1')

    expect(bookChangeEvents).toHaveLength(3)
    expect(bookChangeEvents.every((event) => event.payload.case === 'bookChange')).toBe(true)
  })

  test('publishes normalized meta as kafka headers', async () => {
    if (shouldSkip) {
      return
    }

    const bus = new KafkaEventBus({
      brokers,
      topic: metaHeadersTopic,
      clientId: 'bronze-producer-meta-headers',
      maxBatchSize: 1,
      maxBatchDelayMs: 5,
      metaHeadersPrefix: 'meta.'
    })

    await bus.start()

    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'SOLUSD',
      exchange: 'bitmex',
      id: 't-meta-1',
      price: 150.125,
      amount: 10,
      side: 'buy',
      timestamp: new Date('2024-01-01T00:02:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:02:01.100Z')
    }

    const meta = {
      ...baseMeta,
      requestId: 'req-headers',
      sessionId: 'sess-1',
      extraMeta: {
        transport: 'websocket'
      }
    } as const

    expect(meta.requestId).toBe('req-headers')

    await bus.publish(trade, meta)
    await bus.flush()

    const records = await consumeRecords(kafka, metaHeadersTopic, 1)
    await bus.close().catch(() => undefined)

    expect(records).toHaveLength(1)
    const { headers, event } = records[0]
    expect(event.meta.request_id).toBe('req-headers')
    expect(event.meta.session_id).toBe('sess-1')
    expect(event.meta.transport).toBe('websocket')
    expect(headers['payloadCase']).toBe('trade')
    expect(headers['meta.request_id']).toBe('req-headers')
    expect(headers['meta.session_id']).toBe('sess-1')
    expect(headers['meta.transport']).toBe('websocket')
  })
})

let shouldSkip = false

async function startKafkaContainer() {
  const container = new KafkaContainer('confluentinc/cp-kafka:7.5.3').withStartupTimeout(startTimeoutMs)
  return container.start()
}

async function consumeEvents(
  kafka: Kafka,
  topic: string,
  expectedCount: number,
  timeoutMs = 120000
) {
  const consumer = kafka.consumer({ groupId: `bronze-test-${topic}-${Date.now()}` })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  try {
    return await collectKafkaEvents(consumer, expectedCount, timeoutMs)
  } finally {
    await consumer.disconnect().catch(() => undefined)
  }
}

async function consumeRecords(
  kafka: Kafka,
  topic: string,
  expectedCount: number,
  timeoutMs = 120000
): Promise<KafkaRecord[]> {
  const consumer = kafka.consumer({ groupId: `bronze-test-${topic}-${Date.now()}` })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  try {
    return await collectKafkaRecords(consumer, expectedCount, timeoutMs)
  } finally {
    await consumer.disconnect().catch(() => undefined)
  }
}

type KafkaRecord = {
  event: NormalizedEvent
  headers: Record<string, string>
}

async function collectKafkaEvents(
  consumer: ReturnType<Kafka['consumer']>,
  expectedCount: number,
  timeoutMs: number
): Promise<NormalizedEvent[]> {
  const records = await collectKafkaRecords(consumer, expectedCount, timeoutMs)
  return records.map((record) => record.event)
}

async function collectKafkaRecords(
  consumer: ReturnType<Kafka['consumer']>,
  expectedCount: number,
  timeoutMs: number
): Promise<KafkaRecord[]> {
  const records: KafkaRecord[] = []
  let completed = false

  return new Promise<KafkaRecord[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(
        new Error(
          `Timed out after ${timeoutMs}ms waiting for ${expectedCount} Kafka events (received ${records.length})`
        )
      )
    }, timeoutMs)

    consumer
      .run({
        eachMessage: async ({ message }) => {
          if (completed || !message.value) {
            return
          }

          try {
            const event = fromBinary(NormalizedEventSchema, message.value)
            records.push({ event, headers: normalizeHeaders(message.headers) })
          } catch (error) {
            completed = true
            clearTimeout(timer)
            reject(error as Error)
            return
          }

          if (records.length >= expectedCount) {
            completed = true
            clearTimeout(timer)
            resolve(records)
          }
        }
      })
      .catch((error) => {
        if (completed) {
          return
        }
        completed = true
        clearTimeout(timer)
        reject(error)
      })
  }).finally(async () => {
    await consumer.stop().catch(() => undefined)
  })
}

function normalizeHeaders(headers: IHeaders | undefined): Record<string, string> {
  if (!headers) {
    return {}
  }

  return Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key, headerValueToString(value)])
  )
}

function headerValueToString(value: IHeaders[string]): string {
  if (value === undefined) {
    return ''
  }

  if (Array.isArray(value)) {
    return value.map((entry) => headerValueToString(entry as IHeaders[string])).join(',')
  }

  if (typeof value === 'string') {
    return value
  }

  return value.toString()
}

async function waitForKafkaController(
  admin: ReturnType<Kafka['admin']>,
  timeoutMs = 60000
) {
  const start = Date.now()
  while (true) {
    try {
      await admin.describeCluster()
      return
    } catch (error) {
      if (Date.now() - start >= timeoutMs) {
        throw error
      }
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
}
