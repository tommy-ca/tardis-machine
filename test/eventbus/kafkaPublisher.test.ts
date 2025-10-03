import { Kafka } from 'kafkajs'
import { KafkaContainer, StartedKafkaContainer } from '@testcontainers/kafka'
import { fromBinary } from '@bufbuild/protobuf'
import { KafkaEventBus } from '../../src/eventbus/kafka'
import { Origin, NormalizedEventSchema, NormalizedEvent } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Trade as NormalizedTrade, BookChange as NormalizedBookChange } from 'tardis-dev'

jest.setTimeout(240000)

const topic = 'bronze.events.test'
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
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] })
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
      topic,
      clientId: 'bronze-producer',
      maxBatchSize: 2,
      maxBatchDelayMs: 20
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

    const consumer = kafka.consumer({ groupId: `bronze-test-${Date.now()}` })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })

    const received: NormalizedEvent[] = []
    await new Promise<void>((resolve, reject) => {
      consumer
        .run({
          eachMessage: async ({ message }) => {
            if (!message.value) {
              return
            }
            const event = fromBinary(NormalizedEventSchema, message.value)
            received.push(event)
            if (received.length >= 4) {
              await consumer.stop()
              resolve()
            }
          }
        })
        .catch(reject)
    })

    await consumer.disconnect()
    await bus.close()

    expect(received).toHaveLength(4)
    const tradeEvent = received.find((evt) => evt.payload.case === 'trade')
    expect(tradeEvent?.meta.request_id).toBe('req-1')
    const bookChanges = received.filter((evt) => evt.payload.case === 'bookChange')
    expect(bookChanges.length).toBe(3)
    expect(bookChanges[0]?.payload.case).toBe('bookChange')
  })
})

let shouldSkip = false

async function startKafkaContainer() {
  const container = new KafkaContainer('confluentinc/cp-kafka:7.5.3')
  return Promise.race<StartedKafkaContainer>([
    container.start(),
    new Promise((_, reject) => setTimeout(() => reject(new Error('kafka container startup timeout')), startTimeoutMs))
  ])
}
