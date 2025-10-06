import fetch from 'node-fetch'
import { Kafka } from 'kafkajs'
import { KafkaContainer, StartedKafkaContainer } from '@testcontainers/kafka'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { TradeRecordSchema, Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'

jest.setTimeout(240000)

const PORT = 8093
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const topic = 'silver.records.e2e'
const cacheDir = './.cache-silver-kafka-e2e'
const kafkaImage = 'confluentinc/cp-kafka:7.5.3'
const startTimeoutMs = 180000

let container: StartedKafkaContainer
let brokers: string[]
let kafka: Kafka
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startKafkaContainer()
    brokers = [`${container.getHost()}:${container.getMappedPort(9093)}`]
    kafka = new Kafka({ clientId: 'silver-e2e-admin', brokers })

    const admin = kafka.admin()
    await admin.connect()
    await waitForKafkaController(admin)
    await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] })
    await admin.disconnect()
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine Silver Kafka E2E test:', error)
    if (container) {
      await container.stop().catch(() => undefined)
    }
  }
})

afterAll(async () => {
  if (container && !shouldSkip) {
    await container.stop()
  }
  await rm(cacheDir, { recursive: true, force: true }).catch(() => undefined)
})

test('publishes replay-normalized events to Silver Kafka with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const server = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY,
    cacheDir,
    eventBus: undefined, // Bronze disabled
    silverEventBus: {
      provider: 'kafka-silver',
      brokers,
      topic
    }
  })

  await server.start(PORT)

  try {
    const response = await fetch(HTTP_REPLAY_NORMALIZED_URL, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        exchange: 'binance',
        symbols: ['btcusdt'],
        from: '2024-01-01',
        to: '2024-01-01T00:01:00.000Z'
      })
    })

    expect(response.status).toBe(200)

    // Wait for publishing to complete
    await new Promise((resolve) => setTimeout(resolve, 5000))

    const consumer = kafka.consumer({ groupId: 'silver-e2e-consumer' })
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })

    const messages: any[] = []
    let messageCount = 0
    const maxMessages = 10

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        consumer.disconnect().then(() => reject(new Error('Timeout waiting for messages')))
      }, 30000)

      consumer.run({
        eachMessage: async ({ message: kafkaMessage }) => {
          messages.push(kafkaMessage)
          messageCount++

          if (messageCount >= maxMessages) {
            clearTimeout(timeout)
            await consumer.disconnect()
            resolve()
          }
        }
      })
    })

    expect(messages.length).toBeGreaterThan(0)

    // Verify at least one trade record
    const tradeMessages = messages.filter((m) => {
      try {
        const decoded = fromBinary(TradeRecordSchema, m.value!)
        return decoded.tradeId !== ''
      } catch {
        return false
      }
    })

    expect(tradeMessages.length).toBeGreaterThan(0)

    const sampleMessage = tradeMessages[0]
    const decoded = fromBinary(TradeRecordSchema, sampleMessage.value!)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.origin).toBe(Origin.REPLAY)
    expect(typeof decoded.priceE8).toBe('bigint')
    expect(typeof decoded.qtyE8).toBe('bigint')

    // Verify headers
    const headers = sampleMessage.headers as any
    expect(headers.recordType?.toString()).toBe('trade')
    expect(headers.dataType?.toString()).toBe('trade')
  } finally {
    await server.stop()
  }
})

async function startKafkaContainer(): Promise<StartedKafkaContainer> {
  const container = await new KafkaContainer(kafkaImage).withExposedPorts(9093).start()
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
