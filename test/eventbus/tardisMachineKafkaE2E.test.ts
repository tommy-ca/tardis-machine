import fetch from 'node-fetch'
import { Kafka } from 'kafkajs'
import { KafkaContainer, StartedKafkaContainer } from '@testcontainers/kafka'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8092
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const topic = 'bronze.events.e2e'
const cacheDir = './.cache-kafka-e2e'
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
    kafka = new Kafka({ clientId: 'bronze-e2e-admin', brokers })

    const admin = kafka.admin()
    await admin.connect()
    await waitForKafkaController(admin)
    await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] })
    await admin.disconnect()
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine Kafka E2E test:', error)
    if (container) {
      await container.stop().catch(() => undefined)
    }
  }
})

afterAll(async () => {
  if (container && !shouldSkip) {
    await container.stop()
  }
})

test('publishes replay-normalized events to Kafka with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'kafka',
      brokers,
      topic,
      clientId: 'bronze-e2e-producer',
      maxBatchSize: 32,
      maxBatchDelayMs: 25
    }
  })

  await machine.start(PORT)

  const consumer = kafka.consumer({ groupId: `bronze-e2e-consumer-${Date.now()}` })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  const eventsPromise = collectEvents(consumer, 5, 120000)
  let events: NormalizedEvent[] = []

  try {
    const options = {
      exchange: 'bitmex',
      symbols: ['ETHUSD'],
      from: '2019-06-01',
      to: '2019-06-01 00:01',
      dataTypes: ['trade']
    }

    const params = encodeOptions(options)
    const response = await fetch(`${HTTP_REPLAY_NORMALIZED_URL}?options=${params}`)
    expect(response.status).toBe(200)
    await response.text()

    events = await eventsPromise
  } finally {
    await eventsPromise.catch(() => undefined)
    await machine.stop().catch(() => undefined)
    await consumer.disconnect().catch(() => undefined)
    await rm(cacheDir, { recursive: true, force: true }).catch(() => undefined)
  }

  expect(events.length).toBeGreaterThanOrEqual(1)
  const replayEvents = events.filter((event) => event.origin === Origin.REPLAY)
  expect(replayEvents.length).toBeGreaterThan(0)

  const sample = replayEvents[0]
  expect(sample.meta?.transport).toBe('http')
  expect(sample.meta?.route).toBe('/replay-normalized')
  expect(sample.meta?.request_id).toBeDefined()
  expect(sample.meta?.app_version).toBeDefined()
  expect(sample.payload.case).not.toBe('error')
})

function encodeOptions(options: any): string {
  return encodeURIComponent(JSON.stringify(options))
}

async function collectEvents(consumer: ReturnType<Kafka['consumer']>, minCount: number, timeoutMs: number): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  let completed = false

  return new Promise<NormalizedEvent[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${minCount} Kafka events (received ${events.length})`))
    }, timeoutMs)

    consumer
      .run({
        eachMessage: async ({ message }) => {
          if (completed || !message.value) {
            return
          }
          try {
            events.push(fromBinary(NormalizedEventSchema, message.value))
          } catch (error) {
            completed = true
            clearTimeout(timer)
            reject(error as Error)
            return
          }

          if (events.length >= minCount) {
            completed = true
            clearTimeout(timer)
            resolve(events)
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

async function startKafkaContainer() {
  const container = new KafkaContainer(kafkaImage).withStartupTimeout(startTimeoutMs)
  return container.start()
}

async function waitForKafkaController(admin: ReturnType<Kafka['admin']>, timeoutMs = 60000) {
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
