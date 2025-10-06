import fetch from 'node-fetch'
import * as amqp from 'amqplib'
import { RabbitMQContainer, StartedRabbitMQContainer } from '@testcontainers/rabbitmq'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8194
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const exchange = 'bronze.events.e2e'
const cacheDir = './.cache-rabbitmq-e2e'
const rabbitmqImage = 'rabbitmq:3-management-alpine'
const startTimeoutMs = 180000

let container: StartedRabbitMQContainer
let amqpUrl: string
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startRabbitMQContainer()
    amqpUrl = container.getAmqpUrl()
    // Wait for RabbitMQ to be ready
    const testConn = await amqp.connect(amqpUrl)
    await testConn.close()
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine RabbitMQ E2E test:', error)
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

test('publishes replay-normalized events to RabbitMQ with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'rabbitmq',
      url: amqpUrl,
      exchange,
      exchangeType: 'direct'
    }
  })

  await machine.start(PORT)

  const connection = await amqp.connect(amqpUrl)
  connection.on('error', (err) => console.warn('RabbitMQ connection error:', err))
  connection.on('close', () => console.warn('RabbitMQ connection closed'))
  const channel = await connection.createChannel()
  await channel.assertExchange(exchange, 'direct', { durable: true })
  const queue = await channel.assertQueue('', { exclusive: true })
  await channel.bindQueue(queue.queue, exchange, 'default')

  const eventsPromise = collectEvents(channel, queue.queue, 5, 120000)
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
    await channel.close().catch(() => undefined)
    await connection.close().catch(() => undefined)
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

async function collectEvents(channel: amqp.Channel, queue: string, minCount: number, timeoutMs: number): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  let completed = false

  return new Promise<NormalizedEvent[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${minCount} RabbitMQ events (received ${events.length})`))
    }, timeoutMs)

    channel.consume(queue, (msg) => {
      if (completed || !msg) {
        return
      }
      try {
        events.push(fromBinary(NormalizedEventSchema, msg.content))
        channel.ack(msg)
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
    })
  })
}

async function startRabbitMQContainer() {
  const container = new RabbitMQContainer(rabbitmqImage).withStartupTimeout(startTimeoutMs)
  return container.start()
}
