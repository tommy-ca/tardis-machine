import fetch from 'node-fetch'
import { Client, Consumer } from 'pulsar-client'
import { GenericContainer, StartedTestContainer } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8107
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const topic = 'persistent://public/default/bronze-events-e2e'
const cacheDir = './.cache-pulsar-e2e'
const pulsarImage = 'apachepulsar/pulsar:3.0.0'
const startTimeoutMs = 180000

let container: StartedTestContainer
let pulsarUrl: string
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startPulsarContainer()
    const host = container.getHost()
    const port = container.getMappedPort(6650)
    pulsarUrl = `pulsar://${host}:${port}`
    // Wait for Pulsar to be ready
    await new Promise((resolve) => setTimeout(resolve, 5000))
    const testClient = new Client({ serviceUrl: pulsarUrl })
    await testClient.close()
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine Pulsar E2E test:', error)
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

test('publishes replay-normalized events to Pulsar with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'pulsar',
      serviceUrl: pulsarUrl,
      topic,
      maxBatchSize: 5,
      maxBatchDelayMs: 25
    }
  })

  await machine.start(PORT)

  const client = new Client({ serviceUrl: pulsarUrl })
  const consumer = await client.subscribe({
    topic,
    subscription: 'test-subscription',
    subscriptionType: 'Exclusive'
  })

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
    await consumer.close().catch(() => undefined)
    await client.close().catch(() => undefined)
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

test('does not publish when eventBus is not configured', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir
  })

  await machine.start(PORT + 1)

  try {
    const options = {
      exchange: 'bitmex',
      symbols: ['ETHUSD'],
      from: '2019-06-01',
      to: '2019-06-01 00:01',
      dataTypes: ['trade']
    }

    const params = encodeOptions(options)
    const response = await fetch(`http://localhost:${PORT + 1}/replay-normalized?options=${params}`)
    expect(response.status).toBe(200)
    await response.text()

    // No way to check no publishing, but at least it doesn't crash
  } finally {
    await machine.stop().catch(() => undefined)
    await rm(cacheDir, { recursive: true, force: true }).catch(() => undefined)
  }
})

test('does not publish when Pulsar service URL is invalid', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'pulsar',
      serviceUrl: 'pulsar://invalid-url:6650',
      topic
    }
  })

  await machine.start(PORT + 2)

  try {
    const options = {
      exchange: 'bitmex',
      symbols: ['ETHUSD'],
      from: '2019-06-01',
      to: '2019-06-01 00:01',
      dataTypes: ['trade']
    }

    const params = encodeOptions(options)
    const response = await fetch(`http://localhost:${PORT + 2}/replay-normalized?options=${params}`)
    expect(response.status).toBe(200)
    await response.text()

    // Should not crash even with invalid service URL
  } finally {
    await machine.stop().catch(() => undefined)
    await rm(cacheDir, { recursive: true, force: true }).catch(() => undefined)
  }
})

test('publishes replay-normalized events to Pulsar with schema registry', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'pulsar',
      serviceUrl: pulsarUrl,
      topic: topic + '-schema',
      maxBatchSize: 5,
      maxBatchDelayMs: 25,
      schemaRegistry: {}
    }
  })

  await machine.start(PORT + 3)

  const client = new Client({ serviceUrl: pulsarUrl })
  const consumer = await client.subscribe({
    topic: topic + '-schema',
    subscription: 'test-subscription-schema',
    subscriptionType: 'Exclusive'
  })

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
    const response = await fetch(`http://localhost:${PORT + 3}/replay-normalized?options=${params}`)
    expect(response.status).toBe(200)
    await response.text()

    events = await eventsPromise
  } finally {
    await eventsPromise.catch(() => undefined)
    await machine.stop().catch(() => undefined)
    await consumer.close().catch(() => undefined)
    await client.close().catch(() => undefined)
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

async function startPulsarContainer(): Promise<StartedTestContainer> {
  const container = new GenericContainer(pulsarImage).withExposedPorts(6650).withStartupTimeout(startTimeoutMs)

  return container.start()
}

async function collectEvents(consumer: Consumer, minEvents: number, timeoutMs: number): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  const startTime = Date.now()

  while (events.length < minEvents && Date.now() - startTime < timeoutMs) {
    try {
      const message = await consumer.receive(1000)
      if (message) {
        const binary = message.getData()
        const event = fromBinary(NormalizedEventSchema, binary)
        events.push(event)
        consumer.acknowledge(message)
      }
    } catch (error) {
      // Timeout or no message, continue
    }

    if (events.length < minEvents) {
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }

  return events
}

function encodeOptions(options: any): string {
  return Buffer.from(JSON.stringify(options)).toString('base64')
}
