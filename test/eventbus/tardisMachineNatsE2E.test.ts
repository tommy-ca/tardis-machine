import fetch from 'node-fetch'
import { connect, type NatsConnection } from 'nats'
import { NatsContainer, StartedNatsContainer } from '@testcontainers/nats'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8095
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const subject = 'bronze.events.e2e'
const cacheDir = './.cache-nats-e2e'
const natsImage = 'nats:2.10-alpine'
const startTimeoutMs = 180000

let container: StartedNatsContainer
let natsUrl: string
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startNatsContainer()
    natsUrl = `${container.getHost()}:${container.getMappedPort(4222)}`
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine NATS E2E test:', error)
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

test('publishes replay-normalized events to NATS with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'nats',
      servers: [natsUrl],
      subject
    }
  })

  await machine.start(PORT)

  const connection = await connect({ servers: [natsUrl] })
  const eventsPromise = collectEvents(connection, subject, 5, 120000)
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

async function collectEvents(connection: NatsConnection, subject: string, minCount: number, timeoutMs: number): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  let completed = false
  let subscription: any

  return new Promise<NormalizedEvent[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${minCount} NATS events (received ${events.length})`))
    }, timeoutMs)

    subscription = connection.subscribe(subject, {
      callback: (err, msg) => {
        if (completed) {
          return
        }
        if (err) {
          completed = true
          clearTimeout(timer)
          reject(err)
          return
        }
        try {
          events.push(fromBinary(NormalizedEventSchema, msg.data))
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
  }).finally(async () => {
    subscription.unsubscribe()
  })
}

async function startNatsContainer() {
  const container = new NatsContainer(natsImage).withStartupTimeout(startTimeoutMs)
  return container.start()
}
