import fetch from 'node-fetch'
import { createClient } from 'redis'
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8093
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const stream = 'bronze:events:e2e'
const cacheDir = './.cache-redis-e2e'
const redisImage = 'redis:7-alpine'
const startTimeoutMs = 180000

let container: StartedTestContainer
let redisUrl: string
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startRedisContainer()
    redisUrl = `redis://${container.getHost()}:${container.getMappedPort(6379)}`
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine Redis E2E test:', error)
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

test('publishes replay-normalized events to Redis with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'redis',
      url: redisUrl,
      stream,
      maxBatchSize: 32,
      maxBatchDelayMs: 25
    }
  })

  await machine.start(PORT)

  const redis = createClient({ url: redisUrl })
  await redis.connect()

  const eventsPromise = collectEvents(redis, stream, 5, 120000)
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
    await redis.disconnect().catch(() => undefined)
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

async function collectEvents(
  redis: ReturnType<typeof createClient>,
  streamName: string,
  minCount: number,
  timeoutMs: number
): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  let completed = false

  return new Promise<NormalizedEvent[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${minCount} Redis events (received ${events.length})`))
    }, timeoutMs)

    const checkStream = async () => {
      try {
        const result = await redis.xRead([{ key: streamName, id: '0' }], { COUNT: 100, BLOCK: 1000 })
        if (result) {
          for (const stream of result) {
            for (const message of stream.messages) {
              if (completed) return
              try {
                const data = message.message.data
                if (Buffer.isBuffer(data)) {
                  events.push(fromBinary(NormalizedEventSchema, new Uint8Array(data)))
                } else {
                  throw new Error('Expected data to be Buffer')
                }
              } catch (error) {
                completed = true
                clearTimeout(timer)
                reject(error as Error)
                return
              }
            }
          }
        }

        if (events.length >= minCount) {
          completed = true
          clearTimeout(timer)
          resolve(events)
        } else {
          setTimeout(checkStream, 500)
        }
      } catch (error) {
        if (completed) return
        completed = true
        clearTimeout(timer)
        reject(error)
      }
    }

    checkStream()
  })
}

async function startRedisContainer() {
  const container = new GenericContainer(redisImage)
    .withExposedPorts(6379)
    .withWaitStrategy(Wait.forLogMessage('Ready to accept connections'))
    .withStartupTimeout(startTimeoutMs)
  return container.start()
}
