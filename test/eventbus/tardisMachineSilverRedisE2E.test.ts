import fetch from 'node-fetch'
import { createClient } from 'redis'
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { TradeRecordSchema, Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'

jest.setTimeout(240000)

const PORT = 8106
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const stream = 'silver:records:e2e'
const cacheDir = './.cache-silver-redis-e2e'
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
    console.warn('Skipping tardis-machine Silver Redis E2E test:', error)
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

test('publishes replay-normalized events to Silver Redis with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    silverEventBus: {
      provider: 'redis-silver',
      url: redisUrl,
      stream,
      maxBatchSize: 32,
      maxBatchDelayMs: 25
    }
  })

  await machine.start(PORT)

  const redis = createClient({ url: redisUrl })
  await redis.connect()

  const eventsPromise = collectSilverEvents(redis, stream, 5, 120000)
  let events: any[] = []

  try {
    const options = {
      exchange: 'binance',
      symbols: ['btcusdt'],
      from: '2023-01-01',
      to: '2023-01-01 00:01',
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
  const sample = events[0]
  expect(sample).toHaveProperty('recordType')
  expect(sample).toHaveProperty('dataType')

  if (sample.recordType === 'trade') {
    expect(sample.record).toHaveProperty('exchange')
    expect(sample.record).toHaveProperty('symbol')
    expect(sample.record).toHaveProperty('timestamp')
  }
})

function encodeOptions(options: any): string {
  return encodeURIComponent(JSON.stringify(options))
}

async function collectSilverEvents(
  redis: ReturnType<typeof createClient>,
  streamName: string,
  minCount: number,
  timeoutMs: number
): Promise<any[]> {
  const events: any[] = []
  let completed = false

  return new Promise<any[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${minCount} Silver Redis events (received ${events.length})`))
    }, timeoutMs)

    const checkStream = async () => {
      try {
        const result = await redis.xRead([{ key: streamName, id: '0' }], { COUNT: 100, BLOCK: 1000 })
        if (result) {
          for (const stream of result as any) {
            if (stream && stream.messages) {
              for (const message of stream.messages) {
                if (completed) return
                try {
                  const data = message.message.data
                  const recordType = message.message.recordType as string
                  if (Buffer.isBuffer(data)) {
                    let record
                    if (recordType === 'trade') {
                      record = fromBinary(TradeRecordSchema, new Uint8Array(data))
                    } else {
                      // For other types, just store the data
                      record = data
                    }
                    events.push({
                      recordType,
                      dataType: message.message.dataType,
                      record
                    })
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
        }

        if (events.length >= minCount) {
          completed = true
          clearTimeout(timer)
          resolve(events)
          return
        }

        if (!completed) {
          setTimeout(checkStream, 100)
        }
      } catch (error) {
        if (!completed) {
          completed = true
          clearTimeout(timer)
          reject(error as Error)
        }
      }
    }

    checkStream()
  })
}

async function startRedisContainer(): Promise<StartedTestContainer> {
  const container = new GenericContainer(redisImage)
    .withExposedPorts(6379)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
    .withStartupTimeout(startTimeoutMs)

  return await container.start()
}
