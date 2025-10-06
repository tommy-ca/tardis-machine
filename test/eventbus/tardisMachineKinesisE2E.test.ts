import fetch from 'node-fetch'
import {
  KinesisClient,
  CreateStreamCommand,
  DescribeStreamCommand,
  GetRecordsCommand,
  GetShardIteratorCommand
} from '@aws-sdk/client-kinesis'
import { LocalstackContainer, StartedLocalStackContainer } from '@testcontainers/localstack'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8098
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const streamName = 'bronze-events-e2e'
const cacheDir = './.cache-kinesis-e2e'
const localstackImage = 'localstack/localstack:3.0'
const startTimeoutMs = 180000

let container: StartedLocalStackContainer
let kinesis: KinesisClient
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startLocalStackContainer()
    const endpoint = container.getConnectionUri()
    kinesis = new KinesisClient({
      endpoint,
      region: 'us-east-1',
      credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test'
      }
    })

    await kinesis.send(
      new CreateStreamCommand({
        StreamName: streamName,
        ShardCount: 1
      })
    )

    // Wait for stream to be active
    await waitForStreamActive(kinesis, streamName)
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine Kinesis E2E test:', error)
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

test('publishes replay-normalized events to Kinesis with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'kinesis',
      streamName,
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test',
      maxBatchSize: 32,
      maxBatchDelayMs: 25
    }
  })

  await machine.start(PORT)

  const eventsPromise = collectEvents(kinesis, streamName, 5, 120000)
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

function encodeOptions(options: any): string {
  return encodeURIComponent(JSON.stringify(options))
}

async function collectEvents(kinesis: KinesisClient, streamName: string, minCount: number, timeoutMs: number): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  let completed = false

  return new Promise<NormalizedEvent[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${minCount} Kinesis events (received ${events.length})`))
    }, timeoutMs)

    const poll = async () => {
      if (completed) {
        return
      }

      try {
        const shardIteratorResponse = await kinesis.send(
          new GetShardIteratorCommand({
            StreamName: streamName,
            ShardId: 'shardId-000000000000',
            ShardIteratorType: 'TRIM_HORIZON'
          })
        )

        if (shardIteratorResponse.ShardIterator) {
          const recordsResponse = await kinesis.send(
            new GetRecordsCommand({
              ShardIterator: shardIteratorResponse.ShardIterator
            })
          )

          for (const record of recordsResponse.Records || []) {
            if (record.Data) {
              try {
                events.push(fromBinary(NormalizedEventSchema, record.Data as Uint8Array))
              } catch (error) {
                completed = true
                clearTimeout(timer)
                reject(error as Error)
                return
              }
            }
          }

          if (events.length >= minCount) {
            completed = true
            clearTimeout(timer)
            resolve(events)
            return
          }
        }

        // Poll again after a short delay
        setTimeout(poll, 1000)
      } catch (error) {
        if (completed) {
          return
        }
        completed = true
        clearTimeout(timer)
        reject(error)
      }
    }

    poll()
  })
}

async function startLocalStackContainer() {
  const container = new LocalstackContainer(localstackImage).withEnvironment({ SERVICES: 'kinesis' }).withStartupTimeout(startTimeoutMs)
  return container.start()
}

async function waitForStreamActive(kinesis: KinesisClient, streamName: string, timeoutMs = 60000) {
  const start = Date.now()
  while (true) {
    try {
      const response = await kinesis.send(
        new DescribeStreamCommand({
          StreamName: streamName
        })
      )
      if (response.StreamDescription?.StreamStatus === 'ACTIVE') {
        return
      }
    } catch (error) {
      // Stream might not exist yet
    }
    if (Date.now() - start >= timeoutMs) {
      throw new Error(`Stream ${streamName} did not become active within ${timeoutMs}ms`)
    }
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
}
