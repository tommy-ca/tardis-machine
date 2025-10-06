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
import { TradeRecordSchema, Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'

jest.setTimeout(240000)

const PORT = 8096
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const streamName = 'silver-events-e2e'
const cacheDir = './.cache-silver-kinesis-e2e'
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
    console.warn('Skipping tardis-machine Silver Kinesis E2E test:', error)
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

test('publishes replay-normalized events to Silver Kinesis with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const server = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY,
    cacheDir,
    eventBus: undefined, // Bronze disabled
    silverEventBus: {
      provider: 'kinesis-silver',
      streamName,
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test'
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

    // Get records from Kinesis
    const describeResponse = await kinesis.send(
      new DescribeStreamCommand({
        StreamName: streamName
      })
    )

    const shardId = describeResponse.StreamDescription?.Shards?.[0]?.ShardId
    expect(shardId).toBeDefined()

    const iteratorResponse = await kinesis.send(
      new GetShardIteratorCommand({
        StreamName: streamName,
        ShardId: shardId!,
        ShardIteratorType: 'TRIM_HORIZON'
      })
    )

    const recordsResponse = await kinesis.send(
      new GetRecordsCommand({
        ShardIterator: iteratorResponse.ShardIterator!
      })
    )

    expect(recordsResponse.Records?.length).toBeGreaterThan(0)

    // Verify at least one trade record
    const tradeRecords = recordsResponse.Records!.filter((record) => {
      try {
        const decoded = fromBinary(TradeRecordSchema, record.Data!)
        return decoded.tradeId !== ''
      } catch {
        return false
      }
    })

    expect(tradeRecords.length).toBeGreaterThan(0)

    const sampleRecord = tradeRecords[0]
    const decoded = fromBinary(TradeRecordSchema, sampleRecord.Data!)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.origin).toBe(Origin.REPLAY)
    expect(typeof decoded.priceE8).toBe('bigint')
    expect(typeof decoded.qtyE8).toBe('bigint')
  } finally {
    await server.stop()
  }
})

async function startLocalStackContainer(): Promise<StartedLocalStackContainer> {
  const container = await new LocalstackContainer(localstackImage)
    .withExposedPorts(4566)
    .withEnvironment({
      SERVICES: 'kinesis',
      DEBUG: '1',
      DOCKER_HOST: 'unix:///var/run/docker.sock'
    })
    .start()
  return container
}

async function waitForStreamActive(kinesis: KinesisClient, streamName: string): Promise<void> {
  for (let i = 0; i < 30; i++) {
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
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error('Stream did not become active within timeout')
}
