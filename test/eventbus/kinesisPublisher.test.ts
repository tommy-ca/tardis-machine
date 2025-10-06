import {
  KinesisClient,
  CreateStreamCommand,
  DescribeStreamCommand,
  GetShardIteratorCommand,
  GetRecordsCommand
} from '@aws-sdk/client-kinesis'
import { LocalstackContainer, StartedLocalStackContainer } from '@testcontainers/localstack'
import { fromBinary } from '@bufbuild/protobuf'
import { KinesisEventBus } from '../../src/eventbus/kinesis'
import { Origin, NormalizedEventSchema, NormalizedEvent } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Trade as NormalizedTrade, BookChange as NormalizedBookChange } from 'tardis-dev'

jest.setTimeout(240000)

const baseStream = 'bronze-events-test'
const routingStreams = {
  trade: `${baseStream}-trades`,
  bookChange: `${baseStream}-books`
}
const metaStream = `${baseStream}-meta`
const staticHeadersStream = `${baseStream}-static`
const allStreams = [baseStream, routingStreams.trade, routingStreams.bookChange, metaStream, staticHeadersStream]
const startTimeoutMs = 180000

const baseMeta = {
  source: 'unit-test',
  origin: Origin.REPLAY,
  ingestTimestamp: new Date('2024-01-01T00:00:00.000Z'),
  requestId: 'req-1'
}

describe('KinesisEventBus', () => {
  let container: StartedLocalStackContainer
  let region: string
  let endpoint: string
  let kinesis: KinesisClient

  beforeAll(async () => {
    try {
      container = await startLocalStackContainer()
      region = 'us-east-1'
      endpoint = container.getConnectionUri()
      kinesis = new KinesisClient({
        region,
        endpoint,
        credentials: {
          accessKeyId: 'test',
          secretAccessKey: 'test'
        }
      })

      // Create streams
      for (const stream of allStreams) {
        await kinesis.send(
          new CreateStreamCommand({
            StreamName: stream,
            ShardCount: 1
          })
        )
        // Wait for stream to be active
        await waitForStreamActive(kinesis, stream)
      }
    } catch (error) {
      shouldSkip = true
      console.warn('Skipping KinesisEventBus integration test:', error)
    }
  })

  afterAll(async () => {
    if (container && !shouldSkip) {
      await container.stop()
    }
  })

  test('publishes bronze normalized events to kinesis', async () => {
    if (shouldSkip) {
      return
    }
    const bus = new KinesisEventBus({
      streamName: baseStream,
      region,
      accessKeyId: 'test',
      secretAccessKey: 'test',
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

    let received: NormalizedEvent[] = []
    try {
      received = await consumeEvents(kinesis, baseStream, 4)
    } finally {
      await bus.close().catch(() => undefined)
    }

    expect(received).toHaveLength(4)
    const tradeEvent = received.find((evt) => evt.payload.case === 'trade')
    expect(tradeEvent?.meta.request_id).toBe('req-1')
    const bookChanges = received.filter((evt) => evt.payload.case === 'bookChange')
    expect(bookChanges.length).toBe(3)
    expect(bookChanges[0]?.payload.case).toBe('bookChange')
  })

  test('routes bronze normalized events by payload case', async () => {
    if (shouldSkip) {
      return
    }

    const bus = new KinesisEventBus({
      streamName: metaStream,
      region,
      accessKeyId: 'test',
      secretAccessKey: 'test',
      streamByPayloadCase: {
        trade: routingStreams.trade,
        bookChange: routingStreams.bookChange
      },
      maxBatchSize: 2,
      maxBatchDelayMs: 20
    })

    await bus.start()

    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      id: 't-2',
      price: 2200.75,
      amount: 0.5,
      side: 'sell',
      timestamp: new Date('2024-01-01T00:01:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:01:01.100Z')
    }

    const bookChange: NormalizedBookChange = {
      type: 'book_change',
      symbol: 'ETHUSD',
      exchange: 'bitmex',
      isSnapshot: false,
      bids: [{ price: 2199.5, amount: 1.2 }],
      asks: [
        { price: 2201.25, amount: 1.1 },
        { price: 2201.5, amount: 0 }
      ],
      timestamp: new Date('2024-01-01T00:01:01.500Z'),
      localTimestamp: new Date('2024-01-01T00:01:01.600Z')
    }

    await bus.publish(trade, baseMeta)
    await bus.publish(bookChange, baseMeta)
    await bus.flush()

    const [tradeEvents, bookChangeEvents] = await Promise.all([
      consumeEvents(kinesis, routingStreams.trade, 1),
      consumeEvents(kinesis, routingStreams.bookChange, 3)
    ])

    await bus.close().catch(() => undefined)

    expect(tradeEvents).toHaveLength(1)
    expect(tradeEvents[0]?.payload.case).toBe('trade')
    expect(tradeEvents[0]?.meta.request_id).toBe('req-1')

    expect(bookChangeEvents).toHaveLength(3)
    expect(bookChangeEvents.every((event) => event.payload.case === 'bookChange')).toBe(true)
  })

  test('filters events by allowed payload cases', async () => {
    if (shouldSkip) {
      return
    }

    const filterStream = `${baseStream}-filter-${Date.now()}`
    await kinesis.send(
      new CreateStreamCommand({
        StreamName: filterStream,
        ShardCount: 1
      })
    )
    await waitForStreamActive(kinesis, filterStream)

    const bus = new KinesisEventBus({
      streamName: filterStream,
      region,
      accessKeyId: 'test',
      secretAccessKey: 'test',
      maxBatchSize: 4,
      maxBatchDelayMs: 10,
      includePayloadCases: ['trade']
    })

    await bus.start()

    const trade: NormalizedTrade = {
      type: 'trade',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      id: 't-filter-1',
      price: 31250.25,
      amount: 0.5,
      side: 'buy',
      timestamp: new Date('2024-01-01T00:03:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:03:01.050Z')
    }

    const bookChange: NormalizedBookChange = {
      type: 'book_change',
      symbol: 'BTCUSD',
      exchange: 'bitmex',
      isSnapshot: false,
      bids: [{ price: 31249.5, amount: 1.2 }],
      asks: [{ price: 31250.5, amount: 1.1 }],
      timestamp: new Date('2024-01-01T00:03:01.250Z'),
      localTimestamp: new Date('2024-01-01T00:03:01.300Z')
    }

    try {
      await bus.publish(trade, baseMeta)
      await bus.publish(bookChange, baseMeta)
      await bus.flush()
    } finally {
      await bus.close().catch(() => undefined)
    }

    const events = await consumeEvents(kinesis, filterStream, 1, 60000)
    expect(events).toHaveLength(1)
    expect(events[0]?.payload.case).toBe('trade')
  })
})

let shouldSkip = false

async function startLocalStackContainer() {
  const container = new LocalstackContainer('localstack/localstack:3.0').withStartupTimeout(startTimeoutMs)
  return container.start()
}

async function consumeEvents(
  kinesis: KinesisClient,
  streamName: string,
  expectedCount: number,
  timeoutMs = 120000
): Promise<NormalizedEvent[]> {
  const records = await collectKinesisRecords(kinesis, streamName, expectedCount, timeoutMs)
  return records.map((record) => record.event)
}

async function collectKinesisRecords(
  kinesis: KinesisClient,
  streamName: string,
  expectedCount: number,
  timeoutMs: number
): Promise<KinesisRecord[]> {
  const records: KinesisRecord[] = []
  let completed = false

  return new Promise<KinesisRecord[]>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (completed) {
        return
      }
      completed = true
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for ${expectedCount} Kinesis events (received ${records.length})`))
    }, timeoutMs)

    const collect = async () => {
      try {
        const response = await kinesis.send(new DescribeStreamCommand({ StreamName: streamName }))
        const shardId = response.StreamDescription?.Shards?.[0]?.ShardId
        if (!shardId) {
          throw new Error('No shard found')
        }

        const iteratorResponse = await kinesis.send(
          new GetShardIteratorCommand({
            StreamName: streamName,
            ShardId: shardId,
            ShardIteratorType: 'TRIM_HORIZON'
          })
        )

        let shardIterator = iteratorResponse.ShardIterator

        while (shardIterator && !completed) {
          const recordsResponse = await kinesis.send(new GetRecordsCommand({ ShardIterator: shardIterator }))
          shardIterator = recordsResponse.NextShardIterator

          for (const record of recordsResponse.Records || []) {
            if (!record.Data) continue
            try {
              const event = fromBinary(NormalizedEventSchema, record.Data as Uint8Array)
              records.push({
                event,
                partitionKey: record.PartitionKey || ''
              })
            } catch (error) {
              completed = true
              clearTimeout(timer)
              reject(error as Error)
              return
            }

            if (records.length >= expectedCount) {
              completed = true
              clearTimeout(timer)
              resolve(records)
              return
            }
          }

          if (!recordsResponse.Records || recordsResponse.Records.length === 0) {
            await new Promise((resolve) => setTimeout(resolve, 1000))
          }
        }
      } catch (error) {
        if (completed) return
        completed = true
        clearTimeout(timer)
        reject(error)
      }
    }

    collect()
  })
}

type KinesisRecord = {
  event: NormalizedEvent
  partitionKey: string
}

async function waitForStreamActive(kinesis: KinesisClient, streamName: string, timeoutMs = 60000) {
  const start = Date.now()
  while (true) {
    try {
      const response = await kinesis.send(new DescribeStreamCommand({ StreamName: streamName }))
      if (response.StreamDescription?.StreamStatus === 'ACTIVE') {
        return
      }
    } catch (error) {
      // Stream might not exist yet
    }
    if (Date.now() - start >= timeoutMs) {
      throw new Error(`Stream ${streamName} did not become active within ${timeoutMs}ms`)
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
}
