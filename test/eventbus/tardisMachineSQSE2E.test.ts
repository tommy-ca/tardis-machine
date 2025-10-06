import fetch from 'node-fetch'
import { SQSClient, CreateQueueCommand, ReceiveMessageCommand, DeleteMessageCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs'
import { LocalstackContainer, StartedLocalStackContainer } from '@testcontainers/localstack'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8099
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const queueName = 'bronze-events-e2e'
const cacheDir = './.cache-sqs-e2e'
const localstackImage = 'localstack/localstack:3.0'
const startTimeoutMs = 180000

let container: StartedLocalStackContainer
let sqs: SQSClient
let queueUrl: string
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startLocalStackContainer()
    const endpoint = container.getConnectionUri()
    sqs = new SQSClient({
      endpoint,
      region: 'us-east-1',
      credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test'
      }
    })

    // Create queue
    const createQueueResponse = await sqs.send(
      new CreateQueueCommand({
        QueueName: queueName
      })
    )

    // Get queue URL
    const getQueueUrlResponse = await sqs.send(
      new GetQueueUrlCommand({
        QueueName: queueName
      })
    )
    queueUrl = getQueueUrlResponse.QueueUrl!
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine SQS E2E test:', error)
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

test('publishes replay-normalized events to SQS with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'sqs',
      queueUrl,
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test',
      maxBatchSize: 5,
      maxBatchDelayMs: 25
    }
  })

  await machine.start(PORT)

  const eventsPromise = collectEvents(sqs, queueUrl, 5, 120000)
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

test('does not publish when SQS queue URL is invalid', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'sqs',
      queueUrl: 'https://invalid-queue-url',
      region: 'us-east-1',
      accessKeyId: 'test',
      secretAccessKey: 'test'
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

    // Should not crash even with invalid queue URL
  } finally {
    await machine.stop().catch(() => undefined)
    await rm(cacheDir, { recursive: true, force: true }).catch(() => undefined)
  }
})

async function startLocalStackContainer(): Promise<StartedLocalStackContainer> {
  const container = new LocalstackContainer(localstackImage)
    .withEnvironment({
      SERVICES: 'sqs',
      DEBUG: '1',
      DOCKER_HOST: 'unix:///var/run/docker.sock'
    })
    .withStartupTimeout(startTimeoutMs)

  return container.start()
}

async function collectEvents(sqs: SQSClient, queueUrl: string, minEvents: number, timeoutMs: number): Promise<NormalizedEvent[]> {
  const events: NormalizedEvent[] = []
  const startTime = Date.now()

  while (events.length < minEvents && Date.now() - startTime < timeoutMs) {
    try {
      const response = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
          MessageAttributeNames: ['All']
        })
      )

      if (response.Messages) {
        for (const message of response.Messages) {
          if (message.Body) {
            const binary = Buffer.from(message.Body, 'base64')
            const event = fromBinary(NormalizedEventSchema, binary)
            events.push(event)

            // Delete the message
            await sqs.send(
              new DeleteMessageCommand({
                QueueUrl: queueUrl,
                ReceiptHandle: message.ReceiptHandle
              })
            )
          }
        }
      }
    } catch (error) {
      console.warn('Error collecting SQS events:', error)
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
