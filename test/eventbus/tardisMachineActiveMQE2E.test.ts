import { connect } from 'rhea'
import { GenericContainer, StartedTestContainer } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8111
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const destination = 'bronze.events.e2e'
const cacheDir = './.cache-activemq-e2e'
const activemqImage = 'apache/activemq-artemis:latest-alpine'
const startTimeoutMs = 180000

let container: StartedTestContainer
let amqpUrl: string
let shouldSkip = false

async function startActiveMQContainer(): Promise<StartedTestContainer> {
  return new GenericContainer(activemqImage)
    .withExposedPorts(5672)
    .withStartupTimeout(startTimeoutMs)
    .withEnvironment({ ARTEMIS_USERNAME: 'admin', ARTEMIS_PASSWORD: 'admin' })
    .start()
}

beforeAll(async () => {
  try {
    container = await startActiveMQContainer()
    const host = container.getHost()
    const port = container.getMappedPort(5672)
    amqpUrl = `amqp://${host}:${port}`
    // Wait for ActiveMQ to be ready
    await new Promise((resolve) => setTimeout(resolve, 10000))
    const testConnection = connect({
      host,
      port,
      username: 'admin',
      password: 'admin'
    })
    await new Promise((resolve, reject) => {
      testConnection.on('connection_open', () => {
        testConnection.close()
        resolve(undefined)
      })
      testConnection.on('connection_error', reject)
    })
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine ActiveMQ E2E test:', error)
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

test('publishes replay-normalized events to ActiveMQ with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'activemq',
      url: amqpUrl,
      destination,
      destinationType: 'queue'
    }
  })

  await machine.start(PORT)

  try {
    const receivedMessages: Buffer[] = []

    const connection = connect({
      host: container.getHost(),
      port: container.getMappedPort(5672),
      username: 'admin',
      password: 'admin'
    })

    await new Promise<void>((resolve, reject) => {
      connection.on('connection_open', () => {
        const receiver = connection.open_receiver(destination)
        receiver.on('message', (context) => {
          if (context.message.body && Buffer.isBuffer(context.message.body)) {
            receivedMessages.push(context.message.body)
          }
        })
        resolve()
      })
      connection.on('connection_error', reject)
    })

    // Trigger replay
    const options = {
      exchange: 'bitmex',
      symbols: ['ETHUSD'],
      from: '2019-06-01',
      to: '2019-06-01 00:01',
      dataTypes: ['trade']
    }

    const params = new URLSearchParams({ options: JSON.stringify(options) })
    const response = await fetch(`${HTTP_REPLAY_NORMALIZED_URL}?${params}`)

    expect(response.status).toBe(200)

    // Wait for messages to be received
    await new Promise((resolve) => setTimeout(resolve, 5000))

    expect(receivedMessages.length).toBeGreaterThan(0)

    // Verify the message is a valid NormalizedEvent
    const event = fromBinary(NormalizedEventSchema, receivedMessages[0])
    expect(event).toBeDefined()
    expect(event.origin).toBe(Origin.REPLAY)
    expect(event.exchange).toBeDefined()
    expect(event.symbol).toBeDefined()

    connection.close()
  } finally {
    await machine.stop()
    await rm(cacheDir, { recursive: true, force: true })
  }
})
