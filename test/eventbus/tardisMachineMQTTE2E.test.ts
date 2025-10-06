import { connectAsync as mqttConnect } from 'mqtt'
import { GenericContainer, StartedTestContainer } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { NormalizedEvent, NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'

jest.setTimeout(240000)

const PORT = 8110
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const topic = 'bronze.events.e2e'
const cacheDir = './.cache-mqtt-e2e'
const mqttImage = 'eclipse-mosquitto:2.0'
const startTimeoutMs = 180000

let container: StartedTestContainer
let mqttUrl: string
let shouldSkip = false

async function startMQTTContainer(): Promise<StartedTestContainer> {
  return new GenericContainer(mqttImage).withExposedPorts(1883).withStartupTimeout(startTimeoutMs).start()
}

beforeAll(async () => {
  try {
    container = await startMQTTContainer()
    const host = container.getHost()
    const port = container.getMappedPort(1883)
    mqttUrl = `mqtt://${host}:${port}`
    // Wait for MQTT to be ready
    await new Promise((resolve) => setTimeout(resolve, 5000))
    const testClient = await mqttConnect(mqttUrl)
    await new Promise((resolve, reject) => {
      testClient.on('connect', () => {
        testClient.end()
        resolve(undefined)
      })
      testClient.on('error', reject)
    })
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine MQTT E2E test:', error)
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

test('publishes replay-normalized events to MQTT with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const machine = new TardisMachine({
    cacheDir,
    eventBus: {
      provider: 'mqtt',
      url: mqttUrl,
      topic
    }
  })

  await machine.start(PORT)

  try {
    const receivedMessages: Buffer[] = []

    const client = await mqttConnect(mqttUrl)
    await new Promise<void>((resolve, reject) => {
      client.on('connect', () => {
        client.subscribe(topic, (err) => {
          if (err) reject(err)
          else resolve()
        })
      })
      client.on('error', reject)
    })

    client.on('message', (topic, message) => {
      receivedMessages.push(message)
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
    expect(response.ok).toBe(true)

    // Wait for messages
    await new Promise((resolve) => setTimeout(resolve, 5000))

    expect(receivedMessages.length).toBeGreaterThan(0)

    // Verify message structure
    const firstMessage = receivedMessages[0]
    const event = fromBinary(NormalizedEventSchema, firstMessage)
    expect(event.origin).toBe(Origin.REPLAY)
    expect(event.exchange).toBeDefined()
    expect(event.symbol).toBeDefined()
  } finally {
    await machine.stop()
    await rm(cacheDir, { recursive: true, force: true }).catch(() => undefined)
  }
})

function encodeOptions(options: any): string {
  return encodeURIComponent(JSON.stringify(options))
}
