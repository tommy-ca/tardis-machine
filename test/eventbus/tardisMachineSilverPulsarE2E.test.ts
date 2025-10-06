import fetch from 'node-fetch'
import { Client } from 'pulsar-client'
import { GenericContainer, StartedTestContainer } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { TradeRecordSchema, Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'

jest.setTimeout(240000)

const PORT = 8101
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const topic = 'persistent://public/default/silver-records-e2e'
const cacheDir = './.cache-silver-pulsar-e2e'
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
    console.warn('Skipping tardis-machine Silver Pulsar E2E test:', error)
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

test('publishes replay-normalized events to Silver Pulsar with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const server = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY,
    cacheDir,
    eventBus: undefined, // Bronze disabled
    silverEventBus: {
      provider: 'pulsar-silver',
      serviceUrl: pulsarUrl,
      topic,
      maxBatchSize: 5,
      maxBatchDelayMs: 25
    }
  })

  await server.start(PORT)

  const client = new Client({ serviceUrl: pulsarUrl })
  const consumer = await client.subscribe({
    topic,
    subscription: 'test-subscription',
    subscriptionType: 'Exclusive'
  })

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

    const messages: any[] = []
    let messageCount = 0
    const maxMessages = 10

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        consumer
          .close()
          .then(() => client.close())
          .then(() => reject(new Error('Timeout waiting for messages')))
      }, 30000)

      const receiveMessages = async () => {
        try {
          const message = await consumer.receive(1000)
          if (message) {
            messages.push(message)
            messageCount++
            consumer.acknowledge(message)

            if (messageCount >= maxMessages) {
              clearTimeout(timeout)
              await consumer.close()
              await client.close()
              resolve()
            } else {
              receiveMessages()
            }
          } else {
            receiveMessages()
          }
        } catch (error) {
          // Continue receiving
          receiveMessages()
        }
      }

      receiveMessages()
    })

    expect(messages.length).toBeGreaterThan(0)

    // Verify at least one trade record
    const tradeMessages = messages.filter((m) => {
      try {
        const decoded = fromBinary(TradeRecordSchema, m.getData())
        return decoded.tradeId !== ''
      } catch {
        return false
      }
    })

    expect(tradeMessages.length).toBeGreaterThan(0)

    const sampleMessage = tradeMessages[0]
    const decoded = fromBinary(TradeRecordSchema, sampleMessage.getData())
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.origin).toBe(Origin.REPLAY)
    expect(typeof decoded.priceE8).toBe('bigint')
    expect(typeof decoded.qtyE8).toBe('bigint')

    // Verify properties
    const properties = sampleMessage.getProperties()
    expect(properties.recordType).toBe('trade')
    expect(properties.dataType).toBe('trade')
  } finally {
    await server.stop()
    await consumer.close().catch(() => undefined)
    await client.close().catch(() => undefined)
  }
})

async function startPulsarContainer(): Promise<StartedTestContainer> {
  const container = new GenericContainer(pulsarImage).withExposedPorts(6650).withStartupTimeout(startTimeoutMs)

  return container.start()
}
