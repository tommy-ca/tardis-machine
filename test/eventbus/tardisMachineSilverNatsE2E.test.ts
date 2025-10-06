import fetch from 'node-fetch'
import { connect, type NatsConnection } from 'nats'
import { NatsContainer, StartedNatsContainer } from '@testcontainers/nats'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { TradeRecordSchema, Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'

jest.setTimeout(240000)

const PORT = 8097
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const subject = 'silver.events.e2e'
const cacheDir = './.cache-silver-nats-e2e'
const natsImage = 'nats:2.10'
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
    console.warn('Skipping tardis-machine Silver NATS E2E test:', error)
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

test('publishes replay-normalized events to Silver NATS with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const server = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY,
    cacheDir,
    eventBus: undefined, // Bronze disabled
    silverEventBus: {
      provider: 'nats-silver',
      servers: [natsUrl],
      subject
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

    const connection = await connect({ servers: [natsUrl] })
    const messages: any[] = []
    let messageCount = 0
    const maxMessages = 10

    const subscription = connection.subscribe(subject, {
      callback: (err, msg) => {
        if (err) {
          return
        }
        messages.push(msg)
        messageCount++

        if (messageCount >= maxMessages) {
          subscription.unsubscribe()
        }
      }
    })

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        subscription.unsubscribe()
        connection.close()
        reject(new Error('Timeout waiting for messages'))
      }, 30000)

      const checkMessages = () => {
        if (messageCount >= maxMessages) {
          clearTimeout(timeout)
          connection.close()
          resolve()
        } else {
          setTimeout(checkMessages, 100)
        }
      }
      checkMessages()
    })

    expect(messages.length).toBeGreaterThan(0)

    // Verify at least one trade record
    const tradeMessages = messages.filter((msg) => {
      try {
        const decoded = fromBinary(TradeRecordSchema, msg.data)
        return decoded.tradeId !== ''
      } catch {
        return false
      }
    })

    expect(tradeMessages.length).toBeGreaterThan(0)

    const sampleMessage = tradeMessages[0]
    const decoded = fromBinary(TradeRecordSchema, sampleMessage.data)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.origin).toBe(Origin.REPLAY)
    expect(typeof decoded.priceE8).toBe('bigint')
    expect(typeof decoded.qtyE8).toBe('bigint')

    // Verify headers
    const headers = sampleMessage.headers
    expect(headers?.get('recordType')).toBe('trade')
    expect(headers?.get('dataType')).toBe('trade')
  } finally {
    await server.stop()
  }
})

async function startNatsContainer(): Promise<StartedNatsContainer> {
  const container = await new NatsContainer(natsImage).withExposedPorts(4222).start()
  return container
}
