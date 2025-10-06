import fetch from 'node-fetch'
import * as amqp from 'amqplib'
import { RabbitMQContainer, StartedRabbitMQContainer } from '@testcontainers/rabbitmq'
import { fromBinary } from '@bufbuild/protobuf'
import { rm } from 'fs/promises'
import { TardisMachine } from '../../src'
import { TradeRecordSchema, Origin } from '../../src/generated/lakehouse/silver/v1/records_pb'

jest.setTimeout(240000)

const PORT = 8104
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const exchange = 'silver.events.e2e'
const cacheDir = './.cache-silver-rabbitmq-e2e'
const rabbitmqImage = 'rabbitmq:3-management-alpine'
const startTimeoutMs = 180000

let container: StartedRabbitMQContainer
let amqpUrl: string
let shouldSkip = false

beforeAll(async () => {
  try {
    container = await startRabbitMQContainer()
    amqpUrl = container.getAmqpUrl()
  } catch (error) {
    shouldSkip = true
    console.warn('Skipping tardis-machine Silver RabbitMQ E2E test:', error)
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

test('publishes replay-normalized events to Silver RabbitMQ with Buf payloads', async () => {
  if (shouldSkip) {
    return
  }

  const server = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY,
    cacheDir,
    eventBus: undefined, // Bronze disabled
    silverEventBus: {
      provider: 'rabbitmq-silver',
      url: amqpUrl,
      exchange
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

    const connection = await amqp.connect(amqpUrl)
    const channel = await connection.createChannel()
    const queue = await channel.assertQueue('', { exclusive: true })
    await channel.bindQueue(queue.queue, exchange, '#')

    const messages: any[] = []
    let messageCount = 0
    const maxMessages = 10

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        connection.close().then(() => reject(new Error('Timeout waiting for messages')))
      }, 30000)

      channel.consume(
        queue.queue,
        (msg) => {
          if (msg) {
            messages.push(msg)
            messageCount++

            if (messageCount >= maxMessages) {
              clearTimeout(timeout)
              connection.close()
              resolve()
            }
          }
        },
        { noAck: true }
      )
    })

    expect(messages.length).toBeGreaterThan(0)

    // Verify at least one trade record
    const tradeMessages = messages.filter((m) => {
      try {
        const decoded = fromBinary(TradeRecordSchema, m.content)
        return decoded.tradeId !== ''
      } catch {
        return false
      }
    })

    expect(tradeMessages.length).toBeGreaterThan(0)

    const sampleMessage = tradeMessages[0]
    const decoded = fromBinary(TradeRecordSchema, sampleMessage.content)
    expect(decoded.exchange).toBe('binance')
    expect(decoded.symbol).toBe('btcusdt')
    expect(decoded.origin).toBe(Origin.REPLAY)
    expect(typeof decoded.priceE8).toBe('bigint')
    expect(typeof decoded.qtyE8).toBe('bigint')

    // Verify headers
    const headers = sampleMessage.properties.headers
    expect(headers.recordType).toBe('trade')
    expect(headers.dataType).toBe('trade')
  } finally {
    await server.stop()
  }
})

async function startRabbitMQContainer(): Promise<StartedRabbitMQContainer> {
  const container = await new RabbitMQContainer(rabbitmqImage).withExposedPorts(5672).start()
  return container
}
