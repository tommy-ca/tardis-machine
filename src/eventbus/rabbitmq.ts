import * as amqp from 'amqplib'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, RabbitMQEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { debug } from '../debug'

const log = debug.extend('eventbus:rabbitmq')

export class RabbitMQEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private connection?: any
  private channel?: any
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>
  private readonly staticHeaders?: Record<string, string>

  constructor(private readonly config: RabbitMQEventBusConfig) {
    const keyBuilder = config.routingKeyTemplate ? compileKeyBuilder(config.routingKeyTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
    }
    this.staticHeaders = config.staticHeaders
  }

  async start(): Promise<void> {
    this.connection = await amqp.connect(this.config.url)
    this.channel = await this.connection.createChannel()
    await this.channel.assertExchange(this.config.exchange, this.config.exchangeType ?? 'direct', { durable: true })
  }

  async publish(message: NormalizedMessage, meta: PublishMeta): Promise<void> {
    if (!this.channel) {
      throw new Error('RabbitMQ channel not initialized')
    }

    const events = this.filterEvents(this.encoder.encode(message, meta))
    for (const event of events) {
      const routingKey = event.key || 'default'
      const headers = this.buildHeaders(event)
      this.channel.publish(this.config.exchange, routingKey, Buffer.from(event.binary), { headers })
    }
  }

  async flush(): Promise<void> {
    // RabbitMQ publishes are synchronous, no buffering needed
  }

  async close(): Promise<void> {
    if (this.channel) {
      await this.channel.close()
    }
    if (this.connection) {
      await this.connection.close()
    }
  }

  private filterEvents(events: BronzeEvent[]): BronzeEvent[] {
    if (!this.allowedPayloadCases) {
      return events
    }
    return events.filter((event) => this.allowedPayloadCases!.has(event.payloadCase))
  }

  private buildHeaders(event: BronzeEvent): Record<string, any> {
    const headers: Record<string, any> = {
      payloadCase: event.payloadCase,
      dataType: event.dataType
    }

    if (this.staticHeaders) {
      Object.assign(headers, this.staticHeaders)
    }

    // Add meta as headers
    for (const [key, value] of Object.entries(event.meta)) {
      headers[`meta.${key}`] = value
    }

    return headers
  }
}
