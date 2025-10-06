import * as amqp from 'amqplib'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, ActiveMQEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { debug } from '../debug'

const log = debug.extend('eventbus:activemq')

export class ActiveMQEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private connection?: any
  private channel?: any
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>
  private readonly staticHeaders?: Record<string, string>

  constructor(private readonly config: ActiveMQEventBusConfig) {
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
    // For ActiveMQ, we can use a queue or topic
    if (this.config.destinationType === 'topic') {
      // For topics, we might need to handle differently, but for simplicity, use exchange
      await this.channel.assertExchange(this.config.destination, 'topic', { durable: true })
    } else {
      // For queues, assert queue
      await this.channel.assertQueue(this.config.destination, { durable: true })
    }
  }

  async publish(message: NormalizedMessage, meta: PublishMeta): Promise<void> {
    if (!this.channel) {
      throw new Error('ActiveMQ channel not initialized')
    }

    const events = this.filterEvents(this.encoder.encode(message, meta))
    for (const event of events) {
      const routingKey = event.key || 'default'
      const headers = this.buildHeaders(event)
      if (this.config.destinationType === 'topic') {
        this.channel.publish(this.config.destination, routingKey, Buffer.from(event.binary), { headers })
      } else {
        this.channel.sendToQueue(this.config.destination, Buffer.from(event.binary), { headers })
      }
    }
  }

  async flush(): Promise<void> {
    // AMQP publishes are synchronous, no buffering needed
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
