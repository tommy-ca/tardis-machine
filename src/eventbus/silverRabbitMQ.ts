import * as amqp from 'amqplib'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverRabbitMQEventBusConfig, SilverEventSink, NormalizedMessage, PublishMeta } from './types'
import { debug } from '../debug'

const log = debug.extend('eventbus:silver-rabbitmq')

export class SilverRabbitMQEventBus implements SilverEventSink {
  private readonly encoder: SilverNormalizedEventEncoder
  private connection?: any
  private channel?: any
  private readonly allowedRecordTypes?: Set<SilverRecordType>
  private readonly staticHeaders?: Record<string, string>

  constructor(private readonly config: SilverRabbitMQEventBusConfig) {
    const keyBuilder = config.routingKeyTemplate ? compileSilverKeyBuilder(config.routingKeyTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
    if (config.includeRecordTypes) {
      this.allowedRecordTypes = new Set(config.includeRecordTypes)
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

  private filterEvents(events: SilverEvent[]): SilverEvent[] {
    if (!this.allowedRecordTypes) {
      return events
    }
    return events.filter((event) => this.allowedRecordTypes!.has(event.recordType))
  }

  private buildHeaders(event: SilverEvent): Record<string, any> {
    const headers: Record<string, any> = {
      recordType: event.recordType,
      dataType: event.dataType
    }

    if (this.staticHeaders) {
      Object.assign(headers, this.staticHeaders)
    }

    // Silver records don't have meta like Bronze
    return headers
  }
}
