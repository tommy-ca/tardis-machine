import * as amqp from 'amqplib'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverActiveMQEventBusConfig, NormalizedMessage, PublishMeta } from './types'
import { BaseSilverEventBusPublisher, CommonSilverConfig } from './baseSilverPublisher'
import { debug } from '../debug'

const log = debug.extend('eventbus:silver-activemq')

export class SilverActiveMQEventBus extends BaseSilverEventBusPublisher {
  private readonly encoder: SilverNormalizedEventEncoder
  private connection?: any
  private channel?: any
  private readonly staticHeaders?: Record<string, string>

  constructor(private readonly config: SilverActiveMQEventBusConfig) {
    const commonConfig: CommonSilverConfig = {
      maxBatchSize: 1, // Since AMQP is synchronous, batch size 1
      maxBatchDelayMs: 0,
      includeRecordTypes: config.includeRecordTypes
    }
    super(commonConfig)
    const keyBuilder = config.routingKeyTemplate ? compileSilverKeyBuilder(config.routingKeyTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
    this.staticHeaders = config.staticHeaders
  }

  async start(): Promise<void> {
    this.connection = await amqp.connect(this.config.url)
    this.channel = await this.connection.createChannel()
    if (this.config.destinationType === 'topic') {
      await this.channel.assertExchange(this.config.destination, 'topic', { durable: true })
    } else {
      await this.channel.assertQueue(this.config.destination, { durable: true })
    }
  }

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): SilverEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: SilverEvent[]): Promise<void> {
    if (!this.channel) {
      throw new Error('ActiveMQ channel not initialized')
    }

    for (const event of batch) {
      const routingKey = event.key || 'default'
      const headers = this.buildHeaders(event)
      if (this.config.destinationType === 'topic') {
        this.channel.publish(this.config.destination, routingKey, Buffer.from(event.binary), { headers })
      } else {
        this.channel.sendToQueue(this.config.destination, Buffer.from(event.binary), { headers })
      }
    }
  }

  protected async doClose(): Promise<void> {
    if (this.channel) {
      await this.channel.close()
    }
    if (this.connection) {
      await this.connection.close()
    }
  }

  private buildHeaders(event: SilverEvent): Record<string, any> {
    const headers: Record<string, any> = {
      recordType: event.recordType,
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
