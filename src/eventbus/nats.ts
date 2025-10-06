import { connect, type NatsConnection, headers } from 'nats'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, NatsEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { debug } from '../debug'

const log = debug.extend('eventbus:nats')

export class NatsEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private connection?: NatsConnection
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>
  private readonly staticHeaders?: Record<string, string>

  constructor(private readonly config: NatsEventBusConfig) {
    const keyBuilder = config.subjectTemplate ? compileKeyBuilder(config.subjectTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
    }
    this.staticHeaders = config.staticHeaders
  }

  private resolveSubject(payloadCase: BronzePayloadCase): string {
    const { subjectByPayloadCase, subject } = this.config
    return subjectByPayloadCase?.[payloadCase] ?? subject
  }

  async start(): Promise<void> {
    this.connection = await connect({ servers: this.config.servers, user: this.config.user, pass: this.config.pass })
  }

  async publish(message: NormalizedMessage, meta: PublishMeta): Promise<void> {
    if (!this.connection) {
      throw new Error('NATS connection not initialized')
    }

    const events = this.filterEvents(this.encoder.encode(message, meta))
    for (const event of events) {
      const subject = event.key || this.resolveSubject(event.payloadCase)
      const headers = this.buildHeaders(event)
      this.connection.publish(subject, event.binary, { headers })
    }
  }

  async flush(): Promise<void> {
    if (this.connection) {
      await this.connection.flush()
    }
  }

  async close(): Promise<void> {
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

  private buildHeaders(event: BronzeEvent) {
    const hdrs = headers()
    hdrs.set('payloadCase', event.payloadCase)
    hdrs.set('dataType', event.dataType)

    for (const [key, value] of Object.entries(event.meta)) {
      hdrs.set(key, value)
    }

    if (this.staticHeaders) {
      for (const [key, value] of Object.entries(this.staticHeaders)) {
        hdrs.set(key, value)
      }
    }

    return hdrs
  }
}
