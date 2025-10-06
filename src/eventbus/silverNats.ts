import { connect, type NatsConnection, headers } from 'nats'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverNatsEventBusConfig, SilverEventSink, NormalizedMessage, PublishMeta } from './types'
import { debug } from '../debug'

const log = debug.extend('eventbus:silver-nats')

export class SilverNatsEventBus implements SilverEventSink {
  private readonly encoder: SilverNormalizedEventEncoder
  private connection?: NatsConnection
  private readonly allowedRecordTypes?: Set<SilverRecordType>
  private readonly staticHeaders?: Record<string, string>

  constructor(private readonly config: SilverNatsEventBusConfig) {
    const keyBuilder = config.subjectTemplate ? compileSilverKeyBuilder(config.subjectTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
    if (config.includeRecordTypes) {
      this.allowedRecordTypes = new Set(config.includeRecordTypes)
    }
    this.staticHeaders = config.staticHeaders
  }

  private resolveSubject(recordType: SilverRecordType): string {
    const { subjectByRecordType, subject } = this.config
    return subjectByRecordType?.[recordType] ?? subject
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
      const subject = event.key || this.resolveSubject(event.recordType)
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

  private filterEvents(events: SilverEvent[]): SilverEvent[] {
    if (!this.allowedRecordTypes) {
      return events
    }
    return events.filter((event) => this.allowedRecordTypes!.has(event.recordType))
  }

  private buildHeaders(event: SilverEvent) {
    const hdrs = headers()
    hdrs.set('recordType', event.recordType)
    hdrs.set('dataType', event.dataType)

    // Silver records don't have meta like Bronze

    if (this.staticHeaders) {
      for (const [key, value] of Object.entries(this.staticHeaders)) {
        hdrs.set(key, value)
      }
    }

    return hdrs
  }
}
