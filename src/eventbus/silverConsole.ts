import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, NormalizedMessage, PublishMeta } from './types'
import { BaseSilverEventBusPublisher, CommonSilverConfig } from './baseSilverPublisher'
import { debug } from '../debug'

const log = debug.extend('eventbus')

export type SilverConsoleEventBusConfig = {
  /** Optional prefix for console output */
  prefix?: string
  /** Optional allow-list of record types to publish */
  includeRecordTypes?: import('./types').SilverRecordType[]
  /** Template for constructing keys (for display) */
  keyTemplate?: string
}

export class SilverConsoleEventBus extends BaseSilverEventBusPublisher {
  private readonly encoder: SilverNormalizedEventEncoder
  private readonly prefix: string

  constructor(private readonly config: SilverConsoleEventBusConfig) {
    const commonConfig: CommonSilverConfig = {
      maxBatchSize: 1, // Immediate output
      maxBatchDelayMs: 0,
      includeRecordTypes: config.includeRecordTypes
    }
    super(commonConfig)
    const keyBuilder = config.keyTemplate ? compileSilverKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
    this.prefix = config.prefix ?? '[SILVER_EVENTBUS]'
  }

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): SilverEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: SilverEvent[]): Promise<void> {
    for (const event of batch) {
      console.log(
        `${this.prefix} Key: ${event.key}, RecordType: ${event.recordType}, DataType: ${event.dataType}, BinaryLength: ${event.binary.length}`
      )
    }
  }

  protected async doClose(): Promise<void> {
    // No-op
  }
}
