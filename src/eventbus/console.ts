import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, NormalizedMessage, PublishMeta } from './types'
import { BaseBronzeEventBusPublisher, CommonBronzeConfig } from './baseBronzePublisher'
import { debug } from '../debug'

const log = debug.extend('eventbus')

export type ConsoleEventBusConfig = {
  /** Optional prefix for console output */
  prefix?: string
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: import('./types').BronzePayloadCase[]
  /** Template for constructing keys (for display) */
  keyTemplate?: string
}

export class ConsoleEventBus extends BaseBronzeEventBusPublisher {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly prefix: string

  constructor(private readonly config: ConsoleEventBusConfig) {
    const commonConfig: CommonBronzeConfig = {
      maxBatchSize: 1, // Immediate output
      maxBatchDelayMs: 0,
      includePayloadCases: config.includePayloadCases
    }
    super(commonConfig)
    const keyBuilder = config.keyTemplate ? compileKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    this.prefix = config.prefix ?? '[EVENTBUS]'
  }

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: BronzeEvent[]): Promise<void> {
    for (const event of batch) {
      console.log(
        `${this.prefix} Key: ${event.key}, PayloadCase: ${event.payloadCase}, DataType: ${event.dataType}, BinaryLength: ${event.binary.length}`
      )
    }
  }

  protected async doClose(): Promise<void> {
    // No-op
  }
}
