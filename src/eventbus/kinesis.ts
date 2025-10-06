import { KinesisClient, PutRecordsCommand, type PutRecordsResultEntry } from '@aws-sdk/client-kinesis'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, KinesisEventBusConfig, NormalizedMessage, PublishMeta } from './types'
import { BaseBronzeEventBusPublisher, CommonBronzeConfig } from './baseBronzePublisher'
import { debug } from '../debug'

const log = debug.extend('eventbus:kinesis')

export class KinesisEventBus extends BaseBronzeEventBusPublisher {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly kinesis: KinesisClient
  private readonly streamName: string
  private readonly streamByPayloadCase?: Record<string, string>

  constructor(private readonly config: KinesisEventBusConfig) {
    const commonConfig: CommonBronzeConfig = {
      maxBatchSize: config.maxBatchSize ?? 500, // Kinesis max is 500
      maxBatchDelayMs: config.maxBatchDelayMs,
      includePayloadCases: config.includePayloadCases
    }
    super(commonConfig)
    const partitionKeyBuilder = config.partitionKeyTemplate ? compileKeyBuilder(config.partitionKeyTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(partitionKeyBuilder)
    this.kinesis = new KinesisClient({
      region: config.region,
      credentials:
        config.accessKeyId && config.secretAccessKey
          ? {
              accessKeyId: config.accessKeyId,
              secretAccessKey: config.secretAccessKey,
              sessionToken: config.sessionToken
            }
          : undefined
    })
    this.streamName = config.streamName
    this.streamByPayloadCase = config.streamByPayloadCase
  }

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: BronzeEvent[]): Promise<void> {
    const groups = this.groupByStream(batch)
    for (const [streamName, events] of groups) {
      await this.putRecords(streamName, events)
    }
  }

  protected async doClose(): Promise<void> {
    this.kinesis.destroy()
  }

  private async putRecords(streamName: string, events: BronzeEvent[]): Promise<void> {
    const records = events.map((event) => ({
      Data: Buffer.from(event.binary),
      PartitionKey: event.key
    }))

    const command = new PutRecordsCommand({
      StreamName: streamName,
      Records: records
    })

    const response = await this.kinesis.send(command)

    if (response.FailedRecordCount && response.FailedRecordCount > 0) {
      const failedRecords = response.Records?.filter((record: PutRecordsResultEntry) => record.ErrorCode) || []
      log('Kinesis put records failed for %d records: %o', failedRecords.length, failedRecords)
      throw new Error(`Kinesis put records failed for ${failedRecords.length} records`)
    }
  }

  private groupByStream(events: BronzeEvent[]): Map<string, BronzeEvent[]> {
    const groups = new Map<string, BronzeEvent[]>()
    for (const event of events) {
      const streamName = this.resolveStream(event.payloadCase)
      const bucket = groups.get(streamName)
      if (bucket) {
        bucket.push(event)
      } else {
        groups.set(streamName, [event])
      }
    }
    return groups
  }

  private resolveStream(payloadCase: BronzePayloadCase): string {
    return this.streamByPayloadCase?.[payloadCase] ?? this.streamName
  }
}
