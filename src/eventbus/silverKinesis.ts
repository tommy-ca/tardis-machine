import { KinesisClient, PutRecordsCommand, type PutRecordsResultEntry } from '@aws-sdk/client-kinesis'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverKinesisEventBusConfig, SilverEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus:silver-kinesis')

const DEFAULT_BATCH_SIZE = 500 // Kinesis max is 500 records per PutRecords
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class SilverKinesisEventBus implements SilverEventSink {
  private readonly encoder: SilverNormalizedEventEncoder
  private readonly kinesis: KinesisClient
  private readonly buffer: SilverEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly allowedRecordTypes?: Set<SilverRecordType>

  constructor(private readonly config: SilverKinesisEventBusConfig) {
    const partitionKeyBuilder = config.partitionKeyTemplate ? compileSilverKeyBuilder(config.partitionKeyTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(partitionKeyBuilder)
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
    if (config.includeRecordTypes) {
      this.allowedRecordTypes = new Set(config.includeRecordTypes)
    }
  }

  async start() {
    // Kinesis client is ready to use
  }

  async publish(message: NormalizedMessage, meta: PublishMeta): Promise<void> {
    if (this.closed) {
      return
    }

    const events = this.filterEvents(this.encoder.encode(message, meta))
    if (events.length === 0) {
      return
    }

    this.buffer.push(...events)

    if (this.buffer.length >= (this.config.maxBatchSize ?? DEFAULT_BATCH_SIZE)) {
      this.flushImmediately()
    } else {
      this.scheduleFlush()
    }
  }

  private scheduleFlush() {
    if (this.flushTimer) {
      return
    }

    const delay = this.config.maxBatchDelayMs ?? DEFAULT_BATCH_DELAY_MS
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined
      this.flushImmediately()
    }, delay)
  }

  private flushImmediately() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }

    if (this.buffer.length === 0) {
      return
    }

    const batchSize = this.config.maxBatchSize ?? DEFAULT_BATCH_SIZE
    const batches: SilverEvent[][] = []

    while (this.buffer.length > 0) {
      const chunk = this.buffer.splice(0, batchSize)
      if (chunk.length > 0) {
        batches.push(chunk)
      }
    }

    this.sendingPromise = this.sendingPromise
      .then(async () => {
        for (let index = 0; index < batches.length; index++) {
          const batch = batches[index]
          if (batch.length === 0) {
            continue
          }

          try {
            await this.sendBatch(batch)
          } catch (error) {
            for (let remainingIndex = batches.length - 1; remainingIndex > index; remainingIndex--) {
              const remainingBatch = batches[remainingIndex]
              if (remainingBatch.length > 0) {
                this.buffer.unshift(...remainingBatch)
              }
            }
            throw error
          }
        }
      })
      .catch((error) => {
        log('Failed to send Silver Kinesis batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatch(batch: SilverEvent[]): Promise<void> {
    let attempt = 0
    while (attempt < MAX_RETRY_ATTEMPTS) {
      attempt++
      try {
        const groups = this.groupByStream(batch)
        for (const [streamName, events] of groups) {
          await this.putRecords(streamName, events)
        }
        return
      } catch (error) {
        log('Silver Kinesis send attempt %d failed: %o', attempt, error)
        if (attempt >= MAX_RETRY_ATTEMPTS) {
          // requeue events for future flush to preserve at-least-once semantics
          this.buffer.unshift(...batch)
          throw error
        }
        const backoffMs = Math.min(200 * attempt, 1000)
        await wait(backoffMs)
      }
    }
  }

  private async putRecords(streamName: string, events: SilverEvent[]): Promise<void> {
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
      log('Silver Kinesis put records failed for %d records: %o', failedRecords.length, failedRecords)
      throw new Error(`Silver Kinesis put records failed for ${failedRecords.length} records`)
    }
  }

  async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }

    if (this.buffer.length > 0) {
      this.flushImmediately()
    }

    await this.sendingPromise
  }

  async close(): Promise<void> {
    if (this.closed) {
      return
    }

    this.closed = true
    await this.flush()
    this.kinesis.destroy()
  }

  private groupByStream(events: SilverEvent[]): Map<string, SilverEvent[]> {
    const groups = new Map<string, SilverEvent[]>()
    for (const event of events) {
      const streamName = this.resolveStream(event.recordType)
      const bucket = groups.get(streamName)
      if (bucket) {
        bucket.push(event)
      } else {
        groups.set(streamName, [event])
      }
    }
    return groups
  }

  private resolveStream(recordType: SilverRecordType): string {
    const { streamByRecordType, streamName } = this.config
    return streamByRecordType?.[recordType] ?? streamName
  }

  private filterEvents(events: SilverEvent[]): SilverEvent[] {
    if (!this.allowedRecordTypes) {
      return events
    }

    return events.filter((event) => this.allowedRecordTypes!.has(event.recordType))
  }
}
