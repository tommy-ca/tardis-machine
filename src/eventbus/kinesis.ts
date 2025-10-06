import { KinesisClient, PutRecordsCommand, type PutRecordsResultEntry } from '@aws-sdk/client-kinesis'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, KinesisEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus:kinesis')

const DEFAULT_BATCH_SIZE = 500 // Kinesis max is 500 records per PutRecords
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class KinesisEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly kinesis: KinesisClient
  private readonly buffer: BronzeEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>

  constructor(private readonly config: KinesisEventBusConfig) {
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
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
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
    const batches: BronzeEvent[][] = []

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
        log('Failed to send Kinesis batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatch(batch: BronzeEvent[]): Promise<void> {
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
        log('Kinesis send attempt %d failed: %o', attempt, error)
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
    const { streamByPayloadCase, streamName } = this.config
    return streamByPayloadCase?.[payloadCase] ?? streamName
  }

  private filterEvents(events: BronzeEvent[]): BronzeEvent[] {
    if (!this.allowedPayloadCases) {
      return events
    }

    return events.filter((event) => this.allowedPayloadCases!.has(event.payloadCase))
  }
}
