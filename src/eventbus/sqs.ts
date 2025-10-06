import { SQSClient, SendMessageBatchCommand, type SendMessageBatchRequestEntry } from '@aws-sdk/client-sqs'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, SQSEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus:sqs')

const DEFAULT_BATCH_SIZE = 10 // SQS max is 10 messages per SendMessageBatch
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class SQSEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly sqs: SQSClient
  private readonly buffer: BronzeEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>

  constructor(private readonly config: SQSEventBusConfig) {
    const messageGroupIdBuilder = config.queueByPayloadCase ? compileKeyBuilder('') : undefined // For FIFO queues
    this.encoder = new BronzeNormalizedEventEncoder(messageGroupIdBuilder)
    this.sqs = new SQSClient({
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
    // SQS client is ready to use
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
        log('Failed to send SQS batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatch(batch: BronzeEvent[]): Promise<void> {
    let attempt = 0
    while (attempt < MAX_RETRY_ATTEMPTS) {
      attempt++
      try {
        const groups = this.groupByQueue(batch)
        for (const [queueUrl, events] of groups) {
          await this.sendMessageBatch(queueUrl, events)
        }
        return
      } catch (error) {
        log('SQS send attempt %d failed: %o', attempt, error)
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

  private async sendMessageBatch(queueUrl: string, events: BronzeEvent[]): Promise<void> {
    const entries: SendMessageBatchRequestEntry[] = events.map((event, index) => ({
      Id: `msg-${index}`,
      MessageBody: Buffer.from(event.binary).toString('base64'),
      MessageAttributes: this.buildMessageAttributes(event)
    }))

    const command = new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: entries
    })

    const response = await this.sqs.send(command)

    if (response.Failed && response.Failed.length > 0) {
      const failedMessages = response.Failed
      log('SQS send message batch failed for %d messages: %o', failedMessages.length, failedMessages)
      throw new Error(`SQS send message batch failed for ${failedMessages.length} messages`)
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
    this.sqs.destroy()
  }

  private groupByQueue(events: BronzeEvent[]): Map<string, BronzeEvent[]> {
    const groups = new Map<string, BronzeEvent[]>()
    for (const event of events) {
      const queueUrl = this.resolveQueueUrl(event.payloadCase)
      const bucket = groups.get(queueUrl)
      if (bucket) {
        bucket.push(event)
      } else {
        groups.set(queueUrl, [event])
      }
    }
    return groups
  }

  private resolveQueueUrl(payloadCase: BronzePayloadCase): string {
    const { queueByPayloadCase, queueUrl } = this.config
    return queueByPayloadCase?.[payloadCase] ?? queueUrl
  }

  private filterEvents(events: BronzeEvent[]): BronzeEvent[] {
    if (!this.allowedPayloadCases) {
      return events
    }

    return events.filter((event) => this.allowedPayloadCases!.has(event.payloadCase))
  }

  private buildMessageAttributes(event: BronzeEvent): Record<string, { DataType: string; StringValue: string }> {
    const attributes: Record<string, { DataType: string; StringValue: string }> = {
      payloadCase: { DataType: 'String', StringValue: event.payloadCase },
      dataType: { DataType: 'String', StringValue: event.dataType }
    }

    if (this.config.staticMessageAttributes) {
      for (const [key, value] of Object.entries(this.config.staticMessageAttributes)) {
        attributes[key] = { DataType: 'String', StringValue: value }
      }
    }

    for (const [key, value] of Object.entries(event.meta)) {
      attributes[key] = { DataType: 'String', StringValue: value }
    }

    return attributes
  }
}
