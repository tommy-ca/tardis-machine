import { Client } from 'pulsar-client'

type Producer = any
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverEventSink, NormalizedMessage, PublishMeta, SilverPulsarEventBusConfig } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus:silver-pulsar')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class SilverPulsarEventBus implements SilverEventSink {
  private readonly encoder: SilverNormalizedEventEncoder
  private readonly client: Client
  private readonly producers = new Map<string, Producer>()
  private readonly buffer: SilverEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly staticProperties?: Array<[string, string]>
  private readonly allowedRecordTypes?: Set<SilverRecordType>

  constructor(private readonly config: SilverPulsarEventBusConfig) {
    const keyBuilder = config.keyTemplate ? compileSilverKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
    this.client = new Client({
      serviceUrl: config.serviceUrl,
      authentication: config.token ? { token: config.token } : undefined
    })
    if (config.staticProperties) {
      this.staticProperties = Object.entries(config.staticProperties)
    }
    if (config.includeRecordTypes) {
      this.allowedRecordTypes = new Set(config.includeRecordTypes)
    }
  }

  async start() {
    // Producers will be created on demand
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
        log('Failed to send Silver Pulsar batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatch(batch: SilverEvent[]): Promise<void> {
    let attempt = 0
    while (attempt < MAX_RETRY_ATTEMPTS) {
      attempt++
      try {
        const groups = this.groupByTopic(batch)
        for (const [topic, events] of groups) {
          const producer = await this.getProducer(topic)
          for (const event of events) {
            await producer.send({
              data: Buffer.from(event.binary),
              properties: this.buildProperties(event)
            })
          }
        }
        return
      } catch (error) {
        log('Silver Pulsar send attempt %d failed: %o', attempt, error)
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

  private async getProducer(topic: string): Promise<Producer> {
    let producer = this.producers.get(topic)
    if (!producer) {
      producer = await this.client.createProducer({
        topic,
        sendTimeoutMs: 30000
      })
      this.producers.set(topic, producer)
    }
    return producer
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
    for (const producer of this.producers.values()) {
      await producer.close()
    }
    await this.client.close()
  }

  private groupByTopic(events: SilverEvent[]): Map<string, SilverEvent[]> {
    const groups = new Map<string, SilverEvent[]>()
    for (const event of events) {
      const topic = this.resolveTopic(event.recordType)
      const bucket = groups.get(topic)
      if (bucket) {
        bucket.push(event)
      } else {
        groups.set(topic, [event])
      }
    }
    return groups
  }

  private resolveTopic(recordType: SilverRecordType): string {
    const { topicByRecordType, topic } = this.config
    return topicByRecordType?.[recordType] ?? topic
  }

  private filterEvents(events: SilverEvent[]): SilverEvent[] {
    if (!this.allowedRecordTypes) {
      return events
    }

    return events.filter((event) => this.allowedRecordTypes!.has(event.recordType))
  }

  private buildProperties(event: SilverEvent): Record<string, string> {
    const properties: Record<string, string> = {
      recordType: event.recordType,
      dataType: event.dataType
    }

    if (this.staticProperties) {
      for (const [key, value] of this.staticProperties) {
        properties[key] = value
      }
    }

    for (const [key, value] of Object.entries(event.meta)) {
      properties[key] = value
    }

    return properties
  }
}
