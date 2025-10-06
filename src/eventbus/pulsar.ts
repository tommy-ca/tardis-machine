import { Client } from 'pulsar-client'

type Producer = any
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, NormalizedEventSink, NormalizedMessage, PublishMeta, PulsarEventBusConfig } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'
import * as fs from 'fs'
import * as path from 'path'

const log = debug.extend('eventbus')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class PulsarEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly client: Client
  private readonly producers = new Map<string, Producer>()
  private readonly buffer: BronzeEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly staticProperties?: Array<[string, string]>
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>
  private schema?: { schemaType: 'Protobuf'; name?: string; schema?: string; properties?: Record<string, string> }

  constructor(private readonly config: PulsarEventBusConfig) {
    const keyBuilder = config.keyTemplate ? compileKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    this.client = new Client({
      serviceUrl: config.serviceUrl,
      authentication: config.token ? { token: config.token } : undefined
    })
    if (config.staticProperties) {
      this.staticProperties = Object.entries(config.staticProperties)
    }
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
    }
  }

  async start() {
    if (this.config.schemaRegistry) {
      const schemaPath = path.join(__dirname, '../../schemas/proto/lakehouse/bronze/v1/normalized_event.proto')
      const schema = fs.readFileSync(schemaPath, 'utf8')
      this.schema = {
        schemaType: 'Protobuf' as const,
        name: 'NormalizedEvent',
        schema: schema
      }
    }
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
        log('Failed to send Pulsar batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatch(batch: BronzeEvent[]): Promise<void> {
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
        log('Pulsar send attempt %d failed: %o', attempt, error)
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
        sendTimeoutMs: 30000,
        schema: this.schema
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

  private groupByTopic(events: BronzeEvent[]): Map<string, BronzeEvent[]> {
    const groups = new Map<string, BronzeEvent[]>()
    for (const event of events) {
      const topic = this.resolveTopic(event.payloadCase)
      const bucket = groups.get(topic)
      if (bucket) {
        bucket.push(event)
      } else {
        groups.set(topic, [event])
      }
    }
    return groups
  }

  private resolveTopic(payloadCase: BronzePayloadCase): string {
    const { topicByPayloadCase, topic } = this.config
    return topicByPayloadCase?.[payloadCase] ?? topic
  }

  private filterEvents(events: BronzeEvent[]): BronzeEvent[] {
    if (!this.allowedPayloadCases) {
      return events
    }

    return events.filter((event) => this.allowedPayloadCases!.has(event.payloadCase))
  }

  private buildProperties(event: BronzeEvent): Record<string, string> {
    const properties: Record<string, string> = {
      payloadCase: event.payloadCase,
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
