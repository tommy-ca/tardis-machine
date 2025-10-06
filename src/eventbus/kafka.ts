import { Kafka, logLevel, Producer, SASLOptions, CompressionTypes } from 'kafkajs'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, KafkaEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'
import * as fs from 'fs'
import * as path from 'path'

const log = debug.extend('eventbus')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class KafkaEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly kafka: Kafka
  private readonly producer: Producer
  private schemaId?: number
  private readonly buffer: BronzeEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly compression?: CompressionTypes
  private readonly staticHeaders?: Array<[string, Buffer]>
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>
  private readonly acks?: -1 | 0 | 1

  constructor(private readonly config: KafkaEventBusConfig) {
    const keyBuilder = config.keyTemplate ? compileKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    this.kafka = new Kafka({
      clientId: config.clientId ?? 'tardis-machine',
      brokers: config.brokers,
      ssl: config.ssl,
      sasl: mapSasl(config.sasl),
      logLevel: logLevel.NOTHING
    })
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      idempotent: config.idempotent
    })
    this.compression = mapCompression(config.compression)
    if (config.staticHeaders) {
      this.staticHeaders = Object.entries(config.staticHeaders).map(([key, value]) => [key, Buffer.from(value)])
    }
    this.acks = config.acks
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
    }
  }

  async start() {
    await this.producer.connect()

    if (this.config.schemaRegistry) {
      const schemaPath = path.join(__dirname, '../../schemas/proto/lakehouse/bronze/v1/normalized_event.proto')
      const schema = fs.readFileSync(schemaPath, 'utf8')
      const subject = `${this.config.topic}-value`
      // Register schema via REST API
      const response = await fetch(`${this.config.schemaRegistry.url}/subjects/${encodeURIComponent(subject)}/versions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/vnd.schemaregistry.v1+json',
          ...(this.config.schemaRegistry.auth
            ? {
                Authorization: `Basic ${Buffer.from(`${this.config.schemaRegistry.auth.username}:${this.config.schemaRegistry.auth.password}`).toString('base64')}`
              }
            : {})
        },
        body: JSON.stringify({
          schemaType: 'PROTOBUF',
          schema: schema
        })
      })
      if (!response.ok) {
        throw new Error(`Failed to register schema: ${response.statusText}`)
      }
      const result = await response.json()
      this.schemaId = result.id
    }
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
        log('Failed to send Kafka batch: %o', error)
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
          await this.producer.send({
            topic,
            messages: events.map((event) => ({
              key: event.key,
              value: this.encodeValue(event.binary),
              headers: this.buildHeaders(event)
            })),
            compression: this.compression,
            acks: this.acks
          })
        }
        return
      } catch (error) {
        log('Kafka send attempt %d failed: %o', attempt, error)
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
    await this.producer.disconnect()
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

  private encodeValue(binary: Uint8Array): Buffer {
    if (this.schemaId !== undefined) {
      const buffer = Buffer.alloc(5 + binary.length)
      buffer.writeUInt8(0, 0) // magic byte
      buffer.writeUInt32BE(this.schemaId, 1) // schema ID
      buffer.set(binary, 5)
      return buffer
    }
    return Buffer.from(binary)
  }

  private buildHeaders(event: BronzeEvent): Record<string, Buffer> {
    const headers: Record<string, Buffer> = {
      payloadCase: Buffer.from(event.payloadCase),
      dataType: Buffer.from(event.dataType)
    }

    if (this.staticHeaders) {
      for (const [key, value] of this.staticHeaders) {
        headers[key] = value
      }
    }

    const prefix = this.config.metaHeadersPrefix
    if (!prefix) {
      return headers
    }

    for (const [key, value] of Object.entries(event.meta)) {
      headers[`${prefix}${key}`] = Buffer.from(value)
    }

    return headers
  }
}

function mapSasl(config?: KafkaEventBusConfig['sasl']): SASLOptions | undefined {
  if (!config) {
    return undefined
  }

  switch (config.mechanism) {
    case 'plain':
      return {
        mechanism: 'plain',
        username: config.username,
        password: config.password
      }
    case 'scram-sha-256':
      return {
        mechanism: 'scram-sha-256',
        username: config.username,
        password: config.password
      }
    case 'scram-sha-512':
      return {
        mechanism: 'scram-sha-512',
        username: config.username,
        password: config.password
      }
    default:
      return undefined
  }
}

function mapCompression(config?: KafkaEventBusConfig['compression']): CompressionTypes | undefined {
  switch (config) {
    case undefined:
      return undefined
    case 'none':
      return CompressionTypes.None
    case 'gzip':
      return CompressionTypes.GZIP
    case 'snappy':
      return CompressionTypes.Snappy
    case 'lz4':
      return CompressionTypes.LZ4
    case 'zstd':
      return CompressionTypes.ZSTD
    default:
      return undefined
  }
}
