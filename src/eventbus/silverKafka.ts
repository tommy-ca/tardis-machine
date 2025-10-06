import { Kafka, logLevel, Producer, SASLOptions, CompressionTypes } from 'kafkajs'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverKafkaEventBusConfig, SilverEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'
import * as fs from 'fs'
import * as path from 'path'

const log = debug.extend('eventbus')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class SilverKafkaEventBus implements SilverEventSink {
  private readonly encoder: SilverNormalizedEventEncoder
  private readonly kafka: Kafka
  private readonly producer: Producer
  private schemaId?: number
  private readonly buffer: SilverEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly compression?: CompressionTypes
  private readonly staticHeaders?: Array<[string, Buffer]>
  private readonly allowedRecordTypes?: Set<SilverRecordType>
  private readonly acks?: -1 | 0 | 1

  constructor(private readonly config: SilverKafkaEventBusConfig) {
    const keyBuilder = config.keyTemplate ? compileSilverKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
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
    if (config.includeRecordTypes) {
      this.allowedRecordTypes = new Set(config.includeRecordTypes)
    }
  }

  async start() {
    await this.producer.connect()

    if (this.config.schemaRegistry) {
      const schemaPath = path.join(__dirname, '../../schemas/proto/lakehouse/silver/v1/records.proto')
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
        log('Failed to send Silver Kafka batch: %o', error)
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
          if (this.schemaId) {
            // Use schema registry encoding
            const { SchemaRegistry } = await import('@kafkajs/confluent-schema-registry')
            const registry = new SchemaRegistry({
              host: this.config.schemaRegistry!.url,
              auth: this.config.schemaRegistry!.auth
                ? {
                    username: this.config.schemaRegistry!.auth.username,
                    password: this.config.schemaRegistry!.auth.password
                  }
                : undefined
            })
            const messages = await Promise.all(
              events.map(async (event) => ({
                key: event.key,
                value: await registry.encode(this.schemaId!, Buffer.from(event.binary)),
                headers: this.buildHeaders(event)
              }))
            )
            await this.producer.send({
              topic,
              messages,
              compression: this.compression,
              acks: this.acks
            })
          } else {
            await this.producer.send({
              topic,
              messages: events.map((event) => ({
                key: event.key,
                value: Buffer.from(event.binary),
                headers: this.buildHeaders(event)
              })),
              compression: this.compression,
              acks: this.acks
            })
          }
        }
        return
      } catch (error) {
        log('Silver Kafka send attempt %d failed: %o', attempt, error)
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

  private buildHeaders(event: SilverEvent): Record<string, Buffer> {
    const headers: Record<string, Buffer> = {
      recordType: Buffer.from(event.recordType),
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

function mapSasl(config?: SilverKafkaEventBusConfig['sasl']): SASLOptions | undefined {
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

function mapCompression(config?: SilverKafkaEventBusConfig['compression']): CompressionTypes | undefined {
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
