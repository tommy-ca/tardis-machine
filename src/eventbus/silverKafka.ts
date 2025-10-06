import { Kafka, logLevel, Producer, SASLOptions, CompressionTypes } from 'kafkajs'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverKafkaEventBusConfig, NormalizedMessage, PublishMeta } from './types'
import { BaseSilverEventBusPublisher, CommonSilverConfig } from './baseSilverPublisher'
import { debug } from '../debug'
import * as fs from 'fs'
import * as path from 'path'

const log = debug.extend('eventbus')

export class SilverKafkaEventBus extends BaseSilverEventBusPublisher {
  private readonly encoder: SilverNormalizedEventEncoder
  private readonly kafka: Kafka
  private readonly producer: Producer
  private schemaId?: number
  private readonly compression?: CompressionTypes
  private readonly staticHeaders?: Array<[string, Buffer]>
  private readonly acks?: -1 | 0 | 1
  private readonly topic: string
  private readonly topicByRecordType?: Record<string, string>
  private readonly metaHeadersPrefix?: string

  constructor(private readonly config: SilverKafkaEventBusConfig) {
    const commonConfig: CommonSilverConfig = {
      maxBatchSize: config.maxBatchSize,
      maxBatchDelayMs: config.maxBatchDelayMs,
      includeRecordTypes: config.includeRecordTypes
    }
    super(commonConfig)
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
    this.topic = config.topic
    this.topicByRecordType = config.topicByRecordType
    this.metaHeadersPrefix = config.metaHeadersPrefix
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

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): SilverEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: SilverEvent[]): Promise<void> {
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
            value: this.encodeValue(event.binary),
            headers: this.buildHeaders(event)
          })),
          compression: this.compression,
          acks: this.acks
        })
      }
    }
  }

  protected async doClose(): Promise<void> {
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
    return this.topicByRecordType?.[recordType] ?? this.topic
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

    const prefix = this.metaHeadersPrefix
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
