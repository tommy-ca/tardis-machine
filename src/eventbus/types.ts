import type { NormalizedEvent, Origin } from '../generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Disconnect, NormalizedData } from 'tardis-dev'

export type ControlErrorCode = 'unspecified' | 'ws_connect' | 'ws_send' | 'source_auth' | 'source_rate_limit'

export type ControlErrorMessage = {
  type: 'error'
  exchange: string
  localTimestamp: Date
  symbol?: string
  details: string
  subsequentErrors?: number
  code?: ControlErrorCode
}

export type NormalizedMessage = NormalizedData | Disconnect | ControlErrorMessage

export type PublishMeta = {
  /** Identifier for producer instance, defaults to tardis-machine */
  source: string
  /** Origin of the data (replay, realtime, archive) */
  origin: Origin
  /** Timestamp when message was ingested into the publisher */
  ingestTimestamp: Date
  /** Optional logical request/session identifier */
  requestId?: string
  /** Optional websocket session identifier */
  sessionId?: string
  /** Additional metadata propagated to Bronze meta map */
  extraMeta?: Record<string, string | undefined>
}

export type BronzeEvent = {
  key: string
  payloadCase: BronzePayloadCase
  /** Human readable description of payload case */
  dataType: string
  /** Flattened metadata map propagated to headers */
  meta: Record<string, string>
  /** Encoded Binary payload ready for transport */
  binary: Uint8Array
}

export type BronzePayloadCase = Exclude<NormalizedEvent['payload']['case'], undefined>

export type SilverRecordType =
  | 'trade'
  | 'book_change'
  | 'book_snapshot'
  | 'grouped_book_snapshot'
  | 'trade_bar'
  | 'quote'
  | 'derivative_ticker'
  | 'liquidation'
  | 'option_summary'
  | 'book_ticker'

export type SilverEvent = {
  key: string
  recordType: SilverRecordType
  dataType: string
  meta: Record<string, string>
  binary: Uint8Array
}

export interface NormalizedEventEncoder {
  encode(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[]
}

export interface SilverEventEncoder {
  encode(message: NormalizedMessage, meta: PublishMeta): SilverEvent[]
}

export interface NormalizedEventSink {
  start(): Promise<void>
  publish(message: NormalizedMessage, meta: PublishMeta): Promise<void>
  flush(): Promise<void>
  close(): Promise<void>
}

export interface SilverEventSink {
  start(): Promise<void>
  publish(message: NormalizedMessage, meta: PublishMeta): Promise<void>
  flush(): Promise<void>
  close(): Promise<void>
}

export type PublishInjection = Pick<PublishMeta, 'origin' | 'requestId' | 'sessionId' | 'extraMeta'>

export type PublishFn = (message: NormalizedMessage, meta: PublishInjection) => void

export type KafkaEventBusConfig = {
  brokers: string[]
  topic: string
  /** Optional map for routing payload cases to dedicated topics */
  topicByPayloadCase?: Partial<Record<BronzePayloadCase, string>>
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  clientId?: string
  ssl?: boolean
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string
  }
  /** Prefix applied when emitting normalized meta as Kafka headers */
  metaHeadersPrefix?: string
  /** Static Kafka headers applied to every message */
  staticHeaders?: Record<string, string>
  /** Maximum number of Bronze events to send per Kafka batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Compression strategy applied to Kafka batches */
  compression?: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'
  /** Template for constructing Kafka record keys */
  keyTemplate?: string
  /** Ack level passed to Kafka producer sends */
  acks?: -1 | 0 | 1
  /** Enable Kafka idempotent producer semantics */
  idempotent?: boolean
  /** Schema Registry configuration for Avro/Protobuf schema management */
  schemaRegistry?: {
    /** Schema Registry URL */
    url: string
    /** Optional authentication */
    auth?: {
      username: string
      password: string
    }
  }
}

export type RabbitMQEventBusConfig = {
  url: string
  exchange: string
  exchangeType?: 'direct' | 'topic' | 'headers' | 'fanout'
  /** Optional routing key template for constructing routing keys */
  routingKeyTemplate?: string
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  /** Static headers applied to every message */
  staticHeaders?: Record<string, string>
}

export type KinesisEventBusConfig = {
  streamName: string
  region: string
  /** Optional map for routing payload cases to dedicated streams */
  streamByPayloadCase?: Partial<Record<BronzePayloadCase, string>>
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  /** AWS access key ID */
  accessKeyId?: string
  /** AWS secret access key */
  secretAccessKey?: string
  /** AWS session token (for temporary credentials) */
  sessionToken?: string
  /** Static metadata applied to every record */
  staticHeaders?: Record<string, string>
  /** Maximum number of Bronze events to send per Kinesis batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Template for constructing Kinesis partition keys */
  partitionKeyTemplate?: string
}

export type NatsEventBusConfig = {
  servers: string[]
  subject: string
  /** Optional map for routing payload cases to dedicated subjects */
  subjectByPayloadCase?: Partial<Record<BronzePayloadCase, string>>
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  /** Static headers applied to every message */
  staticHeaders?: Record<string, string>
  /** Template for constructing NATS message subjects */
  subjectTemplate?: string
  /** NATS user for authentication */
  user?: string
  /** NATS password for authentication */
  pass?: string
}

export type RedisEventBusConfig = {
  url: string
  stream: string
  /** Optional map for routing payload cases to dedicated streams */
  streamByPayloadCase?: Partial<Record<BronzePayloadCase, string>>
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  /** Static headers applied to every message */
  staticHeaders?: Record<string, string>
  /** Maximum number of Bronze events to send per Redis batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Template for constructing Redis stream keys */
  keyTemplate?: string
}

export type SQSEventBusConfig = {
  queueUrl: string
  region: string
  /** Optional map for routing payload cases to dedicated queues */
  queueByPayloadCase?: Partial<Record<BronzePayloadCase, string>>
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  /** AWS access key ID */
  accessKeyId?: string
  /** AWS secret access key */
  secretAccessKey?: string
  /** AWS session token (for temporary credentials) */
  sessionToken?: string
  /** Static message attributes applied to every message */
  staticMessageAttributes?: Record<string, string>
  /** Maximum number of Bronze events to send per SQS batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
}

export type PulsarEventBusConfig = {
  serviceUrl: string
  topic: string
  /** Optional map for routing payload cases to dedicated topics */
  topicByPayloadCase?: Partial<Record<BronzePayloadCase, string>>
  /** Optional allow-list of payload cases to publish */
  includePayloadCases?: BronzePayloadCase[]
  /** Pulsar authentication token */
  token?: string
  /** Static properties applied to every message */
  staticProperties?: Record<string, string>
  /** Maximum number of Bronze events to send per Pulsar batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Compression type for Pulsar messages */
  compressionType?: 'NONE' | 'LZ4' | 'ZLIB' | 'ZSTD' | 'SNAPPY'
  /** Template for constructing Pulsar message keys */
  keyTemplate?: string
}

export type SilverKafkaEventBusConfig = {
  brokers: string[]
  topic: string
  /** Optional map for routing record types to dedicated topics */
  topicByRecordType?: Partial<Record<SilverRecordType, string>>
  /** Optional allow-list of record types to publish */
  includeRecordTypes?: SilverRecordType[]
  clientId?: string
  ssl?: boolean
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string
  }
  /** Prefix applied when emitting normalized meta as Kafka headers */
  metaHeadersPrefix?: string
  /** Static Kafka headers applied to every message */
  staticHeaders?: Record<string, string>
  /** Maximum number of Silver events to send per Kafka batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Compression strategy applied to Kafka batches */
  compression?: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'
  /** Template for constructing Kafka record keys */
  keyTemplate?: string
  /** Ack level passed to Kafka producer sends */
  acks?: -1 | 0 | 1
  /** Enable Kafka idempotent producer semantics */
  idempotent?: boolean
  /** Schema Registry configuration for Avro/Protobuf schema management */
  schemaRegistry?: {
    /** Schema Registry URL */
    url: string
    /** Optional authentication */
    auth?: {
      username: string
      password: string
    }
  }
}

export type SilverRabbitMQEventBusConfig = {
  url: string
  exchange: string
  exchangeType?: 'direct' | 'topic' | 'headers' | 'fanout'
  /** Optional routing key template for constructing routing keys */
  routingKeyTemplate?: string
  /** Optional allow-list of record types to publish */
  includeRecordTypes?: SilverRecordType[]
  /** Static headers applied to every message */
  staticHeaders?: Record<string, string>
}

export type SilverKinesisEventBusConfig = {
  streamName: string
  region: string
  /** Optional map for routing record types to dedicated streams */
  streamByRecordType?: Partial<Record<SilverRecordType, string>>
  /** Optional allow-list of record types to publish */
  includeRecordTypes?: SilverRecordType[]
  /** AWS access key ID */
  accessKeyId?: string
  /** AWS secret access key */
  secretAccessKey?: string
  /** AWS session token (for temporary credentials) */
  sessionToken?: string
  /** Static metadata applied to every record */
  staticHeaders?: Record<string, string>
  /** Maximum number of Silver events to send per Kinesis batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Template for constructing Kinesis partition keys */
  partitionKeyTemplate?: string
}

export type SilverNatsEventBusConfig = {
  servers: string[]
  subject: string
  /** Optional map for routing record types to dedicated subjects */
  subjectByRecordType?: Partial<Record<SilverRecordType, string>>
  /** Optional allow-list of record types to publish */
  includeRecordTypes?: SilverRecordType[]
  /** Static headers applied to every message */
  staticHeaders?: Record<string, string>
  /** Template for constructing NATS message subjects */
  subjectTemplate?: string
  /** NATS user for authentication */
  user?: string
  /** NATS password for authentication */
  pass?: string
}

export type SilverRedisEventBusConfig = {
  url: string
  stream: string
  /** Optional map for routing record types to dedicated streams */
  streamByRecordType?: Partial<Record<SilverRecordType, string>>
  /** Optional allow-list of record types to publish */
  includeRecordTypes?: SilverRecordType[]
  /** Static headers applied to every message */
  staticHeaders?: Record<string, string>
  /** Maximum number of Silver events to send per Redis batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
  /** Template for constructing Redis stream keys */
  keyTemplate?: string
}

export type EventBusConfig =
  | ({
      provider: 'kafka'
    } & KafkaEventBusConfig)
  | ({
      provider: 'rabbitmq'
    } & RabbitMQEventBusConfig)
  | ({
      provider: 'kinesis'
    } & KinesisEventBusConfig)
  | ({
      provider: 'nats'
    } & NatsEventBusConfig)
  | ({
      provider: 'redis'
    } & RedisEventBusConfig)
  | ({
      provider: 'sqs'
    } & SQSEventBusConfig)
  | ({
      provider: 'pulsar'
    } & PulsarEventBusConfig)
  | ({
      provider: 'kafka-silver'
    } & SilverKafkaEventBusConfig)
  | ({
      provider: 'rabbitmq-silver'
    } & SilverRabbitMQEventBusConfig)
  | ({
      provider: 'kinesis-silver'
    } & SilverKinesisEventBusConfig)
  | ({
      provider: 'nats-silver'
    } & SilverNatsEventBusConfig)
  | ({
      provider: 'redis-silver'
    } & SilverRedisEventBusConfig)
