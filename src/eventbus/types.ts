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

export type EventBusConfig =
  | ({
      provider: 'kafka'
    } & KafkaEventBusConfig)
  | ({
      provider: 'kafka-silver'
    } & SilverKafkaEventBusConfig)
