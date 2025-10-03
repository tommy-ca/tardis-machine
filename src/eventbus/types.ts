import type { NormalizedEvent, Origin } from '../generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Disconnect, NormalizedData } from 'tardis-dev'

export type NormalizedMessage = NormalizedData | Disconnect

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
  /** Encoded Binary payload ready for transport */
  binary: Uint8Array
}

export type BronzePayloadCase = Exclude<NormalizedEvent['payload']['case'], undefined>

export interface NormalizedEventEncoder {
  encode(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[]
}

export interface NormalizedEventSink {
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
  clientId?: string
  ssl?: boolean
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
    username: string
    password: string
  }
  /** Maximum number of Bronze events to send per Kafka batch */
  maxBatchSize?: number
  /** Maximum milliseconds events can wait before forced flush */
  maxBatchDelayMs?: number
}

export type EventBusConfig =
  | (
      {
        provider: 'kafka'
      } & KafkaEventBusConfig
    )
