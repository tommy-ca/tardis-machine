import { Kafka, logLevel, Producer, SASLOptions } from 'kafkajs'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import type { BronzeEvent, KafkaEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class KafkaEventBus implements NormalizedEventSink {
  private readonly encoder = new BronzeNormalizedEventEncoder()
  private readonly kafka: Kafka
  private readonly producer: Producer
  private readonly buffer: BronzeEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false

  constructor(private readonly config: KafkaEventBusConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId ?? 'tardis-machine',
      brokers: config.brokers,
      ssl: config.ssl,
      sasl: mapSasl(config.sasl),
      logLevel: logLevel.NOTHING
    })
    this.producer = this.kafka.producer({ allowAutoTopicCreation: true })
  }

  async start() {
    await this.producer.connect()
  }

  async publish(message: NormalizedMessage, meta: PublishMeta): Promise<void> {
    if (this.closed) {
      return
    }

    const events = this.encoder.encode(message, meta)
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
    const batch = this.buffer.splice(0, batchSize)

    this.sendingPromise = this.sendingPromise
      .then(() => this.sendBatch(batch))
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
        await this.producer.send({
          topic: this.config.topic,
          messages: batch.map((event) => ({
            key: event.key,
            value: Buffer.from(event.binary),
            headers: {
              payloadCase: Buffer.from(event.payloadCase),
              dataType: Buffer.from(event.dataType)
            }
          }))
        })
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
