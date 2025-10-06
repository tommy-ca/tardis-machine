import { createClient, RedisClientType } from 'redis'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, RedisEventBusConfig, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export class RedisEventBus implements NormalizedEventSink {
  private readonly encoder: BronzeNormalizedEventEncoder
  private readonly redis: RedisClientType
  private readonly buffer: BronzeEvent[] = []
  private flushTimer?: NodeJS.Timeout
  private sendingPromise: Promise<void> = Promise.resolve()
  private closed = false
  private readonly staticHeaders?: Array<[string, Buffer]>
  private readonly allowedPayloadCases?: Set<BronzePayloadCase>

  constructor(private readonly config: RedisEventBusConfig) {
    const keyBuilder = config.keyTemplate ? compileKeyBuilder(config.keyTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    this.redis = createClient({ url: config.url })
    if (config.staticHeaders) {
      this.staticHeaders = Object.entries(config.staticHeaders).map(([key, value]) => [key, Buffer.from(value)])
    }
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
    }
  }

  async start() {
    await this.redis.connect()
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
        log('Failed to send Redis batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatch(batch: BronzeEvent[]): Promise<void> {
    let attempt = 0
    while (attempt < MAX_RETRY_ATTEMPTS) {
      attempt++
      try {
        const groups = this.groupByStream(batch)
        for (const [stream, events] of groups) {
          for (const event of events) {
            const fields: Record<string, string | Buffer> = {
              data: Buffer.from(event.binary),
              payloadCase: event.payloadCase,
              dataType: event.dataType
            }
            if (this.staticHeaders) {
              for (const [key, value] of this.staticHeaders) {
                fields[key] = value
              }
            }
            for (const [key, value] of Object.entries(event.meta)) {
              fields[key] = value
            }
            await this.redis.xAdd(stream, '*', fields)
          }
        }
        return
      } catch (error) {
        log('Redis send attempt %d failed: %o', attempt, error)
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
    await this.redis.disconnect()
  }

  private groupByStream(events: BronzeEvent[]): Map<string, BronzeEvent[]> {
    const groups = new Map<string, BronzeEvent[]>()
    for (const event of events) {
      const stream = this.resolveStream(event.payloadCase)
      const bucket = groups.get(stream)
      if (bucket) {
        bucket.push(event)
      } else {
        groups.set(stream, [event])
      }
    }
    return groups
  }

  private resolveStream(payloadCase: BronzePayloadCase): string {
    const { streamByPayloadCase, stream } = this.config
    return streamByPayloadCase?.[payloadCase] ?? stream
  }

  private filterEvents(events: BronzeEvent[]): BronzeEvent[] {
    if (!this.allowedPayloadCases) {
      return events
    }

    return events.filter((event) => this.allowedPayloadCases!.has(event.payloadCase))
  }
}
