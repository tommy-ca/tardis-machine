import type { BronzeEvent, BronzePayloadCase, NormalizedEventSink, NormalizedMessage, PublishMeta } from './types'
import { wait } from '../helpers'
import { debug } from '../debug'

const log = debug.extend('eventbus')

const DEFAULT_BATCH_SIZE = 256
const DEFAULT_BATCH_DELAY_MS = 25
const MAX_RETRY_ATTEMPTS = 3

export interface CommonBronzeConfig {
  maxBatchSize?: number
  maxBatchDelayMs?: number
  includePayloadCases?: BronzePayloadCase[]
}

export abstract class BaseBronzeEventBusPublisher implements NormalizedEventSink {
  protected readonly buffer: BronzeEvent[] = []
  protected flushTimer?: NodeJS.Timeout
  protected sendingPromise: Promise<void> = Promise.resolve()
  protected closed = false
  protected readonly allowedPayloadCases?: Set<BronzePayloadCase>
  protected readonly maxBatchSize: number
  protected readonly maxBatchDelayMs: number

  constructor(config: CommonBronzeConfig) {
    this.maxBatchSize = config.maxBatchSize ?? DEFAULT_BATCH_SIZE
    this.maxBatchDelayMs = config.maxBatchDelayMs ?? DEFAULT_BATCH_DELAY_MS
    if (config.includePayloadCases) {
      this.allowedPayloadCases = new Set(config.includePayloadCases)
    }
  }

  async start(): Promise<void> {
    // Subclasses can override for initialization
  }

  async publish(message: NormalizedMessage, meta: PublishMeta): Promise<void> {
    if (this.closed) {
      return
    }

    const events = this.filterEvents(this.encodeEvents(message, meta))
    if (events.length === 0) {
      return
    }

    this.buffer.push(...events)

    if (this.buffer.length >= this.maxBatchSize) {
      this.flushImmediately()
    } else {
      this.scheduleFlush()
    }
  }

  protected abstract encodeEvents(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[]

  private scheduleFlush() {
    if (this.flushTimer) {
      return
    }

    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined
      this.flushImmediately()
    }, this.maxBatchDelayMs)
  }

  private flushImmediately() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }

    if (this.buffer.length === 0) {
      return
    }

    const batches: BronzeEvent[][] = []

    while (this.buffer.length > 0) {
      const chunk = this.buffer.splice(0, this.maxBatchSize)
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
            await this.sendBatchWithRetry(batch)
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
        log('Failed to send batch: %o', error)
        // try again after short delay
        queueMicrotask(() => this.scheduleFlush())
      })
  }

  private async sendBatchWithRetry(batch: BronzeEvent[]): Promise<void> {
    let attempt = 0
    while (attempt < MAX_RETRY_ATTEMPTS) {
      attempt++
      try {
        await this.sendBatch(batch)
        return
      } catch (error) {
        log('Send attempt %d failed: %o', attempt, error)
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

  protected abstract sendBatch(batch: BronzeEvent[]): Promise<void>

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
    await this.doClose()
  }

  protected abstract doClose(): Promise<void>

  protected filterEvents(events: BronzeEvent[]): BronzeEvent[] {
    if (!this.allowedPayloadCases) {
      return events
    }

    return events.filter((event) => this.allowedPayloadCases!.has(event.payloadCase))
  }
}
