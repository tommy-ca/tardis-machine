import { connect, type MqttClient } from 'mqtt'
import { BronzeNormalizedEventEncoder } from './bronzeMapper'
import { compileKeyBuilder } from './keyTemplate'
import type { BronzeEvent, BronzePayloadCase, MQTTEventBusConfig, NormalizedMessage, PublishMeta } from './types'
import { BaseBronzeEventBusPublisher, CommonBronzeConfig } from './baseBronzePublisher'
import { debug } from '../debug'

const log = debug.extend('eventbus')

type QoS = 0 | 1 | 2

export class MQTTEventBus extends BaseBronzeEventBusPublisher {
  private readonly encoder: BronzeNormalizedEventEncoder
  private client?: MqttClient
  private readonly qos: QoS
  private readonly retain: boolean
  private readonly topic: string
  private readonly topicByPayloadCase?: Record<string, string>
  private readonly staticUserProperties?: Record<string, string>
  private readonly topicTemplate?: string

  constructor(private readonly config: MQTTEventBusConfig) {
    const commonConfig: CommonBronzeConfig = {
      maxBatchSize: config.maxBatchSize,
      maxBatchDelayMs: config.maxBatchDelayMs,
      includePayloadCases: config.includePayloadCases
    }
    super(commonConfig)
    const keyBuilder = config.topicTemplate ? compileKeyBuilder(config.topicTemplate) : undefined
    this.encoder = new BronzeNormalizedEventEncoder(keyBuilder)
    this.qos = config.qos ?? 0
    this.retain = config.retain ?? false
    this.topic = config.topic
    this.topicByPayloadCase = config.topicByPayloadCase
    this.staticUserProperties = config.staticUserProperties
    this.topicTemplate = config.topicTemplate
  }

  async start() {
    return new Promise<void>((resolve, reject) => {
      const options: any = {
        clientId: this.config.clientId ?? `tardis-machine-${Date.now()}`,
        username: this.config.username,
        password: this.config.password,
        clean: true
      }

      this.client = connect(this.config.url, options)

      this.client.on('connect', () => {
        log('MQTT connected')
        resolve()
      })

      this.client.on('error', (err: Error) => {
        log('MQTT connection error', err)
        reject(err)
      })
    })
  }

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): BronzeEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: BronzeEvent[]): Promise<void> {
    if (!this.client) {
      throw new Error('MQTT client not connected')
    }

    const promises = batch.map(async (event) => {
      const topic = this.resolveTopic(event.payloadCase)
      const payload = Buffer.from(event.binary)

      const userProperties: Record<string, string> = {
        payloadCase: event.payloadCase,
        dataType: event.dataType,
        ...event.meta
      }

      if (this.staticUserProperties) {
        Object.assign(userProperties, this.staticUserProperties)
      }

      return new Promise<void>((resolve, reject) => {
        this.client!.publish(
          topic,
          payload,
          {
            qos: this.qos,
            retain: this.retain,
            properties: {
              userProperties
            }
          },
          (err?: Error) => {
            if (err) {
              reject(err)
            } else {
              resolve()
            }
          }
        )
      })
    })

    await Promise.all(promises)
  }

  protected async doClose(): Promise<void> {
    if (this.client) {
      return new Promise<void>((resolve) => {
        this.client!.end(false, {}, () => {
          resolve()
        })
      })
    }
  }

  private resolveTopic(payloadCase: BronzePayloadCase): string {
    return this.topicByPayloadCase?.[payloadCase] ?? this.topic
  }
}
