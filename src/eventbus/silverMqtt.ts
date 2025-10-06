import { connect, type MqttClient } from 'mqtt'
import { SilverNormalizedEventEncoder } from './silverMapper'
import { compileSilverKeyBuilder } from './keyTemplate'
import type { SilverEvent, SilverRecordType, SilverMQTTEventBusConfig, NormalizedMessage, PublishMeta } from './types'
import { BaseSilverEventBusPublisher, CommonSilverConfig } from './baseSilverPublisher'
import { debug } from '../debug'

const log = debug.extend('eventbus')

type QoS = 0 | 1 | 2

export class SilverMQTTEventBus extends BaseSilverEventBusPublisher {
  private readonly encoder: SilverNormalizedEventEncoder
  private client?: MqttClient
  private readonly qos: QoS
  private readonly retain: boolean
  private readonly topic: string
  private readonly topicByRecordType?: Record<string, string>
  private readonly staticUserProperties?: Record<string, string>
  private readonly topicTemplate?: string

  constructor(private readonly config: SilverMQTTEventBusConfig) {
    const commonConfig: CommonSilverConfig = {
      maxBatchSize: config.maxBatchSize,
      maxBatchDelayMs: config.maxBatchDelayMs,
      includeRecordTypes: config.includeRecordTypes
    }
    super(commonConfig)
    const keyBuilder = config.topicTemplate ? compileSilverKeyBuilder(config.topicTemplate) : undefined
    this.encoder = new SilverNormalizedEventEncoder(keyBuilder)
    this.qos = config.qos ?? 0
    this.retain = config.retain ?? false
    this.topic = config.topic
    this.topicByRecordType = config.topicByRecordType
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
        log('Silver MQTT connected')
        resolve()
      })

      this.client.on('error', (err: Error) => {
        log('Silver MQTT connection error', err)
        reject(err)
      })
    })
  }

  protected encodeEvents(message: NormalizedMessage, meta: PublishMeta): SilverEvent[] {
    return this.encoder.encode(message, meta)
  }

  protected async sendBatch(batch: SilverEvent[]): Promise<void> {
    if (!this.client) {
      throw new Error('Silver MQTT client not connected')
    }

    const promises = batch.map(async (event) => {
      const topic = this.resolveTopic(event.recordType)
      const payload = Buffer.from(event.binary)

      const userProperties: Record<string, string> = {
        recordType: event.recordType,
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

  private resolveTopic(recordType: SilverRecordType): string {
    return this.topicByRecordType?.[recordType] ?? this.topic
  }
}
