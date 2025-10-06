import findMyWay from 'find-my-way'
import http from 'http'
import { clearCache, init } from 'tardis-dev'
import { App, DISABLED, TemplatedApp } from 'uWebSockets.js'
import { replayHttp, createReplayNormalizedHttpHandler, healthCheck } from './http'
import { createReplayNormalizedWSHandler, replayWS, createStreamNormalizedWSHandler } from './ws'
import { debug } from './debug'
import {
  KafkaEventBus,
  SilverKafkaEventBus,
  RabbitMQEventBus,
  KinesisEventBus,
  NatsEventBus,
  RedisEventBus,
  SQSEventBus,
  PulsarEventBus,
  AzureEventHubsEventBus,
  PubSubEventBus,
  SilverPubSubEventBus,
  SilverPulsarEventBus,
  SilverSQSEventBus,
  SilverRabbitMQEventBus,
  SilverKinesisEventBus,
  SilverNatsEventBus,
  SilverRedisEventBus,
  SilverAzureEventBus
} from './eventbus'
import type {
  EventBusConfig,
  NormalizedEventSink,
  SilverEventSink,
  NormalizedMessage,
  PublishFn,
  PublishInjection,
  PublishMeta
} from './eventbus/types'

const pkg = require('../package.json')

export class TardisMachine {
  private readonly _httpServer: http.Server
  private readonly _wsServer: TemplatedApp
  private _eventLoopTimerId: NodeJS.Timeout | undefined = undefined
  private readonly _eventBus?: NormalizedEventSink
  private readonly _silverEventBus?: SilverEventSink
  private readonly _publishNormalized?: PublishFn
  private readonly _sourceTag = `tardis-machine/${pkg.version}`

  constructor(private readonly options: Options) {
    init({
      apiKey: options.apiKey,
      cacheDir: options.cacheDir,
      _userAgent: `tardis-machine/${pkg.version} (+https://github.com/tardis-dev/tardis-machine)`
    })

    const router = findMyWay({ ignoreTrailingSlash: true })

    if (options.eventBus) {
      this._eventBus = this._createEventBus(options.eventBus)
    }

    if (options.silverEventBus) {
      this._silverEventBus = this._createSilverEventBus(options.silverEventBus)
    }

    this._publishNormalized = this._createPublishFunction()

    this._httpServer = http.createServer((req, res) => {
      router.lookup(req, res)
    })

    // set timeout to 0 meaning infinite http timout - streaming may take some time expecially for longer date ranges
    this._httpServer.timeout = 0

    router.on('GET', '/replay', replayHttp)
    router.on('GET', '/replay-normalized', createReplayNormalizedHttpHandler(this._publishNormalized))
    router.on('GET', '/health-check', healthCheck)

    const wsRoutes = {
      '/ws-replay': replayWS,
      '/ws-replay-normalized': createReplayNormalizedWSHandler(this._publishNormalized),
      '/ws-stream-normalized': createStreamNormalizedWSHandler(this._publishNormalized)
    } as any

    this._wsServer = App().ws('/*', {
      compression: DISABLED,
      maxPayloadLength: 512 * 1024,
      idleTimeout: 60,
      maxBackpressure: 5 * 1024 * 1024,
      closeOnBackpressureLimit: true,
      upgrade: (res: any, req: any, context: any) => {
        res.upgrade(
          { req },
          req.getHeader('sec-websocket-key'),
          req.getHeader('sec-websocket-protocol'),
          req.getHeader('sec-websocket-extensions'),
          context
        )
      },
      open: (ws: any) => {
        const path = ws.req.getUrl().toLocaleLowerCase()
        ws.closed = false
        const matchingRoute = wsRoutes[path]

        if (matchingRoute !== undefined) {
          matchingRoute(ws, ws.req)
        } else {
          ws.end(1008)
        }
      },

      message: (ws: any, message: ArrayBuffer) => {
        if (ws.onmessage !== undefined) {
          ws.onmessage(message)
        }
      },

      close: (ws: any) => {
        ws.closed = true
        if (ws.onclose !== undefined) {
          ws.onclose()
        }
      }
    } as any)
  }

  public async start(port: number) {
    if (this._eventBus) {
      await this._eventBus.start()
    }

    if (this._silverEventBus) {
      await this._silverEventBus.start()
    }

    let start = process.hrtime()
    const interval = 500

    // based on https://github.com/tj/node-blocked/blob/master/index.js
    this._eventLoopTimerId = setInterval(() => {
      const delta = process.hrtime(start)
      const nanosec = delta[0] * 1e9 + delta[1]
      const ms = nanosec / 1e6
      const n = ms - interval

      if (n > 2000) {
        debug('Tardis-machine server event loop blocked for %d ms.', Math.round(n))
      }

      start = process.hrtime()
    }, interval)

    if (this.options.clearCache) {
      await clearCache()
    }

    await new Promise<void>((resolve, reject) => {
      try {
        this._httpServer.on('error', reject)
        this._httpServer.listen(port, () => {
          this._wsServer.listen(port + 1, (listenSocket) => {
            if (listenSocket) {
              resolve()
            } else {
              reject(new Error('ws server did not start'))
            }
          })
        })
      } catch (e) {
        reject(e)
      }
    })
  }

  public async stop(): Promise<void> {
    if (this._eventLoopTimerId !== undefined) {
      clearInterval(this._eventLoopTimerId)
      this._eventLoopTimerId = undefined
    }

    this._wsServer.close()

    await new Promise<void>((resolve, reject) => {
      this._httpServer.close((err) => {
        if (err) reject(err)
        else resolve()
      })
    })

    // Wait a bit for ports to be released
    await new Promise((resolve) => setTimeout(resolve, 5000))

    if (this._eventBus) {
      await this._eventBus.close().catch(() => undefined)
    }

    if (this._silverEventBus) {
      await this._silverEventBus.close().catch(() => undefined)
    }
  }

  private _createEventBus(config: EventBusConfig): NormalizedEventSink {
    if (config.provider === 'kafka') {
      return new KafkaEventBus(config)
    }
    if (config.provider === 'rabbitmq') {
      return new RabbitMQEventBus(config)
    }
    if (config.provider === 'kinesis') {
      return new KinesisEventBus(config)
    }
    if (config.provider === 'nats') {
      return new NatsEventBus(config)
    }
    if (config.provider === 'redis') {
      return new RedisEventBus(config)
    }
    if (config.provider === 'sqs') {
      return new SQSEventBus(config)
    }
    if (config.provider === 'pulsar') {
      return new PulsarEventBus(config)
    }
    if (config.provider === 'azure-event-hubs') {
      return new AzureEventHubsEventBus(config)
    }
    if (config.provider === 'pubsub') {
      return new PubSubEventBus(config)
    }

    throw new Error(`Unsupported event bus provider: ${(config as any).provider}`)
  }

  private _createSilverEventBus(config: EventBusConfig): SilverEventSink {
    if (config.provider === 'kafka-silver') {
      return new SilverKafkaEventBus(config)
    }
    if (config.provider === 'rabbitmq-silver') {
      return new SilverRabbitMQEventBus(config)
    }
    if (config.provider === 'kinesis-silver') {
      return new SilverKinesisEventBus(config)
    }
    if (config.provider === 'nats-silver') {
      return new SilverNatsEventBus(config)
    }
    if (config.provider === 'redis-silver') {
      return new SilverRedisEventBus(config)
    }
    if (config.provider === 'pulsar-silver') {
      return new SilverPulsarEventBus(config)
    }
    if (config.provider === 'sqs-silver') {
      return new SilverSQSEventBus(config)
    }
    if (config.provider === 'pubsub-silver') {
      return new SilverPubSubEventBus(config)
    }
    if (config.provider === 'azure-event-hubs-silver') {
      return new SilverAzureEventBus(config)
    }

    throw new Error(`Unsupported silver event bus provider: ${(config as any).provider}`)
  }

  private _createPublishFunction(): PublishFn | undefined {
    if (!this._eventBus && !this._silverEventBus) {
      return undefined
    }

    return (message: NormalizedMessage, meta: PublishInjection) => {
      const publishMeta: PublishMeta = {
        source: this._sourceTag,
        origin: meta.origin,
        ingestTimestamp: new Date(),
        requestId: meta.requestId,
        sessionId: meta.sessionId,
        extraMeta: {
          app_version: pkg.version,
          ...meta.extraMeta
        }
      }

      if (this._eventBus) {
        this._eventBus.publish(message, publishMeta).catch((error) => {
          debug('Bronze event bus publish error: %o', error)
        })
      }

      if (this._silverEventBus) {
        this._silverEventBus.publish(message, publishMeta).catch((error) => {
          debug('Silver event bus publish error: %o', error)
        })
      }
    }
  }
}

type Options = {
  apiKey?: string
  cacheDir: string
  clearCache?: boolean
  eventBus?: EventBusConfig
  silverEventBus?: EventBusConfig
}
