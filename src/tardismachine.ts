import findMyWay from 'find-my-way'
import http from 'http'
import { clearCache, init } from 'tardis-dev'
import { App, DISABLED, TemplatedApp } from 'uWebSockets.js'
import { replayHttp, createReplayNormalizedHttpHandler, healthCheck } from './http'
import { createReplayNormalizedWSHandler, replayWS, createStreamNormalizedWSHandler } from './ws'
import { debug } from './debug'
import { KafkaEventBus, RabbitMQEventBus, KinesisEventBus, NatsEventBus } from './eventbus'
import type { EventBusConfig, NormalizedEventSink, NormalizedMessage, PublishFn, PublishInjection, PublishMeta } from './eventbus/types'

const pkg = require('../package.json')

export class TardisMachine {
  private readonly _httpServer: http.Server
  private readonly _wsServer: TemplatedApp
  private _eventLoopTimerId: NodeJS.Timeout | undefined = undefined
  private readonly _eventBus?: NormalizedEventSink
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
      this._publishNormalized = this._createPublishFunction(this._eventBus)
    }

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

  public async stop() {
    await new Promise<void>((resolve, reject) => {
      this._httpServer.close((err) => {
        err ? reject(err) : resolve()
      })
    })

    if (this._eventLoopTimerId !== undefined) {
      clearInterval(this._eventLoopTimerId)
    }

    if (this._eventBus) {
      await this._eventBus.close()
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

    throw new Error(`Unsupported event bus provider: ${(config as any).provider}`)
  }

  private _createPublishFunction(bus: NormalizedEventSink): PublishFn {
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

      bus.publish(message, publishMeta).catch((error) => {
        debug('Event bus publish error: %o', error)
      })
    }
  }
}

type Options = {
  apiKey?: string
  cacheDir: string
  clearCache?: boolean
  eventBus?: EventBusConfig
}
