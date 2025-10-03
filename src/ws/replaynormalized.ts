import qs from 'querystring'
import { combine, compute, replayNormalized } from 'tardis-dev'
import { HttpRequest } from 'uWebSockets.js'
import { randomUUID } from 'crypto'
import { debug } from '../debug'
import { constructDataTypeFilter, getComputables, getNormalizers, ReplayNormalizedRequestOptions, wait } from '../helpers'
import { Origin } from '../generated/lakehouse/bronze/v1/normalized_event_pb'
import type { PublishFn } from '../eventbus/types'

export function createReplayNormalizedWSHandler(publishNormalized?: PublishFn) {
  return async function replayNormalizedWS(ws: any, req: HttpRequest) {
    let messages: AsyncIterableIterator<any> | undefined
    try {
      const startTimestamp = new Date().getTime()
      const parsedQuery = qs.decode(req.getQuery())
      const optionsString = parsedQuery['options'] as string
      const replayNormalizedOptions = JSON.parse(optionsString) as ReplayNormalizedRequestOptions

      debug('WebSocket /ws-replay-normalized started, options: %o', replayNormalizedOptions)

      const requestId = randomUUID()
      const sessionId = randomUUID()

      const options = Array.isArray(replayNormalizedOptions) ? replayNormalizedOptions : [replayNormalizedOptions]

      const messagesIterables = options.map((option) => {
        const messages = replayNormalized(option, ...getNormalizers(option.dataTypes))
        const computables = getComputables(option.dataTypes)

        if (computables.length > 0) {
          return compute(messages, ...computables)
        }

        return messages
      })

      const filterByDataType = constructDataTypeFilter(options)

      messages = messagesIterables.length === 1 ? messagesIterables[0] : combine(...messagesIterables)

      for await (const message of messages) {
        if (!filterByDataType(message)) {
          continue
        }

        if (publishNormalized) {
          publishSafe(publishNormalized, message, {
            origin: Origin.REPLAY,
            requestId,
            sessionId,
            extraMeta: {
              transport: 'ws',
              route: '/ws-replay-normalized'
            }
          })
        }

        const success = ws.send(JSON.stringify(message))
        if (!success) {
          while (ws.getBufferedAmount() > 0) {
            await wait(1)
          }
        }
      }

      while (ws.getBufferedAmount() > 0) {
        await wait(100)
      }

      ws.end(1000, 'WS replay-normalized finished')

      const endTimestamp = new Date().getTime()

      debug(
        'WebSocket /ws-replay-normalized finished, options: %o, time: %d seconds',
        replayNormalizedOptions,
        (endTimestamp - startTimestamp) / 1000
      )
    } catch (e: any) {
      if (messages !== undefined) {
        messages!.return!()
      }
      if (!ws.closed) {
        ws.end(1011, e.toString())
      }

      debug('WebSocket /ws-replay-normalized  error: %o', e)
      console.error('WebSocket /ws-replay-normalized error:', e)
    }
  }
}

function publishSafe(publish: PublishFn, message: any, meta: Parameters<PublishFn>[1]) {
  try {
    publish(message, meta)
  } catch (error) {
    debug('Failed to enqueue normalized event for publishing %o', error)
  }
}
