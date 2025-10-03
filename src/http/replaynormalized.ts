import { once } from 'events'
import { IncomingMessage, OutgoingMessage, ServerResponse } from 'http'
import { combine, compute, replayNormalized } from 'tardis-dev'
import url from 'url'
import { randomUUID } from 'crypto'
import { debug } from '../debug'
import { constructDataTypeFilter, getComputables, getNormalizers, ReplayNormalizedRequestOptions } from '../helpers'
import { Origin } from '../generated/lakehouse/bronze/v1/normalized_event_pb'
import type { PublishFn, NormalizedMessage } from '../eventbus/types'

export const createReplayNormalizedHttpHandler = (publishNormalized?: PublishFn) =>
  async (req: IncomingMessage, res: ServerResponse) => {
    try {
      const startTimestamp = new Date().getTime()
      const parsedQuery = url.parse(req.url!, true).query
      const optionsString = parsedQuery['options'] as string
      const replayNormalizedOptions = JSON.parse(optionsString) as ReplayNormalizedRequestOptions

      debug('GET /replay-normalized request started, options: %o', replayNormalizedOptions)

      const requestId = randomUUID()
      const streamedMessagesCount = await writeMessagesToResponse(
        res,
        replayNormalizedOptions,
        publishNormalized,
        requestId
      )
      const endTimestamp = new Date().getTime()

      debug(
        'GET /replay-normalized request finished, options: %o, time: %d seconds, total messages count: %d',
        replayNormalizedOptions,
        (endTimestamp - startTimestamp) / 1000,
        streamedMessagesCount
      )
    } catch (e: any) {
      const errorInfo = {
        responseText: e.responseText,
        message: e.message,
        url: e.url
      }

      debug('GET /replay-normalized request error: %o', e)
      console.error('GET /replay-normalized request error:', e)

      if (!res.finished) {
        res.statusCode = e.status || 500
        res.end(JSON.stringify(errorInfo))
      }
    }
  }

async function writeMessagesToResponse(
  res: OutgoingMessage,
  options: ReplayNormalizedRequestOptions,
  publishNormalized: PublishFn | undefined,
  requestId: string
) {
  const BATCH_SIZE = 32

  res.setHeader('Content-Type', 'application/x-json-stream')

  let buffers: string[] = []
  let totalMessagesCount = 0

  const replayNormalizedOptions = Array.isArray(options) ? options : [options]

  const messagesIterables = replayNormalizedOptions.map((option) => {
    const messages = replayNormalized(option, ...getNormalizers(option.dataTypes))
    const computables = getComputables(option.dataTypes)

    if (computables.length > 0) {
      return compute(messages, ...computables)
    }

    return messages
  })

  const filterByDataType = constructDataTypeFilter(replayNormalizedOptions)
  const messages = messagesIterables.length === 1 ? messagesIterables[0] : combine(...messagesIterables)

  for await (const message of messages) {
    if (filterByDataType(message) === false) {
      continue
    }

    totalMessagesCount++

    if (publishNormalized) {
      publishSafe(publishNormalized, message, {
        origin: Origin.REPLAY,
        requestId,
        extraMeta: {
          transport: 'http',
          route: '/replay-normalized'
        }
      })
    }

    buffers.push(JSON.stringify(message))

    if (buffers.length === BATCH_SIZE) {
      const ok = res.write(`${buffers.join('\n')}\n`)
      buffers = []

      if (!ok) {
        await once(res, 'drain')
      }
    }
  }

  if (buffers.length > 0) {
    res.write(`${buffers.join('\n')}\n`)
    buffers = []
  }

  res.end('')

  return totalMessagesCount
}

function publishSafe(publish: PublishFn, message: NormalizedMessage, meta: Parameters<PublishFn>[1]) {
  try {
    publish(message, meta)
  } catch (error) {
    debug('Failed to enqueue normalized event for publishing %o', error)
  }
}
