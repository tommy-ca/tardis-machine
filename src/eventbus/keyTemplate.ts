import type { KeyBuilder } from './bronzeMapper'
import { Origin } from '../generated/lakehouse/bronze/v1/normalized_event_pb'

type Accessor = (event: Parameters<KeyBuilder>[0], payloadCase: Parameters<KeyBuilder>[1], dataType: string) => string

type Segment = string | Accessor

const PLACEHOLDER_PATTERN = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g
const META_PREFIX = 'meta.'
const META_KEY_PATTERN = /^[a-zA-Z0-9_]+$/

const BASE_ACCESSORS: Record<string, Accessor> = {
  exchange: (event) => event.exchange ?? '',
  symbol: (event) => event.symbol ?? '',
  payloadCase: (_event, payloadCase) => payloadCase,
  dataType: (_event, _payloadCase, dataType) => dataType ?? '',
  source: (event) => event.source ?? '',
  origin: (event) => originToString(event.origin)
}

export function compileKeyBuilder(template: string): KeyBuilder {
  if (typeof template !== 'string') {
    throw new Error('kafka-key-template must be a non-empty string.')
  }

  if (template.trim() === '') {
    throw new Error('kafka-key-template must be a non-empty string.')
  }

  const segments = parseTemplate(template)

  return (event, payloadCase, dataType) =>
    segments.map((segment) => (typeof segment === 'string' ? segment : segment(event, payloadCase, dataType))).join('')
}

function parseTemplate(template: string): Segment[] {
  const segments: Segment[] = []
  let lastIndex = 0

  PLACEHOLDER_PATTERN.lastIndex = 0

  let match: RegExpExecArray | null
  while ((match = PLACEHOLDER_PATTERN.exec(template)) !== null) {
    if (match.index > lastIndex) {
      segments.push(template.slice(lastIndex, match.index))
    }

    const token = match[1].trim()
    segments.push(resolveAccessor(token))

    lastIndex = match.index + match[0].length
  }

  if (lastIndex < template.length) {
    segments.push(template.slice(lastIndex))
  }

  if (segments.length === 0) {
    return ['']
  }

  return segments
}

function resolveAccessor(token: string): Accessor {
  if (!token) {
    throw new Error('kafka-key-template placeholders cannot be empty.')
  }

  const baseAccessor = BASE_ACCESSORS[token]
  if (baseAccessor) {
    return baseAccessor
  }

  if (token.startsWith(META_PREFIX)) {
    const key = token.slice(META_PREFIX.length)
    if (!META_KEY_PATTERN.test(key)) {
      throw new Error(`Invalid kafka-key-template meta placeholder "{{${token}}}".`)
    }
    return (event) => event.meta?.[key] ?? ''
  }

  throw new Error(`Unknown kafka-key-template placeholder "{{${token}}}".`)
}

function originToString(origin: Origin | undefined): string {
  switch (origin) {
    case Origin.REALTIME:
      return 'realtime'
    case Origin.REPLAY:
      return 'replay'
    case Origin.ARCHIVE:
      return 'archive'
    case Origin.UNSPECIFIED:
    default:
      return 'unspecified'
  }
}
