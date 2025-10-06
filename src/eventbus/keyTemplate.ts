import type { KeyBuilder } from './bronzeMapper'
import type { SilverKeyBuilder } from './silverMapper'
import { Origin } from '../generated/lakehouse/bronze/v1/normalized_event_pb'

type Accessor = (event: Parameters<KeyBuilder>[0], payloadCase: Parameters<KeyBuilder>[1], dataType: string) => string

type Segment = string | Accessor

type SilverAccessor = (record: Parameters<SilverKeyBuilder>[0], recordType: Parameters<SilverKeyBuilder>[1], dataType: string) => string

type SilverSegment = string | SilverAccessor

const PLACEHOLDER_PATTERN = /{{\s*([a-zA-Z0-9_.-]+)\s*}}/g
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

const BASE_SILVER_ACCESSORS: Record<string, SilverAccessor> = {
  exchange: (record) => record.exchange ?? '',
  symbol: (record) => record.symbol ?? '',
  recordType: (_record, recordType) => recordType,
  dataType: (_record, _recordType, dataType) => dataType ?? '',
  origin: (record) => originToString(record.origin)
}

export function compileKeyBuilder(template: string, prefix = 'kafka'): KeyBuilder {
  if (typeof template !== 'string') {
    throw new Error(`${prefix}-key-template must be a non-empty string.`)
  }

  if (template.trim() === '') {
    throw new Error(`${prefix}-key-template must be a non-empty string.`)
  }

  const segments = parseTemplate(template, prefix)

  return (event, payloadCase, dataType) =>
    segments.map((segment) => (typeof segment === 'string' ? segment : segment(event, payloadCase, dataType))).join('')
}

export function compileSilverKeyBuilder(template: string, prefix = 'kafka-silver'): SilverKeyBuilder {
  if (typeof template !== 'string') {
    throw new Error(`${prefix}-key-template must be a non-empty string.`)
  }

  if (template.trim() === '') {
    throw new Error(`${prefix}-key-template must be a non-empty string.`)
  }

  const segments = parseSilverTemplate(template, prefix)

  return (record, recordType, dataType) =>
    segments.map((segment) => (typeof segment === 'string' ? segment : segment(record, recordType, dataType))).join('')
}

function parseTemplate(template: string, prefix = 'kafka'): Segment[] {
  const segments: Segment[] = []
  let lastIndex = 0

  PLACEHOLDER_PATTERN.lastIndex = 0

  let match: RegExpExecArray | null
  while ((match = PLACEHOLDER_PATTERN.exec(template)) !== null) {
    if (match.index > lastIndex) {
      segments.push(template.slice(lastIndex, match.index))
    }

    const token = match[1].trim()
    segments.push(resolveAccessor(token, prefix))

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

function parseSilverTemplate(template: string, prefix = 'kafka-silver'): SilverSegment[] {
  const segments: SilverSegment[] = []
  let lastIndex = 0

  PLACEHOLDER_PATTERN.lastIndex = 0

  let match: RegExpExecArray | null
  while ((match = PLACEHOLDER_PATTERN.exec(template)) !== null) {
    if (match.index > lastIndex) {
      segments.push(template.slice(lastIndex, match.index))
    }

    const token = match[1].trim()
    segments.push(resolveSilverAccessor(token, prefix))

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

function resolveAccessor(token: string, prefix = 'kafka'): Accessor {
  if (!token) {
    throw new Error(`${prefix}-key-template placeholders cannot be empty.`)
  }

  const baseAccessor = BASE_ACCESSORS[token]
  if (baseAccessor) {
    return baseAccessor
  }

  if (token.startsWith(META_PREFIX)) {
    const key = token.slice(META_PREFIX.length)
    if (!META_KEY_PATTERN.test(key)) {
      throw new Error(`Invalid ${prefix}-key-template meta placeholder "{{${token}}}".`)
    }
    return (event) => event.meta?.[key] ?? ''
  }

  throw new Error(`Unknown ${prefix}-key-template placeholder "{{${token}}}".`)
}

function resolveSilverAccessor(token: string, prefix = 'kafka-silver'): SilverAccessor {
  if (!token) {
    throw new Error(`${prefix}-key-template placeholders cannot be empty.`)
  }

  const baseAccessor = BASE_SILVER_ACCESSORS[token]
  if (baseAccessor) {
    return baseAccessor
  }

  if (token.startsWith(META_PREFIX)) {
    const key = token.slice(META_PREFIX.length)
    if (!META_KEY_PATTERN.test(key)) {
      throw new Error(`Invalid ${prefix}-key-template meta placeholder "{{${token}}}".`)
    }
    return (record) => record.meta?.[key] ?? ''
  }

  throw new Error(`Unknown ${prefix}-key-template placeholder "{{${token}}}".`)
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
