import type { BronzePayloadCase, EventBusConfig, KafkaEventBusConfig } from './types'
import { compileKeyBuilder } from './keyTemplate'

const ALLOWED_COMPRESSION = new Set(['none', 'gzip', 'snappy', 'lz4', 'zstd'])
const ALLOWED_PAYLOAD_CASES: ReadonlySet<BronzePayloadCase> = new Set([
  'trade',
  'bookChange',
  'bookSnapshot',
  'groupedBookSnapshot',
  'quote',
  'derivativeTicker',
  'liquidation',
  'optionSummary',
  'bookTicker',
  'tradeBar',
  'error',
  'disconnect'
])

const ACK_VALUE_MAP: Record<string, -1 | 0 | 1> = {
  all: -1,
  '-1': -1,
  '1': 1,
  leader: 1,
  '0': 0,
  none: 0
}

function parseKafkaBrokers(raw: unknown): string[] {
  if (typeof raw !== 'string') {
    return []
  }

  return raw
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean)
}

function parseCompression(raw: unknown): KafkaEventBusConfig['compression'] | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error('kafka-compression must be one of none,gzip,snappy,lz4,zstd.')
  }

  const value = raw.trim().toLowerCase()
  if (!ALLOWED_COMPRESSION.has(value)) {
    throw new Error('kafka-compression must be one of none,gzip,snappy,lz4,zstd.')
  }

  return value as KafkaEventBusConfig['compression']
}

function parseTopicRouting(raw: unknown): KafkaEventBusConfig['topicByPayloadCase'] | undefined {
  if (raw === undefined) {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error('kafka-topic-routing must be a string of payloadCase:topic entries separated by commas.')
  }

  const map: Partial<Record<BronzePayloadCase, string>> = {}
  const invalidPayloadCases = new Set<string>()

  const pairs = raw
    .split(',')
    .map((pair) => pair.trim())
    .filter(Boolean)

  for (const pair of pairs) {
    const [payloadCase, mappedTopic] = pair.split(':').map((part) => part?.trim())
    if (!payloadCase || !mappedTopic) {
      throw new Error(`Invalid kafka-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
    }

    if (!ALLOWED_PAYLOAD_CASES.has(payloadCase as BronzePayloadCase)) {
      invalidPayloadCases.add(payloadCase)
      continue
    }

    map[payloadCase as BronzePayloadCase] = mappedTopic
  }

  if (Object.keys(map).length === 0) {
    throw new Error('kafka-topic-routing must define at least one payloadCase mapping.')
  }

  if (invalidPayloadCases.size > 0) {
    throw new Error(
      `Unknown payload case(s) for kafka-topic-routing: ${Array.from(invalidPayloadCases).join(', ')}.`
    )
  }

  return map
}

function parseIncludePayloadCases(raw: unknown): BronzePayloadCase[] | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error('kafka-include-payloads must be a comma separated string list of payload cases.')
  }

  const cases = Array.from(
    new Set(
      raw
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean)
    )
  ) as BronzePayloadCase[]

  if (cases.length === 0) {
    throw new Error('kafka-include-payloads must list at least one payload case.')
  }

  const invalid = cases.filter((payloadCase) => !ALLOWED_PAYLOAD_CASES.has(payloadCase))
  if (invalid.length > 0) {
    throw new Error(`Unknown payload case(s) for kafka-include-payloads: ${invalid.join(', ')}.`)
  }

  return cases
}

function parseBooleanOption(value: unknown, optionName: string): boolean | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined
  }

  if (typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (normalized === 'true' || normalized === '1') {
      return true
    }
    if (normalized === 'false' || normalized === '0') {
      return false
    }
  }

  throw new Error(`${optionName} must be a boolean value.`)
}

function parseAcks(raw: unknown): KafkaEventBusConfig['acks'] | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw === 'number') {
    if (raw === -1 || raw === 0 || raw === 1) {
      return raw
    }
    throw new Error('kafka-acks must be one of all,leader,none,1,0,-1.')
  }

  if (typeof raw === 'string') {
    const normalized = raw.trim().toLowerCase()
    const mapped = ACK_VALUE_MAP[normalized]
    if (mapped !== undefined) {
      return mapped
    }
    throw new Error('kafka-acks must be one of all,leader,none,1,0,-1.')
  }

  throw new Error('kafka-acks must be one of all,leader,none,1,0,-1.')
}

function parseSasl(argv: Record<string, any>): KafkaEventBusConfig['sasl'] | undefined {
  const mechanism = argv['kafka-sasl-mechanism']
  if (!mechanism) {
    return undefined
  }

  const username = argv['kafka-sasl-username']
  const password = argv['kafka-sasl-password']

  if (!username || !password) {
    throw new Error('Kafka SASL username and password must be provided when sasl mechanism is set.')
  }

  return {
    mechanism,
    username,
    password
  }
}

function parsePositiveInteger(value: unknown, optionName: string): number | undefined {
  if (value === undefined || value === null || value === '') {
    return undefined
  }

  const numeric = typeof value === 'number' ? value : Number(value)

  if (!Number.isFinite(numeric) || numeric <= 0 || !Number.isInteger(numeric)) {
    throw new Error(`${optionName} must be a positive integer.`)
  }

  return numeric
}

export function parseKafkaEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const brokersRaw = argv['kafka-brokers']
  const brokers = parseKafkaBrokers(brokersRaw)
  const topic = argv['kafka-topic']

  if (!brokersRaw || !topic) {
    return undefined
  }

  if (brokers.length === 0) {
    throw new Error('Invalid kafka-brokers value. Provide at least one broker URL.')
  }

  const kafkaConfig: KafkaEventBusConfig = {
    brokers,
    topic,
    clientId: argv['kafka-client-id'] || 'tardis-machine-publisher',
    ssl: Boolean(argv['kafka-ssl'])
  }

  const topicRouting = parseTopicRouting(argv['kafka-topic-routing'])
  if (topicRouting) {
    kafkaConfig.topicByPayloadCase = topicRouting
  }

  const includePayloadCases = parseIncludePayloadCases(argv['kafka-include-payloads'])
  if (includePayloadCases) {
    kafkaConfig.includePayloadCases = includePayloadCases
  }

  const metaHeadersPrefix = argv['kafka-meta-headers-prefix']
  if (metaHeadersPrefix !== undefined) {
    if (typeof metaHeadersPrefix !== 'string' || metaHeadersPrefix.trim() === '') {
      throw new Error('kafka-meta-headers-prefix must be a non-empty string.')
    }
    kafkaConfig.metaHeadersPrefix = metaHeadersPrefix.trim()
  }

  const keyTemplateRaw = argv['kafka-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('kafka-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('kafka-key-template must be a non-empty string.')
    }
    compileKeyBuilder(keyTemplate)
    kafkaConfig.keyTemplate = keyTemplate
  }

  const sasl = parseSasl(argv)
  if (sasl) {
    kafkaConfig.sasl = sasl
  }

  const maxBatchSize = parsePositiveInteger(argv['kafka-max-batch-size'], 'kafka-max-batch-size')
  if (maxBatchSize !== undefined) {
    kafkaConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(
    argv['kafka-max-batch-delay-ms'],
    'kafka-max-batch-delay-ms'
  )
  if (maxBatchDelayMs !== undefined) {
    kafkaConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  const compression = parseCompression(argv['kafka-compression'])
  if (compression) {
    kafkaConfig.compression = compression
  }

  const acks = parseAcks(argv['kafka-acks'])
  if (acks !== undefined) {
    kafkaConfig.acks = acks
  }

  const idempotent = parseBooleanOption(argv['kafka-idempotent'], 'kafka-idempotent')
  if (idempotent !== undefined) {
    kafkaConfig.idempotent = idempotent
  }

  return {
    provider: 'kafka',
    ...kafkaConfig
  }
}

export type { KafkaEventBusConfig }
