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

  const map: Record<string, string> = {}

  const pairs = raw
    .split(',')
    .map((pair) => pair.trim())
    .filter(Boolean)

  for (const pair of pairs) {
    const [payloadCase, mappedTopic] = pair.split(':').map((part) => part?.trim())
    if (!payloadCase || !mappedTopic) {
      throw new Error(`Invalid kafka-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
    }
    map[payloadCase] = mappedTopic
  }

  if (Object.keys(map).length === 0) {
    throw new Error('kafka-topic-routing must define at least one payloadCase mapping.')
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

  return {
    provider: 'kafka',
    ...kafkaConfig
  }
}

export type { KafkaEventBusConfig }
