import type {
  BronzePayloadCase,
  SilverRecordType,
  EventBusConfig,
  KafkaEventBusConfig,
  SilverKafkaEventBusConfig,
  RabbitMQEventBusConfig,
  KinesisEventBusConfig,
  NatsEventBusConfig,
  RedisEventBusConfig,
  SilverRabbitMQEventBusConfig,
  SilverKinesisEventBusConfig,
  SilverNatsEventBusConfig
} from './types'
import { compileKeyBuilder, compileSilverKeyBuilder } from './keyTemplate'

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

const NORMALIZED_PAYLOAD_CASE_LOOKUP = buildPayloadCaseLookup(ALLOWED_PAYLOAD_CASES)

const ALLOWED_SILVER_RECORD_TYPES: ReadonlySet<SilverRecordType> = new Set([
  'trade',
  'book_change',
  'book_snapshot',
  'grouped_book_snapshot',
  'quote',
  'derivative_ticker',
  'liquidation',
  'option_summary',
  'book_ticker',
  'trade_bar'
])

const NORMALIZED_SILVER_RECORD_TYPE_LOOKUP = buildSilverRecordTypeLookup(ALLOWED_SILVER_RECORD_TYPES)

const ACK_VALUE_MAP: Record<string, -1 | 0 | 1> = {
  all: -1,
  '-1': -1,
  '1': 1,
  leader: 1,
  '0': 0,
  none: 0
}

const RESERVED_STATIC_HEADER_KEYS = new Set(['payloadCase', 'dataType'])

function parseKafkaBrokers(raw: unknown): string[] {
  if (typeof raw !== 'string') {
    return []
  }

  return raw
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean)
}

function parseCompression(raw: unknown, flagPrefix = 'kafka'): KafkaEventBusConfig['compression'] | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error(`${flagPrefix}-compression must be one of none,gzip,snappy,lz4,zstd.`)
  }

  const value = raw.trim().toLowerCase()
  if (!ALLOWED_COMPRESSION.has(value)) {
    throw new Error(`${flagPrefix}-compression must be one of none,gzip,snappy,lz4,zstd.`)
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

    const normalizedPayloadCase = normalizePayloadCase(payloadCase)

    if (!normalizedPayloadCase) {
      invalidPayloadCases.add(payloadCase)
      continue
    }

    map[normalizedPayloadCase] = mappedTopic
  }

  if (Object.keys(map).length === 0) {
    throw new Error('kafka-topic-routing must define at least one payloadCase mapping.')
  }

  if (invalidPayloadCases.size > 0) {
    throw new Error(`Unknown payload case(s) for kafka-topic-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
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

  const uniqueInputs = Array.from(
    new Set(
      raw
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean)
    )
  )

  if (uniqueInputs.length === 0) {
    throw new Error('kafka-include-payloads must list at least one payload case.')
  }

  const normalizedCases = new Set<BronzePayloadCase>()
  const invalid: string[] = []

  for (const entry of uniqueInputs) {
    const normalized = normalizePayloadCase(entry)
    if (!normalized) {
      invalid.push(entry)
      continue
    }
    normalizedCases.add(normalized)
  }

  if (invalid.length > 0) {
    throw new Error(`Unknown payload case(s) for kafka-include-payloads: ${invalid.join(', ')}.`)
  }

  return Array.from(normalizedCases)
}

function parseStaticHeaders(raw: unknown, flagPrefix = 'kafka'): Record<string, string> | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error(`${flagPrefix}-static-headers must be a comma separated string list of key:value pairs.`)
  }

  const entries = raw
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)

  if (entries.length === 0) {
    throw new Error(`${flagPrefix}-static-headers must list at least one key:value pair.`)
  }

  const headers: Record<string, string> = {}
  for (const entry of entries) {
    const separatorIndex = entry.indexOf(':')
    if (separatorIndex <= 0 || separatorIndex === entry.length - 1) {
      throw new Error(`${flagPrefix}-static-headers entries must be key:value pairs.`)
    }

    const key = entry.slice(0, separatorIndex).trim()
    const value = entry.slice(separatorIndex + 1).trim()

    if (!key || !value) {
      throw new Error(`${flagPrefix}-static-headers entries must be key:value pairs.`)
    }

    if (RESERVED_STATIC_HEADER_KEYS.has(key)) {
      throw new Error(`${flagPrefix}-static-headers cannot override reserved header "${key}".`)
    }

    if (headers[key] !== undefined) {
      throw new Error(`Duplicate ${flagPrefix}-static-headers key "${key}".`)
    }

    headers[key] = value
  }

  return headers
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

function parseAcks(raw: unknown, flagPrefix = 'kafka'): KafkaEventBusConfig['acks'] | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw === 'number') {
    if (raw === -1 || raw === 0 || raw === 1) {
      return raw
    }
    throw new Error(`${flagPrefix}-acks must be one of all,leader,none,1,0,-1.`)
  }

  if (typeof raw === 'string') {
    const normalized = raw.trim().toLowerCase()
    const mapped = ACK_VALUE_MAP[normalized]
    if (mapped !== undefined) {
      return mapped
    }
    throw new Error(`${flagPrefix}-acks must be one of all,leader,none,1,0,-1.`)
  }

  throw new Error(`${flagPrefix}-acks must be one of all,leader,none,1,0,-1.`)
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

export function parseRabbitMQEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const urlRaw = argv['rabbitmq-url']
  const exchangeRaw = argv['rabbitmq-exchange']

  if (!urlRaw || !exchangeRaw) {
    return undefined
  }

  const url = typeof urlRaw === 'string' ? urlRaw.trim() : ''
  if (url === '') {
    throw new Error('rabbitmq-url must be a non-empty string.')
  }

  const exchange = typeof exchangeRaw === 'string' ? exchangeRaw.trim() : ''
  if (exchange === '') {
    throw new Error('rabbitmq-exchange must be a non-empty string.')
  }

  const rabbitmqConfig: RabbitMQEventBusConfig = {
    url,
    exchange
  }

  const exchangeTypeRaw = argv['rabbitmq-exchange-type']
  if (exchangeTypeRaw !== undefined) {
    if (typeof exchangeTypeRaw !== 'string') {
      throw new Error('rabbitmq-exchange-type must be a string.')
    }
    const exchangeType = exchangeTypeRaw.trim().toLowerCase()
    if (!['direct', 'topic', 'headers', 'fanout'].includes(exchangeType)) {
      throw new Error('rabbitmq-exchange-type must be one of direct,topic,headers,fanout.')
    }
    rabbitmqConfig.exchangeType = exchangeType as 'direct' | 'topic' | 'headers' | 'fanout'
  }

  const routingKeyTemplateRaw = argv['rabbitmq-routing-key-template']
  if (routingKeyTemplateRaw !== undefined) {
    if (typeof routingKeyTemplateRaw !== 'string') {
      throw new Error('rabbitmq-routing-key-template must be a non-empty string.')
    }
    const routingKeyTemplate = routingKeyTemplateRaw.trim()
    if (routingKeyTemplate === '') {
      throw new Error('rabbitmq-routing-key-template must be a non-empty string.')
    }
    compileKeyBuilder(routingKeyTemplate)
    rabbitmqConfig.routingKeyTemplate = routingKeyTemplate
  }

  const includePayloadCases = parseIncludePayloadCases(argv['rabbitmq-include-payloads'])
  if (includePayloadCases) {
    rabbitmqConfig.includePayloadCases = includePayloadCases
  }

  const staticHeaders = parseStaticHeaders(argv['rabbitmq-static-headers'])
  if (staticHeaders) {
    rabbitmqConfig.staticHeaders = staticHeaders
  }

  return {
    provider: 'rabbitmq',
    ...rabbitmqConfig
  }
}

export function parseKafkaEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const brokersRaw = argv['kafka-brokers']
  const topicRaw = argv['kafka-topic']

  if (!brokersRaw || !topicRaw) {
    return undefined
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('kafka-topic must be a non-empty string.')
  }

  const brokers = parseKafkaBrokers(brokersRaw)
  if (brokers.length === 0) {
    throw new Error('Invalid kafka-brokers value. Provide at least one broker URL.')
  }

  const clientIdRaw = argv['kafka-client-id']
  const clientId = typeof clientIdRaw === 'string' ? clientIdRaw.trim() : clientIdRaw

  const kafkaConfig: KafkaEventBusConfig = {
    brokers,
    topic,
    clientId: clientId || 'tardis-machine-publisher'
  }

  const ssl = parseBooleanOption(argv['kafka-ssl'], 'kafka-ssl')
  if (ssl !== undefined) {
    kafkaConfig.ssl = ssl
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

  const staticHeaders = parseStaticHeaders(argv['kafka-static-headers'])
  if (staticHeaders) {
    kafkaConfig.staticHeaders = staticHeaders
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

  const maxBatchDelayMs = parsePositiveInteger(argv['kafka-max-batch-delay-ms'], 'kafka-max-batch-delay-ms')
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

export function parseSilverKafkaEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const brokersRaw = argv['kafka-silver-brokers']
  const topicRaw = argv['kafka-silver-topic']

  if (!brokersRaw || !topicRaw) {
    return undefined
  }

  const brokers = parseKafkaBrokers(brokersRaw)
  if (brokers.length === 0) {
    throw new Error('kafka-silver-brokers must contain at least one broker.')
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('kafka-silver-topic must be a non-empty string.')
  }

  const kafkaConfig: SilverKafkaEventBusConfig = {
    brokers,
    topic
  }

  const topicRoutingRaw = argv['kafka-silver-topic-routing']
  if (topicRoutingRaw !== undefined) {
    if (typeof topicRoutingRaw !== 'string') {
      throw new Error('kafka-silver-topic-routing must be a string of recordType:topic entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = topicRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const [recordType, mappedTopic] = pair.split(':').map((part) => part?.trim())
      if (!recordType || !mappedTopic) {
        throw new Error(`Invalid kafka-silver-topic-routing entry "${pair}". Expected format recordType:topic.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('kafka-silver-topic-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for kafka-silver-topic-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    kafkaConfig.topicByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['kafka-silver-include-records'], 'kafka-silver')
  if (includeRecordTypes) {
    kafkaConfig.includeRecordTypes = includeRecordTypes
  }

  const clientIdRaw = argv['kafka-silver-client-id']
  if (clientIdRaw !== undefined) {
    if (typeof clientIdRaw !== 'string') {
      throw new Error('kafka-silver-client-id must be a string.')
    }
    const clientId = clientIdRaw.trim()
    if (clientId === '') {
      throw new Error('kafka-silver-client-id must be a non-empty string.')
    }
    kafkaConfig.clientId = clientId
  }

  const ssl = parseBooleanOption(argv['kafka-silver-ssl'], 'kafka-silver-ssl')
  if (ssl !== undefined) {
    kafkaConfig.ssl = ssl
  }

  const metaHeadersPrefixRaw = argv['kafka-silver-meta-headers-prefix']
  if (metaHeadersPrefixRaw !== undefined) {
    if (typeof metaHeadersPrefixRaw !== 'string') {
      throw new Error('kafka-silver-meta-headers-prefix must be a string.')
    }
    const metaHeadersPrefix = metaHeadersPrefixRaw.trim()
    if (metaHeadersPrefix === '') {
      throw new Error('kafka-silver-meta-headers-prefix must be a non-empty string.')
    }
    kafkaConfig.metaHeadersPrefix = metaHeadersPrefix
  }

  const staticHeaders = parseStaticHeaders(argv['kafka-silver-static-headers'], 'kafka-silver')
  if (staticHeaders) {
    kafkaConfig.staticHeaders = staticHeaders
  }

  const keyTemplateRaw = argv['kafka-silver-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('kafka-silver-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('kafka-silver-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(keyTemplate)
    kafkaConfig.keyTemplate = keyTemplate
  }

  const sasl = parseSilverSasl(argv)
  if (sasl) {
    kafkaConfig.sasl = sasl
  }

  const maxBatchSize = parsePositiveInteger(argv['kafka-silver-max-batch-size'], 'kafka-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    kafkaConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['kafka-silver-max-batch-delay-ms'], 'kafka-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    kafkaConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  const compression = parseCompression(argv['kafka-silver-compression'], 'kafka-silver')
  if (compression) {
    kafkaConfig.compression = compression
  }

  const acks = parseAcks(argv['kafka-silver-acks'], 'kafka-silver')
  if (acks !== undefined) {
    kafkaConfig.acks = acks
  }

  const idempotent = parseBooleanOption(argv['kafka-silver-idempotent'], 'kafka-silver-idempotent')
  if (idempotent !== undefined) {
    kafkaConfig.idempotent = idempotent
  }

  return {
    provider: 'kafka-silver',
    ...kafkaConfig
  }
}

function parseIncludeSilverRecordTypes(raw: unknown, flagPrefix = 'kafka-silver'): SilverRecordType[] | undefined {
  if (typeof raw !== 'string') {
    return undefined
  }

  const recordTypes: SilverRecordType[] = []
  const invalidRecordTypes = new Set<string>()

  const values = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)

  for (const value of values) {
    const normalized = normalizeSilverRecordType(value)
    if (normalized) {
      recordTypes.push(normalized)
    } else {
      invalidRecordTypes.add(value)
    }
  }

  if (invalidRecordTypes.size > 0) {
    throw new Error(`Unknown record type(s) for ${flagPrefix}-include-records: ${Array.from(invalidRecordTypes).join(', ')}.`)
  }

  return recordTypes.length > 0 ? recordTypes : undefined
}

function parseSilverSasl(argv: Record<string, any>): SilverKafkaEventBusConfig['sasl'] | undefined {
  const mechanism = argv['kafka-silver-sasl-mechanism']
  if (!mechanism) {
    return undefined
  }

  const username = argv['kafka-silver-sasl-username']
  const password = argv['kafka-silver-sasl-password']

  if (!username || !password) {
    throw new Error('Kafka Silver SASL username and password must be provided when sasl mechanism is set.')
  }

  return {
    mechanism,
    username,
    password
  }
}

function buildPayloadCaseLookup(cases: ReadonlySet<BronzePayloadCase>): Map<string, BronzePayloadCase> {
  const lookup = new Map<string, BronzePayloadCase>()
  for (const payloadCase of cases) {
    lookup.set(toPayloadCaseKey(payloadCase), payloadCase)
  }
  return lookup
}

function buildSilverRecordTypeLookup(types: ReadonlySet<SilverRecordType>): Map<string, SilverRecordType> {
  const lookup = new Map<string, SilverRecordType>()
  for (const recordType of types) {
    lookup.set(toRecordTypeKey(recordType), recordType)
  }
  return lookup
}

function normalizePayloadCase(raw: string): BronzePayloadCase | undefined {
  if (!raw) {
    return undefined
  }

  return NORMALIZED_PAYLOAD_CASE_LOOKUP.get(toPayloadCaseKey(raw))
}

function normalizeSilverRecordType(raw: string): SilverRecordType | undefined {
  if (!raw) {
    return undefined
  }

  return NORMALIZED_SILVER_RECORD_TYPE_LOOKUP.get(toRecordTypeKey(raw))
}

function toPayloadCaseKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/gi, '')
}

function toRecordTypeKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/gi, '')
}

export function parseKinesisEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const streamNameRaw = argv['kinesis-stream-name']
  const regionRaw = argv['kinesis-region']

  if (!streamNameRaw || !regionRaw) {
    return undefined
  }

  const streamName = typeof streamNameRaw === 'string' ? streamNameRaw.trim() : ''
  if (streamName === '') {
    throw new Error('kinesis-stream-name must be a non-empty string.')
  }

  const region = typeof regionRaw === 'string' ? regionRaw.trim() : ''
  if (region === '') {
    throw new Error('kinesis-region must be a non-empty string.')
  }

  const kinesisConfig: KinesisEventBusConfig = {
    streamName,
    region
  }

  const streamRoutingRaw = argv['kinesis-stream-routing']
  if (streamRoutingRaw !== undefined) {
    if (typeof streamRoutingRaw !== 'string') {
      throw new Error('kinesis-stream-routing must be a string of payloadCase:streamName entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = streamRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const [payloadCase, mappedStream] = pair.split(':').map((part) => part?.trim())
      if (!payloadCase || !mappedStream) {
        throw new Error(`Invalid kinesis-stream-routing entry "${pair}". Expected format payloadCase:streamName.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedStream
    }

    if (Object.keys(map).length === 0) {
      throw new Error('kinesis-stream-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for kinesis-stream-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    kinesisConfig.streamByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['kinesis-include-payloads'])
  if (includePayloadCases) {
    kinesisConfig.includePayloadCases = includePayloadCases
  }

  const accessKeyIdRaw = argv['kinesis-access-key-id']
  if (accessKeyIdRaw !== undefined) {
    if (typeof accessKeyIdRaw !== 'string') {
      throw new Error('kinesis-access-key-id must be a string.')
    }
    const accessKeyId = accessKeyIdRaw.trim()
    if (accessKeyId === '') {
      throw new Error('kinesis-access-key-id must be a non-empty string.')
    }
    kinesisConfig.accessKeyId = accessKeyId
  }

  const secretAccessKeyRaw = argv['kinesis-secret-access-key']
  if (secretAccessKeyRaw !== undefined) {
    if (typeof secretAccessKeyRaw !== 'string') {
      throw new Error('kinesis-secret-access-key must be a string.')
    }
    const secretAccessKey = secretAccessKeyRaw.trim()
    if (secretAccessKey === '') {
      throw new Error('kinesis-secret-access-key must be a non-empty string.')
    }
    kinesisConfig.secretAccessKey = secretAccessKey
  }

  const sessionTokenRaw = argv['kinesis-session-token']
  if (sessionTokenRaw !== undefined) {
    if (typeof sessionTokenRaw !== 'string') {
      throw new Error('kinesis-session-token must be a string.')
    }
    const sessionToken = sessionTokenRaw.trim()
    if (sessionToken === '') {
      throw new Error('kinesis-session-token must be a non-empty string.')
    }
    kinesisConfig.sessionToken = sessionToken
  }

  const staticHeaders = parseStaticHeaders(argv['kinesis-static-headers'])
  if (staticHeaders) {
    kinesisConfig.staticHeaders = staticHeaders
  }

  const partitionKeyTemplateRaw = argv['kinesis-partition-key-template']
  if (partitionKeyTemplateRaw !== undefined) {
    if (typeof partitionKeyTemplateRaw !== 'string') {
      throw new Error('kinesis-partition-key-template must be a non-empty string.')
    }
    const partitionKeyTemplate = partitionKeyTemplateRaw.trim()
    if (partitionKeyTemplate === '') {
      throw new Error('kinesis-partition-key-template must be a non-empty string.')
    }
    compileKeyBuilder(partitionKeyTemplate)
    kinesisConfig.partitionKeyTemplate = partitionKeyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['kinesis-max-batch-size'], 'kinesis-max-batch-size')
  if (maxBatchSize !== undefined) {
    kinesisConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['kinesis-max-batch-delay-ms'], 'kinesis-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    kinesisConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'kinesis',
    ...kinesisConfig
  }
}

export function parseNatsEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const serversRaw = argv['nats-servers']
  const subjectRaw = argv['nats-subject']

  if (!serversRaw || !subjectRaw) {
    return undefined
  }

  const subject = typeof subjectRaw === 'string' ? subjectRaw.trim() : ''
  if (subject === '') {
    throw new Error('nats-subject must be a non-empty string.')
  }

  let servers: string[]
  if (typeof serversRaw === 'string') {
    servers = serversRaw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  } else if (Array.isArray(serversRaw)) {
    servers = serversRaw.map((s) => String(s).trim()).filter(Boolean)
  } else {
    throw new Error('nats-servers must be a comma-separated string or array of server URLs.')
  }

  if (servers.length === 0) {
    throw new Error('nats-servers must contain at least one server URL.')
  }

  const natsConfig: NatsEventBusConfig = {
    servers,
    subject
  }

  const subjectRoutingRaw = argv['nats-subject-routing']
  if (subjectRoutingRaw !== undefined) {
    if (typeof subjectRoutingRaw !== 'string') {
      throw new Error('nats-subject-routing must be a string of payloadCase:subject entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = subjectRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const [payloadCase, mappedSubject] = pair.split(':').map((part) => part?.trim())
      if (!payloadCase || !mappedSubject) {
        throw new Error(`Invalid nats-subject-routing entry "${pair}". Expected format payloadCase:subject.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedSubject
    }

    if (Object.keys(map).length === 0) {
      throw new Error('nats-subject-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for nats-subject-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    natsConfig.subjectByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['nats-include-payloads'])
  if (includePayloadCases) {
    natsConfig.includePayloadCases = includePayloadCases
  }

  const staticHeaders = parseStaticHeaders(argv['nats-static-headers'])
  if (staticHeaders) {
    natsConfig.staticHeaders = staticHeaders
  }

  const subjectTemplateRaw = argv['nats-subject-template']
  if (subjectTemplateRaw !== undefined) {
    if (typeof subjectTemplateRaw !== 'string') {
      throw new Error('nats-subject-template must be a non-empty string.')
    }
    const subjectTemplate = subjectTemplateRaw.trim()
    if (subjectTemplate === '') {
      throw new Error('nats-subject-template must be a non-empty string.')
    }
    compileKeyBuilder(subjectTemplate)
    natsConfig.subjectTemplate = subjectTemplate
  }

  return {
    provider: 'nats',
    ...natsConfig
  }
}

export function parseSilverRabbitMQEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const urlRaw = argv['rabbitmq-silver-url']
  const exchangeRaw = argv['rabbitmq-silver-exchange']

  if (!urlRaw || !exchangeRaw) {
    return undefined
  }

  const url = typeof urlRaw === 'string' ? urlRaw.trim() : ''
  if (url === '') {
    throw new Error('rabbitmq-silver-url must be a non-empty string.')
  }

  const exchange = typeof exchangeRaw === 'string' ? exchangeRaw.trim() : ''
  if (exchange === '') {
    throw new Error('rabbitmq-silver-exchange must be a non-empty string.')
  }

  const rabbitmqConfig: SilverRabbitMQEventBusConfig = {
    url,
    exchange
  }

  const exchangeTypeRaw = argv['rabbitmq-silver-exchange-type']
  if (exchangeTypeRaw !== undefined) {
    if (typeof exchangeTypeRaw !== 'string') {
      throw new Error('rabbitmq-silver-exchange-type must be a string.')
    }
    const exchangeType = exchangeTypeRaw.trim().toLowerCase()
    if (!['direct', 'topic', 'headers', 'fanout'].includes(exchangeType)) {
      throw new Error('rabbitmq-silver-exchange-type must be one of direct,topic,headers,fanout.')
    }
    rabbitmqConfig.exchangeType = exchangeType as 'direct' | 'topic' | 'headers' | 'fanout'
  }

  const routingKeyTemplateRaw = argv['rabbitmq-silver-routing-key-template']
  if (routingKeyTemplateRaw !== undefined) {
    if (typeof routingKeyTemplateRaw !== 'string') {
      throw new Error('rabbitmq-silver-routing-key-template must be a non-empty string.')
    }
    const routingKeyTemplate = routingKeyTemplateRaw.trim()
    if (routingKeyTemplate === '') {
      throw new Error('rabbitmq-silver-routing-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(routingKeyTemplate)
    rabbitmqConfig.routingKeyTemplate = routingKeyTemplate
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['rabbitmq-silver-include-records'], 'rabbitmq-silver')
  if (includeRecordTypes) {
    rabbitmqConfig.includeRecordTypes = includeRecordTypes
  }

  const staticHeaders = parseStaticHeaders(argv['rabbitmq-silver-static-headers'], 'rabbitmq-silver')
  if (staticHeaders) {
    rabbitmqConfig.staticHeaders = staticHeaders
  }

  return {
    provider: 'rabbitmq-silver',
    ...rabbitmqConfig
  }
}

export function parseSilverKinesisEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const streamNameRaw = argv['kinesis-silver-stream-name']
  const regionRaw = argv['kinesis-silver-region']

  if (!streamNameRaw || !regionRaw) {
    return undefined
  }

  const streamName = typeof streamNameRaw === 'string' ? streamNameRaw.trim() : ''
  if (streamName === '') {
    throw new Error('kinesis-silver-stream-name must be a non-empty string.')
  }

  const region = typeof regionRaw === 'string' ? regionRaw.trim() : ''
  if (region === '') {
    throw new Error('kinesis-silver-region must be a non-empty string.')
  }

  const kinesisConfig: SilverKinesisEventBusConfig = {
    streamName,
    region
  }

  const streamRoutingRaw = argv['kinesis-silver-stream-routing']
  if (streamRoutingRaw !== undefined) {
    if (typeof streamRoutingRaw !== 'string') {
      throw new Error('kinesis-silver-stream-routing must be a string of recordType:streamName entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = streamRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const [recordType, mappedStream] = pair.split(':').map((part) => part?.trim())
      if (!recordType || !mappedStream) {
        throw new Error(`Invalid kinesis-silver-stream-routing entry "${pair}". Expected format recordType:streamName.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedStream
    }

    if (Object.keys(map).length === 0) {
      throw new Error('kinesis-silver-stream-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for kinesis-silver-stream-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    kinesisConfig.streamByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['kinesis-silver-include-records'], 'kinesis-silver')
  if (includeRecordTypes) {
    kinesisConfig.includeRecordTypes = includeRecordTypes
  }

  const accessKeyIdRaw = argv['kinesis-silver-access-key-id']
  if (accessKeyIdRaw !== undefined) {
    if (typeof accessKeyIdRaw !== 'string') {
      throw new Error('kinesis-silver-access-key-id must be a string.')
    }
    const accessKeyId = accessKeyIdRaw.trim()
    if (accessKeyId === '') {
      throw new Error('kinesis-silver-access-key-id must be a non-empty string.')
    }
    kinesisConfig.accessKeyId = accessKeyId
  }

  const secretAccessKeyRaw = argv['kinesis-silver-secret-access-key']
  if (secretAccessKeyRaw !== undefined) {
    if (typeof secretAccessKeyRaw !== 'string') {
      throw new Error('kinesis-silver-secret-access-key must be a string.')
    }
    const secretAccessKey = secretAccessKeyRaw.trim()
    if (secretAccessKey === '') {
      throw new Error('kinesis-silver-secret-access-key must be a non-empty string.')
    }
    kinesisConfig.secretAccessKey = secretAccessKey
  }

  const sessionTokenRaw = argv['kinesis-silver-session-token']
  if (sessionTokenRaw !== undefined) {
    if (typeof sessionTokenRaw !== 'string') {
      throw new Error('kinesis-silver-session-token must be a string.')
    }
    const sessionToken = sessionTokenRaw.trim()
    if (sessionToken === '') {
      throw new Error('kinesis-silver-session-token must be a non-empty string.')
    }
    kinesisConfig.sessionToken = sessionToken
  }

  const staticHeaders = parseStaticHeaders(argv['kinesis-silver-static-headers'], 'kinesis-silver')
  if (staticHeaders) {
    kinesisConfig.staticHeaders = staticHeaders
  }

  const partitionKeyTemplateRaw = argv['kinesis-silver-partition-key-template']
  if (partitionKeyTemplateRaw !== undefined) {
    if (typeof partitionKeyTemplateRaw !== 'string') {
      throw new Error('kinesis-silver-partition-key-template must be a non-empty string.')
    }
    const partitionKeyTemplate = partitionKeyTemplateRaw.trim()
    if (partitionKeyTemplate === '') {
      throw new Error('kinesis-silver-partition-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(partitionKeyTemplate)
    kinesisConfig.partitionKeyTemplate = partitionKeyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['kinesis-silver-max-batch-size'], 'kinesis-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    kinesisConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['kinesis-silver-max-batch-delay-ms'], 'kinesis-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    kinesisConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'kinesis-silver',
    ...kinesisConfig
  }
}

export function parseSilverNatsEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const serversRaw = argv['nats-silver-servers']
  const subjectRaw = argv['nats-silver-subject']

  if (!serversRaw || !subjectRaw) {
    return undefined
  }

  const subject = typeof subjectRaw === 'string' ? subjectRaw.trim() : ''
  if (subject === '') {
    throw new Error('nats-silver-subject must be a non-empty string.')
  }

  let servers: string[]
  if (typeof serversRaw === 'string') {
    servers = serversRaw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  } else if (Array.isArray(serversRaw)) {
    servers = serversRaw.map((s) => String(s).trim()).filter(Boolean)
  } else {
    throw new Error('nats-silver-servers must be a comma-separated string or array of server URLs.')
  }

  if (servers.length === 0) {
    throw new Error('nats-silver-servers must contain at least one server URL.')
  }

  const natsConfig: SilverNatsEventBusConfig = {
    servers,
    subject
  }

  const subjectRoutingRaw = argv['nats-silver-subject-routing']
  if (subjectRoutingRaw !== undefined) {
    if (typeof subjectRoutingRaw !== 'string') {
      throw new Error('nats-silver-subject-routing must be a string of recordType:subject entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = subjectRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const [recordType, mappedSubject] = pair.split(':').map((part) => part?.trim())
      if (!recordType || !mappedSubject) {
        throw new Error(`Invalid nats-silver-subject-routing entry "${pair}". Expected format recordType:subject.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedSubject
    }

    if (Object.keys(map).length === 0) {
      throw new Error('nats-silver-subject-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for nats-silver-subject-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    natsConfig.subjectByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['nats-silver-include-records'], 'nats-silver')
  if (includeRecordTypes) {
    natsConfig.includeRecordTypes = includeRecordTypes
  }

  const staticHeaders = parseStaticHeaders(argv['nats-silver-static-headers'], 'nats-silver')
  if (staticHeaders) {
    natsConfig.staticHeaders = staticHeaders
  }

  const subjectTemplateRaw = argv['nats-silver-subject-template']
  if (subjectTemplateRaw !== undefined) {
    if (typeof subjectTemplateRaw !== 'string') {
      throw new Error('nats-silver-subject-template must be a non-empty string.')
    }
    const subjectTemplate = subjectTemplateRaw.trim()
    if (subjectTemplate === '') {
      throw new Error('nats-silver-subject-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(subjectTemplate)
    natsConfig.subjectTemplate = subjectTemplate
  }

  return {
    provider: 'nats-silver',
    ...natsConfig
  }
}

export function parseRedisEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const urlRaw = argv['redis-url']
  const streamRaw = argv['redis-stream']

  if (!urlRaw || !streamRaw) {
    return undefined
  }

  const url = typeof urlRaw === 'string' ? urlRaw.trim() : ''
  if (url === '') {
    throw new Error('redis-url must be a non-empty string.')
  }

  const stream = typeof streamRaw === 'string' ? streamRaw.trim() : ''
  if (stream === '') {
    throw new Error('redis-stream must be a non-empty string.')
  }

  const redisConfig: RedisEventBusConfig = {
    url,
    stream
  }

  const streamRoutingRaw = argv['redis-stream-routing']
  if (streamRoutingRaw !== undefined) {
    if (typeof streamRoutingRaw !== 'string') {
      throw new Error('redis-stream-routing must be a string of payloadCase:stream entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = streamRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const [payloadCase, mappedStream] = pair.split(':').map((part) => part?.trim())
      if (!payloadCase || !mappedStream) {
        throw new Error(`Invalid redis-stream-routing entry "${pair}". Expected format payloadCase:stream.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedStream
    }

    if (Object.keys(map).length === 0) {
      throw new Error('redis-stream-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for redis-stream-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    redisConfig.streamByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['redis-include-payloads'])
  if (includePayloadCases) {
    redisConfig.includePayloadCases = includePayloadCases
  }

  const staticHeaders = parseStaticHeaders(argv['redis-static-headers'])
  if (staticHeaders) {
    redisConfig.staticHeaders = staticHeaders
  }

  const keyTemplateRaw = argv['redis-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('redis-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('redis-key-template must be a non-empty string.')
    }
    compileKeyBuilder(keyTemplate)
    redisConfig.keyTemplate = keyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['redis-max-batch-size'], 'redis-max-batch-size')
  if (maxBatchSize !== undefined) {
    redisConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['redis-max-batch-delay-ms'], 'redis-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    redisConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'redis',
    ...redisConfig
  }
}

export type { KafkaEventBusConfig }
