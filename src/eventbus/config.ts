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
  SQSEventBusConfig,
  PulsarEventBusConfig,
  AzureEventHubsEventBusConfig,
  PubSubEventBusConfig,
  MQTTEventBusConfig,
  SilverPubSubEventBusConfig,
  SilverRabbitMQEventBusConfig,
  SilverKinesisEventBusConfig,
  SilverNatsEventBusConfig,
  SilverRedisEventBusConfig,
  SilverPulsarEventBusConfig,
  SilverSQSEventBusConfig,
  SilverAzureEventBusConfig,
  ConsoleEventBusConfig
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
const RESERVED_SILVER_STATIC_HEADER_KEYS = new Set(['recordType', 'dataType'])

function parseStaticHeaders(
  raw: unknown,
  flagName = 'static-headers',
  reservedKeys = RESERVED_STATIC_HEADER_KEYS
): Record<string, string> | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error(`${flagName} must be a comma separated string list of key:value pairs.`)
  }

  const entries = raw
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)

  if (entries.length === 0) {
    throw new Error(`${flagName} must list at least one key:value pair.`)
  }

  const headers: Record<string, string> = {}
  for (const entry of entries) {
    const separatorIndex = entry.indexOf(':')
    if (separatorIndex <= 0 || separatorIndex === entry.length - 1) {
      throw new Error(`${flagName} entries must be key:value pairs.`)
    }

    const key = entry.slice(0, separatorIndex).trim()
    const value = entry.slice(separatorIndex + 1).trim()

    if (!key || !value) {
      throw new Error(`${flagName} entries must be key:value pairs.`)
    }

    if (reservedKeys.has(key)) {
      throw new Error(`${flagName} cannot override reserved header "${key}".`)
    }

    if (headers[key] !== undefined) {
      throw new Error(`Duplicate ${flagName} key "${key}".`)
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
    compileKeyBuilder(routingKeyTemplate, 'rabbitmq')
    rabbitmqConfig.routingKeyTemplate = routingKeyTemplate
  }

  const includePayloadCases = parseIncludePayloadCases(argv['rabbitmq-include-payloads'])
  if (includePayloadCases) {
    rabbitmqConfig.includePayloadCases = includePayloadCases
  }

  const staticHeaders = parseStaticHeaders(argv['rabbitmq-static-headers'], 'rabbitmq-static-headers')
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

  const staticHeaders = parseStaticHeaders(argv['kafka-static-headers'], 'kafka-static-headers')
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

  const schemaRegistryUrl = argv['kafka-schema-registry-url']
  if (schemaRegistryUrl) {
    if (typeof schemaRegistryUrl !== 'string') {
      throw new Error('kafka-schema-registry-url must be a string.')
    }
    kafkaConfig.schemaRegistry = {
      url: schemaRegistryUrl.trim()
    }
    const authUsername = argv['kafka-schema-registry-auth-username']
    const authPassword = argv['kafka-schema-registry-auth-password']
    if (authUsername || authPassword) {
      if (typeof authUsername !== 'string' || typeof authPassword !== 'string') {
        throw new Error('kafka-schema-registry-auth-username and kafka-schema-registry-auth-password must be strings.')
      }
      kafkaConfig.schemaRegistry.auth = {
        username: authUsername.trim(),
        password: authPassword.trim()
      }
    }
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
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid kafka-silver-topic-routing entry "${pair}". Expected format recordType:topic.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedTopic = pair.slice(separatorIndex + 1).trim()

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

  const staticHeaders = parseStaticHeaders(
    argv['kafka-silver-static-headers'],
    'kafka-silver-static-headers',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
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
    compileSilverKeyBuilder(keyTemplate, 'kafka-silver')
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

  const schemaRegistryUrl = argv['kafka-silver-schema-registry-url']
  if (schemaRegistryUrl) {
    if (typeof schemaRegistryUrl !== 'string') {
      throw new Error('kafka-silver-schema-registry-url must be a string.')
    }
    kafkaConfig.schemaRegistry = {
      url: schemaRegistryUrl.trim()
    }
    const authUsername = argv['kafka-silver-schema-registry-auth-username']
    const authPassword = argv['kafka-silver-schema-registry-auth-password']
    if (authUsername || authPassword) {
      if (typeof authUsername !== 'string' || typeof authPassword !== 'string') {
        throw new Error('kafka-silver-schema-registry-auth-username and kafka-silver-schema-registry-auth-password must be strings.')
      }
      kafkaConfig.schemaRegistry.auth = {
        username: authUsername.trim(),
        password: authPassword.trim()
      }
    }
  }

  return {
    provider: 'kafka-silver',
    ...kafkaConfig
  }
}

function parseKafkaBrokers(raw: unknown): string[] {
  if (typeof raw === 'string') {
    return raw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  } else if (Array.isArray(raw)) {
    return raw.map((s) => String(s).trim()).filter(Boolean)
  } else {
    throw new Error('Kafka brokers must be a comma-separated string or array of broker URLs.')
  }
}

function parseTopicRouting(raw: unknown): Partial<Record<BronzePayloadCase, string>> | undefined {
  if (typeof raw !== 'string') {
    return undefined
  }

  const map: Partial<Record<BronzePayloadCase, string>> = {}
  const invalidPayloadCases = new Set<string>()

  const pairs = raw
    .split(',')
    .map((pair) => pair.trim())
    .filter(Boolean)

  for (const pair of pairs) {
    const separatorIndex = pair.indexOf(':')
    if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
      throw new Error(`Invalid kafka-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
    }

    const payloadCase = pair.slice(0, separatorIndex).trim()
    const topic = pair.slice(separatorIndex + 1).trim()

    if (!payloadCase || !topic) {
      throw new Error(`Invalid kafka-topic-routing entry "${pair}". Expected format payloadCase:topic.`)
    }

    const normalizedPayloadCase = normalizePayloadCase(payloadCase)

    if (!normalizedPayloadCase) {
      invalidPayloadCases.add(payloadCase)
      continue
    }

    map[normalizedPayloadCase] = topic
  }

  if (Object.keys(map).length === 0) {
    throw new Error('kafka-topic-routing must define at least one payloadCase mapping.')
  }

  if (invalidPayloadCases.size > 0) {
    throw new Error(`Unknown payload case(s) for kafka-topic-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
  }

  return map
}

function parseCompression(raw: unknown, flagPrefix = 'kafka'): 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd' | undefined {
  if (raw === undefined || raw === null || raw === '') {
    return undefined
  }

  if (typeof raw !== 'string') {
    throw new Error(`${flagPrefix}-compression must be a string.`)
  }

  const compression = raw.trim().toLowerCase()
  if (!ALLOWED_COMPRESSION.has(compression)) {
    throw new Error(`${flagPrefix}-compression must be one of ${Array.from(ALLOWED_COMPRESSION).join(',')}.`)
  }

  return compression as 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd'
}

function parseIncludePayloadCases(raw: unknown, flagPrefix = 'kafka'): BronzePayloadCase[] | undefined {
  if (typeof raw !== 'string') {
    return undefined
  }

  const payloadCasesSet = new Set<BronzePayloadCase>()
  const invalidPayloadCases = new Set<string>()

  const values = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)

  for (const value of values) {
    const normalized = normalizePayloadCase(value)
    if (normalized) {
      payloadCasesSet.add(normalized)
    } else {
      invalidPayloadCases.add(value)
    }
  }

  if (invalidPayloadCases.size > 0) {
    throw new Error(`Unknown payload case(s) for ${flagPrefix}-include-payloads: ${Array.from(invalidPayloadCases).join(', ')}.`)
  }

  return payloadCasesSet.size > 0 ? Array.from(payloadCasesSet) : undefined
}

function parseIncludeSilverRecordTypes(raw: unknown, flagPrefix = 'kafka-silver'): SilverRecordType[] | undefined {
  if (typeof raw !== 'string') {
    return undefined
  }

  const recordTypesSet = new Set<SilverRecordType>()
  const invalidRecordTypes = new Set<string>()

  const values = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)

  for (const value of values) {
    const normalized = normalizeSilverRecordType(value)
    if (normalized) {
      recordTypesSet.add(normalized)
    } else {
      invalidRecordTypes.add(value)
    }
  }

  if (invalidRecordTypes.size > 0) {
    throw new Error(`Unknown record type(s) for ${flagPrefix}-include-records: ${Array.from(invalidRecordTypes).join(', ')}.`)
  }

  return recordTypesSet.size > 0 ? Array.from(recordTypesSet) : undefined
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
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid kinesis-stream-routing entry "${pair}". Expected format payloadCase:streamName.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedStream = pair.slice(separatorIndex + 1).trim()

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

  const staticHeaders = parseStaticHeaders(argv['kinesis-static-headers'], 'kinesis-static-headers')
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
    compileKeyBuilder(partitionKeyTemplate, 'kinesis')
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
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid nats-subject-routing entry "${pair}". Expected format payloadCase:subject.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedSubject = pair.slice(separatorIndex + 1).trim()

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

  const staticHeaders = parseStaticHeaders(argv['nats-static-headers'], 'nats-static-headers')
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
    compileKeyBuilder(subjectTemplate, 'nats')
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
    compileSilverKeyBuilder(routingKeyTemplate, 'rabbitmq-silver')
    rabbitmqConfig.routingKeyTemplate = routingKeyTemplate
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['rabbitmq-silver-include-records'], 'rabbitmq-silver')
  if (includeRecordTypes) {
    rabbitmqConfig.includeRecordTypes = includeRecordTypes
  }

  const staticHeaders = parseStaticHeaders(
    argv['rabbitmq-silver-static-headers'],
    'rabbitmq-silver-static-headers',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
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
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid kinesis-silver-stream-routing entry "${pair}". Expected format recordType:streamName.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedStream = pair.slice(separatorIndex + 1).trim()

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

  const staticHeaders = parseStaticHeaders(
    argv['kinesis-silver-static-headers'],
    'kinesis-silver-static-headers',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
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
    compileSilverKeyBuilder(partitionKeyTemplate, 'kinesis-silver')
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
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid nats-silver-subject-routing entry "${pair}". Expected format recordType:subject.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedSubject = pair.slice(separatorIndex + 1).trim()

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

  const staticHeaders = parseStaticHeaders(
    argv['nats-silver-static-headers'],
    'nats-silver-static-headers',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
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
    compileSilverKeyBuilder(subjectTemplate, 'nats-silver')
    natsConfig.subjectTemplate = subjectTemplate
  }

  return {
    provider: 'nats-silver',
    ...natsConfig
  }
}

export function parseSQSEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const queueUrlRaw = argv['sqs-queue-url']
  const regionRaw = argv['sqs-region']

  if (!queueUrlRaw || !regionRaw) {
    return undefined
  }

  const queueUrl = typeof queueUrlRaw === 'string' ? queueUrlRaw.trim() : ''
  if (queueUrl === '') {
    throw new Error('sqs-queue-url must be a non-empty string.')
  }

  const region = typeof regionRaw === 'string' ? regionRaw.trim() : ''
  if (region === '') {
    throw new Error('sqs-region must be a non-empty string.')
  }

  const sqsConfig: SQSEventBusConfig = {
    queueUrl,
    region
  }

  const queueRoutingRaw = argv['sqs-queue-routing']
  if (queueRoutingRaw !== undefined) {
    if (typeof queueRoutingRaw !== 'string') {
      throw new Error('sqs-queue-routing must be a string of payloadCase:queueUrl entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = queueRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid sqs-queue-routing entry "${pair}". Expected format payloadCase:queueUrl.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedQueue = pair.slice(separatorIndex + 1).trim()

      if (!payloadCase || !mappedQueue) {
        throw new Error(`Invalid sqs-queue-routing entry "${pair}". Expected format payloadCase:queueUrl.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedQueue
    }

    if (Object.keys(map).length === 0) {
      throw new Error('sqs-queue-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for sqs-queue-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    sqsConfig.queueByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['sqs-include-payloads'], 'sqs')
  if (includePayloadCases) {
    sqsConfig.includePayloadCases = includePayloadCases
  }

  const accessKeyIdRaw = argv['sqs-access-key-id']
  if (accessKeyIdRaw !== undefined) {
    if (typeof accessKeyIdRaw !== 'string') {
      throw new Error('sqs-access-key-id must be a string.')
    }
    const accessKeyId = accessKeyIdRaw.trim()
    if (accessKeyId === '') {
      throw new Error('sqs-access-key-id must be a non-empty string.')
    }
    sqsConfig.accessKeyId = accessKeyId
  }

  const secretAccessKeyRaw = argv['sqs-secret-access-key']
  if (secretAccessKeyRaw !== undefined) {
    if (typeof secretAccessKeyRaw !== 'string') {
      throw new Error('sqs-secret-access-key must be a string.')
    }
    const secretAccessKey = secretAccessKeyRaw.trim()
    if (secretAccessKey === '') {
      throw new Error('sqs-secret-access-key must be a non-empty string.')
    }
    sqsConfig.secretAccessKey = secretAccessKey
  }

  const sessionTokenRaw = argv['sqs-session-token']
  if (sessionTokenRaw !== undefined) {
    if (typeof sessionTokenRaw !== 'string') {
      throw new Error('sqs-session-token must be a string.')
    }
    const sessionToken = sessionTokenRaw.trim()
    if (sessionToken === '') {
      throw new Error('sqs-session-token must be a non-empty string.')
    }
    sqsConfig.sessionToken = sessionToken
  }

  const staticMessageAttributes = parseStaticHeaders(argv['sqs-static-message-attributes'], 'sqs-static-message-attributes')
  if (staticMessageAttributes) {
    sqsConfig.staticMessageAttributes = staticMessageAttributes
  }

  const maxBatchSize = parsePositiveInteger(argv['sqs-max-batch-size'], 'sqs-max-batch-size')
  if (maxBatchSize !== undefined) {
    sqsConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['sqs-max-batch-delay-ms'], 'sqs-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    sqsConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'sqs',
    ...sqsConfig
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
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid redis-stream-routing entry "${pair}". Expected format payloadCase:stream.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedStream = pair.slice(separatorIndex + 1).trim()

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

  const includePayloadCases = parseIncludePayloadCases(argv['redis-include-payloads'], 'redis')
  if (includePayloadCases) {
    redisConfig.includePayloadCases = includePayloadCases
  }

  const staticHeaders = parseStaticHeaders(argv['redis-static-headers'], 'redis-static-headers')
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
    compileKeyBuilder(keyTemplate, 'redis')
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

export function parseSilverRedisEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const urlRaw = argv['redis-silver-url']
  const streamRaw = argv['redis-silver-stream']

  if (!urlRaw || !streamRaw) {
    return undefined
  }

  const url = typeof urlRaw === 'string' ? urlRaw.trim() : ''
  if (url === '') {
    throw new Error('redis-silver-url must be a non-empty string.')
  }

  const stream = typeof streamRaw === 'string' ? streamRaw.trim() : ''
  if (stream === '') {
    throw new Error('redis-silver-stream must be a non-empty string.')
  }

  const redisConfig: SilverRedisEventBusConfig = {
    url,
    stream
  }

  const streamRoutingRaw = argv['redis-silver-stream-routing']
  if (streamRoutingRaw !== undefined) {
    if (typeof streamRoutingRaw !== 'string') {
      throw new Error('redis-silver-stream-routing must be a string of recordType:stream entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = streamRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid redis-silver-stream-routing entry "${pair}". Expected format recordType:stream.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedStream = pair.slice(separatorIndex + 1).trim()

      if (!recordType || !mappedStream) {
        throw new Error(`Invalid redis-silver-stream-routing entry "${pair}". Expected format recordType:stream.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedStream
    }

    if (Object.keys(map).length === 0) {
      throw new Error('redis-silver-stream-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for redis-silver-stream-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    redisConfig.streamByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['redis-silver-include-records'], 'redis-silver')
  if (includeRecordTypes) {
    redisConfig.includeRecordTypes = includeRecordTypes
  }

  const staticHeaders = parseStaticHeaders(
    argv['redis-silver-static-headers'],
    'redis-silver-static-headers',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
  if (staticHeaders) {
    redisConfig.staticHeaders = staticHeaders
  }

  const keyTemplateRaw = argv['redis-silver-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('redis-silver-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('redis-silver-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(keyTemplate, 'redis-silver')
    redisConfig.keyTemplate = keyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['redis-silver-max-batch-size'], 'redis-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    redisConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['redis-silver-max-batch-delay-ms'], 'redis-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    redisConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'redis-silver',
    ...redisConfig
  }
}

export function parsePulsarEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const serviceUrlRaw = argv['pulsar-service-url']
  const topicRaw = argv['pulsar-topic']

  if (!serviceUrlRaw || !topicRaw) {
    return undefined
  }

  const serviceUrl = typeof serviceUrlRaw === 'string' ? serviceUrlRaw.trim() : ''
  if (serviceUrl === '') {
    throw new Error('pulsar-service-url must be a non-empty string.')
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('pulsar-topic must be a non-empty string.')
  }

  const pulsarConfig: PulsarEventBusConfig = {
    serviceUrl,
    topic
  }

  const tokenRaw = argv['pulsar-token']
  if (tokenRaw !== undefined) {
    if (typeof tokenRaw !== 'string') {
      throw new Error('pulsar-token must be a string.')
    }
    const token = tokenRaw.trim()
    if (token === '') {
      throw new Error('pulsar-token must be a non-empty string.')
    }
    pulsarConfig.token = token
  }

  const topicRoutingRaw = argv['pulsar-topic-routing']
  if (topicRoutingRaw !== undefined) {
    if (typeof topicRoutingRaw !== 'string') {
      throw new Error('pulsar-topic-routing must be a string of payloadCase:topic entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = topicRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid pulsar-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedTopic = pair.slice(separatorIndex + 1).trim()

      if (!payloadCase || !mappedTopic) {
        throw new Error(`Invalid pulsar-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('pulsar-topic-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for pulsar-topic-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    pulsarConfig.topicByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['pulsar-include-payloads'], 'pulsar')
  if (includePayloadCases) {
    pulsarConfig.includePayloadCases = includePayloadCases
  }

  const staticProperties = parseStaticHeaders(argv['pulsar-static-properties'], 'pulsar-static-properties')
  if (staticProperties) {
    pulsarConfig.staticProperties = staticProperties
  }

  const keyTemplateRaw = argv['pulsar-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('pulsar-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('pulsar-key-template must be a non-empty string.')
    }
    compileKeyBuilder(keyTemplate, 'pulsar')
    pulsarConfig.keyTemplate = keyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['pulsar-max-batch-size'], 'pulsar-max-batch-size')
  if (maxBatchSize !== undefined) {
    pulsarConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['pulsar-max-batch-delay-ms'], 'pulsar-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    pulsarConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  const compressionTypeRaw = argv['pulsar-compression-type']
  if (compressionTypeRaw !== undefined) {
    if (typeof compressionTypeRaw !== 'string') {
      throw new Error('pulsar-compression-type must be a string.')
    }
    const compressionType = compressionTypeRaw.trim().toUpperCase()
    if (!['NONE', 'LZ4', 'ZLIB', 'ZSTD', 'SNAPPY'].includes(compressionType)) {
      throw new Error('pulsar-compression-type must be one of NONE,LZ4,ZLIB,ZSTD,SNAPPY.')
    }
    pulsarConfig.compressionType = compressionType as PulsarEventBusConfig['compressionType']
  }

  return {
    provider: 'pulsar',
    ...pulsarConfig
  }
}

export function parseAzureEventHubsEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const connectionStringRaw = argv['event-hubs-connection-string']
  const eventHubNameRaw = argv['event-hubs-event-hub-name']

  if (!connectionStringRaw || !eventHubNameRaw) {
    return undefined
  }

  const connectionString = typeof connectionStringRaw === 'string' ? connectionStringRaw.trim() : ''
  if (connectionString === '') {
    throw new Error('event-hubs-connection-string must be a non-empty string.')
  }

  const eventHubName = typeof eventHubNameRaw === 'string' ? eventHubNameRaw.trim() : ''
  if (eventHubName === '') {
    throw new Error('event-hubs-event-hub-name must be a non-empty string.')
  }

  const azureConfig: AzureEventHubsEventBusConfig = {
    connectionString,
    eventHubName
  }

  const eventHubRoutingRaw = argv['event-hubs-event-hub-routing']
  if (eventHubRoutingRaw !== undefined) {
    if (typeof eventHubRoutingRaw !== 'string') {
      throw new Error('event-hubs-event-hub-routing must be a string of payloadCase:eventHubName entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = eventHubRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid event-hubs-event-hub-routing entry "${pair}". Expected format payloadCase:eventHubName.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedEventHub = pair.slice(separatorIndex + 1).trim()

      if (!payloadCase || !mappedEventHub) {
        throw new Error(`Invalid event-hubs-event-hub-routing entry "${pair}". Expected format payloadCase:eventHubName.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedEventHub
    }

    if (Object.keys(map).length === 0) {
      throw new Error('event-hubs-event-hub-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for event-hubs-event-hub-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    azureConfig.eventHubByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['event-hubs-include-payloads'], 'event-hubs')
  if (includePayloadCases) {
    azureConfig.includePayloadCases = includePayloadCases
  }

  const staticProperties = parseStaticHeaders(argv['event-hubs-static-properties'], 'event-hubs-static-properties')
  if (staticProperties) {
    azureConfig.staticProperties = staticProperties
  }

  const partitionKeyTemplateRaw = argv['event-hubs-partition-key-template']
  if (partitionKeyTemplateRaw !== undefined) {
    if (typeof partitionKeyTemplateRaw !== 'string') {
      throw new Error('event-hubs-partition-key-template must be a non-empty string.')
    }
    const partitionKeyTemplate = partitionKeyTemplateRaw.trim()
    if (partitionKeyTemplate === '') {
      throw new Error('event-hubs-partition-key-template must be a non-empty string.')
    }
    compileKeyBuilder(partitionKeyTemplate, 'event-hubs')
    azureConfig.partitionKeyTemplate = partitionKeyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['event-hubs-max-batch-size'], 'event-hubs-max-batch-size')
  if (maxBatchSize !== undefined) {
    azureConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['event-hubs-max-batch-delay-ms'], 'event-hubs-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    azureConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'azure-event-hubs',
    ...azureConfig
  }
}

export function parseSilverPulsarEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const serviceUrlRaw = argv['pulsar-silver-service-url']
  const topicRaw = argv['pulsar-silver-topic']

  if (!serviceUrlRaw || !topicRaw) {
    return undefined
  }

  const serviceUrl = typeof serviceUrlRaw === 'string' ? serviceUrlRaw.trim() : ''
  if (serviceUrl === '') {
    throw new Error('pulsar-silver-service-url must be a non-empty string.')
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('pulsar-silver-topic must be a non-empty string.')
  }

  const pulsarConfig: SilverPulsarEventBusConfig = {
    serviceUrl,
    topic
  }

  const tokenRaw = argv['pulsar-silver-token']
  if (tokenRaw !== undefined) {
    if (typeof tokenRaw !== 'string') {
      throw new Error('pulsar-silver-token must be a string.')
    }
    const token = tokenRaw.trim()
    if (token === '') {
      throw new Error('pulsar-silver-token must be a non-empty string.')
    }
    pulsarConfig.token = token
  }

  const topicRoutingRaw = argv['pulsar-silver-topic-routing']
  if (topicRoutingRaw !== undefined) {
    if (typeof topicRoutingRaw !== 'string') {
      throw new Error('pulsar-silver-topic-routing must be a string of recordType:topic entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = topicRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid pulsar-silver-topic-routing entry "${pair}". Expected format recordType:topicName.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedTopic = pair.slice(separatorIndex + 1).trim()

      if (!recordType || !mappedTopic) {
        throw new Error(`Invalid pulsar-silver-topic-routing entry "${pair}". Expected format recordType:topicName.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('pulsar-silver-topic-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for pulsar-silver-topic-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    pulsarConfig.topicByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['pulsar-silver-include-records'], 'pulsar-silver')
  if (includeRecordTypes) {
    pulsarConfig.includeRecordTypes = includeRecordTypes
  }

  const staticProperties = parseStaticHeaders(
    argv['pulsar-silver-static-properties'],
    'pulsar-silver-static-properties',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
  if (staticProperties) {
    pulsarConfig.staticProperties = staticProperties
  }

  const keyTemplateRaw = argv['pulsar-silver-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('pulsar-silver-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('pulsar-silver-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(keyTemplate, 'pulsar-silver')
    pulsarConfig.keyTemplate = keyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['pulsar-silver-max-batch-size'], 'pulsar-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    pulsarConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['pulsar-silver-max-batch-delay-ms'], 'pulsar-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    pulsarConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  const compressionTypeRaw = argv['pulsar-silver-compression-type']
  if (compressionTypeRaw !== undefined) {
    if (typeof compressionTypeRaw !== 'string') {
      throw new Error('pulsar-silver-compression-type must be a string.')
    }
    const compressionType = compressionTypeRaw.trim().toUpperCase()
    if (!['NONE', 'LZ4', 'ZLIB', 'ZSTD', 'SNAPPY'].includes(compressionType)) {
      throw new Error('pulsar-silver-compression-type must be one of NONE,LZ4,ZLIB,ZSTD,SNAPPY.')
    }
    pulsarConfig.compressionType = compressionType as SilverPulsarEventBusConfig['compressionType']
  }

  return {
    provider: 'pulsar-silver',
    ...pulsarConfig
  }
}

export function parseSilverSQSEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const queueUrlRaw = argv['sqs-silver-queue-url']
  const regionRaw = argv['sqs-silver-region']

  if (!queueUrlRaw || !regionRaw) {
    return undefined
  }

  const queueUrl = typeof queueUrlRaw === 'string' ? queueUrlRaw.trim() : ''
  if (queueUrl === '') {
    throw new Error('sqs-silver-queue-url must be a non-empty string.')
  }

  const region = typeof regionRaw === 'string' ? regionRaw.trim() : ''
  if (region === '') {
    throw new Error('sqs-silver-region must be a non-empty string.')
  }

  const sqsConfig: SilverSQSEventBusConfig = {
    queueUrl,
    region
  }

  const queueRoutingRaw = argv['sqs-silver-queue-routing']
  if (queueRoutingRaw !== undefined) {
    if (typeof queueRoutingRaw !== 'string') {
      throw new Error('sqs-silver-queue-routing must be a string of recordType:queueUrl entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = queueRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid sqs-silver-queue-routing entry "${pair}". Expected format recordType:queueUrl.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedQueue = pair.slice(separatorIndex + 1).trim()

      if (!recordType || !mappedQueue) {
        throw new Error(`Invalid sqs-silver-queue-routing entry "${pair}". Expected format recordType:queueUrl.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedQueue
    }

    if (Object.keys(map).length === 0) {
      throw new Error('sqs-silver-queue-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for sqs-silver-queue-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    sqsConfig.queueByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['sqs-silver-include-records'], 'sqs-silver')
  if (includeRecordTypes) {
    sqsConfig.includeRecordTypes = includeRecordTypes
  }

  const accessKeyIdRaw = argv['sqs-silver-access-key-id']
  if (accessKeyIdRaw !== undefined) {
    if (typeof accessKeyIdRaw !== 'string') {
      throw new Error('sqs-silver-access-key-id must be a string.')
    }
    const accessKeyId = accessKeyIdRaw.trim()
    if (accessKeyId === '') {
      throw new Error('sqs-silver-access-key-id must be a non-empty string.')
    }
    sqsConfig.accessKeyId = accessKeyId
  }

  const secretAccessKeyRaw = argv['sqs-silver-secret-access-key']
  if (secretAccessKeyRaw !== undefined) {
    if (typeof secretAccessKeyRaw !== 'string') {
      throw new Error('sqs-silver-secret-access-key must be a string.')
    }
    const secretAccessKey = secretAccessKeyRaw.trim()
    if (secretAccessKey === '') {
      throw new Error('sqs-silver-secret-access-key must be a non-empty string.')
    }
    sqsConfig.secretAccessKey = secretAccessKey
  }

  const sessionTokenRaw = argv['sqs-silver-session-token']
  if (sessionTokenRaw !== undefined) {
    if (typeof sessionTokenRaw !== 'string') {
      throw new Error('sqs-silver-session-token must be a string.')
    }
    const sessionToken = sessionTokenRaw.trim()
    if (sessionToken === '') {
      throw new Error('sqs-silver-session-token must be a non-empty string.')
    }
    sqsConfig.sessionToken = sessionToken
  }

  const staticMessageAttributes = parseStaticHeaders(
    argv['sqs-silver-static-message-attributes'],
    'sqs-silver-static-message-attributes',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
  if (staticMessageAttributes) {
    sqsConfig.staticMessageAttributes = staticMessageAttributes
  }

  const maxBatchSize = parsePositiveInteger(argv['sqs-silver-max-batch-size'], 'sqs-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    sqsConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['sqs-silver-max-batch-delay-ms'], 'sqs-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    sqsConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'sqs-silver',
    ...sqsConfig
  }
}

export function parseSilverAzureEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const connectionStringRaw = argv['event-hubs-silver-connection-string']
  const eventHubNameRaw = argv['event-hubs-silver-event-hub-name']

  if (!connectionStringRaw || !eventHubNameRaw) {
    return undefined
  }

  const connectionString = typeof connectionStringRaw === 'string' ? connectionStringRaw.trim() : ''
  if (connectionString === '') {
    throw new Error('event-hubs-silver-connection-string must be a non-empty string.')
  }

  const eventHubName = typeof eventHubNameRaw === 'string' ? eventHubNameRaw.trim() : ''
  if (eventHubName === '') {
    throw new Error('event-hubs-silver-event-hub-name must be a non-empty string.')
  }

  const azureConfig: SilverAzureEventBusConfig = {
    connectionString,
    eventHubName
  }

  const eventHubRoutingRaw = argv['event-hubs-silver-event-hub-routing']
  if (eventHubRoutingRaw !== undefined) {
    if (typeof eventHubRoutingRaw !== 'string') {
      throw new Error('event-hubs-silver-event-hub-routing must be a string of recordType:eventHubName entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = eventHubRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid event-hubs-silver-event-hub-routing entry "${pair}". Expected format recordType:eventHubName.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedEventHub = pair.slice(separatorIndex + 1).trim()

      if (!recordType || !mappedEventHub) {
        throw new Error(`Invalid event-hubs-silver-event-hub-routing entry "${pair}". Expected format recordType:eventHubName.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedEventHub
    }

    if (Object.keys(map).length === 0) {
      throw new Error('event-hubs-silver-event-hub-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for event-hubs-silver-event-hub-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    azureConfig.eventHubByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['event-hubs-silver-include-records'], 'event-hubs-silver')
  if (includeRecordTypes) {
    azureConfig.includeRecordTypes = includeRecordTypes
  }

  const staticProperties = parseStaticHeaders(
    argv['event-hubs-silver-static-properties'],
    'event-hubs-silver-static-properties',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
  if (staticProperties) {
    azureConfig.staticProperties = staticProperties
  }

  const partitionKeyTemplateRaw = argv['event-hubs-silver-partition-key-template']
  if (partitionKeyTemplateRaw !== undefined) {
    if (typeof partitionKeyTemplateRaw !== 'string') {
      throw new Error('event-hubs-silver-partition-key-template must be a non-empty string.')
    }
    const partitionKeyTemplate = partitionKeyTemplateRaw.trim()
    if (partitionKeyTemplate === '') {
      throw new Error('event-hubs-silver-partition-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(partitionKeyTemplate, 'event-hubs-silver')
    azureConfig.partitionKeyTemplate = partitionKeyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['event-hubs-silver-max-batch-size'], 'event-hubs-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    azureConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['event-hubs-silver-max-batch-delay-ms'], 'event-hubs-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    azureConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'azure-event-hubs-silver',
    ...azureConfig
  }
}

export function parsePubSubEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const projectIdRaw = argv['pubsub-project-id']
  const topicRaw = argv['pubsub-topic']

  if (!projectIdRaw || !topicRaw) {
    return undefined
  }

  const projectId = typeof projectIdRaw === 'string' ? projectIdRaw.trim() : ''
  if (projectId === '') {
    throw new Error('pubsub-project-id must be a non-empty string.')
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('pubsub-topic must be a non-empty string.')
  }

  const pubsubConfig: PubSubEventBusConfig = {
    projectId,
    topic
  }

  const topicRoutingRaw = argv['pubsub-topic-routing']
  if (topicRoutingRaw !== undefined) {
    if (typeof topicRoutingRaw !== 'string') {
      throw new Error('pubsub-topic-routing must be a string of payloadCase:topic entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = topicRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid pubsub-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedTopic = pair.slice(separatorIndex + 1).trim()

      if (!payloadCase || !mappedTopic) {
        throw new Error(`Invalid pubsub-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('pubsub-topic-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for pubsub-topic-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    pubsubConfig.topicByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['pubsub-include-payloads'], 'pubsub')
  if (includePayloadCases) {
    pubsubConfig.includePayloadCases = includePayloadCases
  }

  const staticAttributes = parseStaticHeaders(argv['pubsub-static-attributes'], 'pubsub-static-attributes')
  if (staticAttributes) {
    pubsubConfig.staticAttributes = staticAttributes
  }

  const orderingKeyTemplateRaw = argv['pubsub-ordering-key-template']
  if (orderingKeyTemplateRaw !== undefined) {
    if (typeof orderingKeyTemplateRaw !== 'string') {
      throw new Error('pubsub-ordering-key-template must be a non-empty string.')
    }
    const orderingKeyTemplate = orderingKeyTemplateRaw.trim()
    if (orderingKeyTemplate === '') {
      throw new Error('pubsub-ordering-key-template must be a non-empty string.')
    }
    compileKeyBuilder(orderingKeyTemplate, 'pubsub')
    pubsubConfig.orderingKeyTemplate = orderingKeyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['pubsub-max-batch-size'], 'pubsub-max-batch-size')
  if (maxBatchSize !== undefined) {
    pubsubConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['pubsub-max-batch-delay-ms'], 'pubsub-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    pubsubConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'pubsub',
    ...pubsubConfig
  }
}

export function parseMQTTEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const urlRaw = argv['mqtt-url']
  const topicRaw = argv['mqtt-topic']

  if (!urlRaw || !topicRaw) {
    return undefined
  }

  const url = typeof urlRaw === 'string' ? urlRaw.trim() : ''
  if (url === '') {
    throw new Error('mqtt-url must be a non-empty string.')
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('mqtt-topic must be a non-empty string.')
  }

  const mqttConfig: MQTTEventBusConfig = {
    url,
    topic
  }

  const topicRoutingRaw = argv['mqtt-topic-routing']
  if (topicRoutingRaw !== undefined) {
    if (typeof topicRoutingRaw !== 'string') {
      throw new Error('mqtt-topic-routing must be a string of payloadCase:topic entries separated by commas.')
    }

    const map: Partial<Record<BronzePayloadCase, string>> = {}
    const invalidPayloadCases = new Set<string>()

    const pairs = topicRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid mqtt-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
      }

      const payloadCase = pair.slice(0, separatorIndex).trim()
      const mappedTopic = pair.slice(separatorIndex + 1).trim()

      if (!payloadCase || !mappedTopic) {
        throw new Error(`Invalid mqtt-topic-routing entry "${pair}". Expected format payloadCase:topicName.`)
      }

      const normalizedPayloadCase = normalizePayloadCase(payloadCase)

      if (!normalizedPayloadCase) {
        invalidPayloadCases.add(payloadCase)
        continue
      }

      map[normalizedPayloadCase] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('mqtt-topic-routing must define at least one payloadCase mapping.')
    }

    if (invalidPayloadCases.size > 0) {
      throw new Error(`Unknown payload case(s) for mqtt-topic-routing: ${Array.from(invalidPayloadCases).join(', ')}.`)
    }

    mqttConfig.topicByPayloadCase = map
  }

  const includePayloadCases = parseIncludePayloadCases(argv['mqtt-include-payloads'], 'mqtt')
  if (includePayloadCases) {
    mqttConfig.includePayloadCases = includePayloadCases
  }

  const qosRaw = argv['mqtt-qos']
  if (qosRaw !== undefined) {
    if (typeof qosRaw === 'number') {
      if (qosRaw < 0 || qosRaw > 2) {
        throw new Error('mqtt-qos must be 0, 1, or 2.')
      }
      mqttConfig.qos = qosRaw as 0 | 1 | 2
    } else {
      throw new Error('mqtt-qos must be a number.')
    }
  }

  const retainRaw = argv['mqtt-retain']
  if (retainRaw !== undefined) {
    mqttConfig.retain = parseBooleanOption(retainRaw, 'mqtt-retain')
  }

  const clientIdRaw = argv['mqtt-client-id']
  if (clientIdRaw !== undefined) {
    if (typeof clientIdRaw !== 'string') {
      throw new Error('mqtt-client-id must be a string.')
    }
    const clientId = clientIdRaw.trim()
    if (clientId === '') {
      throw new Error('mqtt-client-id must be a non-empty string.')
    }
    mqttConfig.clientId = clientId
  }

  const usernameRaw = argv['mqtt-username']
  if (usernameRaw !== undefined) {
    if (typeof usernameRaw !== 'string') {
      throw new Error('mqtt-username must be a string.')
    }
    const username = usernameRaw.trim()
    if (username === '') {
      throw new Error('mqtt-username must be a non-empty string.')
    }
    mqttConfig.username = username
  }

  const passwordRaw = argv['mqtt-password']
  if (passwordRaw !== undefined) {
    if (typeof passwordRaw !== 'string') {
      throw new Error('mqtt-password must be a string.')
    }
    const password = passwordRaw.trim()
    if (password === '') {
      throw new Error('mqtt-password must be a non-empty string.')
    }
    mqttConfig.password = password
  }

  const staticUserProperties = parseStaticHeaders(argv['mqtt-static-user-properties'], 'mqtt-static-user-properties')
  if (staticUserProperties) {
    mqttConfig.staticUserProperties = staticUserProperties
  }

  const topicTemplateRaw = argv['mqtt-topic-template']
  if (topicTemplateRaw !== undefined) {
    if (typeof topicTemplateRaw !== 'string') {
      throw new Error('mqtt-topic-template must be a non-empty string.')
    }
    const topicTemplate = topicTemplateRaw.trim()
    if (topicTemplate === '') {
      throw new Error('mqtt-topic-template must be a non-empty string.')
    }
    compileKeyBuilder(topicTemplate, 'mqtt')
    mqttConfig.topicTemplate = topicTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['mqtt-max-batch-size'], 'mqtt-max-batch-size')
  if (maxBatchSize !== undefined) {
    mqttConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['mqtt-max-batch-delay-ms'], 'mqtt-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    mqttConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'mqtt',
    ...mqttConfig
  }
}

export function parseSilverPubSubEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const projectIdRaw = argv['pubsub-silver-project-id']
  const topicRaw = argv['pubsub-silver-topic']

  if (!projectIdRaw || !topicRaw) {
    return undefined
  }

  const projectId = typeof projectIdRaw === 'string' ? projectIdRaw.trim() : ''
  if (projectId === '') {
    throw new Error('pubsub-silver-project-id must be a non-empty string.')
  }

  const topic = typeof topicRaw === 'string' ? topicRaw.trim() : ''
  if (topic === '') {
    throw new Error('pubsub-silver-topic must be a non-empty string.')
  }

  const pubsubConfig: SilverPubSubEventBusConfig = {
    projectId,
    topic
  }

  const topicRoutingRaw = argv['pubsub-silver-topic-routing']
  if (topicRoutingRaw !== undefined) {
    if (typeof topicRoutingRaw !== 'string') {
      throw new Error('pubsub-silver-topic-routing must be a string of recordType:topic entries separated by commas.')
    }

    const map: Partial<Record<SilverRecordType, string>> = {}
    const invalidRecordTypes = new Set<string>()

    const pairs = topicRoutingRaw
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    for (const pair of pairs) {
      const separatorIndex = pair.indexOf(':')
      if (separatorIndex <= 0 || separatorIndex === pair.length - 1) {
        throw new Error(`Invalid pubsub-silver-topic-routing entry "${pair}". Expected format recordType:topicName.`)
      }

      const recordType = pair.slice(0, separatorIndex).trim()
      const mappedTopic = pair.slice(separatorIndex + 1).trim()

      if (!recordType || !mappedTopic) {
        throw new Error(`Invalid pubsub-silver-topic-routing entry "${pair}". Expected format recordType:topicName.`)
      }

      const normalizedRecordType = normalizeSilverRecordType(recordType)

      if (!normalizedRecordType) {
        invalidRecordTypes.add(recordType)
        continue
      }

      map[normalizedRecordType] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('pubsub-silver-topic-routing must define at least one recordType mapping.')
    }

    if (invalidRecordTypes.size > 0) {
      throw new Error(`Unknown record type(s) for pubsub-silver-topic-routing: ${Array.from(invalidRecordTypes).join(', ')}.`)
    }

    pubsubConfig.topicByRecordType = map
  }

  const includeRecordTypes = parseIncludeSilverRecordTypes(argv['pubsub-silver-include-records'], 'pubsub-silver')
  if (includeRecordTypes) {
    pubsubConfig.includeRecordTypes = includeRecordTypes
  }

  const staticAttributes = parseStaticHeaders(
    argv['pubsub-silver-static-attributes'],
    'pubsub-silver-static-attributes',
    RESERVED_SILVER_STATIC_HEADER_KEYS
  )
  if (staticAttributes) {
    pubsubConfig.staticAttributes = staticAttributes
  }

  const orderingKeyTemplateRaw = argv['pubsub-silver-ordering-key-template']
  if (orderingKeyTemplateRaw !== undefined) {
    if (typeof orderingKeyTemplateRaw !== 'string') {
      throw new Error('pubsub-silver-ordering-key-template must be a non-empty string.')
    }
    const orderingKeyTemplate = orderingKeyTemplateRaw.trim()
    if (orderingKeyTemplate === '') {
      throw new Error('pubsub-silver-ordering-key-template must be a non-empty string.')
    }
    compileSilverKeyBuilder(orderingKeyTemplate, 'pubsub-silver')
    pubsubConfig.orderingKeyTemplate = orderingKeyTemplate
  }

  const maxBatchSize = parsePositiveInteger(argv['pubsub-silver-max-batch-size'], 'pubsub-silver-max-batch-size')
  if (maxBatchSize !== undefined) {
    pubsubConfig.maxBatchSize = maxBatchSize
  }

  const maxBatchDelayMs = parsePositiveInteger(argv['pubsub-silver-max-batch-delay-ms'], 'pubsub-silver-max-batch-delay-ms')
  if (maxBatchDelayMs !== undefined) {
    pubsubConfig.maxBatchDelayMs = maxBatchDelayMs
  }

  return {
    provider: 'pubsub-silver',
    ...pubsubConfig
  }
}

export function parseConsoleEventBusConfig(argv: Record<string, any>): EventBusConfig | undefined {
  const enableConsole = argv['console-enable']
  if (!enableConsole) {
    return undefined
  }

  const consoleConfig: ConsoleEventBusConfig = {}

  const prefixRaw = argv['console-prefix']
  if (prefixRaw !== undefined) {
    if (typeof prefixRaw !== 'string') {
      throw new Error('console-prefix must be a string.')
    }
    const prefix = prefixRaw.trim()
    if (prefix === '') {
      throw new Error('console-prefix must be a non-empty string.')
    }
    consoleConfig.prefix = prefix
  }

  const includePayloadCases = parseIncludePayloadCases(argv['console-include-payloads'], 'console')
  if (includePayloadCases) {
    consoleConfig.includePayloadCases = includePayloadCases
  }

  const keyTemplateRaw = argv['console-key-template']
  if (keyTemplateRaw !== undefined) {
    if (typeof keyTemplateRaw !== 'string') {
      throw new Error('console-key-template must be a non-empty string.')
    }
    const keyTemplate = keyTemplateRaw.trim()
    if (keyTemplate === '') {
      throw new Error('console-key-template must be a non-empty string.')
    }
    compileKeyBuilder(keyTemplate, 'console')
    consoleConfig.keyTemplate = keyTemplate
  }

  return {
    provider: 'console',
    ...consoleConfig
  }
}

export type { KafkaEventBusConfig }
