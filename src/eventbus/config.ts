import type {
  BronzePayloadCase,
  SilverRecordType,
  EventBusConfig,
  KafkaEventBusConfig,
  SilverKafkaEventBusConfig
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
  } else {
    kafkaConfig.clientId = 'tardis-machine-silver-publisher'
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
