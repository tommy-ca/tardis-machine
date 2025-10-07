import * as eventbusConfig from '../../src/eventbus/config'
import { parseKafkaEventBusConfig, parseSilverKafkaEventBusConfig } from '../../src/eventbus/config'

describe('event bus config exports', () => {
  test('only exposes kafka parser functions', () => {
    const exportedKeys = Object.keys(eventbusConfig).filter((key) => key !== '__esModule').sort()
    expect(exportedKeys).toEqual(['parseKafkaEventBusConfig', 'parseSilverKafkaEventBusConfig'])
  })

  test('does not expose non-kafka parser helpers', () => {
    const module = eventbusConfig as Record<string, unknown>
    expect(module.parseRabbitMQEventBusConfig).toBeUndefined()
    expect(module.parseKinesisEventBusConfig).toBeUndefined()
    expect(module.parseRedisEventBusConfig).toBeUndefined()
    expect(module.parseSQSEventBusConfig).toBeUndefined()
    expect(module.parsePulsarEventBusConfig).toBeUndefined()
    expect(module.parseAzureEventHubsEventBusConfig).toBeUndefined()
    expect(module.parsePubSubEventBusConfig).toBeUndefined()
    expect(module.parseMQTTEventBusConfig).toBeUndefined()
    expect(module.parseActiveMQEventBusConfig).toBeUndefined()
    expect(module.parseConsoleEventBusConfig).toBeUndefined()
  })
})

describe('parseKafkaEventBusConfig', () => {
  test('returns undefined when kafka brokers or topic missing', () => {
    expect(parseKafkaEventBusConfig({})).toBeUndefined()
    expect(parseKafkaEventBusConfig({ 'kafka-brokers': 'localhost:9092' })).toBeUndefined()
    expect(parseKafkaEventBusConfig({ 'kafka-topic': 'events' })).toBeUndefined()
  })

  test('builds kafka config with routing and sasl', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092,host2:9092',
      'kafka-topic': 'bronze.events',
      'kafka-client-id': 'custom-producer',
      'kafka-ssl': true,
      'kafka-topic-routing': 'trade:bronze.trade,bookChange:bronze.books',
      'kafka-sasl-mechanism': 'plain',
      'kafka-sasl-username': 'user',
      'kafka-sasl-password': 'pass'
    })

    expect(config).toEqual({
      provider: 'kafka',
      brokers: ['localhost:9092', 'host2:9092'],
      topic: 'bronze.events',
      clientId: 'custom-producer',
      ssl: true,
      topicByPayloadCase: {
        trade: 'bronze.trade',
        bookChange: 'bronze.books'
      },
      sasl: {
        mechanism: 'plain',
        username: 'user',
        password: 'pass'
      }
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-max-batch-size': 512,
      'kafka-max-batch-delay-ms': 125
    })

    expect(config).toMatchObject({
      maxBatchSize: 512,
      maxBatchDelayMs: 125
    })
  })

  test('parses meta headers prefix when provided', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-meta-headers-prefix': 'meta.'
    })

    expect(config).toMatchObject({ metaHeadersPrefix: 'meta.' })
  })

  test('parses kafka key template string', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-key-template': '{{exchange}}.{{payloadCase}}.{{symbol}}'
    })

    expect(config).toMatchObject({
      keyTemplate: '{{exchange}}.{{payloadCase}}.{{symbol}}'
    })
  })

  test('parses kafka ack level and idempotent flag', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-acks': 'all',
      'kafka-idempotent': true
    })

    expect(config).toMatchObject({
      acks: -1,
      idempotent: true
    })

    const leader = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-acks': 'leader'
    })

    expect(leader).toMatchObject({ acks: 1 })

    const none = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-acks': 'none'
    })

    expect(none).toMatchObject({ acks: 0 })
  })

  test('parses kafka ssl booleans from strings', () => {
    const enabled = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-ssl': 'true'
    })

    expect(enabled).toMatchObject({ ssl: true })

    const disabled = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-ssl': 'false'
    })

    expect(disabled).toMatchObject({ ssl: false })
  })

  test('parses kafka compression strategy', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-compression': 'gzip'
    })

    expect(config).toMatchObject({ compression: 'gzip' })
  })

  test('parses kafka static headers', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-static-headers': 'env:prod, region:us-east-1,trace-id:abc123 '
    })

    expect(config).toMatchObject({
      staticHeaders: {
        env: 'prod',
        region: 'us-east-1',
        'trace-id': 'abc123'
      }
    })
  })

  test('parses allowed payload cases list', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-include-payloads': 'trade, bookChange, trade'
    })

    expect(config).toMatchObject({ includePayloadCases: ['trade', 'bookChange'] })
  })

  test('accepts snake_case payload names in include list', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-include-payloads': 'book_change, TRADE_BAR, quote'
    })

    expect(config).toMatchObject({
      includePayloadCases: ['bookChange', 'tradeBar', 'quote']
    })
  })

  test('rejects empty meta headers prefix', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-meta-headers-prefix': '   '
      })
    ).toThrow('kafka-meta-headers-prefix must be a non-empty string.')
  })

  test('rejects non-positive batch tuning values', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-max-batch-size': 0
      })
    ).toThrow('kafka-max-batch-size must be a positive integer.')

    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-max-batch-delay-ms': -1
      })
    ).toThrow('kafka-max-batch-delay-ms must be a positive integer.')
  })

  test('rejects unknown compression strategy', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-compression': 'brotli'
      })
    ).toThrow('kafka-compression must be one of none,gzip,snappy,lz4,zstd.')
  })

  test('rejects blank kafka topic strings', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': '   '
      })
    ).toThrow('kafka-topic must be a non-empty string.')
  })

  test('rejects unknown payload case names', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-include-payloads': 'trade, candles'
      })
    ).toThrow('Unknown payload case(s) for kafka-include-payloads: candles.')
  })

  test('rejects unknown payload cases in topic routing', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-topic-routing': 'trade:bronze.trade, candles:bronze.candles'
      })
    ).toThrow('Unknown payload case(s) for kafka-topic-routing: candles.')
  })

  test('accepts snake_case payload names in topic routing', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-topic-routing': 'book_snapshot:bronze.snapshots, grouped_book_snapshot:bronze.grouped'
    })

    expect(config).toMatchObject({
      topicByPayloadCase: {
        bookSnapshot: 'bronze.snapshots',
        groupedBookSnapshot: 'bronze.grouped'
      }
    })
  })

  test('rejects invalid kafka ack levels', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-acks': 'maybe'
      })
    ).toThrow('kafka-acks must be one of all,leader,none,1,0,-1.')
  })

  test('rejects invalid kafka static header entries', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-static-headers': 'env'
      })
    ).toThrow('kafka-static-headers entries must be key:value pairs.')

    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-static-headers': 'payloadCase:overwritten'
      })
    ).toThrow('kafka-static-headers cannot override reserved header "payloadCase".')
  })

  test('rejects unknown key template placeholders', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-key-template': '{{unknown}}'
      })
    ).toThrow('Unknown kafka-key-template placeholder "{{unknown}}".')
  })

  test('throws on invalid topic routing entry', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-topic-routing': 'trade-only'
      })
    ).toThrow('Invalid kafka-topic-routing entry "trade-only". Expected format payloadCase:topicName.')
  })

  test('builds kafka config with schema registry', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-schema-registry-url': 'http://localhost:8081',
      'kafka-schema-registry-auth-username': 'user',
      'kafka-schema-registry-auth-password': 'pass'
    })

    expect(config).toEqual({
      provider: 'kafka',
      brokers: ['localhost:9092'],
      topic: 'bronze.events',
      clientId: 'tardis-machine-publisher',
      schemaRegistry: {
        url: 'http://localhost:8081',
        auth: {
          username: 'user',
          password: 'pass'
        }
      }
    })
  })

  test('builds kafka config with schema registry without auth', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-schema-registry-url': 'http://localhost:8081'
    })

    expect(config).toEqual({
      provider: 'kafka',
      brokers: ['localhost:9092'],
      topic: 'bronze.events',
      clientId: 'tardis-machine-publisher',
      schemaRegistry: {
        url: 'http://localhost:8081'
      }
    })
  })
})

describe('parseSilverKafkaEventBusConfig', () => {
  test('returns undefined when kafka brokers or topic missing', () => {
    expect(parseSilverKafkaEventBusConfig({})).toBeUndefined()
    expect(parseSilverKafkaEventBusConfig({ 'kafka-silver-brokers': 'localhost:9092' })).toBeUndefined()
    expect(parseSilverKafkaEventBusConfig({ 'kafka-silver-topic': 'events' })).toBeUndefined()
  })

  test('builds silver kafka config with routing and sasl', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092,host2:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-client-id': 'silver-producer',
      'kafka-silver-ssl': true,
      'kafka-silver-topic-routing': 'trade:silver.trade,book_change:silver.books',
      'kafka-silver-sasl-mechanism': 'plain',
      'kafka-silver-sasl-username': 'user',
      'kafka-silver-sasl-password': 'pass'
    })

    expect(config).toEqual({
      provider: 'kafka-silver',
      brokers: ['localhost:9092', 'host2:9092'],
      topic: 'silver.records',
      clientId: 'silver-producer',
      ssl: true,
      topicByRecordType: {
        trade: 'silver.trade',
        book_change: 'silver.books'
      },
      sasl: {
        mechanism: 'plain',
        username: 'user',
        password: 'pass'
      }
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-max-batch-size': 512,
      'kafka-silver-max-batch-delay-ms': 125
    })

    expect(config).toMatchObject({
      maxBatchSize: 512,
      maxBatchDelayMs: 125
    })
  })

  test('parses meta headers prefix when provided', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-meta-headers-prefix': 'meta.'
    })

    expect(config).toMatchObject({ metaHeadersPrefix: 'meta.' })
  })

  test('parses kafka key template string', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-key-template': '{{exchange}}.{{recordType}}.{{symbol}}'
    })

    expect(config).toMatchObject({
      keyTemplate: '{{exchange}}.{{recordType}}.{{symbol}}'
    })
  })

  test('parses kafka ack level and idempotent flag', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-acks': 'all',
      'kafka-silver-idempotent': true
    })

    expect(config).toMatchObject({
      acks: -1,
      idempotent: true
    })

    const leader = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-acks': 'leader'
    })

    expect(leader).toMatchObject({ acks: 1 })

    const none = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-acks': 'none'
    })

    expect(none).toMatchObject({ acks: 0 })
  })

  test('parses kafka ssl booleans from strings', () => {
    const enabled = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-ssl': 'true'
    })

    expect(enabled).toMatchObject({ ssl: true })

    const disabled = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-ssl': 'false'
    })

    expect(disabled).toMatchObject({ ssl: false })
  })

  test('parses kafka compression strategy', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-compression': 'gzip'
    })

    expect(config).toMatchObject({ compression: 'gzip' })
  })

  test('parses kafka static headers', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-static-headers': 'env:prod, region:us-east-1,trace-id:abc123 '
    })

    expect(config).toMatchObject({
      staticHeaders: {
        env: 'prod',
        region: 'us-east-1',
        'trace-id': 'abc123'
      }
    })
  })

  test('parses allowed record types list', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-include-records': 'trade, book_change, trade'
    })

    expect(config).toMatchObject({ includeRecordTypes: ['trade', 'book_change'] })
  })

  test('accepts snake_case record names in include list', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-include-records': 'book_snapshot, TRADE_BAR, quote'
    })

    expect(config).toMatchObject({
      includeRecordTypes: ['book_snapshot', 'trade_bar', 'quote']
    })
  })

  test('rejects empty meta headers prefix', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-meta-headers-prefix': '   '
      })
    ).toThrow('kafka-silver-meta-headers-prefix must be a non-empty string.')
  })

  test('rejects non-positive batch tuning values', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-max-batch-size': 0
      })
    ).toThrow('kafka-silver-max-batch-size must be a positive integer.')

    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-max-batch-delay-ms': -1
      })
    ).toThrow('kafka-silver-max-batch-delay-ms must be a positive integer.')
  })

  test('rejects unknown compression strategy', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-compression': 'brotli'
      })
    ).toThrow('kafka-silver-compression must be one of none,gzip,snappy,lz4,zstd.')
  })

  test('rejects blank kafka topic strings', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': '   '
      })
    ).toThrow('kafka-silver-topic must be a non-empty string.')
  })

  test('rejects unknown record type names', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-include-records': 'trade, candles'
      })
    ).toThrow('Unknown record type(s) for kafka-silver-include-records: candles.')
  })

  test('rejects unknown record types in topic routing', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-topic-routing': 'trade:silver.trade, candles:silver.candles'
      })
    ).toThrow('Unknown record type(s) for kafka-silver-topic-routing: candles.')
  })

  test('accepts snake_case record names in topic routing', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-topic-routing': 'book_snapshot:silver.snapshots, grouped_book_snapshot:silver.grouped'
    })

    expect(config).toMatchObject({
      topicByRecordType: {
        book_snapshot: 'silver.snapshots',
        grouped_book_snapshot: 'silver.grouped'
      }
    })
  })

  test('rejects invalid kafka ack levels', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-acks': 'maybe'
      })
    ).toThrow('kafka-silver-acks must be one of all,leader,none,1,0,-1.')
  })

  test('rejects invalid kafka static header entries', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-static-headers': 'env'
      })
    ).toThrow('kafka-silver-static-headers entries must be key:value pairs.')

    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-static-headers': 'recordType:overwritten'
      })
    ).toThrow('kafka-silver-static-headers cannot override reserved header "recordType".')
  })

  test('rejects unknown key template placeholders', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-key-template': '{{unknown}}'
      })
    ).toThrow('Unknown kafka-silver-key-template placeholder "{{unknown}}".')
  })

  test('throws on invalid topic routing entry', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-topic-routing': 'trade-only'
      })
    ).toThrow('Invalid kafka-silver-topic-routing entry "trade-only". Expected format recordType:topic.')
  })

  test('builds silver kafka config with schema registry', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-schema-registry-url': 'http://localhost:8081',
      'kafka-silver-schema-registry-auth-username': 'user',
      'kafka-silver-schema-registry-auth-password': 'pass'
    })

    expect(config).toEqual({
      provider: 'kafka-silver',
      brokers: ['localhost:9092'],
      topic: 'silver.records',
      clientId: 'tardis-machine-silver-publisher',
      schemaRegistry: {
        url: 'http://localhost:8081',
        auth: {
          username: 'user',
          password: 'pass'
        }
      }
    })
  })

  test('builds silver kafka config with schema registry without auth', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-schema-registry-url': 'http://localhost:8081'
    })

    expect(config).toEqual({
      provider: 'kafka-silver',
      brokers: ['localhost:9092'],
      topic: 'silver.records',
      clientId: 'tardis-machine-silver-publisher',
      schemaRegistry: {
        url: 'http://localhost:8081'
      }
    })
  })
})
