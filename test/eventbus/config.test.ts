import {
  parseKafkaEventBusConfig,
  parseRabbitMQEventBusConfig,
  parseKinesisEventBusConfig,
  parseNatsEventBusConfig,
  parseRedisEventBusConfig,
  parseSQSEventBusConfig,
  parsePulsarEventBusConfig,
  parseSilverKafkaEventBusConfig,
  parseSilverPulsarEventBusConfig,
  parseSilverSQSEventBusConfig
} from '../../src/eventbus/config'

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

describe('parseRedisEventBusConfig', () => {
  test('returns undefined when redis url or stream missing', () => {
    expect(parseRedisEventBusConfig({})).toBeUndefined()
    expect(parseRedisEventBusConfig({ 'redis-url': 'redis://localhost:6379' })).toBeUndefined()
    expect(parseRedisEventBusConfig({ 'redis-stream': 'events' })).toBeUndefined()
  })

  test('builds redis config with routing and filtering', () => {
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-stream-routing': 'trade:bronze.trade,bookChange:bronze.books',
      'redis-include-payloads': 'trade,bookChange',
      'redis-static-headers': 'env:prod,region:us-east-1',
      'redis-key-template': '{{exchange}}.{{payloadCase}}.{{symbol}}',
      'redis-max-batch-size': 256,
      'redis-max-batch-delay-ms': 50
    })

    expect(config).toEqual({
      provider: 'redis',
      url: 'redis://localhost:6379',
      stream: 'bronze.events',
      streamByPayloadCase: {
        trade: 'bronze.trade',
        bookChange: 'bronze.books'
      },
      includePayloadCases: ['trade', 'bookChange'],
      staticHeaders: {
        env: 'prod',
        region: 'us-east-1'
      },
      keyTemplate: '{{exchange}}.{{payloadCase}}.{{symbol}}',
      maxBatchSize: 256,
      maxBatchDelayMs: 50
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-max-batch-size': 512,
      'redis-max-batch-delay-ms': 125
    })

    expect(config).toMatchObject({
      maxBatchSize: 512,
      maxBatchDelayMs: 125
    })
  })

  test('parses redis key template string', () => {
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-key-template': '{{exchange}}.{{payloadCase}}.{{symbol}}'
    })

    expect(config).toMatchObject({
      keyTemplate: '{{exchange}}.{{payloadCase}}.{{symbol}}'
    })
  })

  test('parses redis static headers', () => {
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-static-headers': 'env:prod, region:us-east-1,trace-id:abc123 '
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
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-include-payloads': 'trade, bookChange, trade'
    })

    expect(config).toMatchObject({ includePayloadCases: ['trade', 'bookChange'] })
  })

  test('accepts snake_case payload names in include list', () => {
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-include-payloads': 'book_change, TRADE_BAR, quote'
    })

    expect(config).toMatchObject({
      includePayloadCases: ['bookChange', 'tradeBar', 'quote']
    })
  })

  test('accepts snake_case payload names in stream routing', () => {
    const config = parseRedisEventBusConfig({
      'redis-url': 'redis://localhost:6379',
      'redis-stream': 'bronze.events',
      'redis-stream-routing': 'book_snapshot:bronze.snapshots, grouped_book_snapshot:bronze.grouped'
    })

    expect(config).toMatchObject({
      streamByPayloadCase: {
        bookSnapshot: 'bronze.snapshots',
        groupedBookSnapshot: 'bronze.grouped'
      }
    })
  })

  test('rejects non-positive batch tuning values', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-max-batch-size': 0
      })
    ).toThrow('redis-max-batch-size must be a positive integer.')

    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-max-batch-delay-ms': -1
      })
    ).toThrow('redis-max-batch-delay-ms must be a positive integer.')
  })

  test('rejects blank redis stream strings', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': '   '
      })
    ).toThrow('redis-stream must be a non-empty string.')
  })

  test('rejects blank redis url strings', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': '   ',
        'redis-stream': 'events'
      })
    ).toThrow('redis-url must be a non-empty string.')
  })

  test('rejects unknown payload case names', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-include-payloads': 'trade, candles'
      })
    ).toThrow('Unknown payload case(s) for redis-include-payloads: candles.')
  })

  test('rejects unknown payload cases in stream routing', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-stream-routing': 'trade:bronze.trade, candles:bronze.candles'
      })
    ).toThrow('Unknown payload case(s) for redis-stream-routing: candles.')
  })

  test('rejects invalid redis static header entries', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-static-headers': 'env'
      })
    ).toThrow('redis-static-headers entries must be key:value pairs.')

    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-static-headers': 'payloadCase:overwritten'
      })
    ).toThrow('redis-static-headers cannot override reserved header "payloadCase".')
  })

  test('rejects unknown key template placeholders', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-key-template': '{{unknown}}'
      })
    ).toThrow('Unknown redis-key-template placeholder "{{unknown}}".')
  })

  test('throws on invalid stream routing entry', () => {
    expect(() =>
      parseRedisEventBusConfig({
        'redis-url': 'redis://localhost:6379',
        'redis-stream': 'bronze.events',
        'redis-stream-routing': 'trade-only'
      })
    ).toThrow('Invalid redis-stream-routing entry "trade-only". Expected format payloadCase:stream.')
  })
})

describe('parseSilverKafkaEventBusConfig', () => {
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
      schemaRegistry: {
        url: 'http://localhost:8081'
      }
    })
  })
})

describe('parseSilverKafkaEventBusConfig', () => {
  test('returns undefined when kafka silver brokers or topic missing', () => {
    expect(parseSilverKafkaEventBusConfig({})).toBeUndefined()
    expect(parseSilverKafkaEventBusConfig({ 'kafka-silver-brokers': 'localhost:9092' })).toBeUndefined()
    expect(parseSilverKafkaEventBusConfig({ 'kafka-silver-topic': 'events' })).toBeUndefined()
  })

  test('builds silver kafka config with routing and sasl', () => {
    const config = parseSilverKafkaEventBusConfig({
      'kafka-silver-brokers': 'localhost:9092,host2:9092',
      'kafka-silver-topic': 'silver.records',
      'kafka-silver-client-id': 'custom-silver-producer',
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
      clientId: 'custom-silver-producer',
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

  test('rejects invalid silver kafka compression', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-compression': 'brotli'
      })
    ).toThrow('kafka-silver-compression must be one of none,gzip,snappy,lz4,zstd.')
  })

  test('rejects blank silver kafka topic strings', () => {
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

  test('rejects invalid silver kafka ack levels', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-acks': 'maybe'
      })
    ).toThrow('kafka-silver-acks must be one of all,leader,none,1,0,-1.')
  })

  test('rejects invalid silver kafka static header entries', () => {
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

  test('rejects unknown silver key template placeholders', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-key-template': '{{unknown}}'
      })
    ).toThrow('Unknown kafka-silver-key-template placeholder "{{unknown}}".')
  })

  test('throws on invalid silver topic routing entry', () => {
    expect(() =>
      parseSilverKafkaEventBusConfig({
        'kafka-silver-brokers': 'localhost:9092',
        'kafka-silver-topic': 'silver.records',
        'kafka-silver-topic-routing': 'trade-only'
      })
    ).toThrow('Invalid kafka-silver-topic-routing entry "trade-only". Expected format recordType:topic.')
  })
})

describe('parseSQSEventBusConfig', () => {
  test('returns undefined when sqs queue url or region missing', () => {
    expect(parseSQSEventBusConfig({})).toBeUndefined()
    expect(parseSQSEventBusConfig({ 'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue' })).toBeUndefined()
    expect(parseSQSEventBusConfig({ 'sqs-region': 'us-east-1' })).toBeUndefined()
  })

  test('builds sqs config with routing and credentials', () => {
    const config = parseSQSEventBusConfig({
      'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-region': 'us-east-1',
      'sqs-queue-routing':
        'trade:https://sqs.us-east-1.amazonaws.com/123456789012/trade-queue,bookChange:https://sqs.us-east-1.amazonaws.com/123456789012/books-queue',
      'sqs-include-payloads': 'trade,bookChange',
      'sqs-access-key-id': 'AKIAIOSFODNN7EXAMPLE',
      'sqs-secret-access-key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      'sqs-session-token': 'session-token',
      'sqs-static-message-attributes': 'env:prod,region:us-east-1',
      'sqs-max-batch-size': 5,
      'sqs-max-batch-delay-ms': 100
    })

    expect(config).toEqual({
      provider: 'sqs',
      queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      region: 'us-east-1',
      queueByPayloadCase: {
        trade: 'https://sqs.us-east-1.amazonaws.com/123456789012/trade-queue',
        bookChange: 'https://sqs.us-east-1.amazonaws.com/123456789012/books-queue'
      },
      includePayloadCases: ['trade', 'bookChange'],
      accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
      secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      sessionToken: 'session-token',
      staticMessageAttributes: {
        env: 'prod',
        region: 'us-east-1'
      },
      maxBatchSize: 5,
      maxBatchDelayMs: 100
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parseSQSEventBusConfig({
      'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-region': 'us-east-1',
      'sqs-max-batch-size': 8,
      'sqs-max-batch-delay-ms': 200
    })

    expect(config).toMatchObject({
      maxBatchSize: 8,
      maxBatchDelayMs: 200
    })
  })

  test('parses sqs static message attributes', () => {
    const config = parseSQSEventBusConfig({
      'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-region': 'us-east-1',
      'sqs-static-message-attributes': 'env:prod, region:us-east-1,trace-id:abc123 '
    })

    expect(config).toMatchObject({
      staticMessageAttributes: {
        env: 'prod',
        region: 'us-east-1',
        'trace-id': 'abc123'
      }
    })
  })

  test('parses allowed payload cases list', () => {
    const config = parseSQSEventBusConfig({
      'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-region': 'us-east-1',
      'sqs-include-payloads': 'trade, bookChange, trade'
    })

    expect(config).toMatchObject({ includePayloadCases: ['trade', 'bookChange'] })
  })

  test('accepts snake_case payload names in include list', () => {
    const config = parseSQSEventBusConfig({
      'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-region': 'us-east-1',
      'sqs-include-payloads': 'book_change, TRADE_BAR, quote'
    })

    expect(config).toMatchObject({
      includePayloadCases: ['bookChange', 'tradeBar', 'quote']
    })
  })

  test('accepts snake_case payload names in queue routing', () => {
    const config = parseSQSEventBusConfig({
      'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-region': 'us-east-1',
      'sqs-queue-routing':
        'book_snapshot:https://sqs.us-east-1.amazonaws.com/123456789012/snapshots, grouped_book_snapshot:https://sqs.us-east-1.amazonaws.com/123456789012/grouped'
    })

    expect(config).toMatchObject({
      queueByPayloadCase: {
        bookSnapshot: 'https://sqs.us-east-1.amazonaws.com/123456789012/snapshots',
        groupedBookSnapshot: 'https://sqs.us-east-1.amazonaws.com/123456789012/grouped'
      }
    })
  })

  test('rejects non-positive batch tuning values', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-max-batch-size': 0
      })
    ).toThrow('sqs-max-batch-size must be a positive integer.')

    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-max-batch-delay-ms': -1
      })
    ).toThrow('sqs-max-batch-delay-ms must be a positive integer.')
  })

  test('rejects blank sqs queue url strings', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': '   ',
        'sqs-region': 'us-east-1'
      })
    ).toThrow('sqs-queue-url must be a non-empty string.')
  })

  test('rejects blank sqs region strings', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': '   '
      })
    ).toThrow('sqs-region must be a non-empty string.')
  })

  test('rejects unknown payload case names', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-include-payloads': 'trade, candles'
      })
    ).toThrow('Unknown payload case(s) for sqs-include-payloads: candles.')
  })

  test('rejects unknown payload cases in queue routing', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-queue-routing':
          'trade:https://sqs.us-east-1.amazonaws.com/123456789012/trade, candles:https://sqs.us-east-1.amazonaws.com/123456789012/candles'
      })
    ).toThrow('Unknown payload case(s) for sqs-queue-routing: candles.')
  })

  test('rejects invalid sqs static message attribute entries', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-static-message-attributes': 'env'
      })
    ).toThrow('sqs-static-message-attributes entries must be key:value pairs.')

    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-static-message-attributes': 'payloadCase:overwritten'
      })
    ).toThrow('sqs-static-message-attributes cannot override reserved header "payloadCase".')
  })

  test('throws on invalid queue routing entry', () => {
    expect(() =>
      parseSQSEventBusConfig({
        'sqs-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-region': 'us-east-1',
        'sqs-queue-routing': 'trade-only'
      })
    ).toThrow('Invalid sqs-queue-routing entry "trade-only". Expected format payloadCase:queueUrl.')
  })
})

describe('parsePulsarEventBusConfig', () => {
  test('returns undefined when pulsar service url or topic missing', () => {
    expect(parsePulsarEventBusConfig({})).toBeUndefined()
    expect(parsePulsarEventBusConfig({ 'pulsar-service-url': 'pulsar://localhost:6650' })).toBeUndefined()
    expect(parsePulsarEventBusConfig({ 'pulsar-topic': 'events' })).toBeUndefined()
  })

  test('builds pulsar config with routing and token', () => {
    const config = parsePulsarEventBusConfig({
      'pulsar-service-url': 'pulsar://localhost:6650',
      'pulsar-topic': 'bronze.events',
      'pulsar-token': 'token123',
      'pulsar-topic-routing': 'trade:bronze.trade,bookChange:bronze.books',
      'pulsar-include-payloads': 'trade,bookChange',
      'pulsar-static-properties': 'env:prod,region:us-east-1',
      'pulsar-key-template': '{{exchange}}.{{payloadCase}}.{{symbol}}',
      'pulsar-max-batch-size': 256,
      'pulsar-max-batch-delay-ms': 50,
      'pulsar-compression-type': 'LZ4'
    })

    expect(config).toEqual({
      provider: 'pulsar',
      serviceUrl: 'pulsar://localhost:6650',
      topic: 'bronze.events',
      token: 'token123',
      topicByPayloadCase: {
        trade: 'bronze.trade',
        bookChange: 'bronze.books'
      },
      includePayloadCases: ['trade', 'bookChange'],
      staticProperties: {
        env: 'prod',
        region: 'us-east-1'
      },
      keyTemplate: '{{exchange}}.{{payloadCase}}.{{symbol}}',
      maxBatchSize: 256,
      maxBatchDelayMs: 50,
      compressionType: 'LZ4'
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parsePulsarEventBusConfig({
      'pulsar-service-url': 'pulsar://localhost:6650',
      'pulsar-topic': 'bronze.events',
      'pulsar-max-batch-size': 512,
      'pulsar-max-batch-delay-ms': 125
    })

    expect(config).toMatchObject({
      maxBatchSize: 512,
      maxBatchDelayMs: 125
    })
  })

  test('rejects invalid pulsar compression type', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-compression-type': 'BROTLI'
      })
    ).toThrow('pulsar-compression-type must be one of NONE,LZ4,ZLIB,ZSTD,SNAPPY.')
  })

  test('rejects blank pulsar topic strings', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': '   '
      })
    ).toThrow('pulsar-topic must be a non-empty string.')
  })

  test('rejects unknown payload case names', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-include-payloads': 'trade, candles'
      })
    ).toThrow('Unknown payload case(s) for pulsar-include-payloads: candles.')
  })

  test('rejects unknown payload cases in topic routing', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-topic-routing': 'trade:bronze.trade, candles:bronze.candles'
      })
    ).toThrow('Unknown payload case(s) for pulsar-topic-routing: candles.')
  })

  test('accepts snake_case payload names in topic routing', () => {
    const config = parsePulsarEventBusConfig({
      'pulsar-service-url': 'pulsar://localhost:6650',
      'pulsar-topic': 'bronze.events',
      'pulsar-topic-routing': 'book_snapshot:bronze.snapshots, grouped_book_snapshot:bronze.grouped'
    })

    expect(config).toMatchObject({
      topicByPayloadCase: {
        bookSnapshot: 'bronze.snapshots',
        groupedBookSnapshot: 'bronze.grouped'
      }
    })
  })

  test('rejects invalid pulsar static property entries', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-static-properties': 'env'
      })
    ).toThrow('pulsar-static-properties entries must be key:value pairs.')

    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-static-properties': 'payloadCase:overwritten'
      })
    ).toThrow('pulsar-static-properties cannot override reserved header "payloadCase".')
  })

  test('rejects unknown key template placeholders', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-key-template': '{{unknown}}'
      })
    ).toThrow('Unknown pulsar-key-template placeholder "{{unknown}}".')
  })

  test('throws on invalid topic routing entry', () => {
    expect(() =>
      parsePulsarEventBusConfig({
        'pulsar-service-url': 'pulsar://localhost:6650',
        'pulsar-topic': 'bronze.events',
        'pulsar-topic-routing': 'trade-only'
      })
    ).toThrow('Invalid pulsar-topic-routing entry "trade-only". Expected format payloadCase:topicName.')
  })
})

describe('parseSilverPulsarEventBusConfig', () => {
  test('returns undefined when pulsar silver service url or topic missing', () => {
    expect(parseSilverPulsarEventBusConfig({})).toBeUndefined()
    expect(parseSilverPulsarEventBusConfig({ 'pulsar-silver-service-url': 'pulsar://localhost:6650' })).toBeUndefined()
    expect(parseSilverPulsarEventBusConfig({ 'pulsar-silver-topic': 'events' })).toBeUndefined()
  })

  test('builds silver pulsar config with routing and token', () => {
    const config = parseSilverPulsarEventBusConfig({
      'pulsar-silver-service-url': 'pulsar://localhost:6650',
      'pulsar-silver-topic': 'silver.records',
      'pulsar-silver-token': 'token123',
      'pulsar-silver-topic-routing': 'trade:silver.trade,book_change:silver.books',
      'pulsar-silver-include-records': 'trade,book_change',
      'pulsar-silver-static-properties': 'env:prod,region:us-east-1',
      'pulsar-silver-key-template': '{{exchange}}.{{recordType}}.{{symbol}}',
      'pulsar-silver-max-batch-size': 256,
      'pulsar-silver-max-batch-delay-ms': 50,
      'pulsar-silver-compression-type': 'LZ4'
    })

    expect(config).toEqual({
      provider: 'pulsar-silver',
      serviceUrl: 'pulsar://localhost:6650',
      topic: 'silver.records',
      token: 'token123',
      topicByRecordType: {
        trade: 'silver.trade',
        book_change: 'silver.books'
      },
      includeRecordTypes: ['trade', 'book_change'],
      staticProperties: {
        env: 'prod',
        region: 'us-east-1'
      },
      keyTemplate: '{{exchange}}.{{recordType}}.{{symbol}}',
      maxBatchSize: 256,
      maxBatchDelayMs: 50,
      compressionType: 'LZ4'
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parseSilverPulsarEventBusConfig({
      'pulsar-silver-service-url': 'pulsar://localhost:6650',
      'pulsar-silver-topic': 'silver.records',
      'pulsar-silver-max-batch-size': 512,
      'pulsar-silver-max-batch-delay-ms': 125
    })

    expect(config).toMatchObject({
      maxBatchSize: 512,
      maxBatchDelayMs: 125
    })
  })

  test('rejects blank silver pulsar topic strings', () => {
    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': '   '
      })
    ).toThrow('pulsar-silver-topic must be a non-empty string.')
  })

  test('rejects unknown record type names', () => {
    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': 'silver.records',
        'pulsar-silver-include-records': 'trade, candles'
      })
    ).toThrow('Unknown record type(s) for pulsar-silver-include-records: candles.')
  })

  test('rejects unknown record types in topic routing', () => {
    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': 'silver.records',
        'pulsar-silver-topic-routing': 'trade:silver.trade, candles:silver.candles'
      })
    ).toThrow('Unknown record type(s) for pulsar-silver-topic-routing: candles.')
  })

  test('accepts snake_case record names in topic routing', () => {
    const config = parseSilverPulsarEventBusConfig({
      'pulsar-silver-service-url': 'pulsar://localhost:6650',
      'pulsar-silver-topic': 'silver.records',
      'pulsar-silver-topic-routing': 'book_snapshot:silver.snapshots, grouped_book_snapshot:silver.grouped'
    })

    expect(config).toMatchObject({
      topicByRecordType: {
        book_snapshot: 'silver.snapshots',
        grouped_book_snapshot: 'silver.grouped'
      }
    })
  })

  test('rejects invalid silver pulsar static property entries', () => {
    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': 'silver.records',
        'pulsar-silver-static-properties': 'env'
      })
    ).toThrow('pulsar-silver-static-properties entries must be key:value pairs.')

    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': 'silver.records',
        'pulsar-silver-static-properties': 'recordType:overwritten'
      })
    ).toThrow('pulsar-silver-static-properties cannot override reserved header "recordType".')
  })

  test('rejects unknown silver key template placeholders', () => {
    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': 'silver.records',
        'pulsar-silver-key-template': '{{unknown}}'
      })
    ).toThrow('Unknown pulsar-silver-key-template placeholder "{{unknown}}".')
  })

  test('throws on invalid silver topic routing entry', () => {
    expect(() =>
      parseSilverPulsarEventBusConfig({
        'pulsar-silver-service-url': 'pulsar://localhost:6650',
        'pulsar-silver-topic': 'silver.records',
        'pulsar-silver-topic-routing': 'trade-only'
      })
    ).toThrow('Invalid pulsar-silver-topic-routing entry "trade-only". Expected format recordType:topicName.')
  })
})

describe('parseSilverSQSEventBusConfig', () => {
  test('returns undefined when sqs silver queue url or region missing', () => {
    expect(parseSilverSQSEventBusConfig({})).toBeUndefined()
    expect(
      parseSilverSQSEventBusConfig({ 'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue' })
    ).toBeUndefined()
    expect(parseSilverSQSEventBusConfig({ 'sqs-silver-region': 'us-east-1' })).toBeUndefined()
  })

  test('builds silver sqs config with routing and credentials', () => {
    const config = parseSilverSQSEventBusConfig({
      'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-silver-region': 'us-east-1',
      'sqs-silver-queue-routing':
        'trade:https://sqs.us-east-1.amazonaws.com/123456789012/trade-queue,book_change:https://sqs.us-east-1.amazonaws.com/123456789012/books-queue',
      'sqs-silver-include-records': 'trade,book_change',
      'sqs-silver-access-key-id': 'AKIAIOSFODNN7EXAMPLE',
      'sqs-silver-secret-access-key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      'sqs-silver-session-token': 'session-token',
      'sqs-silver-static-message-attributes': 'env:prod,region:us-east-1',
      'sqs-silver-max-batch-size': 5,
      'sqs-silver-max-batch-delay-ms': 100
    })

    expect(config).toEqual({
      provider: 'sqs-silver',
      queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      region: 'us-east-1',
      queueByRecordType: {
        trade: 'https://sqs.us-east-1.amazonaws.com/123456789012/trade-queue',
        book_change: 'https://sqs.us-east-1.amazonaws.com/123456789012/books-queue'
      },
      includeRecordTypes: ['trade', 'book_change'],
      accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
      secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
      sessionToken: 'session-token',
      staticMessageAttributes: {
        env: 'prod',
        region: 'us-east-1'
      },
      maxBatchSize: 5,
      maxBatchDelayMs: 100
    })
  })

  test('applies batch tuning options when provided', () => {
    const config = parseSilverSQSEventBusConfig({
      'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-silver-region': 'us-east-1',
      'sqs-silver-max-batch-size': 8,
      'sqs-silver-max-batch-delay-ms': 200
    })

    expect(config).toMatchObject({
      maxBatchSize: 8,
      maxBatchDelayMs: 200
    })
  })

  test('parses silver sqs static message attributes', () => {
    const config = parseSilverSQSEventBusConfig({
      'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-silver-region': 'us-east-1',
      'sqs-silver-static-message-attributes': 'env:prod, region:us-east-1,trace-id:abc123 '
    })

    expect(config).toMatchObject({
      staticMessageAttributes: {
        env: 'prod',
        region: 'us-east-1',
        'trace-id': 'abc123'
      }
    })
  })

  test('parses allowed record types list', () => {
    const config = parseSilverSQSEventBusConfig({
      'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-silver-region': 'us-east-1',
      'sqs-silver-include-records': 'trade, book_change, trade'
    })

    expect(config).toMatchObject({ includeRecordTypes: ['trade', 'book_change'] })
  })

  test('accepts snake_case record names in include list', () => {
    const config = parseSilverSQSEventBusConfig({
      'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-silver-region': 'us-east-1',
      'sqs-silver-include-records': 'book_change, TRADE_BAR, quote'
    })

    expect(config).toMatchObject({
      includeRecordTypes: ['book_change', 'trade_bar', 'quote']
    })
  })

  test('accepts snake_case record names in queue routing', () => {
    const config = parseSilverSQSEventBusConfig({
      'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
      'sqs-silver-region': 'us-east-1',
      'sqs-silver-queue-routing':
        'book_snapshot:https://sqs.us-east-1.amazonaws.com/123456789012/snapshots, grouped_book_snapshot:https://sqs.us-east-1.amazonaws.com/123456789012/grouped'
    })

    expect(config).toMatchObject({
      queueByRecordType: {
        book_snapshot: 'https://sqs.us-east-1.amazonaws.com/123456789012/snapshots',
        grouped_book_snapshot: 'https://sqs.us-east-1.amazonaws.com/123456789012/grouped'
      }
    })
  })

  test('rejects non-positive batch tuning values', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-max-batch-size': 0
      })
    ).toThrow('sqs-silver-max-batch-size must be a positive integer.')

    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-max-batch-delay-ms': -1
      })
    ).toThrow('sqs-silver-max-batch-delay-ms must be a positive integer.')
  })

  test('rejects blank silver sqs queue url strings', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': '   ',
        'sqs-silver-region': 'us-east-1'
      })
    ).toThrow('sqs-silver-queue-url must be a non-empty string.')
  })

  test('rejects blank silver sqs region strings', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': '   '
      })
    ).toThrow('sqs-silver-region must be a non-empty string.')
  })

  test('rejects unknown record type names', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-include-records': 'trade, candles'
      })
    ).toThrow('Unknown record type(s) for sqs-silver-include-records: candles.')
  })

  test('rejects unknown record types in queue routing', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-queue-routing':
          'trade:https://sqs.us-east-1.amazonaws.com/123456789012/trade, candles:https://sqs.us-east-1.amazonaws.com/123456789012/candles'
      })
    ).toThrow('Unknown record type(s) for sqs-silver-queue-routing: candles.')
  })

  test('rejects invalid silver sqs static message attribute entries', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-static-message-attributes': 'env'
      })
    ).toThrow('sqs-silver-static-message-attributes entries must be key:value pairs.')

    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-static-message-attributes': 'recordType:overwritten'
      })
    ).toThrow('sqs-silver-static-message-attributes cannot override reserved header "recordType".')
  })

  test('throws on invalid silver queue routing entry', () => {
    expect(() =>
      parseSilverSQSEventBusConfig({
        'sqs-silver-queue-url': 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
        'sqs-silver-region': 'us-east-1',
        'sqs-silver-queue-routing': 'trade-only'
      })
    ).toThrow('Invalid sqs-silver-queue-routing entry "trade-only". Expected format recordType:queueUrl.')
  })
})
