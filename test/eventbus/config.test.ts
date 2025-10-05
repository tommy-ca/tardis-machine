import { parseKafkaEventBusConfig } from '../../src/eventbus/config'

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

  test('parses kafka compression strategy', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-compression': 'gzip'
    })

    expect(config).toMatchObject({ compression: 'gzip' })
  })

  test('parses allowed payload cases list', () => {
    const config = parseKafkaEventBusConfig({
      'kafka-brokers': 'localhost:9092',
      'kafka-topic': 'bronze.events',
      'kafka-include-payloads': 'trade, bookChange, trade'
    })

    expect(config).toMatchObject({ includePayloadCases: ['trade', 'bookChange'] })
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

  test('rejects unknown payload case names', () => {
    expect(() =>
      parseKafkaEventBusConfig({
        'kafka-brokers': 'localhost:9092',
        'kafka-topic': 'bronze.events',
        'kafka-include-payloads': 'trade, candles'
      })
    ).toThrow('Unknown payload case(s) for kafka-include-payloads: candles.')
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
})
