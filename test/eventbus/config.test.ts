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
