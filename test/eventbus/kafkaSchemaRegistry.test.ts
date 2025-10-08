import { Kafka } from 'kafkajs'
import { KafkaContainer, StartedKafkaContainer } from '@testcontainers/kafka'
import { GenericContainer, Network, StartedNetwork, StartedTestContainer } from 'testcontainers'
import { fromBinary } from '@bufbuild/protobuf'
import { KafkaEventBus } from '../../src/eventbus/kafka'
import { NormalizedEventSchema, Origin } from '../../src/generated/lakehouse/bronze/v1/normalized_event_pb'
import type { Trade as NormalizedTrade } from 'tardis-dev'

jest.setTimeout(240000)

const topic = 'bronze.schema.test'
const startTimeoutMs = 180000

const baseMeta = {
  source: 'schema-registry-test',
  origin: Origin.REPLAY,
  ingestTimestamp: new Date('2024-01-01T00:00:00.000Z'),
  requestId: 'req-123'
}

describe('Kafka schema registry (bronze)', () => {
  let network: StartedNetwork | undefined
  let kafkaContainer: StartedKafkaContainer | undefined
  let schemaRegistryContainer: StartedTestContainer | undefined
  let kafka: Kafka | undefined
  let brokers: string[] = []
  let schemaRegistryUrl = ''
  let shouldSkip = false

  beforeAll(async () => {
    try {
      console.info('schema-registry test: starting docker network')
      network = await new Network().start()
      console.info('schema-registry test: network started')

      console.info('schema-registry test: starting kafka container')
      kafkaContainer = await new KafkaContainer('confluentinc/cp-kafka:7.5.3')
        .withNetwork(network)
        .withNetworkAliases('kafka')
        .withStartupTimeout(startTimeoutMs)
        .start()
      console.info('schema-registry test: kafka container started')

      brokers = [`${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`]
      kafka = new Kafka({ clientId: 'schema-registry-client', brokers })

      const admin = kafka.admin()
      console.info('schema-registry test: connecting kafka admin')
      await admin.connect()
      await waitForKafkaController(admin)
      await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] })
      await admin.disconnect()
      console.info('schema-registry test: kafka admin ready')

      console.info('schema-registry test: starting schema registry container')
      const startedSchemaRegistry = await new GenericContainer('confluentinc/cp-schema-registry:7.5.3')
        .withNetwork(network)
        .withNetworkAliases('schema-registry')
        .withEnvironment({
          SCHEMA_REGISTRY_HOST_NAME: 'schema-registry',
          SCHEMA_REGISTRY_LISTENERS: 'http://0.0.0.0:8081',
          SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'PLAINTEXT://kafka:9092',
          SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: '1',
          SCHEMA_REGISTRY_OPTS: '-Djava.net.preferIPv4Stack=true'
        })
        .withExposedPorts(8081)
        .withStartupTimeout(startTimeoutMs)
        .start()
      console.info('schema-registry test: schema registry container started')

      schemaRegistryContainer = startedSchemaRegistry
      schemaRegistryUrl = `http://${startedSchemaRegistry.getHost()}:${startedSchemaRegistry.getMappedPort(8081)}`
      console.info('schema-registry test: waiting for schema registry readiness')
      await waitForSchemaRegistry(schemaRegistryUrl)
      console.info('schema-registry test: schema registry ready')
    } catch (error) {
      shouldSkip = true
      console.warn('Skipping Kafka schema registry test:', error)
    }
  })

  afterAll(async () => {
    if (schemaRegistryContainer) {
      await schemaRegistryContainer.stop().catch(() => undefined)
    }
    if (kafkaContainer) {
      await kafkaContainer.stop().catch(() => undefined)
    }
    if (network) {
      await network.stop().catch(() => undefined)
    }
  })

  test('KafkaEventBus registers schema and prefixes payloads with schema id', async () => {
    if (shouldSkip || !kafka) {
      return
    }

    const bus = new KafkaEventBus({
      brokers,
      topic,
      schemaRegistry: {
        url: schemaRegistryUrl
      }
    })

    await bus.start()

    const trade: NormalizedTrade = {
      type: 'trade',
      exchange: 'binance',
      symbol: 'btcusdt',
      timestamp: new Date('2024-01-01T00:00:01.000Z'),
      localTimestamp: new Date('2024-01-01T00:00:01.100Z'),
      price: 42000.5,
      amount: 0.25,
      side: 'buy',
      id: 'trade-1'
    }

    let payload: Buffer | undefined
    const consumer = kafka.consumer({ groupId: `schema-consumer-${Date.now()}` })

    try {
      await bus.publish(trade, baseMeta)
      await bus.flush()

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: true })

      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Timed out waiting for Kafka message')), 120000)

        consumer
          .run({
            eachMessage: async ({ message }) => {
              if (!message.value) {
                return
              }
              payload = message.value
              clearTimeout(timer)
              await consumer.stop().catch(() => undefined)
              resolve()
            }
          })
          .catch(reject)
      })
    } finally {
      await consumer.disconnect().catch(() => undefined)
      await bus.close().catch(() => undefined)
    }

    expect(payload).toBeDefined()
    const buffer = payload!
    expect(buffer.readUInt8(0)).toBe(0)
    const schemaId = buffer.readUInt32BE(1)

    const response = await fetch(`${schemaRegistryUrl}/subjects/${encodeURIComponent(`${topic}-value`)}/versions/latest`)
    expect(response.ok).toBe(true)
    const body = await response.json()
    expect(schemaId).toBe(body.id)

    const decoded = fromBinary(NormalizedEventSchema, buffer.subarray(5))
    expect(decoded.exchange).toBe('binance')
    expect(decoded.meta.request_id).toBe(baseMeta.requestId)
  })
})

async function waitForKafkaController(admin: ReturnType<Kafka['admin']>): Promise<void> {
  for (let attempt = 0; attempt < 60; attempt++) {
    const cluster = await admin.describeCluster()
    if (cluster.brokers && cluster.brokers.length > 0 && cluster.controller !== null) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error('Kafka controller did not become available in time')
}

async function waitForSchemaRegistry(baseUrl: string): Promise<void> {
  const deadline = Date.now() + 120000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/subjects`)
      if (response.ok) {
        return
      }
    } catch (error) {
      // ignore until timeout
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error('Schema Registry did not become ready in time')
}
