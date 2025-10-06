#!/usr/bin/env node
process.env.UWS_HTTP_MAX_HEADERS_SIZE = '20000'
const yargs = require('yargs')
const os = require('os')
const path = require('path')
const cluster = require('cluster')
const numCPUs = require('os').cpus().length
const isDocker = require('is-docker')
const pkg = require('../package.json')
const {
  parseKafkaEventBusConfig,
  parseRabbitMQEventBusConfig,
  parseKinesisEventBusConfig,
  parseNatsEventBusConfig,
  parseRedisEventBusConfig,
  parseSQSEventBusConfig,
  parsePulsarEventBusConfig,
  parseSilverPulsarEventBusConfig,
  parseSilverSQSEventBusConfig,
  parseSilverKafkaEventBusConfig,
  parseSilverRabbitMQEventBusConfig,
  parseSilverKinesisEventBusConfig,
  parseSilverNatsEventBusConfig,
  parseSilverRedisEventBusConfig,
  parseAzureEventHubsEventBusConfig,
  parsePubSubEventBusConfig,
  parseMQTTEventBusConfig,
  parseActiveMQEventBusConfig,
  parseSilverActiveMQEventBusConfig,
  parseSilverMQTTEventBusConfig,
  parseSilverPubSubEventBusConfig,
  parseSilverAzureEventBusConfig,
  parseConsoleEventBusConfig
} = require('../dist/eventbus/config')

const DEFAULT_PORT = 8000
const argv = yargs
  .scriptName('tardis-machine')
  .env('TM_')
  .strict()

  .option('api-key', {
    type: 'string',
    describe: 'API key for tardis.dev API access'
  })
  .option('cache-dir', {
    type: 'string',
    describe: 'Local cache dir path ',
    default: path.join(os.tmpdir(), '.tardis-cache')
  })
  .option('clear-cache', {
    type: 'boolean',
    describe: 'Clear cache dir on startup',
    default: false
  })
  .option('port', {
    type: 'number',
    describe: 'Port to bind server on',
    default: DEFAULT_PORT
  })
  .option('cluster-mode', {
    type: 'boolean',
    describe: 'Run tardis-machine as cluster of Node.js processes',
    default: false
  })

  .option('debug', {
    type: 'boolean',
    describe: 'Enable debug logs.',
    default: false
  })
  .option('kafka-brokers', {
    type: 'string',
    describe: 'Comma separated Kafka broker list for normalized event publishing'
  })
  .option('kafka-topic', {
    type: 'string',
    describe: 'Kafka topic name for normalized events'
  })
  .option('kafka-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('kafka-topic-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:topic pairs overriding the base topic'
  })
  .option('kafka-client-id', {
    type: 'string',
    describe: 'Kafka client id used by tardis-machine publisher'
  })
  .option('kafka-ssl', {
    type: 'boolean',
    describe: 'Enable SSL when connecting to Kafka brokers',
    default: false
  })
  .option('kafka-meta-headers-prefix', {
    type: 'string',
    describe: 'Prefix applied when emitting normalized meta as Kafka headers'
  })
  .option('kafka-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Kafka headers'
  })
  .option('kafka-key-template', {
    type: 'string',
    describe: 'Template for Kafka record keys, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })
  .option('kafka-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per Kafka batch'
  })
  .option('kafka-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })
  .option('kafka-compression', {
    type: 'string',
    choices: ['none', 'gzip', 'snappy', 'lz4', 'zstd'],
    describe: 'Compression codec applied to Kafka batches'
  })
  .option('kafka-acks', {
    type: 'string',
    describe: 'Ack level for Kafka sends (all, leader, none, 1, 0, -1)'
  })
  .option('kafka-idempotent', {
    type: 'boolean',
    describe: 'Enable Kafka idempotent producer semantics'
  })
  .option('kafka-sasl-mechanism', {
    type: 'string',
    choices: ['plain', 'scram-sha-256', 'scram-sha-512'],
    describe: 'Kafka SASL mechanism'
  })
  .option('kafka-sasl-username', {
    type: 'string',
    describe: 'Kafka SASL username'
  })
  .option('kafka-sasl-password', {
    type: 'string',
    describe: 'Kafka SASL password'
  })
  .option('kafka-schema-registry-url', {
    type: 'string',
    describe: 'Schema Registry URL for Kafka publishing'
  })
  .option('kafka-schema-registry-auth-username', {
    type: 'string',
    describe: 'Schema Registry auth username'
  })
  .option('kafka-schema-registry-auth-password', {
    type: 'string',
    describe: 'Schema Registry auth password'
  })

  .option('kafka-silver-brokers', {
    type: 'string',
    describe: 'Comma separated Kafka broker list for silver event publishing'
  })
  .option('kafka-silver-topic', {
    type: 'string',
    describe: 'Kafka topic name for silver events'
  })
  .option('kafka-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish (others dropped)'
  })
  .option('kafka-silver-topic-routing', {
    type: 'string',
    describe: 'Comma separated recordType:topic pairs overriding the base topic'
  })
  .option('kafka-silver-client-id', {
    type: 'string',
    describe: 'Kafka client id used by silver publisher'
  })
  .option('kafka-silver-ssl', {
    type: 'boolean',
    describe: 'Enable SSL when connecting to Kafka brokers for silver',
    default: false
  })
  .option('kafka-silver-meta-headers-prefix', {
    type: 'string',
    describe: 'Prefix applied when emitting normalized meta as Kafka headers for silver'
  })
  .option('kafka-silver-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Kafka headers for silver'
  })
  .option('kafka-silver-key-template', {
    type: 'string',
    describe: 'Template for Kafka record keys for silver, e.g. {{exchange}}/{{recordType}}/{{symbol}}'
  })
  .option('kafka-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per Kafka batch'
  })
  .option('kafka-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })
  .option('kafka-silver-compression', {
    type: 'string',
    choices: ['none', 'gzip', 'snappy', 'lz4', 'zstd'],
    describe: 'Compression codec applied to Kafka batches for silver'
  })
  .option('kafka-silver-acks', {
    type: 'string',
    describe: 'Ack level for Kafka sends for silver (all, leader, none, 1, 0, -1)'
  })
  .option('kafka-silver-idempotent', {
    type: 'boolean',
    describe: 'Enable Kafka idempotent producer semantics for silver'
  })
  .option('kafka-silver-sasl-mechanism', {
    type: 'string',
    choices: ['plain', 'scram-sha-256', 'scram-sha-512'],
    describe: 'Kafka SASL mechanism for silver'
  })
  .option('kafka-silver-sasl-username', {
    type: 'string',
    describe: 'Kafka SASL username for silver'
  })
  .option('kafka-silver-sasl-password', {
    type: 'string',
    describe: 'Kafka SASL password for silver'
  })
  .option('kafka-silver-schema-registry-url', {
    type: 'string',
    describe: 'Schema Registry URL for silver Kafka publishing'
  })
  .option('kafka-silver-schema-registry-auth-username', {
    type: 'string',
    describe: 'Schema Registry auth username for silver'
  })
  .option('kafka-silver-schema-registry-auth-password', {
    type: 'string',
    describe: 'Schema Registry auth password for silver'
  })

  .option('rabbitmq-silver-url', {
    type: 'string',
    describe: 'RabbitMQ connection URL for silver event publishing'
  })
  .option('rabbitmq-silver-exchange', {
    type: 'string',
    describe: 'RabbitMQ exchange name for silver events'
  })
  .option('rabbitmq-silver-exchange-type', {
    type: 'string',
    choices: ['direct', 'topic', 'headers', 'fanout'],
    describe: 'RabbitMQ exchange type for silver',
    default: 'direct'
  })
  .option('rabbitmq-silver-routing-key-template', {
    type: 'string',
    describe: 'Template for RabbitMQ routing keys for silver, e.g. {{exchange}}.{{recordType}}.{{symbol}}'
  })
  .option('rabbitmq-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('rabbitmq-silver-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static RabbitMQ headers for silver'
  })

  .option('kinesis-silver-stream-name', {
    type: 'string',
    describe: 'Kinesis stream name for silver events'
  })
  .option('kinesis-silver-region', {
    type: 'string',
    describe: 'AWS region for Kinesis stream for silver'
  })
  .option('kinesis-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('kinesis-silver-stream-routing', {
    type: 'string',
    describe: 'Comma separated recordType:streamName pairs overriding the base stream for silver'
  })
  .option('kinesis-silver-access-key-id', {
    type: 'string',
    describe: 'AWS access key ID for Kinesis silver'
  })
  .option('kinesis-silver-secret-access-key', {
    type: 'string',
    describe: 'AWS secret access key for Kinesis silver'
  })
  .option('kinesis-silver-session-token', {
    type: 'string',
    describe: 'AWS session token for temporary Kinesis credentials for silver'
  })
  .option('kinesis-silver-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Kinesis metadata for silver'
  })
  .option('kinesis-silver-partition-key-template', {
    type: 'string',
    describe: 'Template for Kinesis partition keys for silver, e.g. {{exchange}}/{{recordType}}/{{symbol}}'
  })
  .option('kinesis-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per Kinesis batch'
  })
  .option('kinesis-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })

  .option('nats-silver-servers', {
    type: 'string',
    describe: 'Comma separated NATS server URLs for silver event publishing'
  })
  .option('nats-silver-subject', {
    type: 'string',
    describe: 'NATS subject for silver events'
  })
  .option('nats-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('nats-silver-subject-routing', {
    type: 'string',
    describe: 'Comma separated recordType:subject pairs overriding the base subject for silver'
  })
  .option('nats-silver-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static NATS headers for silver'
  })
  .option('nats-silver-subject-template', {
    type: 'string',
    describe: 'Template for NATS subjects for silver, e.g. {{exchange}}.{{recordType}}.{{symbol}}'
  })

  .option('rabbitmq-url', {
    type: 'string',
    describe: 'RabbitMQ connection URL for normalized event publishing'
  })
  .option('rabbitmq-exchange', {
    type: 'string',
    describe: 'RabbitMQ exchange name for normalized events'
  })
  .option('rabbitmq-exchange-type', {
    type: 'string',
    choices: ['direct', 'topic', 'headers', 'fanout'],
    describe: 'RabbitMQ exchange type',
    default: 'direct'
  })
  .option('rabbitmq-routing-key-template', {
    type: 'string',
    describe: 'Template for RabbitMQ routing keys, e.g. {{exchange}}.{{payloadCase}}.{{symbol}}'
  })
  .option('rabbitmq-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('rabbitmq-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static RabbitMQ headers'
  })

  .option('kinesis-stream-name', {
    type: 'string',
    describe: 'Kinesis stream name for normalized events'
  })
  .option('kinesis-region', {
    type: 'string',
    describe: 'AWS region for Kinesis stream'
  })
  .option('kinesis-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('kinesis-stream-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:streamName pairs overriding the base stream'
  })
  .option('kinesis-access-key-id', {
    type: 'string',
    describe: 'AWS access key ID for Kinesis'
  })
  .option('kinesis-secret-access-key', {
    type: 'string',
    describe: 'AWS secret access key for Kinesis'
  })
  .option('kinesis-session-token', {
    type: 'string',
    describe: 'AWS session token for temporary Kinesis credentials'
  })
  .option('kinesis-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Kinesis metadata'
  })
  .option('kinesis-partition-key-template', {
    type: 'string',
    describe: 'Template for Kinesis partition keys, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })
  .option('kinesis-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per Kinesis batch'
  })
  .option('kinesis-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })

  .option('nats-servers', {
    type: 'string',
    describe: 'Comma separated NATS server URLs for normalized event publishing'
  })
  .option('nats-subject', {
    type: 'string',
    describe: 'NATS subject for normalized events'
  })
  .option('nats-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('nats-subject-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:subject pairs overriding the base subject'
  })
  .option('nats-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static NATS headers'
  })
  .option('nats-subject-template', {
    type: 'string',
    describe: 'Template for NATS subjects, e.g. {{exchange}}.{{payloadCase}}.{{symbol}}'
  })

  .option('redis-url', {
    type: 'string',
    describe: 'Redis connection URL for normalized event publishing'
  })
  .option('redis-stream', {
    type: 'string',
    describe: 'Redis stream name for normalized events'
  })
  .option('redis-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('redis-stream-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:stream pairs overriding the base stream'
  })
  .option('redis-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Redis metadata'
  })
  .option('redis-key-template', {
    type: 'string',
    describe: 'Template for Redis stream keys, e.g. {{exchange}}.{{payloadCase}}.{{symbol}}'
  })
  .option('redis-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per Redis batch'
  })
  .option('redis-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })

  .option('sqs-queue-url', {
    type: 'string',
    describe: 'SQS queue URL for normalized event publishing'
  })
  .option('sqs-region', {
    type: 'string',
    describe: 'AWS region for SQS queue'
  })
  .option('sqs-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('sqs-queue-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:queueUrl pairs overriding the base queue'
  })
  .option('sqs-access-key-id', {
    type: 'string',
    describe: 'AWS access key ID for SQS'
  })
  .option('sqs-secret-access-key', {
    type: 'string',
    describe: 'AWS secret access key for SQS'
  })
  .option('sqs-session-token', {
    type: 'string',
    describe: 'AWS session token for temporary SQS credentials'
  })
  .option('sqs-static-message-attributes', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static SQS message attributes'
  })
  .option('sqs-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per SQS batch'
  })
  .option('sqs-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })

  .option('pulsar-service-url', {
    type: 'string',
    describe: 'Pulsar service URL for normalized event publishing'
  })
  .option('pulsar-topic', {
    type: 'string',
    describe: 'Pulsar topic name for normalized events'
  })
  .option('pulsar-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('pulsar-topic-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:topic pairs overriding the base topic'
  })
  .option('pulsar-token', {
    type: 'string',
    describe: 'Pulsar authentication token'
  })
  .option('pulsar-static-properties', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Pulsar properties'
  })
  .option('pulsar-key-template', {
    type: 'string',
    describe: 'Template for Pulsar message keys, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })
  .option('pulsar-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per Pulsar batch'
  })
  .option('pulsar-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })
  .option('pulsar-compression-type', {
    type: 'string',
    choices: ['NONE', 'LZ4', 'ZLIB', 'ZSTD', 'SNAPPY'],
    describe: 'Compression type for Pulsar messages'
  })

  .option('event-hubs-connection-string', {
    type: 'string',
    describe: 'Azure Event Hubs connection string for normalized event publishing'
  })
  .option('event-hubs-event-hub-name', {
    type: 'string',
    describe: 'Azure Event Hub name for normalized events'
  })
  .option('event-hubs-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('event-hubs-event-hub-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:eventHubName pairs overriding the base event hub'
  })
  .option('event-hubs-static-properties', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Event Hubs properties'
  })
  .option('event-hubs-partition-key-template', {
    type: 'string',
    describe: 'Template for Event Hubs partition keys, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })
  .option('event-hubs-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per Event Hubs batch'
  })
  .option('event-hubs-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })

  .option('pubsub-project-id', {
    type: 'string',
    describe: 'Google Cloud Project ID for Pub/Sub publishing'
  })
  .option('pubsub-topic', {
    type: 'string',
    describe: 'Pub/Sub topic name for normalized events'
  })
  .option('pubsub-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('pubsub-topic-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:topic pairs overriding the base topic'
  })
  .option('pubsub-static-attributes', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Pub/Sub attributes'
  })
  .option('pubsub-ordering-key-template', {
    type: 'string',
    describe: 'Template for Pub/Sub ordering keys, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })
  .option('pubsub-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per Pub/Sub batch'
  })
  .option('pubsub-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })

  .option('mqtt-url', {
    type: 'string',
    describe: 'MQTT broker URL for normalized event publishing'
  })
  .option('mqtt-topic', {
    type: 'string',
    describe: 'MQTT topic name for normalized events'
  })
  .option('mqtt-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('mqtt-topic-routing', {
    type: 'string',
    describe: 'Comma separated payloadCase:topic pairs overriding the base topic'
  })
  .option('mqtt-qos', {
    type: 'number',
    choices: [0, 1, 2],
    describe: 'MQTT QoS level',
    default: 0
  })
  .option('mqtt-retain', {
    type: 'boolean',
    describe: 'Set retain flag on MQTT messages',
    default: false
  })
  .option('mqtt-client-id', {
    type: 'string',
    describe: 'MQTT client ID'
  })
  .option('mqtt-username', {
    type: 'string',
    describe: 'MQTT username for authentication'
  })
  .option('mqtt-password', {
    type: 'string',
    describe: 'MQTT password for authentication'
  })
  .option('mqtt-static-user-properties', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static MQTT user properties'
  })
  .option('mqtt-topic-template', {
    type: 'string',
    describe: 'Template for MQTT topic, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })
  .option('mqtt-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of bronze events per MQTT batch'
  })
  .option('mqtt-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush'
  })

  .option('activemq-url', {
    type: 'string',
    describe: 'ActiveMQ connection URL for normalized event publishing'
  })
  .option('activemq-destination', {
    type: 'string',
    describe: 'ActiveMQ destination (queue or topic) for normalized events'
  })
  .option('activemq-destination-type', {
    type: 'string',
    choices: ['queue', 'topic'],
    describe: 'ActiveMQ destination type',
    default: 'queue'
  })
  .option('activemq-routing-key-template', {
    type: 'string',
    describe: 'Template for ActiveMQ routing keys, e.g. {{exchange}}.{{payloadCase}}.{{symbol}}'
  })
  .option('activemq-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish (others dropped)'
  })
  .option('activemq-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static ActiveMQ headers'
  })

  .option('console-enable', {
    type: 'boolean',
    describe: 'Enable console output for normalized events (for debugging)'
  })
  .option('console-prefix', {
    type: 'string',
    describe: 'Prefix for console output'
  })
  .option('console-include-payloads', {
    type: 'string',
    describe: 'Comma separated payload cases to publish to console (others dropped)'
  })
  .option('console-key-template', {
    type: 'string',
    describe: 'Template for console keys, e.g. {{exchange}}/{{payloadCase}}/{{symbol}}'
  })

  .option('pubsub-silver-project-id', {
    type: 'string',
    describe: 'Google Cloud Project ID for Pub/Sub silver publishing'
  })
  .option('pubsub-silver-topic', {
    type: 'string',
    describe: 'Pub/Sub topic name for silver events'
  })
  .option('pubsub-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('pubsub-silver-topic-routing', {
    type: 'string',
    describe: 'Comma separated recordType:topic pairs overriding the base topic for silver'
  })
  .option('pubsub-silver-static-attributes', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Pub/Sub attributes for silver'
  })
  .option('pubsub-silver-ordering-key-template', {
    type: 'string',
    describe: 'Template for Pub/Sub ordering keys for silver, e.g. {{exchange}}/{{recordType}}/{{symbol}}'
  })
  .option('pubsub-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per Pub/Sub batch'
  })
  .option('pubsub-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })

  .option('mqtt-silver-url', {
    type: 'string',
    describe: 'MQTT broker URL for silver event publishing'
  })
  .option('mqtt-silver-topic', {
    type: 'string',
    describe: 'MQTT topic name for silver events'
  })
  .option('mqtt-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('mqtt-silver-topic-routing', {
    type: 'string',
    describe: 'Comma separated recordType:topic pairs overriding the base topic for silver'
  })
  .option('mqtt-silver-qos', {
    type: 'number',
    choices: [0, 1, 2],
    describe: 'MQTT QoS level for silver',
    default: 0
  })
  .option('mqtt-silver-retain', {
    type: 'boolean',
    describe: 'Set retain flag on MQTT messages for silver',
    default: false
  })
  .option('mqtt-silver-client-id', {
    type: 'string',
    describe: 'MQTT client ID for silver'
  })
  .option('mqtt-silver-username', {
    type: 'string',
    describe: 'MQTT username for authentication for silver'
  })
  .option('mqtt-silver-password', {
    type: 'string',
    describe: 'MQTT password for authentication for silver'
  })
  .option('mqtt-silver-static-user-properties', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static MQTT user properties for silver'
  })
  .option('mqtt-silver-topic-template', {
    type: 'string',
    describe: 'Template for MQTT topic for silver, e.g. {{exchange}}/{{recordType}}/{{symbol}}'
  })
  .option('mqtt-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per MQTT batch'
  })
  .option('mqtt-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })

  .option('activemq-silver-url', {
    type: 'string',
    describe: 'ActiveMQ connection URL for silver event publishing'
  })
  .option('activemq-silver-destination', {
    type: 'string',
    describe: 'ActiveMQ destination (queue or topic) for silver events'
  })
  .option('activemq-silver-destination-type', {
    type: 'string',
    choices: ['queue', 'topic'],
    describe: 'ActiveMQ destination type for silver',
    default: 'queue'
  })
  .option('activemq-silver-routing-key-template', {
    type: 'string',
    describe: 'Template for ActiveMQ routing keys for silver, e.g. {{exchange}}.{{recordType}}.{{symbol}}'
  })
  .option('activemq-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('activemq-silver-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static ActiveMQ headers for silver'
  })

  .option('redis-silver-url', {
    type: 'string',
    describe: 'Redis connection URL for silver event publishing'
  })
  .option('redis-silver-stream', {
    type: 'string',
    describe: 'Redis stream name for silver events'
  })
  .option('redis-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('redis-silver-stream-routing', {
    type: 'string',
    describe: 'Comma separated recordType:stream pairs overriding the base stream for silver'
  })
  .option('redis-silver-static-headers', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Redis metadata for silver'
  })
  .option('redis-silver-key-template', {
    type: 'string',
    describe: 'Template for Redis stream keys for silver, e.g. {{exchange}}.{{recordType}}.{{symbol}}'
  })
  .option('redis-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per Redis batch'
  })
  .option('redis-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })

  .option('pulsar-silver-service-url', {
    type: 'string',
    describe: 'Pulsar service URL for silver event publishing'
  })
  .option('pulsar-silver-topic', {
    type: 'string',
    describe: 'Pulsar topic name for silver events'
  })
  .option('pulsar-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('pulsar-silver-topic-routing', {
    type: 'string',
    describe: 'Comma separated recordType:topic pairs overriding the base topic for silver'
  })
  .option('pulsar-silver-token', {
    type: 'string',
    describe: 'Pulsar authentication token for silver'
  })
  .option('pulsar-silver-static-properties', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Pulsar properties for silver'
  })
  .option('pulsar-silver-key-template', {
    type: 'string',
    describe: 'Template for Pulsar message keys for silver, e.g. {{exchange}}/{{recordType}}/{{symbol}}'
  })
  .option('pulsar-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per Pulsar batch'
  })
  .option('pulsar-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })
  .option('pulsar-silver-compression-type', {
    type: 'string',
    choices: ['NONE', 'LZ4', 'ZLIB', 'ZSTD', 'SNAPPY'],
    describe: 'Compression type for Pulsar messages for silver'
  })

  .option('sqs-silver-queue-url', {
    type: 'string',
    describe: 'SQS queue URL for silver event publishing'
  })
  .option('sqs-silver-region', {
    type: 'string',
    describe: 'AWS region for SQS queue for silver'
  })
  .option('sqs-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('sqs-silver-queue-routing', {
    type: 'string',
    describe: 'Comma separated recordType:queueUrl pairs overriding the base queue for silver'
  })
  .option('sqs-silver-access-key-id', {
    type: 'string',
    describe: 'AWS access key ID for SQS silver'
  })
  .option('sqs-silver-secret-access-key', {
    type: 'string',
    describe: 'AWS secret access key for SQS silver'
  })
  .option('sqs-silver-session-token', {
    type: 'string',
    describe: 'AWS session token for temporary SQS credentials for silver'
  })
  .option('sqs-silver-static-message-attributes', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static SQS message attributes for silver'
  })
  .option('sqs-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per SQS batch'
  })
  .option('sqs-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })

  .option('event-hubs-silver-connection-string', {
    type: 'string',
    describe: 'Azure Event Hubs connection string for silver event publishing'
  })
  .option('event-hubs-silver-event-hub-name', {
    type: 'string',
    describe: 'Azure Event Hub name for silver events'
  })
  .option('event-hubs-silver-include-records', {
    type: 'string',
    describe: 'Comma separated record types to publish for silver (others dropped)'
  })
  .option('event-hubs-silver-event-hub-routing', {
    type: 'string',
    describe: 'Comma separated recordType:eventHubName pairs overriding the base event hub for silver'
  })
  .option('event-hubs-silver-static-properties', {
    type: 'string',
    describe: 'Comma separated key:value pairs applied as static Event Hubs properties for silver'
  })
  .option('event-hubs-silver-partition-key-template', {
    type: 'string',
    describe: 'Template for Event Hubs partition keys for silver, e.g. {{exchange}}/{{recordType}}/{{symbol}}'
  })
  .option('event-hubs-silver-max-batch-size', {
    type: 'number',
    describe: 'Maximum number of silver events per Event Hubs batch'
  })
  .option('event-hubs-silver-max-batch-delay-ms', {
    type: 'number',
    describe: 'Maximum milliseconds events can wait before forced flush for silver'
  })

  .help()
  .version()
  .usage('$0 [options]')
  .example('$0 --api-key=YOUR_API_KEY')
  .epilogue('See https://docs.tardis.dev/api/tardis-machine for more information.')
  .detectLocale(false).argv

// if port ENV is defined use it otherwise use provided options
const port = process.env.PORT ? +process.env.PORT : argv['port']
const enableDebug = argv['debug']

if (enableDebug) {
  process.env.DEBUG = 'tardis-dev:machine*,tardis-dev:realtime*'
}

const { TardisMachine } = require('../dist')

async function start() {
  const eventBusConfig =
    parseKafkaEventBusConfig(argv) ||
    parseRabbitMQEventBusConfig(argv) ||
    parseKinesisEventBusConfig(argv) ||
    parseNatsEventBusConfig(argv) ||
    parseRedisEventBusConfig(argv) ||
    parseSQSEventBusConfig(argv) ||
    parsePulsarEventBusConfig(argv) ||
    parseAzureEventHubsEventBusConfig(argv) ||
    parsePubSubEventBusConfig(argv) ||
    parseMQTTEventBusConfig(argv) ||
    parseActiveMQEventBusConfig(argv) ||
    parseConsoleEventBusConfig(argv)

  const silverEventBusConfig =
    parseSilverKafkaEventBusConfig(argv) ||
    parseSilverRabbitMQEventBusConfig(argv) ||
    parseSilverKinesisEventBusConfig(argv) ||
    parseSilverNatsEventBusConfig(argv) ||
    parseSilverRedisEventBusConfig(argv) ||
    parseSilverPulsarEventBusConfig(argv) ||
    parseSilverSQSEventBusConfig(argv) ||
    parseSilverAzureEventBusConfig(argv) ||
    parseSilverPubSubEventBusConfig(argv) ||
    parseSilverMQTTEventBusConfig(argv) ||
    parseSilverActiveMQEventBusConfig(argv)

  const machine = new TardisMachine({
    apiKey: argv['api-key'],
    cacheDir: argv['cache-dir'],
    clearCache: argv['clear-cache'],
    eventBus: eventBusConfig,
    silverEventBus: silverEventBusConfig
  })
  let suffix = ''

  const runAsCluster = argv['cluster-mode']
  if (runAsCluster) {
    cluster.schedulingPolicy = cluster.SCHED_RR

    suffix = '(cluster mode)'
    if (cluster.isMaster) {
      for (let i = 0; i < numCPUs; i++) {
        cluster.fork()
      }
    } else {
      await machine.start(port)
    }
  } else {
    await machine.start(port)
  }

  if (!cluster.isMaster) {
    return
  }

  if (isDocker() && !process.env.RUNKIT_HOST) {
    console.log(`tardis-machine server v${pkg.version} is running inside Docker container ${suffix}`)
  } else {
    console.log(`tardis-machine server v${pkg.version} is running ${suffix}`)
    console.log(`HTTP port: ${port}`)
    console.log(`WS port: ${port + 1}`)
  }

  console.log(`See https://docs.tardis.dev/api/tardis-machine for more information.`)
}

start()

process
  .on('unhandledRejection', (reason, p) => {
    console.error('Unhandled Rejection at Promise', reason, p)
  })
  .on('uncaughtException', (err) => {
    console.error('Uncaught Exception thrown', err)
    process.exit(1)
  })
