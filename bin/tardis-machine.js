#!/usr/bin/env node
process.env.UWS_HTTP_MAX_HEADERS_SIZE = '20000'
const yargs = require('yargs')
const os = require('os')
const path = require('path')
const cluster = require('cluster')
const numCPUs = require('os').cpus().length
const isDocker = require('is-docker')
const pkg = require('../package.json')
const { parseKafkaEventBusConfig } = require('../dist/eventbus/config')

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
  const eventBusConfig = parseKafkaEventBusConfig(argv)

  const machine = new TardisMachine({
    apiKey: argv['api-key'],
    cacheDir: argv['cache-dir'],
    clearCache: argv['clear-cache'],
    eventBus: eventBusConfig
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
