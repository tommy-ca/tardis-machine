#!/usr/bin/env node
process.env.UWS_HTTP_MAX_HEADERS_SIZE = '20000'
const yargs = require('yargs')
const os = require('os')
const path = require('path')
const cluster = require('cluster')
const numCPUs = require('os').cpus().length
const isDocker = require('is-docker')
const pkg = require('../package.json')

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
  const eventBusConfig = buildEventBusConfig(argv)

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

function buildEventBusConfig(argv) {
  const brokersRaw = argv['kafka-brokers']
  const topic = argv['kafka-topic']

  if (!brokersRaw || !topic) {
    return undefined
  }

  const brokers = brokersRaw
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean)

  if (brokers.length === 0) {
    throw new Error('Invalid kafka-brokers value. Provide at least one broker URL.')
  }

  const config = {
    provider: 'kafka',
    brokers,
    topic,
    clientId: argv['kafka-client-id'] || 'tardis-machine-publisher',
    ssl: Boolean(argv['kafka-ssl'])
  }

  const topicRouting = argv['kafka-topic-routing']
  if (topicRouting) {
    const pairs = topicRouting
      .split(',')
      .map((pair) => pair.trim())
      .filter(Boolean)

    const map = {}

    for (const pair of pairs) {
      const [payloadCase, mappedTopic] = pair.split(':').map((part) => part?.trim())
      if (!payloadCase || !mappedTopic) {
        throw new Error(
          `Invalid kafka-topic-routing entry "${pair}". Expected format payloadCase:topicName.`
        )
      }
      map[payloadCase] = mappedTopic
    }

    if (Object.keys(map).length === 0) {
      throw new Error('kafka-topic-routing must define at least one payloadCase mapping.')
    }

    config.topicByPayloadCase = map
  }

  const mechanism = argv['kafka-sasl-mechanism']
  if (mechanism) {
    if (!argv['kafka-sasl-username'] || !argv['kafka-sasl-password']) {
      throw new Error('Kafka SASL username and password must be provided when sasl mechanism is set.')
    }
    config.sasl = {
      mechanism,
      username: argv['kafka-sasl-username'],
      password: argv['kafka-sasl-password']
    }
  }

  return config
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
