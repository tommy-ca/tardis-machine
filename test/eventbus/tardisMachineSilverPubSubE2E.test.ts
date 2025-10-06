import { TardisMachine } from '../../src'

jest.setTimeout(60000)

const PORT = 8108
const HTTP_REPLAY_NORMALIZED_URL = `http://localhost:${PORT}/replay-normalized`
const cacheDir = './.cache-silver-pubsub-e2e'

// Mock the Google Cloud Pub/Sub client to avoid needing real GCP
const mockPublish = jest.fn().mockResolvedValue(['message-id'])
jest.mock('@google-cloud/pubsub', () => ({
  PubSub: jest.fn().mockImplementation(() => ({
    topic: jest.fn().mockReturnValue({
      publish: mockPublish
    })
  }))
}))

let machine: TardisMachine

beforeAll(async () => {
  machine = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY || 'test-key',
    cacheDir,
    silverEventBus: {
      provider: 'pubsub-silver',
      projectId: 'test-project',
      topic: 'silver.events.e2e'
    }
  })
  await machine.start(PORT)
})

afterAll(async () => {
  if (machine) {
    await machine.stop()
  }
})

test('publishes replay-normalized events to Silver Pub/Sub with Buf payloads', async () => {
  const options = {
    exchange: 'bitmex',
    symbols: ['ETHUSD'],
    from: '2019-06-01',
    to: '2019-06-01 00:01',
    dataTypes: ['trade']
  }

  const params = encodeURIComponent(JSON.stringify(options))
  const response = await fetch(`${HTTP_REPLAY_NORMALIZED_URL}?options=${params}`)

  expect(response.status).toBe(200)

  await response.text()

  expect(mockPublish).toHaveBeenCalled()
})

test('does not publish when Silver Pub/Sub is not configured', async () => {
  mockPublish.mockClear()

  const machineWithoutPubSub = new TardisMachine({
    apiKey: process.env.TARDIS_API_KEY || 'test-key',
    cacheDir: `${cacheDir}-no-pubsub`
  })
  await machineWithoutPubSub.start(8097)

  try {
    const options = {
      exchange: 'bitmex',
      symbols: ['ETHUSD'],
      from: '2019-06-01',
      to: '2019-06-01 00:01',
      dataTypes: ['trade']
    }

    const params = encodeURIComponent(JSON.stringify(options))
    const response = await fetch(`http://localhost:8097/replay-normalized?options=${params}`)

    expect(response.status).toBe(200)

    await response.text()

    // Since no Pub/Sub is configured, publish should not be called
    expect(mockPublish).not.toHaveBeenCalled()
  } finally {
    await machineWithoutPubSub.stop()
  }
})
