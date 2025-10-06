const { TardisMachine } = require('./dist')

const tm = new TardisMachine({ cacheDir: './.cache' })

tm.start(0)
  .then((port) => {
    console.log('started on', port)
    setTimeout(() => tm.stop(), 5000)
  })
  .catch((e) => console.error(e))
