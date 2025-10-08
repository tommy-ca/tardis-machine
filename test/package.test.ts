describe('package.json files field', () => {
  test('includes schemas directory for schema registry support', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const packageJson = require('../package.json') as { files?: string[] }
    const files = packageJson.files ?? []
    expect(files).toContain('schemas')
  })
})
