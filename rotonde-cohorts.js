
const mkdirp = require('mkdirp')
const DatArchive = require('node-dat-archive')
const WebDB = require('@beaker/webdb')

const webdbDir = './db'
mkdirp.sync(webdbDir)

const portalDefinition = {
  // uses JSONSchema v6
  schema: {
    type: 'object',
    properties: {
      name: {
        type: 'string'
      },
      port: {
        type: 'array',
        items: {
          type: 'string'
        }
      }
    },
    required: [ 'name' ]
  },

  // secondary indexes for fast queries (optional)
  index: [ 'name' ],

  // files to index
  filePattern: [
    '/portal.json'
  ]
}

var webdbMasterCohort = new WebDB(`${webdbDir}/masterCohort`, { DatArchive })
webdbMasterCohort.define('masterCohort', portalDefinition)
const webdbMasterCohortUrl = 'dat://0c36c5c3b32f8c0b74f36d41344af2b99275c05701a2ef83a211340af987bc0e/'

var webdbCohorts = new WebDB(`${webdbDir}/cohorts`, { DatArchive })
webdbCohorts.define('cohorts', portalDefinition)

async function processMasterCohortList () {
  const portals = await webdbMasterCohort.masterCohort.toArray()
  const { name, port } = portals[0]
  console.log('Master Cohort:', name)
  for (const cohortUrl of port) {
    console.log('Add cohort portal:', cohortUrl)
    try {
      await webdbCohorts.addSource(cohortUrl)
    } catch (err) {
      console.error('Error', err)
    }
  }
}

function sleep (seconds) {
  const promise = new Promise((resolve, reject) => {
    setTimeout(resolve, seconds * 1000)
  })
  return promise
}

async function processCohortPortals () {
  const cohortPortals = await webdbCohorts.cohorts.toArray()
  for (const portal of cohortPortals) {
    const { name, port } = portal
    if (name.match(/^cohort-/)) {
      console.log('Portal:', name)
      const webdbCohortPortals = new WebDB(`${webdbDir}/${name}`, { DatArchive })
      webdbCohortPortals.define('portals', portalDefinition)
      await webdbCohortPortals.open()
      let i = 0
      for (const portalUrl of port) {
        const num = i++
        console.log(`  ${num}:`, portalUrl)
        try {
          await webdbCohortPortals.addSource(portalUrl)
        } catch (err) {
          console.log('Error', err)
          console.log(`  Timeout: ${num}`)
        }
      }
      console.log('Collecting data for 10 seconds')
      webdbCohortPortals.on('index-updated', async ({ url }, version) => {
        console.log('Table was updated for', url, 'at version', version)
        // await listPortals()
      })
      await sleep(10)
      await webdbCohortPortals.close()
    }
  }
}

async function run () {
  await webdbMasterCohort.open()
  await webdbCohorts.open()
  await webdbMasterCohort.addSource(webdbMasterCohortUrl)
  console.log('Master portal list:')
  await processMasterCohortList()
  await processCohortPortals()
  console.log('Done.')
  process.exit(0)
  /*
  webdb.portals.on('index-updated', async ({ url }, version) => {
    console.log('Table was updated for', url, 'at version', version)
    await listPortals()
  })
  */
}

run()
