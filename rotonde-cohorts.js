const fs = require('fs')
const { URL } = require('url')
const mkdirp = require('mkdirp')
const { isEqual } = require('lodash')
const DatArchive = require('node-dat-archive')
const WebDB = require('@beaker/webdb')
const blackList = require('./blacklist')

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

async function writeOutPortals(cohortName, db) {
  console.log('Writing out portals')
  mkdirp.sync('db/portals')
  const portals = await db.portals.toArray()
  for (const portal of portals) {
    const url = portal.getRecordURL().replace(/\/portal\.json$/, '')
    const { name } = portal
    const record = {
      cohortName,
      name,
      url
    }
    if (blackList.includes(url)) continue
    const id = url.replace(/^dat:\/\//, '')
    const file = `db/portals/${id}.json`
    let oldRecord
    if (fs.existsSync(file)) {
      oldRecord = JSON.parse(fs.readFileSync(file, 'utf8'))
    }
    if (!oldRecord || (oldRecord && !isEqual(record, oldRecord))) {
      console.log('Wrote:', file)
      fs.writeFileSync(file, JSON.stringify(record, null, 2))
    }
  }
}

async function processCohortPortals () {
  const cohortPortals = await webdbCohorts.cohorts.toArray()
  for (const portal of cohortPortals) {
    const { name: cohortName, port } = portal
    if (cohortName.match(/^cohort-/)) {
      console.log('Portal:', cohortName)
      const webdbCohortPortals = new WebDB(`${webdbDir}/${cohortName}`, { DatArchive })
      webdbCohortPortals.define('portals', portalDefinition)
      console.log('Opening...')
      await webdbCohortPortals.open()
      console.log('Opened')
      // Blacklist
      // FIXME: Figure out how to automatically remove urls that have been unfollowed
      for (const key of blackList) {
        await webdbCohortPortals.removeSource(key)
      }
      const fetchers = {}
      webdbCohortPortals.on('indexes-updated', async ({ url }, version) => {
        if (!fetchers[url]) {
          console.log('indexes-updated for', url, 'at version', version)
        } else {
          fetchers[url].fetched = true
          fetchers[url].indexed = true
          fetchers[url].version = version
        }
      })
      webdbCohortPortals.on('source-missing', (url) => {
        console.log('WebDB couldnt find', url, '- now searching')
      })
      webdbCohortPortals.on('source-found', (url) => {
        if (!fetchers[url]) {
          console.log('WebDB has found and indexed', url)
        } else {
          fetchers[url].fetched = true
          fetchers[url].indexed = true
        }
      })
      webdbCohortPortals.on('source-error', (url, err) => {
        if (err.name === 'TimeoutError') {
          console.error(`---> Source Timeout: ${url}`)
          if (fetchers[url]) {
            fetchers[url].indexed = false
            fetchers[url].timedOut = err
          } else {
            console.error('Mismatch', url)
          }
          // console.log(err.debugStack)
        } else {
          console.error(`---> Source Error: ${url} ${err}`)
          if (fetchers[url]) {
            fetchers[url].indexed = false
            fetchers[url].error = err
          } else {
            console.error('Mismatch', url)
          }
        }
        // console.error('WebDB failed to index', url, err)
      })
      webdbCohortPortals.on('index-error', (file, err) => {
        const { protocol, host } = new URL(file)
        const url = `${protocol}//${host}`
        if (err.name === 'TimeoutError') {
          console.log(`---> Index Timeout: ${file}`)
          if (fetchers[url]) {
            fetchers[url].indexed = false
            fetchers[url].timedOut = err
          } else {
            console.error('Mismatch', url)
          }
          // console.error(err.debugStack)
        } else {
          console.log(`---> Index Error: ${file} ${err}`)
          if (fetchers[url]) {
            fetchers[url].indexed = false
            fetchers[url].error = err
          } else {
            console.error('Mismatch', url)
          }
        }
        // console.error('WebDB failed to index', url, err)
      })
      let count = 1
      for (const portalUrl of port) {
        const index = count++
        console.log(`  ${index}:`, portalUrl)
        if (blackList.includes(portalUrl)) {
          console.log(`  [Blacklisted]`)
          continue
        }
        const normalizedUrl = portalUrl.replace(/\/$/, '')
        const fetcher = {
          index,
          normalizedUrl,
          portalUrl
        }
        fetchers[normalizedUrl] = fetcher
        fetcher.promise = webdbCohortPortals
          .addSource(portalUrl)
          .catch(err => {
            if (err.name === 'TimeoutError') {
              console.log(`---> Timeout: ${index} ${normalizedUrl}`)
              // console.log(err.debugStack)
              fetcher.indexed = false
              fetcher.timedOut = err
            } else {
              console.log(`---> Error: ${index} ${normalizedUrl}`)
              console.error(err)
              fetcher.indexed = false
              fetcher.error = err
            }
          })
      }
      const total = count - 1
      webdbCohortPortals.on('index-updated', async ({ url }, version) => {
        // await listPortals()
        if (!fetchers[url]) {
          console.log('Table was updated for', url, 'at version', version)
          // console.log('indexes-updated for', url, 'at version', version)
        } else {
          fetchers[url].fetched = true
          fetchers[url].version = version
        }
      })
      const settleTime = 10
      console.log(`Collecting data for ${settleTime} seconds`)
      for (let i = 1; i <= settleTime; i++ ) {
        await sleep(1)
        let fetchedCount = 0
        let indexedCount = 0
        let timedOutCount = 0
        let errorCount = 0
        Object.values(fetchers).forEach(fetcher => {
          const { fetched, indexed, timedOut, error } = fetcher
          if (fetched) fetchedCount++
          if (indexed) indexedCount++
          if (timedOut) timedOutCount++
          if (error) errorCount++
        })
        console.log(
          `  ${i} seconds: ${fetchedCount} fetched, ` +
          `${indexedCount} indexed, ` +
          `${timedOutCount} timeouts, ` +
          `${errorCount} errors`
        )
        if (indexedCount + timedOutCount + errorCount === total) break
      }
      await writeOutPortals(cohortName, webdbCohortPortals)
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

process.on('unhandledRejection', error => {
  console.log('Unhandled rejection', error)
})

run()
