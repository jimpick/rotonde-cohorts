const mkdirp = require('mkdirp')
const DatArchive = require('node-dat-archive')
const WebDB = require('@beaker/webdb')
const fs = require('fs')

const webdbDir = './db-all-portals'
mkdirp.sync(webdbDir)

const portalSummaryDefinition = {
  // uses JSONSchema v6
  schema: {
    type: 'object',
    properties: {
      cohortName: {
        type: 'string'
      },
      name: {
        type: 'string'
      },
      url: {
        type: 'string'
      }
    },
    required: [ 'name' ]
  },

  // secondary indexes for fast queries (optional)
  index: [ 'cohortName', 'name', 'url', 'cohortName+name', 'name+cohortName' ],

  // files to index
  filePattern: [
    '/portals/*.json'
  ]
}

var webdbAllPortals = new WebDB(`${webdbDir}/allPortals`, { DatArchive })
webdbAllPortals.define('allPortals', portalSummaryDefinition)
const webdbAllPortalsUrl = 'dat://5a3a9ce433f80af5d5f024df1d663f02e00cd0aa7cab84c585994ed555d4640c'

async function processPortals () {
  const portals = await webdbAllPortals.allPortals.toArray()
  await webdbAllPortals.addSource(webdbAllPortalsUrl)
}

function sleep (seconds) {
  const promise = new Promise((resolve, reject) => {
    setTimeout(resolve, seconds * 1000)
  })
  return promise
}

async function run () {
  await webdbAllPortals.open()
  await processPortals()
  const portals = await webdbAllPortals.allPortals
    //.orderBy('cohortName+name')
    .orderBy('name+cohortName')
    .toArray()
  let output = '<pre>\n'
  portals.forEach(({name, cohortName, url}) => {
    // console.log(cohortName, name)
    console.log(name, cohortName, url)
    output += `${name} ${cohortName} ${url}\n`
  })
  output += '</pre>\n'
  fs.writeFileSync(`${webdbDir}/index.html`, output)

  console.log('Done.')
  process.exit(0)
}

process.on('unhandledRejection', error => {
  console.log('Unhandled rejection', error)
})

run()
