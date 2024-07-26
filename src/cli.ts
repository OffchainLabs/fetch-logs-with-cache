#!/usr/bin/env node

import Database from 'better-sqlite3'
import { ethers, isHexString } from 'ethers'
import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import {
  FetchLogsToCacheBatchCallback,
  FetchLogsBatchCallback,
  LogCache,
} from '.'

// Main async function
;(async () => {
  // Set up command line arguments using yargs
  const argv = await yargs(hideBin(process.argv))
    .usage('Usage: $0 [OPTIONS] [SIG_OR_TOPIC] [TOPICS_OR_ARGS]...')
    .command('$0 [sigOrTopic] [topicsOrArgs...]', '')
    .positional('sigOrTopic', {
      describe:
        'The signature of the event to filter logs by which will be converted to the first topic or a topic to filter on',
      type: 'string',
    })
    .positional('topicsOrArgs', {
      describe:
        'Indexed fields of the event to filter by. Otherwise, the remaining topics of the filter',
      type: 'string',
      array: true,
    })
    .option('from-block', {
      type: 'string',
      describe: 'The block height to start query at',
      alias: 'f',
      default: 'earliest',
    })
    .option('to-block', {
      type: 'string',
      describe: 'The block height to stop query at',
      alias: 't',
      default: 'latest',
    })
    .option('address', {
      type: 'string',
      describe: 'The contract address to filter on',
      alias: 'a',
    })
    .option('rpc-url', {
      type: 'string',
      describe: 'The RPC URL to use for querying logs',
      alias: 'r',
      default: 'http://localhost:8545',
    })
    .option('page-size', {
      type: 'number',
      describe: 'The block range per eth_getLogs request',
      alias: 'p',
      default: 1000,
    })
    .option('db-path', {
      type: 'string',
      describe:
        'The path to the SQLite database to use for caching logs. Defaults to LOGS_DB_PATH env var.',
    })
    .option('nosave', {
      type: 'boolean',
      describe: 'Do not save logs to a database',
      default: false,
    })
    .option('show-progress', {
      type: 'boolean',
      describe: 'Show progress of fetching logs',
      default: false,
      alias: 's',
    })
    .option('hide-result', {
      type: 'boolean',
      describe: 'Hide the result of fetching logs',
      default: false,
      alias: 'i',
    })
    .help()
    .alias('h', 'help')
    .strictOptions()
    .wrap(yargs.terminalWidth()).argv

  // Determine the database path
  const dbPath = argv.nosave
    ? `:memory:`
    : argv.dbPath || process.env.LOGS_DB_PATH

  if (dbPath === undefined) {
    throw new Error(
      'No database path provided, set LOGS_DB_PATH env var or use --db-path option'
    )
  }

  // Process topics
  const topics: string[] = []

  if (argv.sigOrTopic !== undefined) {
    const topic0 = isHexString(argv.sigOrTopic)
      ? argv.sigOrTopic
      : new ethers.Interface([argv.sigOrTopic]).getEvent(argv.sigOrTopic)!
          .topicHash
    topics.push(topic0)
  }

  topics.push(...(argv.topicsOrArgs || []))

  // Validate topics
  for (const topic of topics) {
    if (!isHexString(topic) || topic.length !== 66) {
      throw new Error(`Invalid topic: ${topic}`)
    }
  }

  const logCache = new LogCache(dbPath)

  try {
    // Set up provider and filter
    const provider = new ethers.JsonRpcProvider(argv.rpcUrl)

    const fromBlock = isNaN(parseInt(argv.fromBlock))
      ? argv.fromBlock
      : parseInt(argv.fromBlock)
    const toBlock = isNaN(parseInt(argv.toBlock))
      ? argv.toBlock
      : parseInt(argv.toBlock)

    const filter = {
      address: argv.address,
      topics,
      fromBlock,
      toBlock,
    }

    // Define callbacks for progress reporting
    const finalizedCallback: FetchLogsToCacheBatchCallback = async (
      _logs,
      _thisBatchLogs,
      thisBatchFrom,
      thisBatchTo,
      _missingRanges,
      _rangeI,
      totalScannedBlocks,
      blocksToScan
    ) => {
      if (!argv.showProgress) return

      const progress = ((totalScannedBlocks / blocksToScan) * 100).toFixed(2)

      console.log(
        `Finalized blocks ${thisBatchFrom} to ${thisBatchTo} (${progress}%)`
      )
    }

    const unfinalizedCallback: FetchLogsBatchCallback = async (
      _logs,
      _thisBatchLogs,
      thisBatchFrom,
      thisBatchTo
    ) => {
      if (!argv.showProgress) return

      console.log(`Unfinalized blocks ${thisBatchFrom} to ${thisBatchTo}`)
    }

    // Fetch logs
    const logs = await logCache.getLogs(
      provider,
      filter,
      argv.pageSize,
      finalizedCallback,
      unfinalizedCallback
    )

    // Output results
    if (!argv.hideResult) console.log(JSON.stringify(logs, null, 2))
  } catch (e) {
    console.error(e)
    process.exit(1)
  }
})().catch(e => {
  console.error(e)
  process.exit(1)
})
