import { ethers } from 'ethers'
import Database from 'better-sqlite3'
import {
  BlockRange,
  StrictFilter,
  Filter,
  mergeRanges,
  subtractRanges,
  EthersProvider,
  EventFilter,
  WrappedProvider,
  EthersLog,
} from './util'
import { PromisePool } from './PromisePool'

// Define internal DB type for log entries
type LogEntry = {
  filterId: string
  blockNumber: number
  logIndex: number
  data: string
}

/**
 * Callback function type for processing batches of logs
 * @param thisBatchLogs - Logs fetched in the current batch
 * @param thisBatchFrom - Starting block number of the current batch
 * @param thisBatchTo - Ending block number of the current batch
 * @param err - Error object if an error occurred
 */
export type FetchLogsBatchCallback = (
  thisBatchLogs: EthersLog[],
  thisBatchFrom: number,
  thisBatchTo: number,
  err: Error | undefined
) => Promise<void> | void

/**
 * LogCache class for caching and retrieving Ethereum logs
 */
export class LogCache {
  private db: Database.Database

  private readonly promisePool: PromisePool<EthersLog[]>

  /**
   * Creates a new LogCache instance
   * @param db - Better-sqlite3 Database instance
   */
  constructor(dbPath: string, promisePoolSize: number = 20) {
    this.db = new Database(dbPath)
    this.promisePool = new PromisePool(promisePoolSize)
    this._setUpDb()
  }

  /**
   * Sets up the database schema
   */
  private _setUpDb(): void {
    // Create tables and indexes if they don't exist
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS logs (
        filterId TEXT,
        blockNumber INTEGER,
        logIndex INTEGER,
        data TEXT,
        UNIQUE(filterId, blockNumber, logIndex)
      );
      CREATE TABLE IF NOT EXISTS fetched_ranges (
        filterId TEXT,
        fromBlock INTEGER,
        toBlock INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_logs_filterId ON logs (filterId);
      CREATE INDEX IF NOT EXISTS idx_fetched_ranges_filterId ON fetched_ranges (filterId);
    `)
  }

  /**
   * Inserts logs into the database
   * @param logs - Array of logs to insert
   * @param filterId - Unique identifier for the filter
   */
  private _insertLogs(logs: EthersLog[], filterId: string): void {
    const insertLog = this.db.prepare(
      'INSERT OR IGNORE INTO logs (filterId, blockNumber, logIndex, data) VALUES (?, ?, ?, ?)'
    )
    for (const log of logs) {
      insertLog.run(filterId, log.blockNumber, log.index, JSON.stringify(log))
    }
  }

  /**
   * Selects logs from the database within a given block range
   * @param filterId - Unique identifier for the filter
   * @param fromBlock - Starting block number
   * @param toBlock - Ending block number
   * @returns Array of LogEntry objects
   */
  private _selectLogs(
    filterId: string,
    fromBlock: number,
    toBlock: number
  ): LogEntry[] {
    return this.db
      .prepare(
        `SELECT blockNumber, logIndex, data FROM logs WHERE filterId = ? AND ? <= blockNumber AND blockNumber <= ? ORDER BY blockNumber, logIndex`
      )
      .all(filterId, fromBlock, toBlock) as LogEntry[]
  }

  /**
   * Inserts a block range into the fetched_ranges table
   * @param filterId - Unique identifier for the filter
   * @param fromBlock - Starting block number
   * @param toBlock - Ending block number
   */
  private _insertRange(
    filterId: string,
    fromBlock: number,
    toBlock: number
  ): void {
    const insertRange = this.db.prepare(
      'INSERT INTO fetched_ranges (filterId, fromBlock, toBlock) VALUES (?, ?, ?)'
    )
    insertRange.run(filterId, fromBlock, toBlock)
  }

  /**
   * Selects all fetched ranges for a given filter
   * @param filterId - Unique identifier for the filter
   * @returns Array of BlockRange objects
   */
  private _selectRanges(filterId: string): BlockRange[] {
    return this.db
      .prepare(
        `SELECT fromBlock, toBlock FROM fetched_ranges WHERE filterId = ? ORDER BY fromBlock`
      )
      .all(filterId) as BlockRange[]
  }

  /**
   * Replaces all ranges for a given filter with new ranges
   * @param filterId - Unique identifier for the filter
   * @param ranges - Array of new BlockRange objects
   */
  private _replaceRanges(filterId: string, ranges: BlockRange[]): void {
    this.db
      .prepare(`DELETE FROM fetched_ranges WHERE filterId = ?`)
      .run(filterId)
    for (const range of ranges) {
      this._insertRange(filterId, range.fromBlock, range.toBlock)
    }
  }

  /**
   * Calculates missing ranges for a given filter and desired range
   * @param filterId - Unique identifier for the filter
   * @param want - Desired BlockRange
   * @returns Array of missing BlockRange objects
   */
  private _getMissingRanges(filterId: string, want: BlockRange): BlockRange[] {
    const have = this._selectRanges(filterId)
    return subtractRanges(want, have)
  }

  /**
   * Merges and optimizes stored ranges for a given filter
   * @param filterId - Unique identifier for the filter
   */
  private _tidyUpRanges(filterId: string): void {
    this.db.transaction(() => {
      const ranges = this._selectRanges(filterId)
      if (ranges.length === 0) return
      const mergedRanges = mergeRanges(ranges)
      this._replaceRanges(filterId, mergedRanges)
    })()
  }

  /**
   * Fetches logs for a specific range and caches them
   * @param provider - Ethereum provider
   * @param strictFilter - StrictFilter object
   * @param minPageSize - Number of blocks to fetch in each batch. Will not split further below this value.
   * @param splitWays - Number of ways to split the range if an error occurs
   * @param batchCallback - Optional callback function for each batch
   */
  private async _fetchRangeToCache(
    provider: EthersProvider,
    strictFilter: StrictFilter,
    minPageSize: number,
    splitWays: number,
    batchCallback?: FetchLogsBatchCallback
  ): Promise<void> {
    const filterId = await LogCache.getFilterId(provider, strictFilter)

    await this.fetchLogs(
      provider,
      strictFilter,
      minPageSize,
      splitWays,
      async (thisBatchLogs, thisBatchFrom, thisBatchTo, err) => {
        if (!err) {
          this.db.transaction(() => {
            this._insertLogs(thisBatchLogs, filterId)
            this._insertRange(filterId, thisBatchFrom, thisBatchTo)
          })()
        }

        await batchCallback?.(thisBatchLogs, thisBatchFrom, thisBatchTo, err)
      }
    )
  }

  /**
   * Fetches logs to cache for the given filter
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'finalized'
   * @param minPageSize - Number of blocks to fetch in each batch. Will not split further below this value.
   * @param splitWays - Number of ways to split the range if an error occurs
   * @param batchCallback - Optional callback function for each batch
   */
  async fetchLogsToCache(
    provider: EthersProvider,
    filter: Filter,
    minPageSize: number = 1000,
    splitWays: number = 2,
    batchCallback?: FetchLogsBatchCallback
  ): Promise<void> {
    const wProvider = new WrappedProvider(provider)

    if (minPageSize < 1) {
      throw new Error(`Invalid page size ${minPageSize}`)
    }

    const lastFinalizedBlock = await wProvider.getFinalizedBlockNumber()

    if (!lastFinalizedBlock) {
      throw new Error('Could not get finalized block')
    }

    // Set blocks to 'earliest' and 'finalized' if not provided
    const strictFilter = await LogCache.toStrictFilter(
      provider,
      filter,
      lastFinalizedBlock
    )

    if (strictFilter.toBlock > lastFinalizedBlock) {
      throw new Error('toBlock is not finalized')
    }

    const filterId = await LogCache.getFilterId(provider, filter)

    this._tidyUpRanges(filterId)

    const missingRanges = this._getMissingRanges(filterId, strictFilter)

    for (let i = 0; i < missingRanges.length; i++) {
      const range = missingRanges[i]
      await this._fetchRangeToCache(
        provider,
        { ...filter, ...range },
        minPageSize,
        splitWays,
        batchCallback
      )
    }

    this._tidyUpRanges(filterId)
  }

  /**
   * Reads logs from cache for the given filter
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'latest'
   * @returns Array of EthersLog objects
   */
  async readLogsFromCache(
    provider: EthersProvider,
    filter: Filter
  ): Promise<EthersLog[]> {
    // Set blocks to 'earliest' and 'latest' if not provided
    const strictFilter = await LogCache.toStrictFilter(provider, filter)
    const filterId = await LogCache.getFilterId(provider, filter)
    const rows = this._selectLogs(
      filterId,
      strictFilter.fromBlock,
      strictFilter.toBlock
    )
    return rows.map(row => JSON.parse(row.data))
  }

  /**
   * Gets logs for the given filter, using the cache for finalized blocks but not unfinalized blocks
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'latest'
   * @param minPageSize - Number of blocks to fetch in each batch. Will not split further below this value.
   * @param splitWays - Number of ways to split the range if an error occurs
   * @param batchCallback - Optional callback for each batch of logs
   * @returns Array of EthersLog objects
   */
  async getLogs(
    provider: EthersProvider,
    filter: Filter,
    minPageSize: number = 1000,
    splitWays: number = 2,
    batchCallback?: FetchLogsBatchCallback,
  ): Promise<EthersLog[]> {
    const wProvider = new WrappedProvider(provider)

    // Get the number of the last finalized block
    const lastFinalizedBlock = (await wProvider.getFinalizedBlockNumber()) || -1

    // Set blocks to 'earliest' and 'latest' if not provided
    const strictFilter = await LogCache.toStrictFilter(provider, filter)

    const logs: EthersLog[] = []

    // Fetch and cache logs for finalized blocks if some blocks in range are finalized
    if (strictFilter.fromBlock <= lastFinalizedBlock) {
      const finalizedFilter = {
        ...strictFilter,
        toBlock: Math.min(strictFilter.toBlock, lastFinalizedBlock),
      }

      await this.fetchLogsToCache(
        provider,
        finalizedFilter,
        minPageSize,
        splitWays,
        batchCallback
      )

      // Read cached logs for finalized blocks
      logs.push(...(await this.readLogsFromCache(provider, finalizedFilter)))
    }

    // If the requested toBlock is beyond the last finalized block,
    // fetch logs for unfinalized blocks directly (without caching)
    if (strictFilter.toBlock > lastFinalizedBlock) {
      const unfinalizedLogs = await this.fetchLogs(
        provider,
        {
          ...strictFilter,
          fromBlock: Math.max(lastFinalizedBlock + 1, strictFilter.fromBlock),
        },
        minPageSize,
        splitWays,
        batchCallback
      )

      // Combine finalized (cached) logs with unfinalized logs
      logs.push(...unfinalizedLogs)
    }

    return logs
  }

  /**
   * Fetches logs for the given filter
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'latest'
   * @param minPageSize - Number of blocks to fetch in each batch. Will not split further below this value.
   * @param splitWays - Number of ways to split the range if an error occurs
   * @param batchCallback - Optional callback function for each batch
   * @returns Array of EthersLog objects
   */
  async fetchLogs(
    provider: EthersProvider,
    filter: Filter,
    minPageSize: number = 1000,
    splitWays: number = 2,
    batchCallback?: FetchLogsBatchCallback
  ): Promise<EthersLog[]> {
    const wProvider = new WrappedProvider(provider)

    // Validate page size
    if (minPageSize < 1) {
      throw new Error('Invalid page size')
    }

    // Convert the filter to a strict filter
    const strictFilter = await LogCache.toStrictFilter(provider, filter)

    return this._fetchLogsBinary(wProvider, strictFilter, minPageSize, splitWays, batchCallback) // todo make these configurable
  }

  private async _fetchLogsBinary(
    provider: WrappedProvider,
    filter: StrictFilter,
    minPageSize: number,
    splitWays: number,
    batchCallback?: FetchLogsBatchCallback
  ): Promise<EthersLog[]> {
    // try full range, if throw, split
    try {
      return await this.promisePool.push(async () => {
        let err: Error | undefined = undefined
        let logs: EthersLog[] = []
        
        try {
          logs = await provider.getLogs(filter)
        }
        catch (e: any) {
          err = e
        }

        batchCallback?.(logs, filter.fromBlock, filter.toBlock, err)

        if (err) {
          throw err
        }

        return logs
      })
    } catch (e) {
      if (filter.toBlock - filter.fromBlock < minPageSize) {
        throw e
      }
      // split K ways
      const promises: Promise<EthersLog[]>[] = [];
      const childRangeSize = Math.floor((filter.toBlock - filter.fromBlock) / splitWays)
      let from = filter.fromBlock
      for (let i = 0; i < splitWays; i++) {
        let to = from + childRangeSize - 1
        if (i === splitWays - 1) {
          to = filter.toBlock
        }

        promises.push(this._fetchLogsBinary(provider, { ...filter, fromBlock: from, toBlock: to }, minPageSize, splitWays, batchCallback))
        from = to + 1
      }

      return (await Promise.all(promises)).flat()
    }
  }

  /**
   * Generates a unique filter ID based on the provider and filter
   * @param provider - AbstractProvider instance
   * @param filter - Filter object containing address and topics
   * @returns Unique filter ID as a string
   */
  static async getFilterId(
    provider: EthersProvider,
    filter: EventFilter
  ): Promise<string> {
    const chainId = (await provider.getNetwork()).chainId

    const str =
      `${chainId}:${filter.address}:${JSON.stringify(filter.topics)}`.toLowerCase()

    return ethers.id(str)
  }

  /**
   * Converts a filter object to a strict filter object.
   * @param provider - The abstract provider.
   * @param filter - The filter object to convert.
   * @param defaultToBlock - The default toBlock value. Defaults to 'latest'.
   * @returns The strict filter object.
   */
  static async toStrictFilter(
    provider: EthersProvider,
    filter: Filter,
    defaultToBlock: ethers.BlockTag = 'latest'
  ): Promise<StrictFilter> {
    const wProvider = new WrappedProvider(provider)
    const fromBlock = await wProvider.getBlockNumberFromTag(
      filter.fromBlock || 'earliest'
    )
    const toBlock = await wProvider.getBlockNumberFromTag(
      filter.toBlock || defaultToBlock
    )

    if (toBlock < fromBlock) {
      throw new Error(`Invalid block range: ${fromBlock} to ${toBlock}`)
    }

    return {
      ...filter,
      fromBlock,
      toBlock,
    }
  }
}
