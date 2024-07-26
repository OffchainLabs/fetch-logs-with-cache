import { AbstractProvider, Filter, ethers } from 'ethers'
import { Database } from 'better-sqlite3'
import {
  BlockRange,
  StrictFilter,
  getFinalizedBlockNumber,
  mergeRanges,
  subtractRanges,
  tagToNumber,
} from './util'

// Define internal DB type for log entries
type LogEntry = {
  filterId: string
  blockNumber: number
  logIndex: number
  data: string
}

/**
 * Callback function type for processing batches of logs
 * @param allLogs - All logs fetched so far
 * @param thisBatchLogs - Logs fetched in the current batch
 * @param thisBatchFrom - Starting block number of the current batch
 * @param thisBatchTo - Ending block number of the current batch
 */
export type FetchLogsBatchCallback = (
  allLogs: ethers.Log[],
  thisBatchLogs: ethers.Log[],
  thisBatchFrom: number,
  thisBatchTo: number
) => Promise<void> | void

/**
 * Callback function type for processing batches of logs during cache fetching
 * @param logs - All logs fetched so far
 * @param thisBatchLogs - Logs fetched in the current batch
 * @param thisBatchFrom - Starting block number of the current batch
 * @param thisBatchTo - Ending block number of the current batch
 * @param ranges - All block ranges to be fetched
 * @param thisRangeIndex - Index of the current range being fetched
 * @param totalScannedBlocks - Total number of blocks scanned so far
 * @param blocksToScan - Total number of blocks to scan
 */
export type FetchLogsToCacheBatchCallback = (
  logs: ethers.Log[],
  thisBatchLogs: ethers.Log[],
  thisBatchFrom: number,
  thisBatchTo: number,
  ranges: BlockRange[],
  thisRangeIndex: number,
  totalScannedBlocks: number,
  blocksToScan: number
) => Promise<void> | void

/**
 * LogCache class for caching and retrieving Ethereum logs
 */
export class LogCache {
  private db: Database

  /**
   * Creates a new LogCache instance
   * @param db - Better-sqlite3 Database instance
   */
  constructor(db: Database) {
    this.db = db
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
  private _insertLogs(logs: ethers.Log[], filterId: string): void {
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
   * @param pageSize - Number of blocks to fetch in each batch
   * @param batchCallback - Optional callback function for each batch
   */
  private async _fetchRangeToCache(
    provider: AbstractProvider,
    strictFilter: StrictFilter,
    pageSize: number,
    batchCallback?: FetchLogsBatchCallback
  ): Promise<void> {
    const filterId = await LogCache.getFilterId(provider, strictFilter)

    await LogCache.fetchLogs(
      provider,
      strictFilter,
      pageSize,
      async (logs, thisBatchLogs, thisBatchFrom, thisBatchTo) => {
        this.db.transaction(() => {
          this._insertLogs(thisBatchLogs, filterId)
          this._insertRange(filterId, thisBatchFrom, thisBatchTo)
        })()

        await batchCallback?.(logs, thisBatchLogs, thisBatchFrom, thisBatchTo)
      }
    )
  }

  /**
   * Fetches logs to cache for the given filter
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'finalized'
   * @param pageSize - Number of blocks to fetch in each batch
   * @param batchCallback - Optional callback function for each batch
   */
  async fetchLogsToCache(
    provider: AbstractProvider,
    filter: Filter,
    pageSize: number,
    batchCallback?: FetchLogsToCacheBatchCallback
  ): Promise<void> {
    if (pageSize < 1) {
      throw new Error(`Invalid page size ${pageSize}`)
    }

    const lastFinalizedBlock = await getFinalizedBlockNumber(provider)

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

    let totalScannedBlocks = 0
    const blocksToScan = missingRanges.reduce(
      (acc, range) => acc + range.toBlock - range.fromBlock + 1,
      0
    )

    for (let i = 0; i < missingRanges.length; i++) {
      const range = missingRanges[i]
      await this._fetchRangeToCache(
        provider,
        { ...filter, ...range },
        pageSize,
        (logs, thisBatchLogs, thisBatchFrom, thisBatchTo) => {
          totalScannedBlocks += thisBatchTo - thisBatchFrom + 1

          return batchCallback?.(
            logs,
            thisBatchLogs,
            thisBatchFrom,
            thisBatchTo,
            missingRanges,
            i,
            totalScannedBlocks,
            blocksToScan
          )
        }
      )
    }

    this._tidyUpRanges(filterId)
  }

  /**
   * Reads logs from cache for the given filter
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'latest'
   * @returns Array of ethers.Log objects
   */
  async readLogsFromCache(
    provider: AbstractProvider,
    filter: Filter
  ): Promise<ethers.Log[]> {
    // Set blocks to 'earliest' and 'latest' if not provided
    const strictFilter = await LogCache.toStrictFilter(provider, filter)
    const filterId = await LogCache.getFilterId(provider, filter)
    const rows = this._selectLogs(
      filterId,
      strictFilter.fromBlock,
      strictFilter.toBlock
    )
    return rows.map(row => new ethers.Log(JSON.parse(row.data), provider))
  }

  /**
   * Gets logs for the given filter, using the cache for finalized blocks but not unfinalized blocks
   * @param provider - Ethereum provider
   * @param filter - Filter object - fromBlock and toBlock default to 'earliest' and 'latest'
   * @param pageSize - Number of blocks to fetch in each batch
   * @param finalizedLogsCallback - Optional callback for finalized logs
   * @param unfinalizedLogsCallback - Optional callback for unfinalized logs
   * @returns Array of ethers.Log objects
   */
  async getLogs(
    provider: AbstractProvider,
    filter: Filter,
    pageSize: number,
    finalizedLogsCallback?: FetchLogsToCacheBatchCallback,
    unfinalizedLogsCallback?: FetchLogsBatchCallback
  ): Promise<ethers.Log[]> {
    // Get the number of the last finalized block
    const lastFinalizedBlock = (await getFinalizedBlockNumber(provider)) || -1

    // Set blocks to 'earliest' and 'latest' if not provided
    const strictFilter = await LogCache.toStrictFilter(provider, filter)

    const logs: ethers.Log[] = []

    // Fetch and cache logs for finalized blocks if some blocks in range are finalized
    if (strictFilter.fromBlock <= lastFinalizedBlock) {
      const finalizedFilter = {
        ...strictFilter,
        toBlock: Math.min(strictFilter.toBlock, lastFinalizedBlock),
      }

      await this.fetchLogsToCache(
        provider,
        finalizedFilter,
        pageSize,
        finalizedLogsCallback
      )

      // Read cached logs for finalized blocks
      logs.push(...(await this.readLogsFromCache(provider, finalizedFilter)))
    }

    // If the requested toBlock is beyond the last finalized block,
    // fetch logs for unfinalized blocks directly (without caching)
    if (strictFilter.toBlock > lastFinalizedBlock) {
      const unfinalizedLogs = await LogCache.fetchLogs(
        provider,
        {
          ...strictFilter,
          fromBlock: Math.max(lastFinalizedBlock + 1, strictFilter.fromBlock),
        },
        pageSize,
        unfinalizedLogsCallback
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
   * @param pageSize - Number of blocks to fetch in each batch
   * @param batchCallback - Optional callback function for each batch
   * @returns Array of ethers.Log objects
   */
  static async fetchLogs(
    provider: AbstractProvider,
    filter: Filter,
    pageSize: number,
    batchCallback?: FetchLogsBatchCallback
  ): Promise<ethers.Log[]> {
    // Validate page size
    if (pageSize < 1) {
      throw new Error('Invalid page size')
    }

    // Convert the filter to a strict filter
    const strictFilter = await LogCache.toStrictFilter(provider, filter)

    let fromBlock = strictFilter.fromBlock

    const logs: ethers.Log[] = []
    while (fromBlock <= strictFilter.toBlock) {
      // Calculate the end block for this batch
      const thisToBlock = Math.min(
        fromBlock + pageSize - 1,
        strictFilter.toBlock
      )

      // Fetch logs for the current batch
      const thisBatchLogs = await provider.getLogs({
        ...filter,
        fromBlock,
        toBlock: thisToBlock,
      })

      // Add the fetched logs to the overall logs array
      logs.push(...thisBatchLogs)

      // Call the batch callback if provided
      await batchCallback?.(logs, thisBatchLogs, fromBlock, thisToBlock)

      // Move to the next batch
      fromBlock = thisToBlock + 1
    }

    return logs
  }

  /**
   * Generates a unique filter ID based on the provider and filter
   * @param provider - AbstractProvider instance
   * @param filter - Filter object containing address and topics
   * @returns Unique filter ID as a string
   */
  static async getFilterId(
    provider: AbstractProvider,
    filter: Pick<Filter, 'address' | 'topics'>
  ): Promise<string> {
    const _filter = await provider._getFilter(filter)
    const chainId = (await provider.getNetwork()).chainId

    const str = `${chainId}:${_filter.address}:${JSON.stringify(_filter.topics)}`

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
    provider: AbstractProvider,
    filter: Filter,
    defaultToBlock: ethers.BlockTag = 'latest'
  ): Promise<StrictFilter> {
    const fromBlock = await tagToNumber(
      provider,
      filter.fromBlock || 'earliest'
    )
    const toBlock = await tagToNumber(
      provider,
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
