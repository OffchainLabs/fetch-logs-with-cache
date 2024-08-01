import { AbstractProvider, ethers } from 'ethers'
import { ethers as ethersv5 } from 'ethers-v5'

/**
 * Makes specified fields of a type required
 */
export type RequiredFields<T, K extends keyof T> = T & Required<Pick<T, K>>

export type EventFilter = {
  address: string
  topics: (string | string[] | null)[]
}
export type Filter = EventFilter & {
  fromBlock: ethers.BlockTag | undefined
  toBlock: ethers.BlockTag | undefined
}
export type StrictFilter = EventFilter & {
  fromBlock: number
  toBlock: number
}

/**
 * Unify ethers v5 and v6 log types
 */
export type EthersLog = ethers.LogParams & ethersv5.providers.Log

/**
 * Represents a range of blocks
 */
export type BlockRange = {
  fromBlock: number
  toBlock: number
}

export type EthersProvider = ethersv5.providers.Provider | AbstractProvider

export class WrappedProvider {
  constructor(public readonly provider: EthersProvider) {}

  async getBlockNumberFromTag(tag: ethers.BlockTag): Promise<number> {
    if (typeof tag === 'number') {
      return tag
    }
    if (typeof tag === 'bigint') {
      return parseInt(tag.toString())
    }
    const y = (await this.provider.getBlock(tag))?.number
    if (y === undefined) {
      throw new Error('Invalid block')
    }
    return y
  }

  async getFinalizedBlockNumber(): Promise<number | undefined> {
    try {
      const blockNumber = await this.getBlockNumberFromTag('finalized')
      return blockNumber
    } catch (e: any) {
      const code: number = e.error?.code || 0
      if (32000 <= -code && -code < 33000) {
        return undefined
      }
      throw e
    }
  }

  async getChainId(): Promise<number> {
    const chainId = (await this.provider.getNetwork()).chainId
    return parseInt(chainId.toString())
  }

  async getLogs(filter: StrictFilter): Promise<EthersLog[]> {
    const result = await this.provider.getLogs(filter)

    return result.map(log => {
      const index = 'logIndex' in log ? log.logIndex : log.index

      return {
        ...log,
        index,
        logIndex: index,
        topics: [...log.topics],
      }
    })
  }

  isV5(): this is { provider: ethersv5.providers.Provider } {
    if ('sendTransaction' in this.provider) {
      return true
    }
    return false
  }

  isV6(): this is { provider: AbstractProvider } {
    return '_getFilter' in this.provider
  }
}

/**
 * Subtracts a list of ranges from a desired range
 * @param want - The desired range
 * @param have - The list of ranges to subtract
 * @returns The list of ranges that are in 'want' but not in 'have'
 */
export function subtractRanges(
  want: BlockRange,
  have: BlockRange[]
): BlockRange[] {
  let z = [want]

  for (const x of have) {
    const newZ: BlockRange[] = []
    for (const z_ of z) {
      if (x.toBlock < z_.fromBlock || x.fromBlock > z_.toBlock) {
        // No overlap
        newZ.push(z_)
      } else {
        // Some overlap
        if (x.fromBlock > z_.fromBlock) {
          // Left non-overlapping part
          newZ.push({ fromBlock: z_.fromBlock, toBlock: x.fromBlock - 1 })
        }
        if (x.toBlock < z_.toBlock) {
          // Right non-overlapping part
          newZ.push({ fromBlock: x.toBlock + 1, toBlock: z_.toBlock })
        }
      }
    }
    z = newZ
  }

  return z
}

/**
 * Merges overlapping or adjacent ranges
 * @param ranges - The list of ranges to merge
 * @returns The list of merged ranges
 */
export function mergeRanges(ranges: BlockRange[]): BlockRange[] {
  // If there are 0 or 1 ranges, no merging is needed
  if (ranges.length <= 1) return ranges

  // Sort ranges by fromBlock
  const sortedRanges = ranges.sort((a, b) => a.fromBlock - b.fromBlock)

  const mergedRanges: BlockRange[] = []
  let currentRange = sortedRanges[0]

  for (let i = 1; i < sortedRanges.length; i++) {
    const nextRange = sortedRanges[i]

    if (nextRange.fromBlock <= currentRange.toBlock + 1) {
      // Ranges overlap or are adjacent, merge them
      currentRange.toBlock = Math.max(currentRange.toBlock, nextRange.toBlock)
    } else {
      // Ranges don't overlap, add current range to result and move to next
      mergedRanges.push(currentRange)
      currentRange = nextRange
    }
  }

  // Add the last range
  mergedRanges.push(currentRange)

  return mergedRanges
}
