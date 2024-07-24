import { AbstractProvider, ethers } from 'ethers'

/**
 * Makes specified fields of a type required
 */
export type RequiredFields<T, K extends keyof T> = T & Required<Pick<T, K>>

/**
 * Restricts the Filter type to have fromBlock and toBlock as numbers
 */
export type StrictFilter = Omit<ethers.Filter, 'fromBlock' | 'toBlock'> & {
  fromBlock: number
  toBlock: number
}

/**
 * Represents a range of blocks
 */
export type BlockRange = {
  fromBlock: number
  toBlock: number
}

/**
 * Converts a block tag to a block number
 * @param provider - The Ethereum provider
 * @param tag - The block tag (number or string)
 * @returns The block number
 * @throws Error if the block is invalid
 */
export async function tagToNumber(
  provider: AbstractProvider,
  tag: ethers.BlockTag
) {
  if (typeof tag === 'number') {
    return tag
  }
  const y = (await provider.getBlock(tag))?.number
  if (y === undefined) {
    throw new Error('Invalid block')
  }
  return y
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

/**
 * Gets the number of the latest finalized block
 * @param provider - The Ethereum provider
 * @returns The number of the latest finalized block
 * @throws Error if unable to get the finalized block number
 */
export async function getFinalizedBlockNumber(provider: AbstractProvider) {
  const blockNumber = (await provider.getBlock('finalized'))?.number
  if (blockNumber === undefined) {
    throw new Error('Cannot get finalized block number')
  }
  return blockNumber
}
