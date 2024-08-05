export class PromisePool<T> {
  private backlog: (() => Promise<T>)[] = []
  private running: Set<Promise<T>> = new Set()
  private maxConcurrent: number

  constructor(maxConcurrent: number) {
    this.maxConcurrent = maxConcurrent
  }

  async push(fn: () => Promise<T>): Promise<T> {
    console.log('pushing task')
    console.log('running:', this.running.size)
    console.log('backlog:', this.backlog.length)
    if (this.running.size < this.maxConcurrent) {
      return this.runTask(fn)
    } else {
      return new Promise<T>((resolve, reject) => {
        this.backlog.push(() =>
          fn()
            .then(y => {
              resolve(y)
              return y
            })
            .catch(z => {
              reject(z)
              return z
            })
        )
      })
    }
  }

  private async runTask(fn: () => Promise<T>): Promise<T> {
    const promise = fn()
    this.running.add(promise)
    console.log('added promise to running')

    try {
      const result = await promise
      this.running.delete(promise)
      console.log('removed promise from running')
      this.runNextTask()
      return result
    } catch (error) {
      this.running.delete(promise)
      console.log('removed FAILED promise from running')
      this.runNextTask()
      throw error
    }
  }

  private runNextTask() {
    if (this.backlog.length > 0 && this.running.size < this.maxConcurrent) {
      const nextTask = this.backlog.shift()!
      this.runTask(nextTask)
    }
  }

  async waitForAll(): Promise<void> {
    while (this.running.size > 0 || this.backlog.length > 0) {
      await Promise.race(this.running)
    }
  }
}
