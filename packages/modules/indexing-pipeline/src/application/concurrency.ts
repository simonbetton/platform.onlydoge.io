export function range(start: number, end: number): number[] {
  if (end < start) {
    return [];
  }

  return Array.from({ length: end - start + 1 }, (_value, index) => start + index);
}

export async function mapWithConcurrency<T, R>(
  values: T[],
  concurrency: number,
  worker: (value: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(values.length);
  const workItems = values.map((value, index) => ({ index, value }));
  const workerCount = Math.max(1, Math.min(concurrency, values.length || 1));

  await Promise.all(
    Array.from({ length: workerCount }, () => runConcurrentWorker(workItems, results, worker)),
  );

  return results;
}

interface WorkItem<T> {
  index: number;
  value: T;
}

async function runConcurrentWorker<T, R>(
  workItems: Array<WorkItem<T>>,
  results: R[],
  worker: (value: T, index: number) => Promise<R>,
): Promise<void> {
  const item = workItems.shift();
  if (!isRunnableWorkItem(item)) {
    return;
  }

  results[item.index] = await worker(item.value, item.index);
  await runConcurrentWorker(workItems, results, worker);
}

function isRunnableWorkItem<T>(item: WorkItem<T> | undefined): item is WorkItem<T> {
  return [item, item?.value].every((value) => value !== undefined);
}
