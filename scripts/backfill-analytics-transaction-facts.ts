import { createRuntime } from '@onlydoge/platform';

interface Options {
  network?: string;
  throughBlockHeight?: number;
}

const options = parseOptions(process.argv.slice(2));
const runtime = await createRuntime({ mode: 'http' });
const result = await runtime.analyticsQuery.backfill(options);

console.log(JSON.stringify(result, null, 2));

function parseOptions(args: string[]): Options {
  const options: Options = {};
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === '--network') {
      options.network = requireOptionValue(args, index, arg);
      index += 1;
      continue;
    }
    if (arg === '--throughBlockHeight') {
      options.throughBlockHeight = parsePositiveInteger(requireOptionValue(args, index, arg), arg);
      index += 1;
      continue;
    }
    throw new Error(`unknown argument: ${arg}`);
  }

  return options;
}

function requireOptionValue(args: string[], index: number, name: string): string {
  const value = args[index + 1];
  if (!value) {
    throw new Error(`missing value for ${name}`);
  }

  return value;
}

function parsePositiveInteger(value: string, name: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`invalid value for ${name}: ${value}`);
  }

  return parsed;
}
