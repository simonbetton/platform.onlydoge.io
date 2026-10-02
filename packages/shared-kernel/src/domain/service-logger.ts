export interface ServiceLogger {
  error(bindings: Record<string, unknown>, message: string): void;
  info(bindings: Record<string, unknown>, message: string): void;
  warn(bindings: Record<string, unknown>, message: string): void;
}

export function noopServiceLogger(): ServiceLogger {
  return {
    info: () => {},
    warn: () => {},
    error: () => {},
  };
}
