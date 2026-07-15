export interface SchemaLockPort {
  withSchemaLock<T>(name: string, work: () => Promise<T>): Promise<T>;
}
