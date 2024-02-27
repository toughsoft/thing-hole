export type WithId<T> = Omit<T, "id"> & { id: string };

export type WithOptionalId<T> = Omit<T, "id"> & { id?: string };

export interface WriteHole<T> {
  putBatch(items: T[]): Promise<string[]>;
  deleteBatch(ids: string[]): Promise<void>;
}

export interface GetSomeOptions {
  limit?: number;
}

export interface ScanOptions {
  limit?: number;
  startKey?: string;
}

export interface ScanResponse<T> {
  items: WithId<T>[];
  lastKey?: string;
}

export interface ReadHole<T> {
  get(id: string): Promise<WithId<T>>;
  getSome(options?: GetSomeOptions): Promise<WithId<T>[]>;
  scan(options?: ScanOptions): Promise<ScanResponse<WithId<T>>>;
}

export interface ReadWriteHole<T> extends ReadHole<T>, WriteHole<T> {}
