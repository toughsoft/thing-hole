import { v4 as uuid } from "npm:uuid";

import type { DynamodbClient, DynamoObject } from "./deps.ts";

import { uuidRootForHashRange } from "../uuidUtil.ts";
import idCodec from "./idCodec.ts";

import type {
  GetSomeOptions,
  ReadWriteHole,
  ScanOptions,
  ScanResponse,
  WithId,
  WithOptionalId,
} from "../types.ts";

export interface DynamoDriverOptions {
  client: DynamodbClient;
  table: string;
  hashRange?: number;
  partition: {
    key: string;
    size: number;
    prefix: string;
  };
  id: {
    key: string;
    prefix?: string;
    getId?: () => Promise<string>;
  };
}

const randomInt = (min: number, max: number): number => {
  const n = Math.random();

  return min + Math.floor((max - min) * n);
};

const create = <T extends DynamoObject>({
  client,
  table,
  hashRange,
  partition: {
    size: partitionSize,
    prefix: partitionPrefix,
    key: partitionKey,
  },
  id: {
    key: idKey,
    prefix: idPrefix = "",
    getId = async () => {
      return await uuid();
    },
  },
}: DynamoDriverOptions): ReadWriteHole<T> => {
  const publicId = idCodec({ partitionKey, idKey });

  const getIdPrefixed = async () => {
    const id = await getId();

    return idPrefix ? `${idPrefix}:${id}` : id;
  };

  const randomPartition = async () => {
    const idx = await randomInt(0, partitionSize);

    return `${partitionPrefix}:${idx}`;
  };

  const getPartitionAttrs = async (): Promise<DynamoObject> => {
    const [partitionName, id] = await Promise.all([
      randomPartition(),
      getIdPrefixed(),
    ]);

    return {
      [partitionKey]: partitionName,
      [idKey]: id,
    };
  };

  const withPartitionAttrs = async (
    item: DynamoObject,
  ): Promise<DynamoObject> => {
    if (!item[partitionKey] || !item[idKey]) {
      const partitionAttrs = await getPartitionAttrs();

      return { ...item, ...partitionAttrs };
    }

    return item;
  };

  const putBatch = async (items: WithOptionalId<T>[]): Promise<string[]> => {
    const partitionedItems = await Promise.all(
      items.map(async ({ id, ...item }) => {
        if (id !== undefined) {
          return publicId.withDecoded({ ...item, id });
        } else {
          return await withPartitionAttrs(item);
        }
      }),
    );

    await client.putBatch(table, partitionedItems);

    return partitionedItems.map(publicId.withEncoded).map(({ id }) => id);
  };

  const deleteBatch = async (ids: string[]): Promise<void> => {
    await client.deleteBatch(table, ids.map((x) => publicId.decode(x)));
  };

  const get = async (id: string): Promise<WithId<T>> => {
    const result = await client.get(table, publicId.decode(id));

    return publicId.withEncoded(result.Item) as WithId<T>;
  };

  const getSome = async (
    { limit = 25 }: GetSomeOptions = {},
  ): Promise<WithId<T>[]> => {
    const [partitionAttrs, sortFactor] = await Promise.all([
      getPartitionAttrs(),
      Promise.resolve(Math.random()),
    ]);
    const {
      [partitionKey]: partitionName,
      [idKey]: id,
    } = partitionAttrs;

    const queryResult = await client.query(table, {
      keyCondition: {
        partition: {
          key: partitionKey,
          value: partitionName,
        },
      },
      scanForward: sortFactor < 0.5,
      startKey: {
        [partitionKey]: partitionName,
        [idKey]: uuidRootForHashRange(id, hashRange),
      },
      limit,
    });

    return queryResult.Items.map(publicId.withEncoded) as WithId<T>[];
  };

  const scan = async ({
    limit = 25,
    startKey,
  }: ScanOptions = {}): Promise<ScanResponse<WithId<T>>> => {
    const partitionAttrs = await getPartitionAttrs();
    const {
      [partitionKey]: partitionName,
    } = partitionAttrs;

    const result = await client.query(table, {
      keyCondition: {
        partition: {
          key: partitionKey,
          value: partitionName,
        },
        sort: {
          key: idKey,
          value: idPrefix,
          cond: "begins_with",
        },
      },
      startKey: startKey !== undefined ? publicId.decode(startKey) : undefined,
      limit,
    });

    const response: ScanResponse<WithId<T>> = {
      items: result.Items.map(publicId.withEncoded) as WithId<T>[],
    };

    if (result.LastEvaluatedKey) {
      response.lastKey = publicId.encode(result.LastEvaluatedKey);
    }

    return response;
  };

  return {
    get,
    getSome,
    scan,
    putBatch,
    deleteBatch,
  };
};

export default create;
