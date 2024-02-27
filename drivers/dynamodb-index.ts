import { v4 as uuid } from "npm:uuid";

import type { DynamodbClient } from "./deps.ts";

import { uuidRootForHashRange } from "../uuidUtil.ts";
import idCodec from "./idCodec.ts";

import type {
  GetSomeOptions,
  ReadHole,
  ScanOptions,
  ScanResponse,
  WithId,
} from "../types.ts";

export interface DynamoIndexDriverOptions {
  client: DynamodbClient;
  table: string;
  hashRange?: number;
  partition: {
    key: string;
    prefix: string;
  };
  index: {
    name: string;
    key: string;
    value: number | string;
  };
  id: {
    key: string;
    prefix: string;
  };
}

const create = <T>({
  client,
  table,
  hashRange,
  partition: {
    key: partitionKey,
    prefix: partitionPrefix,
  },
  index: {
    name: indexName,
    key: indexKey,
    value: indexValue,
  },
  id: {
    key: idKey,
    prefix: idPrefix,
  },
}: DynamoIndexDriverOptions): ReadHole<T> => {
  const publicId = idCodec({ partitionKey, idKey });

  const get = async (id: string): Promise<WithId<T>> => {
    const result = await client.get(table, publicId.decode(id));

    return publicId.withEncoded(result.Item) as WithId<T>;
  };

  const getSome = async (
    { limit = 25 }: GetSomeOptions = {},
  ): Promise<WithId<T>[]> => {
    const sortFactor = Math.random();
    const scanForward = sortFactor < 0.5;
    const id = uuidRootForHashRange(await uuid(), hashRange);
    const idValue = `${idPrefix}:${id}`;

    const result = await client.query(table, {
      index: indexName,
      keyCondition: {
        partition: {
          key: indexKey,
          value: indexValue,
        },
      },
      scanForward,
      startKey: {
        [indexKey]: indexValue,
        [idKey]: idValue,
        [partitionKey]: partitionPrefix,
      },
      limit,
    });

    return result.Items.map(publicId.withEncoded) as WithId<T>[];
  };

  const scan = async (
    { startKey, limit = 25 }: ScanOptions = {},
  ): Promise<ScanResponse<T>> => {
    const result = await client.query(table, {
      index: indexName,
      keyCondition: {
        partition: {
          key: indexKey,
          value: indexValue,
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

    const response: ScanResponse<T> = {
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
  };
};

export default create;
