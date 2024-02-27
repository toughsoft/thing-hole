import { Buffer } from "node:buffer";

import type { DynamoObject } from "./deps.ts";

import type { WithId } from "../types.ts";

interface IdCodecOptions {
  idKey: string;
  partitionKey: string;
}

export default function idCodec({
  idKey,
  partitionKey,
}: IdCodecOptions) {
  const encode = (item: DynamoObject): string => {
    const {
      [partitionKey]: partitionValue,
      [idKey]: idValue,
    } = item;

    return Buffer.from(JSON.stringify({
      [partitionKey]: partitionValue,
      [idKey]: idValue,
    })).toString("base64");
  };

  const decode = (id: string): DynamoObject => {
    return JSON.parse(Buffer.from(id, "base64").toString("utf8"));
  };

  const withEncoded = ({ ...item }: DynamoObject): WithId<DynamoObject> => {
    const id = encode(item);

    delete item[partitionKey];
    delete item[idKey];

    return { ...item, id };
  };

  const withDecoded = (itemWithId: WithId<DynamoObject>): DynamoObject => {
    const decodedProps = decode(itemWithId.id);
    const { ...item }: Partial<WithId<DynamoObject>> = itemWithId;

    delete item.id;

    return {
      ...item,
      ...decodedProps,
    };
  };

  return { encode, decode, withEncoded, withDecoded };
}
