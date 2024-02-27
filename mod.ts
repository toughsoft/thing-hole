export { default as dynamoDriver } from "./drivers/dynamodb.ts";
export { default as dynamoIndexDriver } from "./drivers/dynamodb-index.ts";

export * from "./types.ts";
export type { DynamoDriverOptions } from "./drivers/dynamodb.ts";
export type { DynamoIndexDriverOptions } from "./drivers/dynamodb-index.ts";
