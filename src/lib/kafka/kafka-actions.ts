import crypto from "crypto";
import { producer } from "./kafka-client";

export async function sendMessage(topic: string, message: any) {
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }],
  });
  await producer.disconnect();
}

export function getPartitionFromStoreId(
  storeId: number,
  numPartitions: number
): number {
  // Switching to MD5 Hash to ensure consistent mapping for topic messages
  const hash = crypto
    .createHash("md5")
    .update(storeId.toString())
    .digest("hex");
  const hashInt = parseInt(hash.slice(0, 8), 16);
  return hashInt % numPartitions;
}
