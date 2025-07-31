import dotenv from "dotenv";
import { Kafka, logLevel } from "kafkajs";

dotenv.config();

const KAFKA_BROKER_ADDRESS = process.env.KAFKA_BROKER_1!;
console.log({ KAFKA_BROKER_ADDRESS });

export const kafka = new Kafka({
  clientId: "red-store-streaming-kf-client",
  brokers: [KAFKA_BROKER_ADDRESS],
  logLevel: logLevel.ERROR,
});

export const admin = kafka.admin();
export const producer = kafka.producer({
  retry: {
    retries: 5,
    initialRetryTime: 100,
  },
  idempotent: true,
});
export const consumer = kafka.consumer({ groupId: "x-group" });
