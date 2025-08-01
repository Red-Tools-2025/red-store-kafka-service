import { Request, Response, Router } from "express";
import { getPartitionFromStoreId } from "../lib/kafka/kafka-actions";
import { kafka, producer } from "../lib/kafka/kafka-client";
import { redis } from "../lib/redis/redis-cache-client";
import Redis from "ioredis";

const router = Router();

// producer route for sending across inventory updates
interface InventoryProducerRequest {
  user_id: string;
  store_id: number;
  inv_update: {
    p_id: number;
    quantity: number;
  }[];
}
router.post(
  "/producer-event",
  async (req: Request<{}, {}, InventoryProducerRequest>, res: Response) => {
    try {
      const { store_id, user_id, inv_update } = req.body;
      const topic = `inventory-updates-event_${user_id}`;

      // generate partition number to send data to
      const parition_alloc = getPartitionFromStoreId(store_id, 10);
      await producer.connect();
      await producer.send({
        topic,
        messages: [
          {
            value: JSON.stringify({
              store_id,
              user_id,
              inv_update, // send full array
            }),
            partition: parition_alloc,
          },
        ],
      });

      await producer.disconnect();
      res.status(200).json({ status: `Updates Sent to ${topic}` });
      // Bring in producer to send data
    } catch (err) {
      console.log(err);
    }
  }
);

// consumer route for consuming events from topic
router.get("/consumer-event", async (req: Request, res: Response) => {
  const { user_id, store_id } = req.query;
  const topic = `inventory-updates-event_${user_id}`;
  const groupId = `consumer-${user_id}`;

  const consumer = kafka.consumer({ groupId });
  const partition_alloc = getPartitionFromStoreId(Number(store_id), 10);
  try {
    res.set({
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "http://localhost:3001",
      "Access-Control-Allow-Credentials": "true",
      "Access-Control-Allow-Headers": "Cache-Control",
    });

    await consumer.connect();
    await consumer.subscribe({ topic });

    // Send initial connection confirmation
    res.write(
      `data: {"type":"connected","message":"SSE connection established"}\n\n`
    );

    consumer.run({
      eachMessage: async ({ message, partition }) => {
        const data = message.value?.toString();
        if (partition === partition_alloc) {
          res.write(`data: ${data}\n\n`);
        }
      },
    });

    const pingInterval = setInterval(() => {
      res.write(`data: {"type":"ping"}\n\n`);
    }, 10000);

    req.on("close", async () => {
      console.log("Client disconnected, closing Kafka consumer");
      await consumer.disconnect();
      clearInterval(pingInterval);
      res.end();
    });
  } catch (err) {
    console.error("Error setting up SSE stream:", err);
    res.status(500).json({ message: "Internal Server Error" });
  }
});

// Endpoint for listening to valid channel name for redis messages
router.get("/updates-stream", async (req: Request, res: Response) => {
  const { user_id, store_id } = req.query;
  const channel_name = `updates_channel_inventory:${user_id}:${store_id}`;

  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "http://localhost:3001",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "Cache-Control",
  });

  try {
    // Send initial connection confirmation
    res.write(
      `data: {"type":"connected","message":"SSE connection established"}\n\n`
    );

    // We will need to create a new subscriber instance each time
    const redis_subscriber = new Redis({
      host: "localhost",
      port: 6379,
    });

    console.log(`Connecting new Redis subscriber for channel: ${channel_name}`);

    // Wait for connection
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("Connection timeout")),
        10000
      );

      redis_subscriber!.on("ready", () => {
        clearTimeout(timeout);
        console.log("Redis subscriber connected successfully");
        resolve(null);
      });

      redis_subscriber!.on("error", (err) => {
        clearTimeout(timeout);
        console.error("Redis subscriber connection error:", err);
        reject(err);
      });
    });

    // Subscribe to the channel
    await redis_subscriber.subscribe(channel_name);
    res.write(
      `data: {"type":"connected","message":"Channel connection established ready to listen"}\n\n`
    );

    // Setting up Message handler for writing to stream
    redis_subscriber.on(
      "message",
      (receivedChannel: string, message: string) => {
        console.log(`📫 Received message on ${receivedChannel}:`, message);
        try {
          const data = `data: ${message}\n\n`;
          res.write(data);
        } catch (err) {
          throw new Error(
            `Error during subscriber message reception details: ${err}`
          );
        }
      }
    );

    const pingInterval = setInterval(() => {
      res.write(`data: {"type":"ping"}\n\n`);
    }, 10000);

    // When closing SSE connection
    req.on("close", () => {
      redis_subscriber.unsubscribe(channel_name);
      redis_subscriber.quit();
      clearInterval(pingInterval);
    });
  } catch (err) {
    console.log("Error Setting up SSE stream for updates channel", err);
    res.status(500).json({ message: "Updates channel stream error" });
  }
});

export default router;
