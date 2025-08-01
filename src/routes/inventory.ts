import { Request, Response, Router } from "express";
import { getPartitionFromStoreId } from "../lib/kafka/kafka-actions";
import { kafka, producer } from "../lib/kafka/kafka-client";

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
  } catch (err) {
    console.log("Error Setting up SSE stream for updates channel", err);
    res.status(500).json({ message: "Updates channel stream error" });
  }
});

export default router;
