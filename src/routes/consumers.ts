import { Request, Response, Router } from "express";
import { getPartitionFromStoreId } from "../lib/kafka/kafka-actions";
import { kafka } from "../lib/kafka/kafka-client";

const router = Router();

// consumer route for consuming events from topic
router.get("/consumer-event", async (req: Request, res: Response) => {
  const { user_id, store_id, event_topic } = req.query;
  const topic = `${event_topic}_${user_id}`;
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

export default router;
