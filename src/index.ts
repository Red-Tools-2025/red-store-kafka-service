import cors from "cors";
import express, { Response, Request } from "express";
import { admin, consumer, producer } from "./lib/kafka/kafka-client";
import { getPartitionFromStoreId } from "./lib/kafka/kafka-actions";
import inventoryEventsRouter from "./routes/inventory";
import salesEventRouter from "./routes/sales";

const PORT = 3000;
const app = express();

app.use(express.json());
app.use("/inventory", inventoryEventsRouter);
app.use("/sales", salesEventRouter);

app.use(
  cors({
    origin: "http://localhost:3001", // Your frontend origin
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "Cache-Control"],
  })
);

app.get("/", (req: Request, res: Response) => {
  res.status(200).send("Streaming Service up and running");
});

interface SendTestReqBody {
  topic: string;
  message: any;
  storeId: number;
}

app.post(
  "/send",
  async (req: Request<{}, {}, SendTestReqBody>, res: Response) => {
    const { topic, message, storeId } = req.body;

    if (!topic || !message || !storeId) {
      return res
        .status(400)
        .json({ error: "Both 'topic' and 'message' are required" });
    }

    try {
      const partition_number = getPartitionFromStoreId(storeId, 10);
      await producer.connect();
      await producer.send({
        topic,
        messages: [
          { value: JSON.stringify(message), partition: partition_number },
        ],
      });
      await producer.disconnect();
      res.status(200).json({ status: "Message sent", topic, message });
    } catch (err: any) {
      console.error("Kafka error:", err.message);
      res
        .status(500)
        .json({ error: "Failed to send message", details: err.message });
    }
  }
);

// for consuming messages and consumer groups
app.get("/consume", async (req: Request, res: Response) => {
  const { topic, storeId } = req.query;

  if (!topic || !storeId) {
    return res.status(400).json({ error: "Missing topic or storeId" });
  }

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: topic as string, fromBeginning: true });

    const messages: { decoded: string; partition: number }[] = [];

    await consumer.run({
      eachMessage: async ({ topic, message, partition }) => {
        const decoded = message.value?.toString();
        if (decoded) messages.push({ decoded, partition });
      },
    });

    setTimeout(async () => {
      await consumer.disconnect();
      return res.status(200).json({ messages });
    }, 3000);
  } catch (e) {
    console.error("Error consuming:", e);
    return res.status(500).json({ error: "Kafka consume failed" });
  }
});

// Endpoint for set up of topics for both inventory updates and sales events
app.post("/validate-event-topic", async (req: Request, res: Response) => {
  const { topic, user_id, retention_ms } = req.body;
  if (!topic || typeof topic !== "string" || !user_id) {
    return res.status(400).json({ error: "Invalid or incomplete params" });
  }
  try {
    // Create topic name
    const topic_name = `${topic}_${user_id}`;

    // Validate topic name's existence
    await admin.connect();
    const topics = await admin.listTopics();

    // Create topic
    if (!topics.includes(topic_name)) {
      console.log(`Topic "${topic_name}" does not exist. Creating...`);
      const created = await admin.createTopics({
        topics: [
          {
            topic: topic_name,
            numPartitions: 10,
            configEntries: [
              {
                name: "retention.ms",
                value: retention_ms,
              },
            ],
          },
        ],
      });

      if (!created) {
        throw new Error("Topic creation reported failure");
      }
      console.log(`Topic "${topic_name}" created.`);
    } else {
      console.log(`Topic "${topic_name}" already exists.`);
    }

    await admin.disconnect();

    res.status(200).json({ message: `Topic "${topic_name}" is ready.` });
  } catch (err) {
    console.log(`Error during topic creation: ${err}`);
    try {
      await admin.disconnect();
    } catch {
      // ignore disconnect errors
    }
    res.status(500).json({ error: "Failed to ensure topic", details: err });
  }
});

app
  .listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  })
  .on("error", (error: any) => {
    throw new Error(error.message);
  });
