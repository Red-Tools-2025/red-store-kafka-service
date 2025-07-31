import { Request, Response, Router } from "express";
import { getPartitionFromStoreId } from "../lib/kafka/kafka-actions";
import { kafka, producer } from "../lib/kafka/kafka-client";

const router = Router();

interface SalesProducerRequest {
  store_id: number;
  user_id: string;
  purchase_time: string;
  purchases: {
    product_id: number;
    productQuantity: number;
    product_price: number;
  }[];
}

router.post(
  "/produce-event",
  async (req: Request<{}, {}, SalesProducerRequest>, res: Response) => {
    try {
      const { purchase_time, purchases, store_id, user_id } = req.body;
      const topic = `sales-event_${user_id}`;

      // generate partition number to send data to
      const parition_alloc = getPartitionFromStoreId(store_id, 10);
      await producer.connect();
      await producer.send({
        topic,
        messages: [
          {
            value: JSON.stringify({
              store_id,
              purchase_time,
              purchases,
            }),
            partition: parition_alloc,
          },
        ],
      });

      await producer.disconnect();
      res.status(200).json({ status: `Updates Sent to ${topic}` });
    } catch (err) {
      console.log(err);
    }
  }
);
