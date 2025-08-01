import { Request, Response, Router } from "express";
import { getPartitionFromStoreId } from "../lib/kafka/kafka-actions";
import { producer } from "../lib/kafka/kafka-client";

const router = Router();

interface SalesProducerRequest {
  store_id: number;
  user_id: string;
  sales_records: {
    purchases: {
      product_id: number;
      product_current_stock: number;
      product_name: string;
      product_price: number;
      productQuantity: number;
    }[];
    purchase_time: string;
  }[];
}

router.post(
  "/produce-event",
  async (req: Request<{}, {}, SalesProducerRequest>, res: Response) => {
    try {
      const { sales_records, store_id, user_id } = req.body;
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
              sales_records,
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

export default router;
