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
