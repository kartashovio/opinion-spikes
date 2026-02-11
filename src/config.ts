import dotenv from "dotenv";
import { logger } from "./logger";

dotenv.config();

const envVar = (key: string, fallback?: string): string => {
  const value = process.env[key] ?? fallback;
  if (!value) {
    logger.warn("Missing environment variable", key);
  }
  return value ?? "";
};

const envInt = (key: string, fallback: number): number => {
  const raw = process.env[key];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    logger.warn("Invalid numeric environment variable", key);
    return fallback;
  }
  return Math.floor(value);
};

const envFloat = (key: string, fallback: number): number => {
  const raw = process.env[key];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    logger.warn("Invalid numeric environment variable", key);
    return fallback;
  }
  return value;
};

export const config = {
  apiBase: envVar("OPINION_API_BASE", "https://proxy.opinion.trade:8443/openapi"),
  apiKey: envVar("OPINION_API_KEY"),
  publicApiBase: envVar(
    "OPINION_PUBLIC_API_BASE",
    "https://proxy.opinion.trade:8443/api/bsc/api"
  ),
  publicTopicsEndpoint: envVar("OPINION_PUBLIC_TOPICS_ENDPOINT", "/v2/topic"),
  publicTopicPageWorkers: envInt("OPINION_PUBLIC_TOPIC_PAGE_WORKERS", 16),
  publicTopicPageSize: envInt("OPINION_PUBLIC_TOPIC_PAGE_SIZE", 100),
  publicTopicDetailEndpoint: envVar("OPINION_PUBLIC_TOPIC_DETAIL_ENDPOINT", "/v2/topic"),
  publicTopicMultiEndpoint: envVar("OPINION_PUBLIC_TOPIC_MULTI_ENDPOINT", "/v2/topic/mutil"),
  publicTopicDetailNotFoundStop: envInt("OPINION_PUBLIC_TOPIC_DETAIL_NOT_FOUND_STOP", 5),
  publicTopicMultiNotFoundStop: envInt("OPINION_PUBLIC_TOPIC_MULTI_NOT_FOUND_STOP", 5),
  publicOrderbookEndpoint: envVar(
    "OPINION_PUBLIC_ORDERBOOK_ENDPOINT",
    "/v2/order/market/depth"
  ),
  adjustedZThreshold: envFloat("OPINION_ADJUSTED_Z_THRESHOLD", 2.5),
  telegramToken: envVar("TELEGRAM_BOT_TOKEN"),
  telegramChatId: envVar("TELEGRAM_CHAT_ID"),
};
