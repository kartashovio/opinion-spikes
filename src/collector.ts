import {
  listMarketsPaginated,
  latestPricePublic,
  fetchPrivateMarketVolumeById,
} from "./opinion";
import { storage, StreamRecord, TickRecord } from "./storage";
import { logger } from "./logger";
import { evaluateStream } from "./detector";

const envNumber = (key: string, fallback: number) => {
  const raw = process.env[key];
  if (!raw) return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
};

const MIN_TOTAL_VOLUME = envNumber("OPINION_MIN_TOTAL_VOLUME", 3_000);
const MIN_DELTA_VOLUME = envNumber("OPINION_MIN_DELTA_VOLUME", 80);
const BATCH_SIZE = 60;
const LOG_TICK_DETAILS = (process.env.OPINION_LOG_TICK_DETAILS ?? "0").trim().toLowerCase() === "1";
const LOG_VOLUME_DEBUG = (process.env.OPINION_LOG_VOLUME_DEBUG ?? "0").trim().toLowerCase() === "1";
const TITLE_BLOCKLIST = /up\s+or\s+down/i;

type CollectResult = {
  status: "stored" | "skipped" | "error";
  reason?: "no_payload" | "filters";
  alertSent?: boolean;
};

export async function refreshStreams() {
  logger.info("Refreshing streams from Opinion API");
  const startedAt = Date.now();
  const now = Date.now();
  let count = 0;
  let skippedByTitle = 0;
  for await (const stream of listMarketsPaginated()) {
    try {
      if (TITLE_BLOCKLIST.test(stream.title)) {
        skippedByTitle += 1;
        continue;
      }
      storage.saveStream({ ...stream, updatedAt: now });
      count += 1;
    } catch (error) {
      logger.error("Failed to persist stream", {
        error: (error as Error).message,
        stream,
      });
    }
  }
  logger.info(`Stream refresh complete, tracked ${count} streams`, {
    durationMs: Date.now() - startedAt,
    skippedByTitle,
  });
}

let isPolling = false;

export async function pollTicks() {
  if (isPolling) {
    logger.warn("Skipping tick poll: previous poll still running");
    return;
  }
  isPolling = true;
  const startedAt = Date.now();
  try {
    const streams = storage.listStreams();
    if (streams.length === 0) {
      logger.info("Skipping tick poll: no streams yet");
      return;
    }

    logger.info("Tick poll started", { total: streams.length });

    const stats = {
      total: streams.length,
      stored: 0,
      skippedNoPayload: 0,
      skippedFilters: 0,
      errors: 0,
      alerts: 0,
    };

    for (let i = 0; i < streams.length; i += BATCH_SIZE) {
      const batch = streams.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(
        batch.map((stream) => collectTickForStream(stream))
      );
      for (const result of results) {
        if (result.status === "stored") {
          stats.stored += 1;
          if (result.alertSent) {
            stats.alerts += 1;
          }
        } else if (result.status === "skipped" && result.reason === "no_payload") {
          stats.skippedNoPayload += 1;
        } else if (result.status === "skipped" && result.reason === "filters") {
          stats.skippedFilters += 1;
        } else if (result.status === "error") {
          stats.errors += 1;
        }
      }
    }

    logger.info("Tick poll summary", {
      ...stats,
      durationMs: Date.now() - startedAt,
    });
  } finally {
    isPolling = false;
  }
}

async function collectTickForStream(
  stream: StreamRecord
): Promise<CollectResult> {
  try {
    const pricePayload = await latestPricePublic(
      stream.yesTokenId,
      stream.topicId,
      stream.chainId
    );
    if (!pricePayload) {
      return { status: "skipped", reason: "no_payload" };
    }

    const privateVolume = await fetchPrivateMarketVolumeById(stream.marketId, stream.topicId);
    if (privateVolume === null) {
      if (LOG_VOLUME_DEBUG) {
        logger.info("Volume debug: missing payload", {
          marketId: stream.marketId,
          topicId: stream.topicId,
          parentMarketId: stream.parentMarketId,
          marketType: stream.marketType,
        });
      }
      return { status: "skipped", reason: "no_payload" };
    }

    const effectiveVolume = privateVolume;
    const volumeSource = "private";
    const lastTick = storage.recentRawTicks(stream.marketId, 1)[0];
    const rawDeltaVolume = lastTick ? effectiveVolume - lastTick.volume : 0;
    
    // Защита от отрицательного deltaVolume (ошибка API, сброс объёма)
    // Используем 0 чтобы не испортить EWMA статистику
    const deltaVolume = Math.max(0, rawDeltaVolume);
    
    if (rawDeltaVolume < 0) {
      logger.warn("Negative delta volume detected", {
        marketId: stream.marketId,
        currentVolume: effectiveVolume,
        lastVolume: lastTick?.volume,
        delta: rawDeltaVolume,
      });
    }

    const tick: TickRecord = {
      marketId: stream.marketId,
      ts: pricePayload.timestamp,
      yesPrice: pricePayload.price,
      volume: effectiveVolume,
      deltaVolume,
    };
    if (LOG_TICK_DETAILS) {
      logger.info("Tick details", {
        marketId: stream.marketId,
        price: tick.yesPrice,
        volume: tick.volume,
        deltaVolume: tick.deltaVolume,
        ts: tick.ts,
      });
    }
    if (LOG_VOLUME_DEBUG) {
      logger.info("Volume debug: resolved", {
        marketId: stream.marketId,
        topicId: stream.topicId,
        parentMarketId: stream.parentMarketId,
        marketType: stream.marketType,
        source: volumeSource,
        effectiveVolume,
        privateVolume,
        lastVolume: lastTick?.volume ?? null,
        rawDeltaVolume,
      });
    }

    if (effectiveVolume < MIN_TOTAL_VOLUME && deltaVolume < MIN_DELTA_VOLUME) {
      storage.pushRawTick(tick);
      return { status: "skipped", reason: "filters" };
    }

    // ВАЖНО: evaluateStream ПЕРЕД сохранением отфильтрованного тика,
    // чтобы при инициализации EWMA текущий тик не попал в историю.
    const alertSent = await evaluateStream(stream, tick);
    storage.pushRawAndTick(tick);
    return { status: "stored", alertSent };
  } catch (error) {
    logger.error("Failed to collect tick", {
      marketId: stream.marketId,
      error: (error as Error).message,
      stack: (error as Error).stack,
    });
    return { status: "error" };
  }
}
