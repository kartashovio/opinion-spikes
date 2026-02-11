import { storage, StreamRecord, TickRecord, EwmaState } from "./storage";
import { logger } from "./logger";
import { telegramNotifier } from "./telegram";
import { config } from "./config";

// EWMA parameters
const EWMA_SPAN = 20;
const ALPHA = 2 / (EWMA_SPAN + 1); // ≈ 0.095
const MIN_TICKS_FOR_DETECTION = 20;

// Detection thresholds
const envNumber = (key: string, fallback: number) => {
  const raw = process.env[key];
  if (!raw) return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
};

const ADJUSTED_Z_THRESHOLD = config.adjustedZThreshold;
const MIN_ABS_PRICE_CHANGE = envNumber(
  "OPINION_MIN_ABS_PRICE_CHANGE",
  0.03
); // Minimum 1.5 cents absolute price change
const MIN_STD_PRICE = 0.005;
const MIN_STD_VOLUME = 20;

// Adaptive threshold configuration
const USE_ADAPTIVE_THRESHOLDS = (process.env.OPINION_USE_ADAPTIVE_THRESHOLDS ?? "1") === "1";

// Base thresholds by price zone
const DEEP_EXTREME_ZONE_MIN_CHANGE = envNumber("OPINION_DEEP_EXTREME_ZONE_MIN_CHANGE", 0.07);  // 7% for <1% or >99%
const NEAR_EXTREME_ZONE_MIN_CHANGE = envNumber("OPINION_NEAR_EXTREME_ZONE_MIN_CHANGE", 0.10);  // 10% for 1-3% or 97-99%
const MIDDLE_ZONE_MIN_CHANGE = envNumber("OPINION_MIDDLE_ZONE_MIN_CHANGE", 0.15);              // 15% for 3-97%

// Alert cooldown
const ALERT_COOLDOWN = 6 * 60 * 60 * 1000; // 6 hours

const ALERT_TITLE_BLOCKLIST = (process.env.OPINION_ALERT_TITLE_BLOCKLIST ?? "")
  .split(",")
  .map((entry) => entry.trim().toLowerCase())
  .filter(Boolean);

const ALERT_TITLE_BLOCKLIST_REGEX = (() => {
  const raw = (process.env.OPINION_ALERT_TITLE_BLOCKLIST_REGEX ?? "").trim();
  if (!raw) {
    return null;
  }
  try {
    return new RegExp(raw, "i");
  } catch (error) {
    logger.warn("Invalid OPINION_ALERT_TITLE_BLOCKLIST_REGEX, ignoring", {
      message: (error as Error).message,
    });
    return null;
  }
})();

// Volume boost parameters
const VOLUME_BOOST_THRESHOLD = 1; // Start boosting above 1σ
const VOLUME_BOOST_FACTOR = envNumber("OPINION_VOLUME_BOOST_MULTIPLIER", 0.25); // +25% per σ above threshold

/**
 * Initialize EWMA state from historical ticks (cold start)
 */
export function initializeEwmaFromTicks(marketId: number, ticks: TickRecord[]): EwmaState {
  if (ticks.length === 0) {
    return {
      marketId,
      priceMean: 0,
      priceVar: 0,
      volumeMean: 0,
      volumeVar: 0,
      lastPrice: 0,
      tickCount: 0,
    };
  }

  // Sort oldest first for proper EWMA initialization
  const sorted = [...ticks].reverse();

  // Initialize with first value
  let priceMean = sorted[0].yesPrice;
  let priceVar = 0;
  let volumeMean = sorted[0].deltaVolume;
  let volumeVar = 0;

  // Process remaining ticks
  for (let i = 1; i < sorted.length; i++) {
    const tick = sorted[i];

    // Update price EWMA
    const priceDiff = tick.yesPrice - priceMean;
    priceMean = priceMean + ALPHA * priceDiff;
    priceVar = (1 - ALPHA) * (priceVar + ALPHA * priceDiff ** 2);

    // Update volume EWMA
    const volDiff = tick.deltaVolume - volumeMean;
    volumeMean = volumeMean + ALPHA * volDiff;
    volumeVar = (1 - ALPHA) * (volumeVar + ALPHA * volDiff ** 2);
  }

  const lastTick = sorted[sorted.length - 1];
  return {
    marketId,
    priceMean,
    priceVar,
    volumeMean,
    volumeVar,
    lastPrice: lastTick.yesPrice,
    tickCount: sorted.length,
  };
}

/**
 * Update EWMA state with new tick
 */
function updateEwmaState(state: EwmaState, tick: TickRecord): EwmaState {
  const priceDiff = tick.yesPrice - state.priceMean;
  const newPriceMean = state.priceMean + ALPHA * priceDiff;
  const newPriceVar = (1 - ALPHA) * (state.priceVar + ALPHA * priceDiff ** 2);

  const volDiff = tick.deltaVolume - state.volumeMean;
  const newVolumeMean = state.volumeMean + ALPHA * volDiff;
  const newVolumeVar = (1 - ALPHA) * (state.volumeVar + ALPHA * volDiff ** 2);

  return {
    marketId: state.marketId,
    priceMean: newPriceMean,
    priceVar: newPriceVar,
    volumeMean: newVolumeMean,
    volumeVar: newVolumeVar,
    lastPrice: tick.yesPrice,
    tickCount: state.tickCount + 1,
  };
}

/**
 * Compute Z-score using EWMA statistics
 */
function computeEwmaZ(value: number, mean: number, variance: number, minStd: number): number {
  const std = Math.max(Math.sqrt(variance), minStd);
  return (value - mean) / std;
}

/**
 * Calculate volume boost multiplier
 * Higher volume Z-score amplifies the price signal
 */
function calculateVolumeBoost(volumeZ: number): number {
  const excessZ = Math.max(0, volumeZ - VOLUME_BOOST_THRESHOLD);
  return 1 + excessZ * VOLUME_BOOST_FACTOR;
}

/**
 * Determine minimum absolute price change threshold based on price zone and volume
 * @param price Current YES price (0-1)
 * @param deltaVolume Recent volume delta
 * @returns Minimum absolute price change required to trigger alert
 */
function getAdaptiveThreshold(price: number, _deltaVolume: number): number {
  if (!USE_ADAPTIVE_THRESHOLDS) {
    return MIN_ABS_PRICE_CHANGE; // Fallback to legacy behavior
  }

  // Price zone-based thresholds
  if (price < 0.01 || price > 0.99) {
    return DEEP_EXTREME_ZONE_MIN_CHANGE;   // 7%
  } else if (price < 0.03 || price > 0.97) {
    return NEAR_EXTREME_ZONE_MIN_CHANGE;   // 10%
  } else {
    return MIDDLE_ZONE_MIN_CHANGE;         // 15%
  }
}

export interface DetectionResult {
  triggered: boolean;
  priceZ: number;
  volumeZ: number;
  adjustedScore: number;
  priceChange: number;
  prevPrice: number;
  adaptiveThreshold?: number;  // The threshold that was applied
  reason?: string;
}

const isTitleBlocked = (title: string) => {
  if (ALERT_TITLE_BLOCKLIST_REGEX?.test(title)) {
    return true;
  }
  if (ALERT_TITLE_BLOCKLIST.length === 0) {
    return false;
  }
  const normalized = title.toLowerCase();
  return ALERT_TITLE_BLOCKLIST.some((entry) => normalized.includes(entry));
};

const isStreamBlocked = (stream: StreamRecord) => {
  if (isTitleBlocked(stream.title)) {
    return true;
  }
  if (stream.parentMarketId) {
    const parent = storage.getStreamById(stream.parentMarketId);
    if (parent && isTitleBlocked(parent.title)) {
      return true;
    }
  }
  return false;
};

/**
 * Evaluate stream for spike detection
 */
export async function evaluateStream(
  stream: StreamRecord,
  tick: TickRecord
): Promise<boolean> {
  // Get or initialize EWMA state
  let ewmaState = storage.getEwmaState(stream.marketId);

  if (!ewmaState) {
    // Cold start: initialize from historical ticks
    const ticks = storage.recentTicks(stream.marketId);
    ewmaState = initializeEwmaFromTicks(stream.marketId, ticks);
    logger.debug("Initialized EWMA state from history", {
      marketId: stream.marketId,
      tickCount: ewmaState.tickCount,
    });
  }

  // Not enough data for detection
  if (ewmaState.tickCount < MIN_TICKS_FOR_DETECTION) {
    // Update EWMA state and save
    const updatedState = updateEwmaState(ewmaState, tick);
    storage.updateEwmaState(updatedState);
    return false;
  }

  // Calculate Z-scores BEFORE updating EWMA (compare against historical baseline)
  const priceZ = computeEwmaZ(tick.yesPrice, ewmaState.priceMean, ewmaState.priceVar, MIN_STD_PRICE);
  const volumeZ = computeEwmaZ(tick.deltaVolume, ewmaState.volumeMean, ewmaState.volumeVar, MIN_STD_VOLUME);

  // Calculate adjusted score with volume boost
  const volumeBoost = calculateVolumeBoost(volumeZ);
  const adjustedScore = Math.abs(priceZ) * volumeBoost;

  // Calculate absolute price change (защита от lastPrice = 0 при холодном старте)
  const hasValidLastPrice = ewmaState.lastPrice > 0;
  const priceChange = hasValidLastPrice
    ? tick.yesPrice - ewmaState.lastPrice
    : 0;
  const absPriceChange = Math.abs(priceChange);

  // Update EWMA state for next iteration
  const updatedState = updateEwmaState(ewmaState, tick);
  storage.updateEwmaState(updatedState);

  // Skip detection if no valid last price (first tick after cold start)
  if (!hasValidLastPrice) {
    return false;
  }

  // Check detection criteria
  const adaptiveThreshold = getAdaptiveThreshold(tick.yesPrice, tick.deltaVolume);
  const result: DetectionResult = {
    triggered: false,
    priceZ,
    volumeZ,
    adjustedScore,
    priceChange,
    prevPrice: ewmaState.lastPrice,
    adaptiveThreshold,
  };

  // Filter 1: Absolute price change must be significant
  if (absPriceChange < adaptiveThreshold) {
    result.reason = `price_change_too_small (${(absPriceChange * 100).toFixed(2)}% < ${(adaptiveThreshold * 100).toFixed(2)}%)`;
    return false;
  }

  // Filter 2: Adjusted score must exceed threshold
  if (adjustedScore < ADJUSTED_Z_THRESHOLD) {
    result.reason = "score_below_threshold";
    return false;
  }

  result.triggered = true;

  if (isStreamBlocked(stream)) {
    logger.info("Alert skipped by title blocklist", {
      marketId: stream.marketId,
      title: stream.title,
      parentMarketId: stream.parentMarketId ?? null,
    });
    return false;
  }

  // Check cooldown
  const alertState = storage.lastAlert(stream.marketId);
  if (alertState.lastAlertAt && Date.now() - alertState.lastAlertAt < ALERT_COOLDOWN) {
    logger.debug("Alert cooldown active", {
      marketId: stream.marketId,
      lastAlertAt: alertState.lastAlertAt,
    });
    return false;
  }

  // Check for duplicate alert
  const alertHash = `${stream.marketId}-${adjustedScore.toFixed(2)}-${absPriceChange.toFixed(3)}`;
  const DUPLICATE_ALERT_WINDOW = 6 * 60 * 60 * 1000;
  if (
    alertState.lastAlertHash === alertHash &&
    alertState.lastAlertAt &&
    Date.now() - alertState.lastAlertAt < DUPLICATE_ALERT_WINDOW
  ) {
    logger.debug("Alert already delivered", { marketId: stream.marketId, hash: alertHash });
    return false;
  }

  // Send alert
  try {
    await telegramNotifier.sendAlert(stream, tick, result);
    storage.updateAlert({
      marketId: stream.marketId,
      lastAlertAt: Date.now(),
      lastAlertHash: alertHash,
    });
    logger.info("Spike detected", {
      marketId: stream.marketId,
      title: stream.title,
      priceZ: priceZ.toFixed(2),
      volumeZ: volumeZ.toFixed(2),
      adjustedScore: adjustedScore.toFixed(2),
      priceChange: priceChange.toFixed(4),
      threshold: adaptiveThreshold.toFixed(4),
    });
    return true;
  } catch (error) {
    logger.error("Telegram notification failed", {
      marketId: stream.marketId,
      message: (error as Error).message,
    });
    return false;
  }
}
