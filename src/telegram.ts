import TelegramBot from "node-telegram-bot-api";
import { config } from "./config";
import { logger } from "./logger";
import { storage, StreamRecord, TickRecord } from "./storage";
import { DetectionResult } from "./detector";
import { buildMarketChart } from "./chart";
import { fetchKline } from "./opinion";

const token = config.telegramToken;
const chatIdRaw = config.telegramChatId?.trim() ?? "";
const chatId =
  chatIdRaw.length === 0
    ? null
    : !Number.isNaN(Number(chatIdRaw))
    ? Number(chatIdRaw)
    : chatIdRaw;

const bot = token ? new TelegramBot(token, { polling: false }) : null;

const MARKET_BASE_URL = "https://app.opinion.trade/detail";

const CHART_POINT_INTERVAL_MS = 3 * 60 * 1000;

const escapeHtml = (value: string) =>
  value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

const formatPriceAsPercent = (price: number) => `${(price * 100).toFixed(1)}%`;

const formatPriceChangePercent = (prevPrice: number, currentPrice: number) => {
  const change = currentPrice - prevPrice;
  const changePercent = (change * 100).toFixed(1);
  const sign = change >= 0 ? "+" : "";
  return `${sign}${changePercent}%`;
};

const formatVolume = (volume: number) => {
  if (volume >= 1_000_000) {
    return `$${(volume / 1_000_000).toFixed(1)}M`;
  }
  if (volume >= 1_000) {
    return `$${(volume / 1_000).toFixed(0)}K`;
  }
  return `$${volume.toFixed(0)}`;
};

const toMs = (ts: number) => (ts < 1_000_000_000_000 ? ts * 1000 : ts);

const downsampleTicks = (ticks: TickRecord[], intervalMs: number) => {
  const buckets = new Map<number, { tick: TickRecord; lastTs: number }>();
  for (const tick of ticks) {
    const tsMs = toMs(tick.ts);
    const bucket = Math.floor(tsMs / intervalMs) * intervalMs;
    const existing = buckets.get(bucket);
    if (!existing || existing.lastTs < tsMs) {
      buckets.set(bucket, { tick: { ...tick, ts: bucket }, lastTs: tsMs });
    }
  }
  return Array.from(buckets.values())
    .map((entry) => entry.tick)
    .sort((a, b) => toMs(a.ts) - toMs(b.ts));
};

const formatMessage = (
  stream: StreamRecord,
  tick: TickRecord,
  detection: DetectionResult
) => {
  // URL: children link to parent with &type=multi, solo/parents link to themselves
  const isMultiChild = !!stream.parentMarketId;
  const linkTopicId = isMultiChild ? stream.parentMarketId : stream.marketId;
  const url = isMultiChild
    ? `${MARKET_BASE_URL}?topicId=${linkTopicId}&type=multi`
    : `${MARKET_BASE_URL}?topicId=${linkTopicId}`;

  // Build market title as clickable link
  // For child markets: "Parent Title → Child Title" (entire string is one link)
  // For solo/parent markets: just the title
  let titleText: string;
  if (stream.parentMarketId) {
    const parent = storage.getStreamById(stream.parentMarketId);
    const isValidParent = parent && parent.chainId === stream.chainId;
    titleText = isValidParent
      ? `${parent.title} → ${stream.title}`
      : stream.title;
  } else {
    titleText = stream.title;
  }

  const prevPricePercent = formatPriceAsPercent(detection.prevPrice);
  const currentPricePercent = formatPriceAsPercent(tick.yesPrice);
  const changePercent = formatPriceChangePercent(detection.prevPrice, tick.yesPrice);

  const lines = [
    `🚨 Spike: <a href="${url}">${escapeHtml(titleText)}</a>`,
    "",
    `&gt; Price: ${prevPricePercent} → ${currentPricePercent} (${changePercent})`,
  ];
  return lines.join("\n");
};

export const telegramNotifier = {
  async sendAlert(stream: StreamRecord, tick: TickRecord, detection: DetectionResult) {
    if (!bot || !chatId) {
      logger.warn("Telegram notifier is not configured, skipping alert", {
        marketId: stream.marketId,
      });
      return;
    }
    logger.info("Sending Telegram alert", { marketId: stream.marketId });

    const nowMs = toMs(tick.ts);
    const windowMs = 2 * 60 * 60 * 1000;

    const recentRawTicks = storage.recentRawTicks(stream.marketId);
    let chartTicks: TickRecord[] = [tick, ...recentRawTicks]
      .filter((value, index, self) => self.findIndex((item) => item.ts === value.ts) === index)
      .filter((item) => {
        const tsMs = toMs(item.ts);
        return tsMs >= nowMs - windowMs && tsMs <= nowMs;
      });

    if (chartTicks.length === 0 && stream.topicId && stream.yesTokenId) {
      const kline = await fetchKline(
        stream.yesTokenId,
        stream.topicId,
        stream.chainId,
        nowMs - windowMs,
        nowMs
      );
      if (kline && kline.length > 0) {
        chartTicks = kline.map((point) => ({
          marketId: stream.marketId,
          ts: point.id * 1000,
          yesPrice: point.close,
          volume: point.vol,
          deltaVolume: 0,
        }));
      }
    }

    const chart = await buildMarketChart(downsampleTicks(chartTicks, CHART_POINT_INTERVAL_MS));
    const text = formatMessage(stream, tick, detection);

    if (chart) {
      await bot.sendPhoto(chatId, chart.buffer, {
        caption: text,
        parse_mode: "HTML",
      });
      return;
    }

    await bot.sendMessage(chatId, text, {
      disable_web_page_preview: true,
      parse_mode: "HTML",
    });
  },
};
