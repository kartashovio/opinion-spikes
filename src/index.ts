import { refreshStreams, pollTicks } from "./collector";
import { logger } from "./logger";

const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;
const HEARTBEAT_INTERVAL = 5 * MINUTE;

const isBlackoutWindow = (): boolean => {
  const m = new Date().getMinutes();
  // :56-:00 and :26-:30 — let the volume bot use the API
  return (m >= 56) || (m >= 26 && m <= 30);
};

const schedule = () => {
  setInterval(() => refreshStreams().catch((error) => logger.error("Hourly refresh failed", { error: (error as Error).message })), HOUR);
  setInterval(() => {
    if (isBlackoutWindow()) {
      logger.info("Skipping poll — blackout window");
      return;
    }
    pollTicks().catch((error) => logger.error("Minute poll failed", { error: (error as Error).message }));
  }, MINUTE);
  setInterval(() => {
    logger.info("Heartbeat", { uptimeSec: Math.round(process.uptime()) });
  }, HEARTBEAT_INTERVAL);
};

const run = async () => {
  await refreshStreams();
  await pollTicks();
  schedule();
  logger.info("Opinion spike bot started");
};

run().catch((error) => {
  logger.error("Failed to bootstrap bot", {
    error: (error as Error).message,
    stack: (error as Error).stack,
  });
  process.exit(1);
});
