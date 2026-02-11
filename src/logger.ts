const pretty = (value: unknown) => {
  if (typeof value === "object" && value !== null) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

const format = (level: string, message: string, meta?: unknown) =>
  `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}${
    meta !== undefined ? ` | ${pretty(meta)}` : ""
  }`;

export const logger = {
  info: (message: string, meta?: unknown) => {
    console.info(format("info", message, meta));
  },
  warn: (message: string, meta?: unknown) => {
    console.warn(format("warn", message, meta));
  },
  error: (message: string, meta?: unknown) => {
    console.error(format("error", message, meta));
  },
  debug: (message: string, meta?: unknown) => {
    if (process.env.LOG_LEVEL === "debug") {
      console.debug(format("debug", message, meta));
    }
  },
};
