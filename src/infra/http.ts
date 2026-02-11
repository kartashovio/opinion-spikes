import axios, {
  AxiosInstance,
  AxiosRequestConfig,
  AxiosResponse,
} from "axios";
import Bottleneck from "bottleneck";
import { config } from "../config";
import { logger } from "../logger";

const limiter = new Bottleneck({
  reservoir: 12,
  reservoirRefreshAmount: 12,
  reservoirRefreshInterval: 1000,
  maxConcurrent: 6,
  minTime: 85,
});

const headers: Record<string, string> = {};
if (config.apiKey) {
  headers.apikey = config.apiKey;
}

const timeoutMs = (() => {
  const raw = process.env.OPINION_HTTP_TIMEOUT_MS;
  if (!raw) return 30_000;
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value : 30_000;
})();

const client: AxiosInstance = axios.create({
  baseURL: config.apiBase,
  timeout: timeoutMs,
  headers,
});

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const safeJson = (value: unknown) => {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const joinUrl = (base?: string, path?: string) => {
  if (!path) return base ?? "";
  if (!base) return path;
  if (path.startsWith("http")) return path;
  return `${base.replace(/\/$/, "")}/${path.replace(/^\//, "")}`;
};

const truncate = (value: string | undefined, limit: number) => {
  if (!value) {
    return "";
  }
  return value.length > limit ? `${value.slice(0, limit)}...` : value;
};

const shouldRetry = (error: unknown) => {
  const axiosError = error as { code?: string; response?: { status?: number } };
  if (axiosError?.code === "ECONNABORTED") {
    return true;
  }
  const status = axiosError?.response?.status;
  return typeof status === "number" && status >= 500;
};

const wrap = <T>(call: () => Promise<AxiosResponse<T>>) =>
  limiter.schedule(async () => {
    const retries = [0, 300];
    let lastError: unknown;
    for (const backoff of retries) {
      if (backoff > 0) {
        await delay(backoff);
      }
      try {
        const response = await call();
        return response.data;
      } catch (error) {
        lastError = error;
        const axiosError = error as {
          code?: string;
          response?: { status?: number; data?: unknown };
          config?: {
            baseURL?: string;
            url?: string;
            method?: string;
            params?: unknown;
          };
        };
        if (!shouldRetry(error) || backoff === retries[retries.length - 1]) {
          logger.error("HTTP request failed", {
            message: (error as { message?: string })?.message ?? "unknown",
            stack: (error as { stack?: string })?.stack,
            status: axiosError?.response?.status,
            url: joinUrl(axiosError?.config?.baseURL, axiosError?.config?.url),
            method: axiosError?.config?.method,
            params: axiosError?.config?.params,
            response: truncate(safeJson(axiosError?.response?.data), 1500),
          });
          throw error;
        }
      }
    }
    throw lastError;
  });

export const http = {
  request<T>(options: AxiosRequestConfig) {
    return wrap(() => client.request<T>(options));
  },
  get<T>(path: string, config?: AxiosRequestConfig) {
    return wrap(() => client.get<T>(path, config));
  },
};
