import { http } from "./infra/http";
import { logger } from "./logger";
import { config } from "./config";

const guardResult = (value: unknown): { list: unknown[]; total?: number } | null => {
  if (typeof value !== "object" || value === null) {
    logger.warn("Unexpected /market result format", value);
    return null;
  }
  const list = (value as { list?: unknown[] }).list;
  if (!Array.isArray(list)) {
    logger.warn("Market result payload missing list", value);
    return null;
  }
  const rawTotal = (value as { total?: number | string }).total;
  const total =
    rawTotal === undefined
      ? undefined
      : Number.isFinite(Number(rawTotal))
      ? Number(rawTotal)
      : undefined;
  return { list, total };
};

const buildPublicEndpoint = (endpoint: string) => {
  if (endpoint.startsWith("http")) {
    return endpoint;
  }
  const base = config.publicApiBase.replace(/\/$/, "");
  const suffix = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
  return `${base}${suffix}`;
};

const LOG_VOLUME_DEBUG =
  (process.env.OPINION_LOG_VOLUME_DEBUG ?? "0").trim().toLowerCase() === "1";

const extractServerTime = (payload: unknown): number | null => {
  const candidates = ["serverTime", "server_time", "timestamp", "time", "ts"];
  const probe = (value: unknown): number | null => {
    if (typeof value === "number" || typeof value === "string") {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    }
    if (typeof value === "object" && value !== null) {
      for (const key of candidates) {
        if (key in (value as Record<string, unknown>)) {
          const numeric = Number((value as Record<string, unknown>)[key]);
          if (Number.isFinite(numeric)) {
            return numeric;
          }
        }
      }
    }
    return null;
  };
  return probe(payload);
};

const extractDetailPayload = (payload: unknown): RawMarket | null => {
  if (typeof payload !== "object" || payload === null) {
    return null;
  }
  const root = payload as Record<string, unknown>;
  const result = (root.result ?? root.data ?? root) as Record<string, unknown> | undefined;
  if (!result || typeof result !== "object") {
    return null;
  }
  const data = (result.data ?? result) as Record<string, unknown>;
  if (!data || typeof data !== "object") {
    return null;
  }
  return data as unknown as RawMarket;
};

let cachedServerTime: { value: number; fetchedAt: number } | null = null;
const SERVER_TIME_TTL_MS = 30_000;

export async function fetchServerTime(): Promise<number | null> {
  if (cachedServerTime && Date.now() - cachedServerTime.fetchedAt < SERVER_TIME_TTL_MS) {
    return cachedServerTime.value;
  }
  try {
    const endpoint = buildPublicEndpoint("/v2/server/time");
    const response = await http.get<{
      errno?: number | string;
      errmsg?: string;
      result?: unknown;
      data?: unknown;
    }>(endpoint);
    const payload = response?.result ?? response?.data ?? response;
    const raw = extractServerTime(payload);
    if (!raw || raw <= 0) {
      return null;
    }
    const value = raw < 1_000_000_000_000 ? raw * 1000 : raw;
    cachedServerTime = { value, fetchedAt: Date.now() };
    return value;
  } catch (error) {
    logger.debug("Server time request failed", { message: (error as Error).message });
    return null;
  }
}

export async function fetchMultiChildVolumes(topicId: string): Promise<Map<string, number> | null> {
  const endpoint = buildPublicEndpoint(config.publicTopicMultiEndpoint);
  const url = endpoint.endsWith(`/${topicId}`) ? endpoint : `${endpoint}/${topicId}`;
  try {
    const response = await http.get<{
      errno?: number | string;
      errmsg?: string;
      result?: unknown;
      data?: unknown;
    }>(url);
    const root = (response as any)?.result ?? (response as any)?.data ?? response;
    const data = (root as any)?.data ?? root;
    const childList: unknown[] =
      (data as any)?.childList ??
      (data as any)?.childMarkets ??
      (data as any)?.child_list ??
      [];
    if (!Array.isArray(childList) || childList.length === 0) {
      if (LOG_VOLUME_DEBUG) {
        logger.info("Volume debug: empty multi child list", {
          topicId,
          sampleKeys: typeof data === "object" && data ? Object.keys(data) : [],
        });
      }
      return null;
    }
    const map = new Map<string, number>();
    for (const child of childList) {
      const record = child as Record<string, unknown>;
      const volumeRaw =
        record.volume ??
        record.totalVolume ??
        record.total_volume ??
        record.totalVol ??
        record.total_vol ??
        record.totalPrice ??
        record.total_price ??
        0;
      const volume = Number(volumeRaw);
      if (!Number.isFinite(volume)) {
        continue;
      }
      const candidateIds = [
        record.topicId,
        record.marketId,
        record.questionId,
        record.question_id,
      ]
        .map((value) => (value === undefined || value === null ? null : String(value)))
        .filter((value): value is string => Boolean(value));
      if (candidateIds.length === 0) {
        continue;
      }
      for (const candidate of candidateIds) {
        if (!map.has(candidate)) {
          map.set(candidate, volume);
          continue;
        }
        if (LOG_VOLUME_DEBUG && map.get(candidate) !== volume) {
          logger.info("Volume debug: multi child volume conflict", {
            topicId,
            key: candidate,
            existing: map.get(candidate),
            incoming: volume,
          });
        }
      }
    }
    if (LOG_VOLUME_DEBUG) {
      const sample = Array.from(map.entries()).slice(0, 3);
      logger.info("Volume debug: multi child volumes", {
        topicId,
        childCount: childList.length,
        mappedCount: map.size,
        sample,
      });
    }
    return map.size > 0 ? map : null;
  } catch (error) {
    logger.debug("Multi topic volume request failed", {
      topicId,
      message: (error as Error).message,
    });
    return null;
  }
}

const parseVolume = (value: unknown): number | null => {
  if (value === null || value === undefined) {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const extractVolume = (record: RawMarket): number | null => {
  return (
    parseVolume(record.volume) ??
    parseVolume(record.totalVolume) ??
    parseVolume(record.total_volume) ??
    parseVolume(record.totalVol) ??
    parseVolume(record.total_vol) ??
    parseVolume(record.totalPrice) ??
    parseVolume(record.total_price)
  );
};

export async function fetchTopicVolumeById(topicId: string | number): Promise<number | null> {
  const endpoint = buildPublicEndpoint(config.publicTopicsEndpoint);
  try {
    const detailEndpoint = buildPublicEndpoint(config.publicTopicDetailEndpoint);
    const detailUrl = detailEndpoint.endsWith(`/${topicId}`)
      ? detailEndpoint
      : `${detailEndpoint}/${topicId}`;
    try {
      const detailResponse = await http.get<{
        errno?: number | string;
        errmsg?: string;
        result?: unknown;
        data?: unknown;
      }>(detailUrl);
      const detail = extractDetailPayload(detailResponse);
      if (detail) {
        const volume = extractVolume(detail);
        if (volume !== null) {
          if (LOG_VOLUME_DEBUG) {
            logger.info("Volume debug: topic detail volume resolved", {
              topicId,
              volume,
            });
          }
          return volume;
        }
        if (LOG_VOLUME_DEBUG) {
          logger.info("Volume debug: topic detail missing volume", {
            topicId,
            keys: typeof detail === "object" && detail ? Object.keys(detail) : [],
          });
        }
      }
    } catch (error) {
      logger.debug("Topic detail volume request failed", {
        topicId,
        message: (error as Error).message,
      });
    }
    const response = await http.get<{
      errno?: number | string;
      errmsg?: string;
      result?: unknown;
      data?: unknown;
    }>(endpoint, {
      params: {
        page: 1,
        limit: 1,
        statusEnum: "Activated",
        topicId: String(topicId),
        topic_id: String(topicId),
        questionId: String(topicId),
        question_id: String(topicId),
        marketId: String(topicId),
      },
    });
    const root = (response as any)?.result ?? (response as any)?.data ?? response;
    const list = (root as any)?.list ?? (root as any)?.data?.list ?? [];
    if (!Array.isArray(list) || list.length === 0) {
      if (LOG_VOLUME_DEBUG) {
        logger.info("Volume debug: empty topic list", {
          topicId,
          sampleKeys: typeof root === "object" && root ? Object.keys(root) : [],
        });
      }
      return null;
    }
    const requested = String(topicId);
    const item =
      list.find((entry: any) => {
        const candidates = [
          entry?.marketId,
          entry?.topicId,
          entry?.questionId,
          entry?.question_id,
        ].filter((value) => value !== undefined && value !== null);
        return candidates.some((value) => String(value) === requested);
      }) ?? null;
    if (!item) {
      logger.debug("Topic volume response did not match requested market", {
        requested,
        sample: list[0]?.marketId ?? list[0]?.topicId ?? list[0]?.questionId ?? list[0]?.question_id,
      });
      if (LOG_VOLUME_DEBUG) {
        logger.info("Volume debug: topic not found in list", {
          topicId,
          listSize: list.length,
          firstId: list[0]?.marketId ?? list[0]?.topicId ?? null,
        });
      }
      return null;
    }
    const raw =
      (item as any).volume ??
      (item as any).totalVolume ??
      (item as any).total_volume ??
      (item as any).totalVol ??
      (item as any).total_vol ??
      (item as any).totalPrice ??
      (item as any).total_price ??
      null;
    if (raw === null || raw === undefined) {
      if (LOG_VOLUME_DEBUG) {
        logger.info("Volume debug: topic volume missing fields", {
          topicId,
          keys: typeof item === "object" && item ? Object.keys(item) : [],
        });
      }
      return null;
    }
    const numeric = Number(raw);
    if (LOG_VOLUME_DEBUG) {
      logger.info("Volume debug: topic volume resolved", {
        topicId,
        volume: Number.isFinite(numeric) ? numeric : null,
      });
    }
    return Number.isFinite(numeric) ? numeric : null;
  } catch (error) {
    logger.debug("Topic volume request failed", {
      topicId,
      message: (error as Error).message,
    });
    return null;
  }
}

const pickPrivateMarketList = (payload: unknown): RawMarket[] | null => {
  const root = (payload as any)?.result ?? (payload as any)?.data ?? payload;
  const data = (root as any)?.data ?? root;
  const list = (data as any)?.list ?? (root as any)?.list ?? null;
  if (Array.isArray(list)) {
    return list as RawMarket[];
  }
  if (data && typeof data === "object") {
    return [data as RawMarket];
  }
  return null;
};

const findPrivateMarket = (
  list: RawMarket[],
  marketId: number,
  questionId?: string
): RawMarket | null => {
  const marketIdStr = String(marketId);
  const questionIdStr = questionId ? String(questionId) : null;
  const match = list.find((entry) => {
    const candidates = [
      entry.marketId,
      entry.topicId,
      entry.questionId,
    ].filter((value) => value !== undefined && value !== null);
    return candidates.some((value) => {
      const valueStr = String(value);
      return valueStr === marketIdStr || (questionIdStr !== null && valueStr === questionIdStr);
    });
  });
  return match ?? null;
};

export async function fetchPrivateMarketVolumeById(
  marketId: number,
  questionId?: string
): Promise<number | null> {
  try {
    const detailResponse = await http.get<{
      code?: number | string;
      errno?: number | string;
      errmsg?: string;
      result?: unknown;
      data?: unknown;
    }>(`/market/${marketId}`);
    const detailList = pickPrivateMarketList(detailResponse);
    if (detailList && detailList.length > 0) {
      const match = findPrivateMarket(detailList, marketId, questionId);
      if (match) {
        const volume = extractVolume(match);
        return volume ?? null;
      }
    }
  } catch (error) {
    logger.debug("Private market detail request failed", {
      marketId,
      message: (error as Error).message,
    });
  }

  try {
    const listResponse = await http.get<{
      code?: number | string;
      errno?: number | string;
      errmsg?: string;
      result?: unknown;
      data?: unknown;
    }>("/market", {
      params: {
        page: 1,
        limit: 1,
        marketId: String(marketId),
        topicId: questionId ? String(questionId) : undefined,
        questionId: questionId ? String(questionId) : undefined,
      },
    });
    const list = pickPrivateMarketList(listResponse);
    if (!list || list.length === 0) {
      return null;
    }
    const match = findPrivateMarket(list, marketId, questionId);
    if (!match) {
      return null;
    }
    return extractVolume(match);
  } catch (error) {
    logger.debug("Private market list request failed", {
      marketId,
      message: (error as Error).message,
    });
    return null;
  }
}

const isNotFoundTopicError = (error: unknown) => {
  const axiosError = error as { response?: { data?: { errno?: number | string } } };
  const rawCode = axiosError?.response?.data?.errno;
  const code = rawCode === undefined ? NaN : Number(rawCode);
  return code === 10200;
};

export interface MarketStream {
  marketId: number;
  yesTokenId: string;
  title: string;
  parentMarketId?: number;
  topicId: string;
  marketType?: number;
  chainId?: number;
}

interface RawMarket {
  marketId: number | string;
  marketTitle?: string;
  title?: string;
  yesTokenId?: string;
  yesPos?: string;
  noPos?: string;
  parentMarketId?: number | string;
  childMarkets?: unknown[];
  childList?: unknown[];
  questionId?: string;
  topicId?: number | string;
  marketType?: number | string;
  topicType?: number | string;
  isMulti?: boolean | number;
  isMultiParent?: boolean | number;
  chainId?: number | string;
  statusEnum?: string;
  status?: number | string;
  volume?: number | string;
  totalVolume?: number | string;
  total_volume?: number | string;
  totalVol?: number | string;
  total_vol?: number | string;
  totalPrice?: number | string;
  total_price?: number | string;
  cutoffAt?: number | string;
  resolvedAt?: number | string;
  createdAt?: number | string;
}

const isActivated = (input: RawMarket, fallbackStatusEnum?: string) => {
  const statusEnum = input.statusEnum ?? fallbackStatusEnum;
  if (statusEnum) {
    return statusEnum === "Activated";
  }
  const statusValue = input.status === undefined ? NaN : Number(input.status);
  return statusValue === 2;
};

const asTimestampMs = (value: number | string | undefined): number | null => {
  if (value === undefined || value === null) {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric < 1_000_000_000_000 ? numeric * 1000 : numeric;
};

const isActiveByTime = (input: RawMarket, nowMs: number): boolean => {
  const resolvedAtMs = asTimestampMs(input.resolvedAt);
  if (resolvedAtMs && resolvedAtMs > 0 && resolvedAtMs <= nowMs) {
    return false;
  }
  const cutoffAtMs = asTimestampMs(input.cutoffAt);
  if (cutoffAtMs && cutoffAtMs <= nowMs) {
    return false;
  }
  return true;
};

const normalize = (
  input: RawMarket,
  parent?: number,
  parentTopicId?: string
): MarketStream | null => {
  const marketIdValue =
    input.marketId ?? (input.topicId !== undefined ? input.topicId : undefined);
  const yesTokenValue = input.yesTokenId ?? input.yesPos;
  const hasChildren = Array.isArray(input.childList) && input.childList.length > 0;

  // Multi-market parents don't have yesTokenId - allow them with placeholder
  // They're needed in DB for displaying parent titles in alerts
  if (!marketIdValue) {
    return null;
  }
  if (!yesTokenValue && !hasChildren) {
    logger.warn("Skipping market without required fields", input);
    return null;
  }

  const topicId = input.questionId ? String(input.questionId) : parentTopicId ?? "";
  if (!topicId) {
    logger.warn("Market missing topicId (questionId)", input);
  }
  const marketTypeValue =
    input.marketType === undefined
      ? input.topicType === undefined
        ? undefined
        : Number(input.topicType)
      : Number(input.marketType);
  const chainIdValue = input.chainId === undefined ? undefined : Number(input.chainId);
  return {
    marketId: Number(marketIdValue),
    yesTokenId: yesTokenValue ? String(yesTokenValue) : `multi-parent-${marketIdValue}`,
    title: input.marketTitle ?? input.title ?? `market-${marketIdValue}`,
    parentMarketId: parent ?? (input.parentMarketId ? Number(input.parentMarketId) : undefined),
    topicId,
    marketType: hasChildren ? 1 : (Number.isFinite(marketTypeValue) ? marketTypeValue : undefined),
    chainId: Number.isFinite(chainIdValue) ? chainIdValue : undefined,
  };
};

export async function* listMarketsPaginated() {
  const nowMs = (await fetchServerTime()) ?? Date.now();
  const limit = Math.max(1, config.publicTopicPageSize || 100);
  const workers = Math.max(1, config.publicTopicPageWorkers || 1);
  let page = 1;
  let totalPages: number | null = null;
  let done = false;
  const endpoint = buildPublicEndpoint(config.publicTopicsEndpoint);
  const detailEndpoint = buildPublicEndpoint(config.publicTopicDetailEndpoint);
  const multiEndpoint = buildPublicEndpoint(config.publicTopicMultiEndpoint);
  const detailCache = new Map<string, RawMarket | null>();
  const multiCache = new Map<string, RawMarket | null>();
  let detailNotFoundStreak = 0;
  let multiNotFoundStreak = 0;
  let skipDetailRequests = false;
  let skipMultiRequests = false;

  const fetchPage = async (pageNumber: number) => {
    logger.info("Fetching topic page", { page: pageNumber, limit });
    const response = await http.get<{
      code?: number | string;
      errno?: number | string;
      errmsg?: string;
      result?: unknown;
      data?: unknown;
    }>(endpoint, {
      params: { statusEnum: "Activated", limit, page: pageNumber },
    });

    const rawCode = response?.code ?? response?.errno;
    const code = rawCode === undefined ? NaN : Number(rawCode);
    if (Number.isNaN(code) || code !== 0) {
      logger.warn("Market list returned unexpected code", response);
      return null;
    }

    let resultPayload: unknown = response?.result ?? response?.data ?? response;
    if (typeof resultPayload === "string") {
      try {
        resultPayload = JSON.parse(resultPayload);
      } catch (error) {
        logger.warn("Failed to parse market result string", {
          error: (error as Error).message,
          raw: resultPayload,
        });
        return null;
      }
    }

    const result = guardResult(resultPayload);
    if (!result) {
      return null;
    }
    return result;
  };

  const fetchTopicDetail = async (topicId: string): Promise<RawMarket | null> => {
    if (detailCache.has(topicId)) {
      return detailCache.get(topicId) ?? null;
    }
    if (skipDetailRequests) {
      return null;
    }
    try {
      const response = await http.get(detailEndpoint.endsWith(`/${topicId}`)
        ? detailEndpoint
        : `${detailEndpoint}/${topicId}`);
      const detail = extractDetailPayload(response);
      if (detail) {
        detailNotFoundStreak = 0;
      }
      detailCache.set(topicId, detail);
      return detail;
    } catch (error) {
      if (isNotFoundTopicError(error)) {
        detailNotFoundStreak += 1;
        if (detailNotFoundStreak >= config.publicTopicDetailNotFoundStop) {
          skipDetailRequests = true;
          logger.warn("Disabling topic detail requests after repeated not-found errors", {
            streak: detailNotFoundStreak,
          });
        }
      } else {
        detailNotFoundStreak = 0;
        logger.debug("Topic detail request failed", {
          topicId,
          message: (error as Error).message,
        });
      }
      detailCache.set(topicId, null);
      return null;
    }
  };

  const fetchMultiTopic = async (topicId: string): Promise<RawMarket | null> => {
    if (multiCache.has(topicId)) {
      return multiCache.get(topicId) ?? null;
    }
    if (skipMultiRequests) {
      return null;
    }
    try {
      const response = await http.get(multiEndpoint.endsWith(`/${topicId}`)
        ? multiEndpoint
        : `${multiEndpoint}/${topicId}`);
      const detail = extractDetailPayload(response);
      if (detail) {
        multiNotFoundStreak = 0;
      }
      multiCache.set(topicId, detail);
      return detail;
    } catch (error) {
      if (isNotFoundTopicError(error)) {
        multiNotFoundStreak += 1;
        if (multiNotFoundStreak >= config.publicTopicMultiNotFoundStop) {
          skipMultiRequests = true;
          logger.warn("Disabling multi topic requests after repeated not-found errors", {
            streak: multiNotFoundStreak,
          });
        }
      } else {
        multiNotFoundStreak = 0;
        logger.debug("Multi topic request failed", {
          topicId,
          message: (error as Error).message,
        });
      }
      multiCache.set(topicId, null);
      return null;
    }
  };

  const enrichIfNeeded = async (
    rawMarket: RawMarket,
    parent?: number,
    parentTopicId?: string
  ): Promise<MarketStream | null> => {
    // Multi-market parents may have status != Activated but their children are active
    // Allow them through so we can display parent titles in alerts
    const hasChildren = Array.isArray(rawMarket.childList) && rawMarket.childList.length > 0;
    const passesActivation = isActivated(rawMarket) || hasChildren;

    const initial = passesActivation ? normalize(rawMarket, parent, parentTopicId) : null;
    if (initial || rawMarket.topicId === undefined) {
      return initial;
    }
    const detail = await fetchTopicDetail(String(rawMarket.topicId));
    if (!detail) {
      return null;
    }
    return isActivated(detail, rawMarket.statusEnum)
      ? normalize(detail, parent, parentTopicId)
      : null;
  };

  while (!done) {
    const pages: number[] = [];
    for (let i = 0; i < workers; i += 1) {
      if (totalPages !== null && page > totalPages) {
        break;
      }
      pages.push(page);
      page += 1;
    }

    if (pages.length === 0) {
      break;
    }

    const results = await Promise.all(pages.map((pageNumber) => fetchPage(pageNumber)));
    for (const result of results) {
      if (!result) {
        continue;
      }
      if (result.list.length === 0) {
        done = true;
        continue;
      }

      logger.info("Loaded topic page", { count: result.list.length });
      logger.debug("Sample topic", result.list[0]);
      for (const raw of result.list) {
        const rawMarket = raw as RawMarket;
        const parentStatusEnum = rawMarket.statusEnum;

        // First, check/fetch childList to know if this is a multi-market parent
        let childList = rawMarket.childMarkets ?? rawMarket.childList;
        let multiParent: RawMarket | null = null;
        if ((!childList || childList.length === 0) && rawMarket.topicId) {
          const multiPayload = await fetchMultiTopic(String(rawMarket.topicId));
          if (multiPayload?.childList) {
            childList = multiPayload.childList as unknown[];
            // Use multiPayload as the real parent (may have different topicId on different chain)
            multiParent = multiPayload;
            // Attach to rawMarket so normalize() can detect it's a parent
            rawMarket.childList = childList;
          }
        }

        // Now process the parent (with childList attached if it's a multi-market)
        const market = isActiveByTime(rawMarket, nowMs)
          ? await enrichIfNeeded(rawMarket)
          : null;
        if (market) {
          yield market;
        }

        // If we got a different parent from /mutil endpoint, yield it too
        // This handles case when same topicId is reused on different chains
        const multiParentChain = multiParent?.chainId ? Number(multiParent.chainId) : undefined;
        const rawMarketChain = rawMarket.chainId ? Number(rawMarket.chainId) : undefined;
        if (multiParent && multiParentChain !== rawMarketChain) {
          const multiMarket = await enrichIfNeeded(multiParent);
          if (multiMarket) {
            yield multiMarket;
          }
        }

        // Process children
        if (typeof raw === "object" && raw !== null && Array.isArray(childList)) {
          // Determine the correct parent marketId for children
          const parentMarketId = multiParent?.topicId
            ? Number(multiParent.topicId)
            : (market?.marketId ?? (rawMarket.topicId ? Number(rawMarket.topicId) : undefined));
          const parentTopicId = multiParent?.questionId
            ? String(multiParent.questionId)
            : market?.topicId;

          for (const child of childList) {
            const childMarket = child as RawMarket;
            if (!isActivated(childMarket, parentStatusEnum)) {
              continue;
            }
            if (!isActiveByTime(childMarket, nowMs)) {
              continue;
            }
            const normalizedChild = await enrichIfNeeded(
              childMarket,
              parentMarketId,
              parentTopicId
            );
            if (normalizedChild) {
              yield normalizedChild;
            }
          }
        }
      }

      if (totalPages === null && result.total && Number.isFinite(result.total)) {
        totalPages = Math.max(1, Math.ceil(result.total / limit));
      }

      if (totalPages === null && result.list.length < limit) {
        done = true;
      }
    }
  }
}

export interface LatestPricePayload {
  tokenId: string;
  price: number;
  timestamp: number;
  volume: number;
}

const guardLatestPrice = (payload: unknown): LatestPricePayload | null => {
  if (typeof payload !== "object" || payload === null) {
    logger.warn("Latest price response missing object", payload);
    return null;
  }
  const { tokenId, price, timestamp, volume } = payload as Record<string, unknown>;
  if (typeof tokenId !== "string" || (!price && price !== 0)) {
    logger.warn("Latest price missing tokenId or price", payload);
    return null;
  }
  const numericPrice = typeof price === "string" ? Number(price) : Number(price);
  if (Number.isNaN(numericPrice)) {
    logger.warn("Latest price price is not numeric", payload);
    return null;
  }
  const volumeValue = volume !== undefined ? Number(volume) : 0;
  const numericVolume = Number.isFinite(volumeValue) ? volumeValue : 0;
  return {
    tokenId,
    price: numericPrice,
    timestamp: typeof timestamp === "number" ? timestamp : Date.now(),
    volume: numericVolume,
  };
};

export async function latestPricePrivate(tokenId: string): Promise<LatestPricePayload | null> {
  const response = await http.get<{
    code?: number | string;
    errno?: number | string;
    errmsg?: string;
    result?: unknown;
  }>("/token/latest-price", {
    params: { token_id: tokenId },
  });

  const rawCode = response?.code ?? response?.errno;
  const code = rawCode === undefined ? NaN : Number(rawCode);
  if (Number.isNaN(code) || code !== 0) {
    logger.warn("Latest price returned non-zero code", response);
    return null;
  }

  if (!response.result) {
    logger.warn("Latest price missing result", response);
    return null;
  }

  return guardLatestPrice(response.result);
}

type PublicPricePayload = {
  price: number;
  timestamp: number;
};

type OrderbookPayload = {
  asks?: unknown;
  bids?: unknown;
  last_price?: number | string;
  timestamp?: number | string;
  time?: number | string;
  ts?: number | string;
  result?: unknown;
  data?: unknown;
};

const asNumber = (value: unknown) => {
  const numeric = typeof value === "string" ? Number(value) : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const pickOrderbook = (payload: unknown): OrderbookPayload | null => {
  if (typeof payload !== "object" || payload === null) {
    return null;
  }
  const root = payload as OrderbookPayload;
  if (root.asks || root.bids || root.last_price !== undefined) {
    return root;
  }
  const nested = (root.result ?? root.data) as OrderbookPayload | undefined;
  if (nested && typeof nested === "object") {
    return nested;
  }
  return root;
};

const extractBestPrice = (book: OrderbookPayload | null): number | null => {
  if (!book) {
    return null;
  }
  const lastPrice = asNumber(book.last_price);
  if (lastPrice !== null) {
    return lastPrice;
  }
  const asks = Array.isArray(book.asks) ? book.asks : [];
  const bids = Array.isArray(book.bids) ? book.bids : [];

  const bestAsk = asks.reduce<number | null>((acc, level) => {
    if (!Array.isArray(level) || level.length === 0) {
      return acc;
    }
    const price = asNumber(level[0]);
    if (price === null) {
      return acc;
    }
    return acc === null || price < acc ? price : acc;
  }, null);

  if (bestAsk !== null) {
    return bestAsk;
  }

  const bestBid = bids.reduce<number | null>((acc, level) => {
    if (!Array.isArray(level) || level.length === 0) {
      return acc;
    }
    const price = asNumber(level[0]);
    if (price === null) {
      return acc;
    }
    return acc === null || price > acc ? price : acc;
  }, null);

  return bestBid;
};

const extractTimestamp = (book: OrderbookPayload | null): number | null => {
  if (!book) {
    return null;
  }
  const candidates = [book.timestamp, book.time, book.ts];
  for (const value of candidates) {
    const numeric = asNumber(value);
    if (numeric !== null) {
      return numeric > 10_000_000_000 ? numeric : numeric * 1000;
    }
  }
  return null;
};

export async function latestPricePublic(
  tokenId: string,
  topicId: string,
  chainId: number | undefined
): Promise<PublicPricePayload | null> {
  const base = config.publicApiBase.replace(/\/$/, "");
  const endpoint = config.publicOrderbookEndpoint.startsWith("http")
    ? config.publicOrderbookEndpoint
    : `${base}${config.publicOrderbookEndpoint.startsWith("/") ? "" : "/"}${config.publicOrderbookEndpoint}`;
  const params: Record<string, string> = {
    symbol: tokenId,
    symbol_types: "0",
    question_id: String(topicId),
  };
  if (Number.isFinite(chainId)) {
    params.chainId = String(Number(chainId));
  }

  const response = await http.get<{
    errno?: number | string;
    errmsg?: string;
    result?: unknown;
    data?: unknown;
  }>(endpoint, { params });

  const code = response?.errno === undefined ? NaN : Number(response.errno);
  if (!Number.isNaN(code) && code !== 0) {
    logger.warn("Public orderbook returned non-zero code", response);
    return null;
  }

  const book = pickOrderbook(response?.result ?? response?.data ?? response);
  const price = extractBestPrice(book);
  if (price === null) {
    logger.warn("Public orderbook missing price", response);
    return null;
  }
  const timestamp = extractTimestamp(book) ?? Date.now();
  return { price, timestamp };
}

type KlinePoint = {
  id: number;
  close: number;
  open: number;
  high: number;
  low: number;
  vol: number;
};

const KLINE_PERIODS: Array<{ label: string; seconds: number }> = [
  { label: "15min", seconds: 900 },
];

const chainSlug = (chainId?: number) => {
  if (chainId === 8453) return "base";
  if (chainId === 56) return "bsc";
  if (chainId === 1) return "eth";
  return "bsc";
};

const klineBaseUrl = () => {
  try {
    const origin = new URL(config.apiBase).origin;
    return `${origin}/api`;
  } catch {
    return "https://proxy.opinion.trade:8443/api";
  }
};

export async function fetchKline(
  symbol: string,
  questionId: string,
  chainId: number | undefined,
  startMs: number,
  endMs: number,
  periods: Array<{ label: string; seconds: number }> = KLINE_PERIODS
): Promise<KlinePoint[] | null> {
  const base = klineBaseUrl();
  const slug = chainSlug(chainId);
  const startSeconds = Math.floor(startMs / 1000);
  const endSeconds = Math.floor(endMs / 1000);

  for (const period of periods) {
    const size = Math.min(200, Math.max(10, Math.ceil((endSeconds - startSeconds) / period.seconds)));
    const url = `${base}/${slug}/api/v2/order/kline`;
    try {
      const response = await http.get<{
        errno?: number | string;
        errmsg?: string;
        result?: { data?: unknown[] };
      }>(url, {
        params: {
          symbol,
          period: period.label,
          start_time: startSeconds,
          end_time: endSeconds,
          size,
          chainId,
          question_id: questionId,
          symbol_types: 0,
        },
      });

      const code = response?.errno === undefined ? NaN : Number(response.errno);
      if (!Number.isNaN(code) && code !== 0) {
        logger.warn("Kline returned non-zero code", response);
        continue;
      }

      const data = response?.result?.data;
      if (!Array.isArray(data) || data.length === 0) {
        continue;
      }

      const points = data
        .map((raw) => raw as Record<string, unknown>)
        .map((item) => ({
          id: Number(item.id),
          close: Number(item.close),
          open: Number(item.open),
          high: Number(item.high),
          low: Number(item.low),
          vol: Number(item.vol),
        }))
        .filter((item) => Number.isFinite(item.id) && Number.isFinite(item.close));

      if (points.length >= 2) {
        return points;
      }
    } catch (error) {
      logger.warn("Kline request failed", {
        message: (error as Error).message,
        period: period.label,
        chainId,
        questionId,
      });
    }
  }

  return null;
}
