import Database from "better-sqlite3";
import path from "path";
import fs from "fs";
import { logger } from "./logger";

const dbFolder = path.resolve("data");
if (!fs.existsSync(dbFolder)) {
  fs.mkdirSync(dbFolder, { recursive: true });
}

const dbPath = path.resolve(dbFolder, "opinion.db");
const db = new Database(dbPath);

db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

db.exec(`
CREATE TABLE IF NOT EXISTS streams (
  marketId INTEGER PRIMARY KEY,
  yesTokenId TEXT NOT NULL,
  title TEXT NOT NULL,
  parentMarketId INTEGER,
  topicId TEXT,
  marketType INTEGER,
  chainId INTEGER,
  updatedAt INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ticks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  marketId INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  yesPrice REAL NOT NULL,
  volume REAL NOT NULL,
  deltaVolume REAL NOT NULL,
  FOREIGN KEY(marketId) REFERENCES streams(marketId)
);

CREATE INDEX IF NOT EXISTS idx_ticks_market_ts ON ticks(marketId, ts DESC);

CREATE TABLE IF NOT EXISTS ticks_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  marketId INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  yesPrice REAL NOT NULL,
  volume REAL NOT NULL,
  deltaVolume REAL NOT NULL,
  FOREIGN KEY(marketId) REFERENCES streams(marketId)
);

CREATE INDEX IF NOT EXISTS idx_ticks_raw_market_ts ON ticks_raw(marketId, ts DESC);

CREATE TABLE IF NOT EXISTS alerts (
  marketId INTEGER PRIMARY KEY,
  lastAlertAt INTEGER,
  lastAlertHash TEXT
);

CREATE TABLE IF NOT EXISTS ewma_state (
  marketId INTEGER PRIMARY KEY,
  priceMean REAL NOT NULL,
  priceVar REAL NOT NULL,
  volumeMean REAL NOT NULL,
  volumeVar REAL NOT NULL,
  lastPrice REAL NOT NULL,
  tickCount INTEGER NOT NULL DEFAULT 0
);

`);

const streamColumns = db
  .prepare("PRAGMA table_info(streams)")
  .all() as { name: string }[];
const streamColumnNames = streamColumns.map((column) => column.name);
if (!streamColumnNames.includes("topicId")) {
  db.exec("ALTER TABLE streams ADD COLUMN topicId TEXT");
}
if (!streamColumnNames.includes("marketType")) {
  db.exec("ALTER TABLE streams ADD COLUMN marketType INTEGER");
}
if (!streamColumnNames.includes("chainId")) {
  db.exec("ALTER TABLE streams ADD COLUMN chainId INTEGER");
}
if (!streamColumnNames.includes("cutoffAt")) {
  db.exec("ALTER TABLE streams ADD COLUMN cutoffAt INTEGER");
}

export interface StreamRecord {
  marketId: number;
  yesTokenId: string;
  title: string;
  parentMarketId?: number;
  topicId: string;
  marketType?: number;
  chainId?: number;
  cutoffAt?: number;
  updatedAt: number;
}

export interface TickRecord {
  marketId: number;
  ts: number;
  yesPrice: number;
  volume: number;
  deltaVolume: number;
}

export interface AlertState {
  marketId: number;
  lastAlertAt: number | null;
  lastAlertHash: string | null;
}

export interface EwmaState {
  marketId: number;
  priceMean: number;
  priceVar: number;
  volumeMean: number;
  volumeVar: number;
  lastPrice: number;
  tickCount: number;
}


const insertStream = db.prepare(`
INSERT INTO streams (marketId, yesTokenId, title, parentMarketId, topicId, marketType, chainId, cutoffAt, updatedAt)
VALUES (@marketId, @yesTokenId, @title, @parentMarketId, @topicId, @marketType, @chainId, @cutoffAt, @updatedAt)
ON CONFLICT(marketId) DO UPDATE SET
  yesTokenId=excluded.yesTokenId,
  title=excluded.title,
  parentMarketId=excluded.parentMarketId,
  topicId=excluded.topicId,
  marketType=excluded.marketType,
  chainId=excluded.chainId,
  cutoffAt=excluded.cutoffAt,
  updatedAt=excluded.updatedAt
`);

const insertTick = db.prepare(`
INSERT INTO ticks (marketId, ts, yesPrice, volume, deltaVolume)
VALUES (@marketId, @ts, @yesPrice, @volume, @deltaVolume)
`);

const insertRawTick = db.prepare(`
INSERT INTO ticks_raw (marketId, ts, yesPrice, volume, deltaVolume)
VALUES (@marketId, @ts, @yesPrice, @volume, @deltaVolume)
`);

const TICKS_LIMIT = 120;
const RAW_TICKS_LIMIT = 400;

const pruneTicksStmt = db.prepare(`
DELETE FROM ticks
WHERE marketId = @marketId
  AND id NOT IN (
    SELECT id
    FROM ticks
    WHERE marketId = @marketId
    ORDER BY ts DESC
    LIMIT ${TICKS_LIMIT}
  )
`);

const pruneRawTicksStmt = db.prepare(`
DELETE FROM ticks_raw
WHERE marketId = @marketId
  AND id NOT IN (
    SELECT id
    FROM ticks_raw
    WHERE marketId = @marketId
    ORDER BY ts DESC
    LIMIT ${RAW_TICKS_LIMIT}
  )
`);

const insertTickWithPrune = db.transaction((tick: TickRecord) => {
  insertTick.run(tick);
  pruneTicksStmt.run({ marketId: tick.marketId });
});

const insertRawTickWithPrune = db.transaction((tick: TickRecord) => {
  insertRawTick.run(tick);
  pruneRawTicksStmt.run({ marketId: tick.marketId });
});

const insertRawAndTickWithPrune = db.transaction((tick: TickRecord) => {
  insertRawTick.run(tick);
  insertTick.run(tick);
  pruneRawTicksStmt.run({ marketId: tick.marketId });
  pruneTicksStmt.run({ marketId: tick.marketId });
});

const getRecentTicksStmt = db.prepare<[number, number], TickRecord>(`
SELECT marketId, ts, yesPrice, volume, deltaVolume
FROM ticks
WHERE marketId = ?
ORDER BY ts DESC
LIMIT ?
`);

const getRecentRawTicksStmt = db.prepare<[number, number], TickRecord>(`
SELECT marketId, ts, yesPrice, volume, deltaVolume
FROM ticks_raw
WHERE marketId = ?
ORDER BY ts DESC
LIMIT ?
`);

type StreamRow = {
  marketId: number;
  yesTokenId: string;
  title: string;
  parentMarketId: number | null;
  topicId: string | null;
  marketType: number | null;
  chainId: number | null;
  cutoffAt: number | null;
  updatedAt: number;
};

const getStreamByIdStmt = db.prepare<[number], StreamRow>(`
SELECT marketId, yesTokenId, title, parentMarketId, topicId, marketType, chainId, cutoffAt, updatedAt
FROM streams
WHERE marketId = ?
LIMIT 1
`);

const getStreamsStmt = db.prepare<[], StreamRow>(`
SELECT marketId, yesTokenId, title, parentMarketId, topicId, marketType, chainId, cutoffAt, updatedAt
FROM streams
ORDER BY updatedAt DESC
`);

type AlertRow = {
  marketId: number;
  lastAlertAt: number | null;
  lastAlertHash: string | null;
};

const getAlertStmt = db.prepare<[number], AlertRow>(`
SELECT marketId, lastAlertAt, lastAlertHash
FROM alerts
WHERE marketId = ?
`);

const upsertAlertStmt = db.prepare(`
INSERT INTO alerts (marketId, lastAlertAt, lastAlertHash)
VALUES (@marketId, @lastAlertAt, @lastAlertHash)
ON CONFLICT(marketId) DO UPDATE SET
  lastAlertAt=excluded.lastAlertAt,
  lastAlertHash=excluded.lastAlertHash
`);

type EwmaRow = {
  marketId: number;
  priceMean: number;
  priceVar: number;
  volumeMean: number;
  volumeVar: number;
  lastPrice: number;
  tickCount: number;
};

const getEwmaStmt = db.prepare<[number], EwmaRow>(`
SELECT marketId, priceMean, priceVar, volumeMean, volumeVar, lastPrice, tickCount
FROM ewma_state
WHERE marketId = ?
`);

const upsertEwmaStmt = db.prepare(`
INSERT INTO ewma_state (marketId, priceMean, priceVar, volumeMean, volumeVar, lastPrice, tickCount)
VALUES (@marketId, @priceMean, @priceVar, @volumeMean, @volumeVar, @lastPrice, @tickCount)
ON CONFLICT(marketId) DO UPDATE SET
  priceMean=excluded.priceMean,
  priceVar=excluded.priceVar,
  volumeMean=excluded.volumeMean,
  volumeVar=excluded.volumeVar,
  lastPrice=excluded.lastPrice,
  tickCount=excluded.tickCount
`);


export const storage = {
  saveStream(record: StreamRecord) {
    insertStream.run(record);
  },
  listStreams(): StreamRecord[] {
    return getStreamsStmt.all().map((row) => ({
      marketId: row.marketId,
      yesTokenId: row.yesTokenId,
      title: row.title,
      parentMarketId: row.parentMarketId ?? undefined,
      topicId: row.topicId ?? "",
      marketType: row.marketType ?? undefined,
      chainId: row.chainId ?? undefined,
      cutoffAt: row.cutoffAt ?? undefined,
      updatedAt: row.updatedAt,
    }));
  },
  getStreamById(marketId: number): StreamRecord | null {
    const row = getStreamByIdStmt.get(marketId);
    if (!row) {
      return null;
    }
    return {
      marketId: row.marketId,
      yesTokenId: row.yesTokenId,
      title: row.title,
      parentMarketId: row.parentMarketId ?? undefined,
      topicId: row.topicId ?? "",
      marketType: row.marketType ?? undefined,
      chainId: row.chainId ?? undefined,
      cutoffAt: row.cutoffAt ?? undefined,
      updatedAt: row.updatedAt,
    };
  },
  pushTick(tick: TickRecord) {
    insertTickWithPrune(tick);
  },
  pushRawTick(tick: TickRecord) {
    insertRawTickWithPrune(tick);
  },
  pushRawAndTick(tick: TickRecord) {
    insertRawAndTickWithPrune(tick);
  },
  recentTicks(marketId: number, limit = TICKS_LIMIT): TickRecord[] {
    return getRecentTicksStmt.all(marketId, limit).map((row) => ({
      marketId: row.marketId,
      ts: row.ts,
      yesPrice: row.yesPrice,
      volume: row.volume,
      deltaVolume: row.deltaVolume,
    }));
  },
  recentRawTicks(marketId: number, limit = RAW_TICKS_LIMIT): TickRecord[] {
    return getRecentRawTicksStmt.all(marketId, limit).map((row) => ({
      marketId: row.marketId,
      ts: row.ts,
      yesPrice: row.yesPrice,
      volume: row.volume,
      deltaVolume: row.deltaVolume,
    }));
  },
  lastAlert(marketId: number): AlertState {
    const record = getAlertStmt.get(marketId);
    if (!record) {
      return { marketId, lastAlertAt: null, lastAlertHash: null };
    }
    return {
      marketId: record.marketId,
      lastAlertAt: record.lastAlertAt,
      lastAlertHash: record.lastAlertHash,
    };
  },
  updateAlert(state: AlertState) {
    upsertAlertStmt.run(state);
  },
  getEwmaState(marketId: number): EwmaState | null {
    const record = getEwmaStmt.get(marketId);
    return record ?? null;
  },
  updateEwmaState(state: EwmaState) {
    upsertEwmaStmt.run(state);
  },
};
