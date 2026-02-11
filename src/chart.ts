import sharp from "sharp";
import { TickRecord } from "./storage";

type ChartResult = {
  buffer: Buffer;
  deltaAbs: number;
  deltaPct: number;
  startPrice: number;
  endPrice: number;
  percentMode: boolean;
};

const toMs = (ts: number) => (ts < 1_000_000_000_000 ? ts * 1000 : ts);

const formatPrice = (value: number, percentMode: boolean) => {
  if (percentMode) {
    return `${(value * 100).toFixed(1)}%`;
  }
  if (value >= 1) return value.toFixed(3);
  if (value >= 0.1) return value.toFixed(4);
  return value.toFixed(5);
};

const formatTime = (ts: number) => {
  const date = new Date(ts);
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  return `${hours}:${minutes}`;
};

const clamp = (value: number, min: number, max: number) =>
  Math.min(max, Math.max(min, value));

const percentile = (values: number[], p: number) => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * p)));
  return sorted[idx];
};

export const buildMarketChart = async (ticks: TickRecord[]): Promise<ChartResult | null> => {
  if (ticks.length < 2) {
    return null;
  }

  const width = 650;
  const height = 500;
  const marginLeft = 70;
  const marginRight = 24;
  const marginTop = 34;
  const marginBottom = 48;
  const chartWidth = width - marginLeft - marginRight;
  const chartHeight = height - marginTop - marginBottom;

  const sorted = [...ticks]
    .map((tick) => ({ ...tick, ts: toMs(tick.ts) }))
    .sort((a, b) => a.ts - b.ts);

  const startPrice = sorted[0].yesPrice;
  const endPrice = sorted[sorted.length - 1].yesPrice;
  const deltaAbs = endPrice - startPrice;
  const deltaPct = startPrice > 0 ? (deltaAbs / startPrice) * 100 : 0;

  const prices = sorted.map((tick) => tick.yesPrice);
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const percentMode = minPrice >= 0 && maxPrice <= 1.05;
  const p05 = percentile(prices, 0.05);
  const p95 = percentile(prices, 0.95);

  let scaleMin = minPrice;
  let scaleMax = maxPrice;
  if (!percentMode) {
    if (p95 - p05 > 0 && maxPrice - minPrice > (p95 - p05) * 1.8) {
      scaleMin = p05;
      scaleMax = p95;
    }

    const range = scaleMax - scaleMin;
    const minRange = Math.max(0.002, startPrice * 0.005);
    if (range < minRange) {
      const mid = (scaleMax + scaleMin) / 2;
      scaleMin = mid - minRange / 2;
      scaleMax = mid + minRange / 2;
    } else {
      const pad = range * 0.08;
      scaleMin -= pad;
      scaleMax += pad;
    }
  } else {
    scaleMin = 0;
    scaleMax = 1;
  }

  const tMin = sorted[0].ts;
  const tMax = sorted[sorted.length - 1].ts;
  const tSpan = Math.max(1, tMax - tMin);

  const xFor = (ts: number) =>
    marginLeft + ((ts - tMin) / tSpan) * chartWidth;
  const yFor = (price: number) => {
    const value = clamp(price, scaleMin, scaleMax);
    const ratio = (value - scaleMin) / (scaleMax - scaleMin || 1);
    return marginTop + chartHeight - ratio * chartHeight;
  };

  const points = sorted.map((tick) => {
    const x = xFor(tick.ts);
    const y = yFor(tick.yesPrice);
    return { x, y, price: tick.yesPrice };
  });

  const path = points
    .map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(1)},${point.y.toFixed(1)}`)
    .join(" ");

  // Determine extremum index based on price direction
  const isUp = endPrice > startPrice;
  const extremumIndex = isUp
    ? prices.indexOf(maxPrice)
    : prices.indexOf(minPrice);

  const gridY = [0.25, 0.5, 0.75];

  // Time marks: start, every 15 min, end (NOW)
  // Skip marks too close to start or NOW to avoid overlap
  const interval15min = 15 * 60 * 1000;
  const minGap = interval15min * 0.4; // Minimum gap to avoid overlap
  const timeMarks: { ts: number; label: string }[] = [];
  timeMarks.push({ ts: tMin, label: formatTime(tMin) });
  
  let t = Math.ceil(tMin / interval15min) * interval15min;
  while (t < tMax) {
    const distFromStart = t - tMin;
    const distToEnd = tMax - t;
    if (distFromStart > minGap && distToEnd > minGap) {
      timeMarks.push({ ts: t, label: formatTime(t) });
    }
    t += interval15min;
  }
  timeMarks.push({ ts: tMax, label: "NOW" });

  const priceMarks = percentMode ? [0, 0.5, 1] : [scaleMin, (scaleMin + scaleMax) / 2, scaleMax];

  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
      <rect width="100%" height="100%" fill="#161413"/>

      <text x="${width / 2}" y="22" fill="#C9C2B8" font-family="Arial, sans-serif" font-size="18" font-weight="bold" text-anchor="middle">Beta / 2 last hours</text>

      ${gridY
        .map((ratio) => {
          const y = marginTop + chartHeight * ratio;
          return `<line x1="${marginLeft}" y1="${y}" x2="${marginLeft + chartWidth}" y2="${y}" stroke="#2A2A2A" stroke-width="1"/>`;
        })
        .join("")}

      <line x1="${marginLeft}" y1="${marginTop}" x2="${marginLeft}" y2="${marginTop + chartHeight}" stroke="#333" stroke-width="1"/>
      <line x1="${marginLeft}" y1="${marginTop + chartHeight}" x2="${marginLeft + chartWidth}" y2="${marginTop + chartHeight}" stroke="#333" stroke-width="1"/>

      ${priceMarks
        .map((value, index) => {
          const y = yFor(value);
          const label = formatPrice(value, percentMode);
          const dy = index === 0 ? 12 : index === 2 ? -4 : 4;
          return `<text x="${marginLeft - 8}" y="${y + dy}" fill="#C9C2B8" font-family="Arial, sans-serif" font-size="14" text-anchor="end">${label}</text>`;
        })
        .join("")}

      ${timeMarks
        .map((tm) => {
          const x = xFor(tm.ts);
          return `<text x="${x}" y="${marginTop + chartHeight + 28}" fill="#C9C2B8" font-family="Arial, sans-serif" font-size="16" text-anchor="middle">${tm.label}</text>`;
        })
        .join("")}

      <path d="${path}" fill="none" stroke="#ED6432" stroke-width="2"/>
      ${points
        .map((point, index) => {
          const isExtremum = index === extremumIndex;
          const isLabeled = index % 3 === 0 || isExtremum;
          const radius = isExtremum ? 5 : isLabeled ? 4 : 2;
          const color = isExtremum ? "#FFD166" : "#ED6432";
          const circle = `<circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="${radius}" fill="${color}"/>`;
          const labelColor = isExtremum ? "#FFD166" : "#ED6432";
          const label = isLabeled
            ? `<text x="${point.x.toFixed(1)}" y="${(point.y - 14).toFixed(1)}" fill="${labelColor}" font-family="Arial, sans-serif" font-size="12" text-anchor="middle">${formatPrice(point.price, percentMode)}</text>`
            : "";
          return circle + label;
        })
        .join("")}

      <circle cx="${points[points.length - 1].x.toFixed(1)}" cy="${points[points.length - 1].y.toFixed(1)}" r="5" fill="#FFD166"/>
    </svg>
  `;

  const buffer = await sharp(Buffer.from(svg)).png().toBuffer();
  return {
    buffer,
    deltaAbs,
    deltaPct,
    startPrice,
    endPrice,
    percentMode,
  };
};
