# Opinion.trade Spike Alert

[Telegram bot](https://t.me/opspikes) that watches [Opinion.trade](https://app.opinion.trade) markets, detects price spikes using EWMA Z-score analysis, and sends alerts with charts to Telegram.
Creator: [@kartashovio](https://x.com/kartashovio)

---

## What it does

- Polls prices & volumes every minute across all active markets.
- Detects anomalies via EWMA Z-score with volume boost.
- Adaptive filtering: stricter thresholds for mid-range prices, lenient for extremes.
- Sends Telegram alerts with a 2-hour price chart.

---

## Quick start

```bash
npm install
cp .env.example .env   # fill in your keys
npm run build && npm start
```

Dev mode: `npm run dev`

---

## Config (.env)

See `.env.example` for all options. Key ones:

| Variable | Description |
|---|---|
| `OPINION_API_KEY` | Opinion API key |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Chat ID for alerts |
| `OPINION_USE_ADAPTIVE_THRESHOLDS` | Enable adaptive filtering (default `1`) |

---

Need help or have ideas? Create an issue or ping [@kartashovio](https://x.com/kartashovio).
