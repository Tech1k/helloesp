# HelloESP

[![HelloESP status](https://helloesp.com/status.svg)](https://helloesp.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit/)
[![Build](https://github.com/Tech1k/helloesp/actions/workflows/build.yml/badge.svg)](https://github.com/Tech1k/helloesp/actions/workflows/build.yml)
[![CodeQL](https://github.com/Tech1k/helloesp/actions/workflows/codeql.yml/badge.svg)](https://github.com/Tech1k/helloesp/actions/workflows/codeql.yml)
[![Lighthouse](https://github.com/Tech1k/helloesp/actions/workflows/lighthouse.yml/badge.svg)](https://github.com/Tech1k/helloesp/actions/workflows/lighthouse.yml)
[![PlatformIO](https://img.shields.io/badge/PlatformIO-Arduino-orange)](https://platformio.org/)
[![GitHub stars](https://img.shields.io/github/stars/Tech1k/helloesp?style=social)](https://github.com/Tech1k/helloesp/stargazers)

A public website running entirely on a single ESP32 with 520 KB of RAM. Every page, every sensor reading, every guestbook entry is served by the microcontroller itself. There is no backend server.

**Live at [helloesp.com](https://helloesp.com)**

![HelloESP in its display frame](.github/assets/hero.jpg?v=2)

---

## Why?

Because it's fun to see how far a $10 microcontroller can go. The last version ran until 2023 on an ESP32 behind a Cloudflare tunnel. It eventually went offline and the domain lapsed.

Years later I came back to it. The internet had gotten heavier in the meantime: more of everything, most of it wasteful. This whole website weighs less than a single phone wallpaper. I wanted to see what a single small chip could still do against all that. The domain was available again, which felt like permission.

This time it stays up. If it goes down, I fix it. That's the point now.

## How it works

The ESP32 holds a persistent outbound WebSocket to a Cloudflare Worker. When a browser hits `helloesp.com`, the Worker relays the request over that socket and streams the response back. The ESP never accepts an inbound TCP connection from the internet, only from the LAN.

```
 Browser ──HTTPS──▶ Cloudflare Worker ──WSS──▶ ESP32 (on your home network)
                         ▲                        │
                         └────── response ────────┘
```

Responses larger than a single WebSocket frame are chunked and base64-encoded. Admin endpoints return 404 through the relay and are only reachable on the LAN. A second shared secret (HMAC) can be enabled so a leaked Worker secret alone cannot impersonate the device.

The same WebSocket also carries **live push events** the other direction: every 15 seconds the device pushes sensor stats, and every tracked public request triggers a console event. The Worker fans these out to connected browsers via Server-Sent Events (`/_stream`), so the homepage ticks in real time without polling.

Without the Worker, the site still runs on LAN via mDNS at `http://helloesp.local`.

## Hardware

| Part | Qty | ~Price | Source | Role |
|---|---|---|---|---|
| Inland ESP32-WROOM-32D | 1 | $10 | Microcenter | MCU, 520 KB RAM, dual-core Xtensa. Different silkscreen layout from the DOIT V1, but builds under `esp32doit-devkit-v1` since the firmware addresses pins by GPIO number |
| BME280 breakout | 1 | $7 | Amazon | Temperature, humidity, barometric pressure |
| CCS811 breakout | 1 | $14 | Amazon | eCO₂, VOC |
| Adafruit DS3231 + CR1220 cell | 1 | $14 | Adafruit | Battery-backed real-time clock, accurate timekeeping across reboots |
| SSD1306 128×64 OLED | 1 | $3 | Amazon | Rotating info pages with burn-in shift |
| Adafruit MicroSD breakout | 1 | $8 | Adafruit | SPI SD card adapter |
| Good quality Micro SD card (FAT32, ≤32 GB) | 1 | $15 | anywhere | Filesystem: HTML, images, logs, config |
| 5mm LEDs + 220Ω resistors | 2 each | <$1 | Microcenter | Status LED (visit blink) + notification LED (pending guestbook) |
| Full-size premium breadboard | 1 | $5 | Adafruit | Build substrate; see "build gotchas" below |
| Solid-core hookup wire | 1 | $17 | Adafruit | Cut-to-length connections between modules |

**Total parts: ~$95** as actually built (mix of Amazon, Microcenter, Adafruit). Going all-clones with no Adafruit breakouts gets you closer to $35. Clone-shop the DOIT V1 itself with the most care, since some clones ship undersized USB-UART chips or unstable AMS1117 LDOs.

**Power:** runs from any 1 A USB-A phone charger. Total draw averages well under 1 W, with brief spikes during WiFi association and SD writes.

**My setup:** USB-C brick → USB-C-to-micro-USB cable → smart plug → UPS. The smart plug gives remote power-cycling for when something wedges, and (with a Shelly Gen 2+ plug) feeds live wattage to the homepage. The UPS makes grid blips invisible. Both are optional, but both are part of why "this stays up" is a real claim and not an aspiration.

### Wiring

| Signal | ESP32 Pin |
|---|---|
| I²C SDA (BME280, CCS811, DS3231, OLED) | GPIO 21 |
| I²C SCL (BME280, CCS811, DS3231, OLED) | GPIO 22 |
| SD CS | GPIO 5 |
| SD MOSI | GPIO 23 |
| SD MISO | GPIO 19 |
| SD SCK | GPIO 18 |
| Status LED | GPIO 33 |
| Notification LED | GPIO 32 |
| CCS811 WAKE | GND |

I²C addresses: BME280 `0x76`, CCS811 `0x5A`, DS3231 `0x68`, SSD1306 `0x3C`.

DS3231 is optional. If absent, the firmware falls back to NTP-only timekeeping with a brief 1970-until-NTP window after each reboot; with it, the system clock is pre-seeded from the RTC at boot and writes back on every NTP sync.

### Breadboard build gotchas

A few things that aren't obvious until you ship one of these and watch it fail:

- **Don't oversize the AMS1117 output caps.** The on-board LDO on the DOIT DevKit V1 is stable across roughly 1µF to 100µF on the output. Adding a bulky 470µF "for safety" pushes it into oscillation at boot. The symptom looks like WiFi flake (brownout reboots, repeating connect attempts). Stick with what the dev board ships, or add a small ceramic if you must.
- **Desolder R1 and R2 on the DS3231 module.** Most generic DS3231 boards ship with their own 4.7kΩ I²C pull-ups installed. Leaving them in place stacks four sets of pull-ups on one bus (BME280, CCS811, OLED, DS3231), dropping the effective resistance to ~1.95kΩ. SDA/SCL high levels sag and CCS811 reads start corrupting. Lift R1 and R2 with a soldering iron; the bus settles around 2.4kΩ, which is what the rest of the parts expect.
- **SPI and I²C don't like sharing parallel jumpers.** The SD card's SPI bus runs at ~25 MHz on GPIO 18/19/23. If those wires sit flush against the I²C lines on a breadboard, capacitive coupling shows up as CCS811 readings that look like the sensor is dying. Route the SD lines on a different row, or lift them ~1mm above the I²C wires for vertical air separation.
- **Use solid-core wire, and don't put any jumper under tension.** Stranded jumpers work fine on the bench. Once the device is framed and mounted, any flex on a stranded contact intermittently fails and the bus wedges. Solid-core, cut to length, with no pull on either end.
- **Tie 3.3V and GND on both rails, at both ends.** Most full-size breadboards split each power rail in the middle, and even the ones that look continuous can have a single weak contact mid-board. Tying the rails at one end works on the bench, but on the wall a single point of contact is one whisker of flex away from a brown-out. Two jumpers across, one at each end, costs nothing.

These are all lessons from this build. A PCB respin would fix most of them at the layout level, but with the gotchas above applied the breadboard build is solid; whether to ever PCB this is an open question.

## Features

**Frontend**
- Live dashboard: 12 sensor/system metrics, trend arrows, degraded-sensor indicators
- Real-time SSE updates (push every 15s from the device), with graceful fallback to 30s polling
- Live connection indicator (pulsing green dot) and a "Right now" request ticker on the homepage
- Historical CSV charts with day/week/month switcher; weekly/monthly/yearly aggregate archives
- Hall of Fame (lifetime extremes: peak CO₂, temp range, busiest day, longest uptime) and year-over-year monthly visitor deltas on `/history`
- Visitor country map, request-rate chart, changelog, photo carousel
- `/console` live feed: last 50 public requests with country flags, updated instantly via SSE
- Outdoor weather + air quality context (via Cloudflare Worker proxy to Open-Meteo): conditions with day/night-aware icons, feels-like, dewpoint, wind direction, pressure trend, AQI + PM2.5, atmospheric CO₂, UV index
- Guestbook with two-level reply threading, tombstone deletes, moderation queue, inline AI translation, rate limiting
- Snake at /snake (also embedded on the 404 / offline / timeout pages) with two leaderboards (today's top scores + all-time top ten, top scores cross over automatically), replay viewer for top entries, quarterly Hall of Fame archive of past all-time boards, 3-letter initials with content blocklist, server-side replay verification, idle demo mode
- `/chronicle`: a daily auto-generated diary the chip writes about itself, plus weekly, monthly, and quarterly reflection entries that aggregate prior periods. Around 30 observation detectors fold sensor extremes, visitor counts, weather, calendar events (equinoxes / first-of-month / season transitions), sensor-retire events, owner-defined personal anchors (birthdays, framing day), and self-aware-chip moments (curator absence, tinkering noticed, long-uptime milestones) into one of several narrative templates per cadence. Daily entries seal at chip-local midnight; weekly on Mondays; monthly on the 1st; quarterly at quarter starts. Permalinks for every entry, RSS feed, owner-curated notes that thread to X as replies, public today-in-progress preview card on the index, year/month accordion archive with template-id filter chips (milestones / records / anomalies / busy / quiet / reflections), keyboard navigation (←/→) on detail views, and per-entry share button.
- Dark mode, responsive, SRI-pinned CDN scripts, no tracking

**Firmware**
- Full async HTTP server over WebSocket-relayed traffic
- Non-blocking WebSocket client with linear backoff (500 ms first retry, then 5 s × fails up to 30 s cap), WiFi reassociation on consecutive fails, and a 60 s wall-clock safety-net reboot if recovery stalls
- Atomic writes for all critical files (`tmp → bak → rename`)
- Period aggregation: weekly, monthly, yearly checkpoints
- CSV sensor logging every 5 minutes
- NTP with three-server failover and bounded boot retry; DS3231 pre-seeds the system clock at boot so timestamps are correct before NTP arrives
- WiFi runtime watchdog: reboots if disconnected >10 min
- Heap safety net: reboots at <30 KB free rather than hang
- Event-driven SSE push for sensor stats (15s) and console entries (on each request)
- Admin panel (LAN-only): OTA updates, file manager (with overwrite-confirm + gzip-pair tooltips), full SD backup/restore, R2 backup health + liveness test, SMTP2GO test email, self-test (with I²C bus scanner), sensor + error log viewers, device health sparklines (heap + RSSI), Worker link status, data management (reset counters / export state / clear Snake leaderboard / repair monthly+yearly stats from weekly archives / drop out-of-spec CSV rows / fix truncated record timestamps), Durable Object storage explorer (read + delete-by-key for incident recovery, gated on the worker secret), maintenance mode toggle
- OLED boot sequence, rotating runtime pages, burn-in protection

**Edge**
- Cloudflare Worker (Durable Object) with per-IP rate limiting, 8 KB POST cap
- SSE fanout hub: multiple browser viewers, constant ESP load
- Maintenance mode with auto-expiring window and dedicated 503 page
- Auto-retry + pulsing indicator on all error pages (offline / timeout / maintenance)
- Cloudflare edge caching honors the device's `Cache-Control: max-age` headers so repeat visitors hit the edge instead of the chip
- Optional `worker_exclusive` mode: LAN visitors to public pages (homepage, guestbook, etc.) get redirected to the public site so they go through the Worker's edge cache rather than hitting the chip directly. `/admin` stays direct on LAN. Eliminates LAN-burst-vs-WS-write collisions
- Hourly outdoor-weather refresh cached in the Durable Object
- Inline guestbook translation via Workers AI: `@cf/meta/m2m100-1.2b` for primary translation, `@cf/meta/llama-3.2-1b-instruct` for source-language detection on diacritic-less Latin text and as a final fallback when m2m100 produces nothing usable. Per-(message-id, target-lang, text-hash) cached in DO storage so each unique pair costs at most one neuron set
- Snake leaderboard storage (Durable Object) with replay-verify anti-cheat, ROT13'd 3-letter initials blocklist, per-IP rate limit on score submissions
- Embeddable live status badges at `/status.svg` (and `/status-wide.svg`)
- Optional HMAC challenge-response device auth
- Optional SMTP2GO integration for guestbook-pending alerts, dead-man's-switch (device silent >N hours), backup failures, and overdue-backup warnings
- Optional daily off-site backups to Cloudflare R2 with GFS rotation (7 daily + 4 weekly + 12 monthly + yearly) and sha256 integrity manifests; Worker also writes a daily full Durable Object snapshot to `state/do-snapshot/YYYY-MM-DD.json` (30-day retention) so non-chronicle DO state (snake leaderboards + season archives + replays, caches) is recoverable independently if DO is ever wiped
- Optional Shelly Gen 2+ smart plug integration for live power monitoring: live-wattage banner on homepage, power chart, energy column in CSV logs, per-period energy on `/history` cards, lifetime cost + CO₂ totals on homepage and `/about` (when grid rates are configured), admin observability panel + self-test entry
- Optional X / Twitter auto-post: each sealed Chronicle entry crossposts once via the X API v2 (OAuth 1.0a HMAC-SHA1 signed in-Worker). Owner notes added later thread under as replies. Idempotent via `posted_to_x` and `note_x_tweet_id` flags so DO restarts and re-seals don't double-post. Silently skipped if any of the 4 secrets are unset
- Security headers, no-cache list for dynamic endpoints
- RSS feeds (`/changelog.rss`, `/guestbook.rss`, `/chronicle.rss`), `sitemap.xml`, `robots.txt`, `.well-known/security.txt`

### Embeddable status badges

Live badges you can drop into your own README, blog, or status page. All variants pull from the Worker's cache, so embedding them puts zero load on the ESP no matter how many pages link them. Edge-cached 60 seconds. Keep working (showing "offline") even when the device is unreachable.

**Compact badges** (shields.io-compatible layout, 20px tall):

| Variant | Live preview | URL |
|---|---|---|
| Uptime *(default)* | ![](https://helloesp.com/status.svg) | `https://helloesp.com/status.svg` |
| Visit count | ![](https://helloesp.com/status.svg?metric=visits) | `https://helloesp.com/status.svg?metric=visits` |
| Current temperature | ![](https://helloesp.com/status.svg?metric=temp) | `https://helloesp.com/status.svg?metric=temp` |
| Live power draw | ![](https://helloesp.com/status.svg?metric=power) | `https://helloesp.com/status.svg?metric=power` |
| Online indicator | ![](https://helloesp.com/status.svg?metric=online) | `https://helloesp.com/status.svg?metric=online` |

**Wide stat card** (340×78, multi-line mini-dashboard):

![](https://helloesp.com/status-wide.svg)

`https://helloesp.com/status-wide.svg`

**Embedding**

Markdown:
```markdown
[![HelloESP status](https://helloesp.com/status.svg)](https://helloesp.com)
```

HTML:
```html
<a href="https://helloesp.com">
  <img src="https://helloesp.com/status.svg" alt="HelloESP status">
</a>
```

**State colors** (applies to both compact and wide):

| Color | Meaning |
|---|---|
| Blue | Live: device online, heard from within 45s |
| Gray | Offline or stale: no stats received in >2 minutes, or WS socket is gone |
| Orange | Maintenance: owner-declared downtime window |

**Accessibility**: every badge includes `<title>` text and `role="img"`, so screen readers announce the state rather than saying "image." The `aria-label` reflects the current value ("HelloESP: up 5d 3h" etc.).

**Cache policy**: `Cache-Control: public, max-age=60`. Most embedders don't need sub-minute freshness; plan accordingly if you do.

## Setup

### 1. Firmware

```bash
git clone https://github.com/Tech1k/helloesp.git
cd helloesp
pio run -t upload
```

Requires [PlatformIO](https://platformio.org/).

**Upgrading from before firmware 1.4:** the boot-time CSV migrations were stripped in 1.4 to free flash. If your SD card has guestbook data from before April 23, 2026, flash firmware 1.3 first to run the v1→v3 migrations, then upgrade. The chip checks at boot and halts with an OLED message if it detects stale data, so you'll know if you skipped this step.

### 2. SD card

Format a FAT32 SD card (≤32 GB) and copy the contents of `data/` to the root. The `data/` folder is **not** flashed. It lives on the SD card.

### 3. Config

Rename `config.example.txt` to `config.txt` on the SD card and fill in:

```
wifi_ssid=YOUR_SSID
wifi_pass=YOUR_PASSWORD
admin_user=admin
admin_pass=your-admin-password
timezone=MST7MDT,M3.2.0,M11.1.0
```

Timezone is a POSIX TZ string. Common US examples are listed in the file. Leave `worker_url`, `worker_key`, and `device_key` blank to run LAN-only.

Optional `worker_exclusive=true` redirects LAN public-page hits (`/`, `/guestbook`, etc.) to the Worker so they go through CF's edge cache. `/admin` always serves direct on LAN. Default off; flip to `true` only if your Worker is set up and you want LAN visitors to share the same cache layer as public visitors.

### 4. Cloudflare Worker (optional, for public access)

```bash
# Generate a shared secret; don't pick one by hand
openssl rand -hex 32
```

The same value goes in two places:
- `worker_key` in the SD `config.txt`
- Worker secret `WORKER_SECRET`

Then deploy:

```bash
cd worker
wrangler deploy
wrangler secret put WORKER_SECRET   # paste the value from openssl
```

Add `worker_url` to `config.txt` (e.g. `helloesp.com`). The ESP will connect on next boot.

### 5. HMAC device auth (optional)

Adds a second secret so a leaked `WORKER_SECRET` alone cannot impersonate the device. On every reconnect, the Worker sends a random nonce and the ESP signs it with the device key.

```bash
openssl rand -hex 32
```

Set the same value as `device_key` in `config.txt` and:

```bash
wrangler secret put HMAC_SECRET
```

If either side is unset, HMAC is disabled and auth falls back to `WORKER_SECRET` only.

### 6. Email alerts via SMTP2GO (optional)

Configuring [SMTP2GO](https://smtp2go.com/) once unlocks every alert channel the Worker uses:

- Guestbook moderation notifications (throttled 1/5min)
- Dead-man's-switch: device silent for longer than `DEADMAN_HOURS` (default 6) + recovery email when it comes back
- Backup failures (throttled 1/hr) and overdue-backup warnings (>48 h since last success, 1/day)
- Manual test email from the admin panel

```bash
wrangler secret put SMTP2GO_KEY
wrangler secret put NOTIFY_EMAIL
wrangler secret put NOTIFY_FROM    # optional, e.g. "HelloESP <no-reply@yourdomain>"
wrangler secret put DEADMAN_HOURS  # optional, default 6, accepts fractional hours
```

The ESP never blocks on outbound HTTPS; all email sending lives on the Worker. If any required secret is unset, the corresponding alerts silently no-op.

### 7. Off-site backups to R2 (optional)

Full SD snapshots go to Cloudflare R2 once a day at 4 AM local. Excludes `config.txt`, `*.tmp`/`*.bak` files, and `/logs`. Each backup lives at `state/YYYY-MM-DD/` with a sha256 manifest; `state/latest.json` is the atomic commit pointer.

```bash
wrangler r2 bucket create helloesp-backup
wrangler deploy
```

The binding is declared in `wrangler.toml`. If the binding isn't present, the Worker falls back to emailing the bundle as an attachment (if SMTP2GO is configured) so you don't silently lose backups.

**Rotation (Grandfather-Father-Son).** Keeps 7 daily + 4 weekly (Sundays) + 12 monthly (1st of month) + every Jan 1 forever. Anything outside those rules older than 8 days is pruned. A prefix guard refuses to delete anything not matching `state/YYYY-MM-DD/`.

**Storage math.** ~1.4 MB per snapshot × ~23 retained snapshots ≈ 32 MB/year. Well under R2's 10 GB free tier.

**Alerting.** No email on success. A single email fires if a backup fails (throttled 1/hr) or if no successful backup has been committed for >48 h (once per day).

### Restoring from an R2 backup

1. Format a new FAT32 SD card (≤32 GB).
2. Copy the `data/` folder from this repo to the SD root. HTML/CSS/JS/images are deploy-recreatable and not in the backup.
3. Open the R2 bucket. Read `state/latest.json` for the most recent snapshot date.
4. Download every object under `state/{date}/`, preserving subdirectories (e.g. `stats/weekly/2026-W16.json` → `/stats/weekly/2026-W16.json` on SD).
5. Recreate `config.txt` from your own records. Secrets aren't in the backup on purpose.
6. Insert the SD card and boot.

For single-file fixes, download one object from R2 and drop it in via the admin file manager. No full restore needed.

### 8. X / Twitter auto-post (optional)

Each Chronicle entry crossposts once to X. Useful as a launch smoke test (the seeded launch entry tweets on first deploy) and for ongoing reach. All 4 secrets must be set or the post path silently no-ops, so the seal flow is never blocked by missing creds.

Create an X developer app with **read + write** permission, generate an access token, and:

```bash
wrangler secret put X_API_KEY        # consumer key
wrangler secret put X_API_SECRET     # consumer secret
wrangler secret put X_ACCESS_TOKEN   # access token
wrangler secret put X_ACCESS_SECRET  # access token secret
```

Tweets carry a `Day N · {date}` header, the full entry body, and a permalink to `helloesp.com/chronicle/<date>`. The character budget defaults to 25,000 (X Premium's per-tweet ceiling) so weekly/monthly/quarterly reflections post in full; truncation at last-word-boundary still kicks in past that limit. Forks without Premium should drop the cap in `_chronicleFormatTweet` from 25000 to 280 (X's t.co counts URLs as 23 chars regardless of length, accounted for in the budget). Idempotency uses a `posted_to_x: true` flag stored on the entry itself, so DO eviction, isolate restarts, and same-day reseals can't double-post.

**Owner notes thread under as replies.** Adding a note via the admin panel posts a reply tweet to the original Chronicle tweet, so the X timeline mirrors the website's structure (chip's voice + curatorial annotation). First non-empty note posts; subsequent edits don't re-post (X API v2 has no free tweet-edit). Tracked via `note_x_tweet_id`. Same silent-skip rules apply if creds are missing or the original tweet failed.

OAuth 1.0a is signed in-Worker via `crypto.subtle.HMAC-SHA1`; no third-party libraries.

## Security

- `config.txt` is in `.gitignore`. Do not commit it. Treat `WORKER_SECRET` and `device_key` like passwords.
- Admin endpoints (`/admin`, `/admin/*`, `/_upload`, `/_ota`, `/guestbook/pending`, `/guestbook/moderate`) return 404 to any request arriving through the Worker. They are only reachable from the LAN.
- Admin Basic Auth uses a per-IP lockout (5 failed attempts → 10-minute block).
- Worker enforces 60 req/min per IP, caps POST bodies at 8 KB, and strips hop-by-hop headers.
- CDN scripts are pinned with SHA-384 SRI hashes.
- Guestbook input is stripped of control bytes and CSV-breaking characters before storage; output is JSON- and HTML-escaped on both sides.

## Limitations

If you're thinking about building your own, here's what to expect:

- **Concurrency is modest.** Roughly 5 simultaneous requests before latency is noticeable. Cloudflare's edge absorbs bursts of static content; dynamic endpoints hit the chip.
- **No HTTPS origin.** TLS terminates at the Worker. LAN access is HTTP only, so don't treat the admin panel as secure without a VPN or physical access.
- **Single point of failure.** One chip, one WiFi link, one SD card. A power blip takes the site down until reboot (usually under 30 seconds).
- **SD wear.** CSV logging every 5 min plus guestbook writes is a few hundred writes per day. Consumer SD cards will last years, not decades; swap annually if the site matters.
- **Memory is tight.** 520 KB total, ~180 KB free at idle. Large responses or many concurrent frames can OOM; a heap watchdog reboots at under 30 KB free rather than hang.

## Build your own

The full stack is documented above: firmware, hardware, Worker, optional R2 backups, optional email alerts. Everything you need to run your own is here.

**Got one running?** Open a PR or issue with the link. I'll list notable builds in this section as they come in. Curious to see what people do with it.

## Repository layout

```
src/main.cpp                 Firmware (Arduino framework)
data/                        SD card contents: HTML, CSS, JS, images, favicon SVG
data/*.html.gz               Pre-gzipped HTML (auto-generated, see below)
worker/worker.js             Cloudflare Worker + Durable Object (relay, SSE, weather, badges)
worker/wrangler.toml         Worker config
scripts/gzip_assets.py       PlatformIO pre-build step that gzips data/*.html
platformio.ini               Build config + cppcheck flags
.github/workflows/           CI: PlatformIO build, CodeQL, Lighthouse
.github/dependabot.yml       Dependency update automation (GitHub Actions)
.github/ISSUE_TEMPLATE/      Issue chooser (security policy + discussions)
.github/assets/              README hero image
SECURITY.md                  Security disclosure policy
```

### HTML asset gzipping

HTML files under `data/` are pre-gzipped before upload. `scripts/gzip_assets.py`
runs automatically on every `pio run` (wired via `extra_scripts` in
`platformio.ini`) and produces `.html.gz` companions next to each source.

The firmware's `beginResponseGzipOrRaw()` helper checks for a `.gz` version on
SD and serves it with `Content-Encoding: gzip` when the client accepts it.
Falls back transparently to the uncompressed file if no `.gz` exists.

**Why this matters:** a 90+ KB uncompressed `index.html` served directly
over LAN can saturate LWIP's pbuf pool mid-stream. Under that pressure,
colliding writes on the outbound WebSocket to Cloudflare fail with EAGAIN,
and DNS lookups (which share the same pool) also start failing. Gzipping
drops wire size ~4× and keeps the pool clear.

When you edit an `.html` file, `pio run` regenerates the `.gz` automatically.
Upload both versions to the SD card (the file manager in the admin panel
handles this fine).

## Credits

- [ESPAsyncWebServer](https://github.com/me-no-dev/ESPAsyncWebServer)
- [AsyncTCP](https://github.com/me-no-dev/AsyncTCP)
- [Adafruit BME280](https://github.com/adafruit/Adafruit_BME280_Library)
- [Adafruit CCS811](https://github.com/adafruit/Adafruit_CCS811_Library)
- [Adafruit SSD1306](https://github.com/adafruit/Adafruit_SSD1306) + [GFX](https://github.com/adafruit/Adafruit-GFX-Library)
- [Adafruit RTClib](https://github.com/adafruit/RTClib)
- [Uptime Library](https://github.com/YiannisBourkelis/Uptime-Library)

## Contact

Bug reports and feature questions → [Issues](https://github.com/Tech1k/helloesp/issues).
For press or anything else → hello@tech1k.com.

## License

MIT. See [LICENSE](LICENSE).
