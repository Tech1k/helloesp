// HelloESP Cloudflare Worker relay (WebSocket + chunked base64 streaming)

const PAGE_CSS = `*{margin:0;padding:0;box-sizing:border-box}:root{--bg:#f8f7f4;--ink:#1a1a1a;--mid:#888;--faint:#ccc}@media(prefers-color-scheme:dark){:root{--bg:#111110;--ink:#e8e6e1;--mid:#666;--faint:#2a2a28}}body{background:var(--bg);color:var(--ink);font-family:ui-monospace,"SF Mono","Cascadia Mono","Consolas",monospace;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}main{max-width:480px;text-align:center}h1{font-size:clamp(48px,10vw,72px);font-weight:400;letter-spacing:-0.02em;margin-bottom:20px}p{font-size:13px;color:var(--mid);line-height:1.8;margin-bottom:16px}p.lede{color:var(--ink);font-size:14px;margin-bottom:24px}.note{font-size:11px;color:var(--mid);border-top:1px solid var(--faint);padding-top:20px;margin-top:28px;line-height:1.7}a{color:var(--ink);font-size:11px;letter-spacing:0.1em;text-transform:uppercase;text-underline-offset:3px;display:inline-block;margin:0 8px}.site-name{position:fixed;top:24px;left:50%;transform:translateX(-50%);font-size:11px;letter-spacing:0.15em;text-transform:uppercase;color:var(--mid);text-decoration:none;margin:0}.site-name:hover{color:var(--ink)}.status{font-size:10px;letter-spacing:0.1em;text-transform:uppercase;color:var(--mid);margin-top:28px}.dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--mid);margin-left:6px;vertical-align:middle;animation:pulse 1.4s ease-in-out infinite}@keyframes pulse{0%,100%{opacity:0.25}50%{opacity:1}}a:focus-visible{outline:2px solid var(--ink);outline-offset:2px;border-radius:2px}@media(prefers-reduced-motion:reduce){.dot{animation:none}}`;

const CHIP_ICON_PATHS = `<path fill-rule="evenodd" d="M7 5a2 2 0 00-2 2v18a2 2 0 002 2h18a2 2 0 002-2V7a2 2 0 00-2-2H7zm6 7a1 1 0 00-1 1v6a1 1 0 001 1h6a1 1 0 001-1v-6a1 1 0 00-1-1h-6z"/><rect x="7.5" y="1" width="2" height="3" rx=".5"/><rect x="12.5" y="1" width="2" height="3" rx=".5"/><rect x="17.5" y="1" width="2" height="3" rx=".5"/><rect x="22.5" y="1" width="2" height="3" rx=".5"/><rect x="7.5" y="28" width="2" height="3" rx=".5"/><rect x="12.5" y="28" width="2" height="3" rx=".5"/><rect x="17.5" y="28" width="2" height="3" rx=".5"/><rect x="22.5" y="28" width="2" height="3" rx=".5"/><rect x="1" y="7.5" width="3" height="2" rx=".5"/><rect x="1" y="12.5" width="3" height="2" rx=".5"/><rect x="1" y="17.5" width="3" height="2" rx=".5"/><rect x="1" y="22.5" width="3" height="2" rx=".5"/><rect x="28" y="7.5" width="3" height="2" rx=".5"/><rect x="28" y="12.5" width="3" height="2" rx=".5"/><rect x="28" y="17.5" width="3" height="2" rx=".5"/><rect x="28" y="22.5" width="3" height="2" rx=".5"/>`;

const FAVICON = `<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,${encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="#2686e6">${CHIP_ICON_PATHS}</svg>`).replace(/'/g, '%27').replace(/"/g, '%22')}">`;

// The header "HelloESP" breadcrumb used on every Worker-served page
const SITE_NAME_LINK = `<a href="/" class="site-name"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" width="12" height="12" fill="#2686e6" style="vertical-align:-2px;margin-right:5px;">${CHIP_ICON_PATHS}</svg>HelloESP</a>`;

const RETRY_JS = `<script>(function(){setInterval(function(){if(document.visibilityState!=='visible')return;fetch('/ping?_='+Date.now(),{cache:'no-store'}).then(function(r){if(r.ok)location.reload()}).catch(function(){})},3000)})();</script>`;

const OFFLINE_HTML = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="robots" content="noindex">${FAVICON}<title>Offline / HelloESP</title><style>${PAGE_CSS}</style></head><body>${SITE_NAME_LINK}<main><h1>Offline</h1><p class="lede">The ESP32 serving this site isn't connected right now.</p><p>It might be rebooting, out of WiFi range, or unplugged. It'll come back on its own.</p><p><a href="/">Retry</a><a href="https://github.com/Tech1k/helloesp">GitHub</a></p><p class="status">Reconnecting<span class="dot" aria-hidden="true"></span></p><p class="note">HelloESP runs entirely on an ESP32. When the chip is unreachable, Cloudflare serves this page instead.</p></main>${RETRY_JS}</body></html>`;

const TIMEOUT_HTML = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="robots" content="noindex">${FAVICON}<title>Timeout / HelloESP</title><style>${PAGE_CSS}</style></head><body>${SITE_NAME_LINK}<main><h1>Timeout</h1><p class="lede">The ESP32 got your request but didn't answer in time.</p><p>Probably busy handling something else. Try again in a moment.</p><p><a href="/">Retry</a><a href="https://github.com/Tech1k/helloesp">GitHub</a></p><p class="status">Retrying<span class="dot" aria-hidden="true"></span></p><p class="note">HelloESP runs entirely on an ESP32. If a request takes over 30 seconds, Cloudflare shows this page.</p></main>${RETRY_JS}</body></html>`;

const SEC_HEADERS = {
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'strict-origin-when-cross-origin'
};

function applySecHeaders(h) {
  for (const [k, v] of Object.entries(SEC_HEADERS)) h.set(k, v);
  return h;
}

function offlineResponse() {
  return new Response(OFFLINE_HTML, { status: 502, headers: { 'Content-Type': 'text/html', 'Cache-Control': 'no-store', ...SEC_HEADERS } });
}

function timeoutResponse() {
  return new Response(TIMEOUT_HTML, { status: 504, headers: { 'Content-Type': 'text/html', 'Cache-Control': 'no-store', ...SEC_HEADERS } });
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// Parse "14 days, 3 hours, 22 minutes, 45 seconds" into a compact "14d 3h" / "3h 22m" / "22m 45s" string
function formatUptime(str) {
  if (!str) return '';
  const m = String(str).match(/(\d+)\s*days?[,\s]+(\d+)\s*hours?[,\s]+(\d+)\s*minutes?[,\s]+(\d+)\s*seconds?/);
  if (!m) return str;
  const d = +m[1], h = +m[2], mn = +m[3], s = +m[4];
  if (d > 0)  return `up ${d}d ${h}h`;
  if (h > 0)  return `up ${h}h ${mn}m`;
  if (mn > 0) return `up ${mn}m ${s}s`;
  return `up ${s}s`;
}

function maintenanceResponse(until, message) {
  const remainingMs = Math.max(0, until - Date.now());
  const safeMsg = message ? escapeHtml(String(message).slice(0, 200)) : '';
  const lede = safeMsg || "The site is down for planned maintenance.";
  let etaLine;
  let retryAfter;
  if (remainingMs < 60000) {
    etaLine = 'Back shortly.';
    retryAfter = 30;
  } else {
    const mins = Math.ceil(remainingMs / 60000);
    etaLine = `Back in about ${mins} ${mins === 1 ? 'minute' : 'minutes'}.`;
    retryAfter = Math.min(3600, mins * 60);
  }
  // Include the absolute unix ms in a data attribute so client JS can render
  // the "back at" time in the visitor's local timezone, regardless of where
  // they are. Falls back to the relative phrase if JS is disabled.
  const localTimeJs = `<script>(function(){var el=document.getElementById('back-at');if(!el)return;var t=parseInt(el.getAttribute('data-until'),10);if(!t)return;try{el.textContent=' (around '+new Date(t).toLocaleTimeString(undefined,{hour:'numeric',minute:'2-digit'})+' your time)';}catch(e){}})();</script>`;
  const backAtSpan = remainingMs >= 60000
    ? `<span id="back-at" data-until="${until}"></span>`
    : '';
  const html = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><meta name="robots" content="noindex">${FAVICON}<title>Maintenance / HelloESP</title><style>${PAGE_CSS}</style></head><body>${SITE_NAME_LINK}<main><h1>Maintenance</h1><p class="lede">${lede}</p><p>${etaLine}${backAtSpan}</p><p><a href="/">Retry</a><a href="https://github.com/Tech1k/helloesp">GitHub</a></p><p class="status">Checking<span class="dot" aria-hidden="true"></span></p><p class="note">HelloESP runs entirely on an ESP32. Planned work is in progress. The page will refresh automatically when the site is back.</p></main>${localTimeJs}${RETRY_JS}</body></html>`;
  return new Response(html, {
    status: 503,
    headers: {
      'Content-Type': 'text/html',
      'Cache-Control': 'no-store',
      'Retry-After': String(retryAfter),
      ...SEC_HEADERS
    }
  });
}

function b64ToBytes(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

// Constant-time string compare; avoids leaking secret length/content via
// early-exit timing when comparing provided creds against WORKER_SECRET.
function timingSafeEqualStr(a, b) {
  const enc = new TextEncoder();
  const ab = enc.encode(String(a ?? ''));
  const bb = enc.encode(String(b ?? ''));
  const len = Math.max(ab.length, bb.length);
  let diff = ab.length ^ bb.length;
  for (let i = 0; i < len; i++) {
    const av = i < ab.length ? ab[i] : 0;
    const bv = i < bb.length ? bb[i] : 0;
    diff |= av ^ bv;
  }
  return diff === 0;
}

const MAX_BODY = 8192;
const RATE_LIMIT_WINDOW = 60000; // 1 min
const RATE_LIMIT_MAX = 60;       // per IP per window
const SSE_MAX_CLIENTS = 5000;    // up to ~25 MB of 128 MB DO memory

// Weather proxy; Denver, CO is broad enough to be non-identifying (~3M metro pop)
const WEATHER_LAT = 39.74;
const WEATHER_LON = -104.99;
const WEATHER_LOCATION = 'Denver, CO';
const WEATHER_REFRESH_MS = 3600000; // 1 hour
const WEATHER_STALE_MS   = 7200000; // after 2h with no successful refresh, stop sending outdoor data

// Email backup bundle limits. The Worker chunks the final bundle across however many emails
// are needed (see BACKUP_PART_SIZE). These ceilings are runaway protection only.
const BACKUP_MAX_B64       = 80 * 1024 * 1024; // ~60 MB raw bytes once decoded
const BACKUP_MAX_CHUNKS    = 25000;             // sanity cap on per-session WS frames
const BACKUP_SESSION_IDLE  = 15 * 60 * 1000;    // drop sessions idle > 15 min
const BACKUP_PART_SIZE     = 7 * 1024 * 1024;   // raw-byte slice per email (safely < SMTP2GO 10 MB rec.)
const BACKUP_PART_DELAY_MS = 2000;              // pause between multipart sends

export class EspRelay {
  constructor(state, env) {
    this.state = state;
    this.env = env || {};
    this.espSocket = null;
    this.pendingRequests = new Map();
    this.activeResponses = new Map();
    this.requestId = 0;
    this.currentStreamId = null;
    this.lastActivity = 0;
    this.rateLimits = new Map();
    this.wsAuthFails = new Map();
    this.lastEmailAt = 0;
    this.hmacAuthenticated = false;
    this.maintenanceUntil = 0;
    this.maintenanceMessage = '';
    this.sseClients = new Set();
    this.lastStats = null;  // JSON string of the most recent ESP stats push
    this.lastStatsAt = 0;   // epoch ms when lastStats was set; used to detect staleness for badges
    this.lastCountries = null;  // JSON string of last /countries relay response
    this.lastCountriesAt = 0;
    this.lastRecords = null;    // JSON string of last /records.json relay response
    this.lastRecordsAt = 0;
    this.lastWeather = null; // cached outdoor weather object
    this.deadmanAlertSent = false; // so we don't spam when offline persists past 24h
    this.backupSessions = new Map(); // seq -> { startedAt, meta, files[], currentFile, totalB64, aborted }
    this.lastBackupAt = 0;
    this.lastBackupSize = 0;
    this.lastBackupDate = '';
    this.lastBackupFailureEmailAt = 0;
    this.lastBackupMissedEmailAt = 0;
    this.state.blockConcurrencyWhile(async () => {
      const u = await state.storage.get('maintenanceUntil');
      const m = await state.storage.get('maintenanceMessage');
      const w = await state.storage.get('lastWeather');
      const dm = await state.storage.get('deadmanAlertSent');
      const lba = await state.storage.get('lastBackupAt');
      const lbs = await state.storage.get('lastBackupSize');
      const lbd = await state.storage.get('lastBackupDate');
      if (typeof u === 'number') this.maintenanceUntil = u;
      if (typeof m === 'string') this.maintenanceMessage = m;
      if (w && typeof w === 'object') this.lastWeather = w;
      if (typeof dm === 'boolean') this.deadmanAlertSent = dm;
      if (typeof lba === 'number') this.lastBackupAt = lba;
      if (typeof lbs === 'number') this.lastBackupSize = lbs;
      if (typeof lbd === 'string') this.lastBackupDate = lbd;
    });
    this._ensureAlarm(30000);
  }

  // Only schedule a new alarm if none is set or the existing one is further out
  // than the requested window. Otherwise we keep pushing the alarm into the
  // future on every SSE connect / constructor call, which starves the deadman.
  async _ensureAlarm(msFromNow) {
    const target = Date.now() + msFromNow;
    const existing = await this.state.storage.getAlarm();
    if (existing == null || existing > target) {
      await this.state.storage.setAlarm(target);
    }
  }

  async maybeSendDeadmanAlert() {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;
    const now = Date.now();
    // DEADMAN_HOURS env var overrides default; typical home has near-zero ISP outages >6h
    const hoursCfg = parseFloat(env.DEADMAN_HOURS);
    const DEAD_HOURS = (hoursCfg > 0 && hoursCfg < 720) ? hoursCfg : 6;
    const DEAD_MS = DEAD_HOURS * 3600000;
    const BACK_MS = 300000;   // 5 minutes of fresh activity = considered back
    const elapsed = now - this.lastActivity;

    // device has been silent for >DEAD_HOURS and we haven't alerted yet
    if (this.lastActivity > 0 && elapsed > DEAD_MS && !this.deadmanAlertSent) {
      this.deadmanAlertSent = true;
      await this.state.storage.put('deadmanAlertSent', true);
      const hours = Math.floor(elapsed / 3600000);
      const lastSeen = new Date(this.lastActivity).toISOString();
      const body = `HelloESP has been unreachable for ${hours} hours.\n\nLast heartbeat: ${lastSeen}\n\nThe device may be offline, rebooting into a loop, or has lost WiFi.\nThis is an automated dead-man's-switch alert; you won't get another until it recovers and goes silent again.`;
      try {
        await fetch('https://api.smtp2go.com/v3/email/send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
          body: JSON.stringify({
            sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
            to: [env.NOTIFY_EMAIL],
            subject: `HelloESP unreachable (${hours}h)`,
            text_body: body
          })
        });
      } catch (e) { console.error('deadman email failed:', e && e.message); }
      return;
    }

    // device came back; clear the flag and send a recovery notification
    if (this.deadmanAlertSent && elapsed < BACK_MS) {
      this.deadmanAlertSent = false;
      await this.state.storage.put('deadmanAlertSent', false);
      const body = `HelloESP is back online.\n\nFirst fresh heartbeat: ${new Date(this.lastActivity).toISOString()}\n\nThis is a dead-man's-switch recovery notification.`;
      try {
        await fetch('https://api.smtp2go.com/v3/email/send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
          body: JSON.stringify({
            sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
            to: [env.NOTIFY_EMAIL],
            subject: 'HelloESP recovered',
            text_body: body
          })
        });
      } catch (e) { console.error('deadman-recovered email failed:', e && e.message); }
    }
  }

  async refreshWeather() {
    try {
      const url = `https://api.open-meteo.com/v1/forecast?latitude=${WEATHER_LAT}&longitude=${WEATHER_LON}&current=temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m,surface_pressure&temperature_unit=fahrenheit&wind_speed_unit=mph`;
      const res = await fetch(url, { cf: { cacheTtl: 3600 } });
      if (!res.ok) { console.error('weather http', res.status); return; }
      const data = await res.json();
      if (!data || !data.current) return;
      const c = data.current;
      this.lastWeather = {
        temp_f:        c.temperature_2m,
        humidity:      c.relative_humidity_2m,
        weather_code:  c.weather_code,
        wind_mph:      c.wind_speed_10m,
        pressure_hpa:  c.surface_pressure,
        location:      WEATHER_LOCATION,
        fetched_at:    Date.now()
      };
      await this.state.storage.put('lastWeather', this.lastWeather);
    } catch (e) {
      console.error('weather fetch failed:', e && e.message);
    }
  }

  enrichStats(rawData) {
    // returns the stats object with outdoor weather injected if we have fresh-enough data
    if (!this.lastWeather) return rawData;
    if (Date.now() - this.lastWeather.fetched_at > WEATHER_STALE_MS) return rawData;
    return {
      ...rawData,
      outdoor: {
        temp_f:       this.lastWeather.temp_f,
        humidity:     this.lastWeather.humidity,
        weather_code: this.lastWeather.weather_code,
        wind_mph:     this.lastWeather.wind_mph,
        pressure_hpa: this.lastWeather.pressure_hpa,
        location:     this.lastWeather.location,
        age_ms:       Date.now() - this.lastWeather.fetched_at
      }
    };
  }

  badgeState() {
    // figure out what the badge should say + what color: live / stale / offline / maintenance
    const now = Date.now();
    if (now < this.maintenanceUntil) return { state: 'maintenance', color: '#c06b00' };
    if (!this.lastStats) return { state: 'offline', color: '#666' };
    if (now - this.lastStatsAt > 120000) return { state: 'stale', color: '#666' };
    return { state: 'live', color: '#2686e6' };
  }

  buildCurlCard() {
    let s = null;
    try { if (this.lastStats) s = JSON.parse(this.lastStats); } catch (e) {}
    const fmtNum = (v) => (Number.isFinite(v) ? Math.round(v).toLocaleString() : '—');
    const uptime = s && s.uptime ? String(s.uptime) : '—';
    const tempF  = s && s.temperature && Number.isFinite(s.temperature.fahrenheit)
        ? s.temperature.fahrenheit.toFixed(1) + '°F' : '—';
    const visitors  = s ? fmtNum(s.visitors) : '—';
    const countries = s ? fmtNum(s.countries) : '—';
    const co2  = s ? fmtNum(s.co2_ppm) + ' ppm (eCO₂)' : '—';
    const lines = [
      '',
      '   _   _      _ _        _____ ____  ____',
      '  | | | | ___| | |  ___  | ____/ ___|| _ \\',
      '  | |_| |/ _ \\ | |/ _ \\|  _| \\_\\| |_) |',
      '  |  _  |  __/ | | (_) | |___ ___)  |  __/',
      '  |_| |_|\\___|_|_|\\___/|_____|____/|_|',
      '',
      '  A website running on an ESP32 on a wall in Colorado.',
      '',
      '  Uptime:     ' + uptime,
      '  Indoor:     ' + tempF,
      '  Air:        ' + co2,
      '  Visitors:   ' + visitors + ' all-time',
      '  Countries:  ' + countries,
      '',
      '  Web:     https://helloesp.com',
      '  Source:  https://github.com/Tech1k/helloesp',
      '  RSS:     https://helloesp.com/guestbook.rss',
      '',
      '  (You asked for it with curl. Nice.)',
      ''
    ];
    return lines.join('\n');
  }

  buildStatusBadge(metric) {
    const s = this.badgeState();
    let stats = null;
    try { if (this.lastStats) stats = JSON.parse(this.lastStats); } catch (e) {}

    let valueText;
    if (s.state === 'maintenance') {
      valueText = 'maintenance';
    } else if (s.state === 'offline' || s.state === 'stale' || !stats) {
      valueText = 'offline';
    } else {
      switch (metric) {
        case 'visits':
          valueText = (stats.visitors != null ? stats.visitors : 0) + ' visits';
          break;
        case 'temp':
          if (stats.temperature && typeof stats.temperature.fahrenheit === 'number') {
            valueText = Math.round(stats.temperature.fahrenheit) + '\u00b0F';
          } else { valueText = 'no data'; }
          break;
        case 'online':
          valueText = '\u25CF live';
          break;
        case 'uptime':
        default:
          valueText = formatUptime(stats.uptime || '');
          if (!valueText) valueText = 'up ?';
          break;
      }
    }

    // Approximate Verdana-11 character width ~6.5px. Label "HelloESP" fixed at 78px.
    const labelW = 78;
    const charW = 7;
    const valueW = Math.max(54, Math.round(valueText.length * charW + 20));
    const totalW = labelW + valueW;

    const safeValue = escapeHtml(valueText);
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${totalW}" height="20" viewBox="0 0 ${totalW} 20" role="img" aria-label="HelloESP: ${safeValue}"><title>HelloESP: ${safeValue}</title><linearGradient id="g" x2="0" y2="100%"><stop offset="0" stop-color="#bbb" stop-opacity=".1"/><stop offset="1" stop-opacity=".1"/></linearGradient><clipPath id="r"><rect width="${totalW}" height="20" rx="3" fill="#fff"/></clipPath><g clip-path="url(#r)"><rect width="${labelW}" height="20" fill="#1a1a1a"/><rect x="${labelW}" width="${valueW}" height="20" fill="${s.color}"/><rect width="${totalW}" height="20" fill="url(#g)"/></g><g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11"><text x="${labelW/2}" y="14">HelloESP</text><text x="${labelW + valueW/2}" y="14">${safeValue}</text></g></svg>`;
  }

  buildStatusWide() {
    const s = this.badgeState();
    let stats = null;
    try { if (this.lastStats) stats = JSON.parse(this.lastStats); } catch (e) {}

    const W = 340, H = 78;
    const chipX = 14, chipY = 5;   // chip icon position
    const chipSize = 20;

    let line1Right;  // status indicator text + color
    if (s.state === 'maintenance')       line1Right = { text: 'maintenance', color: '#c06b00' };
    else if (s.state === 'live')         line1Right = { text: '\u25CF live', color: '#0a8b4a' };
    else                                 line1Right = { text: '\u25CF offline', color: '#b02030' };

    let row2 = '', row3 = '';
    if (stats && s.state !== 'offline' && s.state !== 'stale') {
      const tempF = stats.temperature && typeof stats.temperature.fahrenheit === 'number'
        ? Math.round(stats.temperature.fahrenheit) + '\u00b0F' : null;
      const hum = stats.humidity_percent != null ? Math.round(stats.humidity_percent) + '% RH' : null;
      const co2 = stats.co2_ppm != null ? stats.co2_ppm + ' CO\u2082 ppm' : null;
      const rowA = [tempF, hum, co2].filter(Boolean).join('  \u00b7  ');
      const up = formatUptime(stats.uptime || '');
      const vis = stats.visitors != null ? stats.visitors + ' visits' : null;
      const rowB = [up, vis].filter(Boolean).join('  \u00b7  ');
      row2 = escapeHtml(rowA);
      row3 = escapeHtml(rowB);
    } else {
      row2 = s.state === 'maintenance' ? 'Site under planned maintenance.' : 'Device is not currently reachable.';
      row3 = 'Check back in a moment.';
    }

    const statusColor = escapeHtml(line1Right.color);
    const statusText = escapeHtml(line1Right.text);

    return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="HelloESP status"><title>HelloESP status</title><clipPath id="rw"><rect width="${W}" height="${H}" rx="6" fill="#fff"/></clipPath><g clip-path="url(#rw)"><rect width="${W}" height="${H}" fill="#f8f7f4"/><rect width="${W}" height="28" fill="#1a1a1a"/></g><g transform="translate(${chipX} ${chipY}) scale(${chipSize/32})" fill="#2686e6">${CHIP_ICON_PATHS}</g><g font-family="Verdana,Geneva,DejaVu Sans,sans-serif" fill="#fff"><text x="42" y="19" font-size="12" font-weight="bold">HelloESP</text></g><g font-family="Verdana,Geneva,DejaVu Sans,sans-serif"><text x="${W - 14}" y="19" text-anchor="end" font-size="11" fill="${statusColor}">${statusText}</text><text x="14" y="50" font-size="12" fill="#1a1a1a">${row2}</text><text x="14" y="68" font-size="11" fill="#555">${row3}</text></g></svg>`;
  }

  broadcastEvent(eventName, jsonStr) {
    const payload = new TextEncoder().encode(`event: ${eventName}\ndata: ${jsonStr}\n\n`);
    const dead = [];
    for (const w of this.sseClients) {
      w.write(payload).catch(() => dead.push(w));
    }
    for (const w of dead) this.sseClients.delete(w);
  }

  async setMaintenance(minutes, message) {
    const m = Math.min(120, Math.max(0, Number(minutes) || 0));
    if (m === 0) {
      this.maintenanceUntil = 0;
      this.maintenanceMessage = '';
      await this.state.storage.delete('maintenanceUntil');
      await this.state.storage.delete('maintenanceMessage');
    } else {
      this.maintenanceUntil = Date.now() + m * 60000;
      this.maintenanceMessage = String(message || '').slice(0, 200);
      await this.state.storage.put('maintenanceUntil', this.maintenanceUntil);
      await this.state.storage.put('maintenanceMessage', this.maintenanceMessage);
    }
  }

  async verifyHmac(hexSig, nonce) {
    try {
      if (!hexSig || !/^[0-9a-f]{64}$/i.test(hexSig)) return false;
      const sig = new Uint8Array(32);
      for (let i = 0; i < 32; i++) sig[i] = parseInt(hexSig.slice(i * 2, i * 2 + 2), 16);
      const keyBytes = new TextEncoder().encode(this.env.HMAC_SECRET);
      const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
      return await crypto.subtle.verify('HMAC', key, sig, new TextEncoder().encode(nonce));
    } catch (e) {
      return false;
    }
  }

  async handleEvent(msg) {
    try {
      if (msg.event === 'maintenance') {
        await this.setMaintenance(msg.minutes, msg.message);
        return;
      }
      if (msg.event === 'stats_update') {
        if (msg.data) {
          const enriched = this.enrichStats(msg.data);
          // lastStats is served to new SSE joins, /stats HTTP, and the status
          // badge; we don't want a stale `clients` count baked into any of those.
          this.lastStats = JSON.stringify(enriched);
          this.lastStatsAt = Date.now();
          const live = { ...enriched, clients: this.sseClients.size };
          this.broadcastEvent('stats', JSON.stringify(live));
        }
        return;
      }
      if (msg.event === 'console_update') {
        if (msg.data) this.broadcastEvent('console', JSON.stringify(msg.data));
        return;
      }
      if (msg.event && msg.event.startsWith('backup_')) {
        await this.handleBackupEvent(msg);
        return;
      }
      if (msg.event === 'r2_healthcheck') {
        await this._runR2Healthcheck();
        return;
      }
      if (msg.event === 'test_email') {
        await this._sendTestEmail();
        return;
      }
      if (msg.event !== 'pending_guestbook') return;
      const env = this.env;
      if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;

      const now = Date.now();
      if (now - this.lastEmailAt < 300000) return;
      this.lastEmailAt = now;

      const count = Math.max(0, parseInt(msg.count, 10) || 0);
      if (count < 1) return;
      const noun = count === 1 ? 'entry' : 'entries';

      let body = `${count} guestbook ${noun} awaiting moderation on HelloESP.\n\n`;
      if (msg.name) {
        body += `Latest:\n`;
        body += `  ${String(msg.name).slice(0, 40)}`;
        if (msg.country && msg.country !== '??') body += ` (${String(msg.country).slice(0, 3)})`;
        body += `\n`;
        if (msg.message) body += `  "${String(msg.message).slice(0, 300)}"\n`;
        body += `\n`;
      }
      body += `To review, open /admin from your LAN.`;

      const res = await fetch('https://api.smtp2go.com/v3/email/send', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Smtp2go-Api-Key': env.SMTP2GO_KEY
        },
        body: JSON.stringify({
          sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
          to: [env.NOTIFY_EMAIL],
          subject: `HelloESP: ${count} pending guestbook ${noun}`,
          text_body: body
        })
      });
      if (!res.ok) console.error('SMTP2GO non-2xx:', res.status);
    } catch (e) {
      console.error('SMTP2GO send failed:', e && e.message);
    }
  }

  // --- Backup session accumulator (device streams chunked events; Worker reassembles, then
  // storeBackupBundle writes to R2 or falls back to emailBackupBundle if no R2 binding) ---

  pruneBackupSessions() {
    const cutoff = Date.now() - BACKUP_SESSION_IDLE;
    for (const [seq, s] of this.backupSessions) {
      if (s.startedAt < cutoff) this.backupSessions.delete(seq);
    }
  }

  async handleBackupEvent(msg) {
    const seq = msg.seq;
    if (typeof seq !== 'number') return;

    if (msg.event === 'backup_start') {
      this.pruneBackupSessions();
      this.backupSessions.set(seq, {
        startedAt: Date.now(),
        meta: {
          generated_at: String(msg.generated_at || ''),
          firmware: String(msg.firmware || ''),
          uptime: String(msg.uptime || '')
        },
        files: [],
        currentFile: null,
        totalB64: 0,
        chunkCount: 0,
        aborted: false
      });
      return;
    }

    const s = this.backupSessions.get(seq);
    if (!s || s.aborted) return;

    if (msg.event === 'backup_file_start') {
      s.currentFile = {
        name: String(msg.name || 'unknown'),
        size: Math.max(0, parseInt(msg.size, 10) || 0),
        chunks: []
      };
      return;
    }

    if (msg.event === 'backup_file_chunk') {
      if (!s.currentFile) return;
      const data = String(msg.data || '');
      s.currentFile.chunks.push(data);
      s.totalB64 += data.length;
      s.chunkCount++;
      if (s.totalB64 > BACKUP_MAX_B64 || s.chunkCount > BACKUP_MAX_CHUNKS) {
        s.aborted = true;
        this.backupSessions.delete(seq);
        console.error(`backup session ${seq} aborted: totalB64=${s.totalB64} chunks=${s.chunkCount}`);
      }
      return;
    }

    if (msg.event === 'backup_file_end') {
      if (!s.currentFile) return;
      s.files.push({
        name: s.currentFile.name,
        size: s.currentFile.size,
        content_b64: s.currentFile.chunks.join('')
      });
      s.currentFile = null;
      return;
    }

    if (msg.event === 'backup_file_skipped') {
      s.files.push({
        name: String(msg.name || 'unknown'),
        size: Math.max(0, parseInt(msg.size, 10) || 0),
        skipped: String(msg.reason || 'unknown')
      });
      return;
    }

    if (msg.event === 'backup_end') {
      const files = s.files;
      const meta = s.meta;
      const originalSize = Math.max(0, parseInt(msg.size, 10) || 0);
      this.backupSessions.delete(seq);
      await this.storeBackupBundle(meta, files, originalSize);
    }
  }

  // --- R2 write path ---
  //
  // Bundle layout on R2:
  //   state/YYYY-MM-DD/<file-path-from-device>
  //   state/YYYY-MM-DD/_manifest.json    (sha256 per file, firmware/uptime meta)
  //   state/latest.json                   (atomic pointer, written last = commit marker)
  //
  // Rotation (GFS): 7 daily + 4 weekly (Sun) + 12 monthly (1st) + yearly (Jan 1) forever.
  // Prefix + age guards refuse to delete anything recent or outside the state/YYYY-MM-DD/ namespace.

  static _b64ToBytes(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  static _hex(bytes) {
    let s = '';
    for (const b of bytes) s += b.toString(16).padStart(2, '0');
    return s;
  }

  _bucketDate(generated_at) {
    const m = /^(\d{4}-\d{2}-\d{2})/.exec(generated_at || '');
    return m ? m[1] : new Date().toISOString().slice(0, 10);
  }

  _shouldKeepSnapshot(dateStr, nowMs) {
    const d = new Date(dateStr + 'T00:00:00Z');
    if (isNaN(d.getTime())) return true; // malformed, err on keep
    const ageDays = Math.floor((nowMs - d.getTime()) / 86400000);
    if (ageDays < 8) return true;                                            // daily (last week)
    if (d.getUTCDay() === 0 && ageDays < 29) return true;                    // weekly (Sundays, 4wk)
    if (d.getUTCDate() === 1 && ageDays < 366) return true;                  // monthly (1st, 12mo)
    if (d.getUTCMonth() === 0 && d.getUTCDate() === 1) return true;          // yearly (Jan 1, forever)
    return false;
  }

  async storeBackupBundle(meta, files, originalSize) {
    const env = this.env;
    if (!env.BACKUP) {
      console.warn('R2 binding BACKUP not configured; falling back to email attachment path');
      return this.emailBackupBundle(meta, files, originalSize);
    }

    const date = this._bucketDate(meta.generated_at);
    const prefix = `state/${date}/`;

    const included = files.filter(f => !f.skipped && f.content_b64 !== undefined);
    const skipped = files.filter(f => f.skipped);

    const manifest = {
      schema: 'helloesp-backup/2',
      generated_at: meta.generated_at,
      firmware: meta.firmware,
      uptime: meta.uptime,
      date,
      original_size: originalSize,
      files: []
    };

    let bytesWritten = 0;
    try {
      for (const f of included) {
        // Filename came from the device; reject anything that could escape the snapshot prefix
        // or exceed sane limits. `..` and leading `/` would survive as literals in R2 keys but
        // make the bucket messy and could collide with sibling prefixes on restore.
        if (typeof f.name !== 'string' || f.name.length === 0 || f.name.length > 256
            || f.name.startsWith('/') || f.name.includes('..') || f.name.includes('\x00')) {
          console.warn(`backup ${date}: rejecting suspicious filename:`, JSON.stringify(f.name));
          manifest.files.push({ path: String(f.name).slice(0, 64), size: f.size, skipped: 'rejected_name' });
          continue;
        }
        const bytes = EspRelay._b64ToBytes(f.content_b64);
        const hashBuf = await crypto.subtle.digest('SHA-256', bytes);
        const sha256 = EspRelay._hex(new Uint8Array(hashBuf));
        await env.BACKUP.put(prefix + f.name, bytes);
        manifest.files.push({ path: f.name, size: bytes.length, sha256 });
        bytesWritten += bytes.length;
      }
      for (const f of skipped) {
        manifest.files.push({ path: f.name, size: f.size, skipped: f.skipped });
      }
      await env.BACKUP.put(prefix + '_manifest.json', JSON.stringify(manifest, null, 2), {
        httpMetadata: { contentType: 'application/json' }
      });
      // Atomic commit: latest.json update is the last write. If any earlier step failed, the
      // pointer still names whatever snapshot was last fully committed.
      const latest = {
        date,
        files: manifest.files.length,
        included: included.length,
        skipped: skipped.length,
        bytes: bytesWritten,
        at: Date.now(),
        firmware: meta.firmware,
        generated_at: meta.generated_at
      };
      await env.BACKUP.put('state/latest.json', JSON.stringify(latest, null, 2), {
        httpMetadata: { contentType: 'application/json' }
      });
    } catch (e) {
      const reason = (e && e.message) || String(e);
      console.error(`backup ${date} R2 write failed:`, reason);
      await this._sendBackupFailureAlert(date, reason);
      return;
    }

    this.lastBackupAt = Date.now();
    this.lastBackupSize = bytesWritten;
    this.lastBackupDate = date;
    await this.state.storage.put('lastBackupAt', this.lastBackupAt);
    await this.state.storage.put('lastBackupSize', this.lastBackupSize);
    await this.state.storage.put('lastBackupDate', date);

    // Close the loop back to the device so it can surface "R2 commit confirmed" in the admin UI.
    // Without this the ESP only knows "I sent the bundle", not "the bundle was actually stored".
    if (this.espSocket && this.espSocket.readyState === 1) {
      try {
        this.espSocket.send(JSON.stringify({
          type: 'event',
          event: 'backup_committed',
          date,
          bytes: bytesWritten,
          files: manifest.files.length,
          included: included.length,
          skipped: skipped.length,
          at: this.lastBackupAt
        }));
      } catch (e) {
        console.error('backup_committed push failed:', e && e.message);
      }
    }

    // Fire-and-forget rotation. Its failure is logged but doesn't invalidate the committed backup.
    this._rotateSnapshots().catch(e => console.error('rotation failed:', e && e.message));
  }

  async _rotateSnapshots() {
    const env = this.env;
    if (!env.BACKUP) return;
    const now = Date.now();

    // Discover dated snapshot folders via list+delimiter.
    const listing = await env.BACKUP.list({ prefix: 'state/', delimiter: '/' });
    const folders = [];
    for (const p of (listing.delimitedPrefixes || [])) {
      const m = /^state\/(\d{4}-\d{2}-\d{2})\/$/.exec(p);
      if (m) folders.push(m[1]);
    }

    const toDelete = folders.filter(d => {
      if (this._shouldKeepSnapshot(d, now)) return false;
      const ageDays = Math.floor((now - new Date(d + 'T00:00:00Z').getTime()) / 86400000);
      return ageDays >= 8; // hard floor; never prune recent even if classifier says drop
    });

    for (const date of toDelete) {
      // Belt-and-suspenders: iterate each object under the date prefix and verify the key
      // shape before deleting. Refuse anything outside state/YYYY-MM-DD/.
      let cursor;
      do {
        const sub = await env.BACKUP.list({ prefix: `state/${date}/`, cursor });
        const keys = (sub.objects || [])
          .map(o => o.key)
          .filter(k => /^state\/\d{4}-\d{2}-\d{2}\//.test(k));
        if (keys.length) await env.BACKUP.delete(keys);
        cursor = sub.truncated ? sub.cursor : undefined;
      } while (cursor);
    }
  }

  async _sendBackupFailureAlert(date, reason) {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;
    const now = Date.now();
    if (now - this.lastBackupFailureEmailAt < 3600000) return; // one per hour at most
    this.lastBackupFailureEmailAt = now;
    try {
      await fetch('https://api.smtp2go.com/v3/email/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
        body: JSON.stringify({
          sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
          to: [env.NOTIFY_EMAIL],
          subject: `HelloESP backup FAILED - ${date}`,
          text_body: `Backup for ${date} could not be written to R2.\n\nReason: ${reason}\n\nCheck the R2 bucket and Worker logs.`
        })
      });
    } catch (e) {
      console.error('backup failure email send failed:', e && e.message);
    }
  }

  // Admin-triggered SMTP2GO test. Sends a harmless test email; reports back whether SMTP2GO
  // accepted it. Catches silent failures (wrong key, unverified sender, etc.) before a real
  // alert needs to fire.
  async _sendTestEmail() {
    const env = this.env;
    const sendResult = (pass, detail) => {
      if (this.espSocket && this.espSocket.readyState === 1) {
        try {
          // ESP uses naive indexOf-based JSON field extraction, so strip any characters it
          // can't round-trip (embedded quotes / backslashes / control chars would terminate early).
          const safe = String(detail).replace(/["\\\n\r\t\x00-\x1f]/g, ' ').slice(0, 128);
          this.espSocket.send(JSON.stringify({
            type: 'event',
            event: 'test_email_result',
            pass,
            detail: safe
          }));
        } catch (e) {
          console.error('test_email_result push failed:', e && e.message);
        }
      }
    };

    if (!env.SMTP2GO_KEY) { sendResult(false, 'SMTP2GO_KEY not set'); return; }
    if (!env.NOTIFY_EMAIL) { sendResult(false, 'NOTIFY_EMAIL not set'); return; }

    try {
      const res = await fetch('https://api.smtp2go.com/v3/email/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
        body: JSON.stringify({
          sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
          to: [env.NOTIFY_EMAIL],
          subject: 'HelloESP test email',
          text_body: `This is a manual test sent from the admin panel at ${new Date().toISOString()}.\n\nIf you received this, SMTP2GO is working. Dead-man, backup, and guestbook alerts will use the same path.`
        })
      });
      if (!res.ok) {
        let bodyText = '';
        try { bodyText = (await res.text()).slice(0, 100); } catch (e) {}
        sendResult(false, `SMTP2GO HTTP ${res.status}${bodyText ? ': ' + bodyText : ''}`);
        return;
      }
      sendResult(true, `sent to ${env.NOTIFY_EMAIL}`);
    } catch (e) {
      sendResult(false, 'fetch failed: ' + ((e && e.message) || String(e)));
    }
  }

  // Admin-triggered R2 liveness test. PUTs a small file, reads it back byte-for-byte, deletes.
  // Sends the outcome back to the ESP as an event so the admin UI can display it.
  // The test key lives outside the state/YYYY-MM-DD/ rotation namespace, so rotation won't touch it.
  async _runR2Healthcheck() {
    const env = this.env;
    const sendResult = (pass, detail) => {
      if (this.espSocket && this.espSocket.readyState === 1) {
        try {
          const safe = String(detail).replace(/["\\\n\r\t\x00-\x1f]/g, ' ').slice(0, 128);
          this.espSocket.send(JSON.stringify({
            type: 'event',
            event: 'r2_healthcheck_result',
            pass,
            detail: safe
          }));
        } catch (e) {
          console.error('r2_healthcheck_result push failed:', e && e.message);
        }
      }
    };

    if (!env.BACKUP) {
      sendResult(false, 'R2 binding not configured');
      return;
    }

    const key = 'state/_healthcheck/test.txt';
    const payload = `helloesp r2 healthcheck ${Date.now()}`;

    try {
      await env.BACKUP.put(key, payload);
    } catch (e) {
      sendResult(false, 'PUT failed: ' + ((e && e.message) || String(e)));
      return;
    }
    try {
      const obj = await env.BACKUP.get(key);
      if (!obj) { sendResult(false, 'GET returned null'); return; }
      const text = await obj.text();
      if (text !== payload) { sendResult(false, `readback mismatch (${text.length} vs ${payload.length} bytes)`); return; }
    } catch (e) {
      sendResult(false, 'GET failed: ' + ((e && e.message) || String(e)));
      return;
    }
    try {
      await env.BACKUP.delete(key);
    } catch (e) {
      // non-fatal: put/get confirmed the bucket works. Leftover test file gets overwritten next run.
      console.warn('r2 healthcheck delete failed:', e && e.message);
    }
    sendResult(true, 'put/get/delete ok');
  }

  async _maybeSendMissedBackupAlert() {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) return;
    if (!this.lastBackupAt) return; // never backed up -> don't alert until first success
    const now = Date.now();
    const ageMs = now - this.lastBackupAt;
    if (ageMs < 48 * 3600000) return;                              // fresh
    if (now - this.lastBackupMissedEmailAt < 24 * 3600000) return; // one per day
    this.lastBackupMissedEmailAt = now;
    const ageHours = Math.floor(ageMs / 3600000);
    try {
      await fetch('https://api.smtp2go.com/v3/email/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
        body: JSON.stringify({
          sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
          to: [env.NOTIFY_EMAIL],
          subject: `HelloESP backup overdue (${ageHours}h)`,
          text_body: `No successful backup since ${new Date(this.lastBackupAt).toISOString()}.\n\nLast snapshot date: ${this.lastBackupDate || 'unknown'}.\n\nCheck that the device is online and the R2 binding is healthy.`
        })
      });
    } catch (e) {
      console.error('missed-backup email send failed:', e && e.message);
    }
  }

  async emailBackupBundle(meta, files, originalSize) {
    const env = this.env;
    if (!env.SMTP2GO_KEY || !env.NOTIFY_EMAIL) {
      console.warn('backup bundle received but SMTP2GO_KEY or NOTIFY_EMAIL unset');
      return;
    }

    const bundle = {
      schema: 'helloesp-backup/1',
      generated_at: meta.generated_at,
      firmware: meta.firmware,
      uptime: meta.uptime,
      original_size: originalSize,
      files
    };
    const bundleJson = JSON.stringify(bundle);
    // BACKUP_PART_SIZE is a byte budget (SMTP attachment ceiling). Slicing
    // bundleJson.slice() slices by UTF-16 code units, so any non-ASCII content
    // (e.g. a guestbook entry with an em-dash) would make a part over-size or
    // split a code point. Encode once and slice the resulting byte buffer.
    const bundleBytes = new TextEncoder().encode(bundleJson);
    const date = (meta.generated_at || new Date().toISOString()).slice(0, 10);
    const sessionId = Math.random().toString(36).slice(2, 8);
    const totalParts = Math.max(1, Math.ceil(bundleBytes.length / BACKUP_PART_SIZE));
    const padWidth = String(totalParts).length;

    const skipped = files.filter(f => f.skipped);
    const included = files.filter(f => !f.skipped);

    let header = `HelloESP weekly state backup.\n\n`;
    header += `Generated:   ${meta.generated_at || 'n/a'}\n`;
    header += `Firmware:    ${meta.firmware || 'n/a'}\n`;
    header += `Uptime:      ${meta.uptime || 'n/a'}\n`;
    header += `Files:       ${included.length} included, ${skipped.length} skipped\n`;
    header += `Bundle size: ${(bundleBytes.length / 1024).toFixed(1)} KB total\n`;
    header += `Raw size:    ${(originalSize / 1024).toFixed(1)} KB (on device)\n`;
    if (totalParts > 1) header += `Parts:       ${totalParts} emails (session ${sessionId})\n`;
    header += `\n`;
    if (skipped.length) {
      header += `Skipped:\n`;
      for (const f of skipped) header += `  - ${f.name} (${f.size} bytes, ${f.skipped})\n`;
      header += `\n`;
    }

    let footer;
    if (totalParts === 1) {
      footer = `Contents are base64-encoded inside the JSON. To restore a specific file:\n`;
      footer += `  jq -r '.files[] | select(.name=="guestbook.csv") | .content_b64' backup.json | base64 -d > guestbook.csv\n`;
    } else {
      footer = `This bundle is split across ${totalParts} emails. Download every attachment,\n`;
      footer += `then reassemble and restore:\n`;
      footer += `  cat helloesp-backup-${date}-${sessionId}-part*.json > bundle.json\n`;
      footer += `  jq -r '.files[] | select(.name=="guestbook.csv") | .content_b64' bundle.json | base64 -d > guestbook.csv\n`;
      footer += `\nIf a part is missing, the device retries the full backup next week.\n`;
    }

    let sentParts = 0;
    for (let i = 0; i < totalParts; i++) {
      const bytes = bundleBytes.subarray(i * BACKUP_PART_SIZE, (i + 1) * BACKUP_PART_SIZE);
      let bin = '';
      const CHUNK = 0x8000;
      for (let j = 0; j < bytes.length; j += CHUNK) {
        bin += String.fromCharCode.apply(null, bytes.subarray(j, j + CHUNK));
      }
      const attachmentB64 = btoa(bin);

      const partNum = String(i + 1).padStart(padWidth, '0');
      const totalStr = String(totalParts).padStart(padWidth, '0');
      const filename = totalParts === 1
        ? `helloesp-backup-${date}.json`
        : `helloesp-backup-${date}-${sessionId}-part${partNum}of${totalStr}.json`;
      const subject = totalParts === 1
        ? `HelloESP backup - ${date} (${(bundleBytes.length / 1024).toFixed(0)} KB)`
        : `HelloESP backup - ${date} (part ${i + 1}/${totalParts})`;

      let body = header;
      if (totalParts > 1) {
        body += `>>> This is part ${i + 1} of ${totalParts}. Slice offset: ${i * BACKUP_PART_SIZE} of ${bundleBytes.length} bytes.\n\n`;
      }
      body += footer;

      try {
        const res = await fetch('https://api.smtp2go.com/v3/email/send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Smtp2go-Api-Key': env.SMTP2GO_KEY },
          body: JSON.stringify({
            sender: env.NOTIFY_FROM || 'HelloESP <noreply@helloesp.com>',
            to: [env.NOTIFY_EMAIL],
            subject,
            text_body: body,
            attachments: [{
              filename,
              fileblob: attachmentB64,
              mimetype: 'application/json'
            }]
          })
        });
        if (!res.ok) {
          console.error(`backup part ${i + 1}/${totalParts} SMTP2GO non-2xx:`, res.status);
          continue;
        }
        sentParts++;
      } catch (e) {
        console.error(`backup part ${i + 1}/${totalParts} failed:`, e && e.message);
      }

      if (i < totalParts - 1) await new Promise(r => setTimeout(r, BACKUP_PART_DELAY_MS));
    }

    if (sentParts === totalParts) {
      this.lastBackupAt = Date.now();
      this.lastBackupSize = bundleBytes.length;
      await this.state.storage.put('lastBackupAt', this.lastBackupAt);
      await this.state.storage.put('lastBackupSize', this.lastBackupSize);
    } else {
      console.error(`backup partial: ${sentParts}/${totalParts} parts emailed`);
    }
  }

  async alarm() {
    // lazy weather refresh: fetch on first tick, then every WEATHER_REFRESH_MS (1 hour)
    if (!this.lastWeather || Date.now() - this.lastWeather.fetched_at > WEATHER_REFRESH_MS) {
      this.refreshWeather().catch(() => {});
    }

    // dead-man's-switch: email if ESP has been silent for >24h
    this.maybeSendDeadmanAlert().catch(() => {});

    // overdue-backup alert: email if last successful backup is >48h old (once per day)
    this._maybeSendMissedBackupAlert().catch(() => {});

    // dead-client sweep: if ESP isn't pushing events, broadcasts don't prune dead SSE writers.
    // Send a zero-cost SSE comment to every client; prune any that throw.
    if (this.sseClients.size > 0) {
      const ping = new TextEncoder().encode(': ping\n\n');
      const dead = [];
      for (const w of this.sseClients) {
        w.write(ping).catch(() => dead.push(w));
      }
      for (const w of dead) this.sseClients.delete(w);
    }

    if (this.espSocket && Date.now() - this.lastActivity > 75000) {
      try { this.espSocket.close(); } catch (e) {}
      this.espSocket = null;
    }

    // Always rearm. Without this, an offline-ESP + no-SSE-clients state would stop the alarm
    // loop, and dead-man / overdue-backup alerts would be delayed until the next visitor woke
    // the DO. ~86k invocations/month is well under Worker free-tier limits. Direct setAlarm
    // here (not _ensureAlarm). This IS the rearm: we want a fresh 30s window, not to defer
    // to a farther-out existing alarm.
    await this.state.storage.setAlarm(Date.now() + 30000);
  }

  async fetch(request) {
    const url = new URL(request.url);

    // Worker-side load-shedding endpoints.
    if (url.pathname === '/ping' && request.method === 'GET') {
      const espUp = !!(this.espSocket && this.espSocket.readyState === 1);
      return new Response(espUp ? 'pong' : 'offline', {
        status: espUp ? 200 : 503,
        headers: {
          'Content-Type': 'text/plain',
          'Cache-Control': 'no-store',
          ...SEC_HEADERS
        }
      });
    }

    // /stats: serve from Worker's cached lastStats (pushed by ESP via SSE events).
    if (url.pathname === '/stats' && request.method === 'GET') {
      if (this.lastStats) {
        const age = Date.now() - this.lastStatsAt;
        return new Response(this.lastStats, {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=5, stale-while-revalidate=30',
            'X-Worker-Cache-Age': String(Math.floor(age / 1000)),
            ...SEC_HEADERS
          }
        });
      }
      // No cached stats yet; fall through to the ESP relay below.
    }

    // /countries: serve from Worker-local cache
    if (url.pathname === '/countries' && request.method === 'GET') {
      if (this.lastCountries) {
        const age = Date.now() - this.lastCountriesAt;
        const espUp = !!(this.espSocket && this.espSocket.readyState === 1);
        // Serve from cache if (a) fresh, or (b) ESP is down (stale is better than 503).
        if (age < 60000 || !espUp) {
          return new Response(this.lastCountries, {
            status: 200,
            headers: {
              'Content-Type': 'application/json',
              'Cache-Control': 'public, max-age=30, stale-while-revalidate=120',
              'X-Worker-Cache-Age': String(Math.floor(age / 1000)),
              ...SEC_HEADERS
            }
          });
        }
      }
      // Otherwise fall through to ESP relay.
    }

    // /records.json: records change only when a new high/low lands, so a
    // longer cache window is safe. Same ESP-down stale rule as /countries.
    if (url.pathname === '/records.json' && request.method === 'GET') {
      if (this.lastRecords) {
        const age = Date.now() - this.lastRecordsAt;
        const espUp = !!(this.espSocket && this.espSocket.readyState === 1);
        if (age < 300000 || !espUp) {
          return new Response(this.lastRecords, {
            status: 200,
            headers: {
              'Content-Type': 'application/json',
              'Cache-Control': 'public, max-age=60, stale-while-revalidate=600',
              'X-Worker-Cache-Age': String(Math.floor(age / 1000)),
              ...SEC_HEADERS
            }
          });
        }
      }
    }

    // Embeddable live status badges. Uses cached lastStats; zero ESP load.
    if (url.pathname === '/status.svg') {
      const metric = url.searchParams.get('metric') || 'uptime';
      const svg = this.buildStatusBadge(metric);
      return new Response(svg, {
        status: 200,
        headers: {
          'Content-Type': 'image/svg+xml; charset=utf-8',
          'Cache-Control': 'public, max-age=60',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/status-wide.svg') {
      const svg = this.buildStatusWide();
      return new Response(svg, {
        status: 200,
        headers: {
          'Content-Type': 'image/svg+xml; charset=utf-8',
          'Cache-Control': 'public, max-age=60',
          ...SEC_HEADERS
        }
      });
    }

    // SSE fanout: browsers connect here and receive push updates the ESP sends via WS
    if (url.pathname === '/_stream') {
      // Cap concurrent SSE writers. Without this a flood could hoard DO memory (each writer
      // holds stream buffer state). 503 tells well-behaved clients to back off and retry.
      if (this.sseClients.size >= SSE_MAX_CLIENTS) {
        return new Response('Too many live connections, try again shortly', {
          status: 503,
          headers: { 'Content-Type': 'text/plain', 'Retry-After': '30', ...SEC_HEADERS }
        });
      }
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      this.sseClients.add(writer);
      // make sure the alarm is ticking so dead-client sweeps run
      this._ensureAlarm(30000);
      // immediately send the most recent cached stats so new clients don't wait 5s.
      // Inject a fresh `clients` count since lastStats doesn't carry it.
      if (this.lastStats) {
        let snapshot = this.lastStats;
        try {
          const obj = JSON.parse(this.lastStats);
          obj.clients = this.sseClients.size;
          snapshot = JSON.stringify(obj);
        } catch (e) {}
        const payload = new TextEncoder().encode(`event: stats\ndata: ${snapshot}\n\n`);
        writer.write(payload).catch(() => this.sseClients.delete(writer));
      } else {
        // send a zero-length comment so the connection is established promptly
        writer.write(new TextEncoder().encode(': connected\n\n')).catch(() => this.sseClients.delete(writer));
      }
      return new Response(readable, {
        status: 200,
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-store',
          'X-Accel-Buffering': 'no',
          ...SEC_HEADERS
        }
      });
    }

    if (url.pathname === '/_ws' && request.headers.get('Upgrade') === 'websocket') {
      const wsIP = request.headers.get('CF-Connecting-IP') || 'unknown';
      const now = Date.now();
      let wf = this.wsAuthFails.get(wsIP);
      if (wf && now < wf.blockedUntil) {
        return new Response('Too many failed attempts', { status: 429 });
      }

      const key = url.searchParams.get('key');
      if (!key || !timingSafeEqualStr(key, this.env.WORKER_SECRET || '')) {
        if (!wf) wf = { count: 0, firstAt: now, blockedUntil: 0 };
        if (now - wf.firstAt > 60000) { wf.count = 0; wf.firstAt = now; }
        wf.count++;
        if (wf.count >= 5) wf.blockedUntil = now + 600000;
        this.wsAuthFails.set(wsIP, wf);
        return new Response('Unauthorized', { status: 403 });
      }
      this.wsAuthFails.delete(wsIP);

      // opportunistic cleanup of stale auth-fail entries (mirrors rateLimits cleanup)
      if (this.wsAuthFails.size > 500) {
        for (const [k, v] of this.wsAuthFails) {
          if (now - v.firstAt > 600000) this.wsAuthFails.delete(k);
        }
      }

      // valid auth; take over any existing socket (covers stale sockets from unclean reboots)
      if (this.espSocket) {
        try { this.espSocket.close(); } catch (e) {}
        this.espSocket = null;
      }
      // reset auth flag; each new connection must pass HMAC (or fall back if no HMAC_SECRET)
      this.hmacAuthenticated = false;
      if (this.pendingAuth) {
        clearTimeout(this.pendingAuth.timer);
        this.pendingAuth = null;
      }
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      this.espSocket = server;
      this.lastActivity = Date.now();

      // Optional HMAC handshake. If HMAC_SECRET is set, require the ESP to
      // respond to an auth_challenge before trusting the connection.
      if (this.env.HMAC_SECRET) {
        const nonce = crypto.randomUUID();
        this.pendingAuth = {
          nonce,
          socket: server,
          timer: setTimeout(() => {
            if (this.pendingAuth && this.pendingAuth.socket === server) {
              console.error('HMAC auth timeout');
              try { server.close(); } catch (e) {}
              this.pendingAuth = null;
              if (this.espSocket === server) this.espSocket = null;
            }
          }, 5000)
        };
        server.send(JSON.stringify({ type: 'auth_challenge', nonce }));
      } else {
        console.warn('HMAC_SECRET unset; ESP WS auth is WORKER_SECRET-only (development mode).');
        this.hmacAuthenticated = true;
      }

      server.addEventListener('message', (event) => {
        this.lastActivity = Date.now();
        const data = event.data;
        if (typeof data !== 'string') return;

        // HMAC auth response. Parse first (cheap, bounded by WS frame size), then
        // type-check; avoids relying on a substring match to decide whether to parse.
        if (this.pendingAuth && this.pendingAuth.socket === server && data.charAt(0) === '{') {
          let authMsg = null;
          try { authMsg = JSON.parse(data); } catch (e) { /* not JSON; fall through */ }
          if (authMsg && authMsg.type === 'auth_response') {
            const nonce = this.pendingAuth.nonce;
            if (!authMsg.hmac) {
              try { server.send(JSON.stringify({ type: 'auth_result', ok: false, reason: 'missing hmac' })); } catch (e) {}
              try { server.close(); } catch (e) {}
              this.espSocket = null;
              clearTimeout(this.pendingAuth.timer);
              this.pendingAuth = null;
              return;
            }
            this.verifyHmac(authMsg.hmac, nonce).then((valid) => {
              if (this.espSocket !== server) return;
              if (!valid) {
                console.error('HMAC mismatch');
                try { server.send(JSON.stringify({ type: 'auth_result', ok: false, reason: 'hmac mismatch' })); } catch (e) {}
                try { server.close(); } catch (e) {}
                this.espSocket = null;
              } else {
                this.hmacAuthenticated = true;
              }
              if (this.pendingAuth && this.pendingAuth.socket === server) {
                clearTimeout(this.pendingAuth.timer);
                this.pendingAuth = null;
              }
            }).catch((e) => {
              console.error('verifyHmac error:', e && e.message);
              if (this.espSocket === server) {
                try { server.close(); } catch (e2) {}
                this.espSocket = null;
              }
              if (this.pendingAuth && this.pendingAuth.socket === server) {
                clearTimeout(this.pendingAuth.timer);
                this.pendingAuth = null;
              }
            });
            return;
          }
        }
        // Reject all other messages until HMAC verified
        if (!this.hmacAuthenticated) return;

        if (data.length === 0) {
          if (this.currentStreamId === null) return;
          const ar = this.activeResponses.get(this.currentStreamId);
          if (!ar) { this.currentStreamId = null; return; }

          const totalLen = ar.chunks.reduce((s, c) => s + c.length, 0);
          const assembled = new Uint8Array(totalLen);
          let offset = 0;
          for (const chunk of ar.chunks) {
            assembled.set(chunk, offset);
            offset += chunk.length;
          }

          const headers = { 'Content-Type': ar.ct };
          if (ar.cc) headers['Cache-Control'] = ar.cc;

          const pending = this.pendingRequests.get(ar.id);
          if (pending) {
            // Cache-fill hook: stash selected endpoint responses in Worker memory.
            if (pending.path === '/countries' && ar.status === 200) {
              try {
                this.lastCountries = new TextDecoder().decode(assembled);
                this.lastCountriesAt = Date.now();
              } catch (e) {}
            } else if (pending.path === '/records.json' && ar.status === 200) {
              try {
                this.lastRecords = new TextDecoder().decode(assembled);
                this.lastRecordsAt = Date.now();
              } catch (e) {}
            }
            pending.resolve(new Response(assembled.buffer, {
              status: ar.status,
              headers
            }));
            this.pendingRequests.delete(ar.id);
          }
          this.activeResponses.delete(this.currentStreamId);
          this.currentStreamId = null;
          return;
        }

        if (data.charAt(0) === '{') {
          try {
            const msg = JSON.parse(data);
            if (msg.type === 'event') {
              this.handleEvent(msg).catch((e) => console.error('handleEvent error:', e && e.message));
              return;
            }
            this.activeResponses.set(msg.id, {
              id: msg.id,
              status: msg.status || 200,
              ct: msg.ct || 'text/html',
              cc: msg.cc || '',
              chunks: []
            });
            this.currentStreamId = msg.id;
            return;
          } catch (e) {
            console.warn('malformed stream metadata frame:', e && e.message);
          }
        }

        if (this.currentStreamId !== null) {
          const ar = this.activeResponses.get(this.currentStreamId);
          if (ar) {
            try {
              ar.chunks.push(b64ToBytes(data));
            } catch (e) {
              console.error('b64 decode error:', e && e.message);
            }
          }
        }
      });

      server.addEventListener('close', () => {
        // Only clear state if THIS socket is still the active one.
        // A takeover reconnect may have already replaced us with a new socket.
        if (this.espSocket !== server) return;
        this.espSocket = null;
        this.hmacAuthenticated = false;
        if (this.pendingAuth && this.pendingAuth.socket === server) {
          clearTimeout(this.pendingAuth.timer);
          this.pendingAuth = null;
        }
        for (const p of this.pendingRequests.values()) {
          p.resolve(offlineResponse());
        }
        this.pendingRequests.clear();
        this.activeResponses.clear();
        this.currentStreamId = null;
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // maintenance window takes precedence over offline, so planned work shows the right page
    if (Date.now() < this.maintenanceUntil) {
      return maintenanceResponse(this.maintenanceUntil, this.maintenanceMessage);
    }

    // curl / wget / httpie landing on / get a text/plain ASCII card pulled
    // from the cached stats. Zero ESP load, works when the device is down.
    if (request.method === 'GET' && (url.pathname === '/' || url.pathname === '')) {
      const ua = (request.headers.get('User-Agent') || '').toLowerCase();
      const wantsText = /\b(curl|wget|httpie|libwww-perl|powershell)\b/.test(ua);
      if (wantsText) {
        return new Response(this.buildCurlCard(), {
          status: 200,
          headers: {
            'Content-Type': 'text/plain; charset=utf-8',
            'Cache-Control': 'public, max-age=30',
            ...SEC_HEADERS
          }
        });
      }
    }

    // Short-circuit /stats from the Worker-side cache. lastStats is refreshed every 5s by the
    // ESP's SSE push, so it's always ≤5s stale in practice. Staleness guard falls back to the
    // relay path (which returns offline if the ESP is down) so a dead device doesn't silently
    // serve minute-old cached data.
    if (request.method === 'GET' && url.pathname === '/stats' && this.lastStats
        && Date.now() - this.lastStatsAt < 60000) {
      return new Response(this.lastStats, {
        status: 200,
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-store',
          ...SEC_HEADERS
        }
      });
    }

    if (!this.espSocket || !this.hmacAuthenticated) return offlineResponse();

    const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';

    const now = Date.now();
    let rl = this.rateLimits.get(clientIP);
    if (!rl || now > rl.resetAt) {
      rl = { count: 0, resetAt: now + RATE_LIMIT_WINDOW };
      this.rateLimits.set(clientIP, rl);
    }
    rl.count++;
    if (rl.count > RATE_LIMIT_MAX) {
      return new Response('Rate limit exceeded', {
        status: 429,
        headers: { 'Content-Type': 'text/plain', 'Retry-After': '60', ...SEC_HEADERS }
      });
    }
    if (this.rateLimits.size > 500) {
      for (const [k, v] of this.rateLimits) {
        if (v.resetAt < now) this.rateLimits.delete(k);
      }
    }

    const id = ++this.requestId;

    // request.cf.country is the canonical source inside a Worker; the CF-IPCountry HTTP header
    // isn't forwarded to Workers by default, so reading it via headers.get always returned '' and
    // every relayed request showed up as ?? in the console.
    const headers = {
      'CF-Connecting-IP': clientIP,
      'CF-IPCountry': (request.cf && request.cf.country) || request.headers.get('CF-IPCountry') || ''
    };

    let body = '';
    if (request.method === 'POST') {
      const cl = request.headers.get('Content-Length');
      if (cl && parseInt(cl, 10) > MAX_BODY) {
        return new Response('Payload too large', { status: 413, headers: { 'Content-Type': 'text/plain', ...SEC_HEADERS } });
      }
      // Stream the body so a missing or lying Content-Length can't slip a huge
      // payload through and then get fully buffered by request.text(). Cancel
      // the stream the moment we exceed MAX_BODY.
      if (request.body) {
        const reader = request.body.getReader();
        const decoder = new TextDecoder();
        let received = 0;
        let parts = '';
        let tooLarge = false;
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          received += value.byteLength;
          if (received > MAX_BODY) {
            tooLarge = true;
            try { await reader.cancel(); } catch (e) {}
            break;
          }
          parts += decoder.decode(value, { stream: true });
        }
        if (!tooLarge) parts += decoder.decode();
        if (tooLarge) {
          return new Response('Payload too large', { status: 413, headers: { 'Content-Type': 'text/plain', ...SEC_HEADERS } });
        }
        body = parts;
      }
    }

    try {
      this.espSocket.send(JSON.stringify({
        id,
        method: request.method,
        path: url.pathname + url.search,
        headers,
        body
      }));
    } catch (e) {
      this.espSocket = null;
      return offlineResponse();
    }

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        this.activeResponses.delete(id);
        if (this.currentStreamId === id) this.currentStreamId = null;
        resolve(timeoutResponse());
      }, 30000);

      this.pendingRequests.set(id, {
        path: url.pathname,
        resolve: (resp) => {
          clearTimeout(timeout);
          resolve(resp);
        }
      });
    });
  }
}

// /guestbook/entries is cacheable on purpose: firmware sends Cache-Control:
// max-age=30 on the unfiltered list (search skips the header). Letting the
// edge cache honor that header means one ESP round-trip per 30s regardless
// of visitor count.
const NO_CACHE = ['/logs', '/admin', '/_ws', '/_stream', '/console.json',
  '/guestbook/submit', '/guestbook/pending', '/guestbook/moderate'];

function shouldCache(pathname) {
  if (pathname === '/stats') return false;
  return !NO_CACHE.some(p => pathname.startsWith(p));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    const cacheable = request.method === 'GET' && shouldCache(url.pathname);

    if (cacheable) {
      const cached = await caches.default.match(request);
      if (cached) return cached;
    }

    const id = env.ESP_RELAY.idFromName('main');
    const relay = env.ESP_RELAY.get(id);
    const response = await relay.fetch(request);

    // 101 WebSocket upgrades must be returned as-is (can't reconstruct or add headers)
    if (response.status === 101) return response;

    if (cacheable && (response.status === 200 || response.status === 404)) {
      const cc = response.headers.get('Cache-Control');
      if (cc && cc.includes('max-age')) {
        const body = await response.arrayBuffer();
        const headers = applySecHeaders(new Headers(response.headers));
        headers.set('Cache-Control', cc);

        const cacheResp = new Response(body, { status: response.status, headers });
        await caches.default.put(request, cacheResp.clone());
        return cacheResp;
      }
    }

    const passHeaders = applySecHeaders(new Headers(response.headers));
    return new Response(response.body, { status: response.status, headers: passHeaders });
  }
};
