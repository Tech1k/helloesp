// Copyright (c) 2022-2026 Kristian Kramer (Tech1k)
// Distributed under the MIT software license

#include "WiFi.h"
#include "ESPAsyncWebServer.h"
#include "uptime_formatter.h"
#include "uptime.h"
#include "Wire.h"
#include "Adafruit_Sensor.h"
#include "Adafruit_BME280.h"
#include "Adafruit_GFX.h"
#include "Adafruit_SSD1306.h"
#include "Adafruit_CCS811.h"
#include "RTClib.h"
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include "time.h"
#include "Update.h"
#include "esp_system.h"
#include "esp_task_wdt.h"
#include "mbedtls/md.h"
#include "mbedtls/sha256.h"
#include <WiFiClientSecure.h>
#include <ESPmDNS.h>
#include <HTTPClient.h>
#include <atomic>

// Bump the Arduino loop task stack from the 8KB default to 12KB. The
// chronicle_seal handler combined with SD.rename's tmp/bak orchestration
// and JSON unescape can spike past 8KB on receipt of an entry, tripping
// the stack canary and rebooting.
SET_LOOP_TASK_STACK_SIZE(12 * 1024);

// Version
#define FIRMWARE_VERSION "1.4"

// Pins
#define SD_CS    5
#define LED_PIN       33
#define NOTIF_LED_PIN 32

// Config
#define SEALEVELPRESSURE_HPA 1013.25
#define LED_ON_TIME          200

char cfgSsid[64]       = "";
char cfgWifiPass[64]   = "";
char cfgAdminUser[32]  = "admin";
char cfgAdminPass[64]  = "";
char cfgWorkerUrl[128] = "";
char cfgWorkerKey[128] = "";
char cfgDeviceKey[128] = "";
char cfgTimezone[48]   = "UTC0";
// Optional Shelly Gen 2+ smart plug for power monitoring. Polled every
// SHELLY_POLL_INTERVAL_MS for `apower` (W); we integrate ourselves over
// time to derive Wh totals. Leave blank to disable; the homepage's
// power banner simply hides if unset. Disable LAN auth on the Shelly
// (Settings → Security) for the integration to work without Digest auth.
char cfgShellyUrl[96]  = "";
// Optional grid energy cost in $/kWh (e.g. 0.13 for 13¢/kWh). Used to
// derive cost displays for monthly/yearly/lifetime energy. 0 = don't
// display cost anywhere. Sub-penny daily costs are skipped regardless.
float cfgCostPerKwh    = 0.0f;
// Optional grid carbon intensity in kg CO₂/kWh (e.g. 0.485 for the
// Colorado mix; varies ~0.01 in Iceland to ~0.73 in coal-heavy grids).
// Used to derive lifetime carbon displays on the homepage banner and
// /history records. 0 = don't display CO₂ anywhere.
float cfgCo2PerKwh     = 0.0f;
// When true, all non-admin LAN hits to public pages (/, /guestbook, /console,
// /history, /stats, etc.) are 302-redirected to https://<cfgWorkerUrl>/<path>
// regardless of WS connection state. This routes LAN visitors through the
// Worker's edge cache, eliminating the LAN-burst-vs-WS-write collision
// pattern that triggers the cascade. /admin is always served direct on
// LAN regardless. Edit /config.txt via the admin file editor to flip.
//
// Why "regardless of WS state": basing this on `wsConnected` would flip-
// flop during transient WS reconnects, giving inconsistent UX. The
// persisted flag stays the same across reconnects.
bool cfgWorkerExclusive = false;

const long  gmtOffset_sec      = 0;
const int   daylightOffset_sec = 0;

// Globals
AsyncWebServer server(80);
WiFiClientSecure wsClient;
bool wsConnected = false;
unsigned long lastWsAttempt = 0;
unsigned long lastWsPing = 0;
unsigned long lastWsActivity = 0;
int wsReconnectFails = 0;
int wsFastFails = 0;  // connectWorker returns in <1s = socket-layer failure, not network
// Escalation level for the current WS-down period. 0=just retrying, 1=did
// WiFi.reconnect (refresh AP association). Reset to 0 on successful connect.
int wsEscalationLevel = 0;
unsigned long wsDisconnectedSince = 0;  // 120s safety net timer, reset on successful reconnect
#define WS_RECONNECT_MS 5000
#define WS_PING_MS 30000
#define WS_TIMEOUT_MS 60000

// Admin auth bruteforce lockout (per-IP)
#define AUTH_MAX_FAILS     5
#define AUTH_LOCKOUT_MS    600000UL   // 10 min
#define AUTH_TRACK_SIZE    8

#define SHELLY_POLL_INTERVAL_MS 30000UL
#define SHELLY_STALE_MS         180000UL  // hide banner if no Shelly read in 3 min

// minimal WebSocket frame helpers
void wsSendText(WiFiClientSecure& client, const String& msg) {
    if (!client.connected()) { wsConnected = false; return; }
    size_t len = msg.length();
    uint8_t header[10];
    int headerLen = 0;

    header[0] = 0x81; // text frame, fin
    if (len < 126) {
        header[1] = len | 0x80;
        headerLen = 2;
    } else if (len < 65536) {
        header[1] = 126 | 0x80;
        header[2] = (len >> 8) & 0xFF;
        header[3] = len & 0xFF;
        headerLen = 4;
    } else {
        header[1] = 127 | 0x80;
        memset(header + 2, 0, 4);
        header[6] = (len >> 24) & 0xFF;
        header[7] = (len >> 16) & 0xFF;
        header[8] = (len >> 8) & 0xFF;
        header[9] = len & 0xFF;
        headerLen = 10;
    }

    // Check each write: a short write would desync framing (next frame starts mid-header and
    // the Worker would disconnect anyway). Detect here and flip wsConnected so the reconnect
    // path runs cleanly instead of letting the next send produce corrupt frames.
    uint8_t mask[4] = { 0x12, 0x34, 0x56, 0x78 };
    if ((int)client.write(header, headerLen) != headerLen) { wsConnected = false; return; }
    if ((int)client.write(mask, 4) != 4)                   { wsConnected = false; return; }

    const char* data = msg.c_str();
    uint8_t chunk[1024];
    size_t sent = 0;
    while (sent < len) {
        size_t chunkLen = min((size_t)1024, len - sent);
        for (size_t i = 0; i < chunkLen; i++) {
            chunk[i] = data[sent + i] ^ mask[(sent + i) % 4];
        }
        if (client.write(chunk, chunkLen) != chunkLen) { wsConnected = false; return; }
        sent += chunkLen;
    }
}

// read a WebSocket frame, returns payload as String
String wsRead(WiFiClientSecure& client) {
    if (!client.available()) return "";

    uint8_t b0 = client.read();
    uint8_t b1 = client.read();
    bool masked = b1 & 0x80;
    size_t len = b1 & 0x7F;

    if (len == 126) {
        len = ((size_t)client.read() << 8) | client.read();
    } else if (len == 127) {
        // skip upper 4 bytes
        for (int i = 0; i < 4; i++) client.read();
        len = ((size_t)client.read() << 24) | ((size_t)client.read() << 16) |
              ((size_t)client.read() << 8) | client.read();
    }

    // Sanity cap: a malformed or hostile peer (or MitM, since outbound WSS
    // uses setInsecure()) could declare a multi-MB frame and trigger a
    // String::reserve() that exhausts heap. The Worker only ever sends small
    // metadata + base64-chunked bodies; 64 KB is well above the largest
    // legitimate frame.
    if (len > 65536) return "";

    uint8_t mask[4] = {0};
    if (masked) {
        for (int i = 0; i < 4; i++) mask[i] = client.read();
    }

    String result;
    result.reserve(len + 1);
    unsigned long readStart = millis();
    for (size_t i = 0; i < len; i++) {
        while (!client.available()) {
            if (millis() - readStart > 5000 || !client.connected()) {
                if (wsConnected) {
                    Serial.printf("[ws] read aborted: %s at %u/%u (free=%u, largest=%u)\n",
                                  (millis() - readStart > 5000) ? "timeout" : "client disconnected",
                                  (unsigned)i, (unsigned)len,
                                  (unsigned)ESP.getFreeHeap(), (unsigned)ESP.getMaxAllocHeap());
                }
                wsConnected = false;
                return "";
            }
            delay(1);
        }
        uint8_t c = client.read();
        if (masked) c ^= mask[i % 4];
        result += (char)c;
    }

    // check opcode
    uint8_t opcode = b0 & 0x0F;
    if (opcode == 0x08) {
        // close frame: CF proactively closing our WS. Often tells us
        // something upstream is unhappy (keepalive miss, Worker restart,
        // rate limit, etc). Logging lets us correlate with what the device
        // was doing at the time.
        if (wsConnected) {
            Serial.printf("[ws] close frame from peer (free=%u, largest=%u)\n",
                          (unsigned)ESP.getFreeHeap(), (unsigned)ESP.getMaxAllocHeap());
        }
        wsConnected = false;
        return "";
    }
    if (opcode == 0x09) {
        // ping: send pong
        uint8_t pongHeader[2] = { 0x8A, 0x80 };
        uint8_t pongMask[4] = { 0, 0, 0, 0 };
        client.write(pongHeader, 2);
        client.write(pongMask, 4);
        return "";
    }
    if (opcode == 0x0A) {
        // pong
        lastWsActivity = millis();
        return "";
    }

    lastWsActivity = millis();
    return result;
}
Adafruit_BME280 bme;
Adafruit_CCS811 ccs;
Adafruit_SSD1306 display = Adafruit_SSD1306(128, 64, &Wire);
RTC_DS3231 rtc;
bool rtcOk = false;
bool rtcLostPowerAtBoot = false; // latched at boot; survives the rtc.adjust() that clears the live flag
uint32_t sdSpeedHz = 0;          // captured at mount time; 0 if SD never mounted
bool oledOk = false;             // captured at display.begin(); cleared on init failure

// All cached sensor values + degraded-at timestamps are written from the
// main loop and read from HTTP handlers running on the AsyncTCP task
// (separate FreeRTOS task, often pinned to the other core). Without
// `volatile` the compiler may cache reads across loop iterations, making
// HTTP responses go stale indefinitely. 32-bit aligned reads on Xtensa
// are atomic at the instruction level so torn reads aren't a concern,
// only re-reading from memory.
volatile uint16_t cached_co2 = 0;
volatile uint16_t cached_voc = 0;
volatile float    cached_temp_c       = 20.0f;
volatile float    cached_humidity     = 50.0f;
volatile float    cached_pressure_hpa = 1013.25f;
volatile float    cached_altitude_ft  = 0.0f;
volatile unsigned long lastBmeGoodAt = 0;
volatile unsigned long lastCcsGoodAt = 0;
#define SENSOR_STALE_MS  120000UL

// Optional Shelly power-monitoring data. Polled from a Gen 2+ smart plug
// upstream of the device; we only read `apower` (instantaneous W) and
// integrate ourselves over time to derive Wh totals. Independent of the
// Shelly's internal aenergy counter so resets/firmware quirks on the
// Shelly side don't corrupt our running totals. Phase 1 keeps these in
// RAM only; reboots zero the period accumulators (acceptable for a
// "stays up for years" device that rarely reboots).
volatile float    cached_power_w        = NAN;
// Per-interval sum + count for averaging the CSV power_w column over each
// 5-min logStats window rather than snapshotting only the last poll. With
// 10 polls per interval, snapshotting throws away 9 readings and misses
// any sub-interval spike that settles by log time. Averaging gives a
// representative value that reflects every successful sample. Reset by
// logStats() after each write.
volatile float    interval_power_sum    = 0.0f;
volatile uint16_t interval_power_count  = 0;
volatile float    lifetime_energy_wh    = 0.0f;
volatile float    today_energy_wh       = 0.0f;
volatile float    week_energy_wh        = 0.0f;
volatile float    month_energy_wh       = 0.0f;
volatile float    year_energy_wh        = 0.0f;
volatile unsigned long lastShellyOk          = 0;
volatile unsigned long lastShellyAttemptMs   = 0;
volatile unsigned long lastShellyIntegrationMs = 0;
volatile uint32_t shellyConsecutiveFailures  = 0;
// Last poll outcome for /admin/info diagnostics. 0 = never tried,
// 200 = OK, other positive = HTTP status (e.g. 401, 404, 500), negative
// HTTPClient transport codes (-1 connection refused, -11 read timeout,
// etc.), -100 = JSON parse failure, -101 = value out of sanity bounds.
volatile int      lastShellyHttpStatus       = 0;
char shelly_today_date[11] = "";  // YYYY-MM-DD captured at last today rollover
uint32_t shelly_tracking_started_unix = 0; // when we first saw a Shelly poll succeed

// BME280 datasheet operating ranges. Out-of-spec reads almost always indicate
// I2C bus glitches (corrupted register reads from bus contention or wire
// disturbance), not real environmental extremes; the chip can't measure
// outside these bounds. Reject the read so cached_* stays at its last good
// value instead of latching garbage.
#define BME_TEMP_MIN_C    -40.0f
#define BME_TEMP_MAX_C     85.0f
#define BME_HUM_MIN        0.0f
#define BME_HUM_MAX        100.0f
#define BME_PRESS_MIN_HPA  300.0f
#define BME_PRESS_MAX_HPA  1100.0f

// Forward decl: logError lives much further down but the sensor health
// helpers below need it.
void logError(const char* tag, const char* msg);

// Per-sensor graceful-failure tracker. When a sensor produces no good read
// for SENSOR_RETIRE_THRESHOLD consecutive logStats cycles (~2.5 hours at
// the 5-min interval), the chip declares it retired: stops attempting
// reads, persists the retirement date, and emits a chronicle event so
// "the day the chip went blind to humidity" becomes part of the archive.
// Threshold is intentionally conservative: survives I2C glitches, CCS811
// boot warm-up, transient bus contention. Owner can un-retire from the
// admin panel after replacing/fixing a sensor. Existing bmeDegraded() /
// ccsDegraded() timestamp-based degradation logic is unchanged; retire
// sits on top as a stronger persistent signal.
struct SensorHealth {
    uint32_t consecutive_bad;
    bool     retired;
    uint32_t retired_unix;  // 0 = never retired
};
SensorHealth bmeHealth = { 0, false, 0 };
SensorHealth ccsHealth = { 0, false, 0 };
#define SENSOR_RETIRE_THRESHOLD 30
#define SENSOR_HEALTH_PATH      "/sensor_health.json"

static float safeBmeTemp() {
    if (bmeHealth.retired) return cached_temp_c;
    float t = bme.readTemperature();
    if (!isnan(t) && t >= BME_TEMP_MIN_C && t <= BME_TEMP_MAX_C) {
        cached_temp_c = t; lastBmeGoodAt = millis();
    }
    return cached_temp_c;
}
static float safeBmeHumidity() {
    if (bmeHealth.retired) return cached_humidity;
    float h = bme.readHumidity();
    if (!isnan(h) && h >= BME_HUM_MIN && h <= BME_HUM_MAX) {
        cached_humidity = h; lastBmeGoodAt = millis();
    }
    return cached_humidity;
}
static float safeBmePressureHpa() {
    if (bmeHealth.retired) return cached_pressure_hpa;
    float p = bme.readPressure() / 100.0f;
    if (!isnan(p) && p >= BME_PRESS_MIN_HPA && p <= BME_PRESS_MAX_HPA) {
        cached_pressure_hpa = p; lastBmeGoodAt = millis();
    }
    return cached_pressure_hpa;
}
static float safeBmeAltitudeFt() {
    // Derive altitude from the already-bounded cached_pressure_hpa rather
    // than calling bme.readAltitude(), which does its own internal raw
    // pressure read separate from safeBmePressureHpa() and can glitch to
    // 0 ft when that internal read returns garbage. Formula is the same
    // barometric estimation Adafruit_BME280 uses internally.
    float p = cached_pressure_hpa;
    if (p < BME_PRESS_MIN_HPA || p > BME_PRESS_MAX_HPA) return cached_altitude_ft;
    float a_meters = 44330.0f * (1.0f - powf(p / SEALEVELPRESSURE_HPA, 0.1903f));
    float a_ft = a_meters * 3.28084f;
    if (!isnan(a_ft) && a_ft > -1500.0f && a_ft < 30000.0f) {
        cached_altitude_ft = a_ft;
    }
    return cached_altitude_ft;
}

// Treat a sensor that has NEVER produced a good read as degraded once we're
// past the boot-grace window. Without this, a sensor that fails to init at
// boot reports cached seed values (20.0°C / 50% / etc.) with degraded=false
// indefinitely, so the admin UI / OLED show happy-path numbers on a sensor
// that has never actually worked.
#define SENSOR_BOOT_GRACE_MS 60000UL
static bool bmeDegraded() {
    if (lastBmeGoodAt > 0) return (millis() - lastBmeGoodAt) > SENSOR_STALE_MS;
    return millis() > SENSOR_BOOT_GRACE_MS;
}
static bool ccsDegraded() {
    if (lastCcsGoodAt > 0) return (millis() - lastCcsGoodAt) > SENSOR_STALE_MS;
    return millis() > SENSOR_BOOT_GRACE_MS;
}

// Persisted sensor health: simple JSON file with per-sensor retire state.
// Atomic tmp+bak+rename pattern matches the rest of the persisted state on
// SD. Fields are minimal: consecutive_bad isn't persisted (resets on reboot
// and re-accumulates if the sensor is still bad), only the retired flag +
// timestamp need to survive across boots so chronicle isn't double-emitted.
static void loadSensorHealth() {
    if (!SD.exists(SENSOR_HEALTH_PATH)) return;
    File f = SD.open(SENSOR_HEALTH_PATH, FILE_READ);
    if (!f) return;
    String s;
    s.reserve(f.size() + 1);
    while (f.available()) s += (char)f.read();
    f.close();
    auto skipSpace = [&](int p) {
        while (p < (int)s.length() && (s[p] == ' ' || s[p] == '\t')) p++;
        return p;
    };
    auto parseSensor = [&](const char* key, SensorHealth& h) {
        int kPos = s.indexOf(String("\"") + key + "\"");
        if (kPos < 0) return;
        int retPos = s.indexOf("\"retired\":", kPos);
        if (retPos > 0 && retPos < kPos + 200) {
            int v = skipSpace(retPos + 10);
            h.retired = (s.indexOf("true", v) == v);
        }
        int unixPos = s.indexOf("\"retired_unix\":", kPos);
        if (unixPos > 0 && unixPos < kPos + 200) {
            int valStart = skipSpace(unixPos + 15);
            int valEnd = valStart;
            while (valEnd < (int)s.length() && (s[valEnd] >= '0' && s[valEnd] <= '9')) valEnd++;
            if (valEnd > valStart) h.retired_unix = (uint32_t)s.substring(valStart, valEnd).toInt();
        }
    };
    parseSensor("bme280", bmeHealth);
    parseSensor("ccs811", ccsHealth);
    if (bmeHealth.retired) Serial.println("[sensor] BME280 retired (loaded from SD)");
    if (ccsHealth.retired) Serial.println("[sensor] CCS811 retired (loaded from SD)");
}

static void saveSensorHealth() {
    String json;
    json.reserve(256);
    json  = "{\n  \"bme280\": {\"retired\": ";
    json += (bmeHealth.retired ? "true" : "false");
    json += ", \"retired_unix\": ";
    json += String(bmeHealth.retired_unix);
    json += "},\n  \"ccs811\": {\"retired\": ";
    json += (ccsHealth.retired ? "true" : "false");
    json += ", \"retired_unix\": ";
    json += String(ccsHealth.retired_unix);
    json += "}\n}\n";
    String tmp = String(SENSOR_HEALTH_PATH) + ".tmp";
    String bak = String(SENSOR_HEALTH_PATH) + ".bak";
    File f = SD.open(tmp.c_str(), FILE_WRITE);
    if (!f) { logError("sensor", "saveSensorHealth: open .tmp failed"); return; }
    f.print(json);
    f.close();
    if (SD.exists(bak.c_str())) SD.remove(bak.c_str());
    if (SD.exists(SENSOR_HEALTH_PATH)) SD.rename(SENSOR_HEALTH_PATH, bak.c_str());
    if (!SD.rename(tmp.c_str(), SENSOR_HEALTH_PATH)) {
        logError("sensor", "saveSensorHealth: rename failed");
        if (SD.exists(bak.c_str())) SD.rename(bak.c_str(), SENSOR_HEALTH_PATH);
        return;
    }
    if (SD.exists(bak.c_str())) SD.remove(bak.c_str());
}

// Emit chronicle event so the worker can mark the day as "the chip went
// blind to <sensor>". Worker stores under a dedicated DO key so the next
// chronicle seal can surface it. Fires once per retire transition.
static void emitChronicleSensorRetired(const char* sensor, uint32_t retireUnix) {
    if (!wsConnected || !wsClient.connected()) return;
    String msg;
    msg.reserve(160);
    msg  = "{\"type\":\"event\",\"event\":\"chronicle_sensor_retired\",\"data\":{\"sensor\":\"";
    msg += sensor;
    msg += "\",\"unix\":";
    msg += String(retireUnix);
    msg += "}}";
    wsSendText(wsClient, msg);
}

// Called at the end of each logStats cycle (every 5 min). Increments
// per-sensor consecutive_bad if degraded this cycle, resets on a healthy
// cycle. Crosses SENSOR_RETIRE_THRESHOLD → retire transition: persist,
// log, and emit chronicle event. Only fires on the transition itself, so
// a retired sensor doesn't re-emit on every subsequent cycle.
static void evalSensorHealth() {
    bool dirty = false;
    auto eval = [&](SensorHealth& h, bool degradedNow, const char* name) {
        if (h.retired) return;
        if (degradedNow) {
            h.consecutive_bad++;
            if (h.consecutive_bad >= SENSOR_RETIRE_THRESHOLD) {
                h.retired = true;
                time_t now = time(nullptr);
                h.retired_unix = (now > 0) ? (uint32_t)now : 0;
                dirty = true;
                logError("sensor", (String(name) + " RETIRED after "
                    + String(h.consecutive_bad) + " consecutive bad cycles").c_str());
                emitChronicleSensorRetired(name, h.retired_unix);
            }
        } else {
            h.consecutive_bad = 0;
        }
    };
    eval(bmeHealth, bmeDegraded(), "BME280");
    eval(ccsHealth, ccsDegraded(), "CCS811");
    if (dirty) saveSensorHealth();
}

volatile bool          ledOn               = false;
volatile unsigned long lastRequestTime     = 0;
int                    lastLoggedMinute    = -1;
int                    displayPage         = 0;

struct PeriodStats {
    uint32_t visitors;
    uint32_t guestbook;
    uint32_t peak_reqs;
    float    temp_min_c, temp_max_c;
    double   temp_sum_c;
    float    hum_min, hum_max;
    double   hum_sum;
    uint16_t co2_min, co2_max;
    uint32_t co2_sum;
    uint16_t voc_min, voc_max;
    uint32_t voc_sum;
    uint32_t samples;
    uint32_t started_unix;
};

PeriodStats currentWeek;
PeriodStats currentMonth;
PeriodStats currentYear;
char lastWeekLabel[9]  = "";
char lastMonthLabel[8] = "";
char lastYearLabel[5]  = "";
unsigned long lastCheckpointMs = 0;
#define CHECKPOINT_INTERVAL_MS 900000UL

static void resetPeriod(PeriodStats& p) {
    p.visitors = 0;
    p.guestbook = 0;
    p.peak_reqs = 0;
    p.temp_min_c = 1000.0f;
    p.temp_max_c = -1000.0f;
    p.temp_sum_c = 0.0;
    p.hum_min = 1000.0f;
    p.hum_max = -1000.0f;
    p.hum_sum = 0.0;
    p.co2_min = 0xFFFF;
    p.co2_max = 0;
    p.co2_sum = 0;
    p.voc_min = 0xFFFF;
    p.voc_max = 0;
    p.voc_sum = 0;
    p.samples = 0;
    struct tm now;
    // 0ms timeout; default is 5s and this runs before NTP sync
    p.started_unix = getLocalTime(&now, 0) ? (uint32_t)mktime(&now) : 0;
}

static void aggregateSample(PeriodStats& p, float temp_c, float hum, uint16_t co2, uint16_t voc, uint32_t reqs) {
    // Skip the entire sample if either float arrived as NaN. NaN compares
    // false against everything, so the min/max guards wouldn't trip, but
    // `p.temp_sum_c += NaN` poisons the running average for the whole period
    // (every subsequent sample would then divide-by-NaN at periodToJson time).
    if (isnan(temp_c) || isnan(hum)) return;
    if (temp_c < p.temp_min_c) p.temp_min_c = temp_c;
    if (temp_c > p.temp_max_c) p.temp_max_c = temp_c;
    p.temp_sum_c += temp_c;
    if (hum < p.hum_min) p.hum_min = hum;
    if (hum > p.hum_max) p.hum_max = hum;
    p.hum_sum += hum;
    if (co2 > 0 && co2 < p.co2_min) p.co2_min = co2;
    if (co2 > p.co2_max) p.co2_max = co2;
    p.co2_sum += co2;
    if (voc > 0 && voc < p.voc_min) p.voc_min = voc;
    if (voc > p.voc_max) p.voc_max = voc;
    p.voc_sum += voc;
    p.samples++;
    if (reqs > p.peak_reqs) p.peak_reqs = reqs;
}

// `energy_wh` is the period's accumulated energy from the Shelly integration
// (positive values), or a sentinel < 0 when no Shelly data is available
// (e.g., LAN-only installs, or live current-period queries before the
// first Shelly poll). Non-positive values are omitted from the JSON so
// frontends can detect the field's absence to decide whether to render
// the energy cell.
static String periodToJson(const char* label, const PeriodStats& p, float energy_wh = -1.0f) {
    char buf[768];
    int op = 0;
    op += snprintf(buf + op, sizeof(buf) - op,
        "{\"label\":\"%s\",\"visitors\":%u,\"guestbook\":%u,\"peak_reqs\":%u,\"samples\":%u",
        label, p.visitors, p.guestbook, p.peak_reqs, p.samples);
    if (p.samples > 0) {
        if (p.temp_min_c <= p.temp_max_c) {
            op += snprintf(buf + op, sizeof(buf) - op,
                ",\"temp_c\":{\"min\":%.2f,\"max\":%.2f,\"avg\":%.2f}",
                p.temp_min_c, p.temp_max_c, (float)(p.temp_sum_c / p.samples));
        }
        if (p.hum_min <= p.hum_max) {
            op += snprintf(buf + op, sizeof(buf) - op,
                ",\"humidity\":{\"min\":%.1f,\"max\":%.1f,\"avg\":%.1f}",
                p.hum_min, p.hum_max, (float)(p.hum_sum / p.samples));
        }
        if (p.co2_min != 0xFFFF && p.co2_max > 0) {
            op += snprintf(buf + op, sizeof(buf) - op,
                ",\"co2_ppm\":{\"min\":%u,\"max\":%u,\"avg\":%lu}",
                p.co2_min, p.co2_max, (unsigned long)(p.co2_sum / p.samples));
        }
        if (p.voc_min != 0xFFFF && p.voc_max > 0) {
            op += snprintf(buf + op, sizeof(buf) - op,
                ",\"voc_ppb\":{\"min\":%u,\"max\":%u,\"avg\":%lu}",
                p.voc_min, p.voc_max, (unsigned long)(p.voc_sum / p.samples));
        }
    }
    if (energy_wh > 0.0f) {
        op += snprintf(buf + op, sizeof(buf) - op, ",\"energy_wh\":%.1f", energy_wh);
    }
    snprintf(buf + op, sizeof(buf) - op, ",\"started\":%lu}", (unsigned long)p.started_unix);
    return String(buf);
}

// `energy_wh` is forwarded to periodToJson so the archived JSON includes
// the period's accumulated Shelly energy (when configured). Defaults to
// -1 (omitted) so legacy callers that don't track energy stay correct.
static bool flushPeriod(const char* dir, const char* label, const PeriodStats& p, float energy_wh = -1.0f) {
    String path = String(dir) + "/" + label + ".json";
    String tmp  = path + ".tmp";
    String bak  = path + ".bak";
    File f = SD.open(tmp, FILE_WRITE);
    if (!f) { Serial.printf("[stats] flush open failed: %s\n", path.c_str()); return false; }
    f.print(periodToJson(label, p, energy_wh));
    f.close();
    if (SD.exists(bak)) SD.remove(bak);
    if (SD.exists(path)) SD.rename(path, bak);
    if (!SD.rename(tmp, path)) {
        Serial.printf("[stats] flush rename failed: %s\n", path.c_str());
        if (SD.exists(bak)) SD.rename(bak, path);
        return false;
    }
    if (SD.exists(bak)) SD.remove(bak);
    Serial.printf("[stats] flushed %s\n", path.c_str());
    return true;
}

// Compute the calendar month (1-12) of an ISO 8601 week's Monday. Used by
// /admin/repair-periods to determine which month an archived week belongs
// to without trusting the started_unix field in the JSON (which may be 0
// in archives written before NTP synced). Same algorithm as prettyWeek()
// in history.html: Jan 4 is always in week 1, walk backward to its Monday,
// then forward (week-1)*7 days. Returns false if mktime fails (rare).
static bool isoWeekMonth(int year, int week, int& outMonth) {
    struct tm jan4 = {};
    jan4.tm_year = year - 1900;
    jan4.tm_mon  = 0;
    jan4.tm_mday = 4;
    jan4.tm_hour = 12;  // noon avoids DST-fold edge cases
    time_t jan4Time = mktime(&jan4);
    if (jan4Time == (time_t)-1) return false;
    int jan4Iso = (jan4.tm_wday == 0) ? 7 : jan4.tm_wday;  // 1=Mon..7=Sun
    time_t targetMon = jan4Time - (jan4Iso - 1) * 86400 + (long)(week - 1) * 7 * 86400;
    struct tm target;
    localtime_r(&targetMon, &target);
    outMonth = target.tm_mon + 1;
    return true;
}

// Merge `src` PeriodStats into `target`. Used by /admin/repair-periods to
// rebuild monthly/yearly counters from previously-flushed weekly archives.
// Sums where appropriate; takes extremes for min/max.
static void aggregatePeriodInto(PeriodStats& target, const PeriodStats& src) {
    target.visitors  += src.visitors;
    target.guestbook += src.guestbook;
    if (src.peak_reqs > target.peak_reqs) target.peak_reqs = src.peak_reqs;
    if (src.samples == 0) return;
    if (src.temp_min_c < target.temp_min_c) target.temp_min_c = src.temp_min_c;
    if (src.temp_max_c > target.temp_max_c) target.temp_max_c = src.temp_max_c;
    target.temp_sum_c += src.temp_sum_c;
    if (src.hum_min < target.hum_min) target.hum_min = src.hum_min;
    if (src.hum_max > target.hum_max) target.hum_max = src.hum_max;
    target.hum_sum += src.hum_sum;
    if (src.co2_min < target.co2_min) target.co2_min = src.co2_min;
    if (src.co2_max > target.co2_max) target.co2_max = src.co2_max;
    target.co2_sum += src.co2_sum;
    if (src.voc_min < target.voc_min) target.voc_min = src.voc_min;
    if (src.voc_max > target.voc_max) target.voc_max = src.voc_max;
    target.voc_sum += src.voc_sum;
    target.samples += src.samples;
}

// Parse a weekly-archive JSON file (the output of periodToJson) into a
// PeriodStats struct. Reverses avg-times-samples to recover the running sum
// so that aggregating multiple weeks into a month yields a correct overall
// avg. Returns false on parse failure. Also outputs `started` so the caller
// can determine which calendar month this week belongs to.
static bool parseWeeklyJson(const String& json, PeriodStats& out, time_t& started) {
    auto findInt = [&](const String& src, const char* key) -> long {
        int k = src.indexOf(String("\"") + key + "\":");
        if (k < 0) return 0;
        k += strlen(key) + 3;
        int end = k;
        while (end < (int)src.length()) {
            char c = src[end];
            if (c == ',' || c == '}' || c == ' ' || c == '\n') break;
            end++;
        }
        return src.substring(k, end).toInt();
    };
    auto findFloat = [&](const String& src, const char* key) -> float {
        int k = src.indexOf(String("\"") + key + "\":");
        if (k < 0) return NAN;
        k += strlen(key) + 3;
        int end = k;
        while (end < (int)src.length()) {
            char c = src[end];
            if (c == ',' || c == '}' || c == ' ' || c == '\n') break;
            end++;
        }
        return src.substring(k, end).toFloat();
    };
    auto extractBlock = [&](const char* key) -> String {
        int k = json.indexOf(String("\"") + key + "\":{");
        if (k < 0) return "";
        k += strlen(key) + 3;
        int depth = 0, end = k;
        while (end < (int)json.length()) {
            if (json[end] == '{') depth++;
            else if (json[end] == '}') { depth--; if (depth == 0) { end++; break; } }
            end++;
        }
        return json.substring(k, end);
    };

    // Required label check. Bail if the JSON isn't shaped like an archive.
    if (json.indexOf("\"label\":") < 0) return false;

    // Initialize sentinels (matches resetPeriod behavior).
    out.visitors = 0; out.guestbook = 0; out.peak_reqs = 0;
    out.temp_min_c = 1000.0f; out.temp_max_c = -1000.0f; out.temp_sum_c = 0.0;
    out.hum_min = 1000.0f; out.hum_max = -1000.0f; out.hum_sum = 0.0;
    out.co2_min = 0xFFFF; out.co2_max = 0; out.co2_sum = 0;
    out.voc_min = 0xFFFF; out.voc_max = 0; out.voc_sum = 0;
    out.samples = 0; out.started_unix = 0;

    out.visitors  = (uint32_t)findInt(json, "visitors");
    out.guestbook = (uint32_t)findInt(json, "guestbook");
    out.peak_reqs = (uint32_t)findInt(json, "peak_reqs");
    out.samples   = (uint32_t)findInt(json, "samples");
    out.started_unix = (uint32_t)findInt(json, "started");
    started = (time_t)out.started_unix;

    if (out.samples > 0) {
        String t = extractBlock("temp_c");
        if (t.length() > 0) {
            float mn = findFloat(t, "min"), mx = findFloat(t, "max"), av = findFloat(t, "avg");
            if (!isnan(mn)) out.temp_min_c = mn;
            if (!isnan(mx)) out.temp_max_c = mx;
            if (!isnan(av)) out.temp_sum_c = (double)av * out.samples;
        }
        String h = extractBlock("humidity");
        if (h.length() > 0) {
            float mn = findFloat(h, "min"), mx = findFloat(h, "max"), av = findFloat(h, "avg");
            if (!isnan(mn)) out.hum_min = mn;
            if (!isnan(mx)) out.hum_max = mx;
            if (!isnan(av)) out.hum_sum = (double)av * out.samples;
        }
        String c = extractBlock("co2_ppm");
        if (c.length() > 0) {
            long mn = (long)findFloat(c, "min"), mx = (long)findFloat(c, "max"), av = (long)findFloat(c, "avg");
            if (mn > 0) out.co2_min = (uint16_t)mn;
            if (mx > 0) out.co2_max = (uint16_t)mx;
            if (av > 0) out.co2_sum = (uint32_t)av * out.samples;
        }
        String v = extractBlock("voc_ppb");
        if (v.length() > 0) {
            long mn = (long)findFloat(v, "min"), mx = (long)findFloat(v, "max"), av = (long)findFloat(v, "avg");
            if (mn > 0) out.voc_min = (uint16_t)mn;
            if (mx >= 0) out.voc_max = (uint16_t)mx;
            if (av >= 0) out.voc_sum = (uint32_t)av * out.samples;
        }
    }
    return true;
}

#define CHECKPOINT_MAGIC 0x48455333UL
// Bump when on-disk schema changes. V1 = original PeriodStats only.
// V2 adds Shelly energy state (lifetime/today/week/month/year accumulators
// + tracking metadata). loadCheckpoint() handles V1 -> V2 upgrade in place
// (PeriodStats preserved, energy fields default to 0); other version
// mismatches reject the file rather than read shifted fields.
#define CHECKPOINT_VERSION 2

// Forward decl: saveRecords is called from updateRecords() when
// extremes change. Defined further down with the rest of the records
// system.
static void saveRecords();

// Persisted energy state. Layout chosen for natural 4-byte alignment so
// the struct serializes cleanly via direct write(). Padded today_date to
// 12 bytes (10 chars + null + 1 pad) to keep the struct a multiple of 4.
struct EnergyCheckpoint {
    float    lifetime_energy_wh;
    float    today_energy_wh;
    float    week_energy_wh;
    float    month_energy_wh;
    float    year_energy_wh;
    uint32_t tracking_started_unix;
    char     today_date[12];  // YYYY-MM-DD + null + 1 pad
};

static size_t checkpointBaseSize() {
    return sizeof(uint32_t) + sizeof(uint16_t) + sizeof(PeriodStats)*3 +
           sizeof(lastWeekLabel) + sizeof(lastMonthLabel) + sizeof(lastYearLabel);
}
static size_t checkpointV2Size() {
    return checkpointBaseSize() + sizeof(EnergyCheckpoint);
}

static void saveCheckpoint() {
    const char* path = "/stats/checkpoint.bin";
    const char* tmp  = "/stats/checkpoint.tmp";
    const char* bak  = "/stats/checkpoint.bak";
    File f = SD.open(tmp, FILE_WRITE);
    if (!f) { Serial.println("[stats] checkpoint open failed"); return; }
    uint32_t magic = CHECKPOINT_MAGIC;
    f.write((const uint8_t*)&magic, sizeof(magic));
    uint16_t version = CHECKPOINT_VERSION;
    f.write((const uint8_t*)&version, sizeof(version));
    f.write((const uint8_t*)&currentWeek, sizeof(PeriodStats));
    f.write((const uint8_t*)&currentMonth, sizeof(PeriodStats));
    f.write((const uint8_t*)&currentYear, sizeof(PeriodStats));
    f.write((const uint8_t*)lastWeekLabel, sizeof(lastWeekLabel));
    f.write((const uint8_t*)lastMonthLabel, sizeof(lastMonthLabel));
    f.write((const uint8_t*)lastYearLabel, sizeof(lastYearLabel));
    // V2 energy section
    EnergyCheckpoint ec = {};
    ec.lifetime_energy_wh     = lifetime_energy_wh;
    ec.today_energy_wh        = today_energy_wh;
    ec.week_energy_wh         = week_energy_wh;
    ec.month_energy_wh        = month_energy_wh;
    ec.year_energy_wh         = year_energy_wh;
    ec.tracking_started_unix  = shelly_tracking_started_unix;
    strncpy(ec.today_date, shelly_today_date, sizeof(ec.today_date) - 1);
    ec.today_date[sizeof(ec.today_date) - 1] = '\0';
    f.write((const uint8_t*)&ec, sizeof(ec));
    f.close();
    if (SD.exists(bak)) SD.remove(bak);
    if (SD.exists(path)) SD.rename(path, bak);
    if (!SD.rename(tmp, path)) {
        if (SD.exists(bak)) SD.rename(bak, path);
        return;
    }
    if (SD.exists(bak)) SD.remove(bak);

    // Piggyback a records.json save when Shelly is contributing to the
    // lifetime accumulator. Without this, records.json only refreshes on
    // actual record changes (hottest temp, busiest day, etc.), which
    // freeze after a few weeks; lifetime_energy_wh would silently stop
    // growing on /history. Tying it to the checkpoint cadence (15 min)
    // keeps the displayed lifetime within ~15 min of the live value.
    if (cfgShellyUrl[0] != '\0' && lifetime_energy_wh > 0.0f) {
        saveRecords();
    }
}

static void loadCheckpoint() {
    resetPeriod(currentWeek);
    resetPeriod(currentMonth);
    resetPeriod(currentYear);
    // Energy state defaults. V1 checkpoints don't have these, so leaving
    // them zeroed is the right baseline if we promote a V1 file.
    lifetime_energy_wh = 0;
    today_energy_wh    = 0;
    week_energy_wh     = 0;
    month_energy_wh    = 0;
    year_energy_wh     = 0;
    shelly_tracking_started_unix = 0;
    shelly_today_date[0] = '\0';

    bool promotedFromBak = false;
    File f = SD.open("/stats/checkpoint.bin", FILE_READ);
    if (!f) {
        if (SD.exists("/stats/checkpoint.bak")) {
            SD.rename("/stats/checkpoint.bak", "/stats/checkpoint.bin");
            f = SD.open("/stats/checkpoint.bin", FILE_READ);
            promotedFromBak = true;
        }
        if (!f) return;
    }
    size_t v1Size = checkpointBaseSize();
    size_t v2Size = checkpointV2Size();
    size_t actual = f.size();
    if (actual != v1Size && actual != v2Size) {
        // Live file has wrong size (truncated mid-write, or struct drift
        // from a downgrade). Try the .bak before giving up so a single
        // bad write doesn't lose period stats.
        f.close();
        if (!promotedFromBak && SD.exists("/stats/checkpoint.bak")) {
            File bf = SD.open("/stats/checkpoint.bak", FILE_READ);
            if (bf && (bf.size() == v1Size || bf.size() == v2Size)) {
                bf.close();
                SD.remove("/stats/checkpoint.bin");
                SD.rename("/stats/checkpoint.bak", "/stats/checkpoint.bin");
                f = SD.open("/stats/checkpoint.bin", FILE_READ);
                actual = f ? f.size() : 0;
                promotedFromBak = true;
            } else if (bf) {
                bf.close();
            }
        }
        if (!f || (actual != v1Size && actual != v2Size)) {
            if (f) f.close();
            return;  // unrecognized size = struct drift or no usable backup
        }
    }
    uint32_t magic = 0;
    uint16_t version = 0;
    f.read((uint8_t*)&magic, sizeof(magic));
    if (magic != CHECKPOINT_MAGIC) { f.close(); return; }
    f.read((uint8_t*)&version, sizeof(version));
    if (version != 1 && version != CHECKPOINT_VERSION) {
        // Unknown version; reject rather than read into shifted fields.
        Serial.printf("[stats] checkpoint version mismatch (got %u, want %u); resetting\n",
                      (unsigned)version, (unsigned)CHECKPOINT_VERSION);
        f.close();
        return;
    }
    f.read((uint8_t*)&currentWeek, sizeof(PeriodStats));
    f.read((uint8_t*)&currentMonth, sizeof(PeriodStats));
    f.read((uint8_t*)&currentYear, sizeof(PeriodStats));
    f.read((uint8_t*)lastWeekLabel, sizeof(lastWeekLabel));
    f.read((uint8_t*)lastMonthLabel, sizeof(lastMonthLabel));
    f.read((uint8_t*)lastYearLabel, sizeof(lastYearLabel));
    if (version == CHECKPOINT_VERSION && actual >= v2Size) {
        EnergyCheckpoint ec = {};
        f.read((uint8_t*)&ec, sizeof(ec));
        lifetime_energy_wh = ec.lifetime_energy_wh;
        today_energy_wh    = ec.today_energy_wh;
        week_energy_wh     = ec.week_energy_wh;
        month_energy_wh    = ec.month_energy_wh;
        year_energy_wh     = ec.year_energy_wh;
        shelly_tracking_started_unix = ec.tracking_started_unix;
        strncpy(shelly_today_date, ec.today_date, sizeof(shelly_today_date) - 1);
        shelly_today_date[sizeof(shelly_today_date) - 1] = '\0';
    }
    f.close();
    // If we recovered from .bak OR upgraded V1->V2 in memory, persist a
    // fresh dual-copy so the next crash window can fall back cleanly.
    if (promotedFromBak || version < CHECKPOINT_VERSION) saveCheckpoint();
}

// forward decl: getTimestamp is defined further down, but updateRecords below needs it
String getTimestamp();
extern time_t bootTime; // declared below; needed for uptime-days computation in updateRecords
extern char dailyVisitorsDate[11]; // declared below; needed for "Busiest day" record dating

// Hall of fame records: extremes observed over the device's lifetime.
//
// `at` is dual-format BY DESIGN, not by accident:
//   - moment-level records (highest_co2, hottest/coldest temp, longest_uptime_d)
//     store full ISO-8601 from getTimestamp() (e.g. "2026-04-27T15:30:42-0600").
//   - day-level records (most_visitors_day) store just YYYY-MM-DD because
//     "the busiest day was at 3:30 PM" doesn't mean anything.
//
// Any new consumer of `at` MUST use formatRecordDate() in history.html
// (or an equivalent dual-format-aware parser). Naive `new Date(at)` will
// parse YYYY-MM-DD as UTC midnight and display the previous day in
// negative-offset timezones.
struct RecordVal {
    float value;
    // 28 bytes: full ISO-8601 with TZ offset (e.g. "2026-04-23T17:55:00-0600")
    // is 24 chars + null = 25 bytes; the previous size of 24 silently truncated
    // the last digit of the offset, breaking JS Date.parse on the frontend.
    char  at[28];
    bool  set;
};
RecordVal rec_highest_co2       = { 0,    "", false };
RecordVal rec_highest_temp_f    = { 0,    "", false };
RecordVal rec_lowest_temp_f     = { 0,    "", false };
RecordVal rec_most_visitors_day = { 0,    "", false };
RecordVal rec_longest_uptime_d  = { 0,    "", false };
static void saveRecords() {
    const char* path = "/stats/records.json";
    const char* tmp  = "/stats/records.tmp";
    const char* bak  = "/stats/records.bak";
    File f = SD.open(tmp, FILE_WRITE);
    if (!f) return;
    f.print("{");
    auto emit = [&](const char* key, const RecordVal& r, bool last, bool intValue) {
        f.print("\""); f.print(key); f.print("\":");
        if (!r.set) { f.print("null"); }
        else {
            f.print("{\"value\":");
            if (intValue) f.print((int)r.value);
            else          f.print(r.value, 2);
            f.print(",\"at\":\""); f.print(r.at); f.print("\"}");
        }
        if (!last) f.print(",");
    };
    emit("highest_co2",       rec_highest_co2,       false, true);
    emit("highest_temp_f",    rec_highest_temp_f,    false, false);
    emit("lowest_temp_f",     rec_lowest_temp_f,     false, false);
    emit("most_visitors_day", rec_most_visitors_day, false, true);
    // longest_uptime_d is last=true: the two optional blocks below (lifetime
    // energy, cost_per_kwh) self-lead with a comma when they emit, so a
    // trailing comma here would produce invalid JSON in the no-Shelly,
    // no-cost forker case.
    emit("longest_uptime_d",  rec_longest_uptime_d,  true,  false);
    // Lifetime energy + rates + tracking date piggyback on records.json.
    // Conditionally emitted so the JSON shape stays valid for forkers
    // without Shelly. last=true on longest_uptime_d above leaves the
    // standard records list cleanly terminated; each conditional block
    // self-leads with a comma when it emits.
    if (cfgShellyUrl[0] != '\0' && lifetime_energy_wh > 0.0f) {
        f.print(",\"lifetime_energy_wh\":");
        f.print((float)lifetime_energy_wh, 1);
        if (shelly_tracking_started_unix > 0) {
            f.print(",\"tracking_started_unix\":");
            f.print(shelly_tracking_started_unix);
        }
    }
    if (cfgCostPerKwh > 0.0f) {
        f.print(",\"cost_per_kwh\":");
        f.print(cfgCostPerKwh, 4);
    }
    if (cfgCo2PerKwh > 0.0f) {
        f.print(",\"co2_per_kwh\":");
        f.print(cfgCo2PerKwh, 4);
    }
    f.print("}");
    f.close();
    if (SD.exists(bak)) SD.remove(bak);
    if (SD.exists(path)) SD.rename(path, bak);
    if (!SD.rename(tmp, path)) {
        // Roll back: restore from .bak so a transient SD glitch doesn't zero
        // the Hall of Fame. Mirrors the pattern in saveCheckpoint/flushPeriod.
        if (SD.exists(bak)) SD.rename(bak, path);
        return;
    }
    if (SD.exists(bak)) SD.remove(bak);
}

// Sanity ranges for record-able values. BME280 spec is -40..+85°C → -40..+185°F.
// CCS811 eCO2 register max is 32768 ppm; readings beyond that are bus glitches.
// Used both at runtime (updateRecords) and at boot (loadRecords sanity-clean).
#define REC_TEMP_F_MIN  -40.0f
#define REC_TEMP_F_MAX  185.0f
#define REC_CO2_MIN     1
#define REC_CO2_MAX     32768

static void parseRecordField(const String& json, const char* key, RecordVal& out) {
    int k = json.indexOf(String("\"") + key + "\":");
    if (k < 0) return;
    int v = json.indexOf("\"value\":", k);
    int at = json.indexOf("\"at\":\"", k);
    int end = json.indexOf("}", k);
    if (v < 0 || at < 0 || end < 0) return;
    // Bound the value-end search by min(next-comma, brace) so a future
    // schema where `value` is the last numeric field before `}` (no
    // trailing comma) doesn't underflow into substring(v+8, -1).
    int comma = json.indexOf(",", v);
    int valueEnd = (comma >= 0 && comma < end) ? comma : end;
    if (valueEnd <= v + 8) return;  // malformed
    out.value = json.substring(v + 8, valueEnd).toFloat();
    int atEnd = json.indexOf("\"", at + 6);
    if (atEnd <= at + 6) return;  // malformed timestamp
    String atStr = json.substring(at + 6, atEnd);
    strncpy(out.at, atStr.c_str(), sizeof(out.at) - 1);
    out.at[sizeof(out.at) - 1] = '\0';
    out.set = true;
}

static void loadRecords() {
    const char* path = "/stats/records.json";
    if (!SD.exists(path)) return;
    File f = SD.open(path, FILE_READ);
    if (!f) return;
    size_t sz = f.size();
    // Cap to 8KB. Real records.json is ~600 bytes; anything larger is
    // either corruption or a manual-edit gone wrong, and we shouldn't
    // commit a big malloc on low heap. Reject silently.
    if (sz > 8192) { f.close(); return; }
    char* buf = (char*)malloc(sz + 1);
    if (!buf) { f.close(); return; }
    size_t n = f.read((uint8_t*)buf, sz);
    buf[n] = '\0';
    f.close();
    String json(buf);
    free(buf);
    parseRecordField(json, "highest_co2",       rec_highest_co2);
    parseRecordField(json, "highest_temp_f",    rec_highest_temp_f);
    parseRecordField(json, "lowest_temp_f",     rec_lowest_temp_f);
    parseRecordField(json, "most_visitors_day", rec_most_visitors_day);
    parseRecordField(json, "longest_uptime_d",  rec_longest_uptime_d);

    // Sanity-clean any record whose stored value couldn't have been produced
    // by the actual sensor. Catches latched glitch reads (e.g. 355°F from a
    // bus error during the wire-swap incident). Cleared records get re-set
    // naturally on the next legitimate reading. Save back so disk and memory
    // agree and we don't repeat this work every boot.
    bool dirty = false;
    auto clearIfBad = [&](RecordVal& r, float lo, float hi) {
        if (r.set && (r.value < lo || r.value > hi)) {
            r.set = false; r.value = 0; r.at[0] = '\0'; dirty = true;
        }
    };
    clearIfBad(rec_highest_temp_f, REC_TEMP_F_MIN, REC_TEMP_F_MAX);
    clearIfBad(rec_lowest_temp_f,  REC_TEMP_F_MIN, REC_TEMP_F_MAX);
    clearIfBad(rec_highest_co2,    (float)REC_CO2_MIN, (float)REC_CO2_MAX);
    if (dirty) saveRecords();
}

// Called from logStats every 5 min. Updates records if current values exceed stored extremes.
static void updateRecords(float temp_f, int co2, int dailyVis) {
    bool changed = false;
    String nowStr = getTimestamp();

    if (co2 >= REC_CO2_MIN && co2 <= REC_CO2_MAX) {
        if (!rec_highest_co2.set || co2 > rec_highest_co2.value) {
            rec_highest_co2.value = co2;
            strncpy(rec_highest_co2.at, nowStr.c_str(), sizeof(rec_highest_co2.at) - 1);
            rec_highest_co2.at[sizeof(rec_highest_co2.at) - 1] = '\0';
            rec_highest_co2.set = true; changed = true;
        }
    }
    if (!isnan(temp_f) && temp_f >= REC_TEMP_F_MIN && temp_f <= REC_TEMP_F_MAX) {
        if (!rec_highest_temp_f.set || temp_f > rec_highest_temp_f.value) {
            rec_highest_temp_f.value = temp_f;
            strncpy(rec_highest_temp_f.at, nowStr.c_str(), sizeof(rec_highest_temp_f.at) - 1);
            rec_highest_temp_f.at[sizeof(rec_highest_temp_f.at) - 1] = '\0';
            rec_highest_temp_f.set = true; changed = true;
        }
        if (!rec_lowest_temp_f.set || temp_f < rec_lowest_temp_f.value) {
            rec_lowest_temp_f.value = temp_f;
            strncpy(rec_lowest_temp_f.at, nowStr.c_str(), sizeof(rec_lowest_temp_f.at) - 1);
            rec_lowest_temp_f.at[sizeof(rec_lowest_temp_f.at) - 1] = '\0';
            rec_lowest_temp_f.set = true; changed = true;
        }
    }
    if (dailyVis > 0 && (!rec_most_visitors_day.set || dailyVis > rec_most_visitors_day.value)) {
        rec_most_visitors_day.value = dailyVis;
        // Use the date dailyVisitors actually represents, not "today when we happened to check".
        // Fall back to current date only if we never recorded one (first-run edge case).
        if (dailyVisitorsDate[0]) {
            strncpy(rec_most_visitors_day.at, dailyVisitorsDate, sizeof(rec_most_visitors_day.at) - 1);
            rec_most_visitors_day.at[sizeof(rec_most_visitors_day.at) - 1] = '\0';
        } else {
            struct tm now;
            if (getLocalTime(&now, 0)) {
                strftime(rec_most_visitors_day.at, sizeof(rec_most_visitors_day.at), "%Y-%m-%d", &now);
            }
        }
        rec_most_visitors_day.set = true; changed = true;
    }
    // Longest uptime as fractional days. Computed from wall-clock so it
    // survives the 49.7-day millis() rollover. bootTime is the unix
    // timestamp captured at NTP sync; before NTP syncs it's 0 and we skip
    // the record update rather than report garbage.
    //
    // Schema name stays "_d" for backward compatibility, but the value is
    // now a float (e.g. 0.25 = 6h, 1.5 = 36h) so the substat populates as
    // soon as the device has been up at least one logStats interval (5
    // min) instead of needing a full 24h continuous run before showing
    // anything. The client formats <1.0 as "Xh", >=1.0 as "Xd".
    float uptimeDays = 0.0f;
    if (bootTime > 0) {
        time_t nowSec = time(nullptr);
        if (nowSec > bootTime) uptimeDays = (float)(nowSec - bootTime) / 86400.0f;
    }
    if (uptimeDays > 0.0f && (!rec_longest_uptime_d.set || uptimeDays > rec_longest_uptime_d.value)) {
        rec_longest_uptime_d.value = uptimeDays;
        strncpy(rec_longest_uptime_d.at, nowStr.c_str(), sizeof(rec_longest_uptime_d.at) - 1);
        rec_longest_uptime_d.at[sizeof(rec_longest_uptime_d.at) - 1] = '\0';
        rec_longest_uptime_d.set = true; changed = true;
    }
    if (changed) saveRecords();
}

static void checkPeriodBoundaries() {
    struct tm now;
    if (!getLocalTime(&now)) return;
    char w[9], m[8], y[5];
    // ISO week: %G = ISO year, %V = ISO week number (01-53, Monday-start)
    strftime(w, sizeof(w), "%G-W%V", &now);
    snprintf(m, sizeof(m), "%04d-%02d", now.tm_year + 1900, now.tm_mon + 1);
    snprintf(y, sizeof(y), "%04d", now.tm_year + 1900);

    // Only advance past a period boundary if the flush succeeded. If the SD
    // write fails (transient card error, full filesystem, etc.), leave the
    // label and accumulator alone, the next call will retry. Without this
    // guard, a single failed flush would silently zero a week/month/year of
    // accumulated stats.
    if (lastWeekLabel[0] == '\0') {
        strncpy(lastWeekLabel, w, sizeof(lastWeekLabel) - 1);
        lastWeekLabel[sizeof(lastWeekLabel) - 1] = '\0';
    } else if (strcmp(lastWeekLabel, w) != 0) {
        // Capture the period's accumulated energy at flush time so the
        // archive JSON records it. -1 sentinel when no Shelly is set.
        float weekE = (cfgShellyUrl[0] != '\0') ? (float)week_energy_wh : -1.0f;
        // Year-subdir grouping keeps each weekly dir bounded at ~52 files.
        // Year is the first 4 chars of the ISO week label "YYYY-WNN".
        String weeklyDir = String("/stats/weekly/") + String(lastWeekLabel).substring(0, 4);
        if (!SD.exists(weeklyDir.c_str())) SD.mkdir(weeklyDir.c_str());
        if (flushPeriod(weeklyDir.c_str(), lastWeekLabel, currentWeek, weekE)) {
            resetPeriod(currentWeek);
            week_energy_wh = 0.0f;  // reset accumulator for the new period
            strncpy(lastWeekLabel, w, sizeof(lastWeekLabel) - 1);
            lastWeekLabel[sizeof(lastWeekLabel) - 1] = '\0';
        }
    }

    if (lastMonthLabel[0] == '\0') {
        strncpy(lastMonthLabel, m, sizeof(lastMonthLabel) - 1);
        lastMonthLabel[sizeof(lastMonthLabel) - 1] = '\0';
    } else if (strcmp(lastMonthLabel, m) != 0) {
        float monthE = (cfgShellyUrl[0] != '\0') ? (float)month_energy_wh : -1.0f;
        // Year-subdir grouping: same rationale as weekly. Year is the first
        // 4 chars of the month label "YYYY-MM".
        String monthlyDir = String("/stats/monthly/") + String(lastMonthLabel).substring(0, 4);
        if (!SD.exists(monthlyDir.c_str())) SD.mkdir(monthlyDir.c_str());
        if (flushPeriod(monthlyDir.c_str(), lastMonthLabel, currentMonth, monthE)) {
            resetPeriod(currentMonth);
            month_energy_wh = 0.0f;
            strncpy(lastMonthLabel, m, sizeof(lastMonthLabel) - 1);
            lastMonthLabel[sizeof(lastMonthLabel) - 1] = '\0';
        }
    }

    if (lastYearLabel[0] == '\0') {
        strncpy(lastYearLabel, y, sizeof(lastYearLabel) - 1);
        lastYearLabel[sizeof(lastYearLabel) - 1] = '\0';
    } else if (strcmp(lastYearLabel, y) != 0) {
        float yearE = (cfgShellyUrl[0] != '\0') ? (float)year_energy_wh : -1.0f;
        if (flushPeriod("/stats/yearly", lastYearLabel, currentYear, yearE)) {
            resetPeriod(currentYear);
            year_energy_wh = 0.0f;
            strncpy(lastYearLabel, y, sizeof(lastYearLabel) - 1);
            lastYearLabel[sizeof(lastYearLabel) - 1] = '\0';
        }
    }
}
unsigned long          lastPageSwitch      = 0;
#define DISPLAY_PAGES  6
#define PAGE_INTERVAL  10000

// HelloESP favicon (32x32)
const unsigned char logoBitmap[] PROGMEM = {
    0x01, 0xce, 0x73, 0x80, 0x01, 0xce, 0x73, 0x80, 0x01, 0xce, 0x73, 0x80, 0x01, 0xce, 0x73, 0x80,
    0x00, 0x00, 0x00, 0x00, 0x07, 0xff, 0xff, 0xc0, 0x07, 0xff, 0xff, 0xe0, 0xf7, 0xff, 0xff, 0xef,
    0xf7, 0xff, 0xff, 0xef, 0xf7, 0xff, 0xff, 0xef, 0x07, 0xff, 0xff, 0xe0, 0x07, 0xc0, 0x07, 0xe0,
    0xf7, 0xc0, 0x07, 0xe7, 0xf7, 0xc0, 0x07, 0xef, 0xf7, 0xc0, 0x07, 0xef, 0x07, 0xc0, 0x07, 0xe0,
    0x07, 0xc0, 0x07, 0xe0, 0xf7, 0xc0, 0x07, 0xef, 0xf7, 0xc0, 0x07, 0xef, 0xf7, 0xc0, 0x07, 0xef,
    0x07, 0xc0, 0x07, 0xe0, 0x07, 0xff, 0xff, 0xe0, 0xf7, 0xff, 0xff, 0xef, 0xf7, 0xff, 0xff, 0xef,
    0xf7, 0xff, 0xff, 0xef, 0x07, 0xff, 0xff, 0xe0, 0x07, 0xff, 0xff, 0xc0, 0x00, 0x00, 0x00, 0x00,
    0x01, 0xce, 0x73, 0x80, 0x01, 0xce, 0x73, 0x80, 0x01, 0xce, 0x73, 0x80, 0x01, 0xce, 0x73, 0x80
};

// 50x50 QR code for "https://helloesp.com" (2x scaled from 25x25 QR-L)
const unsigned char qrBitmap[] PROGMEM = {
    0xff, 0xfc, 0x3f, 0x3c, 0x0f, 0xff, 0xc0, 0xff, 0xfc, 0x3f, 0x3c, 0x0f, 0xff, 0xc0, 0xc0, 0x0c,
    0x03, 0xf0, 0x0c, 0x00, 0xc0, 0xc0, 0x0c, 0x03, 0xf0, 0x0c, 0x00, 0xc0, 0xcf, 0xcc, 0x3f, 0x0c,
    0xcc, 0xfc, 0xc0, 0xcf, 0xcc, 0x3f, 0x0c, 0xcc, 0xfc, 0xc0, 0xcf, 0xcc, 0xcf, 0x33, 0xcc, 0xfc,
    0xc0, 0xcf, 0xcc, 0xcf, 0x33, 0xcc, 0xfc, 0xc0, 0xcf, 0xcc, 0xcc, 0xf0, 0x0c, 0xfc, 0xc0, 0xcf,
    0xcc, 0xcc, 0xf0, 0x0c, 0xfc, 0xc0, 0xc0, 0x0c, 0x3c, 0x3c, 0x0c, 0x00, 0xc0, 0xc0, 0x0c, 0x3c,
    0x3c, 0x0c, 0x00, 0xc0, 0xff, 0xfc, 0xcc, 0xcc, 0xcf, 0xff, 0xc0, 0xff, 0xfc, 0xcc, 0xcc, 0xcf,
    0xff, 0xc0, 0x00, 0x00, 0x30, 0xcc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0xcc, 0x00, 0x00, 0x00,
    0xf0, 0x3f, 0x3c, 0xff, 0xc0, 0xf0, 0x00, 0xf0, 0x3f, 0x3c, 0xff, 0xc0, 0xf0, 0x00, 0x0f, 0xc3,
    0xcc, 0x3f, 0x03, 0xff, 0x00, 0x0f, 0xc3, 0xcc, 0x3f, 0x03, 0xff, 0x00, 0x33, 0x0f, 0x33, 0xcf,
    0xfc, 0x33, 0xc0, 0x33, 0x0f, 0x33, 0xcf, 0xfc, 0x33, 0xc0, 0x3f, 0xc0, 0x0f, 0x0f, 0x3c, 0xf0,
    0xc0, 0x3f, 0xc0, 0x0f, 0x0f, 0x3c, 0xf0, 0xc0, 0x00, 0xcc, 0xfc, 0x3f, 0x0f, 0x00, 0xc0, 0x00,
    0xcc, 0xfc, 0x3f, 0x0f, 0x00, 0xc0, 0xcc, 0xf0, 0x3c, 0xf3, 0xc3, 0x03, 0x00, 0xcc, 0xf0, 0x3c,
    0xf3, 0xc3, 0x03, 0x00, 0xc3, 0x3f, 0xc0, 0x3f, 0xf3, 0xf3, 0xc0, 0xc3, 0x3f, 0xc0, 0x3f, 0xf3,
    0xf3, 0xc0, 0xc0, 0xc3, 0xfc, 0xfc, 0x0f, 0x3c, 0xc0, 0xc0, 0xc3, 0xfc, 0xfc, 0x0f, 0x3c, 0xc0,
    0xcf, 0x3c, 0x3c, 0xcf, 0xff, 0xcc, 0x00, 0xcf, 0x3c, 0x3c, 0xcf, 0xff, 0xcc, 0x00, 0x00, 0x00,
    0xcc, 0x00, 0xc0, 0xc0, 0x00, 0x00, 0x00, 0xcc, 0x00, 0xc0, 0xc0, 0x00, 0xff, 0xfc, 0xcc, 0xfc,
    0xcc, 0xc0, 0xc0, 0xff, 0xfc, 0xcc, 0xfc, 0xcc, 0xc0, 0xc0, 0xc0, 0x0c, 0xff, 0x0f, 0xc0, 0xc0,
    0xc0, 0xc0, 0x0c, 0xff, 0x0f, 0xc0, 0xc0, 0xc0, 0xcf, 0xcc, 0x03, 0x03, 0xff, 0xcf, 0xc0, 0xcf,
    0xcc, 0x03, 0x03, 0xff, 0xcf, 0xc0, 0xcf, 0xcc, 0x30, 0xf3, 0x3c, 0x03, 0xc0, 0xcf, 0xcc, 0x30,
    0xf3, 0x3c, 0x03, 0xc0, 0xcf, 0xcc, 0x0c, 0x3c, 0xc0, 0x3c, 0xc0, 0xcf, 0xcc, 0x0c, 0x3c, 0xc0,
    0x3c, 0xc0, 0xc0, 0x0c, 0xcc, 0xf0, 0xf3, 0xc0, 0xc0, 0xc0, 0x0c, 0xcc, 0xf0, 0xf3, 0xc0, 0xc0,
    0xff, 0xfc, 0xf3, 0xf0, 0xfc, 0x30, 0xc0, 0xff, 0xfc, 0xf3, 0xf0, 0xfc, 0x30, 0xc0
};

volatile int           requestsThisInterval = 0;
int                    dailyVisitors       = 0;
int                    cachedVisitorCount  = 0;
int                    lastVisitorDay      = -1;
// Date dailyVisitors belongs to (YYYY-MM-DD). Used for the Hall of Fame "Busiest day" record so
// peaks that occur near midnight get dated correctly even if updateRecords() runs after the
// day boundary before the next visitor would have reset the counter.
char                   dailyVisitorsDate[11] = "";
float                  cachedSdUsedMB      = 0;
volatile int           pendingGuestbook    = 0;  // count of status=0 (new/unreviewed)
volatile int           gbCountApproved     = 0;  // count of status=1
volatile int           gbCountDenied       = 0;  // count of status=2
volatile int           gbCountAll          = 0;  // total entries in guestbook.csv

int                    lastNotifiedPending = 0;
volatile bool          pendingNotifyFlag   = false;
char                   notifyEntryName[33]    = "";
char                   notifyEntryCountry[4]  = "";
char                   notifyEntryMessage[201]= "";
time_t                 bootTime            = 0;

volatile bool          pendingMaintenanceFlag     = false;
int                    pendingMaintenanceMinutes  = 0;
char                   pendingMaintenanceMessage[201] = "";
// UI-hint copy of the maintenance window. Worker DO storage is authoritative.
uint32_t               localMaintenanceUntilUnix      = 0;
char                   localMaintenanceMessage[201]   = "";
volatile bool          pendingConsolePush         = false;

// Backup constants. BACKUP_MAX_* are runaway-protection ceilings; the Worker stores
// whatever arrives and splits as needed.
#define BACKUP_HOUR_LOCAL  4
#define BACKUP_MAX_FILE    (10UL * 1024UL * 1024UL)
#define BACKUP_MAX_TOTAL   (50UL * 1024UL * 1024UL)
#define LAST_BACKUP_PATH   "/stats/last_backup.txt"
#define LAST_BACKUP_TMP    "/stats/last_backup.tmp"
#define LAST_COMMIT_PATH   "/stats/last_commit.txt"
#define LAST_COMMIT_TMP    "/stats/last_commit.tmp"

volatile bool          pendingBackupFlag          = false;

// Timestamp of the last multipart upload/OTA chunk we saw.
volatile unsigned long lastUploadChunkMs          = 0;
// Set by the /_upload chunk handler when the target file already exists and
// the request didn't include overwrite=1; the completion handler reads it to
// reply 409 instead of 200, so a misclick can't silently clobber index.html.
bool uploadRejectedExisting                       = false;
// Window after the last chunk during which we still suppress WS work
// (reads, pings, reconnects). Covers both "upload still in flight" between
// chunks and a grace period for AsyncTCP to finalize the HTTP response
// without a competing WS reconnect grabbing task time.
// Was 30000UL originally as defense against the post-launch reconnect
// cascade; AsyncTCP usually drains within 1-2s of the last chunk so 5s
// gives ample margin while keeping the post-upload UX quick (~5-8s total
// before live updates resume vs ~30-35s before).
#define UPLOAD_QUIET_MS 5000UL
unsigned long          lastBackupSentMs           = 0;
char                   lastBackupDate[11]         = ""; // YYYY-MM-DD, persisted across reboots
// Ground truth from Worker's backup_committed event. "sent" is ESP-side, "committed" is R2-side.
char                   r2CommittedDate[11]        = "";
uint32_t               r2CommittedBytes           = 0;
uint16_t               r2CommittedFiles           = 0;
uint32_t               r2CommittedAtUnix          = 0;

// Admin-triggered R2 liveness check. Worker does PUT/GET/DELETE on a test key and reports back.
volatile bool          pendingR2HealthcheckFlag   = false;
uint32_t               r2HealthcheckAtUnix        = 0;
bool                   r2HealthcheckPass          = false;
char                   r2HealthcheckDetail[129]   = "";

// Admin-triggered SMTP2GO test. Catches silent email-integration failures before a real alert fires.
volatile bool          pendingTestEmailFlag       = false;
volatile bool          pendingSnakeClearFlag      = false;
uint32_t               snakeClearAtUnix           = 0;
bool                   snakeClearOk               = false;
uint32_t               testEmailAtUnix            = 0;
bool                   testEmailPass              = false;
char                   testEmailDetail[129]       = "";

// Admin-triggered firmware flash from an SD-card .bin. Decouples the
// upload phase (file manager, retry-friendly) from the flash phase
// (fully local, network-independent). Handler sets these flags + responds
// quickly; main loop performs the actual flash so we don't hold the HTTP
// connection for the ~30-60s flash duration.
volatile bool          pendingSDFlash             = false;
char                   pendingSDFlashPath[96]     = "";

// Admin-triggered partition rollback (flip active OTA partition back
// to the previously-active one). Same flag pattern as SD-flash.
volatile bool          pendingRollback            = false;

// Admin-triggered SHA256 of an SD file (used to verify a firmware .bin
// matches what was uploaded from the PC). Computation runs on the main
// loop so a ~6s 1.5MB file read doesn't starve async_tcp.
volatile bool          pendingSha256              = false;
char                   pendingSha256Path[96]      = "";
char                   sha256Result[80]           = "";   // 64 hex chars + "\0"
char                   sha256ResultPath[96]       = "";   // which file sha256Result is for
volatile bool          sha256Computing            = false;

// Device health sparkline ring buffer (sampled once per minute, 60 samples = 1 hour)
#define HEALTH_SAMPLES 60
struct HealthSample { uint32_t free_heap; int8_t rssi; };
HealthSample healthRing[HEALTH_SAMPLES];
int healthHead = 0;
int healthCount = 0;
unsigned long lastHealthSample = 0;

// Worker link observability
unsigned long wsReconnectCount = 0;

// Request console ring buffer for /console page
#define CONSOLE_SIZE 50
struct ConsoleEntry {
    uint32_t unix_time;
    uint16_t status;
    char     method[6];
    char     country[4];
    char     path[48];
};
ConsoleEntry consoleRing[CONSOLE_SIZE];
int consoleHead = 0;
int consoleCount = 0;

static void normalizeCountry(const char* in, char* out);  // fwd decl; body below

// Serve a pre-gzipped .gz file when the client accepts gzip; fall back to
// the raw file. Pre-gzipping cuts wire-size ~4x for HTML, which matters
// under LAN load: 78KB raw streams saturate lwIP's pbuf pool and cascade
// into WS write + DNS failures. Open the .gz file manually and pass the
// base path; the library detects the .gz suffix on file.name() and adds
// Content-Encoding: gzip automatically.
static AsyncWebServerResponse* beginResponseGzipOrRaw(AsyncWebServerRequest *req,
                                                     const char* path,
                                                     const char* type) {
    String gzPath = String(path) + ".gz";
    bool acceptsGzip = req->hasHeader("Accept-Encoding") &&
                       req->header("Accept-Encoding").indexOf("gzip") >= 0;
    if (acceptsGzip && SD.exists(gzPath)) {
        File f = SD.open(gzPath, FILE_READ);
        if (f) {
            return req->beginResponse(f, String(path), type);
        }
    }
    File f = SD.open(path, FILE_READ);
    if (!f) {
        AsyncWebServerResponse *r = req->beginResponse(503, "text/plain", "Device busy, retry");
        r->addHeader("Retry-After", "2");
        return r;
    }
    return req->beginResponse(f, String(path), type);
}

static void sendGzipOrRaw(AsyncWebServerRequest *req, const char* path, const char* type) {
    req->send(beginResponseGzipOrRaw(req, path, type));
}

static bool consoleShouldSkip(const String& url) {
    if (url.startsWith("/admin")) return true;        // privacy: admin actions are never public
    if (url.startsWith("/_"))     return true;        // /_ws, /_upload, /_ota (internal)
    if (url.startsWith("/logs/")) return true;        // CSV data the charts fetch in the background
    if (url.startsWith("/.well-known/")) return true; // security scanners only
    // AsyncWebServer routes "//sometext" to the "/" handler, logging the raw URL. Visitors discovered they could "chat" by visiting URLs like "//HelloEveryone" and seeing them show up on /console. 
    // Fun in concept, but moderation hell at scale. Skip any path with adjacent slashes so nothing bypasses route matching via this quirk.
    if (url.startsWith("//") || url.indexOf("//") >= 0) return true;
    if (url == "/ping")           return true;        // polled by auto-retry, too noisy
    // AJAX endpoints the homepage + history + console + guestbook pages fetch in the background
    if (url.startsWith("/stats"))   return true;
    if (url == "/countries")        return true;
    if (url == "/records.json")     return true;
    if (url == "/console.json")     return true;
    if (url == "/history.json")     return true;
    if (url == "/guestbook/entries") return true;
    if (url == "/guestbook/submit")  return true;
    // bot-only metadata
    if (url == "/robots.txt")     return true;
    if (url == "/sitemap.xml")    return true;
    if (url == "/changelog.rss")  return true;
    if (url == "/guestbook.rss")  return true;
    if (url == "/.well-known/security.txt") return true;
    // static assets
    if (url == "/favicon.png" || url == "/favicon.svg" || url == "/og-banner.jpg") return true;
    if (url == "/helloesp-framed.jpg" || url == "/helloesp-boot.mp4" || url == "/helloesp-boot-poster.jpg") return true;
    if (url == "/esp32-webserver.jpg" || url == "/esp32-webserver-bme280.jpg" || url == "/esp8266-webserver.jpg") return true;
    return false;
}

static void logConsole(AsyncWebServerRequest *req, int status) {
    String url = req->url();
    if (consoleShouldSkip(url)) return;
    // HEAD requests are monitoring probes / CDN health checks, not real page visits
    if (req->method() == HTTP_HEAD) return;

    // LED blinks on any page visit. Sits above the CF-header/404 filters
    // so LAN tests and 404s still blink.
    digitalWrite(LED_PIN, HIGH);
    ledOn = true;
    lastRequestTime = millis();

    // LAN-origin requests (owner testing) don't have CF-Connecting-IP and would show "??".
    // The public console is meant to reflect public traffic, not local noise.
    if (!req->hasHeader("CF-Connecting-IP")) return;
    // 404s are skipped to prevent the ring buffer from being poisoned with troll probe paths
    // (/<offensive-string> attempts). Admin error/sensor logs still capture them for auditing.
    if (status == 404) return;
    ConsoleEntry &e = consoleRing[consoleHead];
    e.unix_time = (uint32_t)time(nullptr);
    e.status    = (uint16_t)status;
    const char* m = "?";
    if      (req->method() == HTTP_GET)  m = "GET";
    else if (req->method() == HTTP_POST) m = "POST";
    else if (req->method() == HTTP_HEAD) m = "HEAD";
    strncpy(e.method, m, sizeof(e.method) - 1);
    e.method[sizeof(e.method) - 1] = '\0';
    strcpy(e.country, "??");
    if (req->hasHeader("CF-IPCountry")) {
        String cv = req->header("CF-IPCountry");
        normalizeCountry(cv.c_str(), e.country);
    }
    strncpy(e.path, url.c_str(), sizeof(e.path) - 1);
    e.path[sizeof(e.path) - 1] = '\0';
    consoleHead = (consoleHead + 1) % CONSOLE_SIZE;
    if (consoleCount < CONSOLE_SIZE) consoleCount++;
    pendingConsolePush = true;
}

// guestbook rate limiting
#define MAX_RATE_ENTRIES 50
#define RATE_LIMIT_MS    3600000
// Cap on pending (unmoderated) guestbook entries. A determined attacker rotating IPs can
// bypass the 1/hour per-IP limit; this puts a ceiling on moderation-queue abuse until admin
// clears the backlog.
#define MAX_PENDING_GUESTBOOK 1000
struct RateEntry { String ip; unsigned long time; };
RateEntry rateLimits[MAX_RATE_ENTRIES];
int rateLimitCount = 0;

bool isRateLimited(const String& ip) {
    unsigned long now = millis();
    // clean expired entries
    for (int i = rateLimitCount - 1; i >= 0; i--) {
        if (now - rateLimits[i].time > RATE_LIMIT_MS) {
            rateLimits[i] = rateLimits[--rateLimitCount];
        }
    }
    for (int i = 0; i < rateLimitCount; i++) {
        if (rateLimits[i].ip == ip) return true;
    }
    if (rateLimitCount < MAX_RATE_ENTRIES) {
        rateLimits[rateLimitCount].ip = ip;
        rateLimits[rateLimitCount].time = now;
        rateLimitCount++;
    }
    return false;
}

static String clientIpString(AsyncWebServerRequest* request) {
    if (request->hasHeader("CF-Connecting-IP")) {
        String ip = request->header("CF-Connecting-IP");
        ip.trim();
        if (ip.length() > 0) return ip;
    }
    return request->client()->remoteIP().toString();
}

// country tracking
#define MAX_COUNTRIES 200
struct CountryEntry { char code[4]; int count; };
CountryEntry countries[MAX_COUNTRIES];
int countryCount = 0;

void loadCountries() {
    countryCount = 0;
    File f = SD.open("/countries.csv", FILE_READ);
    if (!f) {
        if (SD.exists("/countries.tmp")) {
            SD.rename("/countries.tmp", "/countries.csv");
            f = SD.open("/countries.csv", FILE_READ);
        }
        if (!f) return;
    }
    size_t sz = f.size();
    String all;
    char* buf = (char*)malloc(sz + 1);
    if (buf) {
        size_t n = f.read((uint8_t*)buf, sz);
        buf[n] = '\0';
        all = String(buf);
        free(buf);
    }
    f.close();
    int start = 0;
    while (start < (int)all.length() && countryCount < MAX_COUNTRIES) {
        int end = all.indexOf('\n', start);
        if (end < 0) end = all.length();
        String line = all.substring(start, end);
        start = end + 1;
        line.trim();
        int comma = line.indexOf(',');
        if (comma < 1) continue;
        String code = line.substring(0, comma);
        int cnt = line.substring(comma + 1).toInt();
        // Reject anything that isn't an exact 2-letter ISO code. Without
        // this guard, a corrupted/hand-edited row with a 3-letter code
        // would silently truncate, then never match `tallyCountry()`'s
        // 2-char compare, and grow as a permanent orphan on every save.
        if (code.length() != 2) continue;
        code.toCharArray(countries[countryCount].code, 4);
        countries[countryCount].count = cnt;
        countryCount++;
    }
}

void saveCountries() {
    File tmp = SD.open("/countries.tmp", FILE_WRITE);
    if (!tmp) return;
    for (int i = 0; i < countryCount; i++) {
        tmp.print(countries[i].code);
        tmp.print(",");
        tmp.println(countries[i].count);
    }
    tmp.close();

    if (SD.exists("/countries.bak")) SD.remove("/countries.bak");
    if (SD.exists("/countries.csv")) SD.rename("/countries.csv", "/countries.bak");
    SD.rename("/countries.tmp", "/countries.csv");
}

// Normalize a CF-IPCountry value to an uppercase ISO alpha-2 code or "??".
// Rejects non-alpha codes (T1 Tor, A1/A2/O1 anonymous buckets, etc.) and the
// XX unknown sentinel. out buffer must be at least 3 bytes; always written.
static void normalizeCountry(const char* in, char* out) {
    out[0] = '?'; out[1] = '?'; out[2] = '\0';
    if (!in || !in[0] || !in[1]) return;
    char c0 = in[0], c1 = in[1];
    if (c0 >= 'a' && c0 <= 'z') c0 -= 32;
    if (c1 >= 'a' && c1 <= 'z') c1 -= 32;
    if (c0 < 'A' || c0 > 'Z' || c1 < 'A' || c1 > 'Z') return;
    if (c0 == 'X' && c1 == 'X') return;
    out[0] = c0; out[1] = c1;
}

// Dirty flag + flush interval for country tallies. Under flash-crowd traffic
// (this device has handled 10k visitors in a day) the original per-visit
// saveCountries() rewrite was pinning the SD bus inside the AsyncTCP HTTP
// handler. Now we just flip a dirty flag here; the main loop flushes at most
// once every 30s. Worst-case data loss on a crash is 30s of country tallies.
volatile bool          countriesDirty           = false;
unsigned long          lastCountriesFlush       = 0;
#define COUNTRIES_FLUSH_INTERVAL_MS 30000UL

void tallyCountry(const char* code) {
    if (!code || strlen(code) < 2) return;
    for (int i = 0; i < countryCount; i++) {
        if (strcmp(countries[i].code, code) == 0) {
            countries[i].count++;
            countriesDirty = true;
            return;
        }
    }
    if (countryCount < MAX_COUNTRIES) {
        strncpy(countries[countryCount].code, code, 3);
        countries[countryCount].code[3] = '\0';
        countries[countryCount].count = 1;
        countryCount++;
        countriesDirty = true;
    }
}

// Called from the main loop and from cleanRestart, so we don't lose recent
// tallies on a planned reboot. Also called explicitly before periods flush.
void maybeFlushCountries() {
    if (!countriesDirty) return;
    if (millis() - lastCountriesFlush < COUNTRIES_FLUSH_INTERVAL_MS) return;
    saveCountries();
    countriesDirty = false;
    lastCountriesFlush = millis();
}

// Admin auth
bool isLocalIP(IPAddress ip) {
    return ip[0] == 10
        || (ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31)
        || (ip[0] == 192 && ip[1] == 168);
}

// Worker-exclusive mode: when cfgWorkerExclusive is true, direct LAN hits
// to public pages get a 302 to the Worker. Forces public traffic through
// CF's edge cache, eliminating the LAN-burst-vs-WS-write cascade pattern.
//
// Returns true if a redirect was sent (caller should `return` immediately).
// Returns false if the request should be served normally:
//   - flag is off, OR
//   - request came in via the Worker relay (CF-Connecting-IP set), OR
//   - request is from a non-LAN address (port-forwarded public visitor
//     hitting us directly without going through CF; keep them working
//     on the off-chance CF is misconfigured), OR
//   - no worker_url configured (no destination to redirect to).
//
// /admin and admin sub-paths must NOT call this; admin auth requires
// direct LAN access.
// Minimal RFC 3986 unreserved-only percent-encoder. Keep it tiny, only
// used to rebuild redirect query strings in worker-exclusive mode, which
// fires once per page load.
static String urlEncodeMin(const String& s) {
    String out;
    out.reserve(s.length() + 8);
    for (size_t i = 0; i < s.length(); i++) {
        char c = s[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~') {
            out += c;
        } else {
            char buf[4];
            snprintf(buf, sizeof(buf), "%%%02X", (unsigned char)c);
            out += buf;
        }
    }
    return out;
}

static bool redirectLanToWorker(AsyncWebServerRequest *request) {
    if (!cfgWorkerExclusive) return false;
    if (strlen(cfgWorkerUrl) == 0) return false;
    if (request->hasHeader("CF-Connecting-IP")) return false;
    if (!isLocalIP(request->client()->remoteIP())) return false;
    // Admin-only endpoints called from /admin that don't start with /admin.
    // These need to serve direct on LAN so the moderation UI works even
    // with worker_exclusive=true. Earlier attempt: bypass on Authorization
    // header presence. That was way too broad: once any browser tab was
    // authed against /admin, Basic Auth sends the header on EVERY request
    // in the realm (including / and other public pages), so the redirect
    // never fired. Explicit path list is narrower and predictable.
    {
        const String& url = request->url();
        if (url.startsWith("/guestbook/pending"))   return false;
        if (url.startsWith("/guestbook/moderate"))  return false;
        if (url == "/guestbook/locate")             return false;
        if (url == "/logs/today")                   return false;
    }
    String redir = "https://";
    redir += cfgWorkerUrl;
    redir += request->url();
    // request->url() in this AsyncWebServer build returns the path only;
    // query is parsed into params() and stripped. Rebuild the query from
    // GET params so /guestbook/entries?page=2 redirects to the right page
    // instead of falling back to page=1 on the public side. Re-encode
    // values because params arrive URL-decoded; sending raw spaces or
    // special chars in Location would break the URL.
    int n = request->params();
    bool firstParam = true;
    for (int i = 0; i < n; i++) {
        const AsyncWebParameter* p = request->getParam(i);
        if (!p) continue;
        if (p->isPost() || p->isFile()) continue;  // GET-only redirects
        redir += firstParam ? '?' : '&';
        redir += urlEncodeMin(p->name());
        if (p->value().length() > 0) {
            redir += '=';
            redir += urlEncodeMin(p->value());
        }
        firstParam = false;
    }
    AsyncWebServerResponse *r = request->beginResponse(302, "text/plain", "Redirecting to public site");
    r->addHeader("Location", redir);
    r->addHeader("Cache-Control", "no-store");
    request->send(r);
    return true;
}

struct FailedAuthRec {
    uint32_t ip;
    int count;
    unsigned long firstFailAt;
};
FailedAuthRec failedAuths[AUTH_TRACK_SIZE] = {};

static FailedAuthRec* findOrAllocAuthRec(uint32_t ip) {
    unsigned long now = millis();
    FailedAuthRec* oldest = &failedAuths[0];
    for (int i = 0; i < AUTH_TRACK_SIZE; i++) {
        if (failedAuths[i].ip == ip) return &failedAuths[i];
        if (failedAuths[i].ip == 0) return &failedAuths[i];
        if ((now - failedAuths[i].firstFailAt) > (now - oldest->firstFailAt)) oldest = &failedAuths[i];
    }
    oldest->ip = 0; oldest->count = 0; oldest->firstFailAt = 0;
    return oldest;
}

static bool isAuthLockedOut(uint32_t ip) {
    for (int i = 0; i < AUTH_TRACK_SIZE; i++) {
        if (failedAuths[i].ip == ip) {
            if (millis() - failedAuths[i].firstFailAt > AUTH_LOCKOUT_MS) {
                failedAuths[i].ip = 0; failedAuths[i].count = 0;
                return false;
            }
            return failedAuths[i].count >= AUTH_MAX_FAILS;
        }
    }
    return false;
}

static void recordAuthFail(uint32_t ip) {
    FailedAuthRec* r = findOrAllocAuthRec(ip);
    if (r->ip != ip) { r->ip = ip; r->count = 0; r->firstFailAt = millis(); }
    if (millis() - r->firstFailAt > AUTH_LOCKOUT_MS) {
        r->count = 0; r->firstFailAt = millis();
    }
    r->count++;
}

static void clearAuthFails(uint32_t ip) {
    for (int i = 0; i < AUTH_TRACK_SIZE; i++) {
        if (failedAuths[i].ip == ip) { failedAuths[i].ip = 0; failedAuths[i].count = 0; return; }
    }
}

static bool safePath(const String& p) {
    return p.startsWith("/") && p.indexOf("..") < 0;
}

// Files whose deletion would either brick the device on next boot
// (config.txt has the WiFi + admin creds) or leave the public site broken
// until manual SD-card surgery. The admin file manager has an upload path
// for replacement, so a real "I want to update this" flow is unaffected;
// this just blocks the misclick-into-brick case. .gz companions are
// intentionally NOT listed because the firmware falls back to the
// uncompressed file when the .gz is missing, so deleting one is recoverable
// without rebooting.
static bool isProtectedPath(const String& p) {
    return p == "/config.txt"
        || p == "/index.html"
        || p == "/about.html"
        || p == "/guestbook.html"
        || p == "/history.html"
        || p == "/console.html"
        || p == "/snake.html"
        || p == "/chronicle.html"
        || p == "/admin.html"
        || p == "/404.html";
}

// Reboot by disconnecting WiFi before calling ESP.restart() to clear WiFi
// driver state without a physical power cycle. ESP.restart() alone leaves
// the device in a post-OTA broken state only fixable by power cycling.
static void cleanRestart() {
    // Force-flush dirty country tallies so a planned reboot doesn't lose
    // the most-recent tallyCountry() bumps that hadn't hit the 30s flush yet.
    if (countriesDirty) saveCountries();
    WiFi.disconnect(true);
    WiFi.mode(WIFI_OFF);
    delay(1000);
    ESP.restart();
}

static String jsonEscape(const String& s) {
    String out;
    out.reserve(s.length() + 8);
    for (size_t i = 0; i < s.length(); i++) {
        char c = s[i];
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if ((unsigned char)c < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", (unsigned char)c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

// Case-insensitive substring match on ASCII letters. Used by guestbook search to filter entries by name + message content.
static bool containsCI(const char* haystack, int haylen, const char* needle, int needlen) {
    if (needlen == 0) return true;
    if (haylen < needlen) return false;
    for (int i = 0; i <= haylen - needlen; i++) {
        bool ok = true;
        for (int j = 0; j < needlen; j++) {
            char hc = haystack[i + j];
            char nc = needle[j];
            if (hc >= 'A' && hc <= 'Z') hc += 32;
            if (nc >= 'A' && nc <= 'Z') nc += 32;
            if (hc != nc) { ok = false; break; }
        }
        if (ok) return true;
    }
    return false;
}

bool adminAuth(AsyncWebServerRequest *request) {
    if (!isLocalIP(request->client()->remoteIP()) || request->hasHeader("CF-Connecting-IP")) {
        AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/404.html", "text/html");
        r->setCode(404);
        r->addHeader("Cache-Control", "public, max-age=3600");
        request->send(r);
        return false;
    }
    IPAddress ipAddr = request->client()->remoteIP();
    uint32_t ipKey = (uint32_t)ipAddr;
    if (isAuthLockedOut(ipKey)) {
        AsyncWebServerResponse *r = request->beginResponse(429, "text/plain",
            "Too many failed attempts. Try again in 10 minutes.");
        r->addHeader("Retry-After", "600");
        request->send(r);
        return false;
    }
    if (!request->authenticate(cfgAdminUser, cfgAdminPass)) {
        // only count as a real failure when creds were actually supplied
        if (request->hasHeader("Authorization")) recordAuthFail(ipKey);
        request->requestAuthentication();
        return false;
    }
    clearAuthFails(ipKey);
    return true;
}

// Heat index calculation
float calcHeatIndex(float temp_f, float humidity) {
    // Steadman approximation
    float simple = 0.5f * (temp_f + 61.0f + ((temp_f - 68.0f) * 1.2f) + (humidity * 0.094f));

    if (simple < 80.0f) {
        return simple;
    }

    // Rothfusz regression
    float T = temp_f;
    float R = humidity;
    float hi = -42.379f
        + 2.04901523f   * T
        + 10.14333127f  * R
        - 0.22475541f   * T * R
        - 0.00683783f   * T * T
        - 0.05481717f   * R * R
        + 0.00122874f   * T * T * R
        + 0.00085282f   * T * R * R
        - 0.00000199f   * T * T * R * R;

    // low humidity adjustment
    if (R < 13.0f && T >= 80.0f && T <= 112.0f) {
        hi -= ((13.0f - R) / 4.0f) * sqrtf((17.0f - fabsf(T - 95.0f)) / 17.0f);
    }
    // high humidity adjustment
    if (R > 85.0f && T >= 80.0f && T <= 87.0f) {
        hi += ((R - 85.0f) / 10.0f) * ((87.0f - T) / 5.0f);
    }

    return hi;
}

// NTP timestamps
String getTimestamp() {
    struct tm timeinfo;
    if (!getLocalTime(&timeinfo)) return "unknown";
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S%z", &timeinfo);
    return String(buf);
}

String getLogFilename() {
    struct tm timeinfo;
    if (!getLocalTime(&timeinfo)) return "/logs/fallback.csv";
    char yearDir[12], buf[40];
    strftime(yearDir, sizeof(yearDir), "/logs/%Y", &timeinfo);
    if (!SD.exists(yearDir)) SD.mkdir(yearDir);
    strftime(buf, sizeof(buf), "/logs/%Y/%Y-%m-%d.csv", &timeinfo);
    return String(buf);
}

// Error log: single file at /logs/errors.log, rolls over to .old at 64 KB.
// Rate-limited to 5 writes/sec to protect the SD during runaway spam.
#define ERROR_LOG_PATH "/logs/errors.log"
#define ERROR_LOG_BAK  "/logs/errors.log.old"
#define ERROR_LOG_MAX  65536

void logError(const char* tag, const char* msg) {
    if (!tag) tag = "?";
    if (!msg) msg = "";
    // always mirror to serial for live debugging
    Serial.printf("[err] %s | %s\n", tag, msg);
    // rate-limit: max 5 writes per second to guard against crash-loop spam
    static unsigned long windowMs = 0;
    static int windowCount = 0;
    if (millis() - windowMs > 1000) { windowMs = millis(); windowCount = 0; }
    if (++windowCount > 5) return;
    // rotate if current log is at cap
    if (SD.exists(ERROR_LOG_PATH)) {
        File chk = SD.open(ERROR_LOG_PATH, FILE_READ);
        size_t sz = chk ? chk.size() : 0;
        if (chk) chk.close();
        if (sz >= ERROR_LOG_MAX) {
            if (SD.exists(ERROR_LOG_BAK)) SD.remove(ERROR_LOG_BAK);
            SD.rename(ERROR_LOG_PATH, ERROR_LOG_BAK);
        }
    }
    File f = SD.open(ERROR_LOG_PATH, FILE_APPEND);
    if (!f) return;
    f.print(getTimestamp());
    f.print(" | ");
    f.print(tag);
    f.print(" | ");
    f.println(msg);
    f.close();
}

// Visitor count
int readVisitorCount() {
    if (!SD.exists("/visitors.txt") && SD.exists("/visitors.tmp"))
        SD.rename("/visitors.tmp", "/visitors.txt");

    File f = SD.open("/visitors.txt", FILE_READ);
    if (!f) return 0;
    char buf[16];
    size_t n = f.read((uint8_t*)buf, sizeof(buf) - 1);
    buf[n] = '\0';
    f.close();
    return atoi(buf);
}

void writeVisitorCount(int count) {
    File tmp = SD.open("/visitors.tmp", FILE_WRITE);
    if (!tmp) return;
    tmp.println(count);
    tmp.close();

    if (SD.exists("/visitors.bak")) SD.remove("/visitors.bak");
    if (SD.exists("/visitors.txt")) SD.rename("/visitors.txt", "/visitors.bak");
    if (!SD.rename("/visitors.tmp", "/visitors.txt")) {
        if (SD.exists("/visitors.bak")) SD.rename("/visitors.bak", "/visitors.txt");
        return;
    }
    if (SD.exists("/visitors.bak")) SD.remove("/visitors.bak");
}

// True if s is 8 characters of Crockford base32 (digits + a-z minus i/l/o/u).
static bool isValidCrockfordId(const char* s, int len) {
    if (len != 8) return false;
    for (int i = 0; i < 8; i++) {
        char c = s[i];
        bool digit  = (c >= '0' && c <= '9');
        bool letter = (c >= 'a' && c <= 'z') && c != 'i' && c != 'l' && c != 'o' && c != 'u';
        if (!(digit || letter)) return false;
    }
    return true;
}

// Validates a reply candidate's parent: parent must exist, be approved, and
// be either top-level or itself a reply to a top-level. Caps nesting at 2
// levels in the data layer regardless of what the client sent.
static bool isValidReplyParent(const char* parentId) {
    if (!SD.exists("/guestbook.csv")) return false;
    File f = SD.open("/guestbook.csv", FILE_READ);
    if (!f) return false;
    char line[400];
    bool parentFound = false;
    bool parentIsTopLevel = false;
    char parentsParentId[9] = {0};
    // First pass: find the parent row.
    while (f.available()) {
        int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
        if (n <= 0) continue;
        line[n] = '\0';
        while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
        if (n == 0) continue;
        int c1=-1,c2=-1,c3=-1,c4=-1,c5=-1,cLast=-1;
        for (int j = 0; j < n; j++) {
            if (line[j] == ',') {
                if      (c1 < 0) c1 = j;
                else if (c2 < 0) c2 = j;
                else if (c3 < 0) c3 = j;
                else if (c4 < 0) c4 = j;
                else if (c5 < 0) c5 = j;
                cLast = j;
            }
        }
        if (c1<0||c2<0||c3<0||c4<0||c5<0||cLast<0||cLast<=c5) continue;
        int idLen = c5 - c4 - 1;
        if (idLen != 8 || memcmp(line + c4 + 1, parentId, 8) != 0) continue;
        if (line[n-1] != '1') break;  // parent not approved
        parentFound = true;
        int replyToLen = cLast - c5 - 1;
        if (replyToLen == 0) {
            parentIsTopLevel = true;
        } else if (replyToLen == 8) {
            memcpy(parentsParentId, line + c5 + 1, 8);
            parentsParentId[8] = '\0';
        }
        break;
    }
    if (!parentFound) { f.close(); return false; }
    if (parentIsTopLevel)  { f.close(); return true; }
    if (parentsParentId[0] == '\0') { f.close(); return false; }

    // Second pass: the parent is itself a reply; its parent (the grandparent
    // of the new reply) must be top-level. Allowing this means the new entry
    // lands at level 2 at most.
    f.seek(0);
    bool grandIsTopLevel = false;
    while (f.available()) {
        int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
        if (n <= 0) continue;
        line[n] = '\0';
        while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
        if (n == 0) continue;
        int c1=-1,c2=-1,c3=-1,c4=-1,c5=-1,cLast=-1;
        for (int j = 0; j < n; j++) {
            if (line[j] == ',') {
                if      (c1 < 0) c1 = j;
                else if (c2 < 0) c2 = j;
                else if (c3 < 0) c3 = j;
                else if (c4 < 0) c4 = j;
                else if (c5 < 0) c5 = j;
                cLast = j;
            }
        }
        if (c1<0||c2<0||c3<0||c4<0||c5<0||cLast<0||cLast<=c5) continue;
        int idLen = c5 - c4 - 1;
        if (idLen != 8 || memcmp(line + c4 + 1, parentsParentId, 8) != 0) continue;
        if (line[n-1] != '1') break;
        int replyToLen = cLast - c5 - 1;
        if (replyToLen == 0) grandIsTopLevel = true;
        break;
    }
    f.close();
    return grandIsTopLevel;
}

// Generate an 8-char Crockford base32 ID for guestbook entries.
static void generateGuestbookId(char out[9]) {
    static const char* b32 = "0123456789abcdefghjkmnpqrstvwxyz";  // Crockford
    uint64_t r = ((uint64_t)esp_random() << 32) | esp_random();
    for (int i = 0; i < 8; i++) {
        out[i] = b32[r & 0x1F];  // bottom 5 bits
        r >>= 5;
    }
    out[8] = '\0';
}

// Returns the schema version stamped in /guestbook.schema, or 0 if the file
// is missing or unreadable (which we treat as pre-versioning).
static int readGuestbookSchemaVersion() {
    if (!SD.exists("/guestbook.schema")) return 0;
    File f = SD.open("/guestbook.schema", FILE_READ);
    if (!f) return 0;
    int v = 0;
    while (f.available()) {
        char c = f.read();
        if (c >= '0' && c <= '9') v = v * 10 + (c - '0');
        else break;
    }
    f.close();
    return v;
}

static bool writeGuestbookSchemaVersion(int v) {
    File f = SD.open("/guestbook.schema", FILE_WRITE);
    if (!f) return false;
    f.println(v);
    f.close();
    return true;
}

// Hang the device with an OLED message instead of booting further. Called
// from inside a failing migration so we don't serve corrupted data.
static void migrationAbort(const char* msg) {
    Serial.printf("[migrate] ABORT: %s\n", msg);
    logError("migrate", msg);
    display.clearDisplay();
    display.setTextSize(1);
    display.setCursor(0, 0);
    display.println(F("MIGRATION FAILED"));
    display.println(F("Power off + check"));
    display.println(F("SD card."));
    display.println();
    display.println(msg);
    display.display();
    while (1) { delay(1000); }
}

// Current guestbook schema (v3): time,country,name,message,id,reply_to,status
// In-firmware migrations were stripped in v1.4 to free flash. Stale SD cards
// from pre-v1.3 firmware must flash v1.3 first to run v1->v2->v3 migrations,
// then upgrade. The boot-time guard in setup() (readGuestbookSchemaVersion)
// halts the device with a clear OLED + serial message when stale data is
// detected, rather than serving corrupted rows.

// Recompute guestbook counts (pendingGuestbook, gbCountApproved, gbCountDenied,
// gbCountAll) by streaming through guestbook.csv. Avoids loading the whole file
// into RAM. Called once at boot and as a safety net after delete/compact operations.
void countPendingGuestbook() {
    pendingGuestbook = 0;
    gbCountApproved = 0;
    gbCountDenied = 0;
    gbCountAll = 0;
    if (!SD.exists("/guestbook.csv")) return;
    File f = SD.open("/guestbook.csv", FILE_READ);
    if (!f) return;
    char line[400];
    while (f.available()) {
        int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
        if (n <= 0) continue;
        line[n] = '\0';
        // trim trailing whitespace / CR
        while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
        if (n == 0) continue;
        char s = line[n-1];
        // Tombstones (status=3) stay physically in the CSV so reply chains can
        // reference them, but they don't count toward any user-facing total.
        if (s == '3') continue;
        gbCountAll++;
        if      (s == '0') pendingGuestbook++;
        else if (s == '1') gbCountApproved++;
        else if (s == '2') gbCountDenied++;
    }
    f.close();
}

// Poll the configured Shelly Gen 2+ smart plug for live power draw and
// software-integrate the result into our running Wh accumulators. Pulls
// only `apower` (W); we don't trust the Shelly's own aenergy.total
// counter because some firmware versions reset it on reboot, factory
// reset, or via the app. By integrating apower ourselves, our totals are
// independent of the Shelly's internal state.
//
// Called from loop() every SHELLY_POLL_INTERVAL_MS. Skips silently if no
// shelly_url is configured. Failures (timeout, non-200, parse error) just
// don't update the cache; the staleness check in buildStatsJson() hides
// the banner after SHELLY_STALE_MS without a successful read.
//
// The integration uses elapsed wall-clock time between successful polls,
// not a fixed 30s assumption, so a missed poll doesn't underestimate the
// energy used during that gap (capped at 5 min to prevent runaway delta
// if the integrator stalled for a long time).
void pollShelly() {
    if (cfgShellyUrl[0] == '\0') return;
    if (WiFi.status() != WL_CONNECTED) return;

    lastShellyAttemptMs = millis();

    HTTPClient http;
    // 1500ms cap (was 3000ms): synchronous HTTPClient holds the lwip
    // mutex for the duration of the call. If pollShelly overlaps with
    // WS handshake / heavy activity, async_tcp can't acquire the mutex
    // and its 5s task watchdog trips. Half the timeout = half the worst-
    // case lwip-lock window. A LAN Shelly responds in <100ms when up,
    // so 1500ms is plenty for healthy polls but bounds the bad case.
    http.setTimeout(1500);
    // Use the canonical POST RPC form rather than GET-with-querystring.
    // ESP32 HTTPClient's URL parser drops the `?id=0` query parameter
    // before it reaches the Shelly, so the GET form returns HTTP 500
    // ("Missing required argument 'id'"). POST puts the id in the JSON
    // body where it can't be stripped, and is the documented primary
    // RPC interface so it's better tested across firmware revisions.
    String url = String(cfgShellyUrl) + "/rpc";
    if (!http.begin(url)) {
        lastShellyHttpStatus = -1;
        shellyConsecutiveFailures++;
        return;
    }
    http.addHeader("Accept", "application/json");
    http.addHeader("Content-Type", "application/json");
    // Top-level id is the switch channel (matches the GET querystring's
    // `?id=0`, not JSON-RPC 2.0's request id). This Shelly firmware
    // 500s on the nested `params.id` form with "value 1 not found"
    // because it reads the outer id as the channel.
    int code = http.POST("{\"method\":\"Switch.GetStatus\",\"id\":0}");
    if (code != 200) {
        // Capture the response body once per outage for diagnostics. Logs
        // only on the first failure of each consecutive-failure run so we
        // don't fill errors.log under sustained Shelly downtime.
        if (shellyConsecutiveFailures == 0) {
            String errBody = http.getString();
            if (errBody.length() > 200) errBody = errBody.substring(0, 200);
            char buf[256];
            snprintf(buf, sizeof(buf), "shelly poll failed: HTTP %d body=\"%s\"",
                     code, errBody.c_str());
            logError("shelly", buf);
        }
        http.end();
        lastShellyHttpStatus = code;
        shellyConsecutiveFailures++;
        return;
    }
    String body = http.getString();
    http.end();

    // Lightweight indexOf parse, matching the rest of main.cpp's pattern.
    int p = body.indexOf("\"apower\":");
    if (p < 0) {
        lastShellyHttpStatus = -100;
        shellyConsecutiveFailures++;
        return;
    }
    float power_w = body.substring(p + 9).toFloat();
    // Sanity bound: Shelly Gen 4 plug max is 16A × 240V ≈ 3840W; anything
    // outside [-1, 4000] is a parse glitch. Negative skipped because we
    // don't track returned energy here.
    if (power_w < 0.0f || power_w > 4000.0f) {
        lastShellyHttpStatus = -101;
        shellyConsecutiveFailures++;
        return;
    }

    lastShellyHttpStatus = 200;
    shellyConsecutiveFailures = 0;
    cached_power_w = power_w;

    // Accumulate for the per-interval CSV average. Reset by logStats() on
    // each write. Independent from the energy integrator above; this is
    // purely chart-display fidelity, not energy math.
    interval_power_sum   += power_w;
    interval_power_count += 1;

    // Software integration: P × dt = E. dt comes from actual elapsed time
    // since the last successful integration, not a fixed assumption.
    unsigned long now = millis();
    if (lastShellyIntegrationMs > 0 && now > lastShellyIntegrationMs) {
        unsigned long dt_ms = now - lastShellyIntegrationMs;
        // Cap dt at 5 min to bound runaway integrals if the integrator
        // stalled (e.g., long WiFi outage between polls). 5 min × 4kW max
        // sanity = 333 Wh, still bounded.
        if (dt_ms > 300000UL) dt_ms = 300000UL;
        float dt_hours = dt_ms / 3600000.0f;
        float delta_wh = power_w * dt_hours;

        lifetime_energy_wh += delta_wh;
        today_energy_wh    += delta_wh;
        week_energy_wh     += delta_wh;
        month_energy_wh    += delta_wh;
        year_energy_wh     += delta_wh;
    }
    lastShellyIntegrationMs = now;
    lastShellyOk = now;

    // Capture tracking-start unix on first successful poll, so the banner
    // can show "since X" if/when we want that framing later.
    if (shelly_tracking_started_unix == 0) {
        time_t t = time(nullptr);
        if (t > 1735689600) shelly_tracking_started_unix = (uint32_t)t;
    }

    // First-after-boot records.json refresh. saveCheckpoint piggybacks
    // saveRecords every 15 min, but a fresh boot needs records.json
    // populated with rates + lifetime + tracking date sooner so the
    // homepage banner / about placard / history lifetime row don't sit
    // hidden waiting on the first piggyback. Single SD write per boot,
    // gated on lifetime > 0 (otherwise saveRecords would emit nothing
    // useful anyway).
    static bool initialRecordsSavedThisBoot = false;
    if (!initialRecordsSavedThisBoot && lifetime_energy_wh > 0.0f) {
        saveRecords();
        initialRecordsSavedThisBoot = true;
    }
}

void notifyPendingIfIncreased() {
    if (pendingGuestbook <= lastNotifiedPending) {
        lastNotifiedPending = pendingGuestbook;
        return;
    }
    if (!wsConnected || !wsClient.connected()) return;
    String msg = "{\"type\":\"event\",\"event\":\"pending_guestbook\",\"count\":";
    msg += pendingGuestbook;
    // Skip per-entry preview when delta > 1; the notifyEntry* buffers hold
    // only the most-recent submission and would mis-attribute under bursts.
    int delta = pendingGuestbook - lastNotifiedPending;
    if (notifyEntryName[0] != '\0' && delta == 1) {
        // jsonEscape rather than relying on sanitizeCsv; loosening CSV
        // escaping later shouldn't silently break the worker's JSON.parse.
        msg += ",\"name\":\"";    msg += jsonEscape(notifyEntryName);    msg += "\"";
        msg += ",\"country\":\""; msg += jsonEscape(notifyEntryCountry); msg += "\"";
        msg += ",\"message\":\""; msg += jsonEscape(notifyEntryMessage); msg += "\"";
    }
    msg += "}";
    wsSendText(wsClient, msg);
    lastNotifiedPending = pendingGuestbook;
    notifyEntryName[0] = '\0';
}

// Daily visitors
void loadDailyVisitors() {
    if (!SD.exists("/daily.txt") && SD.exists("/daily.tmp"))
        SD.rename("/daily.tmp", "/daily.txt");
    if (!SD.exists("/daily.txt")) { dailyVisitors = 0; return; }
    File f = SD.open("/daily.txt", FILE_READ);
    if (!f) { dailyVisitors = 0; return; }
    char buf[64];
    size_t n = f.read((uint8_t*)buf, sizeof(buf) - 1);
    buf[n] = '\0';
    f.close();
    String all(buf);
    int comma = all.indexOf(',');
    String date = comma > 0 ? all.substring(0, comma) : "";
    int nl = all.indexOf('\n', comma + 1);
    int count = comma > 0 ? all.substring(comma + 1, nl > 0 ? nl : all.length()).toInt() : 0;

    // Preserve disk count regardless of NTP state. The visit-handler's
    // tm_mday != lastVisitorDay check resets to 0 cleanly once NTP arrives
    // if the date no longer matches; pre-NTP zeroing would lose yesterday's
    // count before Hall-of-Fame records it.
    dailyVisitors = count;
    if (date.length() == 10) {
        strncpy(dailyVisitorsDate, date.c_str(), sizeof(dailyVisitorsDate) - 1);
        dailyVisitorsDate[sizeof(dailyVisitorsDate) - 1] = '\0';
    }

    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {
        char today[12];
        strftime(today, sizeof(today), "%Y-%m-%d", &timeinfo);
        if (date == String(today)) {
            lastVisitorDay = timeinfo.tm_mday;
        } else {
            // Different day: zero in-memory tally; visit handler sets
            // lastVisitorDay on first visit. Keep dailyVisitorsDate as the
            // OLD date so a pending Hall-of-Fame check still attributes right.
            dailyVisitors = 0;
        }
    }
    // If NTP failed: keep loaded count, leave lastVisitorDay = -1 so the
    // first visit triggers a clean rollover check.
}

void saveDailyVisitors() {
    struct tm timeinfo;
    if (!getLocalTime(&timeinfo)) return;
    char today[12];
    strftime(today, sizeof(today), "%Y-%m-%d", &timeinfo);
    File tmp = SD.open("/daily.tmp", FILE_WRITE);
    if (!tmp) return;
    tmp.print(today);
    tmp.print(",");
    tmp.println(dailyVisitors);
    tmp.close();

    if (SD.exists("/daily.bak")) SD.remove("/daily.bak");
    if (SD.exists("/daily.txt")) SD.rename("/daily.txt", "/daily.bak");
    if (!SD.rename("/daily.tmp", "/daily.txt")) {
        if (SD.exists("/daily.bak")) SD.rename("/daily.bak", "/daily.txt");
        return;
    }
    if (SD.exists("/daily.bak")) SD.remove("/daily.bak");
}

// Sensor readings
struct SensorData {
    float temp_c, temp_f;
    float humidity;
    float pressure_hpa;
    float altitude_ft;
    float heat_index_f, heat_index_c;
    float cpu_temp_c, cpu_temp_f;
    float mem_percent;
    float sd_free_mb;
    float sd_used_mb;
    int   rssi;
};

SensorData readSensors() {
    SensorData d;

    d.temp_c       = safeBmeTemp();
    d.temp_f       = d.temp_c * 9.0f / 5.0f + 32.0f;
    d.humidity     = safeBmeHumidity();
    d.pressure_hpa = safeBmePressureHpa();
    d.altitude_ft  = safeBmeAltitudeFt();

    d.heat_index_f = calcHeatIndex(d.temp_f, d.humidity);
    d.heat_index_c = (d.heat_index_f - 32.0f) * 5.0f / 9.0f;

    d.cpu_temp_c   = temperatureRead();
    d.cpu_temp_f   = d.cpu_temp_c * 9.0f / 5.0f + 32.0f;

    uint32_t free_heap  = ESP.getFreeHeap();
    uint32_t total_heap = ESP.getHeapSize();
    d.mem_percent  = 100.0f * ((float)(total_heap - free_heap) / total_heap);

    uint64_t sd_used   = SD.usedBytes();
    uint64_t sd_free   = SD.totalBytes() - sd_used;
    d.sd_used_mb   = sd_used / (1024.0f * 1024.0f);
    d.sd_free_mb   = sd_free / (1024.0f * 1024.0f);

    d.rssi         = WiFi.RSSI();

    return d;
}

// CSV logging
void logStats() {
    // Skip entirely if NTP isn't synced yet. Otherwise getLogFilename()
    // returns "/logs/fallback.csv" which never gets migrated to a dated file
    // and is invisible to /logs viewers (filename doesn't match
    // YYYY-MM-DD pattern), losing 5-30 min of stats per cold boot.
    struct tm tCheck;
    if (!getLocalTime(&tCheck, 0)) return;

    String filename = getLogFilename();
    bool isNew = !SD.exists(filename);

    File log = SD.open(filename, FILE_APPEND);
    if (!log) return;

    if (isNew) {
        log.println("timestamp,cpu_temp_c,cpu_temp_f,memory_percent,temperature_c,temperature_f,humidity_percent,pressure_hpa,altitude_ft,co2_ppm,voc_ppb,heat_index_c,heat_index_f,rssi,sd_free_mb,requests_interval,power_w");
    }

    int intervalRequests    = requestsThisInterval;
    requestsThisInterval    = 0;

    SensorData d = readSensors();

    log.print(getTimestamp());   log.print(",");
    log.print(d.cpu_temp_c, 2); log.print(",");
    log.print(d.cpu_temp_f, 2); log.print(",");
    log.print(d.mem_percent, 2);log.print(",");
    log.print(d.temp_c, 2);     log.print(",");
    log.print(d.temp_f, 2);     log.print(",");
    log.print(d.humidity, 2);   log.print(",");
    log.print(d.pressure_hpa, 2);log.print(",");
    log.print(d.altitude_ft, 2);log.print(",");
    log.print(cached_co2);      log.print(",");
    log.print(cached_voc);      log.print(",");
    log.print(d.heat_index_c, 2);log.print(",");
    log.print(d.heat_index_f, 2);log.print(",");
    log.print(d.rssi);          log.print(",");
    log.print(d.sd_free_mb, 2); log.print(",");
    log.print(intervalRequests);log.print(",");
    // power_w cell: average of every successful Shelly poll since the
    // last logStats call, rounded to nearest 0.1 W to match Shelly's
    // own apower precision (sub-tenth digits would look like fake
    // precision). Empty when no Shelly configured or no successful
    // polls happened this interval; chart frontend treats empty cells
    // as gaps. Reset the accumulators after every logStats run so the
    // next interval starts fresh whether we logged a value or not.
    if (cfgShellyUrl[0] != '\0' && interval_power_count > 0) {
        float avg = interval_power_sum / (float)interval_power_count;
        float rounded = roundf(avg * 10.0f) / 10.0f;
        log.println(rounded, 1);
    } else {
        log.println("");
    }
    interval_power_sum   = 0.0f;
    interval_power_count = 0;

    log.close();

    aggregateSample(currentWeek,  d.temp_c, d.humidity, cached_co2, cached_voc, intervalRequests);
    aggregateSample(currentMonth, d.temp_c, d.humidity, cached_co2, cached_voc, intervalRequests);
    aggregateSample(currentYear,  d.temp_c, d.humidity, cached_co2, cached_voc, intervalRequests);
    checkPeriodBoundaries();
    updateRecords(d.temp_f, cached_co2, dailyVisitors);
    evalSensorHealth();
}

// Build the /stats JSON payload. Used by the /stats route and the 5s SSE push.
String buildStatsJson() {
    unsigned long startMs = millis();
    SensorData d = readSensors();
    unsigned long responseMs = millis() - startMs;

    int visitors       = cachedVisitorCount;
    uint32_t free_heap = ESP.getFreeHeap();
    uint32_t total_heap= ESP.getHeapSize();
    uint32_t used_bytes= total_heap - free_heap;

    // Chip-local YYYY-MM-DD per cfgTimezone. Worker uses this to bucket the
    // chronicle snapshot so /chronicle/<date> aligns with the chip's lived
    // day rather than UTC. Empty when NTP hasn't synced yet; worker falls
    // back to UTC date in that case.
    char today_local[11] = "";
    int  today_local_hour = -1;
    struct tm tlNow;
    if (getLocalTime(&tlNow, 0)) {
        strftime(today_local, sizeof(today_local), "%Y-%m-%d", &tlNow);
        today_local_hour = tlNow.tm_hour;
    }

    // Build into a stack char buffer with snprintf
    char buf[1536];
    String ts = getTimestamp();
    String ut = uptime_formatter::getUptime();
    snprintf(buf, sizeof(buf),
        "{\"response_ms\":%lu"
        ",\"timestamp\":\"%s\""
        ",\"uptime\":\"%s\""
        ",\"rssi\":%d"
        ",\"cpu_temp\":{\"celsius\":%.2f,\"fahrenheit\":%.2f}"
        ",\"memory\":{\"used_bytes\":%lu,\"used_kb\":%.2f,\"used_percent\":%.2f}"
        ",\"sd_used_mb\":%.2f"
        ",\"sd_free_mb\":%.2f"
        ",\"visitors\":%d"
        ",\"daily_visitors\":%d"
        ",\"temperature\":{\"celsius\":%.2f,\"fahrenheit\":%.2f}"
        ",\"heat_index\":{\"celsius\":%.2f,\"fahrenheit\":%.2f}"
        ",\"altitude_ft\":%.2f"
        ",\"humidity_percent\":%.2f"
        ",\"pressure_hpa\":%.2f"
        ",\"co2_ppm\":%u"
        ",\"voc_ppb\":%u"
        ",\"countries\":%d"
        ",\"guestbook_approved\":%d"
        ",\"sensors\":{\"bme_ok\":%s,\"ccs_ok\":%s,\"oled_ok\":%s,\"rtc_ok\":%s}"
        ",\"today_local\":\"%s\""
        ",\"today_local_hour\":%d"
        "}",
        responseMs,
        ts.c_str(),
        ut.c_str(),
        d.rssi,
        d.cpu_temp_c, d.cpu_temp_f,
        (unsigned long)used_bytes, used_bytes / 1024.0f, d.mem_percent,
        d.sd_used_mb,
        d.sd_free_mb,
        visitors,
        dailyVisitors,
        d.temp_c, d.temp_f,
        d.heat_index_c, d.heat_index_f,
        d.altitude_ft,
        d.humidity,
        d.pressure_hpa,
        (unsigned)cached_co2,
        (unsigned)cached_voc,
        countryCount,
        gbCountApproved,
        bmeDegraded() ? "false" : "true",
        ccsDegraded() ? "false" : "true",
        oledOk ? "true" : "false",
        rtcOk ? "true" : "false",
        today_local,
        today_local_hour);

    // Optional Shelly power fields, appended only when shelly_url is set
    // and a successful poll happened within SHELLY_STALE_MS. Insert before
    // the closing brace so the base JSON shape stays unchanged when no
    // Shelly is configured (frontends can detect the feature by presence).
    if (cfgShellyUrl[0] != '\0' && lastShellyOk > 0
        && (millis() - lastShellyOk) < SHELLY_STALE_MS
        && !isnan(cached_power_w)) {
        String out(buf);
        // Strip trailing '}', append power block, re-close.
        if (out.endsWith("}")) out.remove(out.length() - 1);
        char ext[256];
        snprintf(ext, sizeof(ext),
            ",\"power_w\":%.2f"
            ",\"energy_today_wh\":%.2f"
            ",\"energy_total_wh\":%.1f"
            "}",
            cached_power_w,
            today_energy_wh,
            lifetime_energy_wh);
        out += ext;
        return out;
    }
    return String(buf);
}

// ===== Backup bundle =====
// Streams SD contents to the Worker as events:
//   backup_start -> [backup_file_start -> backup_file_chunk... -> backup_file_end] * N -> backup_end
// Contents are base64-encoded (binary-safe). Worker reassembles and writes to R2
// (or emails as a fallback if no R2 binding).

static void backupSendChunk(uint32_t seq, const uint8_t* data, int rd) {
    static const char* b64c = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    String b64;
    b64.reserve((rd * 4) / 3 + 4);
    for (int i = 0; i < rd; i += 3) {
        uint32_t n = ((uint32_t)data[i]) << 16;
        if (i + 1 < rd) n |= ((uint32_t)data[i + 1]) << 8;
        if (i + 2 < rd) n |= data[i + 2];
        b64 += b64c[(n >> 18) & 0x3F];
        b64 += b64c[(n >> 12) & 0x3F];
        b64 += (i + 1 < rd) ? b64c[(n >> 6) & 0x3F] : '=';
        b64 += (i + 2 < rd) ? b64c[n & 0x3F] : '=';
    }
    String ev = "{\"type\":\"event\",\"event\":\"backup_file_chunk\",\"seq\":";
    ev += seq;
    ev += ",\"data\":\"";
    ev += b64;
    ev += "\"}";
    wsSendText(wsClient, ev);
}

static void backupSendFile(uint32_t seq, const char* path, const char* name, size_t& totalOut) {
    if (!SD.exists(path)) return;
    File f = SD.open(path, FILE_READ);
    if (!f) return;
    size_t fsize = f.size();

    if (fsize > BACKUP_MAX_FILE || totalOut + fsize > BACKUP_MAX_TOTAL) {
        f.close();
        String skip = "{\"type\":\"event\",\"event\":\"backup_file_skipped\",\"seq\":";
        skip += seq;
        skip += ",\"name\":\"";
        skip += jsonEscape(name);
        skip += "\",\"size\":";
        skip += fsize;
        skip += ",\"reason\":\"";
        skip += (fsize > BACKUP_MAX_FILE) ? "file_too_large" : "bundle_full";
        skip += "\"}";
        wsSendText(wsClient, skip);
        return;
    }

    String head = "{\"type\":\"event\",\"event\":\"backup_file_start\",\"seq\":";
    head += seq;
    head += ",\"name\":\"";
    head += jsonEscape(name);
    head += "\",\"size\":";
    head += fsize;
    head += "}";
    wsSendText(wsClient, head);

    uint8_t buf[3072];
    while (f.available()) {
        // If the WS dropped mid-stream, the wsSendText calls below would
        // no-op and we'd still pay the SD-read + base64-encode cost for
        // every remaining chunk on every remaining file.
        if (!wsConnected || !wsClient.connected()) break;
        int rd = f.read(buf, sizeof(buf));
        if (rd <= 0) break;
        backupSendChunk(seq, buf, rd);
        yield();
    }
    f.close();
    totalOut += fsize;

    String tail = "{\"type\":\"event\",\"event\":\"backup_file_end\",\"seq\":";
    tail += seq;
    tail += ",\"name\":\"";
    tail += jsonEscape(name);
    tail += "\"}";
    wsSendText(wsClient, tail);
}

// Exclusion rules for the recursive SD walker. Secrets, transient atomic-write artifacts, and
// on-device log rotation don't belong in a restore bundle.
static bool backupExcludeDir(const char* absPath) {
    if (strcmp(absPath, "/logs") == 0)  return true;
    if (strcmp(absPath, "/fw") == 0)    return true;  // firmware staging, not backup-worthy (1-2MB each)
    if (strcmp(absPath, "/System Volume Information") == 0) return true;
    return false;
}

static bool backupExcludeFile(const char* absPath, const char* base) {
    if (strcmp(absPath, "/config.txt") == 0) return true;
    size_t bl = strlen(base);
    if (bl >= 4) {
        const char* ext = base + bl - 4;
        if (strcmp(ext, ".tmp") == 0) return true;
        if (strcmp(ext, ".bak") == 0) return true;
    }
    // Migration snapshots (.v1, .v2, .v3 etc) are duplicates of the live file.
    // Skip so each schema bump doesn't double-up the backup size forever.
    if (bl >= 3 && base[bl - 3] == '.' && base[bl - 2] == 'v' &&
        base[bl - 1] >= '0' && base[bl - 1] <= '9') return true;
    if (base[0] == '.') return true; // hidden / OS metadata (._DS_Store etc)
    return false;
}

// Recursive SD walker. Logical path is the SD-relative path without the leading slash
// (e.g. "stats/weekly/2026-W16.json") so it maps cleanly onto an R2 object key.
// depth limit keeps us within ESP32 SD / VFS concurrent-handle headroom; each recursion
// level holds one open directory plus potentially one open file at the leaf.
#define BACKUP_MAX_DEPTH 6
static void backupSendRecursive(uint32_t seq, const char* absPath, size_t& totalOut, int depth) {
    if (depth > BACKUP_MAX_DEPTH) {
        String skip = "{\"type\":\"event\",\"event\":\"backup_file_skipped\",\"seq\":";
        skip += seq;
        skip += ",\"name\":\"";
        skip += jsonEscape(absPath);
        skip += "\",\"size\":0,\"reason\":\"depth_exceeded\"}";
        wsSendText(wsClient, skip);
        return;
    }
    if (backupExcludeDir(absPath)) return;
    File dir = SD.open(absPath);
    if (!dir) return;
    if (!dir.isDirectory()) { dir.close(); return; }

    File entry = dir.openNextFile();
    while (entry) {
        String fullName = entry.name();
        int slash = fullName.lastIndexOf('/');
        String base = (slash >= 0) ? fullName.substring(slash + 1) : fullName;
        bool isDir = entry.isDirectory();
        entry.close();

        String childAbs;
        if (strcmp(absPath, "/") == 0) {
            childAbs = "/";
            childAbs += base;
        } else {
            childAbs = String(absPath) + "/" + base;
        }

        if (isDir) {
            backupSendRecursive(seq, childAbs.c_str(), totalOut, depth + 1);
        } else if (!backupExcludeFile(childAbs.c_str(), base.c_str())) {
            // logical = absolute without leading slash
            const char* logical = childAbs.c_str();
            if (logical[0] == '/') logical++;
            backupSendFile(seq, childAbs.c_str(), logical, totalOut);
        }
        yield();
        entry = dir.openNextFile();
    }
    dir.close();
}

// Persist last-successful-backup date so a reboot doesn't cause a same-day re-fire.
void loadLastBackupDate() {
    lastBackupDate[0] = '\0';
    if (!SD.exists(LAST_BACKUP_PATH) && SD.exists(LAST_BACKUP_TMP))
        SD.rename(LAST_BACKUP_TMP, LAST_BACKUP_PATH);
    if (!SD.exists(LAST_BACKUP_PATH)) return;
    File f = SD.open(LAST_BACKUP_PATH, FILE_READ);
    if (!f) return;
    char buf[16];
    size_t n = f.read((uint8_t*)buf, sizeof(buf) - 1);
    f.close();
    buf[n] = '\0';
    // trim trailing whitespace
    while (n > 0 && (buf[n-1] == '\n' || buf[n-1] == '\r' || buf[n-1] == ' ')) { buf[--n] = '\0'; }
    // expect YYYY-MM-DD; anything else stays empty
    if (n == 10 && buf[4] == '-' && buf[7] == '-') {
        strncpy(lastBackupDate, buf, sizeof(lastBackupDate) - 1);
        lastBackupDate[sizeof(lastBackupDate) - 1] = '\0';
    }
}

static void saveLastBackupDate(const char* date) {
    File f = SD.open(LAST_BACKUP_TMP, FILE_WRITE);
    if (!f) return;
    f.print(date);
    f.close();
    if (SD.exists(LAST_BACKUP_PATH)) SD.remove(LAST_BACKUP_PATH);
    SD.rename(LAST_BACKUP_TMP, LAST_BACKUP_PATH);
}

#define MAINTENANCE_STATE_PATH "/stats/maintenance.txt"
#define MAINTENANCE_STATE_TMP  "/stats/maintenance.tmp"

// Persist maintenance window across reboots so the admin panel keeps showing
// "currently in maintenance" instead of dropping to "off" while the Worker
// still serves the maintenance page to public visitors.
void loadMaintenanceState() {
    localMaintenanceUntilUnix = 0;
    localMaintenanceMessage[0] = '\0';
    if (!SD.exists(MAINTENANCE_STATE_PATH) && SD.exists(MAINTENANCE_STATE_TMP))
        SD.rename(MAINTENANCE_STATE_TMP, MAINTENANCE_STATE_PATH);
    if (!SD.exists(MAINTENANCE_STATE_PATH)) return;
    File f = SD.open(MAINTENANCE_STATE_PATH, FILE_READ);
    if (!f) return;
    String tsLine  = f.readStringUntil('\n');
    String msgLine = f.readStringUntil('\n');
    f.close();
    tsLine.trim();
    msgLine.trim();
    uint32_t ts = (uint32_t)tsLine.toInt();
    if (ts == 0) return;
    // If the window already expired by the time we boot, drop the file
    // and stay inactive. nowSec == 0 means NTP hasn't synced yet; in that
    // case keep the saved value, the active check in /admin/info gates on
    // a real nowSec anyway.
    time_t nowSec = time(nullptr);
    if (nowSec > 0 && (uint32_t)nowSec >= ts) {
        SD.remove(MAINTENANCE_STATE_PATH);
        return;
    }
    localMaintenanceUntilUnix = ts;
    strncpy(localMaintenanceMessage, msgLine.c_str(), sizeof(localMaintenanceMessage) - 1);
    localMaintenanceMessage[sizeof(localMaintenanceMessage) - 1] = '\0';
}

static void saveMaintenanceState() {
    if (localMaintenanceUntilUnix == 0) {
        if (SD.exists(MAINTENANCE_STATE_PATH)) SD.remove(MAINTENANCE_STATE_PATH);
        if (SD.exists(MAINTENANCE_STATE_TMP)) SD.remove(MAINTENANCE_STATE_TMP);
        return;
    }
    File f = SD.open(MAINTENANCE_STATE_TMP, FILE_WRITE);
    if (!f) return;
    f.println(localMaintenanceUntilUnix);
    f.println(localMaintenanceMessage);
    f.close();
    if (SD.exists(MAINTENANCE_STATE_PATH)) SD.remove(MAINTENANCE_STATE_PATH);
    SD.rename(MAINTENANCE_STATE_TMP, MAINTENANCE_STATE_PATH);
}

// Persist the Worker's backup_committed confirmation so admin UI state survives reboots.
// Format: single line "YYYY-MM-DD BYTES FILES UNIX_SEC"
void loadLastCommit() {
    r2CommittedDate[0] = '\0';
    r2CommittedBytes = 0;
    r2CommittedFiles = 0;
    r2CommittedAtUnix = 0;
    if (!SD.exists(LAST_COMMIT_PATH) && SD.exists(LAST_COMMIT_TMP))
        SD.rename(LAST_COMMIT_TMP, LAST_COMMIT_PATH);
    if (!SD.exists(LAST_COMMIT_PATH)) return;
    File f = SD.open(LAST_COMMIT_PATH, FILE_READ);
    if (!f) return;
    String line = f.readStringUntil('\n');
    f.close();
    line.trim();
    int p1 = line.indexOf(' ');
    if (p1 <= 0) return;
    int p2 = line.indexOf(' ', p1 + 1);
    if (p2 <= 0) return;
    int p3 = line.indexOf(' ', p2 + 1);
    if (p3 <= 0) return;
    String dateStr = line.substring(0, p1);
    if (dateStr.length() != 10 || dateStr[4] != '-' || dateStr[7] != '-') return;
    strncpy(r2CommittedDate, dateStr.c_str(), sizeof(r2CommittedDate) - 1);
    r2CommittedDate[sizeof(r2CommittedDate) - 1] = '\0';
    r2CommittedBytes  = (uint32_t)line.substring(p1 + 1, p2).toInt();
    r2CommittedFiles  = (uint16_t)line.substring(p2 + 1, p3).toInt();
    r2CommittedAtUnix = (uint32_t)line.substring(p3 + 1).toInt();
}

static void saveLastCommit() {
    if (SD.exists(LAST_COMMIT_TMP)) SD.remove(LAST_COMMIT_TMP);
    File f = SD.open(LAST_COMMIT_TMP, FILE_WRITE);
    if (!f) return;
    f.printf("%s %u %u %u",
             r2CommittedDate[0] ? r2CommittedDate : "0000-00-00",
             (unsigned)r2CommittedBytes,
             (unsigned)r2CommittedFiles,
             (unsigned)r2CommittedAtUnix);
    f.close();
    if (SD.exists(LAST_COMMIT_PATH)) SD.remove(LAST_COMMIT_PATH);
    SD.rename(LAST_COMMIT_TMP, LAST_COMMIT_PATH);
}

// Returns true if wall-clock says it's time to fire today's backup.
// Daily cadence at BACKUP_HOUR_LOCAL. Survives reboots because lastBackupDate is persisted.
static bool isBackupDue() {
    struct tm tm;
    if (!getLocalTime(&tm, 0)) return false;         // no NTP yet
    if (tm.tm_hour < BACKUP_HOUR_LOCAL) return false; // before the daily window
    char today[11];
    snprintf(today, sizeof(today), "%04d-%02d-%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    return strcmp(today, lastBackupDate) != 0;
}

// True for the duration of a buildAndSendBackup() call. The 60s safety-net
// reboot in loop() includes this in its localBusy gate so a slow backup
// (~30-60s on a full SD) doesn't get cut off and leave R2 with a partial
// snapshot. Volatile because the safety-net read runs from the same task
// after backupRunning is set, but defensive in case async_tcp pre-empts.
volatile bool backupRunning = false;

void buildAndSendBackup() {
    if (!wsConnected || !wsClient.connected()) return;

    backupRunning = true;
    uint32_t seq = (uint32_t)millis();
    size_t totalOut = 0;

    String start = "{\"type\":\"event\",\"event\":\"backup_start\",\"seq\":";
    start += seq;
    start += ",\"generated_at\":\"";
    start += getTimestamp();
    start += "\",\"firmware\":\"";
    start += FIRMWARE_VERSION;
    start += "\",\"uptime\":\"";
    start += uptime_formatter::getUptime();
    start += "\"}";
    wsSendText(wsClient, start);

    backupSendRecursive(seq, "/", totalOut, 0);

    String end = "{\"type\":\"event\",\"event\":\"backup_end\",\"seq\":";
    end += seq;
    end += ",\"size\":";
    end += totalOut;
    end += "}";
    wsSendText(wsClient, end);

    lastBackupSentMs = millis();

    // Record today's date so the daily scheduler doesn't re-fire.
    struct tm tm;
    if (getLocalTime(&tm, 0)) {
        char today[11];
        snprintf(today, sizeof(today), "%04d-%02d-%02d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
        strncpy(lastBackupDate, today, sizeof(lastBackupDate) - 1);
        lastBackupDate[sizeof(lastBackupDate) - 1] = '\0';
        saveLastBackupDate(today);
    }
    backupRunning = false;
}

// handle incoming WebSocket message from Worker
void handleWsRelay(String& data) {
    // Worker -> ESP events share this channel with relayed HTTP requests. Event branches
    // handle result messages (backup commit, r2 healthcheck, test email) before falling
    // through to HTTP relay parsing for normal browser traffic.
    if (data.indexOf("\"type\":\"event\"") >= 0) {
        if (data.indexOf("\"event\":\"test_email_result\"") >= 0) {
            int p1 = data.indexOf("\"pass\":");
            if (p1 >= 0) {
                testEmailPass = data.indexOf("true", p1) == p1 + 7;
            }
            int d1 = data.indexOf("\"detail\":\"");
            if (d1 >= 0) {
                d1 += 10;
                int d2 = data.indexOf("\"", d1);
                if (d2 > d1) {
                    String ds = data.substring(d1, d2);
                    if (ds.length() > 128) ds = ds.substring(0, 128);
                    strncpy(testEmailDetail, ds.c_str(), sizeof(testEmailDetail) - 1);
                    testEmailDetail[sizeof(testEmailDetail) - 1] = '\0';
                }
            }
            struct tm tm;
            if (getLocalTime(&tm, 0)) testEmailAtUnix = mktime(&tm);
            data = "";
            return;
        }
        if (data.indexOf("\"event\":\"snake_clear_result\"") >= 0) {
            int p1 = data.indexOf("\"ok\":");
            if (p1 >= 0) snakeClearOk = (data.indexOf("true", p1) == p1 + 5);
            struct tm tm;
            if (getLocalTime(&tm, 0)) snakeClearAtUnix = mktime(&tm);
            data = "";
            return;
        }
        if (data.indexOf("\"event\":\"r2_healthcheck_result\"") >= 0) {
            int p1 = data.indexOf("\"pass\":");
            if (p1 >= 0) {
                r2HealthcheckPass = data.indexOf("true", p1) == p1 + 7;
            }
            int d1 = data.indexOf("\"detail\":\"");
            if (d1 >= 0) {
                d1 += 10;
                int d2 = data.indexOf("\"", d1);
                if (d2 > d1) {
                    String ds = data.substring(d1, d2);
                    if (ds.length() > 128) ds = ds.substring(0, 128);
                    strncpy(r2HealthcheckDetail, ds.c_str(), sizeof(r2HealthcheckDetail) - 1);
                    r2HealthcheckDetail[sizeof(r2HealthcheckDetail) - 1] = '\0';
                }
            }
            struct tm tm;
            if (getLocalTime(&tm, 0)) r2HealthcheckAtUnix = mktime(&tm);
            data = "";
            return;
        }
        if (data.indexOf("\"event\":\"chronicle_seal\"") >= 0) {
            // Worker pushes a sealed Chronicle entry for SD persistence so
            // the chip owns its own diary (and the existing daily SD->R2
            // backup loop catches it for free). The "entry" field is the
            // full entry as a stringified JSON value (escapes preserve the
            // inner quotes) so we can extract and write it without parsing
            // the nested object structure.
            int dStart = data.indexOf("\"date\":\"");
            if (dStart < 0) { data = ""; return; }
            dStart += 8;
            int dEnd = data.indexOf("\"", dStart);
            if (dEnd < 0 || dEnd - dStart != 10) { data = ""; return; }
            String date = data.substring(dStart, dEnd);
            // Strict YYYY-MM-DD digit check before using the date in a path.
            bool dateOk = true;
            for (int i = 0; i < 10 && dateOk; i++) {
                char c = date[i];
                if (i == 4 || i == 7) { if (c != '-') dateOk = false; }
                else if (c < '0' || c > '9') dateOk = false;
            }
            if (!dateOk) { data = ""; return; }

            // Walk past escape sequences to find the unescaped closing quote.
            int eStart = data.indexOf("\"entry\":\"", dEnd);
            if (eStart < 0) { data = ""; return; }
            eStart += 9;
            int eEnd = -1;
            for (int i = eStart; i < (int)data.length(); i++) {
                char c = data[i];
                if (c == '\\' && i + 1 < (int)data.length()) { i++; continue; }
                if (c == '"') { eEnd = i; break; }
            }
            if (eEnd < 0) { data = ""; return; }

            // Unescape standard JSON string escapes. \uXXXX is decoded to
            // UTF-8 so owner notes with unicode survive the round trip.
            String entryJson;
            entryJson.reserve(eEnd - eStart);
            for (int i = eStart; i < eEnd; i++) {
                char c = data[i];
                if (c != '\\' || i + 1 >= eEnd) { entryJson += c; continue; }
                char n = data[++i];
                if (n == '"')      entryJson += '"';
                else if (n == '\\') entryJson += '\\';
                else if (n == '/')  entryJson += '/';
                else if (n == 'n')  entryJson += '\n';
                else if (n == 'r')  entryJson += '\r';
                else if (n == 't')  entryJson += '\t';
                else if (n == 'b')  entryJson += '\b';
                else if (n == 'f')  entryJson += '\f';
                else if (n == 'u' && i + 4 < eEnd) {
                    unsigned int cp = 0;
                    bool hexOk = true;
                    for (int k = 0; k < 4 && hexOk; k++) {
                        char h = data[i + 1 + k];
                        cp <<= 4;
                        if      (h >= '0' && h <= '9') cp |= h - '0';
                        else if (h >= 'a' && h <= 'f') cp |= h - 'a' + 10;
                        else if (h >= 'A' && h <= 'F') cp |= h - 'A' + 10;
                        else hexOk = false;
                    }
                    if (hexOk) {
                        i += 4;
                        // Surrogate pair: 0xD800-0xDBFF must be followed by
                        // a low surrogate \uDC00-\uDFFF to form a non-BMP
                        // codepoint (emoji etc.). Without this combine,
                        // each half encodes as invalid UTF-8.
                        if (cp >= 0xD800 && cp <= 0xDBFF
                            && i + 6 < eEnd
                            && data[i + 1] == '\\' && data[i + 2] == 'u') {
                            unsigned int low = 0;
                            bool lowOk = true;
                            for (int k = 0; k < 4 && lowOk; k++) {
                                char h = data[i + 3 + k];
                                low <<= 4;
                                if      (h >= '0' && h <= '9') low |= h - '0';
                                else if (h >= 'a' && h <= 'f') low |= h - 'a' + 10;
                                else if (h >= 'A' && h <= 'F') low |= h - 'A' + 10;
                                else lowOk = false;
                            }
                            if (lowOk && low >= 0xDC00 && low <= 0xDFFF) {
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                                i += 6; // skip "\uXXXX" of the low surrogate
                            }
                        }
                        if (cp < 0x80) entryJson += (char)cp;
                        else if (cp < 0x800) {
                            entryJson += (char)(0xC0 | (cp >> 6));
                            entryJson += (char)(0x80 | (cp & 0x3F));
                        } else if (cp < 0x10000) {
                            entryJson += (char)(0xE0 | (cp >> 12));
                            entryJson += (char)(0x80 | ((cp >> 6) & 0x3F));
                            entryJson += (char)(0x80 | (cp & 0x3F));
                        } else {
                            entryJson += (char)(0xF0 | (cp >> 18));
                            entryJson += (char)(0x80 | ((cp >> 12) & 0x3F));
                            entryJson += (char)(0x80 | ((cp >> 6) & 0x3F));
                            entryJson += (char)(0x80 | (cp & 0x3F));
                        }
                    } else {
                        entryJson += '\\';
                        entryJson += n;
                    }
                }
                else { entryJson += '\\'; entryJson += n; }
            }

            // Atomic write using the project-standard tmp + bak + rename
            // pattern (matches flushPeriod and other critical writes). Any
            // mid-write power loss either keeps the previous version (.bak
            // is the previous good copy until rename succeeds) or leaves
            // the worker's next sync request as the recovery path. Never
            // a window where the entry is permanently gone.
            if (!SD.exists("/chronicle") && !SD.mkdir("/chronicle")) {
                logError("chronicle", "mkdir /chronicle failed");
                data = "";
                return;
            }
            // Year-subdirectory grouping keeps each chronicle dir bounded at
            // ~366 entries so FAT32 traversal stays fast for decades. Year is
            // the first 4 chars of the validated YYYY-MM-DD date.
            String yearDir = "/chronicle/" + date.substring(0, 4);
            if (!SD.exists(yearDir.c_str()) && !SD.mkdir(yearDir.c_str())) {
                logError("chronicle", ("mkdir " + yearDir + " failed").c_str());
                data = "";
                return;
            }
            String path = yearDir + "/" + date + ".json";
            String tmpPath = path + ".tmp";
            String bakPath = path + ".bak";
            File f = SD.open(tmpPath.c_str(), FILE_WRITE);
            if (!f) {
                logError("chronicle", "open .tmp for chronicle write failed");
                data = "";
                return;
            }
            f.print(entryJson);
            f.close();
            // Stash existing .json as .bak so the previous version stays
            // recoverable until the new rename succeeds.
            if (SD.exists(bakPath.c_str())) SD.remove(bakPath.c_str());
            if (SD.exists(path.c_str())) SD.rename(path.c_str(), bakPath.c_str());
            if (!SD.rename(tmpPath.c_str(), path.c_str())) {
                logError("chronicle", "rename .tmp to .json failed");
                // Recovery: restore the previous version we just stashed.
                if (SD.exists(bakPath.c_str())) SD.rename(bakPath.c_str(), path.c_str());
                data = "";
                return;
            }
            // New version is in place, drop the stash.
            if (SD.exists(bakPath.c_str())) SD.remove(bakPath.c_str());
            data = "";
            return;
        }
        if (data.indexOf("\"event\":\"backup_committed\"") >= 0) {
            // Best-effort field extraction; a malformed event leaves fields zero/empty.
            int d1 = data.indexOf("\"date\":\"");
            if (d1 >= 0) {
                d1 += 8;
                int d2 = data.indexOf("\"", d1);
                if (d2 > d1 && d2 - d1 == 10) {
                    String dstr = data.substring(d1, d2);
                    strncpy(r2CommittedDate, dstr.c_str(), sizeof(r2CommittedDate) - 1);
                    r2CommittedDate[sizeof(r2CommittedDate) - 1] = '\0';
                }
            }
            int b1 = data.indexOf("\"bytes\":");
            if (b1 >= 0) {
                b1 += 8;
                int b2 = data.indexOf(",", b1);
                if (b2 < 0) b2 = data.indexOf("}", b1);
                if (b2 > b1) r2CommittedBytes = data.substring(b1, b2).toInt();
            }
            int f1 = data.indexOf("\"files\":");
            if (f1 >= 0) {
                f1 += 8;
                int f2 = data.indexOf(",", f1);
                if (f2 < 0) f2 = data.indexOf("}", f1);
                if (f2 > f1) r2CommittedFiles = data.substring(f1, f2).toInt();
            }
            struct tm tm;
            if (getLocalTime(&tm, 0)) r2CommittedAtUnix = mktime(&tm);
            // Persist to SD so admin UI state survives a reboot between commit ack and next backup.
            saveLastCommit();
        }
        data = "";
        return;
    }

    char method[8] = "GET";
    char path[256] = "/";
    char cfIP[48] = "";
    char cfCountry[4] = "";
    char acceptEncoding[64] = "";
    int reqId = 0;

    int idStart = data.indexOf("\"id\":") + 5;
    if (idStart > 4) reqId = data.substring(idStart, data.indexOf(",", idStart)).toInt();

    int mStart = data.indexOf("\"method\":\"") + 10;
    if (mStart > 9) {
        int mEnd = data.indexOf("\"", mStart);
        data.substring(mStart, mEnd).toCharArray(method, sizeof(method));
    }

    int pStart = data.indexOf("\"path\":\"") + 8;
    if (pStart > 7) {
        int pEnd = data.indexOf("\"", pStart);
        data.substring(pStart, pEnd).toCharArray(path, sizeof(path));
    }

    int ciStart = data.indexOf("\"CF-Connecting-IP\":\"");
    if (ciStart >= 0) {
        ciStart += 20;
        data.substring(ciStart, data.indexOf("\"", ciStart)).toCharArray(cfIP, sizeof(cfIP));
    }

    int ccStart = data.indexOf("\"CF-IPCountry\":\"");
    if (ccStart >= 0) {
        ccStart += 16;
        data.substring(ccStart, data.indexOf("\"", ccStart)).toCharArray(cfCountry, sizeof(cfCountry));
    }

    int aeStart = data.indexOf("\"Accept-Encoding\":\"");
    if (aeStart >= 0) {
        aeStart += 19;
        data.substring(aeStart, data.indexOf("\"", aeStart)).toCharArray(acceptEncoding, sizeof(acceptEncoding));
    }

    String body = "";
    int bStart = data.indexOf("\"body\":\"");
    if (bStart >= 0) {
        bStart += 8;
        // body is last field, ends with "}; find last } then step back past "
        int lastBrace = data.lastIndexOf('}');
        if (lastBrace > bStart && data[lastBrace - 1] == '"') {
            body = data.substring(bStart, lastBrace - 1);
        }
    }
    data = "";

    // Reject any field that contains CR or LF to prevent header injection into
    // the loopback HTTP request we're about to build from these strings.
    for (size_t i = 0; method[i]; i++) if (method[i] == '\r' || method[i] == '\n') { method[0] = '\0'; break; }
    for (size_t i = 0; path[i]; i++) if (path[i] == '\r' || path[i] == '\n') { path[0] = '\0'; break; }
    for (size_t i = 0; cfIP[i]; i++) if (cfIP[i] == '\r' || cfIP[i] == '\n') { cfIP[0] = '\0'; break; }
    for (size_t i = 0; cfCountry[i]; i++) if (cfCountry[i] == '\r' || cfCountry[i] == '\n') { cfCountry[0] = '\0'; break; }
    for (size_t i = 0; acceptEncoding[i]; i++) if (acceptEncoding[i] == '\r' || acceptEncoding[i] == '\n') { acceptEncoding[0] = '\0'; break; }
    if (method[0] == '\0' || path[0] == '\0') {
        String err = "{\"id\":";
        err += reqId;
        err += ",\"status\":400,\"ct\":\"text/plain\",\"len\":11}";
        wsSendText(wsClient, err);
        wsSendText(wsClient, String("YmFkIHJlcXVlc3Q=")); // "bad request"
        wsSendText(wsClient, String(""));
        return;
    }

    // loopback
    WiFiClient localClient;
    if (!localClient.connect("127.0.0.1", 80)) {
        // send metadata + empty body + end marker
        String err = "{\"id\":";
        err += reqId;
        err += ",\"status\":502,\"ct\":\"text/plain\",\"len\":15}";
        wsSendText(wsClient, err);
        // base64 of "loopback failed" = bG9vcGJhY2sgZmFpbGVk
        wsSendText(wsClient, String("bG9vcGJhY2sgZmFpbGVk"));
        wsSendText(wsClient, String(""));
        return;
    }

    localClient.printf("%s %s HTTP/1.1\r\n", method, path);
    localClient.print("Host: 127.0.0.1\r\n");
    if (cfIP[0]) localClient.printf("CF-Connecting-IP: %s\r\n", cfIP);
    if (cfCountry[0]) localClient.printf("CF-IPCountry: %s\r\n", cfCountry);
    if (acceptEncoding[0]) localClient.printf("Accept-Encoding: %s\r\n", acceptEncoding);
    if (strcmp(method, "POST") == 0 && body.length() > 0) {
        localClient.print("Content-Type: application/x-www-form-urlencoded\r\n");
        localClient.printf("Content-Length: %u\r\n", (unsigned)body.length());
    }
    localClient.print("Connection: close\r\n\r\n");
    if (strcmp(method, "POST") == 0 && body.length() > 0) {
        localClient.print(body);
    }
    body = "";

    unsigned long start = millis();
    while (!localClient.available() && millis() - start < 10000) {
        delay(1);
    }

    if (!localClient.available()) {
        localClient.stop();
        String err = "{\"id\":";
        err += reqId;
        err += ",\"status\":504,\"ct\":\"text/plain\",\"len\":7}";
        wsSendText(wsClient, err);
        wsSendText(wsClient, String("dGltZW91dA==")); // "timeout"
        wsSendText(wsClient, String(""));
        return;
    }

    // parse status. Default to 502 so a malformed or empty status line surfaces
    // as an upstream error rather than a silent 200.
    String statusLine = localClient.readStringUntil('\n');
    int statusCode = 502;
    int spaceIdx = statusLine.indexOf(' ');
    if (spaceIdx > 0 && statusLine.length() >= (unsigned)(spaceIdx + 4)) {
        int parsed = statusLine.substring(spaceIdx + 1, spaceIdx + 4).toInt();
        if (parsed >= 100 && parsed <= 599) statusCode = parsed;
    }

    // headers
    String contentType = "text/html";
    String cacheControl = "";
    String contentEncoding = "";
    int contentLength = -1;
    while (localClient.available()) {
        String header = localClient.readStringUntil('\n');
        header.trim();
        if (header.length() == 0) break;
        int colon = header.indexOf(':');
        if (colon < 0) continue;
        String hName = header.substring(0, colon);
        String hVal = header.substring(colon + 1);
        hVal.trim();
        if (hName.equalsIgnoreCase("Content-Type"))     contentType = hVal;
        if (hName.equalsIgnoreCase("Content-Length"))   contentLength = hVal.toInt();
        if (hName.equalsIgnoreCase("Cache-Control"))    cacheControl = hVal;
        if (hName.equalsIgnoreCase("Content-Encoding")) contentEncoding = hVal;
    }

    // send metadata text frame. ce field added so the worker can forward
    // Content-Encoding to the browser; required for gzipped HTML serving.
    String meta = "{\"id\":";
    meta += reqId;
    meta += ",\"status\":";
    meta += statusCode;
    meta += ",\"ct\":\"";
    meta += contentType;
    meta += "\"";
    if (cacheControl.length() > 0) {
        meta += ",\"cc\":\"";
        meta += cacheControl;
        meta += "\"";
    }
    if (contentEncoding.length() > 0) {
        meta += ",\"ce\":\"";
        meta += contentEncoding;
        meta += "\"";
    }
    meta += ",\"len\":";
    meta += contentLength;
    meta += "}";

    wsSendText(wsClient, meta);
    meta = "";

    // stream body as base64-encoded text frames (4KB raw = ~5.3KB b64 per chunk)
    const char* b64c = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    uint8_t chunk[3072];
    while (localClient.available() && millis() - start < 15000) {
        int rd = localClient.read(chunk, sizeof(chunk));
        if (rd > 0) {
            String b64;
            b64.reserve((rd * 4) / 3 + 4);
            for (int i = 0; i < rd; i += 3) {
                uint32_t n = ((uint32_t)chunk[i]) << 16;
                if (i + 1 < rd) n |= ((uint32_t)chunk[i + 1]) << 8;
                if (i + 2 < rd) n |= chunk[i + 2];
                b64 += b64c[(n >> 18) & 0x3F];
                b64 += b64c[(n >> 12) & 0x3F];
                b64 += (i + 1 < rd) ? b64c[(n >> 6) & 0x3F] : '=';
                b64 += (i + 2 < rd) ? b64c[n & 0x3F] : '=';
            }
            wsSendText(wsClient, b64);
        }
    }
    // end marker: empty text frame
    wsSendText(wsClient, String(""));
    localClient.stop();
}

static void hmacSha256Hex(const char* key, const char* msg, char* outHex65) {
    uint8_t hmac[32];
    mbedtls_md_context_t ctx;
    mbedtls_md_init(&ctx);
    mbedtls_md_setup(&ctx, mbedtls_md_info_from_type(MBEDTLS_MD_SHA256), 1);
    mbedtls_md_hmac_starts(&ctx, (const uint8_t*)key, strlen(key));
    mbedtls_md_hmac_update(&ctx, (const uint8_t*)msg, strlen(msg));
    mbedtls_md_hmac_finish(&ctx, hmac);
    mbedtls_md_free(&ctx);
    for (int i = 0; i < 32; i++) {
        snprintf(outHex65 + i * 2, 3, "%02x", hmac[i]);
    }
    outHex65[64] = '\0';
}

// True when connectWorker() failed at DNS specifically (vs TCP/TLS).
// Reconnect loop short-circuits to WiFi.reconnect() in that case since DNS
// failures at the WiFi layer almost always need a fresh association.
volatile bool wsLastFailWasDns = false;

// One-shot boot migration: moves legacy <parentDir>/<base>.json files into
// <parentDir>/<YYYY>/<base>.json so each year subdir stays bounded (FAT32
// traversal slows past ~500 entries in a single directory). Used by both
// /chronicle/ and /stats/weekly/ + /stats/monthly/ (anything whose label
// starts with a 4-digit year).
//
// Idempotent: a file already at the new path causes the legacy duplicate
// at root to be removed; a file only at the legacy path is renamed in
// place. Cap at 64 collected names per call. Current archives are tiny
// (<60 entries) so this covers any realistic restore-from-old-SD scenario.
// Year subdirs themselves are skipped (isDir branch) so re-running is a
// no-op once migration has completed.
static void migrateLegacyToYearSubdir(const char* parentDir, const char* logTag) {
    if (!SD.exists(parentDir)) return;
    File dir = SD.open(parentDir);
    if (!dir) return;
    if (!dir.isDirectory()) { dir.close(); return; }
    const int MAX_MIGRATE = 64;
    String legacy[MAX_MIGRATE];
    int n = 0;
    File entry = dir.openNextFile();
    while (entry) {
        String name = entry.name();
        int slash = name.lastIndexOf('/');
        String base = (slash >= 0) ? name.substring(slash + 1) : name;
        bool isDir = entry.isDirectory();
        entry.close();
        if (!isDir && base.endsWith(".json") && base.length() > 4) {
            bool yearOk = true;
            for (int i = 0; i < 4 && yearOk; i++) {
                if (base[i] < '0' || base[i] > '9') yearOk = false;
            }
            if (yearOk && n < MAX_MIGRATE) legacy[n++] = base;
        }
        entry = dir.openNextFile();
    }
    dir.close();
    for (int i = 0; i < n; i++) {
        const String& base = legacy[i];
        String yearDir = String(parentDir) + "/" + base.substring(0, 4);
        if (!SD.exists(yearDir.c_str()) && !SD.mkdir(yearDir.c_str())) {
            logError(logTag, ("migrate mkdir " + yearDir + " failed").c_str());
            continue;
        }
        String oldPath = String(parentDir) + "/" + base;
        String newPath = yearDir + "/" + base;
        if (SD.exists(newPath.c_str())) {
            SD.remove(oldPath.c_str());
            Serial.printf("[%s] migrate: dup removed %s\n", logTag, oldPath.c_str());
        } else if (SD.rename(oldPath.c_str(), newPath.c_str())) {
            Serial.printf("[%s] migrate: %s -> %s\n", logTag, oldPath.c_str(), newPath.c_str());
        } else {
            logError(logTag, ("migrate rename failed for " + base).c_str());
        }
    }
}

// Boot-time orphan cleanup for /chronicle/. Mirrors the recovery logic
// the other tmp+bak+rename writers do on first access. Called once after
// SD init in setup(). For each leftover from an interrupted write:
//   - .tmp files: incomplete writes from a power loss mid-write. Discard;
//     the worker's catch-up sync will re-push the entry next reconnect.
//   - .bak files: stash of the previous version. If matching .json exists,
//     the rename succeeded and .bak is just stale cleanup. If no .json,
//     a mid-rename crash left only .bak; restore it as .json so the entry
//     survives even without worker connectivity.
//
// Walks both year subdirs (/chronicle/YYYY/) and the root (legacy entries
// pre-migration) so a crash during migration leaves no orphans behind.
static void chronicleStartupRecovery() {
    if (!SD.exists("/chronicle")) return;
    // Cap at 64 each: more than enough for realistic crash patterns (typical
    // is 0 or 1) without unbounded heap growth on a corrupted directory.
    const int MAX_RECOVERY = 64;
    String tmpPaths[MAX_RECOVERY];
    String bakPaths[MAX_RECOVERY];
    int tmpCount = 0, bakCount = 0;

    auto collectFromDir = [&](const String& dirPath) {
        File dir = SD.open(dirPath.c_str());
        if (!dir) return;
        if (!dir.isDirectory()) { dir.close(); return; }
        File entry = dir.openNextFile();
        while (entry) {
            String name = entry.name();
            int slash = name.lastIndexOf('/');
            String base = (slash >= 0) ? name.substring(slash + 1) : name;
            bool isDir = entry.isDirectory();
            entry.close();
            if (!isDir) {
                String full = dirPath + "/" + base;
                if (base.endsWith(".tmp") && tmpCount < MAX_RECOVERY) tmpPaths[tmpCount++] = full;
                else if (base.endsWith(".bak") && bakCount < MAX_RECOVERY) bakPaths[bakCount++] = full;
            }
            entry = dir.openNextFile();
        }
        dir.close();
    };

    // Collect from year subdirs first, then root (legacy stragglers).
    File root = SD.open("/chronicle");
    if (root) {
        if (root.isDirectory()) {
            File entry = root.openNextFile();
            while (entry) {
                String name = entry.name();
                int slash = name.lastIndexOf('/');
                String base = (slash >= 0) ? name.substring(slash + 1) : name;
                bool isDir = entry.isDirectory();
                entry.close();
                if (isDir) {
                    bool yearOk = (base.length() == 4);
                    for (int i = 0; i < 4 && yearOk; i++) {
                        if (base[i] < '0' || base[i] > '9') yearOk = false;
                    }
                    if (yearOk) collectFromDir("/chronicle/" + base);
                }
                entry = root.openNextFile();
            }
        }
        root.close();
    }
    collectFromDir(String("/chronicle"));

    for (int i = 0; i < tmpCount; i++) {
        SD.remove(tmpPaths[i].c_str());
        Serial.printf("[chronicle] recovery: discarded orphan %s\n", tmpPaths[i].c_str());
    }
    for (int i = 0; i < bakCount; i++) {
        const String& bakPath = bakPaths[i];
        // Strip ".bak" to get the canonical .json path.
        String jsonPath = bakPath.substring(0, bakPath.length() - 4);
        if (SD.exists(jsonPath.c_str())) {
            SD.remove(bakPath.c_str());
            Serial.printf("[chronicle] recovery: cleaned stale %s\n", bakPath.c_str());
        } else {
            if (SD.rename(bakPath.c_str(), jsonPath.c_str())) {
                Serial.printf("[chronicle] recovery: restored %s from .bak\n", jsonPath.c_str());
            } else {
                logError("chronicle", "recovery: bak->json rename failed");
            }
        }
    }
}

// Walk /chronicle/ and return the max date the chip has on SD as a
// "YYYY-MM-DD" string (lexically sortable). Empty string when chip has
// nothing. The catch-up sync sends this so the worker can push every
// entry newer than max_date.
//
// Trade-off vs the previous "send all dates" protocol: gives up
// mid-history gap detection (an entry N+1 sealed before entry N would
// not be back-filled). In practice gaps don't happen (sealing is
// strictly chronological), and the protocol now sends 80 bytes per
// reconnect instead of growing 13 bytes per chronicle day, which would
// have hit ~50 KB and OOM'd the chip past year 5.
//
// Layout: entries live at /chronicle/YYYY/YYYY-MM-DD.json. Any
// pre-migration stragglers at /chronicle/YYYY-MM-DD.json are also
// considered so a half-migrated state still reports the true max.
//
// O(1)-stack design: tracks the running greatest year subdir name during
// pass 1 (no array, no cap, so chip can run for centuries without the
// function losing precision). Pass 2 walks that year for the max date.
// On the rare empty-year-subdir fallback (year-rollover crash mid-write),
// re-scan root for the next-greatest year strictly less than the one we
// just visited, and try again. Each re-scan is O(years) so the worst
// case (every year subdir empty) is O(years²); steady state is one scan
// + one walk = O(years + N/years).
static String chronicleMaxDate() {
    String maxDate = "";
    if (!SD.exists("/chronicle")) return maxDate;

    auto considerDateFile = [&](const String& base) {
        if (base.length() != 15 || !base.endsWith(".json")) return;
        String d = base.substring(0, 10);
        for (int i = 0; i < 10; i++) {
            char c = d[i];
            if (i == 4 || i == 7) { if (c != '-') return; }
            else if (c < '0' || c > '9') return;
        }
        if (d > maxDate) maxDate = d;
    };

    // Returns lex-greatest 4-digit year subdir name in /chronicle/. When
    // `exclusiveUpper` is non-empty, the result is strictly less than it
    // (used for the empty-year fallback). Returns "" if no match.
    auto findMaxYear = [&](const String& exclusiveUpper) -> String {
        String found = "";
        File r = SD.open("/chronicle");
        if (!r) return found;
        if (!r.isDirectory()) { r.close(); return found; }
        File e = r.openNextFile();
        while (e) {
            String name = e.name();
            int slash = name.lastIndexOf('/');
            String base = (slash >= 0) ? name.substring(slash + 1) : name;
            bool isDir = e.isDirectory();
            e.close();
            if (isDir) {
                bool yearOk = (base.length() == 4);
                for (int i = 0; i < 4 && yearOk; i++) {
                    if (base[i] < '0' || base[i] > '9') yearOk = false;
                }
                if (yearOk
                    && (exclusiveUpper.length() == 0 || base < exclusiveUpper)
                    && base > found) {
                    found = base;
                }
            }
            e = r.openNextFile();
        }
        r.close();
        return found;
    };

    // Pass 1: combined root scan. Find greatest year subdir AND fold
    // any legacy root-level files. Saves a separate scan in the common
    // case (which is the steady-state post-migration layout).
    File root = SD.open("/chronicle");
    if (!root) return maxDate;
    if (!root.isDirectory()) { root.close(); return maxDate; }
    String currentYear = "";
    File entry = root.openNextFile();
    while (entry) {
        String name = entry.name();
        int slash = name.lastIndexOf('/');
        String base = (slash >= 0) ? name.substring(slash + 1) : name;
        bool isDir = entry.isDirectory();
        entry.close();
        if (isDir) {
            bool yearOk = (base.length() == 4);
            for (int i = 0; i < 4 && yearOk; i++) {
                if (base[i] < '0' || base[i] > '9') yearOk = false;
            }
            if (yearOk && base > currentYear) currentYear = base;
        } else {
            considerDateFile(base);
        }
        entry = root.openNextFile();
    }
    root.close();

    // Pass 2: walk the latest year subdir for max date. If empty (year
    // rollover crash, manual dir creation), re-scan root for the next
    // year less than this one and try again until something yields.
    while (currentYear.length() > 0) {
        String yearPath = "/chronicle/" + currentYear;
        File yearDir = SD.open(yearPath.c_str());
        if (yearDir) {
            File f = yearDir.openNextFile();
            while (f) {
                String fname = f.name();
                int s = fname.lastIndexOf('/');
                String fbase = (s >= 0) ? fname.substring(s + 1) : fname;
                f.close();
                considerDateFile(fbase);
                f = yearDir.openNextFile();
            }
            yearDir.close();
        }
        if (maxDate.length() > 0) break;
        currentYear = findMaxYear(currentYear);
    }
    return maxDate;
}

bool connectWorker() {
    if (strlen(cfgWorkerUrl) == 0 || strlen(cfgWorkerKey) == 0) {
        Serial.println("[ws] skipped: worker_url or worker_key blank");
        return false;
    }

    wsClient.setInsecure();
    // Bound the TLS handshake and per-read timeouts. Defaults are 120s
    // and 30s respectively, which means a slow CF edge response (DO
    // eviction, edge network blip, congestion) can block the main loop
    // for the full default while the chip waits on a stalled handshake.
    // 5s/5s is plenty for a healthy connect; bounds the worst case so
    // the loop-stall watchdog at ~30s never gets a chance to trigger.
    wsClient.setHandshakeTimeout(5);   // seconds
    wsClient.setTimeout(5000);          // milliseconds (read timeout)

    Serial.printf("[ws] connecting to %s:443\n", cfgWorkerUrl);

    // Pre-flight DNS so the reconnect loop can tell a resolver problem
    // (LAN cascade, fixable by WiFi.reconnect) from a CF/TCP issue.
    IPAddress workerIP;
    if (!WiFi.hostByName(cfgWorkerUrl, workerIP)) {
        Serial.println("[ws] DNS resolve failed; flagging for fast WiFi recovery");
        wsLastFailWasDns = true;
        return false;
    }
    wsLastFailWasDns = false;

    if (!wsClient.connect(cfgWorkerUrl, 443)) {
        Serial.println("[ws] TCP/TLS connect failed");
        return false;
    }

    // send WebSocket upgrade
    wsClient.printf("GET /_ws?key=%s HTTP/1.1\r\n", cfgWorkerKey);
    wsClient.printf("Host: %s\r\n", cfgWorkerUrl);
    wsClient.print("Upgrade: websocket\r\n");
    wsClient.print("Connection: Upgrade\r\n");
    wsClient.print("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n");
    wsClient.print("Sec-WebSocket-Version: 13\r\n");
    wsClient.printf("Origin: https://%s\r\n", cfgWorkerUrl);
    wsClient.print("\r\n");

    // wait for response
    unsigned long start = millis();
    while (!wsClient.available() && millis() - start < 5000) {
        delay(10);
    }

    if (!wsClient.available()) {
        Serial.println("[ws] no HTTP response (upgrade timeout 5s)");
        wsClient.stop();
        return false;
    }

    String response = wsClient.readStringUntil('\n');
    if (!response.startsWith("HTTP/1.0 101") && !response.startsWith("HTTP/1.1 101")) {
        Serial.println("[ws] upgrade rejected: " + response);
        logError("ws", ("upgrade rejected: " + response).c_str());
        wsClient.stop();
        return false;
    }

    // skip remaining headers
    while (wsClient.available()) {
        String line = wsClient.readStringUntil('\n');
        line.trim();
        if (line.length() == 0) break;
    }

    // Optional HMAC handshake. If the worker sends an auth_challenge within
    // 2s, respond with HMAC-SHA256(device_key, nonce). Always wait for the
    // challenge so a config mismatch (worker has HMAC_SECRET, device_key
    // empty) gets caught instead of triggering a silent reconnect storm.
    {
        unsigned long hmacWait = millis();
        while (!wsClient.available() && millis() - hmacWait < 2000) delay(10);
    }
    if (wsClient.available()) {
        String msg = wsRead(wsClient);
        int typeIdx = msg.indexOf("\"auth_challenge\"");
        int nonceIdx = msg.indexOf("\"nonce\":\"");
        if (typeIdx >= 0 && nonceIdx >= 0 && strlen(cfgDeviceKey) > 0) {
            nonceIdx += 9;
            int nonceEnd = msg.indexOf("\"", nonceIdx);
            if (nonceEnd > nonceIdx) {
                String nonce = msg.substring(nonceIdx, nonceEnd);
                char hmacHex[65];
                hmacSha256Hex(cfgDeviceKey, nonce.c_str(), hmacHex);
                String resp = "{\"type\":\"auth_response\",\"hmac\":\"";
                resp += hmacHex;
                resp += "\"}";
                wsSendText(wsClient, resp);
                Serial.println("[ws] HMAC handshake sent");
            }
        } else if (typeIdx >= 0) {
            Serial.println("[ws] worker requires HMAC but device_key is empty");
            wsClient.stop();
            return false;
        }
    }

    Serial.println("[ws] connected");
    wsConnected = true;
    lastWsActivity = millis();

    // Catch-up sync: send the worker the max chronicle date we have on
    // SD (or empty when we have nothing). Worker pushes every entry
    // strictly newer than max_date via existing chronicle_seal events.
    // Idempotent on both sides; fires every reconnect so a long offline
    // window doesn't leave silent gaps. Payload is constant-size (~80
    // bytes) regardless of archive depth; previous "send all dates"
    // form would have OOM'd the chip past year 5 (~50 KB+ message).
    String maxDate = chronicleMaxDate();
    String syncMsg = "{\"type\":\"event\",\"event\":\"chronicle_sync_request\",\"data\":{\"max_date\":\"";
    syncMsg += maxDate;
    syncMsg += "\"}}";
    wsSendText(wsClient, syncMsg);

    // Re-emit retired-sensor events for any sensors already retired on SD.
    // Covers the 1-in-a-million case where a sensor crossed the retire
    // threshold while the WS was down: the original transition emit was
    // lost, but the chip's SD record is correct. Worker dedups by
    // sensor_retired/<name> key so re-emits are idempotent.
    if (bmeHealth.retired) emitChronicleSensorRetired("BME280", bmeHealth.retired_unix);
    if (ccsHealth.retired) emitChronicleSensorRetired("CCS811", ccsHealth.retired_unix);

    return true;
}

// Setup
void setup() {
    Serial.begin(115200);

    pinMode(LED_PIN, OUTPUT);
    pinMode(NOTIF_LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, HIGH);
    digitalWrite(NOTIF_LED_PIN, HIGH);
    delay(500);
    digitalWrite(LED_PIN, LOW);
    digitalWrite(NOTIF_LED_PIN, LOW);

    if (!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
        Serial.println("OLED init failed (check I2C wiring and 0x3C address)");
        // Can't bootLog here yet (lambda defined further down, and it uses
        // display.println which would no-op anyway on a failed display).
        // logError persists to SD so admin can see post-reboot.
        logError("hw", "oled init failed at 0x3C; check wiring + bus pull-ups");
    } else {
        oledOk = true;
    }
    display.clearDisplay();
    display.setTextSize(1);
    display.setTextColor(WHITE);

    display.drawBitmap(48, 2, logoBitmap, 32, 32, WHITE);
    display.setTextSize(2);
    display.setCursor(16, 40);
    display.print("HelloESP");
    display.setTextSize(1);
    display.display();
    delay(1500);

    char bootLines[7][22] = {"", "", "", "", "", "", ""};
    auto bootLog = [&](const char* msg) {
        for (int i = 0; i < 6; i++) {
            strncpy(bootLines[i], bootLines[i + 1], 21);
            bootLines[i][21] = '\0';
        }
        strncpy(bootLines[6], msg, 21);
        bootLines[6][21] = '\0';
        display.clearDisplay();
        display.setCursor(0, 0);
        display.println("Loading HelloESP...");
        for (int i = 0; i < 7; i++) display.println(bootLines[i]);
        display.display();
    };

    // Version
    char verBuf[22];
    snprintf(verBuf, sizeof(verBuf), "[sys] v%s", FIRMWARE_VERSION);
    bootLog(verBuf);
    Serial.println(verBuf);

    // Year 2038 audit: time_t must be 64-bit so Chronicle / weekly / records
    // dates keep working past 2038-01-19 03:14:07 UTC. A 32-bit time_t silently
    // wraps to negative values past that instant, corrupting any new archive
    // writes. If this trips, add `-D_TIME_BITS=64 -D_FILE_OFFSET_BITS=64` to
    // platformio.ini build_flags and reflash.
    Serial.printf("[sys] time_t = %u bytes (Year 2038 %s)\n",
                  (unsigned)sizeof(time_t),
                  sizeof(time_t) >= 8 ? "safe" : "VULNERABLE");

    // SD card at 10MHz SPI. Briefly dropped to 4MHz when SI issues surfaced
    // (CS line crossing I2C SDA/SCL caused capacitive crosstalk + bus
    // glitches), but rerouting CS to arch above the I2C plane via solid-
    // core wire restored 10MHz operation cleanly. 1mm air gap kills the
    // coupling that was the actual problem. Bump back to 4MHz if errors
    // ever recur after wire shuffles.
    //
    // Mount retry: if the previous run crashed mid-SD-write (abort from
    // bad_alloc during request parsing, etc.), the card's internal MCU
    // can be left stuck in a half-finished transaction. VCC stays powered
    // across ESP.restart(): the ESP reboots, but the card doesn't, so
    // f_mount returns NOT_READY until the card is physically power-cycled.
    // We can't reset the card's power (that'd need a MOSFET on SD VCC),
    // but we can sometimes wake it by quiescing SPI, waiting long enough
    // for the card's watchdog to time out its pending operation, and
    // retrying. Second attempt drops to 4MHz because if the card is
    // already marginal, higher clocks just fail again. Third attempt
    // (last chance) is the bare-minimum 1MHz startup speed.
    bootLog("[fs] mounting sd");
    bool sdMounted = SD.begin(SD_CS, SPI, 10000000U);
    if (sdMounted) sdSpeedHz = 10000000U;
    if (!sdMounted) {
        Serial.println("[fs] sd mount failed, retrying with SPI reset @ 4MHz");
        bootLog("[fs] sd retry 4MHz");
        SD.end();
        SPI.end();
        delay(500);
        SPI.begin();
        sdMounted = SD.begin(SD_CS, SPI, 4000000U);
        if (sdMounted) sdSpeedHz = 4000000U;
    }
    if (!sdMounted) {
        Serial.println("[fs] sd mount failed again, last-chance retry @ 1MHz");
        bootLog("[fs] sd retry 1MHz");
        SD.end();
        SPI.end();
        delay(1000);
        SPI.begin();
        sdMounted = SD.begin(SD_CS, SPI, 1000000U);
        if (sdMounted) sdSpeedHz = 1000000U;
    }
    if (!sdMounted) {
        Serial.println("SD card failed or not present; auto-restarting in 60s");
        bootLog("[fs] sd FAIL retry");
        // Don't infinite-loop on a framed-behind-glass device: a transient
        // SPI hiccup or brown-out is recoverable on a fresh boot. The 60s
        // delay gives the OLED time to display the failure for an observer
        // (and prevents a tight reboot loop if the SD is actually dead).
        delay(60000);
        ESP.restart();
    }
    Serial.println("SD card initialized");
    bootLog("[fs] sd ok");

    // Ensure /fw/ directory exists for firmware staging (SD-flash feature).
    // Admin uploads .bin files here via the file manager; the SD-flash
    // endpoint reads from here. Created once at boot so the file manager
    // has a landing spot.
    if (!SD.exists("/fw")) {
        SD.mkdir("/fw");
    }

    // config
    bootLog("[fs] loading config");
    if (SD.exists("/config.txt")) {
        File cfg = SD.open("/config.txt", FILE_READ);
        if (cfg) {
            size_t sz = cfg.size();
            char* buf = (char*)malloc(sz + 1);
            String contents;
            if (buf) {
                size_t n = cfg.read((uint8_t*)buf, sz);
                buf[n] = '\0';
                contents = String(buf);
                free(buf);
            }
            cfg.close();
            int start = 0;
            while (start < (int)contents.length()) {
                int end = contents.indexOf('\n', start);
                if (end < 0) end = contents.length();
                String line = contents.substring(start, end);
                start = end + 1;
                line.trim();
                if (line.length() == 0 || line.startsWith("#")) continue;
                int eq = line.indexOf('=');
                if (eq < 0) continue;
                String key = line.substring(0, eq);
                String val = line.substring(eq + 1);
                key.trim();
                val.trim();
                // Strip control chars and DEL inside the value. trim() only
                // touches leading/trailing whitespace, so a worker_key with
                // an embedded CR/LF would survive and inject extra request
                // lines into the WS upgrade at line 3049 (printf("GET ...
                // %s ...", cfgWorkerKey)). Owner controls config, but make
                // header injection structurally impossible.
                {
                    String clean;
                    clean.reserve(val.length());
                    for (size_t i = 0; i < val.length(); i++) {
                        unsigned char c = (unsigned char)val.charAt(i);
                        if (c >= 0x20 && c != 0x7F) clean += (char)c;
                    }
                    val = clean;
                }
                if (key == "wifi_ssid")   val.toCharArray(cfgSsid, sizeof(cfgSsid));
                if (key == "wifi_pass")   val.toCharArray(cfgWifiPass, sizeof(cfgWifiPass));
                if (key == "admin_user")  val.toCharArray(cfgAdminUser, sizeof(cfgAdminUser));
                if (key == "admin_pass")  val.toCharArray(cfgAdminPass, sizeof(cfgAdminPass));
                if (key == "worker_url")  val.toCharArray(cfgWorkerUrl, sizeof(cfgWorkerUrl));
                if (key == "worker_key")  val.toCharArray(cfgWorkerKey, sizeof(cfgWorkerKey));
                if (key == "device_key")  val.toCharArray(cfgDeviceKey, sizeof(cfgDeviceKey));
                if (key == "timezone")    val.toCharArray(cfgTimezone, sizeof(cfgTimezone));
                if (key == "worker_exclusive") cfgWorkerExclusive = (val == "true");
                if (key == "shelly_url")  val.toCharArray(cfgShellyUrl, sizeof(cfgShellyUrl));
                if (key == "cost_per_kwh") cfgCostPerKwh = val.toFloat();
                if (key == "co2_per_kwh")  cfgCo2PerKwh  = val.toFloat();
            }
            bootLog("[fs] config loaded");
            bootLog(cfgShellyUrl[0] != '\0' ? "[net] shelly: on" : "[net] shelly: off");
        }
    } else {
        Serial.println("No config.txt found");
        bootLog("[fs] config MISSING");
        while (1) { delay(1000); }
    }

    if (!SD.exists("/visitors.txt")) {
        File f = SD.open("/visitors.txt", FILE_WRITE);
        if (f) { f.println(0); f.close(); }
    }
    cachedVisitorCount = readVisitorCount();
    loadCountries();
    // Migrate legacy flat /chronicle/YYYY-MM-DD.json paths into year subdirs
    // before recovery + sync run, so the rest of the boot operates on the
    // canonical layout. Idempotent + a no-op once migration has completed.
    migrateLegacyToYearSubdir("/chronicle", "chronicle");
    chronicleStartupRecovery();
    // SD.usedBytes() scans the entire FAT and can take seconds on large cards;
    // the main loop refreshes it every 5 min, so skip the boot-time hit.
    cachedSdUsedMB = 0;
    // Schema guard: refuse to start if the on-disk guestbook is from a
    // pre-v3 firmware. v1.4 stripped the in-firmware migrations to claw
    // back flash; users with stale SD data must flash v1.3 first to run
    // the migrations, then upgrade. Fresh installs stamp v3 here.
    // Missing-marker-on-v3-csv (accidentally deleted marker, FS corruption)
    // is recovered by probing the csv shape: 6 commas = v3, stamp marker;
    // anything else halts with the same flash-v1.3-first message.
    bootLog("[mig] schema");
    int gbSchema = readGuestbookSchemaVersion();
    if (gbSchema > 0 && gbSchema < 3) {
        migrationAbort("guestbook schema v<3. Flash v1.3 first to migrate, then upgrade.");
    }
    if (gbSchema == 0 && SD.exists("/guestbook.csv")) {
        int commas = -1;
        bool probeOpened = false;
        File probe = SD.open("/guestbook.csv", FILE_READ);
        if (probe) {
            probeOpened = true;
            char line[400];
            while (probe.available()) {
                int n = probe.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) n--;
                if (n == 0) continue;
                commas = 0;
                for (int i = 0; i < n; i++) if (line[i] == ',') commas++;
                break;
            }
            probe.close();
        }
        if (probeOpened && (commas == 6 || commas < 0)) {
            // v3 shape (6 commas) or empty/whitespace file: safe to stamp.
            writeGuestbookSchemaVersion(3);
            Serial.println("[migrate] schema marker missing; csv shape compatible, stamped v3");
        } else {
            migrationAbort("guestbook.csv with no schema marker. Flash v1.3 first.");
        }
    }
    if (gbSchema == 0 && !SD.exists("/guestbook.csv")) writeGuestbookSchemaVersion(3);
    countPendingGuestbook();
    lastNotifiedPending = pendingGuestbook; // don't email for pre-existing pending entries on boot

    // if the deepest dir exists, parents do too
    if (!SD.exists("/stats/yearly")) {
        SD.mkdir("/logs");
        SD.mkdir("/stats");
        SD.mkdir("/stats/weekly");
        SD.mkdir("/stats/monthly");
        SD.mkdir("/stats/yearly");
    }
    // Migrate legacy flat archives into year subdirs. Same year-grouping
    // rationale as chronicle. Idempotent. Yearly is intentionally not
    // migrated; 1 file/year stays trivially small even at year 1000.
    migrateLegacyToYearSubdir("/stats/weekly",  "stats");
    migrateLegacyToYearSubdir("/stats/monthly", "stats");
    loadCheckpoint();
    loadRecords();
    loadSensorHealth();

    bootLog("[hw] init sensors");

    // Bound I2C reads to 100ms BEFORE any sensor begin(). Without this, a
    // BME-fail+CCS-success boot leaves the Wire timeout at the library
    // default (often unbounded) so a stuck CCS read could hang the loop
    // forever. Wire.begin() runs implicitly inside the first sensor's
    // begin(), so set the timeout up-front to cover all subsequent I2C ops.
    Wire.setTimeOut(100);

    // Continue boot even if a sensor fails to init. The site is the higher-value
    // workload; safeBme*/cached_* fallbacks keep serving last-known values, and
    // the admin UI shows the degraded state.
    if (!bme.begin(0x76)) {
        Serial.println("BME280 init failed, check wiring.");
        bootLog("[hw] bme280 FAIL");
        logError("hw", "bme280 init failed; continuing without temp/humidity/pressure");
    } else {
        safeBmeTemp(); safeBmeHumidity(); safeBmePressureHpa(); safeBmeAltitudeFt();
        bootLog("[hw] bme280 ok");
    }

    if (!ccs.begin()) {
        Serial.println("CCS811 init failed, check wiring.");
        bootLog("[hw] ccs811 FAIL");
        logError("hw", "ccs811 init failed; continuing without eCO2/VOC");
    } else {
        bootLog("[hw] ccs811 ok");
    }
    // CCS811 warmup happens in the background; runtime reads check ccs.available() each cycle

    // DS3231 RTC. Pre-seeds the system clock from battery-backed time so
    // log/sensor timestamps are correct from the first millisecond, even if
    // NTP is slow or unreachable. NTP, when it lands, calls rtc.adjust() to
    // re-sync the RTC against authoritative time. Apply TZ early so any
    // pre-NTP timestamps render in local time.
    setenv("TZ", cfgTimezone, 1);
    tzset();
    if (!rtc.begin()) {
        Serial.println("DS3231 init failed, continuing without RTC.");
        bootLog("[hw] ds3231 FAIL");
        logError("hw", "ds3231 init failed; relying on NTP only for time");
    } else {
        rtcOk = true;
        if (rtc.lostPower()) {
            // Battery dead or first power-up. RTC has no valid time; skip
            // the pre-seed and let NTP fill it in. We'll write back to the
            // RTC after NTP succeeds, which also clears the lostPower flag.
            rtcLostPowerAtBoot = true;
            bootLog("[hw] ds3231 no time");
            logError("hw", "ds3231 lostPower flag set; CR1220 backup battery may need replacement");
        } else {
            DateTime n = rtc.now();
            // Sanity: lostPower clears once NTP writes back, but a bus glitch
            // mid-read or bit-rot in the time registers could still produce a
            // wildly wrong year while OSF stays clear. Refuse to seed the
            // system clock with garbage; NTP will fill in shortly.
            if (n.year() >= 2024 && n.year() <= 2100) {
                struct timeval tv;
                tv.tv_sec = n.unixtime();
                tv.tv_usec = 0;
                settimeofday(&tv, NULL);
                bootLog("[hw] ds3231 preseeded");
            } else {
                bootLog("[hw] ds3231 bad year");
            }
        }
    }

    bootLog("[net] wifi connecting");
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.setHostname("HelloESP");
    // WiFi.begin with an empty SSID spins until the 5-minute timeout, reboots,
    // and repeats forever. Fail loudly instead so config issues are obvious.
    if (cfgSsid[0] == '\0') {
        Serial.println("[net] WiFi SSID is blank in config, halting.");
        bootLog("[net] wifi NO SSID");
        logError("net", "WiFi SSID blank in /config.txt; halting");
        while (1) { delay(1000); }
    }
    WiFi.begin(cfgSsid, cfgWifiPass);
    unsigned long wifiStart = millis();
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.println("Waiting for WiFi...");
        if (millis() - wifiStart > 300000UL) {
            bootLog("[net] wifi TIMEOUT");
            delay(500);
            cleanRestart();
        }
    }
    Serial.println("WiFi connected: " + WiFi.localIP().toString());
    bootLog("[net] wifi ok");
    if (MDNS.begin("helloesp")) {
        MDNS.addService("http", "tcp", 80);
        bootLog("[net] mdns ok");
    }

    bootLog("[net] ntp sync");
    configTime(gmtOffset_sec, daylightOffset_sec, "pool.ntp.org", "time.google.com", "time.cloudflare.com");
    setenv("TZ", cfgTimezone, 1);
    tzset();
    struct tm timeinfo;
    int retry = 0;
    // retry ~6 seconds (30 × 200ms), enough for UDP to settle after DHCP/NAT sync
    while (!getLocalTime(&timeinfo, 200) && retry < 30) {
        delay(200);
        retry++;
    }
    if (retry == 30) {
        // SNTP daemon keeps retrying in background; boot continues. The
        // main loop has a watchdog (see `if (bootTime == 0 ...)` block)
        // that re-captures bootTime once NTP eventually arrives.
        Serial.println("NTP not yet synced at boot; background sync continues");
        bootLog("[net] ntp pending");
    } else {
        Serial.println("NTP synced: " + getTimestamp());
        bootTime = mktime(&timeinfo);
        // Stamp period start times if zero. Otherwise /history.json shows
        // "Started: 1970-01-01" until the next period boundary.
        uint32_t nowUnix = (uint32_t)bootTime;
        if (currentWeek.started_unix  == 0) currentWeek.started_unix  = nowUnix;
        if (currentMonth.started_unix == 0) currentMonth.started_unix = nowUnix;
        if (currentYear.started_unix  == 0) currentYear.started_unix  = nowUnix;
        bootLog("[net] ntp ok");
        // Write authoritative time back to DS3231. Also clears the lostPower
        // flag if this was a fresh-battery boot.
        if (rtcOk) {
            rtc.adjust(DateTime(nowUnix));
            bootLog("[hw] ds3231 ntp sync");
        }
    }
    // Loaded outside the NTP-success branch so a late-NTP boot doesn't
    // zero out yesterday's daily count before the first visit lands.
    loadDailyVisitors();
    // Independent of NTP: loading the persisted backup date is a pure SD read. If it were
    // inside the NTP-success branch, a late NTP sync would leave lastBackupDate empty and
    // trigger a duplicate same-day backup.
    loadLastBackupDate();
    // Restore the last Worker-confirmed commit state so the admin UI doesn't show "never
    // confirmed" after a reboot between backup completion and receiving the ack event.
    loadLastCommit();
    // Restore maintenance state so the admin panel still shows "active" if a
    // reboot happened mid-window. Worker DO is authoritative for whether the
    // public site shows the maintenance page; this just keeps the admin UI
    // honest after RAM was lost.
    loadMaintenanceState();

    // Routes

    server.on("/", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        // AsyncWebServer routes URLs with adjacent slashes (e.g. "//anything") to this handler.
        // Reject those requests with a real 404 so they don't inflate visitor counts.
        String reqUrl = request->url();
        if (reqUrl != "/" && reqUrl.indexOf("//") >= 0) {
            AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/404.html", "text/html");
            r->setCode(404);
            request->send(r);
            return;
        }

        // Count only public visits. Detects three relay patterns:
        //   - Cloudflare Worker / Cloudflared tunnel: sets CF-Connecting-IP header
        //   - Direct port forward from router: remote IP is not a LAN private range
        //   - Pure LAN (owner testing/admin): remote is LAN AND no CF header -> skip
        bool isPublic = request->hasHeader("CF-Connecting-IP")
                     || !isLocalIP(request->client()->remoteIP());

        requestsThisInterval++;

        if (isPublic) {
            int count = readVisitorCount();
            writeVisitorCount(count + 1);
            cachedVisitorCount = count + 1;

            struct tm timeinfo;
            bool haveTime = getLocalTime(&timeinfo);
            if (haveTime) {
                char nowDate[12];
                strftime(nowDate, sizeof(nowDate), "%Y-%m-%d", &timeinfo);
                // Full YYYY-MM-DD compare (not just tm_mday) so May 8 ->
                // Jun 8 cross-month-same-day rollover correctly resets.
                // Loop-based midnight reset usually fires first; this is
                // belt-and-suspenders for cases where loop didn't catch
                // it (NTP failure across the boundary, etc.).
                if (strcmp(nowDate, dailyVisitorsDate) != 0) {
                    dailyVisitors = 0;
                    strncpy(dailyVisitorsDate, nowDate, sizeof(dailyVisitorsDate) - 1);
                    dailyVisitorsDate[sizeof(dailyVisitorsDate) - 1] = '\0';
                    lastVisitorDay = timeinfo.tm_mday;
                }
            }
            dailyVisitors++;
            saveDailyVisitors();
            currentWeek.visitors++;
            currentMonth.visitors++;
            currentYear.visitors++;

            char cc[3];
            if (request->hasHeader("CF-IPCountry")) {
                normalizeCountry(request->header("CF-IPCountry").c_str(), cc);
            } else {
                strcpy(cc, "??");
            }
            tallyCountry(cc);
        }

        sendGzipOrRaw(request, "/index.html", "text/html");
        logConsole(request, 200);
    });

    // /stats is intentionally NOT registered via server.on() because
    // ESPAsyncWebServer's path matching is prefix-based: server.on("/stats", ...)
    // also matches /stats/weekly/<label>, shadowing the archive-label handlers
    // in onNotFound below. Handling it in onNotFound (where exact-match string
    // comparison is the routing logic) avoids that collision.

    server.on("/countries", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        String json;
        json.reserve(512);
        json = "{";
        for (int i = 0; i < countryCount; i++) {
            if (i > 0) json += ",";
            json += "\"" + String(countries[i].code) + "\":" + String(countries[i].count);
        }
        json += "}";
        AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
        r->addHeader("Cache-Control", "public, max-age=60");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/console", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/console.html", "text/html");
        r->addHeader("Cache-Control", "public, max-age=300");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/about", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/about.html", "text/html");
        r->addHeader("Cache-Control", "public, max-age=300");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/snake", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/snake.html", "text/html");
        r->addHeader("Cache-Control", "public, max-age=300");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/chronicle", HTTP_GET, [](AsyncWebServerRequest *request) {
        // Chronicle data lives in the Worker's DO storage (accumulated from
        // stats_update events, sealed at UTC midnight). The chip just serves
        // the SPA shell here; the page fetches /chronicle.json client-side.
        // Permalinks like /chronicle/YYYY-MM-DD are rewritten worker-side to
        // hit this same handler, so all chronicle URLs serve chronicle.html.
        if (redirectLanToWorker(request)) return;
        AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/chronicle.html", "text/html");
        r->addHeader("Cache-Control", "public, max-age=300");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/console.json", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        String json;
        json.reserve(5120);
        json = "[";
        int idx = consoleHead;
        for (int i = 0; i < consoleCount; i++) {
            idx = (idx - 1 + CONSOLE_SIZE) % CONSOLE_SIZE;
            ConsoleEntry &e = consoleRing[idx];
            if (i > 0) json += ",";
            json += "{\"time\":" + String(e.unix_time);
            json += ",\"country\":\""; json += e.country; json += "\"";
            json += ",\"method\":\""; json += e.method; json += "\"";
            json += ",\"path\":\"" + jsonEscape(e.path) + "\"";
            json += ",\"status\":" + String(e.status);
            json += "}";
        }
        json += "]";
        AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
        r->addHeader("Cache-Control", "no-store");
        request->send(r);
    });

    server.on("/records.json", HTTP_GET, [](AsyncWebServerRequest *request) {
        if (redirectLanToWorker(request)) return;
        String json;
        json.reserve(512);
        json = "{";
        auto rec = [&](const char* key, const RecordVal& r, bool last, bool intVal) {
            json += "\""; json += key; json += "\":";
            if (!r.set) json += "null";
            else {
                json += "{\"value\":";
                if (intVal) json += String((int)r.value);
                else        json += String(r.value, 2);
                json += ",\"at\":\""; json += r.at; json += "\"}";
            }
            if (!last) json += ",";
        };
        rec("highest_co2",       rec_highest_co2,       false, true);
        rec("highest_temp_f",    rec_highest_temp_f,    false, false);
        rec("lowest_temp_f",     rec_lowest_temp_f,     false, false);
        rec("most_visitors_day", rec_most_visitors_day, false, true);
        rec("longest_uptime_d",  rec_longest_uptime_d,  true,  false);
        // Power-related fields read from RAM globals (no SD read needed).
        // Same gates and shape as the SD records.json saveRecords()
        // emission; this is the path frontends actually consume since
        // the SD file only exists for boot-time loadRecords() recovery.
        if (cfgShellyUrl[0] != '\0' && lifetime_energy_wh > 0.0f) {
            json += ",\"lifetime_energy_wh\":";
            json += String((float)lifetime_energy_wh, 1);
            if (shelly_tracking_started_unix > 0) {
                json += ",\"tracking_started_unix\":";
                json += String(shelly_tracking_started_unix);
            }
        }
        if (cfgCostPerKwh > 0.0f) {
            json += ",\"cost_per_kwh\":";
            json += String(cfgCostPerKwh, 4);
        }
        if (cfgCo2PerKwh > 0.0f) {
            json += ",\"co2_per_kwh\":";
            json += String(cfgCo2PerKwh, 4);
        }
        json += "}";
        AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
        r->addHeader("Cache-Control", "public, max-age=300");
        request->send(r);
    });


    // static files with cache headers
    struct { const char* path; const char* type; } staticFiles[] = {
        {"/favicon.png", "image/png"},
        {"/favicon.svg", "image/svg+xml"},
        {"/og-banner.jpg", "image/jpeg"},
        {"/esp32-webserver.jpg", "image/jpeg"},
        {"/esp32-webserver-bme280.jpg", "image/jpeg"},
        {"/esp8266-webserver.jpg", "image/jpeg"},
        {"/helloesp-framed.jpg", "image/jpeg"},
        {"/helloesp-boot.mp4", "video/mp4"},
        {"/helloesp-boot-poster.jpg", "image/jpeg"}
    };
    for (auto& f : staticFiles) {
        server.on(f.path, HTTP_GET, [f](AsyncWebServerRequest *request) {
            if (redirectLanToWorker(request)) return;
            AsyncWebServerResponse *response = request->beginResponse(SD, f.path, f.type);
            response->addHeader("Cache-Control", "public, max-age=86400");
            request->send(response);
        });
    }

    // /_stream is served by the Worker, not the device. On LAN (direct to
    // ESP) this path has no handler, so it used to fall through to
    // onNotFound → 404.html. EventSource auto-retries 404 responses every
    // ~3 seconds forever, which under sustained LAN viewing pile-drove the
    // TCP stack. HTTP 204 is the spec-defined "stop permanently" signal.
    server.on("/_stream", HTTP_GET, [](AsyncWebServerRequest *request) {
        request->send(204);
    });

    server.on("/ping", HTTP_GET, [](AsyncWebServerRequest *request) {
        AsyncWebServerResponse *r = request->beginResponse(200, "text/plain", "pong");
        r->addHeader("Cache-Control", "no-store");
        request->send(r);
    });

    server.on("/coffee", HTTP_GET, [](AsyncWebServerRequest *request) {
        request->send(418, "text/plain", "I'm a teapot. Well, technically an ESP32.");
    });

    server.on("/.well-known/security.txt", HTTP_GET, [](AsyncWebServerRequest *request) {
        AsyncWebServerResponse *r = request->beginResponse(200, "text/plain",
            "Contact: mailto:hello@tech1k.com\n"
            "Preferred-Languages: en\n"
            "Canonical: https://helloesp.com/.well-known/security.txt\n"
            "Expires: 2027-04-16T00:00:00.000Z\n");
        r->addHeader("Cache-Control", "public, max-age=86400");
        request->send(r);
    });

    server.on("/humans.txt", HTTP_GET, [](AsyncWebServerRequest *request) {
        request->send(200, "text/plain; charset=utf-8",
            "/* TEAM */\n"
            "    Made by: Kristian Kramer (Tech1k)\n"
            "    Site: https://tech1k.com\n"
            "    GitHub: https://github.com/Tech1k/helloesp\n"
            "\n"
            "/* DEVICE */\n"
            "    MCU: Espressif ESP32 DOIT DevKit V1, 520 KB RAM\n"
            "    Sensors: Bosch BME280, AMS CCS811\n"
            "    Display: SSD1306 128x64 OLED\n"
            "    Storage: 32 GB FAT32 SD card\n"
            "    Relay: Cloudflare Worker (WebSocket + Durable Object)\n"
            "\n"
            "/* SITE */\n"
            "    Language: English\n"
            "    License: MIT\n"
            "    Built with: PlatformIO, Arduino framework, ESPAsyncWebServer, mbedtls, Chart.js\n");
    });

    server.on("/robots.txt", HTTP_GET, [](AsyncWebServerRequest *request) {
        request->send(200, "text/plain",
            "User-agent: *\n"
            "Disallow: /stats\n"
            "Disallow: /logs\n"
            "Disallow: /admin\n"
            "Disallow: /_upload\n"
            "Disallow: /_ota\n"
            "Disallow: /_stream\n"
            "Disallow: /guestbook/submit\n"
            "Disallow: /guestbook/entries\n"
            "Disallow: /guestbook/pending\n"
            "Disallow: /guestbook/moderate\n"
            "Disallow: /guestbook/replies\n"
            "Disallow: /guestbook/locate\n"
            "Disallow: /guestbook/translate\n"
            "Disallow: /countries\n"
            "Disallow: /console.json\n"
            "Disallow: /history.json\n"
            "Disallow: /records.json\n"
            "Disallow: /snake/seed\n"
            "Disallow: /snake/score\n"
            "\n"
            "Sitemap: https://helloesp.com/sitemap.xml\n");
    });

    server.on("/sitemap.xml", HTTP_GET, [](AsyncWebServerRequest *request) {
        AsyncWebServerResponse *r = request->beginResponse(200, "application/xml",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
            "  <url><loc>https://helloesp.com/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>\n"
            "  <url><loc>https://helloesp.com/guestbook</loc><changefreq>daily</changefreq><priority>0.8</priority></url>\n"
            "  <url><loc>https://helloesp.com/history</loc><changefreq>weekly</changefreq><priority>0.6</priority></url>\n"
            "  <url><loc>https://helloesp.com/console</loc><changefreq>always</changefreq><priority>0.5</priority></url>\n"
            "  <url><loc>https://helloesp.com/about</loc><changefreq>monthly</changefreq><priority>0.5</priority></url>\n"
            "  <url><loc>https://helloesp.com/snake</loc><changefreq>weekly</changefreq><priority>0.5</priority></url>\n"
            "  <url><loc>https://helloesp.com/chronicle</loc><changefreq>daily</changefreq><priority>0.7</priority></url>\n"
            "  <url><loc>https://helloesp.com/chronicle.rss</loc><changefreq>daily</changefreq><priority>0.4</priority></url>\n"
            "  <url><loc>https://helloesp.com/changelog.rss</loc><changefreq>monthly</changefreq><priority>0.4</priority></url>\n"
            "  <url><loc>https://helloesp.com/guestbook.rss</loc><changefreq>daily</changefreq><priority>0.4</priority></url>\n"
            "</urlset>\n");
        r->addHeader("Cache-Control", "public, max-age=86400");
        request->send(r);
    });

    // keep this list in sync with the changelog section in index.html
    server.on("/changelog.rss", HTTP_GET, [](AsyncWebServerRequest *request) {
        String rss;
        rss.reserve(5120);
        rss = F("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                "<rss version=\"2.0\"><channel>"
                "<title>HelloESP | Changelog</title>"
                "<link>https://helloesp.com/</link>"
                "<description>Updates to the HelloESP project.</description>"
                "<language>en</language>");
        auto item = [&](const char* date, const char* pubDate, const char* text) {
            rss += "<item><title>";
            rss += date;
            rss += "</title><link>https://helloesp.com/</link><guid isPermaLink=\"false\">helloesp-";
            rss += pubDate;
            rss += "</guid><pubDate>";
            rss += pubDate;
            rss += "</pubDate><description>";
            rss += text;
            rss += "</description></item>";
        };
        item("May 8, 2026", "Fri, 08 May 2026 12:00:00 GMT",
             "/chronicle got a few quality-of-life updates. Today's in-progress entry shows above the archive, refreshing as the chip writes more of the day. Filter chips slice the archive to just milestones, records, anomalies, busy days, or quiet days. Each entry has a share button, and arrow keys step between days on entry pages. Guestbook translation got smarter too: better source-language detection so casual English no longer trips the translate button, and previously-untranslatable diacritic-less Czech, Polish, and Italian now translate correctly.");
        item("May 6, 2026", "Wed, 06 May 2026 12:00:00 GMT",
             "Chronicle entries now align to the chip's local timezone instead of UTC, so each day's entry covers the chip's actual day from where it's running. The chip also notices when one of its sensors stops working, retires it, and writes a chronicle entry about the loss. Behind the scenes, archive folders moved to a year-grouped layout so the chip can run for decades without slowing down. Firmware 1.4 also drops the boot-time CSV migrations to free flash for future features; pre-1.3 installs need to flash 1.3 first.");
        item("May 4, 2026", "Mon, 04 May 2026 12:00:00 GMT",
             "Added /chronicle: a daily entry the chip writes about itself. Auto-generated from sensor readings, visitors, and weather, archived from today onward. Each midnight the day's snapshot freezes into a permanent entry. Five starter templates pick the right shape for the day (busy, quiet, anomaly, milestone, generic), and there's a permalink for every entry. Each new entry also crossposts to X.");
        item("May 3, 2026", "Sun, 03 May 2026 12:00:00 GMT",
             "Snake got its own page. Same game, with two leaderboards now: today's top scores and the all-time top ten. Strong scores cross to all-time on their own. Each top entry has a watch button that plays back the actual game. Past quarters move to a Hall of Fame archive so the leaderboard stays fresh.");
        item("May 1, 2026", "Fri, 01 May 2026 12:00:00 GMT",
             "The site can now track its own electricity use with an optional smart plug. The homepage shows live wattage and lifetime energy, plus cost and CO2 if you set your grid rate. Outdoor weather card grew with air quality, UV index, atmospheric CO2, dewpoint, and pressure trend, and the icons now match the time of day.");
        item("Apr 29, 2026", "Wed, 29 Apr 2026 12:00:00 GMT",
             "Added a DS3231 real-time clock module on the breadboard. Boot logs and timestamps are correct right away instead of being wrong for a few seconds until NTP syncs. History page got a year-grouped collapsible layout that stays readable as archives pile up over time. New /about page with the short version of the project story for casual visitors.");
        item("Apr 27, 2026", "Mon, 27 Apr 2026 12:00:00 GMT",
             "Snake now has a global leaderboard. Top 10 with 3-letter initials, shared across the 404, offline, and timeout pages. Anti-cheat is server-side: the worker hands out a seed at game start, you submit your move log on game over, and the worker replays it to confirm the score. Only way onto the board is actually playing.");
        item("Apr 26, 2026", "Sun, 26 Apr 2026 12:00:00 GMT",
             "Snake added to the 404, offline, and timeout fallback pages. WebSocket reconnect path is simpler now and recovers faster from transient drops.");
        item("Apr 25, 2026", "Sat, 25 Apr 2026 12:00:00 GMT",
             "Live uptime's &quot;Best&quot; substat shows from the start instead of waiting a full day.");
        item("Apr 24, 2026", "Fri, 24 Apr 2026 12:00:00 GMT",
             "Homepage sections lazy-load now, so quick visitors don't fetch things they never scroll to. Inline one-tap translate on non-English guestbook entries via Cloudflare Workers AI. Reply forms got the same character counter as the main form. Console flag tooltips show the full country name on hover.");
        item("Apr 23, 2026", "Thu, 23 Apr 2026 12:00:00 GMT",
             "Guestbook got two-level reply threading with inline previews and per-message share/copy links. Pagination expanded with a jump-to-page input once past 10 pages. Moderation deletes now tombstone, so the reply chain stays intact with the original message blanked. HTML pages now serve pre-gzipped, cutting transfer size about 4&times; on every page load.");
        item("Apr 21, 2026", "Tue, 21 Apr 2026 12:00:00 GMT",
             "Fixed a URL quirk where &quot;//anything&quot; was leaking through as a page view. Visitors had discovered it and were &quot;chatting&quot; by putting messages in URLs that showed up on the console. Those now return a proper 404. Reboots switched to a deep-sleep wakeup so the device comes back cleanly after OTA updates, instead of needing a physical power cycle.");
        item("Apr 19, 2026", "Sun, 19 Apr 2026 12:00:00 GMT",
             "Public relaunch on a fresh ESP32. Full rebuild: air quality sensors, historical charts, guestbook with moderation, admin panel, OLED dashboard, daily off-site backups, and a Cloudflare Worker relay so it can live on WiFi without a tunnel.");
        item("Nov 2023", "Wed, 15 Nov 2023 12:00:00 GMT",
             "The original ESP32 burned out after a little over 500 days online. Site went into a read-only archive of stats, news, and photos until the relaunch.");
        item("Jan 30, 2023", "Mon, 30 Jan 2023 12:00:00 GMT",
             "Published the first &quot;final&quot; version of HelloESP.");
        item("Jul 6, 2022", "Wed, 06 Jul 2022 12:00:00 GMT",
             "Added a photo of the ESP32 with the BME280 attached.");
        item("Jul 5, 2022", "Tue, 05 Jul 2022 12:00:00 GMT",
             "Images now served from the ESP32 itself. Migrated to ESPAsyncWebServer.");
        item("Jul 4, 2022", "Mon, 04 Jul 2022 12:00:00 GMT",
             "Visitor counter now runs fully off the ESP32 via SPIFFS.");
        item("Jul 2, 2022", "Sat, 02 Jul 2022 12:00:00 GMT",
             "Added BME280 sensor readings: temperature, humidity, and altitude.");
        item("Jun 27, 2022", "Mon, 27 Jun 2022 12:00:00 GMT",
             "HelloESP launched. A little project showing what these tiny boards can do.");
        rss += "</channel></rss>";
        AsyncWebServerResponse *r = request->beginResponse(200, "application/rss+xml", rss);
        r->addHeader("Cache-Control", "public, max-age=86400");
        request->send(r);
        logConsole(request, 200);
    });

    // upload + OTA (multipart, must be server.on)
    server.on("/_upload", HTTP_POST,
        [](AsyncWebServerRequest *request) {
            if (!adminAuth(request)) return;
            String path = request->hasParam("path", true) ? request->getParam("path", true)->value() : "/";
            if (!safePath(path)) { request->send(400, "text/plain", "Invalid path"); return; }
            if (uploadRejectedExisting) {
                uploadRejectedExisting = false;
                request->send(409, "text/plain", "File exists; resubmit with overwrite=1 to replace");
                return;
            }
            request->send(200, "text/plain", "OK");
        },
        [](AsyncWebServerRequest *request, const String& filename, size_t index, uint8_t *data, size_t len, bool final) {
            // Auth must be checked in the upload handler itself. The completion handler
            // runs AFTER chunks land, so skipping auth here would let a LAN attacker without
            // the admin password overwrite any SD file (including config.txt).
            if (!isLocalIP(request->client()->remoteIP()) || request->hasHeader("CF-Connecting-IP")) return;
            if (isAuthLockedOut((uint32_t)request->client()->remoteIP())) return;
            if (!request->authenticate(cfgAdminUser, cfgAdminPass)) return;

            static File uploadFile;
            String path = request->hasParam("path", true) ? request->getParam("path", true)->value() : "/";
            if (!safePath(path)) return;
            if (path != "/" && !path.endsWith("/")) path += "/";

            if (index == 0) {
                uploadRejectedExisting = false;
                // Close any handle left over from a prior aborted upload before reopening.
                if (uploadFile) uploadFile.close();
                String fullPath = path + filename;
                bool wantOverwrite = request->hasParam("overwrite", true) &&
                                     request->getParam("overwrite", true)->value() == "1";
                if (SD.exists(fullPath) && !wantOverwrite) {
                    uploadRejectedExisting = true;
                    Serial.println("Upload rejected (exists, no overwrite): " + fullPath);
                    return;
                }
                uploadFile = SD.open(fullPath, FILE_WRITE);
                if (!uploadFile) {
                    Serial.println("Upload failed to open: " + fullPath);
                    return;
                }
                Serial.println("Upload start: " + fullPath);
            }
            lastUploadChunkMs = millis();
            if (uploadFile && len) {
                uploadFile.write(data, len);
            }
            if (final && uploadFile) {
                uploadFile.close();
                Serial.println("Upload complete: " + String(index + len) + " bytes");
            }
        }
    );

    // OTA update
    server.on("/_ota", HTTP_POST,
        [](AsyncWebServerRequest *request) {
            if (!adminAuth(request)) return;
            // Require BOTH no-error AND finished. `hasError()` can be false
            // on a clean-but-truncated upload (no `final=true` chunk ever
            // arrived); rebooting at that point flashes a partial image.
            // isFinished() flips true only after Update.end(true) succeeded.
            bool success = !Update.hasError() && Update.isFinished();
            request->send(200, "text/plain", success ? "OTA success, rebooting..." : "OTA failed (truncated or invalid image)");
            if (success) {
                delay(500);
                cleanRestart();
            }
        },
        [](AsyncWebServerRequest *request, const String& filename, size_t index, uint8_t *data, size_t len, bool final) {
            // Upload chunks arrive before the completion handler runs, so auth MUST be checked
            // here too. Otherwise flash would be committed (Update.end) before the completion
            // handler gets a chance to reject an unauthenticated caller.
            if (!isLocalIP(request->client()->remoteIP()) || request->hasHeader("CF-Connecting-IP")) return;
            if (isAuthLockedOut((uint32_t)request->client()->remoteIP())) return;
            if (!request->authenticate(cfgAdminUser, cfgAdminPass)) return;

            // Per-request flag tracking whether Update.end has already run
            // for this stream. Multipart edge cases (re-entry, retransmits)
            // could otherwise call Update.write after Update.end, which is
            // undefined and could leave the partition table inconsistent.
            static bool otaCommitted = false;

            if (index == 0) {
                otaCommitted = false;
                Serial.println("OTA update start: " + filename);
                if (!Update.begin(UPDATE_SIZE_UNKNOWN)) {
                    Update.printError(Serial);
                    // Defensive: don't process this chunk. The isRunning()
                    // check below catches this case too, but early return
                    // makes the failure path explicit and safer against
                    // future refactors.
                    return;
                }
            }
            lastUploadChunkMs = millis();
            if (Update.isRunning() && !otaCommitted) {
                if (Update.write(data, len) != len) {
                    Update.printError(Serial);
                    // Abort so the next OTA can start; without this the slot
                    // stays in-progress until reboot and Update.begin() fails.
                    Update.abort();
                    otaCommitted = true;
                }
            }
            if (final && !otaCommitted) {
                otaCommitted = true;
                if (Update.end(true)) {
                    Serial.println("OTA complete: " + String(index + len) + " bytes");
                } else {
                    Update.printError(Serial);
                    if (Update.isRunning()) Update.abort();
                }
            }
        }
    );

    server.onNotFound([](AsyncWebServerRequest *request) {
        String url = request->url();

        // Worker-exclusive redirect: applies to all public-facing read endpoints
        // routed through onNotFound. Skip for admin paths (handled below by
        // returning early before adminAuth checks) and internal endpoints.
        // Routes that should continue serving on LAN even in exclusive mode:
        //   - /admin and /admin/* (auth surface)
        //   - /_ws, /_upload, /_ota, /_stream (internal)
        //   - /ping (health)
        //   - /robots.txt, /sitemap.xml (bot-facing, harmless to serve)
        //   - /guestbook/submit, /guestbook/translate (POST endpoints; redirects
        //     don't preserve POST body, so let them serve direct)
        // Anything else gets the redirect when cfgWorkerExclusive is on.
        if (cfgWorkerExclusive
            && !url.startsWith("/admin")
            && !url.startsWith("/_")
            && url != "/ping"
            && url != "/robots.txt"
            && url != "/sitemap.xml"
            && request->method() == HTTP_GET) {
            if (redirectLanToWorker(request)) return;
        }

        if (url == "/history") {
            // 5-min cache to match /console. Short enough that bursts of repeat
            // visits dedupe at the edge but most distinct user visits still hit
            // the device (LED blinks, console event fires). Dynamic data on
            // the page loads via separately-cached JSON endpoints.
            AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/history.html", "text/html");
            r->addHeader("Cache-Control", "public, max-age=300");
            request->send(r);
            logConsole(request, 200);
            return;
        }

        if (url == "/history.json") {
            String json;
            // Reserve 8KB up front: covers ~10 years of weekly+monthly+yearly
            // labels (each ~13 bytes) before the underlying String triggers a
            // realloc-and-copy. Beyond that the String resizes naturally.
            json.reserve(8192);
            json = "{";
            const char* sections[] = {"weekly", "monthly", "yearly"};
            for (int s = 0; s < 3; s++) {
                if (s > 0) json += ",";
                json += "\""; json += sections[s]; json += "\":[";
                String dirPath = "/stats/" + String(sections[s]);
                bool first = true;
                // Walk both flat files (monthly/yearly always; weekly during
                // any post-migration transition) and 4-digit year subdirs
                // (weekly canonical layout). One open-handle at a time so
                // we stay well under VFS concurrent-handle limits.
                auto emitFile = [&](const String& fname) {
                    if (!fname.endsWith(".json")) return;
                    if (!first) json += ",";
                    json += "\"" + jsonEscape(fname.substring(0, fname.length() - 5)) + "\"";
                    first = false;
                };
                if (SD.exists(dirPath)) {
                    File dir = SD.open(dirPath);
                    if (dir && dir.isDirectory()) {
                        while (true) {
                            File f = dir.openNextFile();
                            if (!f) break;
                            String n = String(f.name());
                            int lastSlash = n.lastIndexOf('/');
                            if (lastSlash >= 0) n = n.substring(lastSlash + 1);
                            bool isDir = f.isDirectory();
                            f.close();
                            if (isDir) {
                                bool yearOk = (n.length() == 4);
                                for (int i = 0; i < 4 && yearOk; i++) {
                                    if (n[i] < '0' || n[i] > '9') yearOk = false;
                                }
                                if (!yearOk) continue;
                                File yearDir = SD.open(dirPath + "/" + n);
                                if (yearDir) {
                                    while (true) {
                                        File yf = yearDir.openNextFile();
                                        if (!yf) break;
                                        String yn = String(yf.name());
                                        int ys = yn.lastIndexOf('/');
                                        if (ys >= 0) yn = yn.substring(ys + 1);
                                        yf.close();
                                        emitFile(yn);
                                    }
                                    yearDir.close();
                                }
                            } else {
                                emitFile(n);
                            }
                        }
                    }
                    if (dir) dir.close();
                }
                json += "]";
            }
            json += ",\"current\":{";
            json += "\"week\":\""  + String(lastWeekLabel)  + "\",";
            json += "\"month\":\"" + String(lastMonthLabel) + "\",";
            json += "\"year\":\""  + String(lastYearLabel)  + "\"}}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "public, max-age=300");
            request->send(r);
            logConsole(request, 200);
            return;
        }

        if (url.startsWith("/stats/weekly/")) {
            String label = url.substring(14);
            if (label.indexOf("..") >= 0 || label.indexOf('/') >= 0) {
                request->send(400, "text/plain", "bad label");
                return;
            }
            // Year-subdir layout: /stats/weekly/YYYY/YYYY-WNN.json. Fall back
            // to legacy flat path for any straggler that escaped migration.
            String path;
            if (label.length() >= 4) {
                path = "/stats/weekly/" + label.substring(0, 4) + "/" + label + ".json";
                if (!SD.exists(path)) path = "/stats/weekly/" + label + ".json";
            } else {
                path = "/stats/weekly/" + label + ".json";
            }
            if (SD.exists(path)) {
                AsyncWebServerResponse *r = request->beginResponse(SD, path, "application/json");
                r->addHeader("Cache-Control", "public, max-age=31536000, immutable");
                request->send(r);
            } else {
                request->send(404, "text/plain", "not found");
            }
            return;
        }

        if (url.startsWith("/stats/monthly/")) {
            String label = url.substring(15);
            if (label.indexOf("..") >= 0 || label.indexOf('/') >= 0) {
                request->send(400, "text/plain", "bad label");
                return;
            }
            // Year-subdir layout (matches weekly). Falls back to legacy flat
            // path for any pre-migration straggler.
            String path;
            if (label.length() >= 4) {
                path = "/stats/monthly/" + label.substring(0, 4) + "/" + label + ".json";
                if (!SD.exists(path)) path = "/stats/monthly/" + label + ".json";
            } else {
                path = "/stats/monthly/" + label + ".json";
            }
            if (SD.exists(path)) {
                AsyncWebServerResponse *r = request->beginResponse(SD, path, "application/json");
                r->addHeader("Cache-Control", "public, max-age=31536000, immutable");
                request->send(r);
            } else {
                request->send(404, "text/plain", "not found");
            }
            return;
        }

        if (url.startsWith("/stats/yearly/")) {
            String label = url.substring(14);
            if (label.indexOf("..") >= 0 || label.indexOf('/') >= 0) {
                request->send(400, "text/plain", "bad label");
                return;
            }
            String path = "/stats/yearly/" + label + ".json";
            if (SD.exists(path)) {
                AsyncWebServerResponse *r = request->beginResponse(SD, path, "application/json");
                r->addHeader("Cache-Control", "public, max-age=86400");
                request->send(r);
            } else {
                request->send(404, "text/plain", "not found");
            }
            return;
        }

        if (url == "/stats/current") {
            // Pass live energy accumulators so current-period cards on the
            // history page show the in-progress kWh used. Sentinel -1 when
            // no Shelly configured so the field is omitted entirely.
            float weekE  = (cfgShellyUrl[0] != '\0') ? (float)week_energy_wh  : -1.0f;
            float monthE = (cfgShellyUrl[0] != '\0') ? (float)month_energy_wh : -1.0f;
            float yearE  = (cfgShellyUrl[0] != '\0') ? (float)year_energy_wh  : -1.0f;
            String body = "{\"week\":"  + periodToJson(lastWeekLabel,  currentWeek,  weekE)
                       + ",\"month\":" + periodToJson(lastMonthLabel, currentMonth, monthE)
                       + ",\"year\":"  + periodToJson(lastYearLabel,  currentYear,  yearE) + "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", body);
            r->addHeader("Cache-Control", "public, max-age=300");
            request->send(r);
            return;
        }

        if (url == "/stats" && request->method() == HTTP_GET) {
            String json = buildStatsJson();
            request->send(200, "application/json", json);
            logConsole(request, 200);
            return;
        }

        if (url == "/logs/today") {
            String filename = getLogFilename();
            if (SD.exists(filename)) {
                // Go through beginResponseGzipOrRaw rather than the
                // (FS, path, type) overload directly. That overload
                // re-opens the file internally and null-derefs under
                // VFS FD pool exhaustion. The helper opens manually,
                // null-checks, and returns 503 if SD is saturated.
                // text/plain (not text/csv) so CF's edge brotli kicks
                // in. text/csv isn't on CF's auto-compress MIME list
                // and the body is plaintext either way; admin XHR
                // reads it as text regardless.
                request->send(beginResponseGzipOrRaw(request, filename.c_str(), "text/plain"));
            } else {
                AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/404.html", "text/html");
                r->setCode(404);
                request->send(r);
            }
            return;
        }

        // /logs
        if (url == "/logs") {
            File root = SD.open("/logs");
            String json;
            json.reserve(2048);
            json = "[";
            bool first = true;
            while (true) {
                File yearDir = root.openNextFile();
                if (!yearDir) break;
                if (yearDir.isDirectory()) {
                    while (true) {
                        File f = yearDir.openNextFile();
                        if (!f) break;
                        String n = String(f.name());
                        // Strip directory prefix; openNextFile() returns
                        // full path on this library version. See history.json
                        // handler for matching pattern.
                        int lastSlash = n.lastIndexOf('/');
                        if (lastSlash >= 0) n = n.substring(lastSlash + 1);
                        if (n.endsWith(".csv")) {
                            if (!first) json += ",";
                            json += "\"" + jsonEscape(n.substring(0, n.length() - 4)) + "\"";
                            first = false;
                        }
                        f.close();
                    }
                } else {
                    String n = String(yearDir.name());
                    int lastSlash = n.lastIndexOf('/');
                    if (lastSlash >= 0) n = n.substring(lastSlash + 1);
                    if (n.endsWith(".csv")) {
                        if (!first) json += ",";
                        json += "\"" + jsonEscape(n.substring(0, n.length() - 4)) + "\"";
                        first = false;
                    }
                }
                yearDir.close();
            }
            root.close();
            json += "]";
            request->send(200, "application/json", json);
            return;
        }

        // /logs/YYYY-MM-DD
        if (url.startsWith("/logs/") && url.length() == 16) {
            String date = url.substring(6);
            if (date.charAt(4) == '-' && date.charAt(7) == '-') {
                String year = date.substring(0, 4);
                String filename = "/logs/" + year + "/" + date + ".csv";
                if (!SD.exists(filename)) filename = "/logs/" + date + ".csv";
                if (SD.exists(filename)) {
                    // Same FD-safe pattern + text/plain content-type as
                    // /logs/today above.
                    request->send(beginResponseGzipOrRaw(request, filename.c_str(), "text/plain"));
                } else {
                    AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/404.html", "text/html");
                    r->setCode(404);
                    request->send(r);
                }
                return;
            }
        }

        // guestbook
        if (url == "/guestbook") {
            // 5-min cache, consistent with /console and /history. The HTML is
            // a static wrapper; entries/replies/translations load via XHR with
            // their own per-endpoint cache headers, so freshness of dynamic
            // content is unaffected. Form submission, permalinks, and search
            // (which uses query params, busting the cache key) all still work.
            AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/guestbook.html", "text/html");
            r->addHeader("Cache-Control", "public, max-age=300");
            request->send(r);
            logConsole(request, 200);
            return;
        }

        // guestbook entries (paginated). Char-buffer streaming so heap stays flat
        // regardless of CSV size. Supports optional q= for case-insensitive
        // substring search over name + message fields (approved entries only).
        if (url == "/guestbook/entries") {
            int page = request->hasParam("page") ? request->getParam("page")->value().toInt() : 1;
            if (page < 1) page = 1;
            const int perPage = 20;

            String qStr;
            if (request->hasParam("q")) qStr = request->getParam("q")->value();
            qStr.trim();
            if (qStr.length() > 64) qStr.remove(64);
            bool isSearch = qStr.length() > 0;
            const char* qCstr = qStr.c_str();
            int qLen = qStr.length();

            if (!SD.exists("/guestbook.csv")) {
                AsyncWebServerResponse *r = request->beginResponse(200, "application/json",
                    "{\"entries\":[],\"hasMore\":false,\"total\":0,\"countries\":0}");
                if (!isSearch) r->addHeader("Cache-Control", "public, max-age=30");
                request->send(r);
                return;
            }

            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) {
                request->send(200, "application/json",
                    "{\"entries\":[],\"hasMore\":false,\"total\":0,\"countries\":0}");
                return;
            }

            // First pass: total approved top-level + unique countries (kept global
            // so the header is stable across searches), plus match count if q is set.
            // Replies (reply_to non-empty) are excluded from the public listing;
            // they're surfaced only via /guestbook/replies?id=<parent>.
            int totalApproved = 0;
            int totalMatching = 0;
            uint16_t seenCountries[256];
            int seenCountryCount = 0;
            char line[400];
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;
                // v3 schema: time,country,name,message,id,reply_to,status
                int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                for (int j = 0; j < n; j++) {
                    if (line[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0) continue;
                if (line[n-1] != '1') continue;  // only approved
                if (cLast - c5 > 1) continue;    // skip replies (reply_to non-empty)
                totalApproved++;

                // Unique-country tally over all approved top-level entries.
                int ccLen = c2 - c1 - 1;
                if (ccLen >= 2) {
                    uint16_t key = ((uint8_t)line[c1 + 1] << 8) | (uint8_t)line[c1 + 2];
                    if (!(line[c1 + 1] == '?' && line[c1 + 2] == '?')) {
                        bool seen = false;
                        for (int i = 0; i < seenCountryCount; i++) {
                            if (seenCountries[i] == key) { seen = true; break; }
                        }
                        if (!seen && seenCountryCount < 256) seenCountries[seenCountryCount++] = key;
                    }
                }

                bool qMatch = !isSearch || (
                    containsCI(line + c2 + 1, c3 - c2 - 1, qCstr, qLen) ||
                    containsCI(line + c3 + 1, c4 - c3 - 1, qCstr, qLen));
                if (qMatch) totalMatching++;
            }

            if (totalMatching == 0) {
                f.close();
                String body = "{\"entries\":[],\"hasMore\":false,\"total\":" + String(totalApproved) +
                              ",\"countries\":" + String(seenCountryCount) + "}";
                AsyncWebServerResponse *r = request->beginResponse(200, "application/json", body);
                if (!isSearch) r->addHeader("Cache-Control", "public, max-age=30");
                request->send(r);
                logConsole(request, 200);
                return;
            }

            int skip = (page - 1) * perPage;
            if (skip >= totalMatching) {
                f.close();
                String body = "{\"entries\":[],\"hasMore\":false,\"total\":" + String(totalApproved) +
                              ",\"countries\":" + String(seenCountryCount) + "}";
                AsyncWebServerResponse *r = request->beginResponse(200, "application/json", body);
                if (!isSearch) r->addHeader("Cache-Control", "public, max-age=30");
                request->send(r);
                logConsole(request, 200);
                return;
            }
            int fromIdx = totalMatching - skip - perPage;
            int toIdx = totalMatching - skip - 1;
            if (fromIdx < 0) fromIdx = 0;
            bool hasMore = fromIdx > 0;

            // Second pass: stash the 20 rows for this page. Read oldest-first,
            // render in reverse later to get newest-on-top display.
            static char pageEntries[20][400];
            int pageLens[20] = {0};
            int pageCount = 0;
            int matchingIdx = 0;

            f.seek(0);
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;
                int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                for (int j = 0; j < n; j++) {
                    if (line[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0) continue;
                if (line[n-1] != '1') continue;
                if (cLast - c5 > 1) continue;  // skip replies
                if (isSearch) {
                    bool qMatch = containsCI(line + c2 + 1, c3 - c2 - 1, qCstr, qLen) ||
                                  containsCI(line + c3 + 1, c4 - c3 - 1, qCstr, qLen);
                    if (!qMatch) continue;
                }
                if (matchingIdx >= fromIdx && matchingIdx <= toIdx && pageCount < 20) {
                    memcpy(pageEntries[pageCount], line, n + 1);
                    pageLens[pageCount] = n;
                    pageCount++;
                }
                matchingIdx++;
                if (matchingIdx > toIdx) break;
            }

            // Third pass: count descendants per page entry (reply_count) and
            // stash file offsets of up to 2 preview-candidate rows per entry.
            // Level-2 rows always follow their level-1 parent in the CSV
            // (append-only), so a single forward pass resolves the tree.
            // Offsets instead of full-row stashing: 160 bytes of stack vs the
            // 16KB static buffer that was fragmenting heap. Emit loop seeks
            // directly to each offset rather than rescanning the whole file.
            int replyCounts[20] = {0};
            uint32_t previewOffsets[20][2];
            int previewCount[20] = {0};
            static const int MAX_L1 = 200;
            static char level1Ids[MAX_L1][8];
            static int  level1PageIdx[MAX_L1];
            int  level1Count = 0;
            char pageIds[20][8];
            if (pageCount > 0) {
                for (int i = 0; i < pageCount; i++) {
                    const char* lp = pageEntries[i];
                    int len = pageLens[i];
                    int k = 0, cnt = 0, c4pos = -1;
                    for (; k < len; k++) {
                        if (lp[k] == ',' && ++cnt == 4) { c4pos = k; break; }
                    }
                    if (c4pos >= 0 && c4pos + 9 <= len) {
                        memcpy(pageIds[i], lp + c4pos + 1, 8);
                    } else {
                        memset(pageIds[i], 0, 8);
                    }
                }

                f.seek(0);
                while (f.available()) {
                    uint32_t rowStart = f.position();
                    int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                    if (n <= 0) continue;
                    line[n] = '\0';
                    while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                    if (n == 0) continue;
                    char s = line[n-1];
                    if (s != '1' && s != '3') continue;
                    int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                    for (int j = 0; j < n; j++) {
                        if (line[j] == ',') {
                            if      (c1 < 0) c1 = j;
                            else if (c2 < 0) c2 = j;
                            else if (c3 < 0) c3 = j;
                            else if (c4 < 0) c4 = j;
                            else if (c5 < 0) c5 = j;
                            cLast = j;
                        }
                    }
                    if (c5 < 0 || cLast < 0) continue;
                    int replyToLen = cLast - c5 - 1;
                    if (replyToLen != 8) continue;  // top-level or malformed
                    const char* rt = line + c5 + 1;
                    int pageIdx = -1;
                    bool isLevel1 = false;
                    for (int i = 0; i < pageCount; i++) {
                        if (memcmp(rt, pageIds[i], 8) == 0) { pageIdx = i; isLevel1 = true; break; }
                    }
                    if (!isLevel1) {
                        for (int i = 0; i < level1Count; i++) {
                            if (memcmp(rt, level1Ids[i], 8) == 0) { pageIdx = level1PageIdx[i]; break; }
                        }
                    }
                    if (pageIdx < 0) continue;
                    replyCounts[pageIdx]++;
                    if (isLevel1 && level1Count < MAX_L1) {
                        memcpy(level1Ids[level1Count], line + c4 + 1, 8);
                        level1PageIdx[level1Count] = pageIdx;
                        level1Count++;
                    }
                    // Stash offset of up to 2 approved descendants per entry.
                    // Final emission depends on the entry's total reply_count.
                    if (s == '1' && previewCount[pageIdx] < 2) {
                        previewOffsets[pageIdx][previewCount[pageIdx]] = rowStart;
                        previewCount[pageIdx]++;
                    }
                }
            }
            // f stays open here; emit loop seeks to stashed offsets.

            String json;
            // Heap-friendly reserve: budget ~800 bytes per entry (covers
            // typical message + preview replies; worst-case escaping grows
            // via realloc if needed). Dropped from 2000/entry (40KB peak
            // for 20 entries) because the 40KB contiguous block was
            // competing with mbedtls's 16KB TLS read buffer under LAN
            // load, triggering "fillBuffer: Not enough memory" cascades.
            // 16KB reserve fits comfortably alongside TLS state; if a
            // specific page has heavy escaping, it grows naturally.
            json.reserve(256 + pageCount * 800);
            json = "{\"entries\":[";
            bool first = true;
            for (int i = pageCount - 1; i >= 0; i--) {
                const char* lp = pageEntries[i];
                int len = pageLens[i];
                int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                for (int j = 0; j < len; j++) {
                    if (lp[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0 || cLast >= len - 1) continue;

                String timeField(lp);             timeField.remove(c1);
                String countryField(lp + c1 + 1); countryField.remove(c2 - c1 - 1);
                String nameField(lp + c2 + 1);    nameField.remove(c3 - c2 - 1);
                String messageField(lp + c3 + 1); messageField.remove(c4 - c3 - 1);
                String idField(lp + c4 + 1);      idField.remove(c5 - c4 - 1);

                if (!first) json += ",";
                json += "{\"time\":\""    + jsonEscape(timeField)    + "\",";
                json += "\"country\":\""  + jsonEscape(countryField) + "\",";
                json += "\"name\":\""     + jsonEscape(nameField)    + "\",";
                json += "\"message\":\""  + jsonEscape(messageField) + "\",";
                json += "\"id\":\""       + jsonEscape(idField)      + "\",";
                json += "\"reply_count\":" + String(replyCounts[i]);
                // Hybrid preview: bundle only when the thread is small enough
                // that the preview IS the whole thread (<= 2 replies). Bigger
                // threads get a "Show N replies" button instead. Pass 3 stashed
                // the file offset of each candidate row; here we seek to each
                // offset and read one line. Constant work per preview, no more
                // O(n^2) full-file rescans under sustained load.
                if (replyCounts[i] > 0 && replyCounts[i] <= 2 && previewCount[i] > 0) {
                    json += ",\"preview_replies\":[";
                    char pline[400];
                    for (int p = 0; p < previewCount[i]; p++) {
                        f.seek(previewOffsets[i][p]);
                        int pn = f.readBytesUntil('\n', pline, sizeof(pline) - 1);
                        if (pn <= 0) continue;
                        pline[pn] = '\0';
                        while (pn > 0 && (pline[pn-1] == '\r' || pline[pn-1] == ' ' || pline[pn-1] == '\t')) { pline[--pn] = '\0'; }
                        if (pn == 0) continue;
                        int pc1=-1,pc2=-1,pc3=-1,pc4=-1,pc5=-1,pcLast=-1;
                        for (int j = 0; j < pn; j++) {
                            if (pline[j] == ',') {
                                if      (pc1 < 0) pc1 = j;
                                else if (pc2 < 0) pc2 = j;
                                else if (pc3 < 0) pc3 = j;
                                else if (pc4 < 0) pc4 = j;
                                else if (pc5 < 0) pc5 = j;
                                pcLast = j;
                            }
                        }
                        if (pc1<0||pc2<0||pc3<0||pc4<0||pc5<0||pcLast<0) continue;
                        String pt(pline);             pt.remove(pc1);
                        String pco(pline + pc1 + 1); pco.remove(pc2 - pc1 - 1);
                        String pna(pline + pc2 + 1); pna.remove(pc3 - pc2 - 1);
                        String pms(pline + pc3 + 1); pms.remove(pc4 - pc3 - 1);
                        String pid(pline + pc4 + 1); pid.remove(pc5 - pc4 - 1);
                        String prt(pline + pc5 + 1); prt.remove(pcLast - pc5 - 1);
                        if (p > 0) json += ",";
                        json += "{\"time\":\""    + jsonEscape(pt)  + "\",";
                        json += "\"country\":\""  + jsonEscape(pco) + "\",";
                        json += "\"name\":\""     + jsonEscape(pna) + "\",";
                        json += "\"message\":\""  + jsonEscape(pms) + "\",";
                        json += "\"id\":\""       + jsonEscape(pid) + "\",";
                        json += "\"reply_to\":\"" + jsonEscape(prt) + "\"}";
                    }
                    json += "]";
                }
                json += "}";
                first = false;
            }
            f.close();
            json += "],\"hasMore\":" + String(hasMore ? "true" : "false");
            json += ",\"total\":" + String(totalApproved);
            json += ",\"countries\":" + String(seenCountryCount);
            if (isSearch) json += ",\"matching\":" + String(totalMatching);
            json += "}";

            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            // Edge-cache the unfiltered list for 30s; skip for searches.
            if (!isSearch) r->addHeader("Cache-Control", "public, max-age=30");
            request->send(r);
            logConsole(request, 200);
            return;
        }

        // Given id=<id>, return the top-level entry fields + page number that
        // contains it. Accepts both top-level ids and reply ids (level-1 or
        // level-2); for reply ids we trace up to the top-level and also emit
        // target_id so the client can scroll to the specific reply after the
        // pinned thread loads. Only approved rows are considered.
        if (url == "/guestbook/locate") {
            if (!request->hasParam("id")) {
                request->send(400, "application/json", "{\"found\":false}");
                return;
            }
            String idQuery = request->getParam("id")->value();
            idQuery.trim();
            idQuery.toLowerCase();
            if (!isValidCrockfordId(idQuery.c_str(), idQuery.length())) {
                request->send(400, "application/json", "{\"found\":false}");
                return;
            }

            if (!SD.exists("/guestbook.csv")) {
                request->send(200, "application/json", "{\"found\":false}");
                return;
            }
            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(200, "application/json", "{\"found\":false}"); return; }

            const char* idCstr = idQuery.c_str();

            // Pass A: find the queried row (top-level or reply). Capture its
            // reply_to so we can trace up on subsequent passes.
            char queryReplyTo[9] = {0};
            bool queryFound = false;
            char line[400];
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;
                int c1=-1,c2=-1,c3=-1,c4=-1,c5=-1,cLast=-1;
                for (int j = 0; j < n; j++) {
                    if (line[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1<0||c2<0||c3<0||c4<0||c5<0||cLast<0) continue;
                if (line[n-1] != '1') continue;
                int entryIdLen = c5 - c4 - 1;
                if (entryIdLen != 8 || memcmp(line + c4 + 1, idCstr, 8) != 0) continue;
                queryFound = true;
                int replyToLen = cLast - c5 - 1;
                if (replyToLen == 8) memcpy(queryReplyTo, line + c5 + 1, 8);
                break;
            }
            if (!queryFound) { f.close(); request->send(200, "application/json", "{\"found\":false}"); return; }

            char topLevelId[9] = {0};
            bool queryIsTopLevel = (queryReplyTo[0] == '\0');
            if (queryIsTopLevel) {
                memcpy(topLevelId, idCstr, 8);
            } else {
                // Pass B: resolve the query's immediate parent to reach the
                // top-level. Accept tombstones (status=3) here so that a
                // permalink to an approved level-2 whose level-1 parent was
                // tombstoned still resolves to the top-level thread.
                f.seek(0);
                bool parentFound = false;
                while (f.available()) {
                    int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                    if (n <= 0) continue;
                    line[n] = '\0';
                    while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                    if (n == 0) continue;
                    int c1=-1,c2=-1,c3=-1,c4=-1,c5=-1,cLast=-1;
                    for (int j = 0; j < n; j++) {
                        if (line[j] == ',') {
                            if      (c1 < 0) c1 = j;
                            else if (c2 < 0) c2 = j;
                            else if (c3 < 0) c3 = j;
                            else if (c4 < 0) c4 = j;
                            else if (c5 < 0) c5 = j;
                            cLast = j;
                        }
                    }
                    if (c1<0||c2<0||c3<0||c4<0||c5<0||cLast<0) continue;
                    if (line[n-1] != '1' && line[n-1] != '3') continue;
                    int entryIdLen = c5 - c4 - 1;
                    if (entryIdLen != 8 || memcmp(line + c4 + 1, queryReplyTo, 8) != 0) continue;
                    parentFound = true;
                    int replyToLen = cLast - c5 - 1;
                    if (replyToLen == 0) {
                        memcpy(topLevelId, queryReplyTo, 8);
                    } else if (replyToLen == 8) {
                        memcpy(topLevelId, line + c5 + 1, 8);
                    }
                    break;
                }
                if (!parentFound || topLevelId[0] == '\0') {
                    f.close();
                    request->send(200, "application/json", "{\"found\":false}");
                    return;
                }
            }

            // Pass C: snapshot the top-level row + count approved top-levels
            // for page computation (matches /guestbook/entries pagination).
            f.seek(0);
            int totalApproved = 0;
            int matchOldestFirstIdx = -1;
            int approvedIdx = 0;
            char matchedLine[400];
            int mc1 = -1, mc2 = -1, mc3 = -1, mc4 = -1, mc5 = -1;
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;
                int c1=-1,c2=-1,c3=-1,c4=-1,c5=-1,cLast=-1;
                for (int j = 0; j < n; j++) {
                    if (line[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1<0||c2<0||c3<0||c4<0||c5<0||cLast<0) continue;
                if (line[n-1] != '1') continue;
                if (cLast - c5 > 1) continue;  // only top-levels participate in pagination
                int entryIdLen = c5 - c4 - 1;
                if (entryIdLen == 8 &&
                    memcmp(line + c4 + 1, topLevelId, 8) == 0 &&
                    matchOldestFirstIdx < 0) {
                    matchOldestFirstIdx = approvedIdx;
                    memcpy(matchedLine, line, n);
                    matchedLine[n] = '\0';
                    mc1 = c1; mc2 = c2; mc3 = c3; mc4 = c4; mc5 = c5;
                }
                approvedIdx++;
                totalApproved++;
            }
            f.close();

            if (matchOldestFirstIdx < 0) {
                request->send(200, "application/json", "{\"found\":false}");
                return;
            }
            int newestFirstIdx = totalApproved - 1 - matchOldestFirstIdx;
            int pageNum = (newestFirstIdx / 20) + 1;

            String timeF(matchedLine);             timeF.remove(mc1);
            String countryF(matchedLine + mc1 + 1); countryF.remove(mc2 - mc1 - 1);
            String nameF(matchedLine + mc2 + 1);    nameF.remove(mc3 - mc2 - 1);
            String msgF(matchedLine + mc3 + 1);     msgF.remove(mc4 - mc3 - 1);
            String idF(matchedLine + mc4 + 1);      idF.remove(mc5 - mc4 - 1);

            String body;
            body.reserve(576);
            body = "{\"found\":true,\"page\":";
            body += pageNum;
            body += ",\"entry\":{";
            body += "\"time\":\"";    body += jsonEscape(timeF);    body += "\",";
            body += "\"country\":\""; body += jsonEscape(countryF); body += "\",";
            body += "\"name\":\"";    body += jsonEscape(nameF);    body += "\",";
            body += "\"message\":\""; body += jsonEscape(msgF);     body += "\",";
            body += "\"id\":\"";      body += jsonEscape(idF);      body += "\"}";
            if (!queryIsTopLevel) {
                body += ",\"target_id\":\"";
                body += idQuery;
                body += "\"";
            }
            body += "}";
            request->send(200, "application/json", body);
            logConsole(request, 200);
            return;
        }

        // Full reply subtree for a top-level entry (direct + indirect replies).
        // Two passes over the CSV:
        //   Pass A: collect ids of direct replies (reply_to == topLevelId).
        //   Pass B: emit any approved-or-tombstoned row whose reply_to is either
        //           the top-level id (level-1) or any collected level-1 id
        //           (level-2). Rows include their own reply_to so the client
        //           can render "replying to X" attribution on level-2.
        // Tombstones (status=3) are emitted with name+message empty and a
        // removed=true flag so the client can render "[removed]" in place.
        if (url == "/guestbook/replies") {
            if (!request->hasParam("id")) {
                request->send(400, "application/json", "{\"entries\":[]}");
                return;
            }
            String pid = request->getParam("id")->value();
            pid.trim();
            pid.toLowerCase();
            if (!isValidCrockfordId(pid.c_str(), pid.length())) {
                request->send(400, "application/json", "{\"entries\":[]}");
                return;
            }

            if (!SD.exists("/guestbook.csv")) {
                request->send(200, "application/json", "{\"entries\":[]}");
                return;
            }
            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(200, "application/json", "{\"entries\":[]}"); return; }

            const char* pidCstr = pid.c_str();
            // Cap at 200 level-1 replies per top-level; realistic ceiling and
            // bounds worst-case pass-B lookup cost.
            static const int MAX_L1 = 200;
            static char level1Ids[MAX_L1][8];
            int level1Count = 0;

            char line[400];
            // Pass A.
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;
                char s = line[n-1];
                if (s != '1' && s != '3') continue;  // approved or tombstone
                int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                for (int j = 0; j < n; j++) {
                    if (line[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0) continue;
                if (cLast - c5 - 1 != 8) continue;
                if (memcmp(line + c5 + 1, pidCstr, 8) != 0) continue;
                if (level1Count < MAX_L1) {
                    memcpy(level1Ids[level1Count], line + c4 + 1, 8);
                    level1Count++;
                }
            }

            String json;
            json.reserve(4096);
            json = "{\"entries\":[";
            bool first = true;

            // Pass B: emit level-1 and level-2 rows chronologically.
            f.seek(0);
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;
                char s = line[n-1];
                if (s != '1' && s != '3') continue;
                int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                for (int j = 0; j < n; j++) {
                    if (line[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0) continue;
                int replyToLen = cLast - c5 - 1;
                if (replyToLen != 8) continue;  // top-level row; not part of subtree
                bool isLevel1 = (memcmp(line + c5 + 1, pidCstr, 8) == 0);
                bool isLevel2 = false;
                if (!isLevel1) {
                    for (int i = 0; i < level1Count; i++) {
                        if (memcmp(line + c5 + 1, level1Ids[i], 8) == 0) { isLevel2 = true; break; }
                    }
                    if (!isLevel2) continue;
                }

                String timeField(line);             timeField.remove(c1);
                String countryField(line + c1 + 1); countryField.remove(c2 - c1 - 1);
                String nameField(line + c2 + 1);    nameField.remove(c3 - c2 - 1);
                String messageField(line + c3 + 1); messageField.remove(c4 - c3 - 1);
                String idField(line + c4 + 1);      idField.remove(c5 - c4 - 1);
                String replyToField(line + c5 + 1); replyToField.remove(cLast - c5 - 1);

                if (!first) json += ",";
                json += "{\"time\":\""    + jsonEscape(timeField)    + "\",";
                json += "\"country\":\""  + jsonEscape(countryField) + "\",";
                json += "\"name\":\""     + jsonEscape(nameField)    + "\",";
                json += "\"message\":\""  + jsonEscape(messageField) + "\",";
                json += "\"id\":\""       + jsonEscape(idField)      + "\",";
                json += "\"reply_to\":\"" + jsonEscape(replyToField) + "\"";
                if (s == '3') json += ",\"removed\":true";
                json += "}";
                first = false;
            }
            f.close();
            json += "]}";
            request->send(200, "application/json", json);
            logConsole(request, 200);
            return;
        }

        // guestbook RSS: latest 20 approved entries
        if (url == "/guestbook.rss") {
            String rss;
            rss.reserve(4096);
            rss = F("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<rss version=\"2.0\"><channel>"
                    "<title>HelloESP | Guestbook</title>"
                    "<link>https://helloesp.com/guestbook</link>"
                    "<description>Approved guestbook entries from HelloESP.</description>"
                    "<language>en</language>");

            auto xmlEscape = [](const String& s) {
                String o;
                o.reserve(s.length() + 8);
                for (size_t i = 0; i < s.length(); i++) {
                    char c = s[i];
                    if (c == '&') o += "&amp;";
                    else if (c == '<') o += "&lt;";
                    else if (c == '>') o += "&gt;";
                    else if (c == '"') o += "&quot;";
                    else if (c == '\'') o += "&apos;";
                    else o += c;
                }
                return o;
            };

            if (SD.exists("/guestbook.csv")) {
                File f = SD.open("/guestbook.csv", FILE_READ);
                if (f) {
                    String ring[20];
                    int count = 0, head = 0;
                    while (f.available()) {
                        String line = f.readStringUntil('\n');
                        line.trim();
                        if (line.length() == 0) continue;
                        // v3: need last two commas (cLast=before status, c5=before reply_to)
                        // to detect top-level (reply_to empty) approved rows.
                        int cLast = line.lastIndexOf(',');
                        if (cLast < 0) continue;
                        int c5 = line.lastIndexOf(',', cLast - 1);
                        if (c5 < 0) continue;
                        String a = line.substring(cLast + 1); a.trim();
                        if (a != "1") continue;
                        if (cLast - c5 > 1) continue;  // skip replies
                        ring[head] = line;
                        head = (head + 1) % 20;
                        if (count < 20) count++;
                    }
                    f.close();
                    // emit newest first
                    for (int k = 0; k < count; k++) {
                        int idx = (head - 1 - k + 20) % 20;
                        String& line = ring[idx];
                        // v3 schema: time,country,name,message,id,reply_to,status
                        int c1 = line.indexOf(',');
                        int c2 = line.indexOf(',', c1 + 1);
                        int c3 = line.indexOf(',', c2 + 1);
                        int c4 = line.indexOf(',', c3 + 1);
                        int c5 = line.indexOf(',', c4 + 1);
                        if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || c5 <= c4) continue;
                        String t = line.substring(0, c1);
                        String country = line.substring(c1 + 1, c2);
                        String name = xmlEscape(line.substring(c2 + 1, c3));
                        String msg = xmlEscape(line.substring(c3 + 1, c4));
                        String entryId = line.substring(c4 + 1, c5);
                        rss += "<item><title>";
                        rss += name;
                        if (country.length() > 0 && country != "??") { rss += " ("; rss += country; rss += ")"; }
                        rss += "</title><link>https://helloesp.com/guestbook#";
                        rss += entryId;
                        rss += "</link>";
                        rss += "<guid isPermaLink=\"false\">gb-";
                        rss += entryId;
                        rss += "</guid><pubDate>";
                        rss += t;
                        rss += "</pubDate><description>";
                        rss += msg;
                        rss += "</description></item>";
                    }
                }
            }
            rss += "</channel></rss>";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/rss+xml", rss);
            r->addHeader("Cache-Control", "public, max-age=600");
            request->send(r);
            logConsole(request, 200);
            return;
        }

        // guestbook submit
        if (url == "/guestbook/submit" && request->method() == HTTP_POST) {
            if (isRateLimited(clientIpString(request))) {
                request->send(429, "text/plain", "Please wait before posting again");
                return;
            }
            if (pendingGuestbook >= MAX_PENDING_GUESTBOOK) {
                AsyncWebServerResponse *r = request->beginResponse(429, "text/plain",
                    "Moderation queue is full, please try again later");
                r->addHeader("Retry-After", "3600");
                request->send(r);
                return;
            }
            if (!request->hasParam("name", true) || !request->hasParam("message", true)) {
                request->send(400, "text/plain", "Name and message required");
                return;
            }
            String name = request->getParam("name", true)->value();
            String message = request->getParam("message", true)->value();
            name.trim();
            message.trim();
            if (name.length() == 0 || name.length() > 32 || message.length() == 0 || message.length() > 200) {
                request->send(400, "text/plain", "Name (1-32 chars) and message (1-200 chars) required");
                return;
            }
            // Strip control chars, neutralize CSV delimiters, and drop the
            // Unicode bidi-override + invisible-formatting code points. UTF-8
            // encodes those as fixed 3-byte sequences:
            //   E2 80 8B..8F  (U+200B..U+200F, ZWSP/ZWNJ/ZWJ/LRM/RLM)
            //   E2 80 AA..AE  (U+202A..U+202E, LRE/RLE/PDF/LRO/RLO)
            //   E2 81 A6..A9  (U+2066..U+2069, LRI/RLI/FSI/PDI)
            //   EF BB BF      (U+FEFF, BOM / ZWNBSP)
            // Without this, an attacker can RTL-override a guestbook display
            // name to spoof another entry in the rendered list.
            auto sanitizeCsv = [](String &s) {
                String out;
                out.reserve(s.length());
                size_t i = 0;
                while (i < s.length()) {
                    unsigned char c = (unsigned char)s[i];
                    if (c < 0x20 || c == 0x7F) { i++; continue; }
                    if (c == '\\') { i++; continue; }
                    if (c == ',')  { out += ' '; i++; continue; }
                    if (c == '"')  { out += '\''; i++; continue; }
                    // U+200B..U+200F, U+202A..U+202E (E2 80 ..)
                    if (c == 0xE2 && i + 2 < s.length()
                        && (unsigned char)s[i+1] == 0x80
                        && (((unsigned char)s[i+2] >= 0x8B && (unsigned char)s[i+2] <= 0x8F) ||
                            ((unsigned char)s[i+2] >= 0xAA && (unsigned char)s[i+2] <= 0xAE))) {
                        i += 3; continue;
                    }
                    // U+2066..U+2069 (E2 81 A6..A9)
                    if (c == 0xE2 && i + 2 < s.length()
                        && (unsigned char)s[i+1] == 0x81
                        && (unsigned char)s[i+2] >= 0xA6 && (unsigned char)s[i+2] <= 0xA9) {
                        i += 3; continue;
                    }
                    // U+FEFF (EF BB BF)
                    if (c == 0xEF && i + 2 < s.length()
                        && (unsigned char)s[i+1] == 0xBB && (unsigned char)s[i+2] == 0xBF) {
                        i += 3; continue;
                    }
                    out += (char)c;
                    i++;
                }
                s = out;
                s.trim();
            };
            sanitizeCsv(name);
            sanitizeCsv(message);
            if (name.length() == 0 || message.length() == 0) {
                request->send(400, "text/plain", "Name and message required");
                return;
            }

            char ccBuf[3];
            normalizeCountry(
                request->hasHeader("CF-IPCountry") ? request->header("CF-IPCountry").c_str() : "",
                ccBuf);
            String country(ccBuf);

            // Optional reply_to: must be 8-char Crockford, must name an existing
            // approved entry that is either top-level or itself a reply to a
            // top-level entry. The data layer alone caps nesting at 2 levels.
            String replyTo = request->hasParam("reply_to", true)
                ? request->getParam("reply_to", true)->value() : String("");
            replyTo.trim();
            replyTo.toLowerCase();
            if (replyTo.length() > 0) {
                if (!isValidCrockfordId(replyTo.c_str(), replyTo.length())) {
                    request->send(400, "text/plain", "Invalid reply target");
                    return;
                }
                if (!isValidReplyParent(replyTo.c_str())) {
                    request->send(400, "text/plain", "Reply target not found");
                    return;
                }
            }

            File f = SD.open("/guestbook.csv", FILE_APPEND);
            if (!f) {
                logError("sd", "guestbook.csv append failed");
                request->send(500, "text/plain", "Failed to save");
                return;
            }
            char newId[9];
            generateGuestbookId(newId);
            f.print(getTimestamp()); f.print(",");
            f.print(country);       f.print(",");
            f.print(name);          f.print(",");
            f.print(message);       f.print(",");
            f.print(newId);         f.print(",");
            f.print(replyTo);       f.println(",0");
            f.close();
            pendingGuestbook++;
            gbCountAll++;
            currentWeek.guestbook++;
            currentMonth.guestbook++;
            currentYear.guestbook++;
            strncpy(notifyEntryName,    name.c_str(),    sizeof(notifyEntryName) - 1);
            notifyEntryName[sizeof(notifyEntryName) - 1] = '\0';
            strncpy(notifyEntryCountry, country.c_str(), sizeof(notifyEntryCountry) - 1);
            notifyEntryCountry[sizeof(notifyEntryCountry) - 1] = '\0';
            strncpy(notifyEntryMessage, message.c_str(), sizeof(notifyEntryMessage) - 1);
            notifyEntryMessage[sizeof(notifyEntryMessage) - 1] = '\0';
            // Release fence: ensure the buffer writes above are visible to
            // the main-loop reader before it sees pendingNotifyFlag=true.
            // Without this, AsyncTCP (often core 0) can publish the flag
            // before the strncpy stores propagate to core 1's view.
            std::atomic_thread_fence(std::memory_order_release);
            pendingNotifyFlag = true;

            request->send(200, "text/plain", "Thanks! Your message will appear after review.");
            logConsole(request, 200);
            return;
        }

        // guestbook admin (paginated, filterable, searchable)
        if (url == "/guestbook/pending") {
            if (!adminAuth(request)) return;
            int page = request->hasParam("page") ? request->getParam("page")->value().toInt() : 1;
            if (page < 1) page = 1;
            int perPage = 20;
            // filter: "new" = 0 only, "approved" = 1, "denied" = 2, "all" = everything
            String filter = request->hasParam("filter") ? request->getParam("filter")->value() : String("all");
            char wantStatus = 0;
            bool matchAny = false;
            if      (filter == "new")      wantStatus = '0';
            else if (filter == "approved") wantStatus = '1';
            else if (filter == "denied")   wantStatus = '2';
            else                           matchAny = true;

            // Optional case-insensitive substring search over name + message.
            // When q is non-empty, we can't use in-memory counts for the filter total
            // (counts don't know about q), so we do a streaming scan-and-count pass
            // inline with the collect pass.
            String qStr = request->hasParam("q") ? request->getParam("q")->value() : String("");
            qStr.trim();
            if (qStr.length() > 80) qStr = qStr.substring(0, 80);
            const char* qCstr = qStr.c_str();
            int qLen = qStr.length();
            bool isSearch = qLen > 0;

            int countNew = pendingGuestbook;
            int countApproved = gbCountApproved;
            int countDenied = gbCountDenied;
            int countAll = gbCountAll;

            auto buildCountsJson = [&]() -> String {
                String s = "\"counts\":{\"new\":";
                s += countNew; s += ",\"approved\":"; s += countApproved;
                s += ",\"denied\":"; s += countDenied; s += ",\"all\":";
                s += countAll; s += "}";
                return s;
            };

            if (!SD.exists("/guestbook.csv")) {
                String body = "{\"entries\":[],\"hasMore\":false," + buildCountsJson() + "}";
                request->send(200, "application/json", body);
                return;
            }

            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(500, "text/plain", "Read failed"); return; }

            static char pageEntries[20][400];
            int pageLens[20] = {0};
            int pageIdxs[20] = {0};
            int pageCount = 0;

            char line[400];
            int rawIdx = 0;
            int filteredIdx = 0;
            int totalMatching = 0;  // for search mode; used to compute fromIdx after scan

            if (isSearch) {
                // Search mode: streaming scan counts matches AND stashes candidates.
                // Keep a sliding window of the last `perPage` matches since we want the
                // newest-first display (just like archived entries) but we read oldest-first.
                // Simpler: collect all match positions then resolve page at end. Cap memory
                // by using a bounded ring of up to MAX_SEARCH_TRACK matches; older matches
                // past page's range are discarded.
                // Easier still: two passes. Pass 1 counts matches, pass 2 collects page.
                // Two passes at 10k entries with char buffers is still ~O(0 heap), so fine.

                // Pass 1: count matches and per-status counts (re-derive counts restricted
                // to search matches so badges reflect search scope).
                int cNewS = 0, cApprovedS = 0, cDeniedS = 0, cAllS = 0;
                while (f.available()) {
                    int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                    if (n <= 0) continue;
                    line[n] = '\0';
                    while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                    if (n == 0) continue;
                    // v3 schema: time,country,name,message,id,reply_to,status
                    int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                    for (int j = 0; j < n; j++) {
                        if (line[j] == ',') {
                            if      (c1 < 0) c1 = j;
                            else if (c2 < 0) c2 = j;
                            else if (c3 < 0) c3 = j;
                            else if (c4 < 0) c4 = j;
                            else if (c5 < 0) c5 = j;
                            cLast = j;
                        }
                    }
                    if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0) continue;
                    if (line[n-1] == '3') continue;  // tombstones are invisible to admin UI
                    const char* nameStart = line + c2 + 1;
                    int nameLen = c3 - c2 - 1;
                    const char* msgStart = line + c3 + 1;
                    int msgLen = c4 - c3 - 1;
                    bool qMatch = containsCI(nameStart, nameLen, qCstr, qLen) ||
                                  containsCI(msgStart,  msgLen,  qCstr, qLen);
                    if (!qMatch) continue;
                    char s = line[n-1];
                    cAllS++;
                    if      (s == '0') cNewS++;
                    else if (s == '1') cApprovedS++;
                    else if (s == '2') cDeniedS++;
                    bool statusMatches = matchAny || s == wantStatus;
                    if (statusMatches) totalMatching++;
                }
                // In search mode, tab badges show search-scoped counts.
                countNew = cNewS; countApproved = cApprovedS;
                countDenied = cDeniedS; countAll = cAllS;
            } else {
                totalMatching =
                    matchAny            ? countAll      :
                    wantStatus == '0'   ? countNew      :
                    wantStatus == '1'   ? countApproved :
                    /* '2' */             countDenied;
            }

            if (totalMatching == 0) {
                f.close();
                String body = "{\"entries\":[],\"hasMore\":false," + buildCountsJson() + "}";
                request->send(200, "application/json", body);
                return;
            }

            int skip = (page - 1) * perPage;
            if (skip >= totalMatching) {
                f.close();
                String body = "{\"entries\":[],\"hasMore\":false," + buildCountsJson() + "}";
                request->send(200, "application/json", body);
                return;
            }
            int fromIdx = totalMatching - skip - perPage;
            int toIdx = totalMatching - skip - 1;
            if (fromIdx < 0) fromIdx = 0;
            bool hasMore = fromIdx > 0;

            // Collect-pass: rewind and iterate again to grab the target page.
            // For search mode this is a second pass; for non-search it's the only pass.
            f.seek(0);
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;

                if (line[n-1] == '3') { rawIdx++; continue; }  // hide tombstones from admin UI
                bool statusMatches = matchAny || line[n-1] == wantStatus;
                bool qMatches = true;
                if (isSearch) {
                    int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                    for (int j = 0; j < n; j++) {
                        if (line[j] == ',') {
                            if      (c1 < 0) c1 = j;
                            else if (c2 < 0) c2 = j;
                            else if (c3 < 0) c3 = j;
                            else if (c4 < 0) c4 = j;
                            else if (c5 < 0) c5 = j;
                            cLast = j;
                        }
                    }
                    if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0) { rawIdx++; continue; }
                    qMatches = containsCI(line + c2 + 1, c3 - c2 - 1, qCstr, qLen) ||
                               containsCI(line + c3 + 1, c4 - c3 - 1, qCstr, qLen);
                }

                if (statusMatches && qMatches) {
                    if (filteredIdx >= fromIdx && filteredIdx <= toIdx && pageCount < 20) {
                        memcpy(pageEntries[pageCount], line, n + 1);
                        pageLens[pageCount] = n;
                        pageIdxs[pageCount] = rawIdx;
                        pageCount++;
                    }
                    filteredIdx++;
                }
                rawIdx++;
            }
            f.close();

            // Build JSON from the cached page entries. Reserve 650 bytes/entry
            // to absorb worst-case jsonEscape expansion (200-char message can
            // double if every char needs escaping) without mid-build reallocs
            // that would fragment heap.
            String json;
            json.reserve(2048 + pageCount * 650);
            json = "{\"entries\":[";
            bool first = true;
            for (int i = pageCount - 1; i >= 0; i--) {
                const char* lp = pageEntries[i];
                int len = pageLens[i];
                // v3 schema: time,country,name,message,id,reply_to,status
                int c1 = -1, c2 = -1, c3 = -1, c4 = -1, c5 = -1, cLast = -1;
                for (int j = 0; j < len; j++) {
                    if (lp[j] == ',') {
                        if      (c1 < 0) c1 = j;
                        else if (c2 < 0) c2 = j;
                        else if (c3 < 0) c3 = j;
                        else if (c4 < 0) c4 = j;
                        else if (c5 < 0) c5 = j;
                        cLast = j;
                    }
                }
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0 || c5 < 0 || cLast < 0 || cLast >= len - 1) continue;

                String timeField(lp);             timeField.remove(c1);
                String countryField(lp + c1 + 1); countryField.remove(c2 - c1 - 1);
                String nameField(lp + c2 + 1);    nameField.remove(c3 - c2 - 1);
                String messageField(lp + c3 + 1); messageField.remove(c4 - c3 - 1);
                String idField(lp + c4 + 1);      idField.remove(c5 - c4 - 1);
                String replyToField(lp + c5 + 1); replyToField.remove(cLast - c5 - 1);
                char statusChar = lp[cLast + 1];

                if (!first) json += ",";
                json += "{\"idx\":" + String(pageIdxs[i]) + ",";
                json += "\"time\":\""     + jsonEscape(timeField)    + "\",";
                json += "\"country\":\""  + jsonEscape(countryField) + "\",";
                json += "\"name\":\""     + jsonEscape(nameField)    + "\",";
                json += "\"message\":\""  + jsonEscape(messageField) + "\",";
                json += "\"id\":\""       + jsonEscape(idField)      + "\",";
                json += "\"reply_to\":\"" + jsonEscape(replyToField) + "\",";
                json += "\"approved\":"; json += statusChar; json += "}";
                first = false;
            }
            json += "],\"hasMore\":" + String(hasMore ? "true" : "false") + ",";
            json += buildCountsJson() + "}";
            request->send(200, "application/json", json);
            return;
        }

        // Batch moderation: apply many status changes in a single CSV rewrite.
        if (url == "/guestbook/moderate-batch" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!request->hasParam("ops", true)) {
                request->send(400, "text/plain", "Missing ops");
                return;
            }
            String opsStr = request->getParam("ops", true)->value();
            if (opsStr.length() == 0 || opsStr.length() > 4000) {
                request->send(400, "text/plain", "Invalid ops");
                return;
            }

            // Parse "idx:status,idx:status,..." into parallel fixed arrays.
            // Cap at 100 ops per batch (plenty for human-driven moderation).
            const int MAX_OPS = 100;
            int opIdx[MAX_OPS];
            char opStatus[MAX_OPS];
            int opCount = 0;
            int start = 0;
            int slen = opsStr.length();
            const char* ops = opsStr.c_str();
            while (start < slen && opCount < MAX_OPS) {
                // find next comma or end
                int end = start;
                while (end < slen && ops[end] != ',') end++;
                // parse "idx:status" in [start..end)
                int colon = -1;
                for (int i = start; i < end; i++) if (ops[i] == ':') { colon = i; break; }
                if (colon > start && colon < end - 1) {
                    char s = ops[colon + 1];
                    if (s == '0' || s == '1' || s == '2' || s == '3') {
                        // parse idx
                        int parsedIdx = 0;
                        bool ok = true;
                        for (int i = start; i < colon; i++) {
                            if (ops[i] < '0' || ops[i] > '9') { ok = false; break; }
                            parsedIdx = parsedIdx * 10 + (ops[i] - '0');
                        }
                        if (ok) {
                            opIdx[opCount] = parsedIdx;
                            opStatus[opCount] = s;
                            opCount++;
                        }
                    }
                }
                start = end + 1;
            }
            if (opCount == 0) {
                request->send(400, "text/plain", "No valid ops");
                return;
            }

            if (!SD.exists("/guestbook.csv")) {
                request->send(404, "text/plain", "No guestbook data");
                return;
            }

            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(500, "text/plain", "Read failed"); return; }
            File out = SD.open("/guestbook.tmp", FILE_WRITE);
            if (!out) { f.close(); request->send(500, "text/plain", "Write failed"); return; }

            // Counter deltas accumulated across all ops; applied after rename succeeds.
            int dPending = 0, dApproved = 0, dDenied = 0, dAll = 0;
            int appliedCount = 0;   // ops that actually matched a CSV line
            bool anyDeletes = false;

            char line[400];
            int idx = 0;
            while (f.available()) {
                int n = f.readBytesUntil('\n', line, sizeof(line) - 1);
                if (n <= 0) continue;
                line[n] = '\0';
                while (n > 0 && (line[n-1] == '\r' || line[n-1] == ' ' || line[n-1] == '\t')) { line[--n] = '\0'; }
                if (n == 0) continue;

                char applyStatus = 0;
                for (int i = 0; i < opCount; i++) {
                    if (opIdx[i] == idx) applyStatus = opStatus[i];
                }

                if (applyStatus != 0) {
                    char oldS = line[n-1];
                    if (applyStatus == '3') {
                        // Tombstone: rewrite with name + message blanked so the
                        // row stays parseable and replies can still trace chain
                        // continuity, but the original content is gone.
                        int c1=-1,c2=-1,c3=-1,c4=-1,c5=-1,cLast=-1;
                        for (int j = 0; j < n; j++) {
                            if (line[j] == ',') {
                                if      (c1 < 0) c1 = j;
                                else if (c2 < 0) c2 = j;
                                else if (c3 < 0) c3 = j;
                                else if (c4 < 0) c4 = j;
                                else if (c5 < 0) c5 = j;
                                cLast = j;
                            }
                        }
                        if (c1>=0&&c2>=0&&c3>=0&&c4>=0&&c5>=0&&cLast>=0) {
                            if      (oldS == '0') dPending--;
                            else if (oldS == '1') dApproved--;
                            else if (oldS == '2') dDenied--;
                            dAll--;
                            appliedCount++;
                            out.write((const uint8_t*)line, c2 + 1);
                            out.write((uint8_t)',');
                            out.write((uint8_t)',');
                            out.write((const uint8_t*)(line + c4 + 1), cLast - c4);
                            out.write((uint8_t)'3');
                            out.write('\n');
                            idx++;
                            continue;
                        }
                        // Malformed row + tombstone op: pass through unchanged.
                        // Falling into the status-change branch below would stamp
                        // '3' onto the last byte and decrement counters without
                        // balancing dAll, corrupting both the row and the totals.
                        out.write((const uint8_t*)line, n);
                        out.write('\n');
                        idx++;
                        continue;
                    }
                    if (oldS != applyStatus) {
                        if      (oldS == '0') dPending--;
                        else if (oldS == '1') dApproved--;
                        else if (oldS == '2') dDenied--;
                        if      (applyStatus == '0') dPending++;
                        else if (applyStatus == '1') dApproved++;
                        else if (applyStatus == '2') dDenied++;
                        line[n-1] = applyStatus;
                    }
                    appliedCount++;
                }
                out.write((const uint8_t*)line, n);
                out.write('\n');
                idx++;
            }
            f.close();
            out.close();

            if (SD.exists("/guestbook.bak")) SD.remove("/guestbook.bak");
            if (SD.exists("/guestbook.csv")) SD.rename("/guestbook.csv", "/guestbook.bak");
            if (!SD.rename("/guestbook.tmp", "/guestbook.csv")) {
                // Roll back so memory and disk don't diverge: restore the old
                // CSV and skip the in-memory delta application below. Without
                // this, a failed rename would leave guestbook.csv missing
                // while counters reflect the would-be new state.
                if (SD.exists("/guestbook.bak")) SD.rename("/guestbook.bak", "/guestbook.csv");
                request->send(500, "text/plain", "moderation save failed");
                return;
            }
            if (SD.exists("/guestbook.bak")) SD.remove("/guestbook.bak");

            pendingGuestbook += dPending;
            gbCountApproved += dApproved;
            gbCountDenied   += dDenied;
            gbCountAll      += dAll;
            pendingNotifyFlag = true;

            // Response reports what actually matched + whether any hard-deletes
            // happened. Currently always false: status=3 is a tombstone that keeps
            // the row at the same idx, so client-side data-idx attributes stay
            // valid. The flag is preserved as wire contract in case hard-delete
            // ever returns; clients reload the list when it's true.
            char resp[96];
            snprintf(resp, sizeof(resp), "{\"applied\":%d,\"submitted\":%d,\"deletes\":%s}",
                     appliedCount, opCount, anyDeletes ? "true" : "false");
            request->send(200, "application/json", resp);
            return;
        }

        // admin routes (safePath defined at file scope)
        if (url == "/admin") {
            if (!adminAuth(request)) return;
            sendGzipOrRaw(request, "/admin.html", "text/html");
            return;
        }

        if (url == "/admin/backup/list") {
            if (!adminAuth(request)) return;
            String json;
            json.reserve(4096);
            json = "[";
            bool first = true;
            std::function<void(File&, String, int)> walk = [&](File& dir, String prefix, int depth) {
                if (depth > BACKUP_MAX_DEPTH) return;
                while (true) {
                    File f = dir.openNextFile();
                    if (!f) break;
                    String name = String(f.name());
                    // openNextFile() returns full path on this library
                    // version; strip directory prefix so the recursive walk
                    // builds clean paths instead of "//foo//foo/bar.bin".
                    int lastSlash = name.lastIndexOf('/');
                    if (lastSlash >= 0) name = name.substring(lastSlash + 1);
                    String fullPath = prefix + (prefix.endsWith("/") ? "" : "/") + name;
                    if (f.isDirectory()) {
                        File sub = SD.open(fullPath);
                        if (sub) { walk(sub, fullPath, depth + 1); sub.close(); }
                    } else {
                        if (!first) json += ",";
                        json += "{\"path\":\"" + jsonEscape(fullPath) + "\",\"size\":" + String(f.size()) + "}";
                        first = false;
                    }
                    f.close();
                }
            };
            File root = SD.open("/");
            if (root) { walk(root, "", 0); root.close(); }
            json += "]";
            request->send(200, "application/json", json);
            return;
        }

        if (url == "/admin/files") {
            if (!adminAuth(request)) return;
            String path = request->hasParam("path") ? request->getParam("path")->value() : "/";
            if (!safePath(path)) { request->send(400, "text/plain", "Invalid path"); return; }
            File dir = SD.open(path);
            if (!dir || !dir.isDirectory()) {
                request->send(404, "text/plain", "Directory not found");
                return;
            }
            String json;
            json.reserve(1024);
            json = "[";
            bool first = true;
            String pathPrefix = (path == "/") ? String("/") : (path + "/");
            while (true) {
                File f = dir.openNextFile();
                if (!f) break;
                String name = String(f.name());
                int lastSlash = name.lastIndexOf('/');
                if (lastSlash >= 0) name = name.substring(lastSlash + 1);
                bool isDir = f.isDirectory();
                size_t fsize = f.size();
                f.close();

                // Tag gzip pairs so the UI can warn before someone deletes the
                // companion and breaks gzip-serve. `gz` marks the .gz file
                // itself; `paired` is true on either half when its sibling
                // exists in the same directory.
                bool isGz = !isDir && name.endsWith(".gz");
                bool paired = false;
                if (!isDir) {
                    String siblingPath = isGz
                        ? (pathPrefix + name.substring(0, name.length() - 3))
                        : (pathPrefix + name + ".gz");
                    paired = SD.exists(siblingPath);
                }

                if (!first) json += ",";
                json += "{\"name\":\"" + jsonEscape(name) + "\",";
                json += "\"size\":" + String(fsize) + ",";
                json += "\"dir\":" + String(isDir ? "true" : "false") + ",";
                json += "\"gz\":" + String(isGz ? "true" : "false") + ",";
                json += "\"paired\":" + String(paired ? "true" : "false") + "}";
                first = false;
            }
            dir.close();
            json += "]";
            request->send(200, "application/json", json);
            return;
        }

        if (url == "/admin/info") {
            if (!adminAuth(request)) return;
            const char* resetReasons[] = {
                "Unknown", "Power on", "External", "Software",
                "Panic", "Watchdog (int)", "Watchdog (task)",
                "Watchdog (other)", "Deep sleep", "Brownout", "SDIO"
            };
            int reason = (int)esp_reset_reason();
            const char* reasonStr = (reason >= 0 && reason <= 10) ? resetReasons[reason] : "Unknown";
            String json;
            json.reserve(1024);
            json  = "{";
            json += "\"firmware\":\"" + String(FIRMWARE_VERSION) + "\",";
            json += "\"chip_model\":\"" + String(ESP.getChipModel()) + "\",";
            json += "\"chip_revision\":" + String(ESP.getChipRevision()) + ",";
            json += "\"cpu_freq_mhz\":" + String(ESP.getCpuFreqMHz()) + ",";
            json += "\"flash_size_mb\":" + String(ESP.getFlashChipSize() / (1024.0f * 1024.0f), 1) + ",";
            json += "\"sdk_version\":\"" + String(ESP.getSdkVersion()) + "\",";
            json += "\"time_t_bytes\":" + String((unsigned)sizeof(time_t)) + ",";
            json += "\"mac_address\":\"" + WiFi.macAddress() + "\",";
            json += "\"local_ip\":\"" + WiFi.localIP().toString() + "\",";
            json += "\"gateway\":\"" + WiFi.gatewayIP().toString() + "\",";
            json += "\"dns\":\"" + WiFi.dnsIP().toString() + "\",";
            json += "\"subnet\":\"" + WiFi.subnetMask().toString() + "\",";
            json += "\"free_heap\":" + String(ESP.getFreeHeap()) + ",";
            json += "\"min_free_heap\":" + String(ESP.getMinFreeHeap()) + ",";
            json += "\"heap_size\":" + String(ESP.getHeapSize()) + ",";
            json += "\"last_reset\":\"" + String(reasonStr) + "\",";
            json += "\"uptime\":\"" + uptime_formatter::getUptime() + "\",";
            json += "\"rssi\":" + String(WiFi.RSSI()) + ",";
            json += "\"tx_power\":" + String(WiFi.getTxPower() / 4.0f, 1) + ",";
            json += "\"worker_configured\":" + String(strlen(cfgWorkerUrl) > 0 ? "true" : "false") + ",";
            json += "\"worker_connected\":" + String((wsConnected && wsClient.connected()) ? "true" : "false") + ",";
            json += "\"worker_reconnects\":" + String(wsReconnectCount) + ",";
            json += "\"worker_last_activity_ms\":" + String((wsConnected && lastWsActivity > 0) ? (millis() - lastWsActivity) : 0) + ",";
            // Shelly observability. Fields always emit so the frontend
            // can branch on shelly_configured; hidden in the UI when not
            // configured. last_*_seconds_ago = -1 means "never tried/seen".
            unsigned long shNow = millis();
            long shellyOkAgo  = (lastShellyOk > 0)
                ? (long)((shNow - lastShellyOk) / 1000) : -1;
            long shellyAttAgo = (lastShellyAttemptMs > 0)
                ? (long)((shNow - lastShellyAttemptMs) / 1000) : -1;
            float shCachedW = cached_power_w;
            json += "\"shelly_configured\":" + String(cfgShellyUrl[0] != '\0' ? "true" : "false") + ",";
            json += "\"shelly_last_ok_seconds_ago\":" + String(shellyOkAgo) + ",";
            json += "\"shelly_last_attempt_seconds_ago\":" + String(shellyAttAgo) + ",";
            json += "\"shelly_consecutive_failures\":" + String(shellyConsecutiveFailures) + ",";
            json += "\"shelly_last_http_status\":" + String(lastShellyHttpStatus) + ",";
            json += "\"shelly_current_power_w\":";
            json += isnan(shCachedW) ? "null" : String(shCachedW, 2);
            json += ",";
            json += "\"shelly_today_wh\":"    + String((float)today_energy_wh, 2)    + ",";
            json += "\"shelly_week_wh\":"     + String((float)week_energy_wh, 2)     + ",";
            json += "\"shelly_month_wh\":"    + String((float)month_energy_wh, 2)    + ",";
            json += "\"shelly_year_wh\":"     + String((float)year_energy_wh, 2)     + ",";
            json += "\"shelly_lifetime_wh\":" + String((float)lifetime_energy_wh, 2) + ",";
            json += "\"shelly_cost_per_kwh\":" + String(cfgCostPerKwh, 4) + ",";
            // Maintenance state for the admin UI. Worker DO is authoritative.
            time_t nowSec = time(nullptr);
            bool maintActive = (localMaintenanceUntilUnix > 0 && nowSec > 0 && (uint32_t)nowSec < localMaintenanceUntilUnix);
            json += "\"maintenance_active\":" + String(maintActive ? "true" : "false") + ",";
            json += "\"maintenance_until_unix\":" + String(maintActive ? localMaintenanceUntilUnix : 0) + ",";
            json += "\"maintenance_message\":\"" + jsonEscape(maintActive ? localMaintenanceMessage : "") + "\",";
            json += "\"health_heap\":[";
            for (int i = 0; i < healthCount; i++) {
                int idx = (healthHead - healthCount + i + HEALTH_SAMPLES) % HEALTH_SAMPLES;
                if (i > 0) json += ",";
                json += String(healthRing[idx].free_heap);
            }
            json += "],\"health_rssi\":[";
            for (int i = 0; i < healthCount; i++) {
                int idx = (healthHead - healthCount + i + HEALTH_SAMPLES) % HEALTH_SAMPLES;
                if (i > 0) json += ",";
                json += String(healthRing[idx].rssi);
            }
            json += "]";
            json += "}";
            request->send(200, "application/json", json);
            return;
        }

        if (url == "/admin/reset" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            String target = request->hasParam("target", true) ? request->getParam("target", true)->value() : "";
            if (target == "visitors") {
                writeVisitorCount(0);
                cachedVisitorCount = 0;
                dailyVisitors = 0;
                saveDailyVisitors();
                request->send(200, "text/plain", "Visitor counts reset");
            } else if (target == "countries") {
                countryCount = 0;
                saveCountries();
                request->send(200, "text/plain", "Country tally reset");
            } else if (target == "periods") {
                resetPeriod(currentWeek);
                resetPeriod(currentMonth);
                resetPeriod(currentYear);
                saveCheckpoint();
                request->send(200, "text/plain", "Current period aggregates reset");
            } else if (target == "all") {
                writeVisitorCount(0);
                cachedVisitorCount = 0;
                dailyVisitors = 0;
                saveDailyVisitors();
                countryCount = 0;
                saveCountries();
                resetPeriod(currentWeek);
                resetPeriod(currentMonth);
                resetPeriod(currentYear);
                saveCheckpoint();
                request->send(200, "text/plain", "All counters reset");
            } else {
                request->send(400, "text/plain", "Unknown target (visitors|countries|periods|all)");
            }
            return;
        }

        if (url == "/admin/export" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            String json;
            json.reserve(1024);
            json  = "{";
            json += "\"exported_at\":\"" + getTimestamp() + "\",";
            json += "\"uptime\":\"" + uptime_formatter::getUptime() + "\",";
            json += "\"visitors_total\":" + String(cachedVisitorCount) + ",";
            json += "\"visitors_today\":" + String(dailyVisitors) + ",";
            json += "\"pending_guestbook\":" + String(pendingGuestbook) + ",";
            json += "\"worker_reconnects\":" + String(wsReconnectCount) + ",";
            json += "\"current_week\":\"" + String(lastWeekLabel) + "\",";
            json += "\"current_month\":\"" + String(lastMonthLabel) + "\",";
            json += "\"current_year\":\"" + String(lastYearLabel) + "\",";
            json += "\"week_samples\":" + String(currentWeek.samples) + ",";
            json += "\"month_samples\":" + String(currentMonth.samples) + ",";
            json += "\"year_samples\":" + String(currentYear.samples) + ",";
            json += "\"countries\":[";
            for (int i = 0; i < countryCount; i++) {
                if (i > 0) json += ",";
                json += "{\"code\":\"" + String(countries[i].code) + "\",\"count\":" + String(countries[i].count) + "}";
            }
            json += "]}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            r->addHeader("Content-Disposition", "attachment; filename=\"helloesp-state.json\"");
            request->send(r);
            return;
        }

        if (url == "/admin/selftest" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            String json;
            json.reserve(1024);
            json = "{\"at\":\"" + getTimestamp() + "\",\"tests\":[";
            int fails = 0, warns = 0;
            bool first = true; // must be request-scoped, not static; second run would emit a leading comma otherwise

            auto addTest = [&](const String& name, const String& status, const String& detail) {
                if (status == "fail") fails++;
                else if (status == "warn") warns++;
                if (!first) json += ",";
                first = false;
                json += "{\"name\":\"" + name + "\",\"status\":\"" + status + "\",\"detail\":\"" + jsonEscape(detail) + "\"}";
            };

            // 1. BME280
            float tf = safeBmeTemp() * 9.0f / 5.0f + 32.0f;
            float hm = safeBmeHumidity();
            float pr = safeBmePressureHpa();
            bool bmeOk = !isnan(tf) && !isnan(hm) && !isnan(pr) && !bmeDegraded();
            addTest("BME280", bmeOk ? "pass" : "fail",
                bmeOk ? (String(tf, 1) + "F, " + String(hm, 0) + "% RH, " + String(pr, 1) + " hPa")
                      : "sensor not responding or returning NaN");

            // 2. CCS811
            bool ccsOk = !ccsDegraded() && cached_co2 > 0;
            addTest("CCS811", ccsOk ? "pass" : "warn",
                ccsOk ? (String(cached_co2) + " ppm eCO2, " + String(cached_voc) + " ppb VOC")
                      : "sensor stale; CCS811 warms up over first 20 minutes");

            // 2b. OLED. Write-only display, so liveness is "did init succeed
            // at boot AND does the chip still ACK on the bus." Catches "wire
            // came loose" / "chip died" but NOT "displaying garbage" (which
            // requires eyeballs since SSD1306 has no readback path).
            if (!oledOk) {
                addTest("OLED", "fail", "init failed at boot; check wiring at 0x3C");
            } else {
                Wire.beginTransmission(0x3C);
                bool live = (Wire.endTransmission() == 0);
                addTest("OLED", live ? "pass" : "fail",
                    live ? "responding at 0x3C"
                         : "init ok at boot but no longer responding (wire loose? bus hang?)");
            }

            // 3. SD write/read cycle
            bool sdOk = false;
            String sdDetail = "SD test failed";
            {
                const char* testPath = "/_selftest.tmp";
                uint32_t token = (uint32_t)millis();
                File wf = SD.open(testPath, FILE_WRITE);
                if (wf) {
                    wf.println(token);
                    wf.close();
                    File rf = SD.open(testPath, FILE_READ);
                    if (rf) {
                        String line = rf.readStringUntil('\n');
                        line.trim();
                        rf.close();
                        // Compare as uint32_t. After ~24 days uptime millis()
                        // exceeds INT32_MAX; line.toInt() returns int32 and
                        // wraps, making the int-cast comparison spuriously
                        // fail. strtoul handles the full uint32_t range.
                        if ((uint32_t)strtoul(line.c_str(), nullptr, 10) == token) {
                            sdOk = true;
                            // Capacity reported in MB to avoid overflow on large cards (uint64 → uint32 cast)
                            uint32_t cardMb = (uint32_t)(SD.cardSize() / (1024ULL * 1024ULL));
                            const char* cardKind;
                            switch (SD.cardType()) {
                                case CARD_MMC:  cardKind = "MMC";   break;
                                case CARD_SD:   cardKind = "SDSC";  break;
                                case CARD_SDHC: cardKind = "SDHC";  break;
                                default:        cardKind = "?";     break;
                            }
                            sdDetail = "write+read+delete OK; " +
                                       String(sdSpeedHz / 1000000U) + " MHz SPI, " +
                                       String(cardMb) + " MB " + cardKind;
                        }
                        else sdDetail = "readback mismatch";
                    } else sdDetail = "read failed";
                    SD.remove(testPath);
                } else sdDetail = "write failed";
            }
            addTest("SD card", sdOk ? "pass" : "fail", sdDetail);

            // 4. Worker link
            if (strlen(cfgWorkerUrl) == 0) {
                addTest("Worker link", "warn", "not configured (LAN-only install)");
            } else if (wsConnected && wsClient.connected()) {
                unsigned long ageMs = lastWsActivity ? (millis() - lastWsActivity) : 0;
                addTest("Worker link", "pass", "connected, last activity " + String(ageMs / 1000) + "s ago");
            } else {
                addTest("Worker link", "fail", "not connected (reconnect in progress)");
            }

            // 4b. Shelly smart plug. Same pass/warn/fail bands as the admin
            // panel: fresh + zero failures = pass, fresh + failures = warn,
            // stale or never seen = fail.
            if (cfgShellyUrl[0] == '\0') {
                addTest("Shelly", "warn", "not configured");
            } else if (lastShellyOk == 0) {
                addTest("Shelly", "fail", "never reached (check shelly_url + LAN auth disabled)");
            } else {
                unsigned long ageMs = millis() - lastShellyOk;
                if (ageMs >= SHELLY_STALE_MS) {
                    addTest("Shelly", "fail",
                        "no successful poll in " + String(ageMs / 1000) + "s; " +
                        String(shellyConsecutiveFailures) + " consecutive failures");
                } else if (shellyConsecutiveFailures > 0) {
                    addTest("Shelly", "warn",
                        "recovering; " + String(shellyConsecutiveFailures) + " recent failures");
                } else {
                    String detail = "polling OK, " + String(ageMs / 1000) + "s since last read";
                    if (!isnan(cached_power_w)) {
                        detail += ", " + String((float)cached_power_w, 1) + " W";
                    }
                    addTest("Shelly", "pass", detail);
                }
            }

            // 5. NTP
            struct tm now;
            if (getLocalTime(&now, 100) && bootTime > 0) {
                addTest("NTP", "pass", "synced (" + getTimestamp() + ")");
            } else {
                addTest("NTP", "warn", "not yet synced; background daemon retrying");
            }

            // 5b. DS3231 RTC
            if (!rtcOk) {
                addTest("DS3231 RTC", "warn", "not detected at boot; running on NTP only");
            } else if (rtcLostPowerAtBoot) {
                // Latched at boot so this stays visible after NTP clears the live flag.
                // CR1220 needs replacement; until then NTP fills the gap each boot.
                DateTime r = rtc.now();
                float rtcTempC = rtc.getTemperature();
                String detail = "lostPower at boot (CR1220 likely depleted); " +
                                String(r.year()) + "-" +
                                (r.month() < 10 ? "0" : "") + String(r.month()) + "-" +
                                (r.day() < 10 ? "0" : "") + String(r.day()) + " " +
                                (r.hour() < 10 ? "0" : "") + String(r.hour()) + ":" +
                                (r.minute() < 10 ? "0" : "") + String(r.minute()) + " UTC, " +
                                String(rtcTempC, 1) + "C internal";
                addTest("DS3231 RTC", "warn", detail);
            } else {
                DateTime r = rtc.now();
                float rtcTempC = rtc.getTemperature();
                String detail = String(r.year()) + "-" +
                                (r.month() < 10 ? "0" : "") + String(r.month()) + "-" +
                                (r.day() < 10 ? "0" : "") + String(r.day()) + " " +
                                (r.hour() < 10 ? "0" : "") + String(r.hour()) + ":" +
                                (r.minute() < 10 ? "0" : "") + String(r.minute()) + ":" +
                                (r.second() < 10 ? "0" : "") + String(r.second()) + " UTC, " +
                                String(rtcTempC, 1) + "C internal";
                addTest("DS3231 RTC", "pass", detail);
            }

            // 6. Free heap
            uint32_t freeHeap = ESP.getFreeHeap();
            uint32_t minHeap = ESP.getMinFreeHeap();
            if (freeHeap >= 50000) {
                addTest("Free heap", "pass", String(freeHeap / 1024) + " KB free, " + String(minHeap / 1024) + " KB min ever");
            } else if (freeHeap >= 30000) {
                addTest("Free heap", "warn", String(freeHeap / 1024) + " KB free (close to 30 KB watchdog threshold)");
            } else {
                addTest("Free heap", "fail", String(freeHeap / 1024) + " KB free; heap watchdog will reboot soon");
            }

            // 7. WiFi signal
            int rssi = WiFi.RSSI();
            if (rssi > -60)      addTest("WiFi signal", "pass", String(rssi) + " dBm (excellent)");
            else if (rssi > -75) addTest("WiFi signal", "pass", String(rssi) + " dBm (good)");
            else if (rssi > -85) addTest("WiFi signal", "warn", String(rssi) + " dBm (weak)");
            else                 addTest("WiFi signal", "fail", String(rssi) + " dBm (very weak; move closer to router)");

            // R2 backup recency. We only know what the Worker told us via backup_committed.
            // No commit yet = warn (not fail) since a fresh device legitimately has never backed up.
            if (strlen(cfgWorkerUrl) == 0) {
                addTest("R2 backup", "warn", "Worker not configured; backups require the relay");
            } else if (r2CommittedAtUnix == 0) {
                addTest("R2 backup", "warn", "no commit confirmation yet (trigger one from the backup panel)");
            } else {
                time_t nowSec = time(nullptr);
                long ageHours = (long)((nowSec - (time_t)r2CommittedAtUnix) / 3600);
                String detail = "last commit " + String(r2CommittedDate) + " (" + String(ageHours) + "h ago, " + String(r2CommittedFiles) + " files)";
                if (ageHours <= 25)      addTest("R2 backup", "pass", detail);
                else if (ageHours <= 72) addTest("R2 backup", "warn", detail + " - overdue");
                else                     addTest("R2 backup", "fail", detail + " - very overdue");
            }

            json += "],\"fails\":" + String(fails) + ",\"warns\":" + String(warns);
            json += ",\"overall\":\"" + String(fails > 0 ? "fail" : (warns > 0 ? "warn" : "pass")) + "\"}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/i2cscan" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            auto nameForAddr = [](uint8_t a) -> const char* {
                switch (a) {
                    case 0x3C: return "OLED (SSD1306)";
                    case 0x3D: return "OLED (SSD1306, alt)";
                    case 0x40: return "INA219";
                    case 0x57: return "AT24C32 EEPROM (often bundled with DS3231)";
                    case 0x5A: return "CCS811";
                    case 0x5B: return "CCS811 (alt)";
                    case 0x68: return "DS3231 RTC";
                    case 0x76: return "BME280";
                    case 0x77: return "BME280 (alt) / BMP180";
                    default:   return "unknown";
                }
            };
            String json;
            json.reserve(512);
            json = "{\"at\":\"" + getTimestamp() + "\",\"devices\":[";
            int count = 0;
            bool first = true;
            for (uint8_t addr = 0x08; addr <= 0x77; addr++) {
                Wire.beginTransmission(addr);
                if (Wire.endTransmission() == 0) {
                    if (!first) json += ",";
                    first = false;
                    char buf[8];
                    snprintf(buf, sizeof(buf), "0x%02X", addr);
                    json += "{\"addr\":\"";
                    json += buf;
                    json += "\",\"name\":\"";
                    json += nameForAddr(addr);
                    json += "\"}";
                    count++;
                }
            }
            json += "],\"count\":" + String(count) + "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/sensor-health") {
            if (!adminAuth(request)) return;
            // POST ?clear=bme280|ccs811 un-retires that sensor in place.
            // Both methods return the (possibly-updated) state JSON.
            if (request->method() == HTTP_POST && request->hasParam("clear")) {
                String s = request->getParam("clear")->value();
                SensorHealth* h = (s == "bme280") ? &bmeHealth
                                : (s == "ccs811") ? &ccsHealth : nullptr;
                if (h && h->retired) {
                    h->retired = false; h->retired_unix = 0; h->consecutive_bad = 0;
                    saveSensorHealth();
                    logError("sensor", (s + " un-retired by admin").c_str());
                }
            }
            String json;
            json.reserve(256);
            auto emit = [&](const char* name, const SensorHealth& h, bool degradedNow) {
                json += "\""; json += name; json += "\":{";
                json += "\"retired\":";       json += h.retired ? "true" : "false";
                json += ",\"retired_unix\":"; json += String(h.retired_unix);
                json += ",\"consecutive_bad\":"; json += String(h.consecutive_bad);
                json += ",\"degraded_now\":"; json += degradedNow ? "true" : "false";
                json += "}";
            };
            json  = "{";
            emit("bme280", bmeHealth, bmeDegraded()); json += ",";
            emit("ccs811", ccsHealth, ccsDegraded());
            json += ",\"retire_threshold\":" + String(SENSOR_RETIRE_THRESHOLD) + "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/repair-periods" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            struct tm now;
            if (!getLocalTime(&now, 100)) {
                request->send(503, "application/json",
                    "{\"ok\":false,\"error\":\"no time available; wait for NTP and retry\"}");
                return;
            }
            int curYear  = now.tm_year + 1900;
            int curMonth = now.tm_mon + 1;
            char curYearStr[5];
            snprintf(curYearStr, sizeof(curYearStr), "%04d", curYear);

            // Snapshot the in-progress week so we can re-add it after the reset.
            PeriodStats inProgress = currentWeek;

            // Reset month/year accumulators (preserve started_unix so the
            // /history "Started: ..." text doesn't go to 1970).
            uint32_t monthStarted = currentMonth.started_unix;
            uint32_t yearStarted  = currentYear.started_unix;
            resetPeriod(currentMonth);
            resetPeriod(currentYear);
            if (monthStarted > 0) currentMonth.started_unix = monthStarted;
            if (yearStarted  > 0) currentYear.started_unix  = yearStarted;

            int weeksYear = 0, weeksMonth = 0;
            // Helper: aggregate one weekly archive file (full path) into the
            // year and (if applicable) month accumulators. Skips files whose
            // year prefix doesn't match curYearStr.
            auto aggregateWeeklyFile = [&](const String& fullPath, const String& fname) {
                if (!fname.endsWith(".json")) return;
                if (!fname.startsWith(curYearStr)) return;
                File f = SD.open(fullPath);
                if (!f) return;
                String content;
                content.reserve(f.size() + 1);
                while (f.available()) content += (char)f.read();
                f.close();
                yield();
                esp_task_wdt_reset();

                PeriodStats wk;
                time_t wkStarted = 0;
                if (!parseWeeklyJson(content, wk, wkStarted)) return;

                aggregatePeriodInto(currentYear, wk);
                weeksYear++;

                // Month determination via ISO week math (don't trust started_unix
                // from the JSON; archives written before NTP synced will have 0).
                int dashW = fname.indexOf("-W");
                if (dashW > 0 && (int)fname.length() > dashW + 2) {
                    int wkYear = fname.substring(0, dashW).toInt();
                    int wkNum  = fname.substring(dashW + 2, fname.length() - 5).toInt();
                    int wkMonth = 0;
                    if (wkYear == curYear && isoWeekMonth(wkYear, wkNum, wkMonth)
                        && wkMonth == curMonth) {
                        aggregatePeriodInto(currentMonth, wk);
                        weeksMonth++;
                    }
                }
            };

            // Canonical layout: /stats/weekly/<curYearStr>/<label>.json. Walk
            // the year subdir directly so we don't iterate other-year dirs.
            String yearDirPath = "/stats/weekly/" + String(curYearStr);
            if (SD.exists(yearDirPath)) {
                File yearDir = SD.open(yearDirPath);
                if (yearDir && yearDir.isDirectory()) {
                    while (true) {
                        File f = yearDir.openNextFile();
                        if (!f) break;
                        String name = String(f.name());
                        int lastSlash = name.lastIndexOf('/');
                        String base = (lastSlash >= 0) ? name.substring(lastSlash + 1) : name;
                        f.close();
                        aggregateWeeklyFile(yearDirPath + "/" + base, base);
                    }
                }
                if (yearDir) yearDir.close();
            }
            // Legacy stragglers at the flat root, in case migration didn't
            // cover everything (cap miss, partial run, etc).
            File dir = SD.open("/stats/weekly");
            if (dir) {
                if (dir.isDirectory()) {
                    while (true) {
                        File f = dir.openNextFile();
                        if (!f) break;
                        String name = String(f.name());
                        int lastSlash = name.lastIndexOf('/');
                        String base = (lastSlash >= 0) ? name.substring(lastSlash + 1) : name;
                        bool isDir = f.isDirectory();
                        f.close();
                        if (isDir) continue;  // year subdirs handled above
                        aggregateWeeklyFile(String("/stats/weekly/") + base, base);
                    }
                }
                dir.close();
            }

            // Re-add the in-progress week (currentWeek): it isn't archived yet
            // and would otherwise be missing from the rebuilt month/year totals.
            aggregatePeriodInto(currentMonth, inProgress);
            aggregatePeriodInto(currentYear,  inProgress);

            saveCheckpoint();
            logError("admin",
                ("repaired periods from weekly archives (year=" + String(weeksYear) +
                 " weeks, month=" + String(weeksMonth) + " weeks)").c_str());
            String resp = "{\"ok\":true,\"weeks_year\":" + String(weeksYear) +
                          ",\"weeks_month\":" + String(weeksMonth) +
                          ",\"month_visitors\":" + String(currentMonth.visitors) +
                          ",\"year_visitors\":" + String(currentYear.visitors) + "}";
            request->send(200, "application/json", resp);
            return;
        }

        if (url == "/admin/sanitize-csvs" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            // CSV column layout (must match logStats header):
            //   0:timestamp 1:cpu_temp_c 2:cpu_temp_f 3:memory_percent
            //   4:temperature_c 5:temperature_f 6:humidity_percent 7:pressure_hpa
            //   8:altitude_ft 9:co2_ppm 10:voc_ppb 11:heat_index_c 12:heat_index_f
            //   13:rssi 14:sd_free_mb 15:requests_interval
            // Drop any row where the BME280-derived columns (4, 6, 7) are out of spec.
            // CCS811 columns (9, 10) aren't bounded here because their datasheet
            // upper bound (32k+ ppm) is loose enough that a value being "high"
            // is rarely a clear glitch indicator on its own.
            // Two-mode endpoint to keep each request short enough to survive any
            // 30s timeout in the chain (CF Worker subrequest, idle TCP, etc.):
            //
            //   POST /admin/sanitize-csvs           → list mode: returns the
            //       set of CSV file paths that need scanning. Client iterates.
            //   POST /admin/sanitize-csvs?file=PATH → process one file in
            //       isolation. Returns rows_scanned / rows_dropped for it.
            //
            // The client (admin.html) calls list mode first, then loops the
            // per-file mode for each path, accumulating totals locally.
            const String logsPath = "/logs";

            if (request->hasParam("file", true)) {
                String csvPath = request->getParam("file", true)->value();
                // Path safety: only allow paths under /logs/ with a .csv suffix
                // and no path-traversal sequences.
                if (!csvPath.startsWith("/logs/") || csvPath.indexOf("..") >= 0
                    || !csvPath.endsWith(".csv")) {
                    request->send(400, "application/json",
                        "{\"ok\":false,\"error\":\"invalid file path\"}");
                    return;
                }
                if (!SD.exists(csvPath)) {
                    request->send(404, "application/json",
                        "{\"ok\":false,\"error\":\"file not found\"}");
                    return;
                }
                String tmpPath = csvPath + ".tmp";
                String bakPath = csvPath + ".bak";
                File rf = SD.open(csvPath, FILE_READ);
                if (!rf) {
                    request->send(500, "application/json",
                        "{\"ok\":false,\"error\":\"open read failed\"}");
                    return;
                }
                File wf = SD.open(tmpPath, FILE_WRITE);
                if (!wf) {
                    rf.close();
                    request->send(500, "application/json",
                        "{\"ok\":false,\"error\":\"open tmp failed\"}");
                    return;
                }
                int rowsScanned = 0, rowsDropped = 0;
                bool isHeader = true;
                while (rf.available()) {
                    String line = rf.readStringUntil('\n');
                    line.trim();
                    if (line.length() == 0) continue;
                    if (isHeader) { wf.println(line); isHeader = false; continue; }
                    int col = 0, start = 0;
                    bool dropRow = false;
                    for (int i = 0; i <= (int)line.length(); i++) {
                        if (i == (int)line.length() || line[i] == ',') {
                            if (col == 4) {
                                float v = line.substring(start, i).toFloat();
                                if (isnan(v) || v < BME_TEMP_MIN_C || v > BME_TEMP_MAX_C) dropRow = true;
                            } else if (col == 6) {
                                float v = line.substring(start, i).toFloat();
                                if (isnan(v) || v < BME_HUM_MIN || v > BME_HUM_MAX) dropRow = true;
                            } else if (col == 7) {
                                float v = line.substring(start, i).toFloat();
                                if (isnan(v) || v < BME_PRESS_MIN_HPA || v > BME_PRESS_MAX_HPA) dropRow = true;
                            }
                            start = i + 1;
                            col++;
                            if (col > 7 || dropRow) break;
                        }
                    }
                    rowsScanned++;
                    if (dropRow) rowsDropped++;
                    else wf.println(line);
                    // Long synchronous SD loops can starve the async_tcp task
                    // watchdog (5s) even with yield(). yield()/delay(0) only
                    // reschedules; it doesn't feed the WDT. Explicit reset
                    // here keeps a large CSV from tripping task_wdt mid-scan.
                    if ((rowsScanned & 0x3F) == 0) {
                        yield();
                        esp_task_wdt_reset();
                    }
                }
                rf.close();
                wf.close();
                if (rowsDropped > 0) {
                    if (SD.exists(bakPath)) SD.remove(bakPath);
                    SD.rename(csvPath, bakPath);
                    if (!SD.rename(tmpPath, csvPath)) {
                        if (SD.exists(bakPath)) SD.rename(bakPath, csvPath);
                        request->send(500, "application/json",
                            "{\"ok\":false,\"error\":\"rename failed; original restored\"}");
                        return;
                    }
                    if (SD.exists(bakPath)) SD.remove(bakPath);
                    logError("admin",
                        ("sanitize-csvs: dropped " + String(rowsDropped) +
                         " row(s) from " + csvPath).c_str());
                } else {
                    SD.remove(tmpPath);
                }
                String resp = "{\"ok\":true,\"file\":\"" + jsonEscape(csvPath) +
                              "\",\"rows_scanned\":" + String(rowsScanned) +
                              ",\"rows_dropped\":" + String(rowsDropped) + "}";
                request->send(200, "application/json", resp);
                return;
            }

            // List mode: enumerate /logs/YYYY/*.csv and return paths.
            String list = "{\"ok\":true,\"files\":[";
            int count = 0;
            File logsRoot = SD.open(logsPath);
            if (logsRoot) {
                while (true) {
                    File yearDir = logsRoot.openNextFile();
                    if (!yearDir) break;
                    if (!yearDir.isDirectory()) { yearDir.close(); continue; }
                    String yearName = String(yearDir.name());
                    int yLast = yearName.lastIndexOf('/');
                    if (yLast >= 0) yearName = yearName.substring(yLast + 1);
                    yearDir.close();
                    String yearPath = logsPath + "/" + yearName;

                    File yd = SD.open(yearPath);
                    if (!yd) continue;
                    while (true) {
                        File csv = yd.openNextFile();
                        if (!csv) break;
                        String csvName = String(csv.name());
                        csv.close();
                        int lastSlash = csvName.lastIndexOf('/');
                        String bareName = (lastSlash >= 0) ? csvName.substring(lastSlash + 1) : csvName;
                        if (!bareName.endsWith(".csv")) continue;
                        if (count > 0) list += ",";
                        list += "\"";
                        list += yearPath + "/" + bareName;
                        list += "\"";
                        count++;
                    }
                    yd.close();
                }
                logsRoot.close();
            }
            list += "],\"count\":" + String(count) + "}";
            request->send(200, "application/json", list);
            return;
        }

        if (url == "/admin/fix-record-timestamps" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            const char* path = "/stats/records.json";
            const char* tmp  = "/stats/records.tmp";
            const char* bak  = "/stats/records.bak";
            File rf = SD.open(path, FILE_READ);
            if (!rf) {
                request->send(404, "application/json",
                    "{\"ok\":false,\"error\":\"records.json not found\"}");
                return;
            }
            String content;
            content.reserve(rf.size() + 32);
            while (rf.available()) content += (char)rf.read();
            rf.close();
            // records.json is normally <1 KB, but yield once after the read
            // so this admin handler doesn't starve async_tcp on a degenerate
            // file that grew unexpectedly.
            yield();
            esp_task_wdt_reset();

            // Scan for truncated ISO 8601 timezone offsets. Pattern is exactly
            // ":DD[+-]DDD\"" where DD is digits (the seconds field) and DDD is
            // a 3-digit offset (truncated from 4). Pad with a trailing '0'
            // before the closing quote. Works for whole-hour TZs (US Mountain,
            // Pacific, Central, Eastern, etc); India/Newfoundland's :30/:45
            // offsets would need different padding but those cases are rare
            // and would have failed differently anyway.
            String fixed;
            fixed.reserve(content.length() + 32);
            int fixes = 0;
            int i = 0;
            while (i < (int)content.length()) {
                bool isTruncatedOffset =
                    (i + 4 < (int)content.length())
                    && (content[i] == '+' || content[i] == '-')
                    && isdigit((unsigned char)content[i+1])
                    && isdigit((unsigned char)content[i+2])
                    && isdigit((unsigned char)content[i+3])
                    && content[i+4] == '"'
                    && (i >= 3)
                    && content[i-3] == ':'
                    && isdigit((unsigned char)content[i-2])
                    && isdigit((unsigned char)content[i-1]);
                if (isTruncatedOffset) {
                    fixed += content.substring(i, i + 4);
                    fixed += '0';
                    fixed += '"';
                    i += 5;
                    fixes++;
                } else {
                    fixed += content[i];
                    i++;
                }
            }

            if (fixes == 0) {
                request->send(200, "application/json",
                    "{\"ok\":true,\"fixes\":0,\"note\":\"no truncated timestamps found\"}");
                return;
            }

            // Atomic write: tmp → bak rotate → rename. Same pattern as saveRecords.
            File wf = SD.open(tmp, FILE_WRITE);
            if (!wf) {
                request->send(500, "application/json",
                    "{\"ok\":false,\"error\":\"failed to open tmp file\"}");
                return;
            }
            wf.print(fixed);
            wf.close();
            if (SD.exists(bak)) SD.remove(bak);
            if (SD.exists(path)) SD.rename(path, bak);
            if (!SD.rename(tmp, path)) {
                if (SD.exists(bak)) SD.rename(bak, path);
                request->send(500, "application/json",
                    "{\"ok\":false,\"error\":\"rename failed; original restored\"}");
                return;
            }
            if (SD.exists(bak)) SD.remove(bak);

            // Reload so in-memory record state matches the file on disk.
            loadRecords();

            String resp = "{\"ok\":true,\"fixes\":" + String(fixes) + "}";
            request->send(200, "application/json", resp);
            return;
        }

        if (url == "/admin/errors.log" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            if (!SD.exists(ERROR_LOG_PATH)) {
                request->send(200, "text/plain", "");
                return;
            }
            AsyncWebServerResponse *r = request->beginResponse(SD, ERROR_LOG_PATH, "text/plain");
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/errors.log.old" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            if (!SD.exists(ERROR_LOG_BAK)) {
                request->send(404, "text/plain", "No rotated log");
                return;
            }
            AsyncWebServerResponse *r = request->beginResponse(SD, ERROR_LOG_BAK, "text/plain");
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/errors/clear" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            bool had = SD.exists(ERROR_LOG_PATH) || SD.exists(ERROR_LOG_BAK);
            if (SD.exists(ERROR_LOG_PATH)) SD.remove(ERROR_LOG_PATH);
            if (SD.exists(ERROR_LOG_BAK))  SD.remove(ERROR_LOG_BAK);
            request->send(200, "text/plain", had ? "Error logs cleared" : "Already empty");
            return;
        }

        if (url == "/admin/backup-now" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!wsConnected || !wsClient.connected()) {
                request->send(503, "text/plain", "Worker link offline");
                return;
            }
            pendingBackupFlag = true;
            request->send(200, "text/plain", "Queued - uploading full SD to R2 (falls back to email if R2 unbound)");
            return;
        }

        if (url == "/admin/test-email" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!wsConnected || !wsClient.connected()) {
                request->send(503, "text/plain", "Worker link offline");
                return;
            }
            pendingTestEmailFlag = true;
            request->send(200, "text/plain", "Triggered - poll /admin/test-email-result for outcome");
            return;
        }

        if (url == "/admin/test-email-result" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            time_t now = time(nullptr);
            long ageSecs = testEmailAtUnix ? (long)(now - (time_t)testEmailAtUnix) : -1;
            String json = "{";
            json += "\"at_unix\":" + String(testEmailAtUnix) + ",";
            json += "\"age_s\":" + String(ageSecs) + ",";
            json += "\"pass\":" + String(testEmailPass ? "true" : "false") + ",";
            json += "\"detail\":\"" + jsonEscape(testEmailDetail) + "\"";
            json += "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/snake-clear" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!wsConnected || !wsClient.connected()) {
                request->send(503, "text/plain", "Worker link offline");
                return;
            }
            pendingSnakeClearFlag = true;
            request->send(200, "text/plain", "Triggered - poll /admin/snake-clear-result for outcome");
            return;
        }

        if (url == "/admin/snake-clear-result" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            time_t now = time(nullptr);
            long ageSecs = snakeClearAtUnix ? (long)(now - (time_t)snakeClearAtUnix) : -1;
            String json = "{";
            json += "\"at_unix\":" + String(snakeClearAtUnix) + ",";
            json += "\"age_s\":" + String(ageSecs) + ",";
            json += "\"ok\":" + String(snakeClearOk ? "true" : "false");
            json += "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/chronicle/note" && request->method() == HTTP_POST) {
            // Owner curatorial note for a Chronicle entry. Body is form-
            // encoded with `date=YYYY-MM-DD&note=...`. The chip just
            // validates and forwards via the authenticated WS event channel
            // to the worker DO, which holds the actual entry storage.
            // Empty note clears any existing note for that date.
            if (!adminAuth(request)) return;
            if (!request->hasParam("date", true)) {
                request->send(400, "text/plain", "date required");
                return;
            }
            String date = request->getParam("date", true)->value();
            // Strict YYYY-MM-DD validation. Length+dash-position alone would
            // accept things like `1234-56-7"` which would break the JSON
            // sent to the worker. Require digits in the other 8 positions.
            bool dateOk = date.length() == 10
                       && date.charAt(4) == '-' && date.charAt(7) == '-';
            for (int i = 0; dateOk && i < 10; i++) {
                if (i == 4 || i == 7) continue;
                char c = date.charAt(i);
                if (c < '0' || c > '9') dateOk = false;
            }
            if (!dateOk) {
                request->send(400, "text/plain", "date must be YYYY-MM-DD");
                return;
            }
            String note = "";
            if (request->hasParam("note", true)) {
                note = request->getParam("note", true)->value();
                if (note.length() > 1000) note = note.substring(0, 1000);
            }
            if (!wsConnected || !wsClient.connected()) {
                request->send(503, "text/plain", "Worker link offline");
                return;
            }
            String msg = "{\"type\":\"event\",\"event\":\"chronicle_note_set\",\"data\":{";
            msg += "\"date\":\"" + date + "\",";
            msg += "\"note\":\"" + jsonEscape(note) + "\"";
            msg += "}}";
            wsSendText(wsClient, msg);
            request->send(200, "text/plain", "ok");
            return;
        }

        if (url == "/admin/r2-healthcheck" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!wsConnected || !wsClient.connected()) {
                request->send(503, "text/plain", "Worker link offline");
                return;
            }
            pendingR2HealthcheckFlag = true;
            request->send(200, "text/plain", "Triggered - poll /admin/r2-healthcheck-result for outcome");
            return;
        }

        if (url == "/admin/r2-healthcheck-result" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            time_t now = time(nullptr);
            long ageSecs = r2HealthcheckAtUnix ? (long)(now - (time_t)r2HealthcheckAtUnix) : -1;
            String json = "{";
            json += "\"at_unix\":" + String(r2HealthcheckAtUnix) + ",";
            json += "\"age_s\":" + String(ageSecs) + ",";
            json += "\"pass\":" + String(r2HealthcheckPass ? "true" : "false") + ",";
            json += "\"detail\":\"" + jsonEscape(r2HealthcheckDetail) + "\"";
            json += "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/backup-status" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            time_t now = time(nullptr);
            long committedAgo = r2CommittedAtUnix ? (long)(now - (time_t)r2CommittedAtUnix) : -1;
            long sentAgo = lastBackupSentMs ? (long)((millis() - lastBackupSentMs) / 1000UL) : -1;
            String json = "{";
            json += "\"last_sent_date\":\"";  json += lastBackupDate;    json += "\",";
            json += "\"last_sent_ago_s\":";   json += String(sentAgo);   json += ",";
            json += "\"r2_committed_date\":\""; json += r2CommittedDate; json += "\",";
            json += "\"r2_committed_ago_s\":" + String(committedAgo) + ",";
            json += "\"r2_committed_bytes\":" + String(r2CommittedBytes) + ",";
            json += "\"r2_committed_files\":" + String(r2CommittedFiles) + ",";
            json += "\"worker_linked\":" + String(wsConnected && wsClient.connected() ? "true" : "false");
            json += "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
            r->addHeader("Cache-Control", "no-store");
            request->send(r);
            return;
        }

        if (url == "/admin/delete" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!request->hasParam("path", true)) {
                request->send(400, "text/plain", "Missing path");
                return;
            }
            if (!safePath(request->getParam("path", true)->value())) {
                request->send(400, "text/plain", "Invalid path");
                return;
            }
            String path = request->getParam("path", true)->value();
            if (isProtectedPath(path)) {
                request->send(403, "text/plain",
                    "Refusing to delete protected file. Upload a replacement instead.");
                return;
            }
            if (!SD.exists(path)) {
                request->send(404, "text/plain", "Not found");
                return;
            }
            File f = SD.open(path);
            if (!f) {
                request->send(500, "text/plain", "Unable to open path");
                return;
            }
            bool isDir = f.isDirectory();
            f.close();
            bool ok = isDir ? SD.rmdir(path) : SD.remove(path);
            if (!ok) {
                request->send(500, "text/plain", isDir ? "rmdir failed (directory not empty?)" : "remove failed");
                return;
            }
            request->send(200, "text/plain", "Deleted");
            return;
        }

        if (url == "/admin/mkdir" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!request->hasParam("path", true)) {
                request->send(400, "text/plain", "Missing path");
                return;
            }
            if (!safePath(request->getParam("path", true)->value())) {
                request->send(400, "text/plain", "Invalid path");
                return;
            }
            String path = request->getParam("path", true)->value();
            if (SD.mkdir(path)) {
                request->send(200, "text/plain", "Created");
            } else {
                request->send(500, "text/plain", "Failed to create directory");
            }
            return;
        }

        if (url == "/admin/download") {
            if (!adminAuth(request)) return;
            if (!request->hasParam("path")) {
                request->send(400, "text/plain", "Missing path");
                return;
            }
            if (!safePath(request->getParam("path")->value())) {
                request->send(400, "text/plain", "Invalid path");
                return;
            }
            String path = request->getParam("path")->value();
            if (SD.exists(path)) {
                String filename = path.substring(path.lastIndexOf('/') + 1);
                String safeName;
                safeName.reserve(filename.length());
                for (size_t i = 0; i < filename.length(); i++) {
                    char c = filename[i];
                    if ((unsigned char)c < 0x20 || c == 0x7F) continue;
                    if (c == '"' || c == '\\') continue;
                    safeName += c;
                }
                if (safeName.length() == 0) safeName = "download";
                AsyncWebServerResponse *response = request->beginResponse(SD, path, "application/octet-stream");
                response->addHeader("Content-Disposition", "attachment; filename=\"" + safeName + "\"");
                request->send(response);
            } else {
                request->send(404, "text/plain", "File not found");
            }
            return;
        }

        if (url == "/admin/reboot" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            request->send(200, "text/plain", "Rebooting...");
            delay(500);
            cleanRestart();
            return;
        }

        // List firmware .bin files in /fw/. Used by the admin UI to populate
        // the "Flash from SD" picker. Returns name, size, mtime per file.
        if (url == "/admin/firmware-list" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            String json = "{\"canRollBack\":";
            json += Update.canRollBack() ? "true" : "false";
            json += ",\"files\":[";
            File dir = SD.open("/fw");
            bool first = true;
            if (dir && dir.isDirectory()) {
                File f = dir.openNextFile();
                while (f) {
                    String name = f.name();
                    // openNextFile returns full path on some library
                    // versions and bare name on others; normalize.
                    int lastSlash = name.lastIndexOf('/');
                    String bare = lastSlash >= 0 ? name.substring(lastSlash + 1) : name;
                    if (!f.isDirectory() && bare.endsWith(".bin")) {
                        if (!first) json += ",";
                        json += "{\"name\":\"" + jsonEscape(bare) + "\",";
                        json += "\"size\":" + String(f.size()) + ",";
                        json += "\"mtime\":" + String((uint32_t)f.getLastWrite()) + "}";
                        first = false;
                    }
                    f.close();
                    f = dir.openNextFile();
                }
                dir.close();
            }
            json += "]}";
            request->send(200, "application/json", json);
            return;
        }

        // Schedule a flash from an SD-stored firmware .bin. The actual flash
        // happens in the main loop so we can respond quickly (the flash
        // takes 30-60s and we don't want to hold the HTTP connection).
        // Path must be under /fw/ and end in .bin (basic validation; the
        // handler also null-checks the file exists before scheduling).
        if (url == "/admin/flash-from-sd" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (pendingSDFlash || pendingRollback) {
                request->send(409, "text/plain", "Another flash or rollback is already scheduled");
                return;
            }
            if (!request->hasParam("path", true)) {
                request->send(400, "text/plain", "Missing path");
                return;
            }
            String path = request->getParam("path", true)->value();
            if (!path.startsWith("/fw/") || !path.endsWith(".bin") ||
                path.indexOf("..") >= 0 || path.length() >= sizeof(pendingSDFlashPath)) {
                request->send(400, "text/plain", "Invalid path (must be /fw/<name>.bin)");
                return;
            }
            if (!SD.exists(path)) {
                request->send(404, "text/plain", "Firmware file not found on SD");
                return;
            }
            File f = SD.open(path, FILE_READ);
            if (!f) {
                request->send(500, "text/plain", "Could not open firmware file");
                return;
            }
            size_t sz = f.size();
            f.close();
            if (sz < 64 * 1024 || sz > 4 * 1024 * 1024) {
                // ESP32 firmwares typically 500KB-2MB. Reject absurd sizes.
                request->send(400, "text/plain", "File size outside expected firmware range (64KB - 4MB)");
                return;
            }
            strncpy(pendingSDFlashPath, path.c_str(), sizeof(pendingSDFlashPath) - 1);
            pendingSDFlashPath[sizeof(pendingSDFlashPath) - 1] = '\0';
            pendingSDFlash = true;
            request->send(200, "text/plain", "Flash scheduled. Device will reboot on success.");
            logConsole(request, 200);
            return;
        }

        // Schedule SHA256 computation for a firmware .bin on SD. Returns
        // immediately; result polled via /admin/firmware-sha256-result.
        // Reading 1.5MB off SD takes ~6s which is too close to the 5s
        // async_tcp watchdog for a sync handler, so compute runs on main
        // loop like the flash path.
        if (url == "/admin/firmware-sha256" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (sha256Computing || pendingSha256) {
                request->send(409, "text/plain", "Another checksum is already in progress");
                return;
            }
            if (!request->hasParam("path", true)) {
                request->send(400, "text/plain", "Missing path");
                return;
            }
            String path = request->getParam("path", true)->value();
            if (!path.startsWith("/fw/") || !path.endsWith(".bin") ||
                path.indexOf("..") >= 0 || path.length() >= sizeof(pendingSha256Path)) {
                request->send(400, "text/plain", "Invalid path");
                return;
            }
            if (!SD.exists(path)) {
                request->send(404, "text/plain", "File not found");
                return;
            }
            strncpy(pendingSha256Path, path.c_str(), sizeof(pendingSha256Path) - 1);
            pendingSha256Path[sizeof(pendingSha256Path) - 1] = '\0';
            sha256Result[0] = '\0';
            sha256ResultPath[0] = '\0';
            pendingSha256 = true;
            request->send(202, "text/plain", "Computing...");
            return;
        }

        // Poll the result of the most-recent SHA256 computation. Returns
        // 200 + hex if the last completed result matches `path`, 202
        // "computing" if still working on `path`, 404 if no result yet
        // for `path`.
        if (url == "/admin/firmware-sha256-result" && request->method() == HTTP_GET) {
            if (!adminAuth(request)) return;
            if (!request->hasParam("path")) {
                request->send(400, "text/plain", "Missing path");
                return;
            }
            String path = request->getParam("path")->value();
            if (sha256Computing && strcmp(pendingSha256Path, path.c_str()) == 0) {
                request->send(202, "text/plain", "Computing...");
                return;
            }
            if (sha256Result[0] != '\0' && strcmp(sha256ResultPath, path.c_str()) == 0) {
                request->send(200, "text/plain", sha256Result);
                return;
            }
            request->send(404, "text/plain", "No checksum yet for this path");
            return;
        }

        // Flip the active OTA partition back to the previously-active one.
        // No state tracking, no watchdog; the user made the decision by
        // clicking the button. Deferred to main loop for consistency with
        // the flash path (and to respond quickly).
        if (url == "/admin/rollback" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (pendingSDFlash || pendingRollback) {
                request->send(409, "text/plain", "Another flash or rollback is already scheduled");
                return;
            }
            if (!Update.canRollBack()) {
                request->send(400, "text/plain", "No previous firmware available to roll back to");
                return;
            }
            pendingRollback = true;
            request->send(200, "text/plain", "Rollback scheduled. Device will reboot.");
            logConsole(request, 200);
            return;
        }

        if (url == "/admin/maintenance" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (strlen(cfgWorkerUrl) == 0) {
                request->send(503, "text/plain", "Worker not configured; maintenance mode is a Cloudflare feature.");
                return;
            }
            if (!wsConnected || !wsClient.connected()) {
                request->send(503, "text/plain", "Worker link is down. Try again in a few seconds.");
                return;
            }
            if (!request->hasParam("minutes", true)) {
                request->send(400, "text/plain", "Missing minutes");
                return;
            }
            int mins = request->getParam("minutes", true)->value().toInt();
            if (mins < 0) mins = 0;
            if (mins > 120) mins = 120;
            String msg = request->hasParam("message", true) ? request->getParam("message", true)->value() : "";
            if (msg.length() > 200) msg = msg.substring(0, 200);
            pendingMaintenanceMinutes = mins;
            strncpy(pendingMaintenanceMessage, msg.c_str(), sizeof(pendingMaintenanceMessage) - 1);
            pendingMaintenanceMessage[sizeof(pendingMaintenanceMessage) - 1] = '\0';
            // Pair with the acquire-fence on the loop reader so the buffer
            // writes above are guaranteed visible before the flag is observed.
            std::atomic_thread_fence(std::memory_order_release);
            pendingMaintenanceFlag = true;
            if (mins == 0) {
                request->send(200, "text/plain", "Maintenance cancelled");
            } else {
                String reply = "Maintenance enabled for " + String(mins) + " min";
                request->send(200, "text/plain", reply);
            }
            return;
        }

        AsyncWebServerResponse *r = beginResponseGzipOrRaw(request, "/404.html", "text/html");
        r->setCode(404);
        request->send(r);
        logConsole(request, 404);
    });

    server.begin();
    Serial.println("Server listening");
    bootLog("[srv] listening :80");

    if (strlen(cfgWorkerUrl) > 0) {
        bootLog("[ws] connecting...");
        if (connectWorker()) {
            bootLog("[ws] connected");
        } else {
            bootLog("[ws] failed");
        }
    }

    delay(5000);
    lastPageSwitch = millis();
}

// If both I2C sensors have been stale past the threshold the bus is probably
// wedged (slave holding SDA low, controller timed out mid-transaction, etc).
// End Wire, clock SCL 9 times by hand so any stuck slave releases the line,
// then restart Wire and re-init the sensors. Bounded by a cooldown so we
// don't thrash if recovery doesn't actually help.
static void tryI2cRecovery() {
    static unsigned long lastResetAt = 0;
    unsigned long now = millis();
    if (lastResetAt && now - lastResetAt < 120000UL) return;
    lastResetAt = now;

    Serial.println("[i2c] both sensors stale, resetting bus");
    logError("i2c", "bus reset: both BME280 and CCS811 stale >2min");

    Wire.end();
    pinMode(SCL, OUTPUT);
    pinMode(SDA, INPUT_PULLUP);
    for (int i = 0; i < 9; i++) {
        digitalWrite(SCL, HIGH); delayMicroseconds(5);
        digitalWrite(SCL, LOW);  delayMicroseconds(5);
    }
    digitalWrite(SCL, HIGH);
    pinMode(SDA, OUTPUT);
    digitalWrite(SDA, LOW);  delayMicroseconds(5);
    digitalWrite(SDA, HIGH); delayMicroseconds(5);

    Wire.begin();
    Wire.setTimeOut(100);
    bme.begin(0x76);
    ccs.begin();

    // Push the staleness clock forward so we give the sensors a full window
    // to produce a fresh read before we'd consider another reset.
    lastBmeGoodAt = now;
    lastCcsGoodAt = now;
}

// Main loop
void loop() {
    // Loop-stall detector: if a previous iteration took unusually long
    // (> 30s gap between ticks), log it so the admin error log captures
    // pathological blocks. Skips the very first iteration and millis()
    // wraparound (49.7-day rollover would otherwise look like a stall).
    static unsigned long lastLoopTickMs = 0;
    unsigned long nowTickMs = millis();
    if (lastLoopTickMs != 0 && nowTickMs > lastLoopTickMs) {
        unsigned long gap = nowTickMs - lastLoopTickMs;
        if (gap > 30000UL) {
            char buf[96];
            snprintf(buf, sizeof(buf), "loop blocked %lums (free=%u, rssi=%d, wifi=%d)",
                     gap, (unsigned)ESP.getFreeHeap(),
                     (int)WiFi.RSSI(), (int)WiFi.status());
            logError("perf", buf);
        }
    }
    lastLoopTickMs = nowTickMs;

    // Late-NTP recovery: re-capture bootTime + period starts if NTP synced
    // after the 6s boot window. At most once per minute.
    static unsigned long lastNtpCheck = 0;
    if (bootTime == 0 && millis() - lastNtpCheck > 60000) {
        lastNtpCheck = millis();
        struct tm tNow;
        if (getLocalTime(&tNow, 0)) {
            time_t nowEpoch = mktime(&tNow);
            // bootTime represents the epoch second of boot. Approximate by
            // subtracting elapsed millis since boot.
            bootTime = nowEpoch - (time_t)(millis() / 1000UL);
            uint32_t nowUnix = (uint32_t)nowEpoch;
            if (currentWeek.started_unix  == 0) currentWeek.started_unix  = nowUnix;
            if (currentMonth.started_unix == 0) currentMonth.started_unix = nowUnix;
            if (currentYear.started_unix  == 0) currentYear.started_unix  = nowUnix;
            Serial.println("[ntp] late-sync recovered; bootTime + period starts stamped");
            // Trust NTP over RTC: write current authoritative time back to RTC.
            if (rtcOk) rtc.adjust(DateTime(nowUnix));
        }
    }

    // Flush dirty country tallies on a 30s cadence (instead of per-visit
    // SD rewrite). Critical under flash-crowd traffic where per-visit SD
    // writes would saturate the bus and starve AsyncTCP.
    maybeFlushCountries();

    // While an upload is streaming (and for a grace window after the last
    // chunk), AsyncTCP owns the SD bus. WS writes can block ~10s under that
    // contention and connect() up to 10s on handshake, which freezes the OLED
    // and starves sensor reads. Every WS code path below checks this flag.
    bool busyWithUpload = lastUploadChunkMs && (millis() - lastUploadChunkMs < UPLOAD_QUIET_MS);

    // On transition to busy (upload just started), reset the fail counter
    // so any in-progress WS escalation doesn't roll into a reboot after the
    // quiet window ends. The WS handling block below is gated on
    // !busyWithUpload, so reconnects don't run during uploads.
    // Recency guard: only count as "upload started" if the last chunk was
    // within the last 2s, otherwise stale lastUploadChunkMs would trigger
    // a spurious counter reset on every loop().
    static bool wasBusyWithUpload = false;
    if (busyWithUpload && !wasBusyWithUpload) {
        if (millis() - lastUploadChunkMs < 2000) {
            Serial.printf("[ws] upload started, suspending WS loop (was fails=%d)\n", wsReconnectFails);
            wsReconnectFails = 0;
            wsFastFails = 0;
        } else {
            Serial.printf("[ws] stale busy transition ignored (last chunk %lums ago)\n",
                          (unsigned long)(millis() - lastUploadChunkMs));
        }
    }
    wasBusyWithUpload = busyWithUpload;

    static bool wasWifiUp = true;
    static unsigned long wifiDownSince = 0;
    if (WiFi.status() != WL_CONNECTED) {
        if (wasWifiUp) {
            Serial.println("[net] wifi disconnected, reconnecting");
            wasWifiUp = false;
            wifiDownSince = millis();
        }
        WiFi.reconnect();
        // if stuck disconnected for 10 min, reboot to recover from bad radio states
        if (wifiDownSince && millis() - wifiDownSince > 600000UL) {
            Serial.println("[net] wifi down >10 min, restarting");
            logError("net", "wifi down >10 min, restarting");
            delay(200);
            cleanRestart();
        }
        delay(500);
    } else if (!wasWifiUp) {
        Serial.println("[net] wifi reconnected: " + WiFi.localIP().toString());
        wasWifiUp = true;
        wifiDownSince = 0;
    }

    // OLED pages
    if (millis() - lastPageSwitch >= PAGE_INTERVAL) {
        displayPage = (displayPage + 1) % DISPLAY_PAGES;
        lastPageSwitch = millis();
    }

    // 1px horizontal shift per page for OLED burn-in protection
    int shiftX = displayPage % 2;

    display.clearDisplay();

    switch (displayPage) {
        case 0: {
            display.setTextSize(1);
            char line[22];
            IPAddress ip = WiFi.localIP();
            snprintf(line, sizeof(line), "%d.%d.%d.%d", ip[0], ip[1], ip[2], ip[3]);
            display.setCursor(shiftX, 4);
            display.print(line);
            struct tm now;
            unsigned long secs = millis() / 1000;
            if (bootTime > 0 && getLocalTime(&now)) {
                time_t nowEpoch = mktime(&now);
                // Guard against backward time-steps (NTP slew, RTC step):
                // unsigned subtraction would wrap to ~136 years and the OLED
                // would print "Uptime: 49710d 14h" until the page rotates.
                if (nowEpoch > bootTime) secs = (unsigned long)(nowEpoch - bootTime);
            }
            int d = secs / 86400; int h = (secs % 86400) / 3600;
            snprintf(line, sizeof(line), "Uptime: %dd %dh", d, h);
            display.setCursor(shiftX, 18);
            display.print(line);
            snprintf(line, sizeof(line), "Visitors: %d", cachedVisitorCount);
            display.setCursor(shiftX, 32);
            display.print(line);
            snprintf(line, sizeof(line), "Today: %d", dailyVisitors);
            display.setCursor(shiftX, 46);
            display.print(line);
            break;
        }
        case 1: {
            float tf = safeBmeTemp() * 9.0f / 5.0f + 32.0f;
            float hu = safeBmeHumidity();
            // Refresh cached pressure for its side effect (lastBmeGoodAt
            // bump + cached_pressure_hpa update); altitude derives from it.
            (void)safeBmePressureHpa();
            bool bmeBad = bmeDegraded();
            bool ccsBad = ccsDegraded();
            display.setTextSize(1);
            char line[24];
            snprintf(line, sizeof(line), "%sTemp: %.1f F", bmeBad ? "!" : "", tf);
            display.setCursor(shiftX, 4);
            display.print(line);
            snprintf(line, sizeof(line), "%sHumidity: %.0f%%", bmeBad ? "!" : "", hu);
            display.setCursor(shiftX, 18);
            display.print(line);
            // Displayed as "eCO2" because the CCS811 is a MOX sensor that estimates CO2 from VOC levels.
            snprintf(line, sizeof(line), "%seCO2: %d ppm", ccsBad ? "!" : "", cached_co2);
            display.setCursor(shiftX, 32);
            display.print(line);
            snprintf(line, sizeof(line), "%sVOC: %d ppb", ccsBad ? "!" : "", cached_voc);
            display.setCursor(shiftX, 46);
            display.print(line);
            break;
        }
        case 2: {
            display.setTextSize(1);
            char line[22];
            snprintf(line, sizeof(line), "RSSI: %d dBm", WiFi.RSSI());
            display.setCursor(shiftX, 4);
            display.print(line);
            snprintf(line, sizeof(line), "Requests: %d", requestsThisInterval);
            display.setCursor(shiftX, 18);
            display.print(line);
            snprintf(line, sizeof(line), "CPU Temp: %.1f C", temperatureRead());
            display.setCursor(shiftX, 32);
            display.print(line);
            snprintf(line, sizeof(line), "Free Heap: %dK", ESP.getFreeHeap() / 1024);
            display.setCursor(shiftX, 46);
            display.print(line);
            break;
        }
        case 3: {
            display.drawBitmap(48 + shiftX, 2, logoBitmap, 32, 32, WHITE);
            display.setTextSize(1);
            display.setCursor(40 + shiftX, 38);
            display.print("HelloESP");
            display.setCursor(28 + shiftX, 52);
            display.print("helloesp.com");
            break;
        }
        case 4: {
            display.drawBitmap(39 + shiftX, 2, qrBitmap, 50, 50, WHITE);
            display.setTextSize(1);
            display.setCursor(28 + shiftX, 55);
            display.print("helloesp.com");
            break;
        }
        case 5: {
            struct tm t;
            if (getLocalTime(&t)) {
                char timeBuf[8], subBuf[8], dateBuf[16];
                strftime(timeBuf, sizeof(timeBuf), "%I:%M", &t);
                if (timeBuf[0] == '0') memmove(timeBuf, timeBuf + 1, strlen(timeBuf));
                strftime(subBuf, sizeof(subBuf), ":%S %p", &t);
                strftime(dateBuf, sizeof(dateBuf), "%a, %b %d", &t);
                display.setTextSize(3);
                int tw = strlen(timeBuf) * 18;
                int subW = strlen(subBuf) * 6;
                int totalW = tw + subW;
                int startX = (128 - totalW) / 2 + shiftX;
                display.setCursor(startX, 8);
                display.print(timeBuf);
                display.setTextSize(1);
                display.setCursor(startX + tw, 22);
                display.print(subBuf);
                int dw = strlen(dateBuf) * 6;
                display.setCursor((128 - dw) / 2 + shiftX, 42);
                display.print(dateBuf);
            } else {
                display.setTextSize(1);
                display.setCursor(shiftX, 28);
                display.print("No time sync");
            }
            break;
        }
    }

    display.display();

    if (!ccsHealth.retired && ccs.available()) {
        ccs.setEnvironmentalData(safeBmeHumidity(), safeBmeTemp());
        if (!ccs.readData()) {
            uint16_t co2 = ccs.geteCO2();
            uint16_t voc = ccs.getTVOC();
            // CCS811 algorithm output: eCO2 400-32768 ppm, TVOC 0-32768 ppb.
            // Bus-error reads can return 0xFFFF (65535); reject those so they
            // don't poison cached values, period averages, or hall-of-fame.
            // Mark the read as good if at least one channel was in range,
            // since partial corruption (e.g. only co2 mangled) is still
            // informative for the other channel.
            bool gotAny = false;
            if (co2 <= 32768) { cached_co2 = co2; gotAny = true; }
            if (voc <= 32768) { cached_voc = voc; gotAny = true; }
            if (gotAny) lastCcsGoodAt = millis();
        } else {
            Serial.println("CCS811 read error");
        }
    }

    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {
        int minute = timeinfo.tm_min;
        if (minute % 5 == 0 && minute != lastLoggedMinute) {
            lastLoggedMinute = minute;
            logStats();
            // SD.usedBytes() scans the whole FAT; "seconds" on a healthy
            // card, but on a fragmented / heavy-use card it can block much
            // longer and cascade into a WS drop. Refresh only at minute 0/30
            // and time the call so we see it in the admin Error Log if it
            // ever exceeds the threshold.
            if (minute == 0 || minute == 30) {
                unsigned long sdStart = millis();
                cachedSdUsedMB = (float)SD.usedBytes() / (1024.0f * 1024.0f);
                unsigned long sdMs = millis() - sdStart;
                if (sdMs > 3000) {
                    char buf[80];
                    snprintf(buf, sizeof(buf), "SD.usedBytes() blocked %lums (used=%.1fMB)",
                             sdMs, cachedSdUsedMB);
                    logError("perf", buf);
                }
            }
            Serial.println("Logged at: " + getTimestamp());
        }
    }

    if (ledOn && (millis() - lastRequestTime >= LED_ON_TIME)) {
        digitalWrite(LED_PIN, LOW);
        ledOn = false;
    }

    digitalWrite(NOTIF_LED_PIN, pendingGuestbook > 0 ? HIGH : LOW);

    // log sensor degradation transitions once when they flip
    static bool wasBmeBad = false, wasCcsBad = false;
    bool bmeBadNow = bmeDegraded();
    bool ccsBadNow = ccsDegraded();
    if (bmeBadNow != wasBmeBad) {
        Serial.println(bmeBadNow ? "[sensor] BME280 stale (>2 min no good read)" : "[sensor] BME280 recovered");
        logError("sensor", bmeBadNow ? "BME280 stale (>2 min no good read)" : "BME280 recovered");
        wasBmeBad = bmeBadNow;
    }
    if (ccsBadNow != wasCcsBad) {
        Serial.println(ccsBadNow ? "[sensor] CCS811 stale (>2 min no good read)" : "[sensor] CCS811 recovered");
        logError("sensor", ccsBadNow ? "CCS811 stale (>2 min no good read)" : "CCS811 recovered");
        wasCcsBad = ccsBadNow;
    }
    // Both sensors stale at once usually means a wedged bus, not two
    // independent failures. Kick the bus (cooldown inside prevents thrash).
    if (bmeBadNow && ccsBadNow) tryI2cRecovery();

    if (millis() - lastCheckpointMs > CHECKPOINT_INTERVAL_MS) {
        lastCheckpointMs = millis();
        saveCheckpoint();
    }

    // sample heap + RSSI once per minute for admin sparklines
    if (millis() - lastHealthSample > 60000UL) {
        lastHealthSample = millis();
        healthRing[healthHead].free_heap = ESP.getFreeHeap();
        healthRing[healthHead].rssi      = (int8_t)WiFi.RSSI();
        healthHead = (healthHead + 1) % HEALTH_SAMPLES;
        if (healthCount < HEALTH_SAMPLES) healthCount++;
    }

    // HTTP handlers run on the AsyncTCP task; send WS frames from the main loop only.
    // Acquire fence pairs with the release fence on the writer side so the
    // notify buffers (notifyEntryName/Country/Message) are guaranteed visible
    // here before we read them.
    if (pendingNotifyFlag) {
        std::atomic_thread_fence(std::memory_order_acquire);
        pendingNotifyFlag = false;
        notifyPendingIfIncreased();
    }

    // SHA256 of a firmware .bin on SD. Yields between chunks so async_tcp
    // stays alive through the ~6s read on a 1.5MB file. Result is cached
    // in sha256Result/sha256ResultPath; clients poll via the status endpoint.
    if (pendingSha256) {
        pendingSha256 = false;
        sha256Computing = true;
        File f = SD.open(pendingSha256Path, FILE_READ);
        if (f) {
            mbedtls_sha256_context ctx;
            mbedtls_sha256_init(&ctx);
            mbedtls_sha256_starts(&ctx, 0);  // 0 = SHA256, 1 = SHA224
            // static so we don't reserve 2KB on loop()'s stack; stack canary
            // would trip under deep call chains otherwise.
            static uint8_t buf[2048];
            while (f.available()) {
                size_t n = f.read(buf, sizeof(buf));
                if (n == 0) break;
                mbedtls_sha256_update(&ctx, buf, n);
                delay(1);  // yield to async_tcp between chunks
            }
            f.close();
            uint8_t hash[32];
            mbedtls_sha256_finish(&ctx, hash);
            mbedtls_sha256_free(&ctx);
            for (int i = 0; i < 32; i++) {
                snprintf(sha256Result + i * 2, 3, "%02x", hash[i]);
            }
            sha256Result[64] = '\0';
            strncpy(sha256ResultPath, pendingSha256Path, sizeof(sha256ResultPath) - 1);
            sha256ResultPath[sizeof(sha256ResultPath) - 1] = '\0';
            Serial.printf("[fw] sha256 %s = %s\n", pendingSha256Path, sha256Result);
        } else {
            sha256Result[0] = '\0';
            sha256ResultPath[0] = '\0';
            Serial.printf("[fw] sha256: open failed for %s\n", pendingSha256Path);
        }
        sha256Computing = false;
    }

    // Firmware rollback via partition-pointer flip. No file I/O, just
    // Update.rollBack() + reboot.
    if (pendingRollback) {
        pendingRollback = false;
        Serial.println("[fw] rollback requested, flipping partition pointer");
        if (Update.canRollBack() && Update.rollBack()) {
            Serial.println("[fw] rollback ok, restarting");
            delay(500);
            cleanRestart();
        } else {
            Serial.println("[fw] rollback failed");
            logError("fw", "rollback failed (canRollBack was true at request time)");
        }
    }

    // Firmware flash from an SD-stored .bin. The heavy work runs here on
    // the main loop so AsyncTCP stays responsive while chunks are read.
    // Copy of the existing /_ota flash pattern but sourced from SD.
    if (pendingSDFlash) {
        pendingSDFlash = false;
        Serial.printf("[fw] SD-flash starting from %s\n", pendingSDFlashPath);
        // Pause WS activity during flash; flash writes disable flash cache
        // and we don't want stats pushes fighting for bandwidth either.
        lastUploadChunkMs = millis();
        File f = SD.open(pendingSDFlashPath, FILE_READ);
        if (!f) {
            Serial.println("[fw] SD-flash: open failed");
            logError("fw", "SD-flash open failed");
        } else {
            size_t total = f.size();
            if (!Update.begin(total)) {
                Serial.println("[fw] SD-flash: Update.begin failed");
                Update.printError(Serial);
                logError("fw", "SD-flash Update.begin failed");
                f.close();
            } else {
                // static so we don't reserve 4KB on loop()'s stack; the
                // SHA256 path has a similar 2KB buffer, combined stack
                // footprint would blow the Arduino loopTask stack canary.
                static uint8_t buf[4096];
                size_t written = 0;
                bool ok = true;
                while (f.available() && ok) {
                    size_t n = f.read(buf, sizeof(buf));
                    if (n == 0) break;
                    if (Update.write(buf, n) != n) {
                        Serial.printf("[fw] SD-flash: write failed at %u/%u\n",
                                      (unsigned)written, (unsigned)total);
                        Update.printError(Serial);
                        ok = false;
                        break;
                    }
                    written += n;
                    lastUploadChunkMs = millis();  // keep busyWithUpload fresh
                    delay(1);  // yield to async_tcp
                }
                f.close();
                if (ok && Update.end(true)) {
                    Serial.printf("[fw] SD-flash complete: %u bytes, restarting\n",
                                  (unsigned)written);
                    delay(500);
                    cleanRestart();
                } else {
                    // Release the in-progress Update slot. Without this, the
                    // partition stays "in progress" and the next Update.begin()
                    // refuses to start until a reboot.
                    if (Update.isRunning()) Update.abort();
                    Serial.println("[fw] SD-flash: Update.end failed or short write");
                    Update.printError(Serial);
                    char errBuf[96];
                    snprintf(errBuf, sizeof(errBuf),
                             "SD-flash failed at %u/%u", (unsigned)written, (unsigned)total);
                    logError("fw", errBuf);
                }
            }
        }
    }

    if (pendingMaintenanceFlag) {
        std::atomic_thread_fence(std::memory_order_acquire);
        pendingMaintenanceFlag = false;
        if (wsConnected && wsClient.connected()) {
            String msg = "{\"type\":\"event\",\"event\":\"maintenance\",\"minutes\":";
            msg += pendingMaintenanceMinutes;
            msg += ",\"message\":\"";
            msg += jsonEscape(String(pendingMaintenanceMessage));
            msg += "\"}";
            wsSendText(wsClient, msg);
            // Stamp local UI state. Worker is authoritative; this is just
            // so the admin panel can show "currently in maintenance, ends
            // in 23 min" without polling the worker.
            if (pendingMaintenanceMinutes == 0) {
                localMaintenanceUntilUnix = 0;
                localMaintenanceMessage[0] = '\0';
            } else {
                struct tm tm;
                if (getLocalTime(&tm, 0)) {
                    localMaintenanceUntilUnix = (uint32_t)mktime(&tm) + (uint32_t)pendingMaintenanceMinutes * 60;
                }
                strncpy(localMaintenanceMessage, pendingMaintenanceMessage, sizeof(localMaintenanceMessage) - 1);
                localMaintenanceMessage[sizeof(localMaintenanceMessage) - 1] = '\0';
            }
            saveMaintenanceState();
        }
    }

    // SSE stats push every 15s. Cadence chosen over 5s because tighter pushes
    // collide with LAN homepage parallel GETs and saturate lwIP pbufs, which
    // can cascade into WS write failures. 15s freshness is well within
    // "ambient stats" tolerance for SSE viewers.
    static unsigned long lastStatsPush = 0;
    if (!busyWithUpload && wsConnected && wsClient.connected() &&
        millis() - lastStatsPush > 15000) {
        lastStatsPush = millis();
        String body = buildStatsJson();
        String msg  = "{\"type\":\"event\",\"event\":\"stats_update\",\"data\":";
        msg += body;
        msg += "}";
        wsSendText(wsClient, msg);
    }

    // event-driven /console push: fires the instant a tracked request lands.
    if (pendingConsolePush) {
        pendingConsolePush = false;
        if (!busyWithUpload && wsConnected && wsClient.connected() && consoleCount > 0) {
            int lastIdx = (consoleHead - 1 + CONSOLE_SIZE) % CONSOLE_SIZE;
            ConsoleEntry &e = consoleRing[lastIdx];
            String msg = "{\"type\":\"event\",\"event\":\"console_update\",\"data\":{";
            msg += "\"time\":" + String(e.unix_time);
            msg += ",\"country\":\""; msg += e.country; msg += "\"";
            msg += ",\"method\":\"";  msg += e.method;  msg += "\"";
            msg += ",\"path\":\"" + jsonEscape(e.path) + "\"";
            msg += ",\"status\":" + String(e.status);
            msg += "}}";
            wsSendText(wsClient, msg);
        }
    }

    // Daily R2 backup at BACKUP_HOUR_LOCAL. isBackupDue() is wall-clock based so reboots
    // don't skip a day and don't cause duplicates within the same day.
    static unsigned long lastBackupCheckMs = 0;
    if (!busyWithUpload && wsConnected && wsClient.connected() && millis() - lastBackupCheckMs > 60000UL) {
        lastBackupCheckMs = millis();
        if (isBackupDue()) pendingBackupFlag = true;
    }
    if (pendingBackupFlag && !busyWithUpload) {
        pendingBackupFlag = false;
        buildAndSendBackup();
    }

    // Shelly poll for power monitoring. Skips silently if no shelly_url
    // configured. Gated on !busyWithUpload (avoids upload contention)
    // AND on the WS being either absent (LAN-only install) or fully
    // connected. The handshake window is when async_tcp is most likely
    // to starve if the synchronous HTTPClient call grabs the lwip mutex
    // for any extended period; a half-connected WS state means we wait
    // for the next 30s tick before trying.
    static unsigned long lastShellyPollMs = 0;
    bool wsQuiet = (strlen(cfgWorkerUrl) == 0)
                || (wsConnected && wsClient.connected());
    if (cfgShellyUrl[0] != '\0' && !busyWithUpload && wsQuiet
        && millis() - lastShellyPollMs > SHELLY_POLL_INTERVAL_MS) {
        lastShellyPollMs = millis();
        pollShelly();
    }

    // Midnight rollover for today_energy_wh. Detects local-date change
    // since the last rollover and zeroes today_energy on transition.
    // Cheap to check every loop; only does work on actual day boundary.
    {
        struct tm tmNow;
        if (getLocalTime(&tmNow, 0)) {
            char todayNow[11];
            snprintf(todayNow, sizeof(todayNow), "%04d-%02d-%02d",
                     tmNow.tm_year + 1900, tmNow.tm_mon + 1, tmNow.tm_mday);
            if (shelly_today_date[0] == '\0') {
                // First successful local time read; capture without resetting.
                strncpy(shelly_today_date, todayNow, sizeof(shelly_today_date) - 1);
                shelly_today_date[sizeof(shelly_today_date) - 1] = '\0';
            } else if (strcmp(todayNow, shelly_today_date) != 0) {
                today_energy_wh = 0.0f;
                strncpy(shelly_today_date, todayNow, sizeof(shelly_today_date) - 1);
                shelly_today_date[sizeof(shelly_today_date) - 1] = '\0';
            }
            // Reset dailyVisitors at chip-local midnight too. Previously
            // this only reset on the first post-midnight visit handler
            // call; without traffic during the gap, the counter held
            // yesterday's value. Full YYYY-MM-DD comparison (not just
            // tm_mday) so May 8 -> Jun 8 cross-month-same-day boundary
            // also fires the reset; tm_mday alone would miss it.
            if (strcmp(todayNow, dailyVisitorsDate) != 0) {
                dailyVisitors = 0;
                strncpy(dailyVisitorsDate, todayNow, sizeof(dailyVisitorsDate) - 1);
                dailyVisitorsDate[sizeof(dailyVisitorsDate) - 1] = '\0';
                lastVisitorDay = tmNow.tm_mday;
                saveDailyVisitors();
            }
        }
    }

    if (pendingR2HealthcheckFlag) {
        pendingR2HealthcheckFlag = false;
        if (wsConnected && wsClient.connected()) {
            wsSendText(wsClient, String("{\"type\":\"event\",\"event\":\"r2_healthcheck\"}"));
        } else {
            // log a failure locally so the admin sees "offline" instead of stale-true
            r2HealthcheckPass = false;
            strncpy(r2HealthcheckDetail, "Worker link offline", sizeof(r2HealthcheckDetail) - 1);
            r2HealthcheckDetail[sizeof(r2HealthcheckDetail) - 1] = '\0';
            struct tm tm;
            if (getLocalTime(&tm, 0)) r2HealthcheckAtUnix = mktime(&tm);
        }
    }

    if (pendingTestEmailFlag) {
        pendingTestEmailFlag = false;
        if (wsConnected && wsClient.connected()) {
            wsSendText(wsClient, String("{\"type\":\"event\",\"event\":\"test_email\"}"));
        } else {
            testEmailPass = false;
            strncpy(testEmailDetail, "Worker link offline", sizeof(testEmailDetail) - 1);
            testEmailDetail[sizeof(testEmailDetail) - 1] = '\0';
            struct tm tm;
            if (getLocalTime(&tm, 0)) testEmailAtUnix = mktime(&tm);
        }
    }

    if (pendingSnakeClearFlag) {
        pendingSnakeClearFlag = false;
        if (wsConnected && wsClient.connected()) {
            wsSendText(wsClient, String("{\"type\":\"event\",\"event\":\"snake_clear\"}"));
        } else {
            // Record a failure locally so the admin UI's polling sees the
            // result instead of staring at stale data forever. Mirrors the
            // r2_healthcheck / test_email offline fallback pattern.
            snakeClearOk = false;
            struct tm tm;
            if (getLocalTime(&tm, 0)) snakeClearAtUnix = mktime(&tm);
        }
    }

    // Diagnostic: if busyWithUpload is preventing WS recovery, log it. The
    // entire WS loop below is gated on !busyWithUpload, including the 120s
    // safety-net reboot. If lastUploadChunkMs keeps getting updated (e.g.,
    // by a stuck upload that never completes), we'd be permanently blocked.
    // Logged at most once per 30s so it doesn't flood serial.
    if (strlen(cfgWorkerUrl) > 0 && busyWithUpload && !wsConnected) {
        static unsigned long lastBusyBlockLog = 0;
        if (millis() - lastBusyBlockLog > 30000) {
            Serial.printf("[ws] recovery blocked: busyWithUpload=true (last chunk %lums ago)\n",
                          lastUploadChunkMs ? (unsigned long)(millis() - lastUploadChunkMs) : 0UL);
            lastBusyBlockLog = millis();
        }
    }

    if (strlen(cfgWorkerUrl) > 0 && !busyWithUpload) {
        // Detect the "wsConnected was true but client.connected() just flipped
        // to false" case explicitly: this is the silent path where TCP gets
        // RST'd externally (CF closes us, network drops us, keepalive miss)
        // without any of our write/read helpers firing. Most common disconnect
        // trigger in the cascade logs; worth catching by itself.
        if (wsConnected && !wsClient.connected()) {
            // RSSI + WiFi status at the moment of disconnect: correlates
            // silent drops with signal dips / AP kicks. Typical reading
            // on healthy LAN is -55 to -70 dBm, WL_CONNECTED (3). If we
            // see RSSI < -80 dBm or WiFi.status() != 3 at drop time,
            // the cause is almost certainly the radio, not CF/TCP.
            Serial.printf("[ws] wsClient.connected() went false (free=%u, largest=%u, rssi=%d, wifi=%d)\n",
                          (unsigned)ESP.getFreeHeap(), (unsigned)ESP.getMaxAllocHeap(),
                          (int)WiFi.RSSI(), (int)WiFi.status());
            wsConnected = false;
        }
        if (wsConnected && wsClient.connected()) {
            // Healthy branch resets the safety-net timer. Without this, a
            // flapping connection (up 3s, down 5s, repeat) would hit 120s
            // wall-clock and reboot even though most of that time was "up."
            // Repurpose wsDisconnectedSince==0 as the "healthy entry" signal
            // and use a separate counter for healthy-since-when.
            static unsigned long healthySince = 0;
            if (wsDisconnectedSince != 0) healthySince = millis();  // first tick after recovery
            wsDisconnectedSince = 0;
            // After 60s of sustained healthy connection, clear the escalation
            // ladder so a brief WiFi blip later doesn't jump straight to
            // cleanRestart() because of a stale level=1 from the prior recovery.
            if (wsEscalationLevel != 0 && healthySince != 0 && millis() - healthySince > 60000) {
                wsEscalationLevel = 0;
                Serial.println("[ws] escalation level cleared after sustained healthy connection");
            }
            // Cap at 8 messages per loop tick. Under a flood of queued relays
            // the old `while available` drained everything in one go and
            // starved OLED/sensor work; this yields after a bounded batch so
            // the rest of the loop still runs each iteration.
            for (int n = 0; n < 8 && wsClient.available(); n++) {
                String msg = wsRead(wsClient);
                if (msg.length() > 0) {
                    handleWsRelay(msg);
                }
                if (!wsConnected) break;
            }
            if (millis() - lastWsPing > WS_PING_MS) {
                lastWsPing = millis();
                uint8_t pingFrame[6] = { 0x89, 0x80, 0, 0, 0, 0 };
                // if the TCP buffer accepts the ping, the connection is live
                if (wsClient.write(pingFrame, 6) == 6) lastWsActivity = millis();
            }
            if (millis() - lastWsActivity > WS_TIMEOUT_MS) {
                Serial.println("[ws] watchdog: no activity for 60s, disconnecting");
                wsConnected = false;
                wsClient.stop();
            }
        } else {
            wsConnected = false;
            // Safety net: if we've been disconnected for 60s CONTINUOUSLY
            // (no successful reconnect in between) without a legitimate
            // upload pause, something is wedged that the escalation ladder
            // didn't catch. Blunt reboot. wsDisconnectedSince is reset at
            // the top of the healthy branch so flapping connections don't
            // accumulate wall-clock time toward this threshold.
            //
            // 60s (down from 120s): tightened to halve the user-visible
            // blank window. Originally went to 45s but that was too tight:
            // a WiFi.reconnect at t≈30s plus 5-10s WiFi reassociation plus
            // post-WiFi WS handshake doesn't always fit under 45s, leading
            // to reboots mid-recovery. 60s gives the recovery path room
            // to finish naturally.
            // Skip the safety-net reboot during any in-progress local
            // operation that would leave SD or flash in a stuck state if
            // interrupted: file upload (busyWithUpload), SD-flash of a
            // .bin (pendingSDFlash), SHA256 computation (sha256Computing),
            // or an in-progress R2 backup (backupRunning). A full SD
            // backup can take ~30-60s; without this gate the safety-net
            // would cut it off and leave R2 with a partial snapshot.
            // SD writes mid-reboot are the exact pattern that leaves the
            // card's MCU in NOT_READY until physical power cycle. The
            // disconnected timer resets on the way out so it doesn't
            // accumulate during these grace periods.
            bool localBusy = busyWithUpload || pendingSDFlash || sha256Computing || backupRunning;
            if (!localBusy) {
                if (wsDisconnectedSince == 0) wsDisconnectedSince = millis();
                if (millis() - wsDisconnectedSince > 60000UL) {
                    Serial.println("[ws] disconnected 60s, hard restart");
                    logError("ws", "ws disconnected 60s, hard restart");
                    wsDisconnectedSince = 0;
                    delay(200);
                    cleanRestart();
                }
            } else {
                wsDisconnectedSince = 0;
            }

            // Backoff schedule. First retry is immediate (500ms) because most
            // drops are transient (LAN-burst TCP stall, momentary WiFi dip);
            // waiting 5s for the first attempt means 5s of blank UX for what
            // usually resolves in one round-trip. After that, LINEAR growth
            // (5s * fails, capped 30s) instead of exponential. Exponential
            // sent us into 40s+ gaps between attempts which couldn't recover
            // within the 45s safety-net window.
            unsigned long backoff;
            if (wsReconnectFails == 0) {
                backoff = 500;
            } else {
                backoff = WS_RECONNECT_MS * wsReconnectFails;
                if (backoff > 30000UL) backoff = 30000UL;
            }
            if (millis() - lastWsAttempt > backoff) {
                lastWsAttempt = millis();
                Serial.printf("[ws] reconnect attempt (fails=%d fastFails=%d)\n",
                              wsReconnectFails, wsFastFails);
                wsClient.stop();
                // Give LWIP time to fully release the socket fd before we
                // grab a new one. Under socket exhaustion the teardown path
                // can stall longer than a simple close.
                delay(500);

                // Escalation ladder at fails=3:
                //   level 0: WiFi.reconnect() to refresh AP association.
                //   level 1+: cleanRestart(). Going straight to reboot when
                //     a refresh didn't help avoids leaving AsyncWebServer's
                //     listening socket in a torn-down state, which would
                //     break LAN access until next boot.
                //   3 fast-fails (connectWorker returning in <1s) -> reboot:
                //     LWIP socket pool exhausted, retries won't help.
                //   60s wall-clock disconnected -> reboot (safety net above).
                // Fast-path: a DNS-resolution failure at fails=1 means the
                // resolver state is wedged, not that CF is down. Skip
                // straight to WiFi.reconnect() instead of waiting another
                // ~12s for two more fails to ramp through the ladder.
                // Re-uses the same escalation tracking so a wedged WiFi
                // (where reconnect() doesn't help) still cleanRestarts at
                // the next round.
                if (wsLastFailWasDns && wsReconnectFails >= 1 && wsEscalationLevel == 0) {
                    Serial.println("[ws] DNS fail; firing WiFi.reconnect early");
                    logError("ws", "DNS fail short-circuit, reconnecting wifi");
                    WiFi.reconnect();
                    wsEscalationLevel = 1;
                    wsReconnectFails = 1;
                    wsFastFails = 0;
                    wsLastFailWasDns = false;
                    lastWsAttempt = millis();
                    return;  // back to top of loop, next cycle does the connect
                }

                if (wsReconnectFails == 3) {
                    if (wsEscalationLevel == 0) {
                        Serial.println("[ws] 3 consecutive fails, refreshing WiFi");
                        logError("ws", "3 consecutive ws fails, reconnecting wifi");
                        WiFi.reconnect();
                        wsEscalationLevel = 1;
                    } else {
                        Serial.println("[ws] WiFi.reconnect didn't help, cleanRestart");
                        logError("ws", "WiFi.reconnect failed, restarting");
                        delay(200);
                        cleanRestart();
                    }
                    // Set fails=1 (NOT 0): backoff for fails=1 is 5s, which
                    // gives WiFi time to actually reassociate before the
                    // next connectWorker(). With fails=0, the next attempt
                    // fires in 500ms, WiFi isn't up yet, connect fast-
                    // fails with "Host unreachable", wsFastFails increments
                    // toward the socket-exhaustion threshold, and the device
                    // reboots mid-recovery, killing in-flight LAN loads.
                    //
                    // Reset wsFastFails too: post-recovery attempts may
                    // legitimately fast-fail if the radio hasn't fully come
                    // up; that shouldn't count toward exhaustion.
                    wsReconnectFails = 1;
                    wsFastFails = 0;
                    lastWsAttempt = millis();
                } else {
                    unsigned long connStart = millis();
                    bool connOk = connectWorker();
                    unsigned long connMs = millis() - connStart;
                    if (connOk) {
                        wsReconnectFails = 0;
                        wsFastFails = 0;
                        wsEscalationLevel = 0;
                        wsReconnectCount++;
                        // Push stats immediately so the Worker has fresh
                        // lastStats from the moment of connect. Without this,
                        // a Worker DO restart (CF eviction) leaves lastStats
                        // null until our regular 15s push cadence rolls
                        // around, during which every page shows the
                        // "offline" badge even though the device is fine.
                        if (wsConnected && wsClient.connected()) {
                            String body = buildStatsJson();
                            String msg  = "{\"type\":\"event\",\"event\":\"stats_update\",\"data\":";
                            msg += body;
                            msg += "}";
                            wsSendText(wsClient, msg);
                        }
                    } else {
                        wsReconnectFails++;
                        if (connMs < 1000) {
                            wsFastFails++;
                            Serial.printf("[ws] fast-fail %d (took %lums)\n", wsFastFails, connMs);
                            if (wsFastFails >= 3) {
                                Serial.println("[ws] 3 fast-fails, socket exhaustion, restarting");
                                logError("ws", "3 ws fast-fails, socket exhaustion, restarting");
                                delay(200);
                                cleanRestart();
                            }
                        } else {
                            wsFastFails = 0;
                        }
                    }
                }
            }
        }
    } else if (strlen(cfgWorkerUrl) > 0 && busyWithUpload) {
        // Don't touch wsClient during the upload. Keep lastWsAttempt rolling
        // so the first reconnect after quiet ends isn't gated by stale backoff.
        lastWsAttempt = millis();
    }

    // heap safety net. Reboots before AsyncTCP starts failing allocations.
    // 30KB threshold should leave enough room for WiFi driver + a few TCP
    // buffers without wasting too much usable headroom.
    if (ESP.getFreeHeap() < 30000) {
        Serial.println("Heap critical, restarting...");
        char buf[48];
        snprintf(buf, sizeof(buf), "heap critical (%u bytes free), restarting", (unsigned)ESP.getFreeHeap());
        logError("heap", buf);
        delay(100);
        cleanRestart();
    }

    delay(500);
}
