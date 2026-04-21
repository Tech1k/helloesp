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
#include "FS.h"
#include "SD.h"
#include "SPI.h"
#include "time.h"
#include "Update.h"
#include "esp_system.h"
#include "mbedtls/md.h"
#include <WiFiClientSecure.h>
#include <ESPmDNS.h>

// Version
#define FIRMWARE_VERSION "1.0"

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

const char* ntpServer          = "pool.ntp.org";
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
#define WS_RECONNECT_MS 5000
#define WS_PING_MS 30000
#define WS_TIMEOUT_MS 60000

// Admin auth bruteforce lockout (per-IP)
#define AUTH_MAX_FAILS     5
#define AUTH_LOCKOUT_MS    600000UL   // 10 min
#define AUTH_TRACK_SIZE    8

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
        // close frame
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

uint16_t cached_co2 = 0;
uint16_t cached_voc = 0;
float    cached_temp_c       = 20.0f;
float    cached_humidity     = 50.0f;
float    cached_pressure_hpa = 1013.25f;
float    cached_altitude_ft  = 0.0f;
unsigned long lastBmeGoodAt = 0;
unsigned long lastCcsGoodAt = 0;
#define SENSOR_STALE_MS  120000UL

static float safeBmeTemp() {
    float t = bme.readTemperature();
    if (!isnan(t)) { cached_temp_c = t; lastBmeGoodAt = millis(); }
    return cached_temp_c;
}
static float safeBmeHumidity() {
    float h = bme.readHumidity();
    if (!isnan(h)) { cached_humidity = h; lastBmeGoodAt = millis(); }
    return cached_humidity;
}
static float safeBmePressureHpa() {
    float p = bme.readPressure() / 100.0f;
    if (!isnan(p) && p > 0.0f) { cached_pressure_hpa = p; lastBmeGoodAt = millis(); }
    return cached_pressure_hpa;
}
static float safeBmeAltitudeFt() {
    float a = bme.readAltitude(SEALEVELPRESSURE_HPA) * 3.28084f;
    if (!isnan(a)) { cached_altitude_ft = a; lastBmeGoodAt = millis(); }
    return cached_altitude_ft;
}

static bool bmeDegraded() { return lastBmeGoodAt > 0 && (millis() - lastBmeGoodAt) > SENSOR_STALE_MS; }
static bool ccsDegraded() { return lastCcsGoodAt > 0 && (millis() - lastCcsGoodAt) > SENSOR_STALE_MS; }

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

static String periodToJson(const char* label, const PeriodStats& p) {
    String j;
    j.reserve(640);
    j = "{\"label\":\"";
    j += label;
    j += "\",\"visitors\":"; j += p.visitors;
    j += ",\"guestbook\":";  j += p.guestbook;
    j += ",\"peak_reqs\":";  j += p.peak_reqs;
    j += ",\"samples\":";    j += p.samples;
    if (p.samples > 0) {
        if (p.temp_min_c <= p.temp_max_c) {
            j += ",\"temp_c\":{\"min\":";   j += String(p.temp_min_c, 2);
            j += ",\"max\":";               j += String(p.temp_max_c, 2);
            j += ",\"avg\":";               j += String((float)(p.temp_sum_c / p.samples), 2);
            j += "}";
        }
        if (p.hum_min <= p.hum_max) {
            j += ",\"humidity\":{\"min\":"; j += String(p.hum_min, 1);
            j += ",\"max\":";               j += String(p.hum_max, 1);
            j += ",\"avg\":";               j += String((float)(p.hum_sum / p.samples), 1);
            j += "}";
        }
        if (p.co2_min != 0xFFFF && p.co2_max > 0) {
            j += ",\"co2_ppm\":{\"min\":"; j += p.co2_min;
            j += ",\"max\":";               j += p.co2_max;
            j += ",\"avg\":";               j += (uint32_t)(p.co2_sum / p.samples);
            j += "}";
        }
        if (p.voc_min != 0xFFFF && p.voc_max > 0) {
            j += ",\"voc_ppb\":{\"min\":"; j += p.voc_min;
            j += ",\"max\":";               j += p.voc_max;
            j += ",\"avg\":";               j += (uint32_t)(p.voc_sum / p.samples);
            j += "}";
        }
    }
    j += ",\"started\":"; j += p.started_unix;
    j += "}";
    return j;
}

static bool flushPeriod(const char* dir, const char* label, const PeriodStats& p) {
    String path = String(dir) + "/" + label + ".json";
    String tmp  = path + ".tmp";
    String bak  = path + ".bak";
    File f = SD.open(tmp, FILE_WRITE);
    if (!f) { Serial.printf("[stats] flush open failed: %s\n", path.c_str()); return false; }
    f.print(periodToJson(label, p));
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

#define CHECKPOINT_MAGIC 0x48455333UL

static void saveCheckpoint() {
    const char* path = "/stats/checkpoint.bin";
    const char* tmp  = "/stats/checkpoint.tmp";
    const char* bak  = "/stats/checkpoint.bak";
    File f = SD.open(tmp, FILE_WRITE);
    if (!f) { Serial.println("[stats] checkpoint open failed"); return; }
    uint32_t magic = CHECKPOINT_MAGIC;
    f.write((const uint8_t*)&magic, sizeof(magic));
    f.write((const uint8_t*)&currentWeek, sizeof(PeriodStats));
    f.write((const uint8_t*)&currentMonth, sizeof(PeriodStats));
    f.write((const uint8_t*)&currentYear, sizeof(PeriodStats));
    f.write((const uint8_t*)lastWeekLabel, sizeof(lastWeekLabel));
    f.write((const uint8_t*)lastMonthLabel, sizeof(lastMonthLabel));
    f.write((const uint8_t*)lastYearLabel, sizeof(lastYearLabel));
    f.close();
    if (SD.exists(bak)) SD.remove(bak);
    if (SD.exists(path)) SD.rename(path, bak);
    if (!SD.rename(tmp, path)) {
        if (SD.exists(bak)) SD.rename(bak, path);
        return;
    }
    if (SD.exists(bak)) SD.remove(bak);
}

static void loadCheckpoint() {
    resetPeriod(currentWeek);
    resetPeriod(currentMonth);
    resetPeriod(currentYear);
    File f = SD.open("/stats/checkpoint.bin", FILE_READ);
    if (!f) {
        if (SD.exists("/stats/checkpoint.bak")) {
            SD.rename("/stats/checkpoint.bak", "/stats/checkpoint.bin");
            f = SD.open("/stats/checkpoint.bin", FILE_READ);
        }
        if (!f) return;
    }
    uint32_t magic = 0;
    size_t expected = sizeof(magic) + sizeof(PeriodStats)*3 + sizeof(lastWeekLabel) + sizeof(lastMonthLabel) + sizeof(lastYearLabel);
    if (f.size() != expected) { f.close(); return; }
    f.read((uint8_t*)&magic, sizeof(magic));
    if (magic != CHECKPOINT_MAGIC) { f.close(); return; }
    f.read((uint8_t*)&currentWeek, sizeof(PeriodStats));
    f.read((uint8_t*)&currentMonth, sizeof(PeriodStats));
    f.read((uint8_t*)&currentYear, sizeof(PeriodStats));
    f.read((uint8_t*)lastWeekLabel, sizeof(lastWeekLabel));
    f.read((uint8_t*)lastMonthLabel, sizeof(lastMonthLabel));
    f.read((uint8_t*)lastYearLabel, sizeof(lastYearLabel));
    f.close();
}

// forward decl: getTimestamp is defined further down, but updateRecords below needs it
String getTimestamp();
extern time_t bootTime; // declared below; needed for uptime-days computation in updateRecords
extern char dailyVisitorsDate[11]; // declared below; needed for "Busiest day" record dating

// Hall of fame records: extremes observed over the device's lifetime
struct RecordVal {
    float value;
    char  at[24];   // ISO-8601 timestamp or YYYY-MM-DD for daily records
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
    emit("longest_uptime_d",  rec_longest_uptime_d,  true,  true);
    f.print("}");
    f.close();
    if (SD.exists(bak)) SD.remove(bak);
    if (SD.exists(path)) SD.rename(path, bak);
    SD.rename(tmp, path);
    if (SD.exists(bak)) SD.remove(bak);
}

static void parseRecordField(const String& json, const char* key, RecordVal& out) {
    int k = json.indexOf(String("\"") + key + "\":");
    if (k < 0) return;
    int v = json.indexOf("\"value\":", k);
    int at = json.indexOf("\"at\":\"", k);
    int end = json.indexOf("}", k);
    if (v < 0 || at < 0 || end < 0) return;
    out.value = json.substring(v + 8, json.indexOf(",", v)).toFloat();
    int atEnd = json.indexOf("\"", at + 6);
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
}

// Called from logStats every 5 min. Updates records if current values exceed stored extremes.
static void updateRecords(float temp_f, int co2, int dailyVis) {
    bool changed = false;
    String nowStr = getTimestamp();

    if (!rec_highest_co2.set || co2 > rec_highest_co2.value) {
        if (co2 > 0) {
            rec_highest_co2.value = co2;
            strncpy(rec_highest_co2.at, nowStr.c_str(), sizeof(rec_highest_co2.at) - 1);
            rec_highest_co2.at[sizeof(rec_highest_co2.at) - 1] = '\0';
            rec_highest_co2.set = true; changed = true;
        }
    }
    if (!isnan(temp_f)) {
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
    // longest uptime in days. Computed from wall-clock so it survives the 49.7-day millis()
    // rollover. bootTime is the unix timestamp captured at NTP sync; before NTP syncs it's 0
    // and we skip the record update rather than report garbage.
    int uptimeDays = 0;
    if (bootTime > 0) {
        time_t nowSec = time(nullptr);
        if (nowSec > bootTime) uptimeDays = (int)((nowSec - bootTime) / 86400);
    }
    if (uptimeDays > 0 && (!rec_longest_uptime_d.set || uptimeDays > rec_longest_uptime_d.value)) {
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

    if (lastWeekLabel[0] == '\0') {
        strncpy(lastWeekLabel, w, sizeof(lastWeekLabel) - 1);
        lastWeekLabel[sizeof(lastWeekLabel) - 1] = '\0';
    } else if (strcmp(lastWeekLabel, w) != 0) {
        flushPeriod("/stats/weekly", lastWeekLabel, currentWeek);
        resetPeriod(currentWeek);
        strncpy(lastWeekLabel, w, sizeof(lastWeekLabel) - 1);
        lastWeekLabel[sizeof(lastWeekLabel) - 1] = '\0';
    }

    if (lastMonthLabel[0] == '\0') {
        strncpy(lastMonthLabel, m, sizeof(lastMonthLabel) - 1);
        lastMonthLabel[sizeof(lastMonthLabel) - 1] = '\0';
    } else if (strcmp(lastMonthLabel, m) != 0) {
        flushPeriod("/stats/monthly", lastMonthLabel, currentMonth);
        resetPeriod(currentMonth);
        strncpy(lastMonthLabel, m, sizeof(lastMonthLabel) - 1);
        lastMonthLabel[sizeof(lastMonthLabel) - 1] = '\0';
    }

    if (lastYearLabel[0] == '\0') {
        strncpy(lastYearLabel, y, sizeof(lastYearLabel) - 1);
        lastYearLabel[sizeof(lastYearLabel) - 1] = '\0';
    } else if (strcmp(lastYearLabel, y) != 0) {
        flushPeriod("/stats/yearly", lastYearLabel, currentYear);
        resetPeriod(currentYear);
        strncpy(lastYearLabel, y, sizeof(lastYearLabel) - 1);
        lastYearLabel[sizeof(lastYearLabel) - 1] = '\0';
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
volatile int           pendingGuestbook    = 0;
int                    lastNotifiedPending = 0;
volatile bool          pendingNotifyFlag   = false;
char                   notifyEntryName[33]    = "";
char                   notifyEntryCountry[4]  = "";
char                   notifyEntryMessage[201]= "";
time_t                 bootTime            = 0;

volatile bool          pendingMaintenanceFlag     = false;
int                    pendingMaintenanceMinutes  = 0;
char                   pendingMaintenanceMessage[201] = "";
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
uint32_t               testEmailAtUnix            = 0;
bool                   testEmailPass              = false;
char                   testEmailDetail[129]       = "";

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

static bool consoleShouldSkip(const String& url) {
    if (url.startsWith("/admin")) return true;        // privacy: admin actions are never public
    if (url.startsWith("/_"))     return true;        // /_ws, /_upload, /_ota (internal)
    if (url.startsWith("/logs/")) return true;        // CSV data the charts fetch in the background
    if (url.startsWith("/.well-known/")) return true; // security scanners only
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
    // static assets
    if (url == "/favicon.png" || url == "/favicon.svg" || url == "/og-banner.jpg") return true;
    if (url == "/helloesp-framed.jpg" || url == "/helloesp-boot.mp4" || url == "/helloesp-boot-poster.jpg") return true;
    if (url == "/esp32-webserver.jpg" || url == "/esp32-webserver-bme280.jpg" || url == "/esp8266-webserver.jpg") return true;
    return false;
}

static void logConsole(AsyncWebServerRequest *req, int status) {
    String url = req->url();
    if (consoleShouldSkip(url)) return;
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
    // Default to "??", then copy the real code in the same scope as the String so we never
    // strncpy from a dangling c_str() after the String has gone out of scope.
    strncpy(e.country, "??", sizeof(e.country) - 1);
    if (req->hasHeader("CF-IPCountry")) {
        String cv = req->header("CF-IPCountry");
        if (cv.length() >= 2) {
            strncpy(e.country, cv.c_str(), sizeof(e.country) - 1);
        }
    }
    e.country[sizeof(e.country) - 1] = '\0';
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
#define MAX_PENDING_GUESTBOOK 100
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

void tallyCountry(const char* code) {
    if (!code || strlen(code) < 2) return;
    for (int i = 0; i < countryCount; i++) {
        if (strcmp(countries[i].code, code) == 0) {
            countries[i].count++;
            saveCountries();
            return;
        }
    }
    if (countryCount < MAX_COUNTRIES) {
        strncpy(countries[countryCount].code, code, 3);
        countries[countryCount].code[3] = '\0';
        countries[countryCount].count = 1;
        countryCount++;
        saveCountries();
    }
}

// Admin auth
bool isLocalIP(IPAddress ip) {
    return ip[0] == 10
        || (ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31)
        || (ip[0] == 192 && ip[1] == 168);
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

bool adminAuth(AsyncWebServerRequest *request) {
    if (!isLocalIP(request->client()->remoteIP()) || request->hasHeader("CF-Connecting-IP")) {
        AsyncWebServerResponse *r = request->beginResponse(SD, "/404.html", "text/html");
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
    SD.rename("/visitors.tmp", "/visitors.txt");
}

// Guestbook pending count
void countPendingGuestbook() {
    pendingGuestbook = 0;
    if (!SD.exists("/guestbook.csv")) return;
    File f = SD.open("/guestbook.csv", FILE_READ);
    if (!f) return;
    size_t sz = f.size();
    char* buf = (char*)malloc(sz + 1);
    if (!buf) { f.close(); return; }
    size_t n = f.read((uint8_t*)buf, sz);
    buf[n] = '\0';
    f.close();
    char* p = buf;
    while (*p) {
        char* nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len > 0) {
            char* lineEnd = p + len;
            // trim trailing \r
            while (lineEnd > p && (lineEnd[-1] == '\r' || lineEnd[-1] == ' ' || lineEnd[-1] == '\t')) lineEnd--;
            // find last comma
            char* c = lineEnd - 1;
            while (c >= p && *c != ',') c--;
            if (c >= p) {
                char* a = c + 1;
                while (a < lineEnd && (*a == ' ' || *a == '\t')) a++;
                if (lineEnd - a == 1 && *a == '0') pendingGuestbook++;
            }
        }
        if (!nl) break;
        p = nl + 1;
    }
    free(buf);
}

void notifyPendingIfIncreased() {
    if (pendingGuestbook <= lastNotifiedPending) {
        lastNotifiedPending = pendingGuestbook;
        return;
    }
    if (!wsConnected || !wsClient.connected()) return;
    String msg = "{\"type\":\"event\",\"event\":\"pending_guestbook\",\"count\":";
    msg += pendingGuestbook;
    if (notifyEntryName[0] != '\0') {
        msg += ",\"name\":\"";    msg += notifyEntryName;    msg += "\"";
        msg += ",\"country\":\""; msg += notifyEntryCountry; msg += "\"";
        msg += ",\"message\":\""; msg += notifyEntryMessage; msg += "\"";
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

    struct tm timeinfo;
    if (getLocalTime(&timeinfo)) {
        char today[12];
        strftime(today, sizeof(today), "%Y-%m-%d", &timeinfo);
        if (date == String(today)) {
            dailyVisitors = count;
            lastVisitorDay = timeinfo.tm_mday;
            // Preserve the date across reboots so the Hall of Fame "Busiest day" record stays
            // anchored to the day the visits actually accrued on.
            strncpy(dailyVisitorsDate, today, sizeof(dailyVisitorsDate) - 1);
            dailyVisitorsDate[sizeof(dailyVisitorsDate) - 1] = '\0';
            return;
        }
    }
    dailyVisitors = 0;
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
    SD.rename("/daily.tmp", "/daily.txt");
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
    String filename = getLogFilename();
    bool isNew = !SD.exists(filename);

    File log = SD.open(filename, FILE_APPEND);
    if (!log) return;

    if (isNew) {
        log.println("timestamp,cpu_temp_c,cpu_temp_f,memory_percent,temperature_c,temperature_f,humidity_percent,pressure_hpa,altitude_ft,co2_ppm,voc_ppb,heat_index_c,heat_index_f,rssi,sd_free_mb,requests_interval");
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
    log.println(intervalRequests);

    log.close();

    aggregateSample(currentWeek,  d.temp_c, d.humidity, cached_co2, cached_voc, intervalRequests);
    aggregateSample(currentMonth, d.temp_c, d.humidity, cached_co2, cached_voc, intervalRequests);
    aggregateSample(currentYear,  d.temp_c, d.humidity, cached_co2, cached_voc, intervalRequests);
    checkPeriodBoundaries();
    updateRecords(d.temp_f, cached_co2, dailyVisitors);
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

    String json;
    json.reserve(768);
    json  = "{";
    json += "\"response_ms\":" + String(responseMs) + ",";
    json += "\"timestamp\":\"" + getTimestamp() + "\",";
    json += "\"uptime\":\"" + uptime_formatter::getUptime() + "\",";
    json += "\"rssi\":" + String(d.rssi) + ",";
    json += "\"cpu_temp\":{\"celsius\":" + String(d.cpu_temp_c, 2) + ",\"fahrenheit\":" + String(d.cpu_temp_f, 2) + "},";
    json += "\"memory\":{";
    json += "\"used_bytes\":" + String(used_bytes) + ",";
    json += "\"used_kb\":" + String(used_bytes / 1024.0f, 2) + ",";
    json += "\"used_percent\":" + String(d.mem_percent, 2) + "},";
    json += "\"sd_used_mb\":" + String(d.sd_used_mb, 2) + ",";
    json += "\"sd_free_mb\":" + String(d.sd_free_mb, 2) + ",";
    json += "\"visitors\":" + String(visitors) + ",";
    json += "\"daily_visitors\":" + String(dailyVisitors) + ",";
    json += "\"temperature\":{\"celsius\":" + String(d.temp_c, 2) + ",\"fahrenheit\":" + String(d.temp_f, 2) + "},";
    json += "\"heat_index\":{\"celsius\":" + String(d.heat_index_c, 2) + ",\"fahrenheit\":" + String(d.heat_index_f, 2) + "},";
    json += "\"altitude_ft\":" + String(d.altitude_ft, 2) + ",";
    json += "\"humidity_percent\":" + String(d.humidity, 2) + ",";
    json += "\"pressure_hpa\":" + String(d.pressure_hpa, 2) + ",";
    json += "\"co2_ppm\":" + String(cached_co2) + ",";
    json += "\"voc_ppb\":" + String(cached_voc) + ",";
    json += "\"countries\":" + String(countryCount) + ",";
    json += "\"sensors\":{";
    json += "\"bme_ok\":" + String(bmeDegraded() ? "false" : "true") + ",";
    json += "\"ccs_ok\":" + String(ccsDegraded() ? "false" : "true");
    json += "}";
    json += "}";
    return json;
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

void buildAndSendBackup() {
    if (!wsConnected || !wsClient.connected()) return;

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
        if (hName.equalsIgnoreCase("Content-Type")) contentType = hVal;
        if (hName.equalsIgnoreCase("Content-Length")) contentLength = hVal.toInt();
        if (hName.equalsIgnoreCase("Cache-Control")) cacheControl = hVal;
    }

    // send metadata text frame
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
    meta += ",\"len\":";
    meta += contentLength;
    meta += "}";

    wsSendText(wsClient, meta);
    meta = "";

    // stream body as base64-encoded text frames (4KB raw = ~5.3KB b64 per chunk)
    const char* b64c = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    uint8_t chunk[3072];
    int totalSent = 0;
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
            totalSent += rd;
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

// WebSocket handshake
bool connectWorker() {
    if (strlen(cfgWorkerUrl) == 0 || strlen(cfgWorkerKey) == 0) {
        Serial.println("[ws] skipped: worker_url or worker_key blank");
        return false;
    }

    wsClient.setInsecure();

    Serial.printf("[ws] connecting to %s:443\n", cfgWorkerUrl);
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

    // Optional HMAC handshake. If the worker sends an auth_challenge within 2s,
    // we respond with HMAC-SHA256(device_key, nonce). If no challenge arrives
    // (worker has no HMAC_SECRET set), we proceed normally.
    unsigned long hmacWait = millis();
    while (!wsClient.available() && millis() - hmacWait < 2000) delay(10);
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

    // SD card
    bootLog("[fs] mounting sd");
    if (!SD.begin(SD_CS)) {
        Serial.println("SD card failed or not present");
        bootLog("[fs] sd FAIL");
        while (1) { delay(1000); }
    }
    Serial.println("SD card initialized");
    bootLog("[fs] sd ok");

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
                if (key == "wifi_ssid")   val.toCharArray(cfgSsid, sizeof(cfgSsid));
                if (key == "wifi_pass")   val.toCharArray(cfgWifiPass, sizeof(cfgWifiPass));
                if (key == "admin_user")  val.toCharArray(cfgAdminUser, sizeof(cfgAdminUser));
                if (key == "admin_pass")  val.toCharArray(cfgAdminPass, sizeof(cfgAdminPass));
                if (key == "worker_url")  val.toCharArray(cfgWorkerUrl, sizeof(cfgWorkerUrl));
                if (key == "worker_key")  val.toCharArray(cfgWorkerKey, sizeof(cfgWorkerKey));
                if (key == "device_key")  val.toCharArray(cfgDeviceKey, sizeof(cfgDeviceKey));
                if (key == "timezone")    val.toCharArray(cfgTimezone, sizeof(cfgTimezone));
            }
            bootLog("[fs] config loaded");
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
    // SD.usedBytes() scans the entire FAT and can take seconds on large cards;
    // the main loop refreshes it every 5 min, so skip the boot-time hit.
    cachedSdUsedMB = 0;
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
    loadCheckpoint();
    loadRecords();

    bootLog("[hw] init sensors");

    // Continue boot even if a sensor fails to init. The site is the higher-value
    // workload; safeBme*/cached_* fallbacks keep serving last-known values, and
    // the admin UI shows the degraded state.
    if (!bme.begin(0x76)) {
        Serial.println("BME280 init failed, check wiring.");
        bootLog("[hw] bme280 FAIL (continuing in degraded mode)");
        logError("hw", "bme280 init failed; continuing without temp/humidity/pressure");
    } else {
        Wire.setTimeOut(100);
        safeBmeTemp(); safeBmeHumidity(); safeBmePressureHpa(); safeBmeAltitudeFt();
        bootLog("[hw] bme280 ok");
    }

    if (!ccs.begin()) {
        Serial.println("CCS811 init failed, check wiring.");
        bootLog("[hw] ccs811 FAIL (continuing in degraded mode)");
        logError("hw", "ccs811 init failed; continuing without CO2/VOC");
    } else {
        bootLog("[hw] ccs811 ok");
    }
    // CCS811 warmup happens in the background; runtime reads check ccs.available() each cycle

    bootLog("[net] wifi connecting");
    WiFi.mode(WIFI_STA);
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
            ESP.restart();
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
        // SNTP daemon keeps retrying in background; boot continues
        Serial.println("NTP not yet synced at boot; background sync continues");
        bootLog("[net] ntp pending");
    } else {
        Serial.println("NTP synced: " + getTimestamp());
        if (getLocalTime(&timeinfo)) lastVisitorDay = timeinfo.tm_mday;
        bootTime = mktime(&timeinfo);
        loadDailyVisitors();
        bootLog("[net] ntp ok");
    }
    // Independent of NTP: loading the persisted backup date is a pure SD read. If it were
    // inside the NTP-success branch, a late NTP sync would leave lastBackupDate empty and
    // trigger a duplicate same-day backup.
    loadLastBackupDate();
    // Restore the last Worker-confirmed commit state so the admin UI doesn't show "never
    // confirmed" after a reboot between backup completion and receiving the ack event.
    loadLastCommit();

    // Routes

    server.on("/", HTTP_GET, [](AsyncWebServerRequest *request) {
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
            if (haveTime && timeinfo.tm_mday != lastVisitorDay) {
                dailyVisitors  = 0;
                lastVisitorDay = timeinfo.tm_mday;
            }
            dailyVisitors++;
            if (haveTime) {
                strftime(dailyVisitorsDate, sizeof(dailyVisitorsDate), "%Y-%m-%d", &timeinfo);
            }
            saveDailyVisitors();
            currentWeek.visitors++;
            currentMonth.visitors++;
            currentYear.visitors++;

            if (request->hasHeader("CF-IPCountry")) {
                String cc = request->header("CF-IPCountry");
                cc.trim();
                if (cc.length() == 0 || cc == "XX" || cc == "T1") cc = "??";
                tallyCountry(cc.c_str());
            } else {
                tallyCountry("??");
            }
        }

        digitalWrite(LED_PIN, HIGH);
        ledOn = true;
        lastRequestTime = millis();
        request->send(SD, "/index.html", "text/html");
        logConsole(request, 200);
    });

    server.on("/stats", HTTP_GET, [](AsyncWebServerRequest *request) {
        String json = buildStatsJson();
        request->send(200, "application/json", json);
        logConsole(request, 200);
    });

    server.on("/countries", HTTP_GET, [](AsyncWebServerRequest *request) {
        String json;
        json.reserve(512);
        json = "{";
        for (int i = 0; i < countryCount; i++) {
            if (i > 0) json += ",";
            json += "\"" + String(countries[i].code) + "\":" + String(countries[i].count);
        }
        json += "}";
        AsyncWebServerResponse *r = request->beginResponse(200, "application/json", json);
        // 60s edge cache. Country list updates only when a new country's first visitor arrives;
        // up to a minute of staleness is fine and drastically reduces refresh-spam load.
        r->addHeader("Cache-Control", "public, max-age=60");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/console", HTTP_GET, [](AsyncWebServerRequest *request) {
        AsyncWebServerResponse *r = request->beginResponse(SD, "/console.html", "text/html");
        r->addHeader("Cache-Control", "public, max-age=300");
        request->send(r);
        logConsole(request, 200);
    });

    server.on("/console.json", HTTP_GET, [](AsyncWebServerRequest *request) {
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
        rec("longest_uptime_d",  rec_longest_uptime_d,  true,  true);
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
            AsyncWebServerResponse *response = request->beginResponse(SD, f.path, f.type);
            response->addHeader("Cache-Control", "public, max-age=86400");
            request->send(response);
        });
    }

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
            "Disallow: /guestbook/submit\n"
            "Disallow: /guestbook/entries\n"
            "Disallow: /guestbook/pending\n"
            "Disallow: /guestbook/moderate\n"
            "Disallow: /countries\n"
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
            "  <url><loc>https://helloesp.com/changelog.rss</loc><changefreq>monthly</changefreq><priority>0.4</priority></url>\n"
            "  <url><loc>https://helloesp.com/guestbook.rss</loc><changefreq>daily</changefreq><priority>0.4</priority></url>\n"
            "</urlset>\n");
        r->addHeader("Cache-Control", "public, max-age=86400");
        request->send(r);
    });

    // keep this list in sync with the changelog section in index.html
    server.on("/changelog.rss", HTTP_GET, [](AsyncWebServerRequest *request) {
        String rss;
        rss.reserve(2048);
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
                // Close any handle left over from a prior aborted upload before reopening.
                if (uploadFile) uploadFile.close();
                String fullPath = path + filename;
                uploadFile = SD.open(fullPath, FILE_WRITE);
                if (!uploadFile) {
                    Serial.println("Upload failed to open: " + fullPath);
                    return;
                }
                Serial.println("Upload start: " + fullPath);
            }
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
            bool success = !Update.hasError();
            request->send(200, "text/plain", success ? "OTA success, rebooting..." : "OTA failed");
            if (success) {
                delay(500);
                ESP.restart();
            }
        },
        [](AsyncWebServerRequest *request, const String& filename, size_t index, uint8_t *data, size_t len, bool final) {
            // Upload chunks arrive before the completion handler runs, so auth MUST be checked
            // here too. Otherwise flash would be committed (Update.end) before the completion
            // handler gets a chance to reject an unauthenticated caller.
            if (!isLocalIP(request->client()->remoteIP()) || request->hasHeader("CF-Connecting-IP")) return;
            if (isAuthLockedOut((uint32_t)request->client()->remoteIP())) return;
            if (!request->authenticate(cfgAdminUser, cfgAdminPass)) return;

            if (index == 0) {
                Serial.println("OTA update start: " + filename);
                if (!Update.begin(UPDATE_SIZE_UNKNOWN)) {
                    Update.printError(Serial);
                }
            }
            if (Update.isRunning()) {
                if (Update.write(data, len) != len) {
                    Update.printError(Serial);
                }
            }
            if (final) {
                if (Update.end(true)) {
                    Serial.println("OTA complete: " + String(index + len) + " bytes");
                } else {
                    Update.printError(Serial);
                }
            }
        }
    );

    server.onNotFound([](AsyncWebServerRequest *request) {
        String url = request->url();

        if (url == "/history") {
            AsyncWebServerResponse *r = request->beginResponse(SD, "/history.html", "text/html");
            r->addHeader("Cache-Control", "public, max-age=3600");
            request->send(r);
            logConsole(request, 200);
            return;
        }

        if (url == "/history.json") {
            String json;
            json.reserve(2048);
            json = "{";
            const char* sections[] = {"weekly", "monthly", "yearly"};
            for (int s = 0; s < 3; s++) {
                if (s > 0) json += ",";
                json += "\""; json += sections[s]; json += "\":[";
                String dirPath = "/stats/" + String(sections[s]);
                bool first = true;
                if (SD.exists(dirPath)) {
                    File dir = SD.open(dirPath);
                    while (true) {
                        File f = dir.openNextFile();
                        if (!f) break;
                        String n = String(f.name());
                        if (n.endsWith(".json")) {
                            if (!first) json += ",";
                            json += "\"" + jsonEscape(n.substring(0, n.length() - 5)) + "\"";
                            first = false;
                        }
                        f.close();
                    }
                    dir.close();
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
            String path = "/stats/weekly/" + label + ".json";
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
            String path = "/stats/monthly/" + label + ".json";
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
            String body = "{\"week\":"  + periodToJson(lastWeekLabel,  currentWeek)
                       + ",\"month\":" + periodToJson(lastMonthLabel, currentMonth)
                       + ",\"year\":"  + periodToJson(lastYearLabel,  currentYear) + "}";
            AsyncWebServerResponse *r = request->beginResponse(200, "application/json", body);
            r->addHeader("Cache-Control", "public, max-age=300");
            request->send(r);
            return;
        }

        if (url == "/logs/today") {
            String filename = getLogFilename();
            if (SD.exists(filename)) {
                request->send(SD, filename, "text/csv");
            } else {
                AsyncWebServerResponse *r = request->beginResponse(SD, "/404.html", "text/html");
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
                        if (n.endsWith(".csv")) {
                            if (!first) json += ",";
                            json += "\"" + jsonEscape(n.substring(0, n.length() - 4)) + "\"";
                            first = false;
                        }
                        f.close();
                    }
                } else {
                    String n = String(yearDir.name());
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
                    request->send(SD, filename, "text/csv");
                } else {
                    AsyncWebServerResponse *r = request->beginResponse(SD, "/404.html", "text/html");
                    r->setCode(404);
                    request->send(r);
                }
                return;
            }
        }

        // guestbook
        if (url == "/guestbook") {
            request->send(SD, "/guestbook.html", "text/html");
            logConsole(request, 200);
            return;
        }

        // guestbook entries (paginated)
        if (url == "/guestbook/entries") {
            int page = request->hasParam("page") ? request->getParam("page")->value().toInt() : 1;
            if (page < 1) page = 1;
            int perPage = 20;

            if (!SD.exists("/guestbook.csv")) {
                request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}");
                return;
            }

            // count approved + unique countries in one pass
            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}"); return; }
            int totalApproved = 0;
            uint16_t seenCountries[256];
            int seenCountryCount = 0;
            while (f.available()) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                int c4 = line.lastIndexOf(',');
                if (c4 < 0) continue;
                String a = line.substring(c4 + 1); a.trim();
                if (a != "1") continue;
                totalApproved++;
                // extract country (field index 1, between first two commas)
                int c1 = line.indexOf(',');
                int c2 = line.indexOf(',', c1 + 1);
                if (c1 < 0 || c2 < 0 || c2 - c1 < 2) continue;
                String cc = line.substring(c1 + 1, c2); cc.trim();
                if (cc.length() < 2 || cc == "??") continue;
                uint16_t key = ((uint8_t)cc[0] << 8) | (uint8_t)cc[1];
                bool seen = false;
                for (int i = 0; i < seenCountryCount; i++) {
                    if (seenCountries[i] == key) { seen = true; break; }
                }
                if (!seen && seenCountryCount < 256) seenCountries[seenCountryCount++] = key;
            }
            f.close();

            // paginate newest first
            int skip = (page - 1) * perPage;
            if (skip >= totalApproved) {
                request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}");
                return;
            }
            int fromIdx = totalApproved - skip - perPage;
            int toIdx = totalApproved - skip - 1;
            if (fromIdx < 0) fromIdx = 0;
            bool hasMore = fromIdx > 0;

            // collect page
            f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}"); return; }
            int approvedIdx = 0;
            String json;
            json.reserve(2048);
            json = "{\"entries\":[";
            String pageEntries[20];
            int pageCount = 0;
            while (f.available()) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                int c4 = line.lastIndexOf(',');
                if (c4 < 0) continue;
                String a = line.substring(c4 + 1); a.trim();
                if (a != "1") continue;
                if (approvedIdx >= fromIdx && approvedIdx <= toIdx && pageCount < 20) {
                    pageEntries[pageCount++] = line;
                }
                approvedIdx++;
            }
            f.close();

            bool first = true;
            for (int i = pageCount - 1; i >= 0; i--) {
                int c1 = pageEntries[i].indexOf(',');
                int c2 = pageEntries[i].indexOf(',', c1 + 1);
                int c3 = pageEntries[i].indexOf(',', c2 + 1);
                int c4 = pageEntries[i].lastIndexOf(',');
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0) continue;
                if (!first) json += ",";
                json += "{\"time\":\"" + jsonEscape(pageEntries[i].substring(0, c1)) + "\",";
                json += "\"country\":\"" + jsonEscape(pageEntries[i].substring(c1 + 1, c2)) + "\",";
                json += "\"name\":\"" + jsonEscape(pageEntries[i].substring(c2 + 1, c3)) + "\",";
                json += "\"message\":\"" + jsonEscape(pageEntries[i].substring(c3 + 1, c4)) + "\"}";
                first = false;
            }
            json += "],\"hasMore\":" + String(hasMore ? "true" : "false");
            json += ",\"total\":" + String(totalApproved);
            json += ",\"countries\":" + String(seenCountryCount) + "}";
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
                        int cLast = line.lastIndexOf(',');
                        if (cLast < 0) continue;
                        String a = line.substring(cLast + 1); a.trim();
                        if (a != "1") continue;
                        ring[head] = line;
                        head = (head + 1) % 20;
                        if (count < 20) count++;
                    }
                    f.close();
                    // emit newest first
                    for (int k = 0; k < count; k++) {
                        int idx = (head - 1 - k + 20) % 20;
                        String& line = ring[idx];
                        int c1 = line.indexOf(',');
                        int c2 = line.indexOf(',', c1 + 1);
                        int c3 = line.indexOf(',', c2 + 1);
                        int c4 = line.lastIndexOf(',');
                        if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0) continue;
                        String t = line.substring(0, c1);
                        String country = line.substring(c1 + 1, c2);
                        String name = xmlEscape(line.substring(c2 + 1, c3));
                        String msg = xmlEscape(line.substring(c3 + 1, c4));
                        rss += "<item><title>";
                        rss += name;
                        if (country.length() > 0 && country != "??") { rss += " ("; rss += country; rss += ")"; }
                        rss += "</title><link>https://helloesp.com/guestbook</link>";
                        rss += "<guid isPermaLink=\"false\">gb-";
                        rss += t;
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
            // strip control chars and neutralize CSV delimiters
            auto sanitizeCsv = [](String &s) {
                String out;
                out.reserve(s.length());
                for (size_t i = 0; i < s.length(); i++) {
                    char c = s[i];
                    if ((unsigned char)c < 0x20 || c == 0x7F) continue;
                    if (c == '\\') continue;
                    if (c == ',') out += ' ';
                    else if (c == '"') out += '\'';
                    else out += c;
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

            String country = "??";
            if (request->hasHeader("CF-IPCountry")) {
                country = request->header("CF-IPCountry");
                sanitizeCsv(country);
                if (country.length() > 3) country = country.substring(0, 3);
                if (country.length() == 0) country = "??";
            }

            File f = SD.open("/guestbook.csv", FILE_APPEND);
            if (!f) {
                logError("sd", "guestbook.csv append failed");
                request->send(500, "text/plain", "Failed to save");
                return;
            }
            f.print(getTimestamp()); f.print(",");
            f.print(country);       f.print(",");
            f.print(name);          f.print(",");
            f.print(message);       f.println(",0");
            f.close();
            pendingGuestbook++;
            currentWeek.guestbook++;
            currentMonth.guestbook++;
            currentYear.guestbook++;
            strncpy(notifyEntryName,    name.c_str(),    sizeof(notifyEntryName) - 1);
            notifyEntryName[sizeof(notifyEntryName) - 1] = '\0';
            strncpy(notifyEntryCountry, country.c_str(), sizeof(notifyEntryCountry) - 1);
            notifyEntryCountry[sizeof(notifyEntryCountry) - 1] = '\0';
            strncpy(notifyEntryMessage, message.c_str(), sizeof(notifyEntryMessage) - 1);
            notifyEntryMessage[sizeof(notifyEntryMessage) - 1] = '\0';
            pendingNotifyFlag = true;

            request->send(200, "text/plain", "Thanks! Your message will appear after review.");
            logConsole(request, 200);
            return;
        }

        // guestbook admin (paginated)
        if (url == "/guestbook/pending") {
            if (!adminAuth(request)) return;
            int page = request->hasParam("page") ? request->getParam("page")->value().toInt() : 1;
            if (page < 1) page = 1;
            int perPage = 20;

            if (!SD.exists("/guestbook.csv")) {
                request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}");
                return;
            }

            // count entries
            File f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}"); return; }
            int totalEntries = 0;
            while (f.available()) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() > 0) totalEntries++;
            }
            f.close();

            int skip = (page - 1) * perPage;
            if (skip >= totalEntries) {
                request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}");
                return;
            }
            int fromIdx = totalEntries - skip - perPage;
            int toIdx = totalEntries - skip - 1;
            if (fromIdx < 0) fromIdx = 0;
            bool hasMore = fromIdx > 0;

            // collect page
            f = SD.open("/guestbook.csv", FILE_READ);
            if (!f) { request->send(200, "application/json", "{\"entries\":[],\"hasMore\":false}"); return; }
            String pageEntries[20];
            int pageIdxs[20];
            int pageCount = 0;
            int idx = 0;
            while (f.available()) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                if (idx >= fromIdx && idx <= toIdx && pageCount < 20) {
                    pageEntries[pageCount] = line;
                    pageIdxs[pageCount] = idx;
                    pageCount++;
                }
                idx++;
            }
            f.close();

            String json;
            json.reserve(2048);
            json = "{\"entries\":[";
            bool first = true;
            for (int i = pageCount - 1; i >= 0; i--) {
                int c1 = pageEntries[i].indexOf(',');
                int c2 = pageEntries[i].indexOf(',', c1 + 1);
                int c3 = pageEntries[i].indexOf(',', c2 + 1);
                int c4 = pageEntries[i].lastIndexOf(',');
                if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0) continue;
                String approved = pageEntries[i].substring(c4 + 1);
                approved.trim();
                if (!first) json += ",";
                json += "{\"idx\":" + String(pageIdxs[i]) + ",";
                json += "\"time\":\"" + jsonEscape(pageEntries[i].substring(0, c1)) + "\",";
                json += "\"country\":\"" + jsonEscape(pageEntries[i].substring(c1 + 1, c2)) + "\",";
                json += "\"name\":\"" + jsonEscape(pageEntries[i].substring(c2 + 1, c3)) + "\",";
                json += "\"message\":\"" + jsonEscape(pageEntries[i].substring(c3 + 1, c4)) + "\",";
                json += "\"approved\":" + approved + "}";
                first = false;
            }
            json += "],\"hasMore\":" + String(hasMore ? "true" : "false") + "}";
            request->send(200, "application/json", json);
            return;
        }

        // guestbook moderation
        if (url == "/guestbook/moderate" && request->method() == HTTP_POST) {
            if (!adminAuth(request)) return;
            if (!request->hasParam("idx", true) || !request->hasParam("status", true)) {
                request->send(400, "text/plain", "Missing idx or status");
                return;
            }
            int targetIdx = request->getParam("idx", true)->value().toInt();
            String newStatus = request->getParam("status", true)->value();
            if (newStatus != "0" && newStatus != "1" && newStatus != "2" && newStatus != "3") {
                request->send(400, "text/plain", "Invalid status");
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

            int idx = 0;
            while (f.available()) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                if (idx == targetIdx) {
                    if (newStatus == "3") {
                        idx++;
                        continue;
                    }
                    int c4 = line.lastIndexOf(',');
                    if (c4 >= 0) {
                        line = line.substring(0, c4 + 1) + newStatus;
                    }
                }
                out.println(line);
                idx++;
            }
            f.close();
            out.close();

            if (SD.exists("/guestbook.bak")) SD.remove("/guestbook.bak");
            if (SD.exists("/guestbook.csv")) SD.rename("/guestbook.csv", "/guestbook.bak");
            SD.rename("/guestbook.tmp", "/guestbook.csv");
            countPendingGuestbook();
            pendingNotifyFlag = true;

            request->send(200, "text/plain", "Updated");
            return;
        }

        // admin routes (safePath defined at file scope)
        if (url == "/admin") {
            if (!adminAuth(request)) return;
            request->send(SD, "/admin.html", "text/html");
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
            while (true) {
                File f = dir.openNextFile();
                if (!f) break;
                if (!first) json += ",";
                json += "{\"name\":\"" + jsonEscape(String(f.name())) + "\",";
                json += "\"size\":" + String(f.size()) + ",";
                json += "\"dir\":" + String(f.isDirectory() ? "true" : "false") + "}";
                first = false;
                f.close();
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
            json.reserve(512);
            json  = "{";
            json += "\"firmware\":\"" + String(FIRMWARE_VERSION) + "\",";
            json += "\"chip_model\":\"" + String(ESP.getChipModel()) + "\",";
            json += "\"chip_revision\":" + String(ESP.getChipRevision()) + ",";
            json += "\"cpu_freq_mhz\":" + String(ESP.getCpuFreqMHz()) + ",";
            json += "\"flash_size_mb\":" + String(ESP.getFlashChipSize() / (1024.0f * 1024.0f), 1) + ",";
            json += "\"sdk_version\":\"" + String(ESP.getSdkVersion()) + "\",";
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
                ccsOk ? (String(cached_co2) + " ppm CO2, " + String(cached_voc) + " ppb VOC")
                      : "sensor stale; CCS811 warms up over first 20 minutes");

            // 3. SD write/read cycle
            bool sdOk = false;
            String sdDetail = "SD test failed";
            {
                const char* testPath = "/_selftest.tmp";
                uint32_t token = (uint32_t)millis();
                File tf = SD.open(testPath, FILE_WRITE);
                if (tf) {
                    tf.println(token);
                    tf.close();
                    File rf = SD.open(testPath, FILE_READ);
                    if (rf) {
                        String line = rf.readStringUntil('\n');
                        line.trim();
                        rf.close();
                        if (line.toInt() == (int)token) { sdOk = true; sdDetail = "write+read+delete OK"; }
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

            // 5. NTP
            struct tm now;
            if (getLocalTime(&now, 100) && bootTime > 0) {
                addTest("NTP", "pass", "synced (" + getTimestamp() + ")");
            } else {
                addTest("NTP", "warn", "not yet synced; background daemon retrying");
            }

            // 6. Free heap
            uint32_t freeHeap = ESP.getFreeHeap();
            uint32_t minHeap = ESP.getMinFreeHeap();
            if (freeHeap >= 50000) {
                addTest("Free heap", "pass", String(freeHeap / 1024) + " KB free, " + String(minHeap / 1024) + " KB min ever");
            } else if (freeHeap >= 20000) {
                addTest("Free heap", "warn", String(freeHeap / 1024) + " KB free (close to 20 KB watchdog threshold)");
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
                time_t now = time(nullptr);
                long ageHours = (long)((now - (time_t)r2CommittedAtUnix) / 3600);
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
            ESP.restart();
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
            pendingMaintenanceFlag = true;
            if (mins == 0) {
                request->send(200, "text/plain", "Maintenance cancelled");
            } else {
                String reply = "Maintenance enabled for " + String(mins) + " min";
                request->send(200, "text/plain", reply);
            }
            return;
        }

        AsyncWebServerResponse *r = request->beginResponse(SD, "/404.html", "text/html");
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

// Main loop
void loop() {
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
            ESP.restart();
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
    int shiftY = 0;

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
            unsigned long secs = 0;
            if (bootTime > 0 && getLocalTime(&now)) secs = mktime(&now) - bootTime;
            else secs = millis() / 1000;
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
            float p = safeBmePressureHpa();
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
            snprintf(line, sizeof(line), "%sCO2: %d ppm", ccsBad ? "!" : "", cached_co2);
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

    if (ccs.available()) {
        ccs.setEnvironmentalData(safeBmeHumidity(), safeBmeTemp());
        if (!ccs.readData()) {
            cached_co2 = ccs.geteCO2();
            cached_voc = ccs.getTVOC();
            lastCcsGoodAt = millis();
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
            cachedSdUsedMB = (float)SD.usedBytes() / (1024.0f * 1024.0f);
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

    // HTTP handlers run on the AsyncTCP task; send WS frames from the main loop only
    if (pendingNotifyFlag) {
        pendingNotifyFlag = false;
        notifyPendingIfIncreased();
    }

    if (pendingMaintenanceFlag) {
        pendingMaintenanceFlag = false;
        if (wsConnected && wsClient.connected()) {
            String msg = "{\"type\":\"event\",\"event\":\"maintenance\",\"minutes\":";
            msg += pendingMaintenanceMinutes;
            msg += ",\"message\":\"";
            msg += jsonEscape(String(pendingMaintenanceMessage));
            msg += "\"}";
            wsSendText(wsClient, msg);
        }
    }

    // 5s SSE stats push: constant cost regardless of viewer count
    static unsigned long lastStatsPush = 0;
    if (wsConnected && wsClient.connected() && millis() - lastStatsPush > 5000) {
        lastStatsPush = millis();
        String body = buildStatsJson();
        String msg  = "{\"type\":\"event\",\"event\":\"stats_update\",\"data\":";
        msg += body;
        msg += "}";
        wsSendText(wsClient, msg);
    }

    // event-driven /console push: fires the instant a tracked request lands
    if (pendingConsolePush) {
        pendingConsolePush = false;
        if (wsConnected && wsClient.connected() && consoleCount > 0) {
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
    if (wsConnected && wsClient.connected() && millis() - lastBackupCheckMs > 60000UL) {
        lastBackupCheckMs = millis();
        if (isBackupDue()) pendingBackupFlag = true;
    }
    if (pendingBackupFlag) {
        pendingBackupFlag = false;
        buildAndSendBackup();
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

    if (strlen(cfgWorkerUrl) > 0) {
        if (wsConnected && wsClient.connected()) {
            while (wsClient.available()) {
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
            // exponential backoff: 5s, 10s, 20s, 40s, 80s, 160s, then capped at 300s
            unsigned long backoff = WS_RECONNECT_MS * (1UL << (wsReconnectFails < 6 ? wsReconnectFails : 6));
            if (backoff > 300000UL) backoff = 300000UL;
            if (millis() - lastWsAttempt > backoff) {
                lastWsAttempt = millis();
                wsClient.stop();
                if (connectWorker()) { wsReconnectFails = 0; wsReconnectCount++; }
                else if (wsReconnectFails < 10) wsReconnectFails++;
            }
        }
    }

    // heap safety net
    if (ESP.getFreeHeap() < 20000) {
        Serial.println("Heap critical, restarting...");
        char buf[48];
        snprintf(buf, sizeof(buf), "heap critical (%u bytes free), restarting", (unsigned)ESP.getFreeHeap());
        logError("heap", buf);
        delay(100);
        ESP.restart();
    }

    delay(500);
}
