// Copyright (c) 2022-2023 Kristian Kramer
// Distributed under the MIT software license, see the accompanying
// file LICENSE or https://opensource.org/license/mit/

#include "ESPAsyncWebServer.h"
#include "uptime_formatter.h"
#include "SPIFFS.h"

#ifdef ESP32
  #include "WiFi.h"
  int max_memory = 512000; // ESP32 max ram, change as necessary - value is in bytes
#else
  #include "ESP8266WiFi.h"
  int max_memory = 80000; // ESP8266 max ram, change as necessary - value is in bytes
#endif

AsyncWebServer server(80);

const char* ssid = "WIFI_SSID";
const char* password = "WIFI_PWD";

String HTML = R"rawliteral(
<html lang='en'>
    <head>
        <meta charset='UTF-8' />
        <meta name='viewport' content='width=device-width, initial-scale=1' />
        <link rel='stylesheet' href='https://pro.fontawesome.com/releases/v5.10.0/css/all.css' integrity='sha384-AYmEC3Yw5cVb3ZcuHtOA93w35dYTsvhLPVnYs9eStHfGJvOvKxVfELGroGkvsg+p' crossorigin='anonymous' />
        <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css' rel='stylesheet' integrity='sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC' crossorigin='anonymous'>
        <link href='https://fonts.googleapis.com/css2?family=Cabin+Condensed:wght@600;700&display=swap' rel='stylesheet' />
        <title>HelloESP - Hosted on an ESP32</title>
        <meta name='description' content='HelloESP is a website that is hosted on an ESP32 to demonstrate what you can do with an ESP32.' />
        <link rel='shortcut icon' href='data:image/png;base64,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' />
        <meta name='keywords' content='esp, esp32, esp8266, development, coding, programming' />
        <meta name='author' content='Kristian Kramer' />
        <meta name='theme-color' content='#2686e6' />
        <meta name='robots' content='index, follow' />

        <style>
            body {
                font-family: 'Cabin Condensed', sans-serif !important;
                background: #8e9eab;
                background: -webkit-linear-gradient(to right, #97a1a3, #8e9eab);
                background: linear-gradient(to right, #97a1a3, #8e9eab);
                margin: 0;
            }
            ul {
                list-style: none;
            }
            a {
                text-decoration: none !important;
            }
            /* Scroll bar */
            * {
                scrollbar-width: thin;
                scrollbar-color: whitesmoke rgba(38,134,230,1);
            }
            *::-webkit-scrollbar {
                width: 7px;
            }
            *::-webkit-scrollbar-track {
                background: transparent;
            }
            *::-webkit-scrollbar-thumb {
                background-color: rgba(38,134,230,1);
                border-radius: 20px;
            }
            ::selection {
                background: rgba(38,134,230,1);
                color: #fff;
            }
            ::-moz-selection {
                background: rgba(38,134,230,1);
                color:#fff;
            }
            ::-webkit-selection {
                background: rgba(38,134,230,1);
                color:#fff;
            }
            .HMP {
                margin-top: 32px;
                text-align: center;
                position: relative;
            }
            .HMP-title {
                position: relative;
                color: white;
                font-size: 45px;
                line-height: 1.5;
                margin-top: 10px;
            }
            .program {
                line-height: 0.1;
                font-size: 33px;
                color: #1e7bdf;
            }
            .box {
                margin: 10px;
                text-align: center;
            }
            .button:hover {
                transform: translateY(-4px);
            }
            .button:active {
                position: relative;
                top: 0.15em;
            }
            .button:focus {
                outline-color: #dedfde;
            }
            .author_link {
                color: lightgrey;
            }
            .author_link:hover {
                color: grey;
                transition: 0.3s;
            }
            .contact-section {
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                text-align: center;
                padding-top: 10px;
                color: white;
                background-color: rgba(44, 53, 128, 1) 100%;
            }
            .contact-links {
                display: flex;
                justify-content: center;
                max-width: 1180px;
                flex-wrap: wrap;
                padding-bottom: 20px;
            }
            .contact-details {
                font-size: 15px;
                transition: transform 0.3s ease-out;
                color: white !important;
            }
            .btn {
                padding: 1rem 1rem;
                margin: auto;
            }
            .border-top {
                border-top: 1px solid #dee2e6 !important;
            }
            .contact-details:hover {
                transform: translateY(-4px);
            }
            .stats_section {
                color: #fff;
                text-align: center;
            }
            .stats_section b {
                color: #cfa2cb;
            }
            .stats_container {
                background-color: rgb(238, 238, 238);
                padding: 1rem;
                border-radius: 0.5rem;
            }
            .grid_container {
                display: grid;
                grid-template-columns: repeat(1, 1fr);
                gap: 10px;
            }
            .stat_card {
                padding: 1rem;
                background-color: white;
                border-radius: 0.5rem;
                box-shadow: 0 0.2rem 0.5rem rgba(0, 0, 0, 0.15);
            }
            .stat_container {
                display: flex;
                flex-direction: column;
            }
            .stat_title {
                color: #6c757d;
            }
            .stat_content {
                font-size: 32px;
                font-weight: 700;
                color: black;
            }
            @media (min-width: 768px) {
                .grid_container {
                    grid-template-columns: repeat(3, 1fr);
                }
            }
            .stat_icon {
                color: grey;
                padding-bottom: 5px;
            }
            .updates_container {
                background-color: rgb(238, 238, 238, 0.8);
                padding: 1rem;
                border-radius: 0.5rem;
                box-shadow: 0 0.2rem 0.5rem rgba(0, 0, 0, 0.1);
            }
            .updates_content {
                padding-bottom: 15px;
            }
        </style>
    </head>
    <body>
        <div class='HMP'>
            <div class='HMP-title m-0 fw-bold'>HelloESP</div>
            <div class='program m-0'>Hosted on an ESP32</div>
            <br />
            <br />
            <blockquote class='box'>
                <p class='lead'>
                    This website is hosted on an ESP32 and used to be hosted on an ESP8266 to demonstrate what you can do with them!<br/>
                    Checkout the <a href='https://github.com/Tech1k/helloesp' target='_blank'>GitHub repo</a> for the code and feel free to contribute!
                </p>
            </blockquote>

            <section class='stats_section'>
                <div class='container'>
                    <div class='row'>
                        <center>
                            <div class='col-lg-8 col-lg-offset-2'>
                                <h2 class='section-heading'><i class='fas fa-analytics'></i> Statistics <label style='font-size: 18px;'>(updates every minute)</label></h2>
                                <div class='grid_container'>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <div class='stat_title'><i class='fas fa-hourglass stat_icon'></i> Uptime</div>
                                            <div class='stat_content' id='uptime'>Loading...</div>
                                        </div>
                                    </div>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <div class='stat_title'><i class='fas fa-memory stat_icon'></i> Memory Usage</div>
                                            <div class='stat_content' id='memory_usage'>Loading...</div>
                                        </div>
                                    </div>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <div class='stat_title'><i class='fas fa-users stat_icon'></i> Visitors</div>
                                            <div class='stat_content' id='visitors'>Loading...</div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </center>
                    </div>
                </div>
            </section>
        </div>
        <br />

        <center>
            <p class='is-size-6' style='font-size: 24px; font-weight: 700; margin-bottom: 5px;'><i class='icon far fa-images'></i> Photos</p>
            <div id='carouselExampleCaptions' class='carousel slide' data-bs-ride='carousel' style='max-width: 512px; border-radius: 0.5rem; overflow: hidden; box-shadow: 0 0.2rem 0.5rem rgba(0, 0, 0, 0.05);'>
                <div class='carousel-indicators'>
                    <button type='button' data-bs-target='#carouselExampleCaptions' data-bs-slide-to='0' class='active' aria-current='true' aria-label='Slide 1'></button>
                    <button type='button' data-bs-target='#carouselExampleCaptions' data-bs-slide-to='1' aria-label='Slide 2'></button>
                    <button type='button' data-bs-target='#carouselExampleCaptions' data-bs-slide-to='2' aria-label='Slide 3'></button>
                </div>
                <div class='carousel-inner'>
                    <div class='carousel-item active'>
                        <img src='/esp32-webserver.jpg' class='d-block w-100' alt='ESP32 Webserver' style='max-width: 512px; height: 280px;'>
                        <div class='carousel-caption d-none d-md-block'>
                            <p style='font-size: 16px; color: white;'>The ESP32 that is hosting this website, taken on 3/1/2023.</p>
                        </div>
                    </div>
                    <div class='carousel-item'>
                        <img src='/esp32-webserver-bme280.jpg' class='d-block w-100' alt='ESP32 Webserver' style='max-width: 512px; height: 280px;'>
                        <div class='carousel-caption d-none d-md-block'>
                            <p style='font-size: 16px; color: black;'>The ESP32 that is hosting this website, taken on 7/5/2022.</p>
                        </div>
                    </div>
                    <div class='carousel-item'>
                        <img src='/esp8266-webserver.jpg' class='d-block w-100' alt='ESP8266 Webserver' style='max-width: 512px; height: 280px;'>
                        <div class='carousel-caption '>
                            <p style='font-size: 16px; color: white;'>The ESP8266 that once hosted this website, taken on 6/27/2022.</p>
                        </div>
                    </div>
                </div>
                <button class='carousel-control-prev' type='button' data-bs-target='#carouselExampleCaptions' data-bs-slide='prev'>
                <span class='carousel-control-prev-icon' aria-hidden='true'></span>
                <span class='visually-hidden'>Previous</span>
                </button>
                <button class='carousel-control-next' type='button' data-bs-target='#carouselExampleCaptions' data-bs-slide='next'>
                <span class='carousel-control-next-icon' aria-hidden='true'></span>
                <span class='visually-hidden'>Next</span>
                </button>
            </div>
            <br/>
            <p class='is-size-6' style='font-size: 24px; font-weight: 700; margin-bottom: 5px;'><i class='icon far fa-newspaper'></i> Updates</p>
            <div style='height: 256px; max-width: 512px; overflow-x: hidden; overflow-y: auto;' class='updates_container'>
                <div class='updates_content' align='left'>
                    <strong>2/16/2023 - </strong>
                    Checkout my other project <a href='https://hellopico.net'>HelloPico</a> which is a similar website hosted on a Raspberry Pi Pico W!
                </div>

                <div class='updates_content' align='left'>
                    <strong>1/30/2023 - </strong>
                    I have published the final version of the site (I might add more stuff later), I hope you all enjoy this project!
                </div>

                <div class='updates_content' align='left'>
                    <strong>7/6/2022 - </strong>
                    I have added an image of the ESP32 with the BME280 sensor.
                </div>

                <div class='updates_content' align='left'>
                    <strong>7/5/2022 - </strong>
                    All images on this website are now hosted on the ESP32 and I have migrated to ESPAsyncWebServer!
                </div>

                <div class='updates_content' align='left'>
                    <strong>7/4/2022 - </strong>
                    The visitor counter is now completely hosted off of the ESP32 using the SPIFFs filesystem!
                </div>

                <div class='updates_content' align='left'>
                    <strong>7/2/2022 - </strong>
                    I have added temperature, altitude and humidity stats with readings from a BME280 sensor and soon I plan on running the ESP32 powering this website fully off of solar!
                </div>

                <div class='updates_content' align='left'>
                    <strong>6/29/2022 - </strong>
                    I have migrated the website over to an ESP32 for a performance boost and to add things such as a BME280 for temperature, pressure and altitude readings when it arrives!
                </div>

                <div class='updates_content' align='left'>
                    <strong>6/28/2022 - </strong>
                    I have started adding some statistics about the chip onto the website. I am still working on the CPU Usage, feel free to help contribute the project on the <a href='https://github.com/Tech1k/helloesp' target='_blank'>Github repo</a>.
                </div>

                <div class='updates_content' align='left'>
                    <strong>6/27/2022 - </strong>
                    HelloESP has launched, this is a little project of mine to show what can be done with an ESP8266/32 and to showcase other cool things with it! I hope you all find this to be as intresting as I did when I made this!
                </div>
            </div>
        </center>
        <br/>
        <footer id='contact' class='contact-section'>
            <div class='contact-section-header'>
                <p class='h5'>Made with <i class='fas fa-heart'></i> by <a href='https://kristiankramer.dev' target='_blank' class='author_link'>Kristian</a></p>
            </div>
            <div class='contact-links border-top'>
                <a href='https://kristiankramer.dev/donate' target='_blank' class='btn contact-details'><i class='fas fa-heart'></i> Donate</a>
                <a href='https://twitter.com/KristianJKramer' target='_blank' class='btn contact-details'><i class='fab fa-twitter'></i> Twitter</a>
                <a href='https://github.com/Tech1k/helloesp' class='btn contact-details'><i class='fab fa-github'></i> Contribute</a>
            </div>
        </footer>
    </body>
    <script>
        var xhttpUptime = new XMLHttpRequest();
        xhttpUptime.onload = function () {
            if (this.status === 200) {
                document.getElementById('uptime').innerHTML = this.responseText;
            }
        };
        xhttpUptime.open('GET', '/uptime', true);
        xhttpUptime.send();

        var xhttpMemoryUsage = new XMLHttpRequest();
        xhttpMemoryUsage.onload = function () {
            if (this.status === 200) {
                document.getElementById('memory_usage').innerHTML = this.responseText;
            }
        };
        xhttpMemoryUsage.open('GET', '/memory_usage', true);
        xhttpMemoryUsage.send();

        var xhttpVisitors = new XMLHttpRequest();
        xhttpVisitors.onload = function () {
            if (this.status === 200) {
                document.getElementById('visitors').innerHTML = this.responseText;
            }
        };
        xhttpVisitors.open('GET', '/visitors', true);
        xhttpVisitors.send();

        setInterval(function () {
            var xhttpUptimeAR = new XMLHttpRequest();
            xhttpUptimeAR.onload = function () {
                if (this.status === 200) {
                    document.getElementById('uptime').innerHTML = this.responseText;
                }
            };
            xhttpUptimeAR.open('GET', '/uptime', true);
            xhttpUptimeAR.send();
        }, 60000);

        setInterval(function () {
            var xhttpMemoryUsageAR = new XMLHttpRequest();
            xhttpMemoryUsageAR.onload = function () {
                if (this.status === 200) {
                    document.getElementById('memory_usage').innerHTML = this.responseText;
                }
            };
            xhttpMemoryUsageAR.open('GET', '/memory_usage', true);
            xhttpMemoryUsageAR.send();
        }, 59500);
    </script>
    <script src='https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js' integrity='sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM' crossorigin='anonymous'></script>
</html>
)rawliteral";

void setup() {

  Serial.begin(115200);
  WiFi.begin(ssid, password);

  while (WiFi.status() != WL_CONNECTED) {

    delay(500);
    Serial.println("Waiting to connect…");

  }

  Serial.println("Connected!");
  Serial.print("Local IP address: ");
  Serial.println(WiFi.localIP());


  server.on("/", HTTP_GET, [](AsyncWebServerRequest *request){

    if(!SPIFFS.begin(true)){
      Serial.println("An Error has occurred while mounting SPIFFS");
      return;
    }

    File visitors_read = SPIFFS.open("/visitors.txt");
 
    if(!visitors_read){
        Serial.println("Failed to open file for reading");
        return;
    }
 
    while(visitors_read.available()){

        int visitor_count = visitors_read.readString().toInt() + 1;

        //Serial.println(visitor_count);

        File count_file = SPIFFS.open("/visitors.txt", FILE_WRITE);
        if(!count_file){
          Serial.println("Failed to open file for reading");
          return;
        }

        if(count_file.println(visitor_count)){
          //Serial.println("File updated");
        } else {
          Serial.println("File update failed");
        }

        count_file.close();

        }
 
    visitors_read.close();

    request->send(200, "text/html", HTML);

  });


  server.on("/uptime", HTTP_GET, [](AsyncWebServerRequest *request) { // Uptime

    request->send(200, "text/html", uptime_formatter::getUptime());

  });

  server.on("/memory_usage", HTTP_GET, [](AsyncWebServerRequest *request) { // Memory usage

    int used_memory = max_memory - ESP.getFreeHeap();
    float memory_usage_kb_float = (float)used_memory / 1000;
    float memory_usage_float = (float)used_memory / max_memory * 100;
    int memory_usage_int = memory_usage_float;
    int memory_usage_kb_int = memory_usage_kb_float;

    String memory_usage = String(memory_usage_int);
    memory_usage.concat("% (");
    memory_usage.concat(String(memory_usage_kb_int));
    memory_usage.concat(" KB)");

    request->send(200, "text/html", String(memory_usage));

  });

  server.on("/visitors", HTTP_GET, [](AsyncWebServerRequest *request) { // Visitors 

    if(!SPIFFS.begin(true)){
      Serial.println("An Error has occurred while mounting SPIFFS");
      return;
    }
  
    File count_file = SPIFFS.open("/visitors.txt", "r");
    if(!count_file){
      Serial.println("Failed to open file for reading");
      return;
    }

    while(count_file.available()){

      request->send(200, "text/plain", String(count_file.readString()));

    }
    count_file.close();

  });


  // Images
  server.on("/og-banner.png", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(SPIFFS, "/og-banner.png", "image/png");
  });
  server.on("/esp32-webserver.jpg", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(SPIFFS, "/esp32-webserver.jpg", "image/jpg");
  });
  server.on("/esp32-webserver-bme280.jpg", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(SPIFFS, "/esp32-webserver-bme280.jpg", "image/jpg");
  });
  server.on("/esp8266-webserver.jpg", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(SPIFFS, "/esp8266-webserver.jpg", "image/jpg");
  });


  server.on("/robots.txt", HTTP_GET, [](AsyncWebServerRequest *request) { // Serve robots.txt

    String robots = "User-agent: *\n\nDisallow: /uptime\nDisallow: /cpu_usage\nDisallow: /memory_usage\nDisallow: /visitors\nDisallow: /temperature\nDisallow: /temperature_celsius\nDisallow: /altitude\nDisallow: /humidity\nDisallow: /pressure";
    request->send(200, "text/plain", robots);

  });

  server.onNotFound([](AsyncWebServerRequest *request) { // Serve 404 not found page on invalid paths
    request->send(404, "text/plain", "404 Not Found");
  });

  server.begin();
  Serial.println("Server listening");

  // Restart every 30 minutes
  Serial.println("Restarting in 30 minutes");
  delay(1800000);
  ESP.restart();
}

void loop() {

}
