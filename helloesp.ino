/*
  helloesp.ino - Code for the ESP webserver

  Copyright (C) 2022 Kristian Kramer. All rights reserved.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

#include "ESPAsyncWebServer.h"
#include "uptime_formatter.h"
#include "SPIFFS.h"
#include "Wire.h"
#include "Adafruit_Sensor.h"
#include "Adafruit_BME280.h"

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

#define SEALEVELPRESSURE_HPA (1013.25)

Adafruit_BME280 bme;

float temperature, humidity, pressure, altitude;

String HTML = R"rawliteral(
<html lang='en'>
    <head>
        <meta charset='UTF-8' />
        <meta name='viewport' content='width=device-width, initial-scale=1' />
        <link rel='stylesheet' href='https://pro.fontawesome.com/releases/v5.10.0/css/all.css' integrity='sha384-AYmEC3Yw5cVb3ZcuHtOA93w35dYTsvhLPVnYs9eStHfGJvOvKxVfELGroGkvsg+p' crossorigin='anonymous' />
        <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css' rel='stylesheet' integrity='sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC' crossorigin='anonymous' />
        <link href='https://fonts.googleapis.com/css2?family=Cabin+Condensed:wght@600;700&display=swap' rel='stylesheet' />
        <title>HelloESP - Hosted on an ESP32</title>
        <meta name='description' content='HelloESP is a website that is hosted on an ESP32 to demonstrate what you can do with an ESP32.' />
        <link rel='shortcut icon' href='https://kk.dev/assets/images/helloesp-favicon.png' />
        <meta name='keywords' content='esp, esp32, esp8266, development, coding, programming' />
        <meta name='author' content='Kristian Kramer' />
        <meta name='theme-color' content='#2686e6' />
        <link rel='canonical' href='https://helloesp.com' />
        <meta name='robots' content='index, follow' />

        <meta name='twitter:card' content='summary_large_image' />
        <meta name='twitter:title' content='HelloESP' />
        <meta name='twitter:description' content='HelloESP is a website that is hosted on an ESP32 to demonstrate what you can do with an ESP32.' />
        <meta name='twitter:image' content='https://kk.dev/assets/images/helloesp-og-banner.png?v=2' />
        <meta name='twitter:site' content='@kristianjkramer' />
        <meta name='twitter:creator' content='@kristianjkramer' />

        <meta property='og:image' content='https://kk.dev/assets/images/helloesp-og-banner.png?v=2' />
        <meta property='og:image:width' content='955' />
        <meta property='og:image:height' content='500' />
        <meta property='og:description' content='HelloESP is a website that is hosted on an ESP32 to demonstrate what you can do with an ESP32.' />
        <meta property='og:title' content='HelloESP' />
        <meta property='og:site_name' content='HelloESP' />
        <meta property='og:url' content='https://helloesp.com/' />

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
                transform: translateY(4px);
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
                    This site is currently hosted on an ESP32 (used to be hosted on an ESP8266) with more things coming to this website soon!<br />
                    I am currently working on adding chip stats such as uptime and cpu usage and more, checkout the <a href='https://github.com/Tech1k/helloesp' target='_blank'>GitHub repo</a> for the code and feel free to contribute!
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
                                    <!--<div class='stat_card'>
                                        <div class='stat_container'>
                                            <i class='fas fa-microchip fa-3x stat_icon'></i>
                                            <div class='stat_content' id='cpu_usage'>coming soon</div>
                                            <div class='stat_title'>CPU Usage</div>
                                        </div>
                                    </div>-->
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

                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <div class='stat_title'><i class='far fa-thermometer-half stat_icon'></i> Temperature (±2°F)</div>
                                            <div class='stat_content' id='temperature'>Loading...</div>
                                            <div class='stat_content' style='font-size: 16px; margin-top: -10px;' id='temperature_celsius'>Loading...</div>
                                        </div>
                                    </div>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <div class='stat_title'><i class='fas fa-clouds stat_icon'></i> Altitude (±1 hPa or 3.3 ft)</div>
                                            <div class='stat_content' id='altitude'>Loading...</div>
                                            <div class='stat_content' style='font-size: 16px; margin-top: -10px;' id='pressure'>Loading...</div>
                                        </div>
                                    </div>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <div class='stat_title'><i class='fas fa-tint stat_icon'></i> Humidity (±3%)</div>
                                            <div class='stat_content' id='humidity'>Loading...</div>
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
            <div style='max-width: 512px;'>
                <img style='max-width: 100%; border-radius: 0.5rem; overflow: hidden; box-shadow: 0 0.2rem 0.5rem rgba(0, 0, 0, 0.05);' src='https://kk.dev/assets/images/esp8266-webserver.jpg' />
                <p>A photo of the ESP8266 running this website, taken on 6/27/2022.</p>
            </div>

            <p class='is-size-6' style='font-size: 24px; font-weight: 700; margin-bottom: 5px;'><i class='icon far fa-newspaper'></i> Updates</p>
            <div style='height: 256px; max-width: 512px; overflow-x: hidden; overflow-y: auto;' class='updates_container'>
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
                <p class='h5'>Made with <i class='fas fa-heart'></i> by <a href='https://kk.dev' target='_blank' class='author_link'>Kristian</a></p>
            </div>
            <div class='contact-links border-top'>
                <a href='https://kk.dev' target='_blank' class='btn contact-details'><i class='fas fa-globe'></i> kk.dev</a>
                <a href='https://kk.dev/donate' target='_blank' class='btn contact-details'><i class='fas fa-heart'></i> Donate</a>
                <a href='https://github.com/Tech1k' target='_blank' class='btn contact-details'><i class='fab fa-github'></i> GitHub</a>
                <a href='https://twitter.com/KristianJKramer' target='_blank' class='btn contact-details'><i class='fab fa-twitter'></i> Twitter</a>
                <a href='https://github.com/Tech1k/helloesp' class='btn contact-details'><i class='fas fa-code'></i> Contribute</a>
            </div>
        </footer>
    </body>
    <script>
        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('uptime').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/uptime', true);
        xhttp.send();

        /*var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('cpu_usage').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/cpu_usage', true);
        xhttp.send();*/

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('memory_usage').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/memory_usage', true);
        xhttp.send();

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('visitors').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/visitors', true);
        xhttp.send();

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('temperature').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/temperature', true);
        xhttp.send();

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('temperature_celsius').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/temperature_celsius', true);
        xhttp.send();

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('humidity').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/humidity', true);
        xhttp.send();

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('pressure').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/pressure', true);
        xhttp.send();

        var xhttp = new XMLHttpRequest();
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                document.getElementById('altitude').innerHTML = this.responseText;
            }
        };
        xhttp.open('GET', '/altitude', true);
        xhttp.send();

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('uptime').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/uptime', true);
            xhttp.send();
        }, 60000);

        /*setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('cpu_usage').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/cpu_usage', true);
            xhttp.send();
        }, 60000);*/

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('memory_usage').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/memory_usage', true);
            xhttp.send();
        }, 60000);

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('visitors').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/visitors', true);
            xhttp.send();
        }, 60000);

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('temperature').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/temperature', true);
            xhttp.send();
        }, 60000);

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('humidity').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/humidity', true);
            xhttp.send();
        }, 60000);

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('temperature_celsius').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/temperature_celsius', true);
            xhttp.send();
        }, 60000);

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('pressure').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/pressure', true);
            xhttp.send();
        }, 60000);

        setInterval(function () {
            var xhttp = new XMLHttpRequest();
            xhttp.onreadystatechange = function () {
                if (this.readyState == 4 && this.status == 200) {
                    document.getElementById('altitude').innerHTML = this.responseText;
                }
            };
            xhttp.open('GET', '/altitude', true);
            xhttp.send();
        }, 60000);
    </script>
</html>
)rawliteral";

void setup() {

  Serial.begin(115200);
  WiFi.begin(ssid, password);

  bme.begin(0x76);

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

  //server.on("/cpu_usage", HTTP_GET, [](AsyncWebServerRequest *request) {   // TODO: CPU usage

    //request->send(200, "text/html", cpu_usage);

  //});

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

  server.on("/visitors", HTTP_GET, [](AsyncWebServerRequest *request) { // Uptime

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

  server.on("/temperature", HTTP_GET, [](AsyncWebServerRequest *request) { // Temperature

    temperature = bme.readTemperature() * 9/5 + 32;
    request->send(200, "text/html", String(temperature) + "°F");

  });

  server.on("/temperature_celsius", HTTP_GET, [](AsyncWebServerRequest *request) { // Temperature in celsius

    temperature = bme.readTemperature();
    request->send(200, "text/html", String(temperature) + "°C");

   });

  server.on("/altitude", HTTP_GET, [](AsyncWebServerRequest *request) { // Altitude

    altitude = bme.readAltitude(SEALEVELPRESSURE_HPA) * 3.28;
    request->send(200, "text/html", String(altitude) + "ft");

  });

  server.on("/humidity", HTTP_GET, [](AsyncWebServerRequest *request) { // Humidity

    humidity = bme.readHumidity();
    request->send(200, "text/html", String(humidity) + "%");

  });

  server.on("/pressure", HTTP_GET, [](AsyncWebServerRequest *request) { // Atmospheric Pressure

    pressure = bme.readPressure() / 100.0F;
    request->send(200, "text/html", String(pressure) + "hPa");

  });


  // Images
  server.on("/favicon.png", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(SPIFFS, "/favicon.png", "image/png");
  });
  server.on("/og-banner.png", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(SPIFFS, "/og-banner.png", "image/png");
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
}

void loop() {

}
