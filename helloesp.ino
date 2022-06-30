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

#ifdef ESP32
  #include "WiFi.h"
  #include "WebServer.h"
#else
  #include "ESP8266WiFi.h"
  #include "ESP8266WebServer.h"
#endif

#include "uptime_formatter.h"

#ifdef ESP32
  WebServer server(80);
  int max_memory = 512000; // ESP32 max ram, change as necessary - value is in bytes
#else
  ESP8266WebServer server(80);
  int max_memory = 80000; // ESP8266 max ram, change as necessary - value is in bytes
#endif

const char* ssid = "WIFI_SSID";
const char* password = "WIFI_PWD";

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
                transform: translateY(4px);
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
                                            <i class='fas fa-hourglass fa-3x stat_icon'></i>
                                            <div class='stat_content' id='uptime'>Loading...</div>
                                            <div class='stat_title'>Uptime</div>
                                        </div>
                                    </div>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <i class='fas fa-microchip fa-3x stat_icon' ></i>
                                            <div class='stat_content' id='cpu_usage'>In development</div>
                                            <div class='stat_title'>CPU Usage</div>
                                        </div>
                                    </div>
                                    <div class='stat_card'>
                                        <div class='stat_container'>
                                            <i class='fas fa-memory fa-3x stat_icon'></i>
                                            <div class='stat_content' id='memory_usage'>Loading...</div>
                                            <div class='stat_title'>Memory Usage</div>
                                        </div>
                                    </div>
                                </div>
                                <div class='stat_content' id='visitors'>Visitors: Loading...</div>
                            </div>
                        </center>
                    </div>
                </div>
            </section>
        </div>
        <br />

        <center>
            <div style='max-width: 512px;'>
                <img style='max-width: 100%; border-radius: 5px; overflow: hidden;' src='https://kk.dev/assets/images/esp8266-webserver.jpg' />
                <p>A photo of the ESP8266 running this website, taken on 6/27/2022.</p>
            </div>

            <p class='is-size-6' style='font-size: 24px; font-weight: 700; margin-bottom: 5px;'><i class='icon far fa-newspaper'></i> Updates</p>
            <div style='height: 256px; max-width: 512px; overflow-x: hidden; overflow-y: auto;' class='updates_container'>
                <div class='updates_content' align='left'>
                    <strong>6/29/2022 - </strong>
                    I have migrated the website over to an ESP32 for a performance boost and to add things such as a BMP388 for temperature, pressure and altitude readings when it arrives!
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
                document.getElementById('visitors').innerHTML = 'Visitors: ' + this.responseText;
            }
        };
        xhttp.open('GET', 'https://kk.dev/helloesp_visitors', true);
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
                    document.getElementById('visitors').innerHTML = 'Visitors: ' + this.responseText;
                }
            };
            xhttp.open('GET', 'https://kk.dev/helloesp_visitors', true);
            xhttp.send();
        }, 60000);
    </script>
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

  server.on("/uptime", []() { // Uptime

    server.send(200, "text/html", uptime_formatter::getUptime());

  });

  //server.on("/cpu_usage", []() {   // TODO: CPU usage

    //server.send(200, "text/html", HTML);

  //});

  server.on("/memory_usage", []() { // Memory usage

    int used_memory = max_memory - ESP.getFreeHeap();
    float memory_usage_kb_float = (float)used_memory / 1000;
    float memory_usage_float = (float)used_memory / max_memory * 100;
    int memory_usage_int = memory_usage_float;
    int memory_usage_kb_int = memory_usage_kb_float;

    String memory_usage = String(memory_usage_int);
    memory_usage.concat("% (");
    memory_usage.concat(String(memory_usage_kb_int));
    memory_usage.concat(" KB)");

    server.send(200, "text/html", String(memory_usage));

  });

  server.on("/", handleRootPath);

  server.on("/robots.txt", handleRobots); // Serve robots.txt

  server.onNotFound(handleNotFound); // Serve 404 not found page on invalid paths

  server.begin();
  Serial.println("Server listening");
}

void loop() {

  server.handleClient();

}

void handleRootPath() {

  server.send(200, "text/html", HTML);

}

void handleRobots() {

  String error_message = "User-agent: *\n\nDisallow: /uptime\nDisallow: /cpu_usage\nDisallow: /memory_usage";
  server.send(404, "text/plain", error_message);

}

void handleNotFound() {

  String error_message = "404 Not Found";
  server.send(404, "text/plain", error_message);

}
