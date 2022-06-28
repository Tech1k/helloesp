/*
  main.ino - Code for the ESP webserver
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

// TODO: add identifiers for ESP32
#include "ESP8266WiFi.h"
#include "ESP8266WebServer.h"
 
ESP8266WebServer server(80);

String HTML = R"rawliteral(
<html lang='en'>
    <head>
        <meta charset='UTF-8' />
        <meta name='viewport' content='width=device-width, initial-scale=1' />
        <link rel='stylesheet' href='https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.1.1/css/all.min.css' integrity='sha512-KfkfwYDsLkIlwQp6LFnl8zNdLGxu9YAA1QvwINks4PhcElQSvqcyVLLD9aMhXd13uQjoXtEKNosOWaZqXgel0g==' crossorigin='anonymous' referrerpolicy='no-referrer' />
        <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css' rel='stylesheet' integrity='sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC' crossorigin='anonymous' />
        <link href='https://fonts.googleapis.com/css2?family=Cabin+Condensed:wght@600;700&display=swap' rel='stylesheet' />
        <title>HelloESP - Hosted on an ESP8266</title>
        <meta name='description' content='HelloESP is a website hosted on an ESP8266 to demonstrate what you can do with an ESP8266.' />
        <link rel='shortcut icon' href='https://kk.dev/assets/images/helloesp-favicon.png' />
        <meta name='keywords' content='esp, esp32, esp8266, development, coding, programming' />
        <meta name='author' content='Kristian Kramer' />
        <meta name='theme-color' content='#2686e6' />
        <link rel='canonical' href='https://helloesp.com' />
        <meta name='robots' content='index, follow' />

        <meta name='twitter:card' content='summary_large_image' />
        <meta name='twitter:title' content='HelloESP' />
        <meta name='twitter:description' content='HelloESP is a website hosted on an ESP8266 to demonstrate what you can do with an ESP8266.' />
        <meta name='twitter:image' content='https://kk.dev/assets/images/helloesp-og-banner.png' />
        <meta name='twitter:site' content='@kristianjkramer' />
        <meta name='twitter:creator' content='@kristianjkramer' />

        <meta property='og:image' content='https://kk.dev/assets/images/helloesp-og-banner.png' />
        <meta property='og:image:width' content='955' />
        <meta property='og:image:height' content='500' />
        <meta property='og:description' content='HelloESP is a website hosted on an ESP8266 to demonstrate what you can do with an ESP8266.' />
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
                padding: 10px;
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
        </style>
    </head>
    <body>
        <div class='HMP'>
            <div class='HMP-title m-0 fw-bold'>HelloESP</div>
            <div class='program m-0'>Hosted on an ESP8266</div>
            <br />
            <br />
            <blockquote class='box'>
                <p class='lead'>
                    This site is currently hosted on an ESP8266 (I plan on moving it onto an ESP32 soon) with more things coming to this website soon!<br />
                    I am currently working on adding chip stats such as uptime and cpu usage and more as well as switching over to an ESP32 once it arrives.
                </p>
            </blockquote>
        </div>
        <br />

        <center>
            <div style='max-width: 512px;'>
                <img style='max-width: 100%; border-radius: 5px; overflow: hidden;' src='https://kk.dev/assets/images/esp8266-webserver.jpg' />
                <p>A photo of the ESP8266 running this website, taken on 6/27/2022.</p>
            </div>
        </center>

        <footer id='contact' class='contact-section'>
            <div class='contact-section-header'>
                <p class='h5'>Made by <a href='https://kk.dev' target='_blank' style='text-decoration: none; color: lightgrey;'>Kristian</a></p>
            </div>
            <div class='contact-links border-top'>
                <a href='https://kk.dev' target='_blank' class='btn contact-details'><i class='fas fa-globe'></i> kk.dev</a>
                <a href='https://kk.dev/donate' target='_blank' class='btn contact-details'><i class='fa-brands fa-codepen'></i> Donate</a>
                <a href='https://github.com/Tech1k' target='_blank' class='btn contact-details'><i class='fab fa-github'></i> GitHub</a>
                <a href='https://twitter.com/KristianJKramer' target='_blank' class='btn contact-details'><i class='fab fa-twitter'></i> Twitter</a>
                <a href='mailto:hello@kk.dev' class='btn contact-details'><i class='fas fa-at'></i> Email</a>
            </div>
        </footer>
    </body>
</html>
)rawliteral";

void setup() {
 
  Serial.begin(115200);
  WiFi.begin("WIFI_NETWORK", "WIFI_PWD");
 
  while (WiFi.status() != WL_CONNECTED) {
 
    delay(500);
    Serial.println("Waiting to connect…");
 
  }
 
  Serial.print("Local IP address: ");
  Serial.println(WiFi.localIP());
 
  //server.on("/status", []() {   // Code for other paths
 
  //  server.send(200, "text/html", HTML);
 
  //});
 
  server.on("/", handleRootPath);
  server.begin();
  Serial.println("Server listening");

  pinMode(0, OUTPUT); 
  pinMode(2, OUTPUT); 
}


void loop() {
 
  server.handleClient();

}

 
void handleRootPath() {
 
  server.send(200, "text/html", HTML);
 
}
