# <img src="https://github.com/Tech1k/helloesp/blob/master/images/helloesp-favicon.png" alt="HelloESP" width="64"/> HelloESP - A website hosted on an ESP32/8266
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Donate](https://img.shields.io/badge/Support%20me-Donate-blue)](https://kk.dev/donate)

<img src="https://github.com/Tech1k/helloesp/blob/master/images/esp8266-webserver.jpg" alt="ESP8266 Webserver" max-width="100%"/>

A website hosted on an ESP8266.


## Notes
- ``index.html`` is the HTML in a more editable state for the site in ``helloesp.ino``


## TODO
- [x] Add identifiers for the ESP32 to allow cross-compatibility
- [x] Add uptime
- [ ] Add CPU usage
- [x] Add memory usage
- [x] Add visitor counter
- [ ] Make visitor counter locally hosted on the ESP either via a txt file or sqlite db stored in the onboard flash memory
- [ ] Format uptime better - only display if number is above 0 (e.g. if 0 days, hide days)
- [ ] Add BMP388 for temperature, pressure and altitude


## Branches
``master`` - Master branch compatible with the ESP32/8266 without any external hardware

``bmp388`` - Code for the ESP32 with the BMP822 module for altitude and temperature readings


## External Libraries
- Uptime Library by Yiannis Bourkelis

## Contribute
I welcome contributions to this project; please feel free to contribute to this repository by providing documentation to code, submitting issue reports, enhancement requests and pull requests.
