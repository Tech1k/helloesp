"""
Pre-build step: gzip HTML assets in data/ so the firmware can serve them
with Content-Encoding: gzip. Under LAN-direct load a 90+KB HTML stream can
saturate LWIP's pbuf pool and cascade into WS write failures; serving the
~22KB gzipped version keeps the pool clear.

Runs automatically before every `pio run` via platformio.ini's extra_scripts.
Also runnable standalone: `python3 scripts/gzip_assets.py`.

Skip-current check is content-based, not mtime-based: compares the source
file's CRC32 to the CRC32 stored in the existing .gz's gzip trailer (which
records the uncompressed data's CRC32 by spec). This sidesteps an mtime
comparison failing when something else touches the .gz timestamp without
changing its content.

Removes orphan .gz files whose source HTML has been deleted.

The firmware's beginResponseGzipOrRaw() helper falls back to the uncompressed
file if no .gz is present, so this script is a performance optimization, not
a hard dependency.
"""

import gzip
import os
import shutil
import zlib

try:
    Import("env")
    _PROJECT_DIR = env.get("PROJECT_DIR")
except NameError:
    # Standalone CLI mode: project root is one level up from this script
    _PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _file_crc32(path):
    crc = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            crc = zlib.crc32(chunk, crc)
    return crc & 0xffffffff


def _gzip_trailer_crc32(gz_path):
    """Read CRC32 of uncompressed data from gzip trailer (last 8 bytes:
    4 bytes CRC32 + 4 bytes ISIZE, little-endian, per RFC 1952)."""
    with open(gz_path, "rb") as f:
        f.seek(-8, 2)
        return int.from_bytes(f.read(4), "little")


def gzip_html_assets():
    data_dir = os.path.join(_PROJECT_DIR, "data")
    if not os.path.isdir(data_dir):
        return

    entries = os.listdir(data_dir)
    html_files = {
        f for f in entries
        if f.endswith(".html") and os.path.isfile(os.path.join(data_dir, f))
    }

    # Sweep before regen so an interrupted prior run is cleaned up even if
    # THIS run aborts mid-loop. Handles orphan .gz (source HTML deleted) and
    # stale .tmp (hard-killed run; the atomic-write path normally clears
    # these via the except handler below).
    orphans = 0
    for filename in entries:
        path = os.path.join(data_dir, filename)
        if filename.endswith(".html.gz.tmp"):
            try:
                os.remove(path)
                print("gzip: removed stale %s" % filename)
                orphans += 1
            except OSError:
                pass
            continue
        if not filename.endswith(".html.gz"):
            continue
        if filename[:-3] in html_files:
            continue
        try:
            os.remove(path)
            print("gzip: removed orphan %s" % filename)
            orphans += 1
        except OSError:
            pass

    count = 0
    for filename in sorted(html_files):
        src_path = os.path.join(data_dir, filename)
        gz_path = src_path + ".gz"
        if os.path.exists(gz_path):
            try:
                if _gzip_trailer_crc32(gz_path) == _file_crc32(src_path):
                    continue
            except (OSError, ValueError):
                # Malformed or unreadable .gz: regenerate
                pass
        # Write to .tmp then rename so a Ctrl+C mid-compress doesn't leave a
        # half-written .gz that looks current on the next run. mtime=0 in the
        # gzip header makes output a deterministic function of input bytes so
        # rebuilding from the same source produces identical checksums.
        tmp_path = gz_path + ".tmp"
        try:
            with open(src_path, "rb") as src, open(tmp_path, "wb") as raw, \
                 gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9, mtime=0) as dst:
                shutil.copyfileobj(src, dst)
            os.replace(tmp_path, gz_path)
        except BaseException:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise
        src_size = os.path.getsize(src_path)
        gz_size = os.path.getsize(gz_path)
        pct = (100.0 * gz_size / src_size) if src_size else 0
        print("gzip %s -> %s.gz (%d -> %d bytes, %.0f%%)" %
              (filename, filename, src_size, gz_size, pct))
        count += 1

    if count == 0 and orphans == 0:
        print("gzip: all .html.gz up to date")


gzip_html_assets()
