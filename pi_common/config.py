"""The `key: value` config format every client on the Pi reads.

Was split across pi_common/config.py and sensorVPD/configReader.py, which is
the same file boundary the repo's "PI sensor" / "Pi Actuator" split created and
the flat Pi layout removed.
"""

import json
import os
from pathlib import Path

from .identity import get_device_uuid


def parse_text_config(text):
    """Plain-text config, one `key: value` per line:

        Type: DHT22
        Location: Inside Dome 1
        GPIO: D17

    Split on the FIRST colon only, so a value containing one survives. Blank
    lines and # comments are skipped. Values stay strings - each caller does
    its own conversion, because what needs converting differs between a sensor
    (Period -> float) and an actuator (Poll, MaxRun -> int).

    A line with no colon is not a setting, and is ignored rather than guessed
    at.
    """
    data = {}

    for raw in text.splitlines():
        line = raw.strip()

        if not line or line.startswith("#"):
            continue

        key, separator, value = line.partition(":")

        if not separator:
            continue

        key = key.strip()

        if key:
            data[key] = value.strip()

    return data


def load_config_data(path):
    """Read a config file as a dict.

    JSON is tried first, so files in either format work and Pis can be
    converted one at a time rather than all at once.
    """
    with open(path, "r") as file:
        raw = file.read()

    try:
        data = json.loads(raw)
    except ValueError:
        return parse_text_config(raw)

    if not isinstance(data, dict):
        raise ValueError(
            f"{path}: expected a set of settings, got {type(data).__name__}"
        )

    return data


def to_number(value, fallback):
    """Convert to the same type as `fallback`, or return `fallback`.

    Config values are strings, and a typo in a hand-edited file should not stop
    a Pi from starting - it should fall back to the documented default.
    """
    try:
        return type(fallback)(value)
    except (TypeError, ValueError):
        return fallback


# ===========================================================================
# SENSOR CONFIG  (was sensorVPD/configReader.py)
# ===========================================================================

BASE_DIR = Path(__file__).resolve().parent.parent

# One UUID per physical Pi, deliberately shared between dht22.py and C5A.py:
# the backend identifies a sensor by (deviceUUID, type, location), so the same
# UUID with different types registers as different sensors.
UUID_FILE = Path(os.environ.get("SENSOR_UUID_FILE", BASE_DIR / "device_uuid.txt"))

# config.txt is per-sensor, and WHICH file a script reads is declared by the
# SCRIPT - CONFIG_FILE at the top of dht22.py / C5A.py. That is what lets one
# Pi run both sensors, and it is why a bench run reads the same file the
# service does instead of whatever the environment happened to hold.
#
# $SENSOR_CONFIG survives as an override, for pointing a single run at a test
# file. It beats the script's own name; see resolve_config_path().
SENSOR_CONFIG_ENV = os.environ.get("SENSOR_CONFIG")

# Only reached by a script that names no config of its own. On a Pi that also
# runs the mist maker this is the MISTER's file - exactly the collision the
# per-script CONFIG_FILE exists to prevent.
FALLBACK_CONFIG_FILE = "config.txt"

# Used only when the backend is unreachable and no period has ever been polled.
DEFAULT_PERIOD_SECONDS = 5

# Days to keep a reading in the local cache AFTER the backend has acknowledged
# it. 0 deletes on upload.
#
# Not zero by default, and not server-controlled like the sampling intervals.
# What retention protects against is the backend losing rows it already
# acknowledged - a restore from an old dump, or a Pi pointed at the test
# database - and in every one of those cases the server is the unreliable
# party, so it is the wrong place to keep the switch. One day keeps almost all
# of the SD-card saving (the write volume, not the space, is the real cost)
# while a same-day mistake is still recoverable from the Pi.
DEFAULT_CLEANUP_DAYS = 1


def source_key(config):
    """Stable LOCAL identity of one sensor: "type|location".

    The two fields that, with the device UUID, are `uq_sensor_identity` in the
    database - so two sensors that would collide here are already the same row
    in `Sensor` and are not a configuration that means anything.

    Local is the whole point. It comes from config.txt, so it survives being
    pointed at a different backend, which the server's numeric sensorID does
    not. cache.get_unsent() filters on it; see the reasoning there.
    """
    return f"{config['sensorType']}|{config['locationName']}"


def resolve_config_path(filename=None):
    # Order matters. The environment overrides the script, not the reverse:
    # flipped, a script's own CONFIG_FILE would silently beat $SENSOR_CONFIG
    # and there would be no way to point one run at a different file.
    path = Path(SENSOR_CONFIG_ENV or filename or FALLBACK_CONFIG_FILE)

    if not path.is_absolute():
        # Prefer the file next to the scripts; fall back to the cwd copy.
        candidate = BASE_DIR / path
        path = candidate if candidate.exists() else path

    return path


def readConfig(filename=None):
    """Read the sensor's config.

    Accepts either JSON or `key: value` text - the parsing is shared with the
    actuator tree (pi_common.config), so the two cannot disagree about what a
    config file is. What stays here is what the SENSORS need out of it.
    """
    path = resolve_config_path(filename)

    config_data = load_config_data(path)

    sensor_type = config_data.get("Type") or config_data.get("sensorType") or config_data.get("type")
    location = config_data.get("Location") or config_data.get("locationName") or config_data.get("location")
    gpio = config_data.get("GPIO") or config_data.get("gpio")
    description = config_data.get("description") or config_data.get("Description") or config_data.get("desc")

    # Local fallback only. The PC is the authority on the sampling period
    # (POST /api/schedule); this just keeps a Pi sampling sensibly before it
    # has ever reached the backend.
    period = config_data.get("Period") or config_data.get("period") or config_data.get("periodSeconds")

    try:
        period = float(period) if period is not None else DEFAULT_PERIOD_SECONDS
    except (TypeError, ValueError):
        period = DEFAULT_PERIOD_SECONDS

    # Per-Pi, unlike the sampling and upload intervals - see
    # DEFAULT_CLEANUP_DAYS. to_number() rather than a bare int() so a typo
    # falls back to the documented default instead of stopping the service.
    cleanup_days = config_data.get("Retention") or config_data.get("cleanupDays")
    cleanup_days = (DEFAULT_CLEANUP_DAYS if cleanup_days is None
                    else max(0, to_number(cleanup_days, DEFAULT_CLEANUP_DAYS)))

    # /api/registerSensor rejects a payload without these, and a sensor that
    # never registers just caches locally forever. Failing here names the file
    # instead of leaving a 400 to be traced back from the dashboard.
    if not sensor_type or not location:
        raise ValueError(
            f"{path}: 'Type' and 'Location' are required "
            f"(got Type={sensor_type!r}, Location={location!r})"
        )

    return {
        "deviceUUID": get_device_uuid(UUID_FILE),
        "sensorType": sensor_type,
        "locationName": location,
        "gpio": gpio,
        "description": description,
        "periodSeconds": period,
        "cleanupDays": cleanup_days,
    }
