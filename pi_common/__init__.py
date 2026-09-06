"""Everything every Pi client shares.

ONE LIB FOLDER. This was two - `sensorVPD` in the sensor tree and `pi_common`
next to it - and that boundary only ever existed because the repo splits code
into "PI sensor" and "Pi Actuator" to say which files go to which device. The
Pi itself is flat, so the split does not survive deployment and neither should
the lib.

Merging them removed the seams it had grown: config parsing was in both
(`config.py` + `configReader.py`), backend discovery was in both
(`discovery.py` + `network.py`), and clock handling was in both (`clock.py` +
`timesync.py`).

    C5A.py             wind sensor
    dht22.py           temperature / humidity sensor
    relay_control.py   mist maker      (+ mist_trigger.py, one pulse per run)
    fan_control.py     fan
    device_agent.py    liveness + clock, on every Pi
    sync_clock.py      boot-time clock step, root, once
    pi_common/         this

Every one of those is a CLIENT and lives outside this folder. Nothing in here
runs on its own.

NO INSTALL STEP. A plain lib folder sitting next to the scripts that import it;
both `python script.py` and `python -m pkg.mod` put that directory first on
sys.path.

WHAT IS SENSOR-ONLY, AND STAYS SO
`cache`, `client`, `samplingLoop`, `scheduler` and `vpd` are imported only by
the sensor scripts. They live here because the folder is the Pi's one lib, not
because the mister needs them - an actuator deliberately has no offline cache,
no clock sync, no tick scheduler and no background thread, because a command is
not data.
"""

# --- shared by every client ---
from . import clock
from . import config
from . import discovery
from . import identity

from .clock import Clock, probe, read_boot_id, read_uptime
from .config import parse_text_config, load_config_data, readConfig, source_key, to_number
from .discovery import find_backend, network_search, resolve_network_list
from .identity import get_device_uuid

# --- sensors only ---
from . import cache
from . import client
from . import samplingLoop
from . import scheduler
from . import vpd

from .client import BackendClient
from .samplingLoop import SamplingLoop
from .scheduler import TickScheduler
from .vpd import vpd_kpa

__all__ = [
    "clock", "config", "discovery", "identity",
    "Clock", "probe", "read_boot_id", "read_uptime",
    "parse_text_config", "load_config_data", "readConfig", "source_key", "to_number",
    "find_backend", "network_search", "resolve_network_list",
    "get_device_uuid",
    "cache", "client", "samplingLoop", "scheduler", "vpd",
    "BackendClient", "SamplingLoop", "TickScheduler", "vpd_kpa",
]
