"""Clock measurement, and the file the device agent publishes it to.

The Pis have no RTC and no internet. The backend PC is the time source, reached
over the same LAN link that already carries sensor data.

WHO MEASURES, WHO READS

One process per Pi measures the offset - the device agent - and publishes it
here. Every sensor client reads it. Before this, each sensor process probed the
backend itself, so a Pi with two sensors ran two independent syncs and could
stamp two different offsets onto readings taken on the same tick grid.

WHAT IS DELIBERATELY NOT DONE

The system clock is never stepped while anything is scheduled. `sync_clock.py`
steps it once at boot, as root, before the sensor services start - that is what
makes logs and file mtimes sane. After that the offset is applied in-process
only. A backward step under a running scheduler would produce duplicate or
skipped ticks, and `UNIQUE (sensorID, datetime)` would absorb the duplicates:
data loss that looks like nothing happened.
"""

import json
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

import requests

BOOT_ID_PATH = "/proc/sys/kernel/random/boot_id"
UPTIME_PATH = "/proc/uptime"

# systemd creates and cleans this up via RuntimeDirectory=sp_dashboard, so the
# agent never has to mkdir and a stale file cannot outlive the service. /run is
# tmpfs, so it clears on reboot - an offset measured before a power cut must
# never be trusted afterwards.
CLOCK_FILE = Path(os.environ.get("SP_CLOCK_FILE", "/run/sp_dashboard/clock.json"))

DEFAULT_SAMPLES = 10
DEFAULT_TIMEOUT = 5.0

# Cristian's algorithm assumes symmetric delay. Server-side processing breaks
# that assumption, and the backend PC is a slow box whose event loop can stall
# on a large /api/logs response. A bad sync is worse than a slightly stale one,
# so anything slower than this is rejected and the previous offset is kept.
MAX_ACCEPTABLE_RTT = 0.100  # seconds

# Five missed agent cycles at 60 s. Long enough that one failed probe or a brief
# network blip does not flap timeConfidence, short enough that a stranded Pi
# stops claiming SYNCED well before its drift matters.
MAX_PUBLISHED_AGE = 300.0  # seconds


def read_boot_id():
    """Kernel boot id - a fresh UUID on every boot. Identifies which power-on a
    cached reading, or a published offset, belongs to."""
    try:
        with open(BOOT_ID_PATH, "r") as f:
            return f.read().strip()
    except Exception:
        return None


def read_uptime():
    """Seconds since boot, or None. The server turns this into a boot time."""
    try:
        with open(UPTIME_PATH, "r") as f:
            return float(f.read().split()[0])
    except Exception:
        return None


def probe(base_url, samples=DEFAULT_SAMPLES, timeout=DEFAULT_TIMEOUT,
          max_rtt=MAX_ACCEPTABLE_RTT, session=None):
    """Measure the offset to the backend clock.

    Cristian's algorithm, which cancels the network delay by assuming it is
    symmetric::

        t0     = local clock before the request
        t1     = PC clock from the response body
        t3     = local clock after the response
        rtt    = t3 - t0
        offset = t1 - t0 - rtt/2

    Several samples are taken and the one with the LOWEST round trip is kept - a
    fast exchange is the one least distorted by queueing delay. This is what NTP
    does. On a LAN it lands within a few milliseconds.

    Returns ``(offset_seconds, rtt_seconds)``, or ``None`` if every sample
    failed or the best one was still too slow.
    """
    if not base_url:
        return None

    http = session or requests
    url = f"{base_url}/api/time"
    best = None

    for _ in range(samples):
        try:
            t0 = time.time()
            r = http.get(url, timeout=timeout)
            t3 = time.time()

            if r.status_code != 200:
                continue

            epoch_ms = r.json().get("epochMs")

            if epoch_ms is None:
                continue

            t1 = epoch_ms / 1000.0
            rtt = t3 - t0
            offset = t1 - t0 - rtt / 2.0

            if best is None or rtt < best[1]:
                best = (offset, rtt)

        except Exception:
            continue

        # A sample this fast cannot be improved on meaningfully; stop early so
        # the agent's cycle is not held up.
        if best is not None and best[1] < 0.005:
            break

    if best is None:
        return None

    if best[1] > max_rtt:
        return None

    return best


def publish(offset, rtt, path=None, boot_id=None):
    """Write the measured offset for the sensor clients to read.

    ATOMIC. A sensor reading this mid-write would get truncated JSON on the tick
    path, so it is written to a temp file in the same directory and renamed -
    os.replace() is atomic on POSIX. Same pattern relay_control.py uses for
    mist_state.json.

    `syncedAtMonotonic` is a monotonic reading, not a wall-clock one: the reader
    is comparing it against its own time.monotonic() to judge staleness, and
    monotonic is the only clock that is trustworthy for that here.
    """
    path = Path(path or CLOCK_FILE)

    payload = {
        "offset": offset,
        "rtt": rtt,
        "syncedAtMonotonic": time.monotonic(),
        "bootID": boot_id if boot_id is not None else read_boot_id(),
    }

    directory = path.parent

    try:
        fd, temp_path = tempfile.mkstemp(dir=str(directory), prefix=".clock-")

        with os.fdopen(fd, "w") as f:
            json.dump(payload, f)

        os.replace(temp_path, path)
        return True

    except OSError as e:
        print(f"[agent] cannot publish clock to {path}: {e}")
        return False


def load_published(path=None, max_age=MAX_PUBLISHED_AGE, boot_id=None):
    """Read the agent's published offset, or None if it cannot be trusted.

    Rejected, in order:

    * no file - the agent is not running, or has not completed a sync yet
    * a different bootID - the file survived a reboot somehow, so the offset was
      measured against a clock this boot never saw
    * older than `max_age` - the agent is running but has not reached the
      backend recently, so the offset has been drifting unmeasured

    Returns ``(offset, rtt)`` on success.
    """
    path = Path(path or CLOCK_FILE)

    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None

    expected = boot_id if boot_id is not None else read_boot_id()

    if expected is not None and data.get("bootID") != expected:
        return None

    synced_at = data.get("syncedAtMonotonic")

    if synced_at is None or time.monotonic() - synced_at > max_age:
        return None

    offset = data.get("offset")

    if offset is None:
        return None

    return offset, data.get("rtt")


# ===========================================================================
# THE APP'S VIEW OF THE TIME  (was sensorVPD/timesync.py)
# ===========================================================================

# Matches the format already stored in MySQL.
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

SYNCED = "SYNCED"
CORRECTED = "CORRECTED"
ESTIMATED = "ESTIMATED"
UNKNOWN = "UNKNOWN"


def step_system_clock(epoch_seconds):
    """Step the OS clock. Root only - meant for the boot-time oneshot service.

    ``systemd-timesyncd`` is disabled first: it is currently failing in the
    background trying to reach public NTP servers, and can refuse or overwrite
    our setting. Both calls are purely local syscalls.
    """
    target = datetime.fromtimestamp(epoch_seconds).strftime("%Y-%m-%d %H:%M:%S")

    try:
        subprocess.run(
            ["timedatectl", "set-ntp", "false"],
            check=False, capture_output=True, timeout=10,
        )
        result = subprocess.run(
            ["timedatectl", "set-time", target],
            check=False, capture_output=True, timeout=10,
        )

        if result.returncode != 0:
            print("timedatectl set-time failed:", result.stderr.decode(errors="replace").strip())
            return False

        return True

    except Exception as e:
        print("Clock step failed:", e)
        return False


class Clock:
    """The app's view of the true time.

    ``now()`` is ``time.time() + offset``. Nothing here ever touches the system
    clock, so a sync arriving mid-sleep cannot disturb a running schedule.

    Also records the (monotonic, epoch) pair captured at the first successful
    sync of this boot. The monotonic clock is continuous and correct-rate
    within a boot, so readings taken before the sync can have their true time
    reconstructed exactly afterwards::

        true_epoch(m) = ref_epoch + (m - ref_monotonic)
    """

    def __init__(self):
        self.offset = 0.0
        self.rtt = None
        self.synced = False
        self.last_sync_monotonic = None
        self.boot_id = read_boot_id()
        # Captured once, at the first successful sync of this boot.
        self.ref_monotonic = None
        self.ref_epoch = None

    # -- time ------------------------------------------------------------

    def now(self):
        return time.time() + self.offset

    @staticmethod
    def monotonic():
        return time.monotonic()

    def timestamp(self, epoch=None):
        """Local-time string, matching the format already stored in MySQL."""
        return datetime.fromtimestamp(
            self.now() if epoch is None else epoch
        ).strftime(TIMESTAMP_FORMAT)

    @property
    def confidence(self):
        return SYNCED if self.synced else ESTIMATED

    def age_seconds(self):
        if self.last_sync_monotonic is None:
            return None
        return time.monotonic() - self.last_sync_monotonic

    # -- syncing ---------------------------------------------------------

    def refresh(self):
        """Adopt the offset the device agent published. No network.

        Returns ``(offset, rtt)`` when a usable one was found, ``None``
        otherwise - and ``None`` is not an error, it is the honest state of a Pi
        whose agent has not reached the backend recently.

        On failure the PREVIOUS offset is kept, because a slightly stale offset
        beats none at all, but ``synced`` goes False so the reading is stamped
        ESTIMATED rather than SYNCED.

        That decay is new, and it fixes a real bug: ``synced`` used to be set
        once and never cleared, so a Pi that synced at boot and then lost the
        network for six hours went on claiming SYNCED for every row.
        """
        result = load_published(boot_id=self.boot_id)

        if result is None:
            self.synced = False
            return None

        offset, rtt = result

        # Keyed on ref_monotonic, not on `synced`, because `synced` now flaps
        # as the published file ages in and out of date. The reference pair is
        # captured exactly once per boot, at the first usable offset.
        first_sync = self.ref_monotonic is None

        self.offset = offset
        self.rtt = rtt
        self.synced = True
        self.last_sync_monotonic = time.monotonic()

        if first_sync:
            # Order matters: read monotonic and epoch as close together as
            # possible so the reference pair is self-consistent.
            self.ref_monotonic = time.monotonic()
            self.ref_epoch = self.now()

        return offset, rtt

    def reference(self):
        """``(ref_monotonic, ref_epoch)`` for back-correcting cached rows, or
        ``None`` if this boot has never synced."""
        if self.ref_monotonic is None:
            return None
        return self.ref_monotonic, self.ref_epoch
