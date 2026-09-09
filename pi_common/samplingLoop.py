"""The sampling loop itself, written once.

dht22.py and C5A.py were the same program twice: 124 identical lines out of
dht22.py's 176, covering startup, the tick grid, timestamp and confidence
stamping, latency instrumentation, the cache write and the shutdown path. The
two scripts genuinely differ in exactly three things - how you open the device,
how you read one sample, and how you print it - and everything else was
duplication waiting to drift.

WHY THIS MATTERS MORE THAN TIDINESS

timeSyncPlan.md is explicit that a reading must be stamped with its TICK rather
than with read completion: a DHT22 read takes about a second and a C5A read
takes a different amount, so stamping at completion bakes a permanent bias
between sensor types into the data and defeats the whole exercise. That
guarantee used to be upheld by two copies of the same eight lines plus a
comment in C5A.py saying "see dht22.py". A third sensor meant a third copy.

Now it exists once, and adding a sensor type is: read one sample, describe it.

WHAT IS DELIBERATELY NOT SHARED

The retry POLICY. DHT22 fails often and retries against a time budget so a
shorter period cannot push a read past its own tick; the C5A retries a fixed
number of times. Two policies are provided below and the caller picks one - the
loop is shared, the policy is not.
"""

import signal
import threading
import time

from . import cache
from . import config as configlib
from .client import BackendClient
from .clock import ESTIMATED, SYNCED
from .scheduler import TickScheduler

# Fraction of the period a read is allowed to spend before it has to give up.
# Leaves the rest of the tick for the cache write and the next tick's setup.
RETRY_BUDGET_FRACTION = 0.6

# The three fields every sensor produces. Anything else in a sample dict is
# passed to cache.save_reading() as a keyword, which is how the C5A adds
# windspeed and windDirection without this module knowing what wind is.
CORE_FIELDS = ("temperature", "humidity", "vpd")


def retry_within_budget(read_sample, budget_seconds, stop_event, spacing_seconds):
    """Keep retrying until the budget runs out. DHT22's policy.

    Returns (sample, error). Raising all the way out would drop the tick
    silently; an exhausted budget is recorded instead.

    `spacing_seconds` is waited on the stop_event rather than slept, so SIGTERM
    during a retry gap stops the process now instead of at the end of the gap.
    """
    deadline = time.monotonic() + budget_seconds
    last_error = None

    while True:
        try:
            return read_sample(), None
        except Exception as e:
            last_error = e

        if time.monotonic() + spacing_seconds > deadline:
            return None, last_error

        if stop_event.wait(spacing_seconds):
            return None, last_error


def retry_fixed(read_sample, budget_seconds, stop_event, attempts):
    """A fixed number of attempts, still bounded by the budget. C5A's policy."""
    deadline = time.monotonic() + budget_seconds
    last_error = None

    for _ in range(attempts):
        try:
            return read_sample(), None
        except Exception as e:
            last_error = e

        if time.monotonic() >= deadline:
            break

    return None, last_error


class SamplingLoop:
    """Everything a sensor script needs except the sensor.

    Construction does the shared startup - cache, config, backend client, clock,
    tick scheduler, signal handlers - and then stops, so the caller can open its
    hardware using self.config before run() takes over.

    `config_file` is the config this sensor reads, named by the calling script
    (CONFIG_FILE in dht22.py / C5A.py) rather than by the systemd unit. None
    falls back to $SENSOR_CONFIG and then to config.txt - see
    pi_common.config.resolve_config_path().
    """

    def __init__(self, stream, period_default=5, config_file=None):
        cache.init_db()

        self.stream = stream
        self.config = configlib.readConfig(config_file)
        self.stop_event = threading.Event()

        self.client = BackendClient(self.config, stream=stream)
        self.clock = self.client.clock
        self.scheduler = TickScheduler(
            self.clock,
            self.config.get("periodSeconds", period_default),
            stop_event=self.stop_event,
        )

        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    def _handle_stop(self, signum, frame):
        print("Stopping...")
        self.stop_event.set()

    def _apply_pending_schedule(self):
        update = self.client.take_schedule()

        if update is None:
            return

        period, effective_from = update

        if self.scheduler.schedule_period_change(period, effective_from):
            print(f"Sampling period -> {period}s at "
                  f"{self.clock.timestamp(effective_from)}")

    def run(self, read_with_retries, describe, label=""):
        """Sample on every tick until stopped.

        read_with_retries(budget_seconds) -> (sample, error)
            `sample` is a dict carrying at least temperature, humidity and vpd.
            Any other key is passed straight to cache.save_reading().
            None means the read failed; `error` says why.

        describe(sample) -> str
            The sensor-specific middle of the console line. The timestamp
            prefix and the [confidence jitter read] suffix are added here, so
            the instrumentation reads identically for every sensor type.

        label
            Printed next to the timestamp, e.g. the GPIO pin. Empty for a
            sensor that has nothing useful to add.
        """
        tag = f" ({label})" if label else ""

        while not self.stop_event.is_set():
            self._apply_pending_schedule()

            tick = self.scheduler.wait_for_next_tick()

            if tick is None:
                break

            if tick.skipped:
                print(f"Warning: {tick.skipped} tick(s) skipped - "
                      f"read overran the period")

            # Stamped with the TICK, not with read completion. A DHT22 read
            # takes ~1 s and a C5A read a different amount; stamping at
            # completion would bake a permanent bias between sensor types into
            # the data. The real duration is stored separately as readLatencyMs.
            timestamp = self.clock.timestamp(tick.epoch)
            confidence = SYNCED if self.clock.synced else ESTIMATED
            # The sync in force for THIS reading - captured at the tick, because
            # the background thread may resync before the row is uploaded.
            sync_rtt_ms = (None if self.clock.rtt is None
                           else int(round(self.clock.rtt * 1000)))

            read_started = time.monotonic()
            sample, error = read_with_retries(tick.period * RETRY_BUDGET_FRACTION)
            read_latency_ms = int(round((time.monotonic() - read_started) * 1000))

            if sample is None:
                print(f"{timestamp}{tag} Reading error:", error)
                continue

            extra = {k: v for k, v in sample.items() if k not in CORE_FIELDS}

            try:
                cache.save_reading(
                    timestamp,
                    sample["temperature"],
                    sample["humidity"],
                    sample["vpd"],
                    sensor_id=self.client.sensor_id,
                    stream=self.stream,
                    boot_id=self.client.boot_id,
                    monotonic=tick.monotonic,
                    tick_epoch=tick.epoch,
                    time_confidence=confidence,
                    read_latency_ms=read_latency_ms,
                    tick_jitter_ms=tick.jitter_ms,
                    sync_rtt_ms=sync_rtt_ms,
                    # Which sensor wrote this row, so the flush can tell it
                    # from the one the other process on this Pi is writing.
                    source_key=self.client.source_key,
                    **extra,
                )

                print(f"{timestamp}{tag} {describe(sample)} "
                      f"[{confidence} jitter={tick.jitter_ms}ms "
                      f"read={read_latency_ms}ms]")

            except Exception as e:
                print(f"{timestamp}{tag} Cache write failed:", e)

        # No shutdown notice here: that is a DEVICE event and the device agent
        # owns it. Stopping one sensor service does not mean the Pi went away.
        self.client.stop()
