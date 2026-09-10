"""All backend traffic, on a background thread.

Everything network-related lives here so the sampling loop stays real-time.
That matters concretely: ``network_search`` blocks up to 5 s *per IP* in
``networkList.txt``, which on a 5 s sampling period would miss ticks by itself.
The tick loop must never wait on the network.

``dht22.py`` and ``C5A.py`` previously duplicated ~80 lines of this each; both
now share it.

Thread safety: every ``cache`` call opens its own SQLite connection and the
existing ``timeout=10`` covers write contention, so the maintenance thread and
the tick loop can both touch the cache.
"""

import threading
import time

import requests

from . import cache
from . import config as configlib
from . import discovery
from .clock import Clock, read_boot_id, read_uptime

DEFAULT_NETWORK_LIST = "networkList.txt"
DEFAULT_PORT = 5000

# FIXED, and deliberately not configurable. Discovery, registration, clock sync,
# the schedule poll and - on the device agent - the heartbeat all ride this
# cadence, and the backend's offline detector is built on the same number:
# server.js sets OFFLINE_AFTER_MS = 3 * HEARTBEAT_INTERVAL_MS. Make this the
# upload interval and setting uploads to 5 minutes marks every Pi OFFLINE,
# which looks like a network fault rather than a config change. The upload
# interval is send_seconds, below, and it is a separate due-time.
DEFAULT_MAINTENANCE_SECONDS = 60

# How often the cache is drained. Server-controlled (SamplingConfig.sendSeconds)
# because the request was to change it from the dashboard; 60 s here is only the
# value in force before the first successful schedule poll, and it matches what
# used to be hardcoded.
#
# Uploads need no coordination between Pis - unlike the sampling period, which
# must switch on a shared instant - so there is no effectiveFrom and Pis
# staggering their uploads is mildly good, spreading the write load.
DEFAULT_SEND_SECONDS = 60

# Mirrors MIN/MAX_SEND_SECONDS in the backend's config.js, which is where a bad
# value is actually rejected. Clamped here as well so a Pi cannot be driven into
# a per-row upload loop by a backend that skipped its own validation.
#
# The ceiling is not arbitrary: one cycle accumulates sendSeconds/periodSeconds
# rows and drains at most batch_size * max_batches_per_cycle = 2000, so at the
# fastest legal sampling (2 s) a cycle stops keeping up somewhere near 4000 s.
# 3600 keeps every legal combination inside the drain capacity, with margin.
MIN_SEND_SECONDS = 5
MAX_SEND_SECONDS = 3600

# How soon to try discovery again when there is no backend at all. The normal
# cycle is a minute, which is right while connected but far too slow to be the
# recovery path: a Pi that boots before the PC, or rides out a router reboot,
# would sit idle for up to a minute per attempt. It keeps sampling into the
# cache throughout, so nothing is lost - but the clock stays unsynced and the
# backlog keeps growing, and only discovery can end that.
DEFAULT_RECONNECT_SECONDS = 5

# Chunk size for the batch upload. Small enough that a large backlog cannot
# monopolise the server, large enough that a 6-hour outage drains in seconds
# instead of minutes of fsync.
DEFAULT_BATCH_SIZE = 200
DEFAULT_MAX_BATCHES_PER_CYCLE = 10

STREAM_ROUTES = {
    "DHT": "/api/getDataDHT",
    "C5A": "/api/getDataC5A",
}


class BackendClient:
    """Discovery, registration, clock sync, schedule polling, heartbeat and
    upload - all driven by one maintenance cycle on a daemon thread."""

    def __init__(self, config, stream, network_list=None, port=DEFAULT_PORT,
                 maintenance_seconds=DEFAULT_MAINTENANCE_SECONDS,
                 cleanup_days=None,
                 batch_size=DEFAULT_BATCH_SIZE,
                 max_batches_per_cycle=DEFAULT_MAX_BATCHES_PER_CYCLE,
                 reconnect_seconds=DEFAULT_RECONNECT_SECONDS,
                 clock=None):
        self.config = config
        self.stream = stream
        self.data_route = STREAM_ROUTES[stream]
        # Which rows in the shared cache belong to THIS sensor. Local, from
        # config.txt - see pi_common.config.source_key().
        self.source_key = configlib.source_key(config)
        self.network_list = str(network_list or (cache.BASE_DIR / DEFAULT_NETWORK_LIST))
        self.port = port
        self.maintenance_seconds = maintenance_seconds
        self.cleanup_days = (config.get("cleanupDays", configlib.DEFAULT_CLEANUP_DAYS)
                             if cleanup_days is None else cleanup_days)
        self.batch_size = batch_size
        self.max_batches_per_cycle = max_batches_per_cycle
        self.reconnect_seconds = reconnect_seconds
        self._retry_delay = reconnect_seconds

        self.clock = clock or Clock()
        self.boot_id = read_boot_id()

        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._session = requests.Session()

        self._base_url = None
        self._sensor_id = None
        self._schedule = None       # (period_seconds, effective_from_epoch)
        self._send_seconds = DEFAULT_SEND_SECONDS
        self._batch_supported = True
        self._corrected_this_boot = False
        self._clock_warning_shown = False

    # -- thread-safe accessors -------------------------------------------

    @property
    def base_url(self):
        with self._lock:
            return self._base_url

    @property
    def sensor_id(self):
        with self._lock:
            return self._sensor_id

    @property
    def send_seconds(self):
        with self._lock:
            return self._send_seconds

    def take_schedule(self):
        """Pop a pending schedule update, or None. Called from the tick loop."""
        with self._lock:
            schedule, self._schedule = self._schedule, None
            return schedule

    # -- lifecycle --------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return

        self._thread = threading.Thread(
            target=self._run, name=f"backend-{self.stream}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout=5):
        self.stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self):
        """Two independent due-times on one thread.

        Maintenance keeps its fixed 60 s (see DEFAULT_MAINTENANCE_SECONDS -
        the offline detector depends on it); the upload runs on the
        server-controlled send interval. Each iteration runs whatever is due
        and then sleeps until the EARLIER of the two next due-times.

        One thread, not two. The upload is already asynchronous with respect
        to the tick loop, which is the property that actually matters, and a
        second thread would just add a second writer to the cache.
        """
        # Both due immediately rather than one interval from now: the clock is
        # wrong until the first sync, and a Pi restarting with a backlog should
        # not sit on it.
        next_maintenance = next_upload = time.monotonic()

        while not self.stop_event.is_set():
            now = time.monotonic()

            if now >= next_maintenance:
                try:
                    self.run_maintenance()
                except Exception as e:
                    print(f"[{self.stream}] maintenance error:", e)

                # Re-read the clock: _cycle_delay() is an interval from the END
                # of the work, and discovery can spend 5 s per listed address.
                next_maintenance = time.monotonic() + self._cycle_delay()

            if now >= next_upload:
                try:
                    self.run_upload()
                except Exception as e:
                    print(f"[{self.stream}] upload error:", e)

                next_upload = time.monotonic() + self._send_delay()

            delay = min(next_maintenance, next_upload) - time.monotonic()

            if delay > 0:
                self.stop_event.wait(delay)

    def _send_delay(self):
        """The upload interval, clamped. See MIN_SEND_SECONDS."""
        return min(max(self.send_seconds, MIN_SEND_SECONDS), MAX_SEND_SECONDS)

    def _cycle_delay(self):
        """Normal cadence when connected, a short backing-off retry when not.

        Backing off rather than retrying at a fixed few seconds forever: a scan
        costs up to one timeout per listed address, so a permanently absent
        backend would otherwise mean near-continuous scanning for as long as
        the Pi is powered. Doubling from `reconnect_seconds` up to the normal
        cycle keeps a brief outage - the common case, a PC restart or a router
        reboot - recovering in seconds, while a long one settles to once a
        minute.

        The delay resets as soon as there is a backend again, so the next
        outage gets the same fast first retry rather than inheriting the last
        one's backoff.
        """
        if self.base_url is not None:
            self._retry_delay = self.reconnect_seconds
            return self.maintenance_seconds

        delay = self._retry_delay
        self._retry_delay = min(delay * 2, self.maintenance_seconds)

        return delay

    def run_maintenance(self):
        """Everything the backend has to hear from on a fixed cadence.

        The upload is NOT here - see run_upload().
        """
        self.discover_backend()
        self.register_sensor_if_needed()
        self.sync_clock()
        self.poll_schedule()

    def run_upload(self):
        """Drain the cache, then collect what has aged out of it."""
        self.flush()

        try:
            cache.cleanup(self.cleanup_days, stream=self.stream)
        except Exception as e:
            print(f"[{self.stream}] cache cleanup error:", e)

    # -- discovery / registration ----------------------------------------

    def discover_backend(self):
        if self._base_url is not None and self._probe_current_backend():
            return

        try:
            new_url = discovery.network_search(self.network_list, self.port, "")
        except Exception as e:
            print(f"[{self.stream}] backend discovery error:", e)
            new_url = None

        with self._lock:
            changed = new_url != self._base_url
            self._base_url = new_url

        if changed:
            print(f"[{self.stream}] Backend discovered:" if new_url
                  else f"[{self.stream}] Backend unavailable",
                  new_url or "")

    def _probe_current_backend(self):
        """Cheap liveness check so a working backend is not re-scanned every
        cycle (the scan costs up to 5 s per listed IP)."""
        try:
            r = self._session.get(f"{self._base_url}/api/time", timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    def register_sensor_if_needed(self):
        base_url = self.base_url

        if base_url is None or self.sensor_id is not None:
            return

        try:
            r = self._session.post(
                f"{base_url}/api/registerSensor", json=self.config, timeout=5
            )
            r.raise_for_status()
            sensor_id = r.json().get("sensorID")

            if sensor_id:
                with self._lock:
                    self._sensor_id = sensor_id
                print(f"[{self.stream}] Registered sensor ID: {sensor_id}")
            else:
                print(f"[{self.stream}] Registration response did not include sensorID")

        except Exception as e:
            print(f"[{self.stream}] Registration failed:", e)

    # -- clock ------------------------------------------------------------

    def sync_clock(self):
        """Adopt the offset the device agent published. No network call.

        The measuring moved to one process per Pi (device_agent.py) so
        two sensors on one box cannot stamp two different offsets onto readings
        taken on the same tick grid.
        """
        was_synced = self.clock.synced
        result = self.clock.refresh()

        if result is None:
            # No usable published offset: the agent is not running, has not
            # reached the backend recently, or the file is from another boot.
            # The previous offset is kept and readings stamp ESTIMATED.
            #
            # Said once per outage, not once per cycle - this runs every 60 s
            # and the old style of logging would fill the journal.
            if not self._clock_warning_shown:
                print(f"[{self.stream}] No published clock - is device-agent "
                      f"running? Timestamps are boot-stepped only, "
                      f"confidence ESTIMATED")
                self._clock_warning_shown = True
            return

        offset, rtt = result

        if not was_synced:
            rtt_text = "unknown" if rtt is None else f"{rtt * 1000:.1f}ms"
            print(f"[{self.stream}] Clock: offset={offset * 1000:.1f}ms "
                  f"rtt={rtt_text} (from device agent)")
            self._clock_warning_shown = False
            self._correct_cached_timestamps()

    def _correct_cached_timestamps(self):
        """Turn ESTIMATED rows from this boot into CORRECTED ones."""
        if self._corrected_this_boot:
            return

        reference = self.clock.reference()

        if reference is None or self.boot_id is None:
            return

        try:
            corrected = cache.correct_boot_timestamps(self.boot_id, *reference)
            self._corrected_this_boot = True

            if corrected:
                print(f"[{self.stream}] Corrected {corrected} pre-sync timestamps")

        except Exception as e:
            print(f"[{self.stream}] Timestamp correction failed:", e)

    # -- schedule ---------------------------------------------------------

    def poll_schedule(self):
        base_url = self.base_url

        if base_url is None:
            return

        try:
            r = self._session.get(f"{base_url}/api/schedule", timeout=5)

            if r.status_code != 200:
                return

            body = r.json()
            period = body.get("periodSeconds")
            send_seconds = body.get("sendSeconds")
            effective_from_ms = body.get("effectiveFromMs")

            # No effectiveFrom for the send interval, and deliberately none:
            # only the sampling period needs every Pi to switch on the same
            # tick. Giving uploads the coordination machinery would advertise a
            # guarantee that is not being made. It takes effect at the end of
            # whatever upload cycle is already counting down.
            if send_seconds is not None:
                with self._lock:
                    self._send_seconds = float(send_seconds)

            if period is None:
                return

            effective_from = (effective_from_ms / 1000.0
                              if effective_from_ms is not None
                              else self.clock.now())

            with self._lock:
                self._schedule = (float(period), effective_from)

        except Exception as e:
            print(f"[{self.stream}] Schedule poll failed:", e)

    # -- liveness ---------------------------------------------------------
    #
    # send_heartbeat() and report_shutdown() used to live here. Both moved to
    # the device agent (device_agent.py), which runs once per Pi and
    # identifies itself by deviceUUID.
    #
    # Why they had to move rather than just being duplicated: liveness and the
    # SHUTDOWN event are properties of the BOX, not of one sensor stream. A Pi
    # running two sensors reported two heartbeats and, on a clean stop, two
    # SHUTDOWN events for a single machine powering down. And an actuator-only
    # Pi had no sensorID to heartbeat with at all, which is why the mist Pi was
    # invisible under Devices.
    #
    # Stopping one sensor service is no longer a device event, and that is
    # correct - the device did not stop.

    # -- upload -----------------------------------------------------------

    def _row_payload(self, row, now_epoch):
        payload = {
            # This client's CURRENT sensorID, not the one stored on the row.
            #
            # Safe only because get_unsent() filters on source_key, so every
            # row here was written by this process's sensor. That filter
            # replaced an older fix for the same problem - two dht22.py
            # processes sharing sensor_cache.db and the stream "DHT", each
            # flushing the other's rows - which sent the row's own stored
            # sensorID instead.
            #
            # Sending the stored ID is what breaks when a Pi is moved between
            # databases: the number is either absent there, so the insert fails
            # fk_sensorlog_sensor and (a non-200 being retried unchanged) wedges
            # the queue forever, or it belongs to a DIFFERENT physical sensor
            # and the readings are filed under it with no error at all. The
            # current ID is re-earned from /api/registerSensor against whatever
            # database this Pi is actually talking to, so neither can happen.
            "sensorID": self.sensor_id,
            "temperature": row["temperature"],
            "humidity": row["humidity"],
            "VPD": row["vpd"],
            "time": row["timestamp"],
            "timeConfidence": row["timeConfidence"] or "UNKNOWN",
            "readLatencyMs": row["readLatencyMs"],
            "tickJitterMs": row["tickJitterMs"],
            "syncRttMs": row["syncRttMs"],
        }

        # Tick -> upload attempt. Large values mean this row was replayed from
        # the offline cache rather than sent live, which is what separates
        # "the network was slow" from "the Pi was offline for six hours".
        if row["tickEpoch"] is not None:
            payload["queueDelayMs"] = int(round((now_epoch - row["tickEpoch"]) * 1000))

        if self.stream == "C5A":
            payload["windSpeed"] = row["windspeed"]
            payload["windDirection"] = row["windDirection"]

        return payload

    def flush(self):
        base_url = self.base_url

        if base_url is None or self.sensor_id is None:
            return

        for _ in range(self.max_batches_per_cycle):
            if self.stop_event.is_set():
                return

            try:
                rows = cache.get_unsent(stream=self.stream,
                                        source_key=self.source_key,
                                        limit=self.batch_size)
            except Exception as e:
                print(f"[{self.stream}] Cache read failed:", e)
                return

            if not rows:
                return

            now_epoch = self.clock.now()
            payloads = [self._row_payload(row, now_epoch) for row in rows]

            if self._batch_supported:
                result = self._send_batch(base_url, payloads)

                if result is None:
                    return  # network problem - try again next cycle

                # Rows the backend refused, by index into this batch.
                #
                # They are malformed - a missing field, or a value the
                # SensorLog columns cannot hold, such as the 6553.5 a corrupt
                # C5A frame produces - so they can never succeed, and NOT
                # retiring them would wedge this queue forever behind rows that
                # get rejected again every cycle. They are still discarded here.
                #
                # The loss is recorded in two places, neither of which is this
                # cache: the line below, and an ErrorLog row the backend writes
                # with errorType "data corrupt", stamped with the READING's own
                # time. So a sensor drifting into garbage is answerable after
                # the fact, which it was not when this retired the whole batch
                # and reported "Uploaded N rows" without a word.
                rejected = result.get("rejected") or []

                if rejected:
                    stamps = ", ".join(
                        str(rows[i]["timestamp"]) for i in rejected
                        if 0 <= i < len(rows)
                    )
                    print(f"[{self.stream}] Backend rejected {len(rejected)} of "
                          f"{len(rows)} rows as malformed - discarding: {stamps}")

                self._retire([row["id"] for row in rows])
                print(f"[{self.stream}] Uploaded {len(rows) - len(rejected)} rows "
                      f"({rows[0]['timestamp']} .. {rows[-1]['timestamp']})")
            else:
                if not self._send_one_by_one(base_url, rows, payloads):
                    return

            if len(rows) < self.batch_size:
                return

    def _send_batch(self, base_url, payloads):
        try:
            r = self._session.post(
                f"{base_url}/api/sensorLogBatch",
                json={"rows": payloads},
                timeout=30,
            )

            if r.status_code == 404:
                # Older backend. Fall back permanently for this run.
                print(f"[{self.stream}] Batch endpoint missing, using single-row upload")
                self._batch_supported = False
                return None

            if r.status_code == 409:
                # The backend has no Sensor row for this sensorID any more -
                # almost always a rebuilt database. These rows can never be
                # accepted as they stand, and a non-200 is retried unchanged,
                # so without this the queue jams on them forever. Dropping the
                # id makes register_sensor_if_needed() earn a fresh one on the
                # next maintenance cycle; the cached readings are untouched and
                # go up under the new id.
                print(f"[{self.stream}] Backend does not know sensorID "
                      f"{self.sensor_id} - re-registering")
                with self._lock:
                    self._sensor_id = None
                return None

            if r.status_code != 200:
                print(f"[{self.stream}] Batch upload failed: {r.status_code} {r.text[:200]}")
                return None

            # The whole body, not just `inserted`: the caller needs `rejected`
            # too, and dropping it here is what made malformed rows vanish
            # without any record that they had been thrown away.
            try:
                return r.json()
            except ValueError:
                return {"inserted": len(payloads), "rejected": []}

        except Exception as e:
            print(f"[{self.stream}] Batch upload error:", e)
            return None

    def _send_one_by_one(self, base_url, rows, payloads):
        url = f"{base_url}{self.data_route}"
        uploaded = []

        for row, payload in zip(rows, payloads):
            try:
                r = self._session.post(url, json=payload, timeout=5)

                if r.status_code == 200:
                    uploaded.append(row["id"])
                elif r.status_code == 409:
                    # Same reasoning as the batch path above.
                    print(f"[{self.stream}] Backend does not know sensorID "
                          f"{self.sensor_id} - re-registering")
                    with self._lock:
                        self._sensor_id = None
                    break
                else:
                    print(f"[{self.stream}] Upload failed for record {row['id']}: "
                          f"{r.status_code} {r.text[:200]}")
                    break

            except Exception as e:
                print(f"[{self.stream}] Upload error for record {row['id']}:", e)
                break

        self._retire(uploaded)
        return len(uploaded) == len(rows)

    def _retire(self, record_ids):
        """Finish with rows the server has acknowledged.

        Retention of 0 deletes them here instead of leaving them for
        cache.cleanup() to collect later. Either way this runs only after a
        200, so nothing leaves the Pi's copy unacknowledged.
        """
        if self.cleanup_days == 0:
            cache.delete_many(record_ids)
        else:
            cache.mark_uploaded_many(record_ids)
