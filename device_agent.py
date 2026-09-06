#!/usr/bin/env python3
"""Device status agent - one per Pi, the only thing that reports liveness.

    python3 device_agent.py

A CLIENT, not a library. It sits next to dht22.py, C5A.py, relay_control.py and
fan_control.py, and it is the one that runs on EVERY Pi regardless of what
hardware is attached. pi_common/ is what all of them import; this is not part of
it.

Runs on EVERY Pi, sensor or actuator or both. It answers one question - is this
box alive, and what is its clock doing - and it identifies itself by the device
UUID, which is the one identity every client on the Pi already shares.

WHAT IT DOES NOT DO

It registers nothing. Registration belongs to whatever owns the thing being
registered: the sensor clients create Device + Sensor, the actuator clients
create Device + Actuator. So a heartbeat for a UUID nobody has registered is
REJECTED by the backend rather than conjuring a Device row with nothing on it.

That is self-healing and needs no coordination: this agent retries every cycle,
the sensor or actuator client on the same Pi registers within one maintenance
cycle, and the next heartbeat lands. A Pi that runs only this agent never
appears on the dashboard, which is correct - a box with no sensors and no
actuators is not part of the system.

It also reads no config.txt. There is nothing to configure.

CLOCK

It measures the offset to the backend and publishes it to /run/sp_dashboard/
clock.json for the sensor clients to read (pi_common/clock.py). It never steps
the system clock - sync_clock.py does that once at boot, as root, before
anything is scheduled. That is why this runs as `pi` and needs no privileges.

LET IT CRASH

systemd restarts this with Restart=always. Network errors are swallowed because
an unreachable backend is an ordinary state; anything else should kill the
process and get restarted clean.
"""

import os
import signal
import sys
import threading
import time
from pathlib import Path

import requests

from pi_common import clock as clockmod
from pi_common.discovery import find_backend
from pi_common.identity import get_device_uuid

# Matches HEARTBEAT_INTERVAL_MS in the backend config: the server marks a device
# OFFLINE after three missed cycles.
HEARTBEAT_SECONDS = float(os.environ.get("SP_AGENT_INTERVAL", 60))

BACKEND_PORT = int(os.environ.get("SP_BACKEND_PORT", 5000))

# The Pi is FLAT. The repo splits code into "PI sensor" and "Pi Actuator" only
# to say which files go to which kind of device; on the Pi itself there is one
# directory holding every script, its config, and the pi_common/ lib folder:
#
#     dht22.py  C5A.py  relay_control.py  fan_control.py  device_agent.py
#     config_dht22.txt  networkList.txt  device_uuid.txt
#     pi_common/
#
# Anchored to this file's own folder, exactly like relay_control.py and
# fan_control.py. There is one device_uuid.txt and one networkList.txt per Pi,
# by layout rather than by symlink.
BASE_DIR = Path(__file__).resolve().parent

UUID_FILE = Path(os.environ.get("SENSOR_UUID_FILE", BASE_DIR / "device_uuid.txt"))
NETWORK_LIST = Path(os.environ.get("SP_NETWORK_LIST", BASE_DIR / "networkList.txt"))

stop_event = threading.Event()


def handle_stop(signum, frame):
    print("[agent] stopping...")
    stop_event.set()


class DeviceAgent:
    def __init__(self, device_uuid, network_list, port=BACKEND_PORT):
        self.device_uuid = device_uuid
        self.network_list = str(network_list)
        self.port = port
        self.boot_id = clockmod.read_boot_id()

        # One session for the whole process: connection reuse matters here,
        # because a fresh TCP handshake on every /api/time sample would show up
        # directly in the measured round trip.
        self.session = requests.Session()

        self.base_url = None
        self._warned_unregistered = False

    # -- backend ---------------------------------------------------------

    def ensure_backend(self):
        """Keep the current backend while it answers; rescan only when it stops."""
        if self.base_url is not None:
            try:
                if self.session.get(f"{self.base_url}/api/time", timeout=3).status_code == 200:
                    return self.base_url
            except requests.RequestException:
                pass

            print(f"[agent] lost backend {self.base_url}, rescanning")
            self.base_url = None

        self.base_url = find_backend(self.network_list, port=self.port)

        if self.base_url:
            print(f"[agent] backend {self.base_url}")

        return self.base_url

    # -- clock -----------------------------------------------------------

    def sync_clock(self):
        """Measure and publish. Returns (offset, rtt) or None."""
        result = clockmod.probe(self.base_url, session=self.session)

        if result is None:
            # Not an error worth logging every cycle - a busy backend fails the
            # RTT ceiling now and then, and the previous published offset stays
            # valid until it ages out.
            return None

        offset, rtt = result
        clockmod.publish(offset, rtt, boot_id=self.boot_id)
        return offset, rtt

    # -- heartbeat -------------------------------------------------------

    def send_heartbeat(self, sync):
        payload = {
            "deviceUUID": self.device_uuid,
            "bootID": self.boot_id,
            "uptimeSeconds": clockmod.read_uptime(),
            "detail": "device agent",
        }

        if sync is not None:
            offset, rtt = sync
            payload["offsetMs"] = round(offset * 1000, 3)
            payload["rttMs"] = None if rtt is None else round(rtt * 1000, 3)

        try:
            r = self.session.post(
                f"{self.base_url}/api/heartbeat", json=payload, timeout=5
            )
        except requests.RequestException as e:
            print("[agent] heartbeat failed:", e)
            return

        if r.status_code != 200:
            print(f"[agent] heartbeat rejected ({r.status_code})")
            return

        # tracked:false means this Pi has registered nothing yet. Normal on a
        # fresh box, and it fixes itself the moment a sensor or actuator client
        # registers - so say it once, not every 60 seconds.
        tracked = r.json().get("tracked")

        if not tracked and not self._warned_unregistered:
            print(f"[agent] device {self.device_uuid} is not registered yet - "
                  f"waiting for a sensor or actuator client on this Pi to register")
            self._warned_unregistered = True

        elif tracked and self._warned_unregistered:
            print("[agent] device registered, heartbeats are landing")
            self._warned_unregistered = False

    def report_shutdown(self):
        """Best-effort clean-stop notice.

        This is what distinguishes 'deliberately stopped' from 'power cut' in
        DeviceEvent - the watchdog can only ever infer the latter from silence.
        Best-effort by nature: a real power cut never reaches this code, which
        is exactly why the watchdog exists.
        """
        if self.base_url is None:
            return

        try:
            self.session.post(
                f"{self.base_url}/api/deviceEvent",
                json={
                    "deviceUUID": self.device_uuid,
                    "eventType": "SHUTDOWN",
                    "bootID": self.boot_id,
                    "detail": "device agent stopped",
                },
                timeout=5,
            )
        except requests.RequestException:
            pass

    # -- loop ------------------------------------------------------------

    def run(self):
        print(f"[agent] started (uuid {self.device_uuid}, boot {self.boot_id}, "
              f"every {HEARTBEAT_SECONDS:.0f}s)")
        print(f"[agent] backend list: {self.network_list}")
        print(f"[agent] publishing clock to {clockmod.CLOCK_FILE}")

        while not stop_event.is_set():
            started = time.monotonic()

            if self.ensure_backend():
                sync = self.sync_clock()
                self.send_heartbeat(sync)

            # Measured from the START of the cycle, so a slow probe does not
            # stretch the interval and push the Pi past the server's threshold.
            elapsed = time.monotonic() - started
            stop_event.wait(max(1.0, HEARTBEAT_SECONDS - elapsed))

        self.report_shutdown()
        print("[agent] stopped")


def main():
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    agent = DeviceAgent(
        device_uuid=get_device_uuid(UUID_FILE),
        network_list=NETWORK_LIST,
    )
    agent.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
