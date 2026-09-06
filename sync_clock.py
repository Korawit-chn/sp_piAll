#!/usr/bin/env python3
"""Boot-time clock step. Runs as root, once, before the sensor service.

The Pi has no RTC and no internet, so ``fake-hwclock`` restores whatever time
was saved at shutdown. This asks the backend PC for the real time and steps the
system clock, which is what makes Pi logs and file mtimes meaningful. The
running sensor app does NOT do this - it keeps a software offset instead, so
the clock is never stepped underneath a live sampling schedule.

``timedatectl`` only accepts whole seconds, so this correction is coarse. That
is fine; the software offset provides the fine correction.

Always exits 0: a Pi that cannot reach the PC must still boot and start
collecting (the readings get their true timestamps reconstructed later).
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pi_common import discovery                                # noqa: E402
from pi_common.clock import probe, step_system_clock           # noqa: E402

BASE_DIR = Path(__file__).resolve().parent
NETWORK_LIST = BASE_DIR / "networkList.txt"
PORT = 5000

# Bounded: the network may simply not be there, and boot must not hang.
TOTAL_BUDGET_SECONDS = 90
RETRY_DELAY_SECONDS = 10

# More generous than the running app's 100 ms: this is a one-shot whole-second
# step, so a slow-but-usable exchange is still worth taking.
MAX_RTT = 1.0


def find_backend():
    try:
        return discovery.network_search(str(NETWORK_LIST), PORT, "")
    except Exception as e:
        print("Backend discovery error:", e)
        return None


def main():
    deadline = time.monotonic() + TOTAL_BUDGET_SECONDS

    while time.monotonic() < deadline:
        base_url = find_backend()

        if base_url:
            print("Backend:", base_url)
            result = probe(base_url, samples=10, max_rtt=MAX_RTT)

            if result is not None:
                offset, rtt = result
                target = time.time() + offset

                print(f"Offset {offset:.3f}s (rtt {rtt * 1000:.1f}ms) -> "
                      f"stepping clock")

                if step_system_clock(target):
                    print("System clock set from backend")
                    return 0

                print("Clock step failed (need root?)")
                return 0

            print("No usable time sample")
        else:
            print("Backend unavailable")

        if time.monotonic() + RETRY_DELAY_SECONDS >= deadline:
            break

        time.sleep(RETRY_DELAY_SECONDS)

    print("Giving up - continuing with the restored fake-hwclock time. "
          "Readings will be marked ESTIMATED and corrected once the PC is "
          "reachable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
