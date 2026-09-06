"""Epoch-anchored tick scheduler.

Replaces ``time.sleep(INTERVAL)`` with "sleep until the next tick"::

    next_tick = floor(now / P) * P + P      # anchored to the Unix epoch

Anchoring to the epoch is the whole trick: given a shared clock, every Pi
derives identical instants with zero coordination. No phase value has to be
distributed and no per-sample message is sent, so a network outage cannot
break alignment.

Two things this module is careful about:

* The sleep runs against a ``time.monotonic()`` deadline, not the wall clock.
  A clock correction arriving mid-sleep cannot then stretch or collapse it.
* If a read overran (DHT22 retries can take 6 s on a 5 s period), the next
  *future* multiple is used. Ticks are skipped, never fired back-to-back to
  catch up - catching up would put two readings a few milliseconds apart and
  then land the sensors on different instants again.
"""

import math
import time

# DHT22 datasheet limit: one reading per 2 s. This is the binding constraint,
# not read duration. Enforced here and in the backend validator.
MIN_PERIOD_SECONDS = 2.0

# Sleep in slices so a stop request is noticed promptly on shutdown.
_SLEEP_SLICE = 0.5


class Tick:
    """One scheduled sampling instant.

    ``epoch`` is the exact tick time - an exact multiple of the period. This
    is what readings are stamped with, NOT the time the read completed.
    ``jitter_ms`` is how late the scheduler actually woke up.
    """

    __slots__ = ("epoch", "monotonic", "jitter_ms", "skipped", "period")

    def __init__(self, epoch, monotonic, jitter_ms, skipped, period):
        self.epoch = epoch
        self.monotonic = monotonic
        self.jitter_ms = jitter_ms
        self.skipped = skipped
        self.period = period

    def __repr__(self):
        return (f"Tick(epoch={self.epoch:.3f}, jitter={self.jitter_ms}ms, "
                f"skipped={self.skipped}, period={self.period})")


def next_multiple(now, period):
    """First multiple of ``period`` strictly after ``now``."""
    return math.floor(now / period) * period + period


class TickScheduler:
    def __init__(self, clock, period, min_period=MIN_PERIOD_SECONDS, stop_event=None):
        self.clock = clock
        self.min_period = float(min_period)
        self.period = max(float(period), self.min_period)
        self.stop_event = stop_event
        self.last_tick_epoch = None
        self._pending = None  # (new_period, effective_from_epoch)

    # -- period changes ---------------------------------------------------

    def schedule_period_change(self, period, effective_from):
        """Queue a period switch at an absolute epoch.

        ``effective_from`` comes from the PC already snapped to a multiple of
        the new period, so every Pi switches at the same instant and lands on
        the same new grid.
        """
        period = max(float(period), self.min_period)

        if period == self.period and self._pending is None:
            return False

        if self._pending == (period, effective_from):
            return False

        self._pending = (period, effective_from)
        return True

    @property
    def pending(self):
        return self._pending

    # -- ticking ----------------------------------------------------------

    def _target_epoch(self, now):
        """Next tick instant, applying a pending period switch if it is due."""
        candidate = next_multiple(now, self.period)

        if self._pending is not None:
            new_period, effective_from = self._pending

            if candidate >= effective_from:
                self.period = new_period
                self._pending = None
                # The old grid is gone, so a gap across the switch is not a
                # skipped tick - clear the reference rather than report one.
                self.last_tick_epoch = None
                # effective_from is a multiple of the new period, so either
                # branch of the max() is on the new grid.
                candidate = max(effective_from, next_multiple(now, new_period))

        return candidate

    def wait_for_next_tick(self):
        """Block until the next tick. Returns a :class:`Tick`, or ``None`` if
        the stop event fired while waiting."""
        now_epoch = self.clock.now()
        now_mono = time.monotonic()

        target_epoch = self._target_epoch(now_epoch)
        deadline_mono = now_mono + (target_epoch - now_epoch)

        while True:
            remaining = deadline_mono - time.monotonic()

            if remaining <= 0:
                break

            if self.stop_event is not None:
                if self.stop_event.wait(min(remaining, _SLEEP_SLICE)):
                    return None
            else:
                time.sleep(min(remaining, _SLEEP_SLICE))

        woke_mono = time.monotonic()
        # Jitter is measured on the monotonic clock, so an offset correction
        # applied during the sleep does not show up as fake jitter.
        jitter_ms = int(round((woke_mono - deadline_mono) * 1000))

        skipped = 0
        if self.last_tick_epoch is not None:
            elapsed_periods = int(round((target_epoch - self.last_tick_epoch) / self.period))
            skipped = max(0, elapsed_periods - 1)

        self.last_tick_epoch = target_epoch

        return Tick(target_epoch, woke_mono, jitter_ms, skipped, self.period)
