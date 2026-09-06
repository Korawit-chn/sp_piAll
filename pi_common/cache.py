"""Offline-first local buffer.

Readings are always written here first, before any network call, so collection
never stops for network reasons. The backend client drains this in batches.

Two things this file has to get right beyond plain buffering:

* **Stream separation.** ``dht22.py`` and ``C5A.py`` run from the same folder
  and therefore share this database. Without the ``stream`` column each one
  flushed the other's rows to the wrong endpoint - ``dht22.py`` posting C5A
  rows to ``/api/getDataDHT`` silently dropped windspeed. Every read and
  delete is filtered by stream.
* **Ownership within a stream.** Two ``dht22.py`` processes on one Pi share
  this database AND the stream ``DHT``, so the stream filter alone still let
  either one drain the other's rows. ``sourceKey`` - the sensor's own
  ``type|location`` from config.txt - narrows a drain to the rows that
  process actually wrote. See ``get_unsent()``.
* **Rows taken before the clock was synced.** Each row stores ``bootID`` and a
  ``monotonic`` value. The monotonic clock is continuous and correct-rate
  within one boot, so once the offset to the PC is learned the true time of
  every row from that boot can be reconstructed exactly. See
  ``correct_boot_timestamps()``.
"""

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from .clock import CORRECTED, ESTIMATED, UNKNOWN

# Resolved against the package's parent (the "PI Env" folder), not the current
# working directory - otherwise the file that gets used depends on where the
# script happened to be launched from.
BASE_DIR = Path(__file__).resolve().parent.parent

DB_FILE = str(Path(os.environ.get("SENSOR_CACHE_DB", BASE_DIR / "sensor_cache.db")))

DEFAULT_FLUSH_LIMIT = 500

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS SensorLog (
    logID INTEGER PRIMARY KEY AUTOINCREMENT,
    sensorID INTEGER,
    datetime TEXT DEFAULT CURRENT_TIMESTAMP,
    temperature REAL,
    humidity REAL,
    windspeed REAL,
    windDirection INTEGER,
    VPD REAL,
    uploaded INTEGER DEFAULT 0
);
"""

# Added by migration on existing databases; see _migrate().
EXTRA_COLUMNS = (
    ("stream", "TEXT"),
    ("bootID", "TEXT"),
    ("monotonic", "REAL"),
    ("tickEpoch", "REAL"),
    ("timeConfidence", "TEXT DEFAULT 'UNKNOWN'"),
    ("readLatencyMs", "INTEGER"),
    ("tickJitterMs", "INTEGER"),
    # RTT of the clock sync in force when this reading was taken. Without it a
    # latency figure is a point value; with it the report can carry error bars.
    ("syncRttMs", "INTEGER"),
    # Which sensor on this Pi wrote the row: "type|location", taken from
    # config.txt. LOCAL identity, deliberately not the server's sensorID - it
    # is the same before and after a database swap, so it stays valid when the
    # numeric ID does not. See get_unsent().
    ("sourceKey", "TEXT"),
)


def _connect():
    return sqlite3.connect(DB_FILE, timeout=10)


def set_db_file(path):
    """Point the cache at a different file (per-script isolation if wanted)."""
    global DB_FILE
    DB_FILE = str(path)


def _migrate(conn):
    """Add any missing columns. Existing deployments already hold unsent rows,
    so the table is extended in place rather than recreated."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(SensorLog)")}
    added = []

    for name, spec in EXTRA_COLUMNS:
        if name not in existing:
            conn.execute(f"ALTER TABLE SensorLog ADD COLUMN {name} {spec}")
            added.append(name)

    if "stream" in added:
        # Legacy rows predate the column. Their stream is recoverable from the
        # data itself - only the C5A writes windspeed - so backfill it rather
        # than leave rows that both scripts would try to flush.
        conn.execute(
            "UPDATE SensorLog SET stream = "
            "CASE WHEN windspeed IS NOT NULL THEN 'C5A' ELSE 'DHT' END "
            "WHERE stream IS NULL"
        )

    if "syncRttMs" in added:
        # One-time, on the upgrade to the v3-compatible client.
        #
        # A tickEpoch means the row came from the tick-driven sampler, so it is
        # real synchronised data and must still be uploaded. A NULL tickEpoch
        # means the free-running loop wrote it, with a clock that had drifted
        # since boot - that is v2 data by definition, and letting it flush into
        # v3 would put unsynchronised readings in the clean dataset.
        #
        # Marked uploaded rather than deleted: the rows stay on the Pi if the
        # old data is ever wanted.
        retired = conn.execute(
            "UPDATE SensorLog SET uploaded = 1 "
            "WHERE uploaded = 0 AND tickEpoch IS NULL"
        ).rowcount

        if retired:
            print(f"[cache] retired {retired} pre-tick row(s) - "
                  f"free-running data is not uploaded to v3")

    if "sourceKey" in added:
        # Rows written before the column have no local identity, so no client
        # can prove one is its own. Retired rather than guessed at.
        #
        # The guess available - "attribute it to whoever's current sensorID
        # matches the row's stored sensorID" - is exactly the reasoning the
        # column exists to remove: a stale sensorID after a database swap is
        # either meaningless or, worse, another sensor's. Same call as the
        # syncRttMs migration above, and marked rather than deleted for the
        # same reason: the rows stay on the Pi if the data is ever wanted.
        orphaned = conn.execute(
            "UPDATE SensorLog SET uploaded = 1 "
            "WHERE uploaded = 0 AND sourceKey IS NULL"
        ).rowcount

        if orphaned:
            print(f"[cache] retired {orphaned} row(s) with no source identity - "
                  f"they predate the sourceKey column")


def init_db():
    conn = _connect()
    try:
        conn.execute(CREATE_TABLE_SQL)
        _migrate(conn)
        # Replaces idx_sensorlog_unsent, which predates sourceKey. Named
        # differently because CREATE INDEX IF NOT EXISTS would keep the old
        # definition under the old name and quietly do nothing.
        conn.execute("DROP INDEX IF EXISTS idx_sensorlog_unsent")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sensorlog_drain "
            "ON SensorLog (uploaded, stream, sourceKey, logID)"
        )
        conn.commit()
    finally:
        conn.close()


def save_reading(timestamp, temperature, humidity, vpd, windspeed=None,
                 windDirection=None, sensor_id=None, stream=None, boot_id=None,
                 monotonic=None, tick_epoch=None, time_confidence=UNKNOWN,
                 read_latency_ms=None, tick_jitter_ms=None, sync_rtt_ms=None,
                 source_key=None):
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO SensorLog (
                sensorID, datetime, temperature, humidity, windspeed,
                windDirection, VPD, stream, bootID, monotonic, tickEpoch,
                timeConfidence, readLatencyMs, tickJitterMs, syncRttMs,
                sourceKey
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sensor_id, timestamp, temperature, humidity, windspeed,
                windDirection, vpd, stream, boot_id, monotonic, tick_epoch,
                time_confidence, read_latency_ms, tick_jitter_ms, sync_rtt_ms,
                source_key,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_unsent(stream=None, source_key=None, limit=DEFAULT_FLUSH_LIMIT):
    """Oldest unsent rows written by this sensor.

    `source_key` is what makes a drain safe to relabel. Two dht22.py processes
    on one Pi share this database and the stream "DHT", so filtering on stream
    alone hands whichever one flushes first the rows from BOTH sensors. That
    used to be handled downstream, by sending each row's own stored sensorID -
    which works right up until the Pi is pointed at a different database, where
    that number is either absent (the insert fails a foreign key and the queue
    wedges) or belongs to a different physical sensor (the readings are filed
    under it, silently).

    Filtering here removes both: a client only ever sees rows it wrote, so it
    can stamp its own CURRENT sensorID on them and the stale one is never read.

    Passing no source_key drains the whole stream, which is what a single
    sensor per stream gets and what the count/legacy paths want.

    The limit is not optional: after a long outage the backlog is tens of
    thousands of rows, and loading all of them into memory on every flush
    attempt is how a Pi runs out of RAM while already struggling.
    """
    conn = _connect()
    conn.row_factory = sqlite3.Row
    try:
        query = (
            "SELECT logID AS id, sensorID, datetime AS timestamp, temperature, "
            "humidity, windspeed, windDirection, VPD AS vpd, stream, bootID, "
            "monotonic, tickEpoch, timeConfidence, readLatencyMs, tickJitterMs, "
            "syncRttMs, sourceKey "
            "FROM SensorLog WHERE uploaded = 0"
        )
        params = []

        if stream is not None:
            query += " AND stream = ?"
            params.append(stream)

        if source_key is not None:
            query += " AND sourceKey = ?"
            params.append(source_key)

        query += " ORDER BY logID ASC LIMIT ?"
        params.append(int(limit))

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def count_unsent(stream=None, source_key=None):
    conn = _connect()
    try:
        query = "SELECT COUNT(*) FROM SensorLog WHERE uploaded = 0"
        params = []

        if stream is not None:
            query += " AND stream = ?"
            params.append(stream)

        if source_key is not None:
            query += " AND sourceKey = ?"
            params.append(source_key)

        return conn.execute(query, params).fetchone()[0]
    finally:
        conn.close()


def mark_uploaded(record_id):
    mark_uploaded_many([record_id])


def mark_uploaded_many(record_ids):
    ids = list(record_ids)

    if not ids:
        return

    conn = _connect()
    try:
        placeholders = ", ".join("?" for _ in ids)
        conn.execute(
            f"UPDATE SensorLog SET uploaded = 1 WHERE logID IN ({placeholders})",
            ids,
        )
        conn.commit()
    finally:
        conn.close()


def delete_many(record_ids):
    """Drop rows outright instead of marking them uploaded.

    The delete-on-upload half of the retention setting (cleanup_days = 0).
    Only ever called with IDs the server answered 200 for, so this deletes
    nothing the backend has not acknowledged - the risk it accepts is not lost
    uploads, it is a backend that loses the rows AFTER acknowledging them (a
    restore from an old dump, a Pi pointed at the wrong database). Retention
    above zero is what covers that; see BackendClient.cleanup_days.
    """
    ids = list(record_ids)

    if not ids:
        return

    conn = _connect()
    try:
        placeholders = ", ".join("?" for _ in ids)
        conn.execute(
            f"DELETE FROM SensorLog WHERE logID IN ({placeholders})", ids
        )
        conn.commit()
    finally:
        conn.close()


def correct_boot_timestamps(boot_id, ref_monotonic, ref_epoch):
    """Rewrite timestamps for rows taken before the clock was synced.

    The offline-first requirement and time sync genuinely conflict at boot:
    with no network there is no correct time, but collection must continue
    anyway. The monotonic clock bridges the two::

        true_epoch(m) = ref_epoch + (m - ref_monotonic)

    Only un-uploaded rows of the current boot are touched. Rows already sent
    are the server's problem, not the cache's.

    Returns the number of rows corrected.
    """
    if boot_id is None or ref_monotonic is None or ref_epoch is None:
        return 0

    conn = _connect()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT logID, monotonic FROM SensorLog "
            "WHERE uploaded = 0 AND bootID = ? AND monotonic IS NOT NULL "
            "AND timeConfidence = ?",
            (boot_id, ESTIMATED),
        ).fetchall()

        updates = []
        for row in rows:
            true_epoch = ref_epoch + (row["monotonic"] - ref_monotonic)
            updates.append((
                datetime.fromtimestamp(true_epoch).strftime("%Y-%m-%d %H:%M:%S"),
                true_epoch,
                CORRECTED,
                row["logID"],
            ))

        if updates:
            conn.executemany(
                "UPDATE SensorLog SET datetime = ?, tickEpoch = ?, "
                "timeConfidence = ? WHERE logID = ?",
                updates,
            )
            conn.commit()

        return len(updates)
    finally:
        conn.close()


def cleanup(days=30, stream=None):
    """Delete uploaded rows older than `days`.

    Not filtered by sourceKey, unlike the drain: everything it touches has
    already been acknowledged by the server, so there is no ownership question
    left to answer - and rows retired by _migrate(), which have no sourceKey at
    all, would otherwise never be collected.

    days = 0 makes the cutoff `now`, which sweeps every uploaded row. That is
    the belt to delete_many()'s braces: it also clears rows marked uploaded
    before retention was set to zero.
    """
    # Rows are written with local time (datetime.now()), so the cutoff has to
    # be local too. utcnow() here put the cutoff 7 hours off at UTC+7.
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    conn = _connect()
    try:
        query = "DELETE FROM SensorLog WHERE uploaded = 1 AND datetime < ?"
        params = [cutoff_str]

        if stream is not None:
            query += " AND stream = ?"
            params.append(stream)

        conn.execute(query, params)
        conn.commit()
    finally:
        conn.close()
