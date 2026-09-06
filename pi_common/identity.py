"""One UUID per physical Pi."""

import uuid
from pathlib import Path


def get_device_uuid(path):
    """Read the device UUID, minting one on first call.

    ONE UUID PER PHYSICAL PI, shared by every client on the box. The backend
    identifies a sensor by (deviceUUID, type, location), so the same UUID with
    different types registers as different sensors on one Device row - which is
    what makes "this Pi lost power" a single fact rather than one per sensor.

    Sharing needs no effort now: the Pi's directory is FLAT, so every client
    resolves this to the same device_uuid.txt by layout. It is minted on first
    call by whichever client starts first, and the rest read it.
    """
    path = Path(path)

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(uuid.uuid4()))

    return path.read_text().strip()
