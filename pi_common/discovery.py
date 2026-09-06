"""Backend discovery: the first address in networkList.txt that answers.

Was written three times - sensorVPD/network.py, relay_control.find_backend(),
and again inside real_fanapi_test.py.

NEVER RAISES. A missing, empty or unreadable list is an ordinary state on a Pi
that has not been set up yet, and on a Pi that has, the network is simply down
sometimes. Either way the caller's job is to keep retrying, so a failure here
returns None rather than throwing an exception that has to be caught at every
call site to avoid killing the maintenance cycle.
"""

import os
from pathlib import Path

import requests

DEFAULT_TIMEOUT = 5
DEFAULT_PORT = 5000

# Logged once per outage, not once per attempt. Discovery runs every few
# seconds while the backend is missing, and logging per attempt would put one
# identical line in the journal every time - same reasoning as the serial
# reopen logging in C5A.py.
_list_error_logged = False


def resolve_network_list(path, env_var=None):
    """The list file to read: `$env_var` if it is set, otherwise `path`.

    There is nothing cleverer to do. The Pi is FLAT - one directory holding the
    scripts, the config and the lib folders - so there is exactly ONE
    networkList.txt per machine, and every client resolves it to the same file
    by layout. No search order, no machine-wide copy under /etc, no symlink
    between folders: those all existed to reconcile the two per-folder copies
    that the repo's "PI sensor" / "Pi Actuator" split used to imply, and the
    split does not survive deployment.

    The env override stays because it is how you point one client at a
    different backend without touching the shared file - useful on the bench.
    """
    if env_var:
        override = os.environ.get(env_var)

        if override:
            return Path(override)

    return Path(path)


def candidates(filename):
    """Addresses from the list file, in order.

    Blank lines and `#` comments are skipped. Without that, a list written from
    the documented example - which is commented - turns every comment into a URL
    and burns one full timeout per line before the first real address is tried.
    """
    global _list_error_logged

    try:
        with open(filename, "r") as file:
            lines = file.readlines()
    except OSError as e:
        if not _list_error_logged:
            print(f"Cannot read {filename}: {e}")
            print("Create it with one backend address per line - check the "
                  "backend PC's address with ipconfig. Retrying meanwhile.")
            _list_error_logged = True
        return []

    hosts = [
        host for host in (raw.strip() for raw in lines)
        if host and not host.startswith("#")
    ]

    if not hosts:
        if not _list_error_logged:
            print(f"{filename} lists no addresses. Retrying meanwhile.")
            _list_error_logged = True
        return []

    # The file is readable again; let the next problem be reported.
    _list_error_logged = False

    return hosts


def network_search(filename, port, route, timeout=DEFAULT_TIMEOUT):
    """First listed address that answers with 200, or None if none does.

    Returns the full URL INCLUDING the route, which is what the sensor clients
    want. find_backend() returns the base URL instead.
    """
    for host in candidates(filename):
        url = f"http://{host}:{port}{route}"

        try:
            if requests.get(url, timeout=timeout).status_code == 200:
                return url

        except requests.RequestException:
            pass

    return None


def find_backend(filename, port=DEFAULT_PORT, timeout=3):
    """Base URL of the first address that answers GET /api/time, or None.

    Same search as network_search(), returning `http://host:port` rather than
    the probed URL - the actuator clients build many different routes off it.
    """
    for host in candidates(filename):
        base = f"http://{host}:{port}"

        try:
            if requests.get(f"{base}/api/time", timeout=timeout).status_code == 200:
                return base

        except requests.RequestException:
            pass

    return None
