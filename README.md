# Link

The project ships as four repos:

| Part | Repo | Latest release |
| ---- | ---- | -------------- |
| Dashboard + database | [sp_dashboard](https://github.com/Korawit-chn/sp_dashboard) | [v2.4.0](https://github.com/Korawit-chn/sp_dashboard/tree/v2.4.0) |
| Pi sensor node | [sp_piSensor](https://github.com/Korawit-chn/sp_piSensor) | [v2.2.0](https://github.com/Korawit-chn/sp_piSensor/tree/v2.2.0) |
| Pi actuator node | [sp_piActuator](https://github.com/Korawit-chn/sp_piActuator) | [v1.2.0](https://github.com/Korawit-chn/sp_piActuator/tree/v1.2.0) |
| Shared Pi environment **(this repo)** | [sp_piAll](https://github.com/Korawit-chn/sp_piAll) | [v1.0.0](https://github.com/Korawit-chn/sp_piAll/tree/v1.0.0) |

# Shared Pi environment

Everything that runs on **every** Pi, sensor box or actuator box or both.
Deployed flat on the Pi alongside the node scripts from the other two repos.

| Path | |
|------|--|
| `pi_common/` | The library every client imports: config parsing, backend discovery, the offline cache, the epoch-anchored tick scheduler, the sampling loop, clock measurement, device identity, VPD. |
| `device_agent.py` | Liveness agent. One per Pi, the only thing that reports device status. It registers nothing and reads no config. |
| `sync_clock.py` | Steps the system clock from the backend PC once at boot, as root, before anything is scheduled. |
| `deploy/` | systemd units (`device-agent.service`, `sensor-timesync.service`) and the optional `010-timesync-sudoers` drop-in. |
| `networkList.txt` | Candidate backend addresses, tried in order. |

## Why one library folder

`pi_common/` used to be two packages - `sensorVPD` in the sensor tree and
`pi_common` next to it. That boundary only existed because the working project
splits files by which device they go to. The Pi itself is flat, so the split
does not survive deployment; the code every node shares lives here once.

## Install

```
sudo apt update
sudo apt install python3-pip python3-venv

python3 -m venv ~/venv
source ~/venv/bin/activate
pip install requests
```

## Services

```
sudo cp deploy/device-agent.service /etc/systemd/system/
sudo cp deploy/sensor-timesync.service /etc/systemd/system/
sudo nano /etc/systemd/system/device-agent.service     # paths and the venv

sudo systemctl daemon-reload
sudo systemctl enable --now sensor-timesync.service
sudo systemctl enable --now device-agent.service
```

`sensor-timesync.service` is a root oneshot ordered **before** the sampling
services, so Pi logs and file mtimes are correct from boot.
`device-agent.service` runs as `pi` and needs no privileges - it measures the
clock offset and publishes it to `/run/sp_dashboard/clock.json`, it never steps
the system clock.

```
systemctl status device-agent
journalctl -u device-agent -f
timedatectl                    # "NTP service: inactive" is what we want
```
