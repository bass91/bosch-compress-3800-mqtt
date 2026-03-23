# Bosch MQTT Bridge

Small local Bosch `HTTP -> MQTT` bridge for Bosch/IVT heat pump gateways.

What it does:
- reads live values from local `/heatSources/...`
- reads hourly energy/runtime buckets from `/recordings/...`
- publishes Home Assistant MQTT discovery + states

What it does not do:
- no Bosch cloud
- no app reverse engineering
- no direct live power endpoint if your firmware does not expose one

## Requirements

- Docker
- MQTT broker
- Bosch gateway IP on your LAN
- Bosch `access_key` from Home Assistant Bosch config entry

Use the Home Assistant Bosch `access_key`, not the sticker token, if you have it.

## Config

Copy `.env.example` to `.env` and fill in values.

Example:

```env
BOSCH_HOST=192.168.68.145
BOSCH_ACCESS_TOKEN=your_home_assistant_bosch_access_key
MQTT_HOST=192.168.68.149
MQTT_PORT=1883
MQTT_USER=bosch
MQTT_PASSWORD=your_mqtt_password
BOSCH_CONTAINER_UID=1000
BOSCH_CONTAINER_GID=1000
BOSCH_STATE_PATH=/data/bridge_state.json
BOSCH_CUMULATIVE_BACKFILL_DAYS=7
BOSCH_TIMEZONE=Europe/Stockholm
```

## Run

```bash
docker compose up -d --build
```

This creates a local `./state` directory on the server for cumulative totals.
The container runs as your host UID/GID by default so `./state` stays writable.

Logs:

```bash
docker compose logs -f
```

Stop:

```bash
docker compose down
```

## Home Assistant

Set up the MQTT integration in Home Assistant against the same broker.
The bridge publishes MQTT discovery under `homeassistant/...`, so entities should appear automatically.

For Energy Dashboard, prefer the new cumulative MQTT sensors:
- `Consumed Energy Total`
- `E-Heater Energy Total`

These are built from Bosch hourly buckets but exposed as monotonic `total_increasing` sensors.

## Notes

- Live values are polled every `30s` by default.
- Recording summaries are polled every `3600s` by default.
- Daily recording rollovers use `BOSCH_TIMEZONE`, default `UTC`.
- Cumulative totals survive container restarts via `BOSCH_STATE_PATH`.
- Energy values are based on Bosch recording buckets, not live monotonic counters.
