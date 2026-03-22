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
```

## Run

```bash
docker compose up -d --build
```

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

## Notes

- Live values are polled every `30s` by default.
- Recording summaries are polled every `3600s` by default.
- Energy values are based on Bosch recording buckets, not live monotonic counters.
