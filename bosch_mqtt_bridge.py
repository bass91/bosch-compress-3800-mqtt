#!/usr/bin/env python3
"""Bosch LAN to MQTT bridge.

First pragmatic bridge:
- poll direct /heatSources/... values for live telemetry
- poll recording buckets for energy/runtime summaries
- publish Home Assistant MQTT discovery and state topics
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from bosch_local_probe import BoschHttpClient


LOG = logging.getLogger("bosch_mqtt_bridge")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge Bosch LAN values to MQTT.")
    parser.add_argument("--host", default=os.getenv("BOSCH_HOST"), required=os.getenv("BOSCH_HOST") is None)
    parser.add_argument(
        "--token",
        default=os.getenv("BOSCH_ACCESS_TOKEN"),
        required=os.getenv("BOSCH_ACCESS_TOKEN") is None,
        help="Use Home Assistant access_key here if available.",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("BOSCH_PASSWORD"),
        help="Only needed if token is the sticker token, not the derived access_key.",
    )
    parser.add_argument("--timeout", type=int, default=int(os.getenv("BOSCH_TIMEOUT", "10")))
    parser.add_argument("--mqtt-host", default=os.getenv("MQTT_HOST"), required=os.getenv("MQTT_HOST") is None)
    parser.add_argument("--mqtt-port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    parser.add_argument("--mqtt-user", default=os.getenv("MQTT_USER"))
    parser.add_argument("--mqtt-password", default=os.getenv("MQTT_PASSWORD"))
    parser.add_argument("--mqtt-client-id", default=os.getenv("MQTT_CLIENT_ID", "bosch-bridge"))
    parser.add_argument("--mqtt-topic-prefix", default=os.getenv("MQTT_TOPIC_PREFIX", "bosch"))
    parser.add_argument("--ha-discovery-prefix", default=os.getenv("HA_DISCOVERY_PREFIX", "homeassistant"))
    parser.add_argument("--live-interval", type=int, default=int(os.getenv("BOSCH_LIVE_INTERVAL", "30")))
    parser.add_argument("--recordings-interval", type=int, default=int(os.getenv("BOSCH_RECORDINGS_INTERVAL", "3600")))
    parser.add_argument(
        "--state-path",
        default=os.getenv("BOSCH_STATE_PATH", "bridge_state.json"),
        help="Persistent JSON state path for cumulative totals.",
    )
    parser.add_argument(
        "--cumulative-backfill-days",
        type=int,
        default=int(os.getenv("BOSCH_CUMULATIVE_BACKFILL_DAYS", "7")),
        help="How many local days to scan when building cumulative totals.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("BOSCH_TIMEZONE", os.getenv("TZ", "UTC")),
        help="Timezone used for daily recording rollovers, for example Europe/Stockholm.",
    )
    parser.add_argument("--once", action="store_true", help="Run one poll cycle and exit.")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return parser.parse_args()


@dataclass(frozen=True)
class LiveSensorSpec:
    object_id: str
    path: str
    name: str
    unit: str | None = None
    device_class: str | None = None
    state_class: str | None = None
    icon: str | None = None
    entity_category: str | None = None


LIVE_SENSOR_SPECS = [
    LiveSensorSpec("actual_modulation", "/heatSources/actualModulation", "Actual Modulation", "%", state_class="measurement"),
    LiveSensorSpec("ch_pump_modulation", "/heatSources/CHpumpModulation", "CH Pump Modulation", "%", state_class="measurement"),
    LiveSensorSpec("actual_supply_temperature", "/heatSources/actualSupplyTemperature", "Actual Supply Temperature", "C", device_class="temperature", state_class="measurement"),
    LiveSensorSpec("appliance_supply_temperature", "/heatSources/applianceSupplyTemperature", "Appliance Supply Temperature", "C", device_class="temperature", state_class="measurement"),
    LiveSensorSpec("return_temperature", "/heatSources/returnTemperature", "Return Temperature", "C", device_class="temperature", state_class="measurement"),
    LiveSensorSpec("supply_temperature_setpoint", "/heatSources/supplyTemperatureSetpoint", "Supply Temperature Setpoint", "C", device_class="temperature", state_class="measurement"),
    LiveSensorSpec("power_setpoint_pct", "/heatSources/powerSetpoint", "Power Setpoint", "%", state_class="measurement"),
    LiveSensorSpec("number_of_starts", "/heatSources/numberOfStarts", "Number Of Starts", state_class="total_increasing"),
    LiveSensorSpec("working_time_total_system_s", "/heatSources/workingTime/totalSystem", "Working Time Total System", "s", state_class="total_increasing"),
]

LIVE_BINARY_SPECS = [
    {
        "object_id": "flame_status",
        "path": "/heatSources/flameStatus",
        "name": "Flame Status",
        "payload_on": "on",
        "payload_off": "off",
    },
    {
        "object_id": "chimney_sweeper",
        "path": "/heatSources/ChimneySweeper",
        "name": "Chimney Sweeper",
        "payload_on": "on",
        "payload_off": "off",
    },
]

RECORDING_SPECS = [
    {
        "slug": "compressor",
        "path": "/recordings/heatSources/total/energyMonitoring/compressor",
        "name": "Compressor",
        "kind": "compressor",
    },
    {
        "slug": "consumed_energy",
        "path": "/recordings/heatSources/total/energyMonitoring/consumedEnergy",
        "name": "Consumed Energy",
        "kind": "energy",
    },
    {
        "slug": "eheater_energy",
        "path": "/recordings/heatSources/total/energyMonitoring/eheater",
        "name": "E-Heater Energy",
        "kind": "energy",
    },
    {
        "slug": "output_produced",
        "path": "/recordings/heatSources/total/energyMonitoring/outputProduced",
        "name": "Output Produced",
        "kind": "energy",
    },
]


def import_mqtt():
    try:
        import paho.mqtt.client as mqtt
    except ImportError as err:  # pragma: no cover - env specific
        raise SystemExit("Missing dependency paho-mqtt. Install with: .venv/bin/pip install paho-mqtt") from err
    return mqtt


def clean_unit(unit: str | None) -> str | None:
    if not unit:
        return None
    if unit == "C":
        return "°C"
    if unit.strip() == "":
        return None
    return unit


def get_last_full_hour_reference(now: datetime) -> tuple[date, int]:
    target = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return target.date(), target.hour


def load_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as err:
        raise SystemExit(
            f"Unknown timezone '{timezone_name}'. Use an IANA name like Europe/Stockholm."
        ) from err


def safe_div(y_value: float | int, count: float | int) -> float | None:
    if not count:
        return None
    return round(float(y_value) / float(count), 3)


def sum_buckets(recording: list[dict[str, Any]], end_idx: int) -> float:
    total = 0.0
    for idx, bucket in enumerate(recording):
        if idx > end_idx:
            break
        value = safe_div(bucket.get("y", 0), bucket.get("c", 0))
        if value is not None:
            total += value
    return round(total, 3)


def clamp_end_idx(recording: list[dict[str, Any]], end_idx: int | None) -> int:
    if not recording:
        return -1
    if end_idx is None:
        return len(recording) - 1
    return min(end_idx, len(recording) - 1)


class BridgeStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.state = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"version": 1, "sensors": {}}
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict) and isinstance(payload.get("sensors"), dict):
                return payload
        except Exception as err:
            LOG.warning("Failed to load state file %s: %s", self.path, err)
        return {"version": 1, "sensors": {}}

    def _sensor_state(self, slug: str) -> dict[str, Any]:
        sensors = self.state.setdefault("sensors", {})
        sensor = sensors.setdefault(slug, {"total": 0.0, "buckets": {}})
        sensor.setdefault("buckets", {})
        sensor.setdefault("total", 0.0)
        return sensor

    def apply_buckets(
        self,
        slug: str,
        bucket_date: date,
        recording: list[dict[str, Any]],
        end_idx: int | None,
    ) -> tuple[float, bool]:
        sensor = self._sensor_state(slug)
        buckets = sensor["buckets"]
        total = float(sensor.get("total", 0.0))
        changed = False
        final_idx = clamp_end_idx(recording, end_idx)
        for idx, bucket in enumerate(recording):
            if idx > final_idx:
                break
            value = safe_div(bucket.get("y", 0), bucket.get("c", 0))
            if value is None:
                continue
            bucket_key = f"{bucket_date.isoformat()}T{idx:02d}"
            previous = buckets.get(bucket_key)
            if previous is None:
                buckets[bucket_key] = value
                total += value
                changed = True
                continue
            previous_value = float(previous)
            if round(previous_value, 3) != value:
                buckets[bucket_key] = value
                total += value - previous_value
                changed = True
        sensor["total"] = round(total, 3)
        return sensor["total"], changed

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=self.path.parent, delete=False) as handle:
            json.dump(self.state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            tmp_path = Path(handle.name)
        tmp_path.replace(self.path)


class HomeAssistantMqttPublisher:
    def __init__(
        self,
        mqtt: Any,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
        client_id: str,
        discovery_prefix: str,
        topic_prefix: str,
        device: dict[str, Any],
    ) -> None:
        self.discovery_prefix = discovery_prefix.rstrip("/")
        self.topic_prefix = topic_prefix.strip("/")
        self.device = device
        callback_api = getattr(mqtt, "CallbackAPIVersion", None)
        if callback_api is not None:
            self.client = mqtt.Client(
                callback_api_version=callback_api.VERSION2,
                client_id=client_id,
                clean_session=True,
            )
        else:  # pragma: no cover - pre-paho-2 fallback
            self.client = mqtt.Client(client_id=client_id, clean_session=True)
        if username:
            self.client.username_pw_set(username, password=password)
        self.client.enable_logger(LOG)
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()

    def stop(self) -> None:
        try:
            self.client.loop_stop()
        finally:
            self.client.disconnect()

    def _publish(self, topic: str, payload: Any, retain: bool = True) -> None:
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        elif payload is None:
            payload = ""
        else:
            payload = str(payload)
        info = self.client.publish(topic, payload, qos=0, retain=retain)
        info.wait_for_publish()

    def publish_availability(self, online: bool) -> None:
        self._publish(
            f"{self.topic_prefix}/{self.device['identifiers'][0]}/availability",
            "online" if online else "offline",
        )

    def discovery_sensor(self, component: str, object_id: str, payload: dict[str, Any]) -> None:
        topic = f"{self.discovery_prefix}/{component}/{self.device['identifiers'][0]}/{object_id}/config"
        payload["availability_topic"] = f"{self.topic_prefix}/{self.device['identifiers'][0]}/availability"
        payload["payload_available"] = "online"
        payload["payload_not_available"] = "offline"
        payload["device"] = self.device
        self._publish(topic, payload)

    def publish_state(self, object_id: str, state: Any, attributes: dict[str, Any] | None = None) -> None:
        base = f"{self.topic_prefix}/{self.device['identifiers'][0]}/{object_id}"
        self._publish(f"{base}/state", state)
        if attributes is not None:
            self._publish(f"{base}/attributes", attributes)


def build_device_info(client: BoschHttpClient) -> dict[str, Any]:
    uuid_payload = client.get("/gateway/uuid")
    firmware_payload = client.get("/gateway/versionFirmware")
    hardware_payload = client.get("/gateway/versionHardware")
    uuid = str(uuid_payload.get("value"))
    return {
        "identifiers": [uuid],
        "manufacturer": "Bosch",
        "model": str(hardware_payload.get("value", "unknown")),
        "name": f"Bosch {uuid}",
        "sw_version": str(firmware_payload.get("value", "unknown")),
        "configuration_url": f"http://{client.host}",
    }


def publish_discovery(publisher: HomeAssistantMqttPublisher) -> None:
    for spec in LIVE_SENSOR_SPECS:
        payload = {
            "name": spec.name,
            "unique_id": f"{publisher.device['identifiers'][0]}_{spec.object_id}",
            "state_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{spec.object_id}/state",
            "json_attributes_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{spec.object_id}/attributes",
        }
        unit = clean_unit(spec.unit)
        if unit:
            payload["unit_of_measurement"] = unit
        if spec.device_class:
            payload["device_class"] = spec.device_class
        if spec.state_class:
            payload["state_class"] = spec.state_class
        if spec.icon:
            payload["icon"] = spec.icon
        if spec.entity_category:
            payload["entity_category"] = spec.entity_category
        publisher.discovery_sensor("sensor", spec.object_id, payload)

    for spec in LIVE_BINARY_SPECS:
        payload = {
            "name": spec["name"],
            "unique_id": f"{publisher.device['identifiers'][0]}_{spec['object_id']}",
            "state_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{spec['object_id']}/state",
            "json_attributes_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{spec['object_id']}/attributes",
            "payload_on": spec["payload_on"],
            "payload_off": spec["payload_off"],
        }
        publisher.discovery_sensor("binary_sensor", spec["object_id"], payload)

    summary_specs = [
        ("compressor_duty_last_hour_pct", "Compressor Duty Last Hour", "%", None, "measurement"),
        ("compressor_runtime_today_hours", "Compressor Runtime Today", "h", None, "total"),
        ("consumed_energy_last_hour_kwh", "Consumed Energy Last Hour", "kWh", "energy", "measurement"),
        ("consumed_energy_today_kwh", "Consumed Energy Today", "kWh", "energy", "total"),
        ("eheater_energy_last_hour_kwh", "E-Heater Energy Last Hour", "kWh", "energy", "measurement"),
        ("eheater_energy_today_kwh", "E-Heater Energy Today", "kWh", "energy", "total"),
        ("output_produced_last_hour_kwh", "Output Produced Last Hour", "kWh", "energy", "measurement"),
        ("output_produced_today_kwh", "Output Produced Today", "kWh", "energy", "total"),
    ]
    for object_id, name, unit, device_class, state_class in summary_specs:
        payload = {
            "name": name,
            "unique_id": f"{publisher.device['identifiers'][0]}_{object_id}",
            "state_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{object_id}/state",
            "json_attributes_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{object_id}/attributes",
            "unit_of_measurement": unit,
        }
        if device_class:
            payload["device_class"] = device_class
        if state_class:
            payload["state_class"] = state_class
        publisher.discovery_sensor("sensor", object_id, payload)

    cumulative_specs = [
        ("compressor_runtime_total_hours", "Compressor Runtime Total", "h", None, "total_increasing"),
        ("consumed_energy_total_kwh", "Consumed Energy Total", "kWh", "energy", "total_increasing"),
        ("eheater_energy_total_kwh", "E-Heater Energy Total", "kWh", "energy", "total_increasing"),
        ("output_produced_total_kwh", "Output Produced Total", "kWh", "energy", "total_increasing"),
    ]
    for object_id, name, unit, device_class, state_class in cumulative_specs:
        payload = {
            "name": name,
            "unique_id": f"{publisher.device['identifiers'][0]}_{object_id}",
            "state_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{object_id}/state",
            "json_attributes_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{object_id}/attributes",
            "unit_of_measurement": unit,
        }
        if device_class:
            payload["device_class"] = device_class
        if state_class:
            payload["state_class"] = state_class
        publisher.discovery_sensor("sensor", object_id, payload)

    diagnostics = [
        ("raw_energy_monitoring_consumption", "Raw Energy Monitoring Consumption", "kWh"),
        ("raw_energy_monitoring_start_datetime", "Raw Energy Monitoring Start", None),
    ]
    for object_id, name, unit in diagnostics:
        payload = {
            "name": name,
            "unique_id": f"{publisher.device['identifiers'][0]}_{object_id}",
            "state_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{object_id}/state",
            "json_attributes_topic": f"{publisher.topic_prefix}/{publisher.device['identifiers'][0]}/{object_id}/attributes",
            "entity_category": "diagnostic",
        }
        if unit:
            payload["unit_of_measurement"] = unit
        publisher.discovery_sensor("sensor", object_id, payload)


def publish_live_values(client: BoschHttpClient, publisher: HomeAssistantMqttPublisher) -> None:
    for spec in LIVE_SENSOR_SPECS:
        try:
            payload = client.get(spec.path)
            publisher.publish_state(
                spec.object_id,
                payload.get("value"),
                {
                    "path": spec.path,
                    "unit_of_measure": payload.get("unitOfMeasure"),
                    "type": payload.get("type"),
                    "recordable": payload.get("recordable"),
                    "writeable": payload.get("writeable"),
                },
            )
        except Exception as err:
            LOG.warning("Failed to read %s: %s", spec.path, err)

    for spec in LIVE_BINARY_SPECS:
        try:
            payload = client.get(spec["path"])
            publisher.publish_state(
                spec["object_id"],
                payload.get("value"),
                {
                    "path": spec["path"],
                    "allowed_values": payload.get("allowedValues"),
                },
            )
        except Exception as err:
            LOG.warning("Failed to read %s: %s", spec["path"], err)

    try:
        consumption = client.get("/heatSources/energyMonitoring/consumption")
        publisher.publish_state(
            "raw_energy_monitoring_consumption",
            consumption.get("value"),
            {"path": "/heatSources/energyMonitoring/consumption", "raw": consumption},
        )
    except Exception as err:
        LOG.warning("Failed to read raw energyMonitoring/consumption: %s", err)

    try:
        start_time = client.get("/heatSources/energyMonitoring/startDateTime")
        publisher.publish_state(
            "raw_energy_monitoring_start_datetime",
            start_time.get("value"),
            {"path": "/heatSources/energyMonitoring/startDateTime", "raw": start_time},
        )
    except Exception as err:
        LOG.warning("Failed to read raw energyMonitoring/startDateTime: %s", err)


def fetch_recording_for_day(client: BoschHttpClient, path: str, target_date: date) -> dict[str, Any]:
    return client.get(f"{path}?interval={target_date.isoformat()}")


def date_range_inclusive(end_date: date, days: int) -> list[date]:
    days = max(days, 1)
    start_date = end_date - timedelta(days=days - 1)
    return [start_date + timedelta(days=offset) for offset in range(days)]


def cutoff_for_bucket_date(bucket_date: date, target_date: date, last_hour_index: int) -> int | None:
    if bucket_date < target_date:
        return None
    if bucket_date == target_date:
        return last_hour_index
    return -1


def publish_cumulative_totals(
    client: BoschHttpClient,
    publisher: HomeAssistantMqttPublisher,
    state_store: BridgeStateStore,
    bridge_tz: ZoneInfo,
    backfill_days: int,
) -> None:
    now = datetime.now(bridge_tz)
    target_date, last_hour_index = get_last_full_hour_reference(now)
    state_changed = False

    for bucket_date in date_range_inclusive(target_date, backfill_days):
        end_idx = cutoff_for_bucket_date(bucket_date, target_date, last_hour_index)
        if end_idx == -1:
            continue
        for spec in RECORDING_SPECS:
            try:
                payload = fetch_recording_for_day(client, spec["path"], bucket_date)
            except Exception as err:
                LOG.warning("Failed cumulative read %s for %s: %s", spec["path"], bucket_date, err)
                continue
            recording = payload.get("recording", [])
            if not recording:
                continue
            total_value, changed = state_store.apply_buckets(
                slug=spec["slug"],
                bucket_date=bucket_date,
                recording=recording,
                end_idx=end_idx,
            )
            state_changed = state_changed or changed
            object_id = (
                "compressor_runtime_total_hours"
                if spec["slug"] == "compressor"
                else f"{spec['slug']}_total_kwh"
            )
            publisher.publish_state(
                object_id,
                total_value,
                {
                    "path": payload["recordedResource"]["id"],
                    "bridge_timezone": str(bridge_tz),
                    "state_path": str(state_store.path),
                    "backfill_days": backfill_days,
                    "latest_processed_date": bucket_date.isoformat(),
                    "latest_processed_hour": end_idx if end_idx is not None else len(recording) - 1,
                },
            )

    if state_changed:
        state_store.save()


def publish_recording_summaries(
    client: BoschHttpClient,
    publisher: HomeAssistantMqttPublisher,
    bridge_tz: ZoneInfo,
) -> None:
    now = datetime.now(bridge_tz)
    target_date, last_hour_index = get_last_full_hour_reference(now)
    raw_payloads: dict[str, dict[str, Any]] = {}

    for spec in RECORDING_SPECS:
        try:
            raw_payloads[spec["slug"]] = fetch_recording_for_day(client, spec["path"], target_date)
        except Exception as err:
            LOG.warning("Failed to read recording %s: %s", spec["path"], err)

    compressor = raw_payloads.get("compressor")
    if not compressor:
        return
    compressor_buckets = compressor.get("recording", [])
    if compressor_buckets and last_hour_index < len(compressor_buckets):
        bucket = compressor_buckets[last_hour_index]
        duty_ratio = safe_div(bucket.get("y", 0), bucket.get("c", 0))
        runtime_today_hours = round(sum_buckets(compressor_buckets, last_hour_index), 3)
        publisher.publish_state(
            "compressor_duty_last_hour_pct",
            None if duty_ratio is None else round(duty_ratio * 100, 2),
            {
                "path": compressor["recordedResource"]["id"],
                "bridge_timezone": str(bridge_tz),
                "bucket_date": target_date.isoformat(),
                "bucket_hour": last_hour_index,
                "raw_bucket": bucket,
            },
        )
        publisher.publish_state(
            "compressor_runtime_today_hours",
            runtime_today_hours,
            {
                "path": compressor["recordedResource"]["id"],
                "bridge_timezone": str(bridge_tz),
                "bucket_date": target_date.isoformat(),
                "last_hour_index": last_hour_index,
            },
        )

    for spec in [x for x in RECORDING_SPECS if x["kind"] == "energy"]:
        payload = raw_payloads.get(spec["slug"])
        if not payload:
            continue
        buckets = payload.get("recording", [])
        if not buckets or last_hour_index >= len(buckets):
            continue
        bucket = buckets[last_hour_index]
        last_hour_value = safe_div(bucket.get("y", 0), bucket.get("c", 0))
        today_total = sum_buckets(buckets, last_hour_index)
        publisher.publish_state(
            f"{spec['slug']}_last_hour_kwh",
            last_hour_value,
            {
                "path": payload["recordedResource"]["id"],
                "bridge_timezone": str(bridge_tz),
                "bucket_date": target_date.isoformat(),
                "bucket_hour": last_hour_index,
                "raw_bucket": bucket,
            },
        )
        publisher.publish_state(
            f"{spec['slug']}_today_kwh",
            today_total,
            {
                "path": payload["recordedResource"]["id"],
                "bridge_timezone": str(bridge_tz),
                "bucket_date": target_date.isoformat(),
                "last_hour_index": last_hour_index,
            },
        )


def run_bridge(args: argparse.Namespace) -> int:
    mqtt = import_mqtt()
    bridge_tz = load_timezone(args.timezone)
    state_store = BridgeStateStore(Path(args.state_path).expanduser().resolve())
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    LOG.info("Using recording timezone %s", bridge_tz)
    LOG.info("Using cumulative state file %s", state_store.path)

    client = BoschHttpClient(
        host=args.host,
        token=args.token,
        password=args.password,
        timeout=args.timeout,
    )
    device = build_device_info(client)
    topic_prefix = f"{args.mqtt_topic_prefix.strip('/')}/{device['identifiers'][0]}"
    publisher = HomeAssistantMqttPublisher(
        mqtt=mqtt,
        host=args.mqtt_host,
        port=args.mqtt_port,
        username=args.mqtt_user,
        password=args.mqtt_password,
        client_id=args.mqtt_client_id,
        discovery_prefix=args.ha_discovery_prefix,
        topic_prefix=args.mqtt_topic_prefix.strip("/"),
        device=device,
    )

    LOG.info("Publishing discovery for Bosch %s on mqtt://%s:%s", device["identifiers"][0], args.mqtt_host, args.mqtt_port)
    publish_discovery(publisher)
    publisher.publish_availability(True)

    next_live = 0.0
    next_recordings = 0.0
    stop_requested = False

    def request_stop(signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        LOG.info("Received signal %s, stopping bridge", signum)
        stop_requested = True

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        while not stop_requested:
            now = time.monotonic()
            if now >= next_live:
                LOG.info("Polling live Bosch values")
                try:
                    publish_live_values(client, publisher)
                except Exception:
                    LOG.exception("Live poll failed")
                next_live = now + max(args.live_interval, 5)

            if now >= next_recordings:
                LOG.info("Polling recording summaries")
                try:
                    publish_recording_summaries(client, publisher, bridge_tz)
                    publish_cumulative_totals(
                        client,
                        publisher,
                        state_store,
                        bridge_tz,
                        args.cumulative_backfill_days,
                    )
                except Exception:
                    LOG.exception("Recording poll failed")
                next_recordings = now + max(args.recordings_interval, 300)

            if args.once:
                break
            time.sleep(1)
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        publisher.publish_availability(False)
        publisher.stop()
        LOG.info("Stopped mqtt bridge for topic prefix %s", topic_prefix)
    return 0


def main() -> int:
    args = parse_args()
    return run_bridge(args)


if __name__ == "__main__":
    raise SystemExit(main())
