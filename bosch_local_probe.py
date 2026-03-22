#!/usr/bin/env python3
"""Local Bosch probe for first bridge bring-up.

No MQTT here. Only discovery and raw JSON dumps.
Implements the same local HTTP auth/encryption logic verified in the public client.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


WORKSPACE = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = WORKSPACE / "probe_output"

MAGIC_IVT = bytearray.fromhex(
    "867845e97c4e29dce522b9a7d3a3e07b152bffadddbed7f5ffd842e9895ad1e4"
)

HTTP_HEADERS = {
    "User-Agent": "TeleHeater",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
}

ROOT_PATHS = [
    "/systemStates",
    "/dhwCircuits",
    "/gateway",
    "/heatingCircuits",
    "/heatSources",
    "/notifications",
    "/system",
    "/solarCircuits",
    "/recordings",
    "/devices",
    "/energy",
    "/events",
    "/programs",
    "/zones",
    "/ecus",
    "/application",
    "/gservice_tariff",
]

CORE_PATHS = [
    "/gateway",
    "/gateway/uuid",
    "/gateway/versionFirmware",
    "/gateway/versionHardware",
    "/gateway/DateTime",
    "/systemStates",
    "/heatSources",
    "/recordings",
    "/energy",
    "/system",
]

LIVE_CANDIDATE_PATHS = [
    "/heatSources/actualPower",
    "/heatSources/energyMonitoring/consumption",
    "/heatSources/energyMonitoring/startDateTime",
    "/heatSources/workingTime/totalSystem",
    "/heatSources/numberOfStarts",
    "/systemStates/compressor",
    "/systemStates/additionalHeater",
]

TARGET_ENTITY_TAILS = {
    "rcompressor": "compressor",
    "rconsumedEnergy": "consumedEnergy",
    "reheater": "reheater",
    "routputProduced": "outputProduced",
}

CANDIDATE_KEYWORDS = {
    "actualpower",
    "compressor",
    "consumedenergy",
    "consumption",
    "energymonitoring",
    "heatsource",
    "heatsources",
    "outputproduced",
    "power",
    "recordings",
    "reheater",
    "workingtime",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe a local Bosch IVT/MBLAN gateway and dump raw JSON."
    )
    parser.add_argument("--host", default=os.getenv("BOSCH_HOST"), required=os.getenv("BOSCH_HOST") is None)
    parser.add_argument(
        "--token",
        default=os.getenv("BOSCH_ACCESS_TOKEN"),
        required=os.getenv("BOSCH_ACCESS_TOKEN") is None,
        help="Sticker access token. Dashes accepted.",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("BOSCH_PASSWORD"),
        help="App password for key derivation.",
    )
    parser.add_argument(
        "--device-type",
        default=os.getenv("BOSCH_DEVICE_TYPE", "IVT"),
        choices=["IVT", "IVT_MBLAN"],
        help="Metadata only here. Local HTTP auth path is the same.",
    )
    parser.add_argument(
        "--interval-date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Date used for direct recording bucket pulls. Default yesterday.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory. Default probe_output/<timestamp>.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP timeout seconds.",
    )
    parser.add_argument(
        "--path",
        action="append",
        default=[],
        help="Extra raw path to query. Repeatable.",
    )
    parser.add_argument(
        "--full-rawscan",
        action="store_true",
        help="Also recurse through all known root trees. Slower.",
    )
    return parser.parse_args()


class IvtEncryption:
    """Minimal copy of the public client IVT encryption logic."""

    block_size = 16

    def __init__(self, access_token: str, password: str | None) -> None:
        access_token = access_token.replace("-", "")
        if password:
            key_hash = hashlib.md5(bytearray(access_token, "utf8") + MAGIC_IVT)
            password_hash = hashlib.md5(MAGIC_IVT + bytearray(password, "utf8"))
            saved_key = key_hash.hexdigest() + password_hash.hexdigest()
        else:
            saved_key = access_token
        self.key = saved_key
        self._key = binascii.unhexlify(saved_key)

    def _pad(self, raw: bytes) -> bytes:
        pad_len = self.block_size - (len(raw) % self.block_size)
        if pad_len == self.block_size:
            return raw
        return raw + (chr(0) * pad_len).encode("utf-8")

    def decrypt_json(self, raw: bytes) -> Any:
        from pyaes import AESModeOfOperationECB, Decrypter, PADDING_NONE

        if not raw:
            return None
        enc = base64.b64decode(raw)
        if len(enc) % self.block_size != 0:
            enc = self._pad(enc)
        cipher = Decrypter(AESModeOfOperationECB(self._key), padding=PADDING_NONE)
        decrypted = cipher.feed(enc) + cipher.feed()
        text = decrypted.decode("utf-8").rstrip(chr(0))
        return json.loads(text) if text else None


class BoschHttpClient:
    def __init__(self, host: str, token: str, password: str | None, timeout: int) -> None:
        self.host = host
        self.timeout = timeout
        self.encryption = IvtEncryption(token, password)

    def url(self, path: str) -> str:
        return f"http://{self.host}{path}"

    def get(self, path: str) -> Any:
        request = Request(self.url(path), headers=HTTP_HEADERS, method="GET")
        with urlopen(request, timeout=self.timeout) as response:
            payload = response.read()
            return self.encryption.decrypt_json(payload)


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip("/"))
    return sanitized or "root"


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Unsupported type: {type(value)!r}")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=json_default)
        handle.write("\n")


def iter_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from iter_dicts(nested)
    elif isinstance(value, list):
        for item in value:
            yield from iter_dicts(item)


def get_all_intervals() -> list[str]:
    yesterday = datetime.today() - timedelta(days=1)
    ytt = yesterday.timetuple()
    yttiso = yesterday.isocalendar()
    return [
        f"{ytt[0]}-{ytt[1]}-{ytt[2]}",
        f"{ytt[0]}-{ytt[1]}",
        f"{ytt[0]}-W{yttiso[1]}",
    ]


def deep_into(client: BoschHttpClient, path: str, seen: set[str] | None = None) -> list[Any]:
    seen = seen or set()
    if path in seen:
        return []
    seen.add(path)
    collected: list[Any] = []
    try:
        response = client.get(path)
    except Exception:
        return collected
    collected.append(response)
    if not isinstance(response, dict):
        return collected
    if "id" in response and "recordings" in str(response["id"]).lower() and "references" not in response:
        for interval in get_all_intervals():
            try:
                collected.append(client.get(f"{path}?interval={interval}"))
            except Exception:
                pass
    for reference in response.get("references", []):
        if isinstance(reference, dict) and "id" in reference:
            collected.extend(deep_into(client, str(reference["id"]), seen=seen))
    return collected


def collect_recording_mappings(payloads: list[Any]) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        for obj in iter_dicts(payload):
            recorded_resource = obj.get("recordedResource")
            if not isinstance(recorded_resource, dict):
                continue
            recorded_resource_id = recorded_resource.get("id")
            if not recorded_resource_id:
                continue
            tail = recorded_resource_id.rstrip("/").split("/")[-1]
            entity_id = f"r{tail}"
            found[entity_id] = {
                "entity_id": entity_id,
                "recording_path": obj.get("id"),
                "recorded_resource_id": recorded_resource_id,
                "recording_name": obj.get("name"),
            }
    return [found[key] for key in sorted(found)]


def collect_candidate_paths(payloads: list[Any]) -> list[str]:
    candidates: set[str] = set(LIVE_CANDIDATE_PATHS)
    for payload in payloads:
        for obj in iter_dicts(payload):
            value = obj.get("id")
            if isinstance(value, str) and value.startswith("/"):
                lowered = value.lower()
                if any(keyword in lowered for keyword in CANDIDATE_KEYWORDS):
                    candidates.add(value)
            recorded_resource = obj.get("recordedResource")
            if isinstance(recorded_resource, dict):
                value = recorded_resource.get("id")
                if isinstance(value, str) and value.startswith("/"):
                    lowered = value.lower()
                    if any(keyword in lowered for keyword in CANDIDATE_KEYWORDS):
                        candidates.add(value)
    return sorted(candidates)


def run_query(client: BoschHttpClient, path: str) -> dict[str, Any]:
    try:
        result = client.get(path)
        return {"ok": result is not None, "path": path, "response": result}
    except HTTPError as err:
        return {"ok": False, "path": path, "error": f"HTTP {err.code}: {err.reason}"}
    except URLError as err:
        return {"ok": False, "path": path, "error": repr(err.reason)}
    except Exception as err:
        return {"ok": False, "path": path, "error": repr(err)}


def main() -> int:
    args = parse_args()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else (DEFAULT_OUTPUT_ROOT / timestamp).resolve()
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    client = BoschHttpClient(
        host=args.host,
        token=args.token,
        password=args.password,
        timeout=args.timeout,
    )

    manifest: dict[str, Any] = {
        "started_at": datetime.now().isoformat(),
        "host": args.host,
        "device_type": args.device_type,
        "protocol": "HTTP",
        "interval_date": args.interval_date,
        "output_dir": str(output_dir),
        "connection": {},
        "recording_entity_targets": TARGET_ENTITY_TAILS,
        "attempted_queries": [],
        "http_headers": HTTP_HEADERS,
    }

    uuid_result = run_query(client, "/gateway/uuid")
    manifest["connection"]["uuid_probe"] = uuid_result
    write_json(output_dir / "queries" / "gateway_uuid.json", uuid_result)

    firmware_result = run_query(client, "/gateway/versionFirmware")
    manifest["connection"]["firmware_probe"] = firmware_result
    write_json(output_dir / "queries" / "gateway_versionFirmware.json", firmware_result)

    core_results = []
    for path in list(dict.fromkeys(CORE_PATHS + args.path)):
        result = run_query(client, path)
        manifest["attempted_queries"].append(path)
        core_results.append(result)
        write_json(output_dir / "queries" / f"{sanitize_filename(path)}.json", result)

    payloads_for_analysis: list[Any] = [
        result["response"] for result in core_results if result.get("response") is not None
    ]

    recordings_scan = deep_into(client, "/recordings")
    write_json(output_dir / "recordings_scan.json", recordings_scan)
    payloads_for_analysis.append(recordings_scan)

    if args.full_rawscan:
        rawscan = {}
        for root in ROOT_PATHS:
            rawscan[root] = deep_into(client, root)
        write_json(output_dir / "rawscan.json", rawscan)
        payloads_for_analysis.append(rawscan)

    recording_mappings = collect_recording_mappings(payloads_for_analysis)
    write_json(output_dir / "recording_entity_mappings.json", recording_mappings)
    manifest["recording_entity_mappings"] = recording_mappings
    manifest["recording_entity_matches"] = {
        entity_id: next(
            (row for row in recording_mappings if row["entity_id"] == entity_id),
            None,
        )
        for entity_id in sorted(TARGET_ENTITY_TAILS)
    }

    candidate_paths = collect_candidate_paths(payloads_for_analysis)
    manifest["candidate_paths"] = candidate_paths

    candidate_results = []
    for path in candidate_paths:
        final_path = (
            f"{path}?interval={args.interval_date}"
            if "/recordings/" in path and "?" not in path
            else path
        )
        result = run_query(client, final_path)
        candidate_results.append(result)
        write_json(
            output_dir / "candidate_queries" / f"{sanitize_filename(final_path)}.json",
            result,
        )
    write_json(output_dir / "candidate_queries.json", candidate_results)

    manifest["finished_at"] = datetime.now().isoformat()
    write_json(output_dir / "manifest.json", manifest)

    print(json.dumps({"ok": True, "output_dir": str(output_dir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
