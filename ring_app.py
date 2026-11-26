"""HTTP and fixture-runner adapters for :mod:`ring_service`.

The domain logic lives in :class:`ring_service.LoopRingService`.  This module
adds a dependency-free JSON/raw-byte HTTP interface and a deterministic runner
for the checked-in fixture events.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from ring_service import (
    DeviceWriteError,
    LoopRingService,
    MemoryBlockDevice,
    FileBlockDevice,
    HttpRingDevice,
    RingError,
    sha256_bytes,
)


def json_safe(value: Any) -> Any:
    """Convert bytes and arbitrary exception objects into JSON-safe values."""
    if isinstance(value, bytes):
        return {"__type__": "bytes", "sha256": sha256_bytes(value),
                "length": len(value), "data_b64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    return str(value)


def json_dumps(value: Any) -> bytes:
    return json.dumps(json_safe(value), ensure_ascii=False, sort_keys=True).encode("utf-8")


def load_fixture(fixture_dir: str | Path) -> tuple[dict[str, Any], dict[str, bytes]]:
    root = Path(fixture_dir)
    record = json.loads((root / "events.json").read_text("utf-8"))
    objects = {p.name: p.read_bytes() for p in (root / "objects").iterdir() if p.is_file()}
    return record, objects


def install_packet_faults(device: Any, packets: list[dict[str, Any]], objects: dict[str, bytes]) -> None:
    """Emulate the partial/disconnect packets recorded in events.json."""
    rules: list[dict[str, Any]] = []
    for packet in packets:
        match = re.fullmatch(r"/ring/(\d+)", packet.get("path", ""))
        if packet.get("method") != "PUT" or not match:
            continue
        body_name = packet.get("body")
        body = objects.get(body_name, b"") if body_name else b""
        reply = packet.get("reply") or {}
        rules.append({
            "offset": int(match.group(1)),
            "sha256": sha256_bytes(body) if body else None,
            "written": reply.get("written"),
            "drop": bool(packet.get("drop")),
        })

    def fault(offset: int, data: bytes) -> int | None:
        for rule in rules:
            if rule["offset"] != offset or (rule["sha256"] and rule["sha256"] != sha256_bytes(data)):
                continue
            written = rule["written"]
            if written is None:
                # A drop without a report leaves the amount unknowable.  The
                # device has nevertheless received the complete request body in
                # this synthetic adapter; use zero confirmed bytes safely.
                if rule["drop"]:
                    raise DeviceWriteError("scripted disconnect", written=None,
                                           state="disconnected", drop=True)
                return None
            written = max(0, min(int(written), len(data)))
            if rule["drop"]:
                raise DeviceWriteError("scripted partial write then disconnect",
                                       written=written, state="partial", drop=True,
                                       response={"state": "partial", "written": written})
            return written
        return None

    device.fault = fault


def build_fixture_service(
    fixture_dir: str | Path,
    state_dir: str | Path | None = None,
    *,
    durable: bool = True,
    script_faults: bool = True,
) -> tuple[LoopRingService, dict[str, Any], dict[str, bytes]]:
    record, objects = load_fixture(fixture_dir)
    policy = record["policy"]
    if state_dir is None:
        state_dir = tempfile.mkdtemp(prefix="loop-ring-fixture-")
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    device: Any
    if durable:
        device = FileBlockDevice(state_dir / "ring.img", int(policy["capacity"]))
    else:
        device = MemoryBlockDevice(int(policy["capacity"]))
    if script_faults:
        install_packet_faults(device, record.get("packets", []), objects)
    service = LoopRingService(policy=policy, data_dir=state_dir, block_device=device)
    # If this is a fresh state directory, add initial facts.  On replay the
    # durable state already contains them; add_initial_segment is identity-safe.
    for item in policy.get("initial", []):
        service.add_initial_segment(
            camera=item["camera"], boot=item["boot"], seq=item["seq"], offset=item["offset"],
            data=objects[item["file"]], clock=item["clock"], duration=item["duration"],
            at=item.get("at"), file=item.get("file"),
        )
    return service, record, objects


def run_fixture(
    fixture_dir: str | Path,
    state_dir: str | Path | None = None,
    *,
    durable: bool = True,
    script_faults: bool = True,
) -> dict[str, Any]:
    service, record, objects = build_fixture_service(
        fixture_dir, state_dir, durable=durable, script_faults=script_faults
    )
    responses: list[dict[str, Any]] = []
    for event in record.get("events", []):
        data = objects.get(event.get("file", "")) if event.get("file") else None
        response = service.dispatch(event, data)
        responses.append({"at": event.get("at"), "action": event.get("action"),
                          "request": json_safe(event), "response": json_safe(response)})
    report = {
        "fixture": str(Path(fixture_dir)),
        "state_dir": str(service.data_dir) if service.data_dir else None,
        "responses": responses,
        "usage": service.usage(),
        "progress": service.progress(),
        "status": service.status(),
    }
    return json_safe(report)


class RingHTTPHandler(BaseHTTPRequestHandler):
    server_version = "LoopRing/1.0"

    @property
    def service(self) -> LoopRingService:
        return self.server.service  # type: ignore[attr-defined]

    @property
    def fixture_objects(self) -> dict[str, bytes]:
        return getattr(self.server, "fixture_objects", {})  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json_dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(length) if length else b""

    def _json_body(self) -> dict[str, Any]:
        raw = self._body()
        if not raw:
            return {}
        value = json.loads(raw.decode("utf-8"))
        return value if isinstance(value, dict) else {"value": value}

    def do_GET(self) -> None:  # noqa: N802
        url = urlsplit(self.path)
        path = url.path
        query = parse_qs(url.query)
        try:
            if path == "/health":
                self._send_json(200, {"ok": True, "service": "loop-ring"})
            elif path in {"/state", "/status"}:
                self._send_json(200, self.service.status())
            elif path == "/usage":
                self._send_json(200, self.service.usage())
            elif path == "/progress":
                camera = query.get("camera", [None])[0]
                self._send_json(200, self.service.progress(camera))
            elif path in {"/segment", "/segments", "/read_segment"}:
                segment_id = query.get("segment_id", query.get("id", [None]))[0]
                if segment_id is None:
                    segment_id = "/".join([query["camera"][0], query["boot"][0], query["seq"][0]])
                byte_range = None
                if "start" in query and "end" in query:
                    byte_range = [int(query["start"][0]), int(query["end"][0])]
                self._send_json(200, self.service.read_segment(segment_id, byte_range))
            elif path.startswith("/holds/"):
                hold_id = path.split("/")[2]
                self._send_json(200, self.service._hold_status(hold_id))
            elif path.startswith("/reviews/"):
                token = path.split("/")[2]
                self._send_json(200, self.service.read_review(token))
            elif path == "/events":
                self._send_json(200, getattr(self.server, "events", []))
            elif path.startswith("/ring/"):
                offset = int(path.rsplit("/", 1)[1])
                length = int(query.get("length", ["0"])[0])
                data = self.service.device.get(offset, length)
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            elif path.startswith("/reviews/") and path.endswith("/read"):
                token = path.split("/")[2]
                self._send_json(200, self.service.read_review(token))
            else:
                self._send_json(404, {"state": "not_found", "path": path})
        except KeyError as exc:
            self._send_json(404, {"state": "not_found", "error": str(exc)})
        except NotImplementedError as exc:
            self._send_json(501, {"state": "not_implemented", "error": str(exc)})
        except Exception as exc:
            self._send_json(400, {"state": "error", "error": type(exc).__name__, "message": str(exc)})

    def do_PUT(self) -> None:  # noqa: N802
        url = urlsplit(self.path)
        try:
            match = re.fullmatch(r"/ring/(\d+)", url.path)
            if not match:
                self._send_json(404, {"state": "not_found"})
                return
            offset = int(match.group(1))
            data = self._body()
            # Test/debug hook: X-Fault-Written: n forces a partial prefix.
            fault_written = self.headers.get("X-Fault-Written")
            if fault_written is not None and hasattr(self.service.device, "fault"):
                n = int(fault_written)
                original = self.service.device.fault

                def one_shot(off: int, body: bytes) -> int | None:
                    self.service.device.fault = original
                    return n

                self.service.device.fault = one_shot
            result = self.service.device.put(offset, data)
            self._send_json(200, result)
        except DeviceWriteError as exc:
            payload = exc.response or {"state": exc.state or "partial", "written": exc.written}
            # Recorded fixture drop mode closes the transport rather than
            # sending JSON.  X-Fault-Drop can force the same behavior.
            if self.headers.get("X-Fault-Drop") == "1" or getattr(exc, "drop", False):
                self.close_connection = True
                try:
                    self.connection.shutdown(1)
                except OSError:
                    pass
                return
            self._send_json(200, payload)
        except Exception as exc:
            self._send_json(400, {"state": "error", "error": type(exc).__name__, "message": str(exc)})

    def do_POST(self) -> None:  # noqa: N802,C901
        url = urlsplit(self.path)
        path = url.path.rstrip("/") or "/"
        query = parse_qs(url.query)
        try:
            raw = self._body()
            content_type = self.headers.get("Content-Type", "")
            if content_type.startswith("application/octet-stream"):
                params = {k: v[0] for k, v in query.items()}
                result = self.service.submit_capture(data=raw, **params)
                self._send_json(200, result)
                return
            body = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(body, dict):
                raise ValueError("JSON body must be an object")
            if body.get("data_b64") and "data" not in body:
                body["data"] = base64.b64decode(body.pop("data_b64"))

            if path in {"/event", "/events", "/dispatch"}:
                data = body.pop("data", None)
                if data is None and body.get("file") and body["file"] in self.fixture_objects:
                    data = self.fixture_objects[body["file"]]
                result = self.service.dispatch(body, data)
            elif path in {"/capture", "/captures"}:
                result = self.service.submit_capture(**body)
            elif path in {"/retain", "/holds"}:
                result = self.service.create_hold(
                    hold_id=body.get("id") or body.get("hold_id"),
                    camera=body["camera"], span=body["span"], until=body["until"],
                    at=body.get("at"),
                )
            elif re.fullmatch(r"/holds/[^/]+/collect", path):
                hold_id = path.split("/")[2]
                result = self.service.handover(hold_id, at=body.get("at"),
                                               include_data=body.get("include_data", True),
                                               release=bool(body.get("release", False)))
            elif path in {"/collect", "/handover"}:
                result = self.service.handover(body.get("id") or body["hold_id"], at=body.get("at"),
                                               include_data=body.get("include_data", True),
                                               release=bool(body.get("release", False)))
            elif re.fullmatch(r"/holds/[^/]+/release", path):
                result = self.service.release_hold(path.split("/")[2], at=body.get("at"))
            elif path in {"/release"}:
                result = self.service.release_hold(body.get("id") or body["hold_id"], at=body.get("at"))
            elif path in {"/open", "/reviews"}:
                result = self.service.open(
                    token=body.get("token") or body.get("id"), camera=body["camera"],
                    span=body["span"], until=body["until"], at=body.get("at"),
                )
            elif path in {"/close"}:
                result = self.service.close(body["token"], at=body.get("at"))
            elif re.fullmatch(r"/reviews/[^/]+/close", path):
                result = self.service.close(path.split("/")[2], at=body.get("at"))
            elif path in {"/query", "/timeline"}:
                result = self.service.query(body["camera"], body["span"], at=body.get("at"),
                                            include_data=bool(body.get("include_data", False)))
            elif path in {"/read_segment", "/segment"}:
                result = self.service.read_segment(body.get("segment_id") or body.get("id"),
                                                   body.get("byte_range"))
            elif path in {"/power_loss", "/power-loss"}:
                result = self.service.power_loss(at=body.get("at"))
            elif path in {"/restart", "/recover"}:
                result = self.service.restart(at=body.get("at"))
            elif path == "/tick":
                result = self.service.tick(float(body["at"]))
            else:
                self._send_json(404, {"state": "not_found", "path": path})
                return
            self._send_json(200, result)
        except KeyError as exc:
            self._send_json(404, {"state": "not_found", "error": str(exc)})
        except (RingError, ValueError, TypeError) as exc:
            self._send_json(409, {"state": "rejected", "error": type(exc).__name__, "message": str(exc)})
        except Exception as exc:
            self._send_json(400, {"state": "error", "error": type(exc).__name__, "message": str(exc)})


def create_server(
    service: LoopRingService,
    host: str = "127.0.0.1",
    port: int = 8091,
    *,
    fixture_objects: dict[str, bytes] | None = None,
    events: list[dict[str, Any]] | None = None,
) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), RingHTTPHandler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.fixture_objects = fixture_objects or {}  # type: ignore[attr-defined]
    httpd.events = events or []  # type: ignore[attr-defined]
    httpd.verbose = False  # type: ignore[attr-defined]
    return httpd


def replay_packets(fixture_dir: str | Path, base_url: str) -> list[dict[str, Any]]:
    """Send the exact recorded PUT packets to a device fixture/base URL."""
    record, objects = load_fixture(fixture_dir)
    device = HttpRingDevice(base_url)
    journal: list[dict[str, Any]] = []
    for packet in record.get("packets", []):
        if packet.get("method") != "PUT":
            continue
        offset = int(re.fullmatch(r"/ring/(\d+)", packet["path"]).group(1))  # type: ignore[union-attr]
        data = objects[packet["body"]] if packet.get("body") else b""
        try:
            result = device.put(offset, data)
            journal.append({"path": packet["path"], "result": result})
        except DeviceWriteError as exc:
            journal.append({"path": packet["path"], "error": str(exc), "written": exc.written,
                            "response": exc.response})
    return journal


def main(argv: list[str] | None = None) -> int:
    import os
    import sys

    raw = list(sys.argv[1:] if argv is None else argv)
    # Convenience: `python solution.py fixtures/case` replays that fixture.
    if raw and not raw[0].startswith("-") and Path(raw[0], "events.json").exists():
        raw = ["simulate", *raw]
    elif not raw or raw[0].startswith("-"):
        raw = ["serve", *raw]

    parser = argparse.ArgumentParser(description="Cyclic retention ring service")
    sub = parser.add_subparsers(dest="command")

    sim = sub.add_parser("simulate", help="replay a fixture events.json")
    sim.add_argument("fixture")
    sim.add_argument("--state-dir")
    sim.add_argument("--memory", action="store_true")
    sim.add_argument("--no-faults", action="store_true")
    sim.add_argument("--report")

    serve = sub.add_parser("serve", help="run HTTP JSON/raw service")
    serve.add_argument("--fixture")
    serve.add_argument("--state-dir")
    serve.add_argument("--host", default=os.environ.get("RING_HOST", "127.0.0.1"))
    serve.add_argument("--port", type=int,
                       default=int(os.environ.get("RING_PORT", os.environ.get("PORT", "8080"))))

    replay = sub.add_parser("replay-packets", help="send fixture PUT packets to base URL")
    replay.add_argument("fixture")
    replay.add_argument("base_url")

    args = parser.parse_args(raw)
    command = args.command or "serve"
    if command == "simulate":
        report = run_fixture(args.fixture, args.state_dir, durable=not args.memory,
                             script_faults=not args.no_faults)
        text = json.dumps(report, ensure_ascii=False, indent=2)
        if args.report:
            Path(args.report).write_text(text, "utf-8")
        print(text)
        return 0
    if command == "replay-packets":
        print(json.dumps(json_safe(replay_packets(args.fixture, args.base_url)), indent=2))
        return 0
    if command == "serve":
        if args.fixture:
            service, record, objects = build_fixture_service(args.fixture, args.state_dir)
            httpd = create_server(service, args.host, args.port,
                                  fixture_objects=objects, events=record.get("events", []))
        else:
            capacity = int(os.environ.get("RING_CAPACITY", str(64 * 1024 * 1024)))
            service = LoopRingService(capacity=capacity, data_dir=args.state_dir)
            httpd = create_server(service, args.host, args.port)
        print(f"http://{args.host}:{args.port}", flush=True)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()
        return 0
    parser.error(f"unknown command: {command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
