import base64
import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from ring_app import create_server, run_fixture
from ring_service import LoopRingService, MemoryBlockDevice


ROOT = Path(__file__).parent
FIX1 = ROOT / "fixtures" / "wrapped-write-while-two-holds-overlap"
FIX2 = ROOT / "fixtures" / "no-unused-space-until-release"


def object_bytes(folder: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in (folder / "objects").iterdir()}


class RingSemanticsTest(unittest.TestCase):
    def test_clock_bounds_and_boot_identity(self):
        svc = LoopRingService(
            capacity=16,
            clock_bounds={"p/2": [4, 6]},
            rate_bound={"p": 2},
        )
        mapping = svc.time_mapping("p", 2, 17, 3)
        self.assertEqual(mapping["camera_span"], [17.0, 20.0])
        self.assertEqual(mapping["local_possible"], [21.0, 26.0])
        self.assertEqual(mapping["local_confirmed"], [23.0, 24.0])
        self.assertTrue(mapping["uncertain"])
        self.assertEqual(LoopRingService.segment_uid("p", 1, 7), "p/1/7")
        self.assertEqual(LoopRingService.segment_uid("p", 2, 7), "p/2/7")

    def test_wrapped_torn_write_is_not_readable_and_review_is_stable(self):
        data = object_bytes(FIX1)
        device = MemoryBlockDevice(24)

        def fault(offset, body):
            if offset == 0 and len(body) == 2:
                return 1
            return None

        device.fault = fault
        svc = LoopRingService(
            capacity=24,
            block_device=device,
            next_offset=20,
            clock_bounds={"p/1": [0, 0]},
            rate_bound={"p": 2},
        )
        svc.add_initial_segment(camera="p", boot=1, seq=7, offset=0,
                                data=data["old.bin"], clock=8, duration=3)
        svc.add_initial_segment(camera="p", boot=1, seq=8, offset=8,
                                data=data["mid.bin"], clock=11, duration=3)

        opened = svc.open(token="v1", camera="p", span=[8, 11], until=23, at=14)
        self.assertEqual(opened["status"], "open")
        h1 = svc.create_hold("h1", "p", [11, 18], 31, at=15)
        h2 = svc.create_hold("h2", "p", [13, 19], 35, at=16)
        self.assertEqual(h1["state"], "accepted")
        self.assertEqual(h2["state"], "accepted")
        self.assertEqual(svc.usage()["protected_hold_bytes"], 8)

        result = svc.submit_capture(camera="p", boot=1, seq=9, at=17, clock=14,
                                   duration=3, data=data["on-edge.bin"])
        self.assertEqual(result["state"], "torn")
        self.assertEqual(result["extents"], [[20, 4], [0, 2]])
        self.assertEqual(result["confirmed_prefix_bytes"], 1)
        seg = svc.state["segments"]["p/1/9"]
        self.assertEqual(seg["status"], "torn")
        self.assertEqual(svc.progress("p")["failed_sequences"], [9])
        self.assertEqual(svc.progress("p")["continuous_seq_watermark"], 8)

        # The open review was snapshotted before the wrapped overwrite.
        review = svc.read_review("v1")
        reviewed = base64.b64decode(review["ranges"][0]["data_b64"])
        self.assertEqual(reviewed, data["old.bin"])
        self.assertEqual(review["ranges"][0]["source"], "snapshot")

        # Duplicate identity is idempotent and does not write again.
        calls_before = len(device.calls)
        duplicate = svc.submit_capture(camera="p", boot=1, seq=9, at=19, clock=14,
                                       duration=3, data=data["on-edge.bin"])
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["state"], "torn")
        self.assertEqual(len(device.calls), calls_before)

        # Old identity cannot be read through a new occupant/address.
        with self.assertRaises(Exception):
            svc.read_segment("p/1/9")

    def test_second_fixture_blocks_until_hold_expiry_then_reclaims_base(self):
        data = object_bytes(FIX2)
        svc = LoopRingService(capacity=12, next_offset=8,
                              clock_bounds={"q/5": [0, 0]}, rate_bound={"q": 2})
        svc.add_initial_segment(camera="q", boot=5, seq=2, offset=0,
                               data=data["base.bin"], clock=30, duration=4)
        hold = svc.create_hold("kept", "q", [30, 34], 40, at=34)
        self.assertEqual(hold["status"], "active")
        self.assertEqual(hold["state"], "accepted")
        self.assertEqual(hold["safe_run_bytes"], 4)

        rejected = svc.submit_capture(camera="q", boot=5, seq=3, at=35, clock=34,
                                      duration=3, data=data["more.bin"])
        self.assertEqual(rejected["state"], "rejected")
        self.assertEqual(rejected["reason"], "no_safe_space")
        self.assertEqual(rejected["space"]["candidate"], [[8, 4], [0, 2]])

        stored = svc.submit_capture(camera="q", boot=5, seq=4, at=41, clock=37,
                                    duration=3, data=data["more.bin"])
        self.assertEqual(stored["state"], "stored")
        self.assertEqual(stored["extents"], [[8, 4], [0, 2]])

        result = svc.query("q", [30, 40], at=42, include_data=True)
        kinds = [(item["kind"], item.get("span")) for item in result["items"]]
        self.assertIn(("gap", [30.0, 34.0]), kinds)
        self.assertIn(("gap", [34.0, 37.0]), kinds)
        content = [x for x in result["items"] if x["kind"] == "content"]
        self.assertEqual(len(content), 1)
        self.assertEqual(content[0]["segment_id"], "q/5/4")
        self.assertEqual(base64.b64decode(content[0]["data_b64"]), data["more.bin"])
        progress = svc.progress("q")
        self.assertEqual(progress["continuous_seq_watermark"], 2)
        self.assertTrue(progress["has_real_gap"])

    def test_restart_discovers_suspect_positions_and_preserves_hold_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            svc = LoopRingService(capacity=12, data_dir=td,
                                  next_offset=8, clock_bounds={"q/5": [0, 0]})

            def fault(offset, body):
                if offset == 8 and len(body) == 4:
                    return 1
                return None

            svc.device.fault = fault
            svc.add_initial_segment(camera="q", boot=5, seq=2, offset=0,
                                    data=b"01234567", clock=30, duration=4)
            svc.create_hold("kept", "q", [30, 34], 90, at=34)
            torn = svc.submit_capture(camera="q", boot=5, seq=3, at=35, clock=34,
                                      duration=2, data=b"wxyz")
            self.assertEqual(torn["state"], "torn")
            svc.power_loss()

            restored = LoopRingService(capacity=12, data_dir=td,
                                       clock_bounds={"q/5": [0, 0]})
            report = restored.restart()
            self.assertIn("q/5/3", report["torn"])
            self.assertEqual(report["protected_bytes"], 8)
            self.assertIn([8, 12], report["suspect_positions"])
            self.assertEqual(restored.usage()["protected_hold_bytes"], 8)


class FixtureRunnerTest(unittest.TestCase):
    def test_both_checked_in_scenarios(self):
        with tempfile.TemporaryDirectory() as td1, tempfile.TemporaryDirectory() as td2:
            report1 = run_fixture(FIX1, td1)
            actions = {(r["at"], r["action"]): r["response"] for r in report1["responses"]}
            self.assertEqual(actions[(17, "capture")]["state"], "torn")
            self.assertEqual(actions[(22, "capture")]["state"], "rejected")
            self.assertEqual(actions[(37, "capture")]["state"], "stored")
            self.assertEqual(actions[(21, "restart")]["protected_bytes"], 8)

            report2 = run_fixture(FIX2, td2)
            states = [r["response"].get("state") for r in report2["responses"] if r["action"] == "capture"]
            self.assertEqual(states, ["rejected", "stored"])


class HTTPInterfaceTest(unittest.TestCase):
    def test_health_raw_ring_and_json_query(self):
        svc = LoopRingService(capacity=8, next_offset=0)
        httpd = create_server(svc, "127.0.0.1", 0)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{httpd.server_port}"
            with urllib.request.urlopen(base + "/health", timeout=3) as response:
                self.assertEqual(json.loads(response.read())["ok"], True)

            request = urllib.request.Request(base + "/ring/0", data=b"abcdefgh", method="PUT")
            with urllib.request.urlopen(request, timeout=3) as response:
                self.assertEqual(json.loads(response.read())["state"], "stored")

            with urllib.request.urlopen(base + "/ring/2?length=4", timeout=3) as response:
                self.assertEqual(response.read(), b"cdef")

            request = urllib.request.Request(
                base + "/tick",
                data=json.dumps({"at": 1}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=3) as response:
                self.assertEqual(response.status, 200)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
