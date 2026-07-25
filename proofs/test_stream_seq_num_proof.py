import asyncio
import time
from collections.abc import AsyncIterator

import pytest
from bluesky import RunEngine
from bluesky.plans import count
from bluesky.protocols import Reading
from event_model import ComposeStreamResource, DataKey
from ophyd_async.core import (
    DetectorAcquireLogic,
    DetectorDataLogic,
    SignalR,
    StandardDetector,
    StreamResourceDataProvider,
    StreamResourceInfo,
    StreamableDataProvider,
    TriggerInfo,
)
from ophyd_async.core._signal_backend import SignalBackend
from ophyd_async.core._utils import Callback


class CompletedStatus:
    @property
    def done(self):
        return True

    @property
    def success(self):
        return True

    def add_callback(self, cb):
        cb(self)

    def exception(self, timeout=0.0):
        return None


class LateStreamDevice:
    """A minimal Bluesky device that emits the first stream datum one event late."""

    parent = None

    def __init__(self):
        self.name = "det"
        self.read_calls = 0
        self.last_emitted = 0
        bundle = ComposeStreamResource()(
            mimetype="application/x-hdf5",
            uri="file://localhost/tmp/f.h5",
            data_key="det",
            parameters={"dataset": "/entry/data/data", "chunk_shape": (1,)},
        )
        self.stream_resource = bundle.stream_resource_doc
        self.compose_stream_datum = bundle.compose_stream_datum

    def stage(self):
        return [self]

    def unstage(self):
        return [self]

    def trigger(self):
        return CompletedStatus()

    def describe(self):
        return {
            "det": {
                "source": self.stream_resource["uri"],
                "shape": [1],
                "dtype": "array",
                "external": "STREAM:",
            }
        }

    def read(self):
        self.read_calls += 1
        return {}

    def describe_configuration(self):
        return {}

    def read_configuration(self):
        return {}

    async def collect_asset_docs(self, index=None):
        # This is the exact late-doc condition inferred from the real document stream:
        # event 1 sees no assets, event 2 sees file index 0:1.
        indices_written = max(0, self.read_calls - 1)
        if indices_written and not self.last_emitted:
            yield "stream_resource", self.stream_resource
        if indices_written > self.last_emitted:
            indices = {"start": self.last_emitted, "stop": indices_written}
            self.last_emitted = indices_written
            yield "stream_datum", self.compose_stream_datum(indices)


def test_bluesky_assigns_wrong_seq_nums_when_stream_datum_arrives_late():
    docs = []
    RunEngine({})(count([LateStreamDevice()], num=3), lambda name, doc: docs.append((name, doc)))

    stream_datums = [doc for name, doc in docs if name == "stream_datum"]
    events = [doc for name, doc in docs if name == "event"]

    assert [event["seq_num"] for event in events] == [1, 2, 3]
    assert stream_datums[0]["indices"] == {"start": 0, "stop": 1}

    # This assertion documents the bug: file index 0:1 is the first event's data,
    # but Bluesky assigns the current sequence counter, which is already 2.
    assert stream_datums[0]["seq_nums"] == {"start": 2, "stop": 3}


class StaleExplicitReadBackend(SignalBackend[int]):
    """A counter whose monitor value can be newer than explicit get_value()."""

    def __init__(self):
        super().__init__(int)
        self.explicit_value = 0
        self.monitor_value = 0
        self.callback: Callback[Reading[int]] | None = None

    def source(self, name: str, read: bool) -> str:
        return f"proof://{name}"

    async def connect(self, timeout: float):
        return None

    async def put(self, value: int | None):
        if value is not None:
            self.explicit_value = value
            self.monitor_value = value
            self._notify(value)

    async def get_datakey(self, source: str) -> DataKey:
        return {"source": source, "shape": [], "dtype": "integer"}

    async def get_reading(self) -> Reading[int]:
        return {"value": self.explicit_value, "timestamp": time.time(), "alarm_severity": 0}

    async def get_value(self) -> int:
        return self.explicit_value

    async def get_setpoint(self) -> int:
        return self.explicit_value

    def set_callback(self, callback: Callback[Reading[int]] | None) -> None:
        self.callback = callback
        if callback is not None:
            self._notify(self.monitor_value)

    def advance_monitor_only(self, value: int) -> None:
        self.monitor_value = value
        self._notify(value)

    def catch_up_explicit_read(self) -> None:
        self.explicit_value = self.monitor_value

    def _notify(self, value: int) -> None:
        if self.callback:
            self.callback({"value": value, "timestamp": time.time(), "alarm_severity": 0})


class NoopAcquireLogic(DetectorAcquireLogic):
    async def start_acquiring(self):
        return None

    async def wait_for_idle(self):
        return None

    async def ensure_stopped(self):
        return None


class HdfLikeDataLogic(DetectorDataLogic):
    def __init__(self, counter: SignalR[int]):
        self.counter = counter

    async def prepare_unbounded(self, datakey_name: str) -> StreamableDataProvider:
        resource = StreamResourceInfo(
            data_key=datakey_name,
            shape=(512, 512),
            chunk_shape=(1, 512, 512),
            dtype_numpy="<u2",
            parameters={"dataset": "/entry/data/data"},
        )
        return StreamResourceDataProvider(
            uri="file://localhost/tmp/proof.h5",
            resources=[resource],
            mimetype="application/x-hdf5",
            collections_written_signal=self.counter,
        )


class ProofDetector(StandardDetector):
    def __init__(self, counter: SignalR[int], name="det"):
        super().__init__(name=name)
        self.add_detector_logics(NoopAcquireLogic(), HdfLikeDataLogic(counter))


async def collect_docs(detector: StandardDetector) -> list:
    return [doc async for doc in detector.collect_asset_docs()]


@pytest.mark.asyncio
async def test_ophyd_async_trigger_can_complete_before_collect_asset_docs_sees_counter():
    backend = StaleExplicitReadBackend()
    counter = SignalR(backend, name="num_captured")
    det = ProofDetector(counter)

    await det.stage()
    await det.prepare(TriggerInfo(number_of_events=1, collections_per_event=1))

    status = det.trigger()
    await asyncio.sleep(0)

    # trigger() is waiting on a subscription. Give that subscription the target
    # value, but leave explicit get_value() stale at 0.
    backend.advance_monitor_only(1)
    await status

    # This proves the ophyd-async gap: trigger() has completed, but
    # collect_asset_docs() recomputes the index using an explicit get_value(),
    # so it emits no StreamDatum for the point that just completed.
    assert await collect_docs(det) == []

    backend.catch_up_explicit_read()
    docs = await collect_docs(det)

    assert [name for name, _ in docs] == ["stream_resource", "stream_datum"]
    assert docs[1][1]["indices"] == {"start": 0, "stop": 1}
