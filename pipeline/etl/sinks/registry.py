"""Small registry for destination adapters used by a pipeline run."""

from __future__ import annotations

from typing import Iterable

from pipeline.core.contracts import DataSink


class SinkRegistry:
    def __init__(self, sinks: Iterable[DataSink] = ()) -> None:
        self._sinks: dict[str, DataSink] = {}
        for sink in sinks:
            self.register(sink)

    def register(self, sink: DataSink) -> None:
        if sink.name in self._sinks:
            raise ValueError(f"A sink named {sink.name!r} is already registered")
        self._sinks[sink.name] = sink

    def __iter__(self):
        return iter(self._sinks.values())
