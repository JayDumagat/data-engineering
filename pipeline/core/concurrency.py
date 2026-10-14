"""Bounded ordered work submission for network and transformation stages."""

from __future__ import annotations

from collections import deque
from concurrent.futures import Executor
from typing import Callable, Iterable, Iterator, TypeVar


InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


def bounded_map(
    executor: Executor,
    function: Callable[[InputT], OutputT],
    items: Iterable[InputT],
    *,
    max_pending: int,
) -> Iterator[OutputT]:
    """Map without queuing the entire input and yield results in input order."""
    source = iter(items)
    pending = deque()
    for _ in range(max(1, max_pending)):
        try:
            pending.append(executor.submit(function, next(source)))
        except StopIteration:
            break

    while pending:
        future = pending.popleft()
        yield future.result()
        try:
            pending.append(executor.submit(function, next(source)))
        except StopIteration:
            continue
