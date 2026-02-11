from __future__ import annotations

from collections import deque

from pydantic import BaseModel


class _PendingLsn(BaseModel):
    lsn: int
    published: bool = False


class AckTracker:
    """Tracks highest contiguous LSN confirmed published to Kinesis."""

    def __init__(self, *, initial_lsn: int = 0) -> None:
        self._frontier = initial_lsn
        self._last_registered = initial_lsn
        self._pending: deque[_PendingLsn] = deque()

    @property
    def frontier_lsn(self) -> int:
        return self._frontier

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def register(self, lsn: int) -> None:
        if lsn < self._last_registered:
            raise ValueError(
                f"LSN must be non-decreasing: got {lsn}, last registered {self._last_registered}"
            )

        self._last_registered = lsn
        self._pending.append(_PendingLsn(lsn=lsn))

    def mark_published(self, lsn: int) -> int | None:
        for pending in self._pending:
            if pending.lsn == lsn and not pending.published:
                pending.published = True
                break
        else:
            raise KeyError(f"Unknown or already-published LSN: {lsn}")

        advanced_to: int | None = None
        while self._pending and self._pending[0].published:
            published_lsn = self._pending.popleft().lsn
            if published_lsn > self._frontier:
                self._frontier = published_lsn
                advanced_to = self._frontier

        return advanced_to
