from __future__ import annotations

import pytest
from cdc_logical_replication.ack import AckTracker


def test_frontier_advances_only_for_contiguous_successes() -> None:
    tracker = AckTracker(initial_lsn=0)
    tracker.register(100)
    tracker.register(200)
    tracker.register(300)

    assert tracker.mark_published(200) is None
    assert tracker.frontier_lsn == 0

    assert tracker.mark_published(100) == 200
    assert tracker.frontier_lsn == 200

    assert tracker.mark_published(300) == 300
    assert tracker.frontier_lsn == 300


def test_duplicate_lsn_entries_are_handled_in_order() -> None:
    tracker = AckTracker(initial_lsn=0)
    tracker.register(100)
    tracker.register(100)
    tracker.register(200)

    assert tracker.mark_published(100) == 100
    assert tracker.frontier_lsn == 100

    assert tracker.mark_published(200) is None
    assert tracker.frontier_lsn == 100

    assert tracker.mark_published(100) == 200
    assert tracker.frontier_lsn == 200


def test_unknown_or_duplicate_publish_raises() -> None:
    tracker = AckTracker(initial_lsn=0)
    tracker.register(100)

    with pytest.raises(KeyError):
        tracker.mark_published(200)

    tracker.mark_published(100)
    with pytest.raises(KeyError):
        tracker.mark_published(100)


def test_register_requires_non_decreasing_lsn() -> None:
    tracker = AckTracker(initial_lsn=0)
    tracker.register(200)

    with pytest.raises(ValueError):
        tracker.register(100)
