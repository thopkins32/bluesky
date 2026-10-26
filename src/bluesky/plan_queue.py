"""An editable sequence of plans, executed one at a time by a RunEngine.

A :class:`PlanQueue` holds *plan factories* (a plan function plus the arguments
to call it with), not plan generators.  Each item is run with its own
``RE(plan)`` call, so between items the RunEngine is idle, an item that fails
or is interrupted leaves the rest of the queue intact, and the remaining items
can be inspected and edited at any time the queue is not actively running.
"""

from __future__ import annotations

import reprlib
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Any

from bluesky.run_engine import RunEngine, RunEngineResult
from bluesky.simulators import check_limits, summarize_plan
from bluesky.utils import Msg, RunEngineInterrupted

PAUSE_MSG = """
The RunEngine is paused on this queue item:

    {item}

These are your options for continuing:

queue.resume()    Resume the item from its last checkpoint, then continue with the queue.
queue.skip()      Stop the item (clean up, exit_status='success'), then continue with the next one.
queue.hold()      Abort the item (clean up, exit_status='abort'), put it back at the front of the
                  queue and release the RunEngine for other work.

RE.resume(), RE.stop(), RE.abort() and RE.halt() also work. The queue is then held with the
remaining items intact, and queue.resume() continues with the next one.
"""

FAILED_MSG = """
Queue item {item} failed and was put back at the front of the queue ({n} items pending).
queue.resume() runs it again; queue.skip() drops it and continues with the next one.
"""

INTERRUPTED_MSG = """
Queue item {item} was interrupted (exit_status={status!r}) and put back at the front of the
queue ({n} items pending). queue.resume() runs it again; queue.skip() drops it and continues.
"""

OUTSIDE_MSG = """
Queue item {item} finished outside the queue (exit_status={status!r}). The queue is held with
{n} items pending; queue.resume() continues with the next one.
"""

_HISTORY_ROWS_IN_REPR = 5


def _describe(obj: Any) -> str:
    """Short, human-oriented rendering of a plan argument: device names, not device reprs."""
    name = getattr(obj, "name", None)
    if isinstance(name, str) and not isinstance(obj, str):
        return name
    if isinstance(obj, (list, tuple)):
        return "[" + ", ".join(_describe(x) for x in obj) + "]"
    return reprlib.repr(obj)


def _plan_name(plan: Callable[..., Any]) -> str:
    name = getattr(plan, "__name__", None)
    if name is None:  # e.g. functools.partial
        name = getattr(getattr(plan, "func", None), "__name__", None)
    return name or repr(plan)


@dataclass(eq=False)
class QueueItem:
    """One entry of a :class:`PlanQueue`: a plan function and the arguments to call it with.

    Calling the item builds a fresh plan, so it can be run, simulated, or retried
    any number of times.  ``args`` and ``kwargs`` are read when the item runs, so
    editing them while the item is pending takes effect.

    Attributes
    ----------
    plan : callable
        A plan function (anything that returns an iterable of ``Msg`` when called).
    args, kwargs :
        Arguments passed to ``plan`` when the item runs.
    label : str, optional
        Free text shown in the queue listing and recorded as ``queue_item_label``
        metadata on every run the item produces.
    uid : str
        Unique id, recorded as ``queue_item_uid`` metadata on every run the item produces.
    """

    plan: Callable[..., Iterable[Msg]]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    label: str | None = None
    uid: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __call__(self) -> Iterable[Msg]:
        return self.plan(*self.args, **self.kwargs)

    @property
    def description(self) -> str:
        """The plan call this item makes, e.g. ``count([det], num=2)``."""
        args = [_describe(a) for a in self.args]
        args += [f"{k}={_describe(v)}" for k, v in self.kwargs.items()]
        return f"{_plan_name(self.plan)}({', '.join(args)})"

    def __str__(self) -> str:
        return f"{self.label}: {self.description}" if self.label else self.description

    def __repr__(self) -> str:
        return f"<QueueItem {self}>"


@dataclass(frozen=True)
class HistoryEntry:
    """Record of one finished queue item.

    Attributes
    ----------
    item : QueueItem
    description : str
        ``str(item)`` as it was when the item ran.  The item's arguments may be
        edited afterwards; this does not change.
    result : RunEngineResult
        Outcome of the RunEngine call.  ``result.exception`` holds the exception
        the item raised, if any.
    start_time, stop_time : float
        ``time.time()`` when the item started and when it finished.
    """

    item: QueueItem
    description: str
    result: RunEngineResult
    start_time: float
    stop_time: float


class PlanQueue:
    """An editable, ordered list of plans run one at a time by a RunEngine.

    Items are added as a plan function plus arguments and run in order by
    :meth:`run`, each with its own ``RE(plan)`` call, on the calling thread.
    Ctrl+C pauses the running item exactly as it does for a bare ``RE(plan)``;
    the queue then offers :meth:`resume`, :meth:`skip` and :meth:`hold`.  An
    item that raises is put back at the front of the queue and the queue holds,
    so nothing after it is lost and nothing after it runs until you decide.

    Parameters
    ----------
    RE : RunEngine

    Attributes
    ----------
    RE : RunEngine
    history : list of HistoryEntry
        Every item that has finished, in order, with its :class:`RunEngineResult`.

    Notes
    -----
    Arguments are captured when an item is added; plan bodies run later.  A step
    that depends on what an earlier step measured must read that value inside
    its plan, not take it as an argument.

    The queue wraps ``RE.state_hook`` to notice an item being finished directly
    through ``RE.resume()``, ``RE.stop()``, ``RE.abort()`` or ``RE.halt()``.
    Replacing ``RE.state_hook`` after creating the queue disables that.
    """

    def __init__(self, RE: RunEngine):
        self.RE = RE
        self.history: list[HistoryEntry] = []
        self._pending: list[QueueItem] = []
        self._current: QueueItem | None = None
        self._current_start = 0.0
        self._state = "idle"
        self._install_state_hook()

    # ------------------------------------------------------------------ inspection

    @property
    def state(self) -> str:
        """``'idle'``, ``'running'``, ``'paused'`` (an item is paused in the RunEngine) or
        ``'held'`` (stopped with items pending; :meth:`resume` continues)."""
        return self._state

    @property
    def current(self) -> QueueItem | None:
        """The item that is running or paused, if any."""
        return self._current

    @property
    def pending(self) -> tuple[QueueItem, ...]:
        return tuple(self._pending)

    def __len__(self) -> int:
        return len(self._pending)

    def __iter__(self) -> Iterator[QueueItem]:
        return iter(self.pending)

    def __getitem__(self, pos: int) -> QueueItem:
        return self._pending[pos]

    def __repr__(self) -> str:
        lines = [f"<PlanQueue state={self._state!r} pending={len(self._pending)} history={len(self.history)}>"]
        if self.history:
            skipped = len(self.history) - _HISTORY_ROWS_IN_REPR
            lines.append(f"  history: ... {skipped} earlier" if skipped > 0 else "  history:")
            for entry in self.history[-_HISTORY_ROWS_IN_REPR:]:
                runs = len(entry.result.run_start_uids)
                lines.append(f"    {entry.result.exit_status:<8} {entry.description}  ({runs} runs)")
        if self._current is not None:
            lines.append(f"  {self._state}:")
            lines.append(f"    {self._current}")
        if self._pending:
            lines.append("  pending:")
            lines.extend(f"    {pos:>3}  {item}" for pos, item in enumerate(self._pending))
            if self._state == "held" and self.history and self.history[-1].item is self._pending[0]:
                lines.append("  (item 0 was put back after it failed or was interrupted; resume() retries it)")
        return "\n".join(lines)

    # ------------------------------------------------------------------ editing

    def add(self, plan: Callable[..., Iterable[Msg]], /, *args: Any, **kwargs: Any) -> QueueItem:
        """Append ``plan(*args, **kwargs)`` to the queue and return the new item.

        All arguments go to the plan.  Set a label on the returned item if you
        want one: ``queue.add(count, [det]).label = "dark"``.
        """
        return self.insert(len(self._pending), plan, *args, **kwargs)

    def insert(self, pos: int, plan: Callable[..., Iterable[Msg]], /, *args: Any, **kwargs: Any) -> QueueItem:
        """Insert ``plan(*args, **kwargs)`` before pending position ``pos`` and return the new item."""
        item = QueueItem(plan, args, kwargs)
        self._pending.insert(pos, item)
        return item

    def remove(self, item: QueueItem | int) -> QueueItem:
        """Remove and return a pending item, given the item or its position."""
        return self._pending.pop(self._position(item))

    def move(self, item: QueueItem | int, pos: int) -> None:
        """Move a pending item so that it sits at position ``pos``."""
        moved = self._pending.pop(self._position(item))
        self._pending.insert(pos, moved)

    def clear(self) -> list[QueueItem]:
        """Remove and return all pending items."""
        cleared, self._pending = self._pending, []
        return cleared

    def _position(self, item: QueueItem | int) -> int:
        return self._pending.index(item) if isinstance(item, QueueItem) else item

    def check(self) -> None:
        """Print a summary of every pending item and check motor limits, without running anything.

        Each item's plan is built and iterated twice (for
        :func:`~bluesky.simulators.summarize_plan` and
        :func:`~bluesky.simulators.check_limits`).  Nothing is sent to hardware, but
        the plans' own Python runs with ``None`` as the response to every message,
        so an item whose plan raises shows up here, with its traceback.
        """
        for pos, item in enumerate(self._pending):
            print(f"{pos}: {item}")
            summarize_plan(item())
            check_limits(item())

    # ------------------------------------------------------------------ running

    def run(self, limit: int | None = None) -> None:
        """Run pending items in order, blocking until the queue is empty or holds.

        Parameters
        ----------
        limit : int, optional
            Finish at most this many items, then hold.  ``queue.run(1)`` runs
            just the next item.

        Raises
        ------
        RunEngineInterrupted
            The running item was paused (Ctrl+C or a ``pause`` message).  Use
            :meth:`resume`, :meth:`skip` or :meth:`hold`.
        Exception
            The running item failed.  It is back at the front of the queue and
            the queue is held; the item's exception propagates.
        """
        if self._state == "paused":
            raise RuntimeError("An item is paused. Use resume(), skip() or hold().")
        if self._state == "running":
            raise RuntimeError("The queue is already running.")
        self._run_loop(limit)

    def resume(self, limit: int | None = None) -> None:
        """Continue the queue, first resuming the paused item if there is one.

        ``limit`` is as for :meth:`run`; ``queue.resume(1)`` finishes the paused
        item and then holds.
        """
        if self._state == "running":
            raise RuntimeError("The queue is already running.")
        self._sync()
        self._run_loop(limit, first=self.RE.resume if self._state == "paused" else None)

    def skip(self, limit: int | None = None) -> None:
        """Drop the paused item (``RE.stop()``) or the item at the front, then continue.

        ``limit`` is as for :meth:`run`.
        """
        if self._state == "running":
            raise RuntimeError("The queue is already running.")
        self._sync()
        if self._state == "paused":
            self._run_loop(limit, first=self.RE.stop)
            return
        if not self._pending:
            raise RuntimeError("Nothing to skip.")
        self._pending.pop(0)
        self._run_loop(limit)

    def hold(self) -> None:
        """Abort the paused item, put it back at the front of the queue and release the RunEngine.

        Use this to do other work with the RunEngine before continuing with
        :meth:`resume`, which will run the item again from the start.
        """
        self._sync()
        if self._state != "paused":
            raise RuntimeError("No item is paused.")
        item = self._current
        assert item is not None
        self._state = "running"
        self._drive(partial(self.RE.abort, "held by the plan queue"))
        self._pending.insert(0, item)
        self._state = "held"

    def _run_loop(self, limit: int | None, first: Callable[[], Any] | None = None) -> None:
        self._state = "running"
        finished = 0
        try:
            if first is not None:
                self._drive(first)
                finished += 1
            while self._state == "running" and self._pending and (limit is None or finished < limit):
                if self.RE.state != "idle":
                    raise RuntimeError(f"The RunEngine is in a {self.RE.state} state; cannot start the next item.")
                item = self._pending.pop(0)
                self._current, self._current_start = item, time.time()
                try:
                    plan = item()
                except Exception as exc:
                    self._put_back(item, RunEngineResult((), self.RE.NO_PLAN_RETURN, "fail", False, "", exc))
                    print(FAILED_MSG.format(item=item, n=len(self._pending)))
                    raise
                self._drive(partial(self.RE, plan, **self._metadata(item)))
                finished += 1
        finally:
            if self._current is not None and self.RE.state == "idle":
                # Interrupted (e.g. KeyboardInterrupt) after taking an item but before the
                # RunEngine took over; put it back rather than lose it.
                self._pending.insert(0, self._current)
                self._current = None
            if self._state == "running":
                self._state = "held" if self._pending else "idle"

    def _drive(self, call: Callable[[], Any]) -> None:
        """Make one blocking RunEngine call on behalf of the current item and record its outcome."""
        item = self._current
        assert item is not None
        try:
            ret = call()
        except RunEngineInterrupted:
            if self.RE.state == "paused":
                self._state = "paused"
                raise RunEngineInterrupted(PAUSE_MSG.format(item=item)) from None
            # Interrupted without pausing, e.g. a pause requested with no checkpoint to
            # rewind to aborts the plan instead.  The user wanted the prompt: hold.
            result = self._result()
            self._put_back(item, result)
            print(INTERRUPTED_MSG.format(item=item, status=result.exit_status, n=len(self._pending)))
        except Exception as exc:
            self._put_back(item, replace(self._result(), exception=exc))
            print(FAILED_MSG.format(item=item, n=len(self._pending)))
            raise
        else:
            self._record(item, ret if self.RE.call_returns_result else self._result())

    def _put_back(self, item: QueueItem, result: RunEngineResult) -> None:
        self._record(item, result)
        self._pending.insert(0, item)
        self._state = "held"

    def _record(self, item: QueueItem, result: RunEngineResult) -> None:
        self.history.append(HistoryEntry(item, str(item), result, self._current_start, time.time()))
        self._current = None

    def _result(self) -> RunEngineResult:
        # Without call_returns_result the RunEngine hands back only uids; build the full
        # result from the state of the call that just finished.
        return self.RE._create_result(self.RE.NO_PLAN_RETURN)

    def _metadata(self, item: QueueItem) -> dict[str, Any]:
        md = {"queue_item_uid": item.uid}
        if item.label is not None:
            md["queue_item_label"] = item.label
        return md

    def _sync(self) -> None:
        # Fallback for a replaced RE.state_hook: notice an item finished outside the queue.
        if self._state == "paused" and self.RE.state == "idle":
            self._finish_outside()

    def _finish_outside(self) -> None:
        item = self._current
        assert item is not None
        result = self._result()
        self._record(item, result)
        self._state = "held"
        print(OUTSIDE_MSG.format(item=item, status=result.exit_status, n=len(self._pending)))

    def _install_state_hook(self) -> None:
        previous = self.RE.state_hook

        def state_hook(new_state: str, old_state: str) -> None:
            if previous is not None:
                previous(new_state, old_state)
            # Only a paused item can reach 'idle' without the queue driving the call.
            if new_state == "idle" and self._state == "paused":
                self._finish_outside()

        self.RE.state_hook = state_hook
