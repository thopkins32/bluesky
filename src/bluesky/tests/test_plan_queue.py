import pytest

from bluesky.plan_queue import PlanQueue
from bluesky.plan_stubs import checkpoint, mv, null, pause
from bluesky.plans import count
from bluesky.utils import RunEngineInterrupted


def pausing(motor, pause_once):
    """Move to 1, pause (only while ``pause_once`` is non-empty), move to 2."""
    yield from checkpoint()
    yield from mv(motor, 1)
    if pause_once:
        pause_once.clear()
        yield from pause()
    yield from mv(motor, 2)


def paused_queue(RE, motor):
    """A queue paused inside its first item, with one more item pending."""
    queue = PlanQueue(RE)
    item = queue.add(pausing, motor, [True])
    after = queue.add(mv, motor, 10)
    with pytest.raises(RunEngineInterrupted, match="queue.resume"):
        queue.run()
    assert queue.state == "paused"
    assert queue.current is item
    assert RE.state == "paused"
    return queue, item, after


def test_items_run_in_order_each_as_its_own_call(RE, hw):
    starts = []
    RE.subscribe(lambda name, doc: starts.append(doc), "start")

    def returning():
        yield from null()
        return 42

    queue = PlanQueue(RE)
    first = queue.add(count, [hw.det], num=2)
    first.label = "dark"
    second = queue.add(mv, hw.motor, 3)
    third = queue.add(returning)
    fourth = queue.add(count, [hw.det])

    queue.run()

    assert queue.state == "idle"
    assert len(queue) == 0
    assert queue.current is None
    assert [entry.item for entry in queue.history] == [first, second, third, fourth]
    assert [entry.result.exit_status for entry in queue.history] == ["success"] * 4
    assert hw.motor.position == 3
    assert queue.history[0].result.run_start_uids == (starts[0]["uid"],)
    assert [doc["queue_item_uid"] for doc in starts] == [first.uid, fourth.uid]
    assert starts[0]["queue_item_label"] == "dark"
    assert "queue_item_label" not in starts[1]
    assert [doc["plan_name"] for doc in starts] == ["count", "count"]
    plan_result = queue.history[2].result.plan_result
    assert plan_result == 42 if RE.call_returns_result else plan_result is RE.NO_PLAN_RETURN


def test_failed_item_goes_back_to_the_front_and_queue_holds(RE, hw):
    attempts = []

    def flaky():
        attempts.append(None)
        yield from null()
        if len(attempts) == 1:
            raise ValueError("first attempt fails")

    queue = PlanQueue(RE)
    before = queue.add(mv, hw.motor, 1)
    bad = queue.add(flaky)
    after = queue.add(mv, hw.motor, 2)

    with pytest.raises(ValueError, match="first attempt"):
        queue.run()

    assert queue.state == "held"
    assert queue.pending == (bad, after)
    assert hw.motor.position == 1
    failed = queue.history[-1]
    assert failed.item is bad
    assert failed.result.exit_status == "fail"
    assert isinstance(failed.result.exception, ValueError)

    queue.resume()

    assert queue.state == "idle"
    assert [entry.item for entry in queue.history] == [before, bad, bad, after]
    assert queue.history[2].result.exit_status == "success"
    assert hw.motor.position == 2


def test_skip_drops_the_failed_item(RE, hw):
    def broken():
        yield from null()
        raise RuntimeError("no")

    queue = PlanQueue(RE)
    bad = queue.add(broken)
    after = queue.add(mv, hw.motor, 5)
    with pytest.raises(RuntimeError, match="no"):
        queue.run()

    queue.skip()

    assert queue.state == "idle"
    assert hw.motor.position == 5
    assert [entry.item for entry in queue.history] == [bad, after]


def test_unbuildable_plan_is_recorded_as_a_failure(RE, hw):
    queue = PlanQueue(RE)
    bad = queue.add(count, [hw.det], no_such_argument=1)

    with pytest.raises(TypeError):
        queue.run()

    assert queue.state == "held"
    assert queue.pending == (bad,)
    assert queue.history[-1].result.exit_status == "fail"
    assert isinstance(queue.history[-1].result.exception, TypeError)


def test_pause_then_resume_finishes_item_and_continues(RE, hw):
    queue, item, after = paused_queue(RE, hw.motor)
    with pytest.raises(RuntimeError, match="paused"):
        queue.run()

    queue.resume()

    assert queue.state == "idle"
    assert RE.state == "idle"
    assert hw.motor.position == 10
    assert [entry.item for entry in queue.history] == [item, after]
    assert queue.history[0].result.exit_status == "success"
    assert not queue.history[0].result.interrupted


def test_skip_stops_paused_item_and_continues(RE, hw):
    queue, item, after = paused_queue(RE, hw.motor)
    positions = []
    hw.motor.subscribe(lambda value, **kwargs: positions.append(value))

    queue.skip()

    assert queue.state == "idle"
    assert RE.state == "idle"
    assert 2 not in positions  # the rest of the paused item never ran
    assert hw.motor.position == 10
    assert [entry.item for entry in queue.history] == [item, after]
    assert queue.history[0].result.interrupted
    assert queue.history[0].result.exit_status == "success"


def test_hold_aborts_item_requeues_it_and_frees_the_run_engine(RE, hw):
    queue, item, after = paused_queue(RE, hw.motor)

    queue.hold()

    assert queue.state == "held"
    assert RE.state == "idle"
    assert queue.pending == (item, after)
    assert queue.history[0].item is item
    assert queue.history[0].result.exit_status == "abort"

    RE(mv(hw.motor, -1))  # the RunEngine is free for other work meanwhile
    assert hw.motor.position == -1

    queue.resume()

    assert queue.state == "idle"
    assert [entry.item for entry in queue.history] == [item, item, after]
    assert queue.history[1].result.exit_status == "success"
    assert hw.motor.position == 10


@pytest.mark.parametrize(
    "method, exit_status",
    [("resume", "success"), ("stop", "success"), ("abort", "abort"), ("halt", "abort")],
)
def test_item_finished_directly_through_run_engine_holds_queue(RE, hw, capsys, method, exit_status):
    seen = []
    RE.state_hook = lambda new, old: seen.append(new)  # a pre-existing hook keeps working
    queue, item, after = paused_queue(RE, hw.motor)

    getattr(RE, method)()

    assert queue.state == "held"
    assert queue.current is None
    assert queue.pending == (after,)
    assert queue.history[0].item is item
    assert queue.history[0].result.exit_status == exit_status
    assert "finished outside the queue" in capsys.readouterr().out
    assert "idle" in seen

    queue.resume()

    assert queue.state == "idle"
    assert hw.motor.position == 10
    assert [entry.item for entry in queue.history] == [item, after]


def test_editing_pending_items(RE, hw):
    queue = PlanQueue(RE)
    a = queue.add(mv, hw.motor, 1)
    b = queue.add(mv, hw.motor, 2)
    c = queue.add(mv, hw.motor, 3)
    d = queue.insert(0, mv, hw.motor, 4)
    assert queue.pending == (d, a, b, c)

    assert queue.remove(b) is b
    queue.move(c, 0)
    assert queue.pending == (c, d, a)
    assert queue[1] is d
    assert list(queue) == [c, d, a]
    with pytest.raises(ValueError):
        queue.remove(b)
    assert queue.remove(0) is c
    assert queue.clear() == [d, a]
    assert len(queue) == 0

    e = queue.add(mv, hw.motor, 1)
    e.args = (hw.motor, 7)  # arguments are read when the item runs
    queue.run()
    assert hw.motor.position == 7
    e.args = (hw.motor, 8)  # history records what ran, not what the item says now
    assert queue.history[-1].description == "mv(motor, 7)"


def test_run_limit_holds_with_items_pending(RE, hw):
    queue = PlanQueue(RE)
    items = [queue.add(mv, hw.motor, i) for i in range(3)]

    queue.run(1)
    assert queue.state == "held"
    assert queue.pending == tuple(items[1:])
    assert hw.motor.position == 0

    queue.resume(1)
    assert queue.state == "held"
    assert hw.motor.position == 1

    queue.resume()
    assert queue.state == "idle"
    assert hw.motor.position == 2


def test_resume_limit_counts_the_paused_item(RE, hw):
    queue, item, after = paused_queue(RE, hw.motor)

    queue.resume(1)

    assert queue.state == "held"
    assert queue.pending == (after,)
    assert hw.motor.position == 2


def test_check_summarizes_without_running(RE, hw, capsys):
    queue = PlanQueue(RE)
    queue.add(mv, hw.motor, 5)
    queue.add(count, [hw.det])

    queue.check()

    out = capsys.readouterr().out
    assert "motor -> 5" in out
    assert "Open Run" in out
    assert hw.motor.position == 0
    assert len(queue) == 2
    assert queue.state == "idle"


def test_queue_refuses_to_start_an_item_on_a_busy_run_engine(RE, hw):
    with pytest.raises(RunEngineInterrupted):
        RE(pausing(hw.motor, [True]))
    queue = PlanQueue(RE)
    item = queue.add(mv, hw.motor, 10)

    with pytest.raises(RuntimeError, match="paused state"):
        queue.run()

    assert queue.state == "held"
    assert queue.pending == (item,)
    RE.stop()


def test_repr_lists_history_current_and_pending(RE, hw):
    queue, item, after = paused_queue(RE, hw.motor)
    text = repr(queue)
    assert "state='paused'" in text
    assert f"paused:\n    {item}" in text
    assert f"0  {after}" in text
    assert str(after) == "mv(motor, 10)"
    queue.skip()
    text = repr(queue)
    assert "state='idle'" in text
    assert f"success  {item}" in text
