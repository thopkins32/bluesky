from unittest.mock import MagicMock

import pytest

import bluesky.plan_stubs as bps
from bluesky.preprocessors import (
    contingency_decorator,
    contingency_wrapper,
    msg_mutator,
    repeat_as_stub_wrapper,
    run_decorator,
    stage_decorator,
)
from bluesky.protocols import HasName, HasParent, Movable, Stageable
from bluesky.run_engine import RequestStop, RunEngine


def test_given_a_plan_that_raises_contigency_will_call_except_plan_with_exception_and_run_engine_errors():
    expected_exception = Exception()

    def except_plan(exception: Exception):
        assert exception == expected_exception
        yield from bps.null()

    # Mock so we can assert called
    except_plan = MagicMock(side_effect=except_plan)

    @contingency_decorator(except_plan=except_plan)
    def raising_plan():
        yield from bps.null()
        raise expected_exception

    RE = RunEngine()

    with pytest.raises(Exception) as exception:
        RE(raising_plan())
        assert exception == expected_exception

    except_plan.assert_called_once()


def test_given_a_plan_that_raises_contigency_with_no_auto_raise_will_call_except_plan_and_RE_does_not_raise():
    expected_exception = Exception()
    expected_return_value = "test"

    def except_plan(exception: Exception):
        assert exception == expected_exception
        yield from bps.null()
        return expected_return_value

    # Mock so we can assert called
    except_plan = MagicMock(side_effect=except_plan)

    @contingency_decorator(except_plan=except_plan, auto_raise=False)
    def raising_plan():
        yield from bps.null()
        raise expected_exception

    RE = RunEngine(call_returns_result=True)

    returned_value = RE(raising_plan())

    except_plan.assert_called_once()
    assert returned_value.plan_result == expected_return_value


def test_given_a_plan_that_raises_contigency_with_no_auto_raise_and_except_plan_that_reraises_run_engine_errors():
    expected_exception = Exception()

    def except_plan(exception: Exception):
        assert exception == expected_exception
        yield from bps.null()
        raise exception

    # Mock so we can assert called
    except_plan = MagicMock(side_effect=except_plan)

    @contingency_decorator(except_plan=except_plan, auto_raise=False)
    def raising_plan():
        yield from bps.null()
        raise expected_exception

    RE = RunEngine()

    with pytest.raises(Exception) as exception:
        RE(raising_plan())
        assert exception == expected_exception

    except_plan.assert_called_once()


def test_exceptions_through_msg_mutator():
    from bluesky import Msg

    def outer():
        for j in range(50):
            yield Msg(f"step {j}")

    def attach(msg):
        cmd = msg.command
        return msg._replace(command=f"{cmd}+")

    def except_plan(e):
        yield Msg("handle it")

    gen = msg_mutator(contingency_wrapper(outer(), except_plan=except_plan), attach)

    msgs = []

    msgs.append(next(gen))
    msgs.append(next(gen))
    msgs.append(next(gen))
    msgs.append(next(gen))
    msgs.append(gen.throw(RequestStop))
    try:
        while True:
            msgs.append(next(gen))
    except RequestStop:
        pass
    else:
        raise False  # noqa: B016
    assert ["step 0+", "step 1+", "step 2+", "step 3+", "handle it+"] == [m.command for m in msgs]


def test_repeat_as_stub_wrapper():
    class Device(Stageable, HasParent, HasName, Movable): ...

    stageable1 = MagicMock(spec=Device)
    stageable2 = MagicMock(spec=Device)
    stageable1.name = "stageable1"
    stageable2.name = "stageable2"
    stageable1.parent = None
    stageable2.parent = None

    @stage_decorator([stageable1, stageable2])
    @run_decorator(md={"plan_name": "test_plan"})
    def plan(stageable1, stageable2):
        yield from bps.mv(stageable1, 1)
        yield from bps.mv(stageable2, 2)

    gen = repeat_as_stub_wrapper(plan(stageable1, stageable2), num_repeats=2)

    commands = []
    for msg in gen:
        commands.append(msg.command)
        if msg.command == "open_run":
            assert msg.kwargs == {"plan_name": "test_plan", "num_repeats": 2}
    assert commands == [
        "open_run",
        "stage",
        "set",
        "wait",
        "stage",
        "set",
        "wait",
        "set",
        "wait",
        "set",
        "wait",
        "close_run",
        "unstage",
        "unstage",
    ]
