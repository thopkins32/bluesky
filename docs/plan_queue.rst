.. currentmodule:: bluesky.plan_queue

Plan Queue
==========

A :class:`PlanQueue` is an ordered, editable list of plans that the RunEngine
runs one at a time.  It is the alternative to a "macro": a plan that strings
together ``yield from`` steps for a whole shift.  A macro's steps are control
flow inside a running generator, so they cannot be seen, reordered or dropped
once it has started, and an exception at step 7 of 20 ends steps 8 to 20.  A
queue keeps the same steps as *data*: you can inspect and edit what is left at
any pause, a failing step leaves the rest intact, and between steps the
RunEngine is free for other work.

Plans themselves do not change.  A step that needs adaptive logic, or that
must share state with the step before it, is still written as a plan; it just
becomes one item of the queue.

Quick start
-----------

.. code-block:: python

    from bluesky.plan_queue import PlanQueue
    from bluesky.plan_stubs import mv
    from bluesky.plans import count, scan

    queue = PlanQueue(RE)

    for sample in samples:
        queue.add(mv, sx, sample.x, sy, sample.y).label = f"{sample.name}: move"
        queue.add(scan, [det], motor, -1, 1, 21).label = f"{sample.name}: scan"
        queue.add(count, [det], num=10, md={"sample": sample.name})

    queue           # lists history, the current item and what is pending
    queue.check()   # summarize every item and check motor limits, moving nothing
    queue.run()     # blocks, like RE(plan); Ctrl+C pauses the running item

Everything after the plan in :meth:`PlanQueue.add` is passed to the plan, so
``md=`` and any other plan keyword work as usual.  :meth:`PlanQueue.add`
returns the :class:`QueueItem`; set ``label`` on it to name the step in
listings and in the ``queue_item_label`` metadata of its runs.

Items are plan *factories*
--------------------------

An item stores the plan function and its arguments, not a plan generator.
The plan is built fresh each time the item runs, so an item can be simulated
with :meth:`PlanQueue.check`, retried after a failure, or run again after
:meth:`PlanQueue.hold`.

This has one consequence worth remembering: **arguments are evaluated when
the item is added; plan bodies run later.**  In a macro,

.. code-block:: python

    peak = yield from align()
    yield from scan([det], motor, peak - 1, peak + 1, 21)

reads ``peak`` when that line runs.  The queue equivalent must read it inside
a plan:

.. code-block:: python

    def scan_around_peak():
        c = peak_position.get()
        yield from scan([det], motor, c - 1, c + 1, 21)

    queue.add(align)
    queue.add(scan_around_peak)

Two steps that share a variable belong in one plan, added as one item.

Editing the queue
-----------------

While the queue is not running (idle, paused or held) the pending items can
be changed freely:

====================================  =====================================================
``queue.add(plan, *args, **kwargs)``  Append an item.
``queue.insert(pos, plan, ...)``      Insert before pending position ``pos``.
``queue.remove(item_or_pos)``         Remove and return a pending item.
``queue.move(item_or_pos, pos)``      Move a pending item to position ``pos``.
``queue.clear()``                     Remove and return all pending items.
``queue[pos]``, ``len(queue)``        Index and count the pending items.
====================================  =====================================================

``args`` and ``kwargs`` of a pending :class:`QueueItem` are read when it runs,
so ``queue[3].kwargs["num"] = 5`` takes effect.

Running, pausing and recovering
-------------------------------

:meth:`PlanQueue.run` runs pending items in order on the calling thread and
returns when the queue is empty, when ``limit`` items have finished, or when
something needs a decision.  Each item is its own ``RE(plan)`` call: the runs
it produces carry the real ``plan_name`` and ``plan_args`` of that step plus
``queue_item_uid`` (and ``queue_item_label`` if set), suspenders and
``RE.preprocessors`` apply per item, and the RunEngine is idle between items.

Ctrl+C behaves exactly as it does for a bare ``RE(plan)``: the running item
pauses and you get the prompt.  The queue's state is then ``'paused'`` and the
message lists the choices:

========================  ===========================================================
``queue.resume()``        Resume the item from its last checkpoint, then carry on.
``queue.skip()``          ``RE.stop()`` the item (clean up, ``exit_status='success'``)
                          and carry on with the next one.
``queue.hold()``          ``RE.abort()`` the item, put it back at the front, and
                          leave the RunEngine free.  ``queue.resume()`` later runs the
                          item again from the start.
========================  ===========================================================

``RE.resume()``, ``RE.stop()``, ``RE.abort()`` and ``RE.halt()`` still work
on the paused item.  The queue notices the item finishing and goes to
``'held'`` with the remaining items intact; ``queue.resume()`` continues.

An item that raises is handled the same way a failed step at 3 a.m. should
be: the RunEngine cleans up as usual, the exception propagates out of
:meth:`PlanQueue.run`, the item goes **back to the front of the queue**, and
the queue is ``'held'``.  Nothing after it runs until you decide:
``queue.resume()`` retries the item, ``queue.skip()`` drops it and continues.

``queue.run(1)`` or ``queue.resume(1)`` finish one item and then hold, which
is the way to do something else with the RunEngine between two steps.

History
-------

Every finished item is appended to ``queue.history`` as a
:class:`HistoryEntry` holding the item, a
:class:`~bluesky.run_engine.RunEngineResult` (run uids, exit status, whether
it was interrupted, the exception if it raised, and the plan's return value
when the RunEngine is constructed with ``call_returns_result=True``) and
start/stop times.

Limitations
-----------

* The queue runs on the calling thread and blocks while an item runs, as
  ``RE(plan)`` does.  The prompt is available at pauses and between items,
  not during an item.
* The queue calls the RunEngine from inside a method, so a ``RE.max_depth``
  set to forbid calling the RunEngine from functions must allow two more
  frames.
* The queue wraps ``RE.state_hook`` to notice items finished directly through
  the RunEngine.  Wrap, do not replace, ``RE.state_hook`` after creating a
  queue.
* Items hold live Python objects and are not persisted.  Keep the script that
  builds the queue as its record.

API
---

.. autosummary::
   :toctree: generated
   :nosignatures:

   PlanQueue
   QueueItem
   HistoryEntry
