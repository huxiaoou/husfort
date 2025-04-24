import time
from multiprocessing import Pool, Queue, Process, Manager
from multiprocessing import set_start_method, get_start_method
from rich.progress import Progress, TaskID, BarColumn, TimeRemainingColumn, TimeElapsedColumn
from typing import Callable
from enum import IntEnum
from dataclasses import dataclass


class EStatusWorker(IntEnum):
    START = 0
    FINISHED = 1


class EOperationQueue(IntEnum):
    SET_DESC = 0
    SET_TOTAL = 1
    SET_ADVANCE = 2
    SET_COMPLETE = 3
    SET_STATUS = 4


@dataclass
class TMsg:
    task_id: TaskID
    operation: EOperationQueue
    val: int | float | str | EStatusWorker


class CAgentQueue:
    def __init__(self, task_id: TaskID, queue: Queue):
        self.task_id = task_id
        self.queue = queue

    def set_description(self, desc: str):
        msg = TMsg(self.task_id, EOperationQueue.SET_DESC, desc)
        self.queue.put(msg)
        return 0

    def set_total(self, total: int):
        msg = TMsg(self.task_id, EOperationQueue.SET_TOTAL, total)
        self.queue.put(msg)
        return 0

    def set_advance(self, advance: int):
        msg = TMsg(self.task_id, EOperationQueue.SET_ADVANCE, advance)
        self.queue.put(msg)
        return 0

    def set_completed(self, completed: int):
        msg = TMsg(self.task_id, EOperationQueue.SET_COMPLETE, completed)
        self.queue.put(msg)
        return 0

    def set_status(self, status: EStatusWorker):
        msg = TMsg(self.task_id, EOperationQueue.SET_STATUS, status)
        self.queue.put(msg)
        return 0


def update_progress(pb: Progress, size: int, queue: Queue, seconds_between_check: float):
    completed = 0
    while completed < size:
        if not queue.empty():
            msg: TMsg = queue.get()
            if msg.operation == EOperationQueue.SET_DESC:
                pb.update(msg.task_id, description=msg.val)
            elif msg.operation == EOperationQueue.SET_TOTAL:
                pb.update(msg.task_id, total=msg.val)
            elif msg.operation == EOperationQueue.SET_ADVANCE:
                pb.update(msg.task_id, advance=msg.val)
            elif msg.operation == EOperationQueue.SET_COMPLETE:
                pb.update(msg.task_id, completed=msg.val)
            elif msg.operation == EOperationQueue.SET_STATUS:
                if msg.val == EStatusWorker.FINISHED:
                    completed += 1
        time.sleep(seconds_between_check)
    return 0


TTask = tuple[Callable, tuple]
"""
+ TTask is a type designed to manage functions and their parameters. If you want to use it with
  uni_process_for_tasks and mul_process_for_tasks, then the following 2 rules must be applied:
    + TTask.Callable: a function, the type of its first parameter MUST be CAgentQueue
    + TTask.Tuple: other parameters accepted by TTask.Callable, with the FIRST one EXCLUDED
"""


def mul_process_for_tasks(
        tasks: list[TTask],
        processes: int = None,
        bar_width: int = 100,
        seconds_between_check: float = 0.01,
):
    """

    :param tasks: a TTask, the first parameter of TTask.callable must be CAgentQueue.
                  And other parameters for the callable are provided by the second
                  element in TTask, i.e. TTask.tuple.
    :param processes: number of processes to use, by default is None.If processes is
                      None then the number returned by os.process_cpu_count() is used.
    :param bar_width: the width of the progress bar
    :param seconds_between_check: time duration between checks of tasks
    :return:
    """

    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=bar_width),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            TimeElapsedColumn(),
    ) as pb:
        if not get_start_method():
            set_start_method("spawn")
        with Manager() as manager:
            queue = manager.Queue()
            with Pool(processes=processes) as pool:
                for f, args in tasks:
                    task_id = pb.add_task(description="New Task")
                    agent_queue = CAgentQueue(task_id, queue)
                    pool.apply_async(
                        f, args=(agent_queue, *args),
                        error_callback=lambda e: print(e),
                    )
                pool.close()
                update_progress(pb, len(tasks), queue, seconds_between_check)
                pool.join()
    return 0


def uni_process_loop(tasks: list[TTask], task_ids: list[TaskID], queue: Queue):
    for (f, args), task_id in zip(tasks, task_ids):
        agent_queue = CAgentQueue(task_id, queue)
        f(agent_queue, *args)
    return 0


def uni_process_for_tasks(
        tasks: list[TTask],
        bar_width: int = 100,
        seconds_between_check: float = 0.01,
):
    """

    :param tasks: a TTask, the first parameter of TTask.callable must be CAgentQueue.
                  And other parameters for the callable are provided by the second
                  element in TTask, i.e. TTask.tuple.
    :param bar_width: the width of the progress bar
    :param seconds_between_check: time duration between checks of tasks
    :return:
    """
    with Progress(
            "[progress.description]{task.description}",
            BarColumn(bar_width=bar_width),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            TimeElapsedColumn(),
    ) as pb:
        task_ids = [pb.add_task(description="New Task") for _ in range(len(tasks))]

        if not get_start_method():
            set_start_method("spawn")
        with Manager() as manager:
            queue = manager.Queue()
            p = Process(target=uni_process_loop, args=(tasks, task_ids, queue))
            p.start()
            update_progress(pb, len(tasks), queue, seconds_between_check)
            p.join()
    return 0
