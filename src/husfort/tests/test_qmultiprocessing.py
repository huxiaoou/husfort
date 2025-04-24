import time
from random import randint
from husfort.qmultiprocessing import CAgentQueue, EStatusWorker, TTask, mul_process_for_tasks, uni_process_for_tasks


def worker(agent_queue: CAgentQueue, total: int, minimum_sleep: float):
    """

    :param agent_queue: the first parameter must be CAgentQueue
    :param total: the total steps of this task
    :param minimum_sleep: the minimum sleep time, unit = seconds
    :return:
    """

    agent_queue.set_description(f"Task {agent_queue.task_id:>02d} Total = {total}")
    agent_queue.set_total(total)
    for i in range(total):
        time.sleep(minimum_sleep * randint(a=1, b=3))
        agent_queue.set_completed(i + 1)  # agent_queue.set_advance(1)
    agent_queue.set_status(EStatusWorker.FINISHED)
    return 0


if __name__ == "__main__":
    tasks = [
        TTask((worker, (20, 0.1))),
        TTask((worker, (15, 0.2))),
        TTask((worker, (20, 0.3))),
        TTask((worker, (10, 0.4))),
    ]
    print("Start tasks in mul-process model")
    mul_process_for_tasks(tasks=tasks)
    print("\n", end="")
    print("Start tasks in uni-process model")
    uni_process_for_tasks(tasks=tasks)
