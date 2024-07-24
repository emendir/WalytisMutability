"""Class with the ability to queue methods to be run on the same thread."""

import functools
import queue
import threading
from queue import Queue
from threading import Thread, Event
from typing import Any, Callable, Tuple
from utils import logger


class DedicatedThreadClass:
    """Class to run functions sequentially on the same dedicated thread."""

    def __init__(self) -> None:
        # A flag to stop the thread gracefully
        self._terminate_flag: bool = False
        # Initialize a queue for task communication
        self.task_queue: Queue[Tuple[Callable, Tuple, dict]] = Queue()
        # Create and start a dedicated thread
        self.thread: Thread = Thread(target=self._run)
        self.thread.start()

    def _run(self) -> None:
        while not self._terminate_flag:
            try:
                # Fetch a task from the queue and execute it
                task, args, kwargs, result_queue = self.task_queue.get(timeout=0.1)
                try:
                    # logger.debug(f"DTC: Running function {task.__name__} on dedicated thread...")
                    result = task(*args, **kwargs)
                    # logger.debug(f"DTC: Finished running function {
                    # task.__name__} on dedicated thread.")

                except Exception as e:
                    logger.debug(f"DTC: Function {task.__name__} ran into an exception.")
                    logger.debug(str(e))
                # logger.debug("DTC: Returning result...")
                result_queue.put(result)
            except queue.Empty:
                continue

    def _is_on_dedicated_thread(self) -> bool:
        # Check if current thread is the dedicated thread
        return threading.current_thread() == self.thread

    def _run_on_dedicated_thread(
        self, func: Callable, *args: Any, **kwargs: Any
    ) -> None:
        if self._is_on_dedicated_thread():
            # Run the function directly if already on the dedicated thread
            return func(*args, **kwargs)
        else:
            # Otherwise, enqueue the task to be run on the dedicated thread
            result_queue = Queue()  # prepare a queue for capturing the result
            self.task_queue.put((func, args, kwargs, result_queue))

            # wait for the function's return value
            result = result_queue.get(block=True)
            return result

    def terminate(self) -> None:
        # Stop the thread gracefully
        self._terminate_flag = True
        self.thread.join()
        # Clean up resources
        with self.task_queue.mutex:
            self.task_queue.queue.clear()


def run_on_dedicated_thread(func: Callable) -> Callable:
    """Run methods of children of DedicatedThreadClass on dedicated thread.

    Is a decorator.
    """
    @functools.wraps(func)
    def wrapper(self: DedicatedThreadClass, *args: Any, **kwargs: Any) -> None:
        if hasattr(self, '_run_on_dedicated_thread'):
            result = self._run_on_dedicated_thread(func, self, *args, **kwargs)
            return result
        else:
            raise AttributeError(
                f"{self.__class__.__name__} must inherit from "
                "DedicatedThreadClass to use run_on_dedicated_thread")
    return wrapper


if __name__ == "__main__":
    # Example subclass using the decorator
    class MyClass(DedicatedThreadClass):
        def __init__(self) -> None:
            super().__init__()

        @run_on_dedicated_thread
        def core_method(self, param1: str, param2: str) -> None:
            logger.debug(
                f"Executing core_method with {param1} and {param2} on thread: "
                f"{threading.current_thread().name}"
            )
            # Core functionality here

    # Example usage:
    obj = MyClass()
    obj.core_method('arg1', 'arg2')  # This will run on the dedicated thread
    obj.terminate()                  # This will stop the dedicated thread
