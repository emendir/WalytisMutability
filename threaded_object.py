"""Class with the ability to queue methods to be run on the same thread."""

import functools
import queue
import threading
from queue import Queue
from threading import Thread
from typing import Any, Callable, Tuple


class DedicatedThreadClass:
    """Class with the ability to queue methods to be run on the same thread."""

    def __init__(self) -> None:
        self._terminate_flag: bool = False
        # Initialize a queue for task communication
        self.task_queue: Queue[Tuple[Callable, Tuple, dict]] = Queue()
        # Create and start a dedicated thread
        self.thread: Thread = Thread(target=self._run)
        self.thread.start()
        # A flag to stop the thread gracefully

    def _run(self) -> None:
        while not self._terminate_flag:
            try:
                # Fetch a task from the queue and execute it
                task, args, kwargs = self.task_queue.get(timeout=0.1)
                task(*args, **kwargs)
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
            func(*args, **kwargs)
        else:
            # Otherwise, enqueue the task to be run on the dedicated thread
            self.task_queue.put((func, args, kwargs))

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
            self._run_on_dedicated_thread(func, self, *args, **kwargs)
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
            print(
                f"Executing core_method with {param1} and {param2} on thread: "
                f"{threading.current_thread().name}"
            )
            # Core functionality here

    # Example usage:
    obj = MyClass()
    obj.core_method('arg1', 'arg2')  # This will run on the dedicated thread
    obj.terminate()                  # This will stop the dedicated thread
