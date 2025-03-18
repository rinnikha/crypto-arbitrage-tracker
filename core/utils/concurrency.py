# core/concurrency.py
"""
Utilities for handling concurrent operations safely.
"""
import logging
import time
import threading
import concurrent.futures
from typing import List, Dict, Any, Callable, TypeVar, Generic, Optional, Tuple, Union, Set
from dataclasses import dataclass
from functools import wraps

logger = logging.getLogger(__name__)

# Type variables for generics
T = TypeVar('T')  # Input type
R = TypeVar('R')  # Result type


@dataclass
class TaskResult(Generic[T, R]):
    """Result of a task execution with context."""
    input_data: T
    result: Optional[R] = None
    error: Optional[Exception] = None
    success: bool = True
    execution_time: float = 0.0

    @property
    def failed(self) -> bool:
        """Check if the task failed."""
        return not self.success


class ThreadSafeCounter:
    """Thread-safe counter for tracking progress."""

    def __init__(self, initial_value: int = 0):
        """
        Initialize the counter.

        Args:
            initial_value: Initial counter value
        """
        self._value = initial_value
        self._lock = threading.RLock()

    def increment(self, amount: int = 1) -> int:
        """
        Increment the counter.

        Args:
            amount: Amount to increment by

        Returns:
            New counter value
        """
        with self._lock:
            self._value += amount
            return self._value

    def decrement(self, amount: int = 1) -> int:
        """
        Decrement the counter.

        Args:
            amount: Amount to decrement by

        Returns:
            New counter value
        """
        with self._lock:
            self._value -= amount
            return self._value

    def reset(self, value: int = 0) -> None:
        """
        Reset the counter.

        Args:
            value: Value to reset to
        """
        with self._lock:
            self._value = value

    @property
    def value(self) -> int:
        """Get the current counter value."""
        with self._lock:
            return self._value


class ThreadSafeSet(Generic[T]):
    """Thread-safe set implementation."""

    def __init__(self, initial_items: Optional[Set[T]] = None):
        """
        Initialize the set.

        Args:
            initial_items: Initial set items
        """
        self._items = set(initial_items or set())
        self._lock = threading.RLock()

    def add(self, item: T) -> None:
        """
        Add an item to the set.

        Args:
            item: Item to add
        """
        with self._lock:
            self._items.add(item)

    def remove(self, item: T) -> None:
        """
        Remove an item from the set.

        Args:
            item: Item to remove
        """
        with self._lock:
            self._items.remove(item)

    def clear(self) -> None:
        """Clear the set."""
        with self._lock:
            self._items.clear()

    def __contains__(self, item: T) -> bool:
        """Check if an item is in the set."""
        with self._lock:
            return item in self._items

    def __len__(self) -> int:
        """Get the set size."""
        with self._lock:
            return len(self._items)

    def copy(self) -> Set[T]:
        """Get a copy of the set."""
        with self._lock:
            return self._items.copy()


class ConcurrentExecutor(Generic[T, R]):
    """
    Executor for running tasks concurrently with proper error handling and result tracking.
    """

    def __init__(self,
                 max_workers: Optional[int] = None,
                 thread_name_prefix: str = "worker",
                 use_processes: bool = False):
        """
        Initialize the executor.

        Args:
            max_workers: Maximum number of workers (default: CPU count * 5 for threads, CPU count for processes)
            thread_name_prefix: Prefix for thread names
            use_processes: Whether to use processes instead of threads
        """
        self.max_workers = max_workers
        self.thread_name_prefix = thread_name_prefix
        self.use_processes = use_processes
        self.executor_class = (concurrent.futures.ProcessPoolExecutor
                               if use_processes
                               else concurrent.futures.ThreadPoolExecutor)

    def execute(self,
                items: List[T],
                worker_func: Callable[[T], R],
                timeout: Optional[float] = None,
                return_when_first_exception: bool = False,
                callback: Optional[Callable[[TaskResult[T, R]], None]] = None) -> List[TaskResult[T, R]]:
        """
        Execute tasks concurrently.

        Args:
            items: List of input items
            worker_func: Worker function to execute for each item
            timeout: Timeout for the entire execution
            return_when_first_exception: Whether to return immediately when an exception occurs
            callback: Callback function to call with each task result

        Returns:
            List of task results
        """
        if not items:
            return []

        results: List[TaskResult[T, R]] = []

        # Create the executor
        executor_kwargs = {"max_workers": self.max_workers}
        if not self.use_processes:  # Thread-specific args
            executor_kwargs["thread_name_prefix"] = self.thread_name_prefix

        with self.executor_class(**executor_kwargs) as executor:
            # Submit all tasks
            future_to_item = {
                executor.submit(self._execute_task, worker_func, item): item
                for item in items
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_item, timeout=timeout):
                item = future_to_item[future]
                try:
                    task_result = future.result()
                    results.append(task_result)

                    # Call the callback if provided
                    if callback:
                        try:
                            callback(task_result)
                        except Exception as e:
                            logger.error(f"Error in callback for item {item}: {e}")

                    # Check if we should return early
                    if return_when_first_exception and not task_result.success:
                        logger.warning(f"Returning early due to exception in task: {task_result.error}")
                        executor.shutdown(wait=False)
                        break

                except concurrent.futures.TimeoutError:
                    logger.error(f"Task for item {item} timed out")
                    results.append(TaskResult(
                        input_data=item,
                        result=None,
                        error=concurrent.futures.TimeoutError(f"Task for item {item} timed out"),
                        success=False,
                        execution_time=0.0
                    ))
                except Exception as e:
                    logger.exception(f"Error processing task result for item {item}: {e}")
                    results.append(TaskResult(
                        input_data=item,
                        result=None,
                        error=e,
                        success=False,
                        execution_time=0.0
                    ))

        return results

    def _execute_task(self, worker_func: Callable[[T], R], item: T) -> TaskResult[T, R]:
        """
        Execute a single task with timing and error handling.

        Args:
            worker_func: Worker function to execute
            item: Input item

        Returns:
            Task result
        """
        start_time = time.time()
        try:
            result = worker_func(item)
            execution_time = time.time() - start_time
            return TaskResult(
                input_data=item,
                result=result,
                error=None,
                success=True,
                execution_time=execution_time
            )
        except Exception as e:
            execution_time = time.time() - start_time
            logger.exception(f"Error executing task for item {item}: {e}")
            return TaskResult(
                input_data=item,
                result=None,
                error=e,
                success=False,
                execution_time=execution_time
            )


def run_in_thread(daemon: bool = True):
    """
    Decorator to run a function in a separate thread.

    Args:
        daemon: Whether the thread should be a daemon
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            thread = threading.Thread(
                target=func,
                args=args,
                kwargs=kwargs,
                daemon=daemon
            )
            thread.start()
            return thread

        return wrapper

    return decorator


class LimitedThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    Thread pool executor with an upper bound on running tasks.
    """

    def __init__(self, max_workers=None, thread_name_prefix="",
                 initializer=None, initargs=(), max_queue_size=None):
        """
        Initialize the executor.

        Args:
            max_workers: Maximum number of workers
            thread_name_prefix: Prefix for thread names
            initializer: Initializer function for workers
            initargs: Arguments for initializer
            max_queue_size: Maximum number of tasks waiting in queue (None for unlimited)
        """
        super().__init__(max_workers, thread_name_prefix, initializer, initargs)
        self.max_queue_size = max_queue_size
        self._work_queue_semaphore = (
            threading.Semaphore(max_queue_size) if max_queue_size is not None else None
        )

    def submit(self, fn, *args, **kwargs):
        """
        Submit a task, respecting the queue size limit.

        Args:
            fn: Function to execute
            *args: Arguments
            **kwargs: Keyword arguments

        Returns:
            Future object
        """
        if self._work_queue_semaphore:
            self._work_queue_semaphore.acquire()

            # Create a callback to release the semaphore when the task completes
            callback = lambda future: self._work_queue_semaphore.release()

            # Submit the task and add the callback
            future = super().submit(fn, *args, **kwargs)
            future.add_done_callback(callback)
            return future
        else:
            return super().submit(fn, *args, **kwargs)