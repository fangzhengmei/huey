import logging
import os
import sys
import time

logging.basicConfig(level=logging.WARNING,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

os.environ['HUEY_SLOW_TESTS'] = '1'

from huey.api import MemoryHuey
from huey.consumer import Consumer
from huey.consumer import Scheduler
from huey.exceptions import TaskException, TaskTimeout
from huey.utils import thread_timeout


class TestConsumer(Consumer):
    class _Scheduler(Scheduler):
        def sleep_for_interval(self, current, interval):
            pass
    scheduler_class = _Scheduler


class TestHueyTimeoutLogging:
    def __init__(self):
        self.huey = MemoryHuey(utc=False)
        self.consumer_class = TestConsumer

    def consumer(self, **params):
        params.setdefault('initial_delay', 0.001)
        params.setdefault('max_delay', 0.001)
        params.setdefault('workers', 2)
        params.setdefault('check_worker_health', False)
        return self.consumer_class(self.huey, **params)

    def work_on_tasks(self, consumer, n=1, now=None):
        worker, _ = consumer.worker_threads[0]
        for i in range(n):
            assert len(self.huey) == n - i
            worker.loop(now)

    def test_timeout_logging(self):
        print("\n" + "="*60)
        print("Testing timeout logging")
        print("="*60)
        
        @self.huey.task(timeout=0.05)
        def cpu_bound_task():
            while True:
                for _ in range(10000):
                    pass

        @self.huey.task(timeout=10)
        def quick_task():
            return 42

        r1 = quick_task()
        r2 = cpu_bound_task()
        
        print(f"\nTask 1 ID: {r1.id}")
        print(f"Task 2 ID: {r2.id}")
        
        consumer = self.consumer(workers=1)
        
        print("\nExecuting tasks (watch for timeout logs)...")
        print("-"*60)
        start_time = time.time()
        self.work_on_tasks(consumer, 2)
        elapsed = time.time() - start_time
        print("-"*60)
        print(f"Tasks executed in {elapsed:.3f}s")

        print("\nChecking results...")
        result1 = r1.get()
        print(f"  - Quick task result: {result1}")
        assert result1 == 42, f"Expected 42, got {result1}"

        result_count = self.huey.result_count()
        print(f"  - Result count: {result_count}")

        print("\nChecking timeout task raises TaskException...")
        try:
            r2.get()
            assert False, "Should have raised TaskException"
        except TaskException as exc:
            print(f"  - Caught expected TaskException")
            print(f"  - Error: {exc.metadata['error']}")
            print(f"  - Task ID: {exc.metadata['task_id']}")
            print(f"  - Retries: {exc.metadata['retries']}")
            assert 'TaskTimeout' in exc.metadata['error'], \
                f"Expected 'TaskTimeout' in error, got: {exc.metadata['error']}"
            assert exc.metadata['task_id'] == r2.id, \
                f"Expected task_id {r2.id}, got {exc.metadata['task_id']}"

        print("\n" + "="*60)
        print("SUCCESS: All tests passed!")
        print("="*60)
        print("\nYou should see warning logs like:")
        print("  - 'Thread timeout triggered: raising TaskTimeout after 0.05s'")
        print("  - 'Task xxx timed out after 0.05s.'")
        return True


if __name__ == '__main__':
    test = TestHueyTimeoutLogging()
    
    try:
        test.test_timeout_logging()
        sys.exit(0)
    except Exception as e:
        print(f"\nFAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
