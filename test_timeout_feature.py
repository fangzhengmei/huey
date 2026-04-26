import time
import threading
from huey.api import MemoryHuey
from huey.consumer import Consumer
from huey.utils import thread_timeout
from huey.exceptions import TaskTimeout, TaskException
from huey.utils import Error


def test_thread_timeout_cpu_bound():
    """Test that thread_timeout can interrupt CPU-bound loops."""
    state = {'count': 0, 'completed': False}
    
    def cpu_bound_task():
        try:
            with thread_timeout(0.1):
                for i in range(10000000):
                    state['count'] += 1
            state['completed'] = True
        except TaskTimeout:
            state['completed'] = False
            raise
    
    t = threading.Thread(target=cpu_bound_task)
    t.start()
    t.join(timeout=2.0)
    
    assert state['count'] > 0, "Task should have executed some iterations"
    assert not state['completed'], "Task should have been timed out"
    print(f"CPU-bound test passed: count={state['count']}, completed={state['completed']}")


def test_thread_timeout_completes_before_timeout():
    """Test that tasks completing before timeout work normally."""
    state = {'completed': False, 'result': None}
    
    def quick_task():
        with thread_timeout(1.0):
            state['result'] = 42
            state['completed'] = True
    
    t = threading.Thread(target=quick_task)
    t.start()
    t.join(timeout=2.0)
    
    assert state['completed'], "Task should have completed"
    assert state['result'] == 42, "Task should have returned correct result"
    print(f"Quick task test passed: completed={state['completed']}, result={state['result']}")


def test_thread_timeout_raises_tasktimeout():
    """Test that thread_timeout raises TaskTimeout when timeout occurs."""
    caught_exception = [None]
    
    def long_running_task():
        try:
            with thread_timeout(0.05):
                for i in range(10000000):
                    pass
        except TaskTimeout as e:
            caught_exception[0] = e
    
    t = threading.Thread(target=long_running_task)
    t.start()
    t.join(timeout=2.0)
    
    assert caught_exception[0] is not None, "TaskTimeout should have been raised"
    assert isinstance(caught_exception[0], TaskTimeout), "Should be TaskTimeout"
    print(f"Exception test passed: caught {type(caught_exception[0])}")


def test_timeout_task_result_storage():
    """
    Test that when a task times out:
    1. The failure is properly recorded in result storage
    2. The error metadata includes task_id, error message, traceback
    3. The result can be retrieved and raises TaskException
    """
    huey = MemoryHuey(utc=False)
    
    @huey.task(timeout=0.05, context=True)
    def long_running_task(task=None):
        while True:
            for _ in range(10000):
                pass
    
    consumer = Consumer(
        huey,
        workers=1,
        periodic=False,
        initial_delay=0.001,
        max_delay=0.001,
        worker_type='thread',
        check_worker_health=False
    )
    
    result = long_running_task()
    task_id = result.id
    print(f"Task ID: {task_id}")
    
    worker, _ = consumer.worker_threads[0]
    
    worker.loop()
    
    assert huey.result_count() == 1, "Result should be stored"
    
    raw_result = huey.get_raw(task_id, peek=True)
    assert raw_result is not None, "Raw result should exist"
    
    deserialized = huey.serializer.deserialize(raw_result)
    print(f"Result type: {type(deserialized)}")
    print(f"Is Error: {isinstance(deserialized, Error)}")
    
    assert isinstance(deserialized, Error), "Result should be an Error"
    
    error_metadata = deserialized.metadata
    print(f"Error metadata: {error_metadata}")
    
    assert 'error' in error_metadata, "Metadata should contain 'error'"
    assert 'task_id' in error_metadata, "Metadata should contain 'task_id'"
    assert 'traceback' in error_metadata, "Metadata should contain 'traceback'"
    assert 'retries' in error_metadata, "Metadata should contain 'retries'"
    
    assert 'TaskTimeout' in error_metadata['error'], \
        f"Error message should contain 'TaskTimeout', got: {error_metadata['error']}"
    assert error_metadata['task_id'] == task_id, \
        f"Task ID should match: expected {task_id}, got {error_metadata['task_id']}"
    
    try:
        result.get()
        assert False, "Should have raised TaskException"
    except TaskException as e:
        print(f"Caught expected TaskException: {e.metadata}")
        assert 'TaskTimeout' in e.metadata['error'], \
            f"TaskException error should contain 'TaskTimeout', got: {e.metadata['error']}"
        assert e.metadata['task_id'] == task_id, \
            f"Task ID should match in exception"
    
    print(f"Timeout result storage test passed!")
    print(f"  - Task ID: {error_metadata['task_id']}")
    print(f"  - Error: {error_metadata['error']}")
    print(f"  - Retries: {error_metadata['retries']}")
    print(f"  - Has traceback: {bool(error_metadata['traceback'])}")


def test_successful_task_result_storage():
    """Test that successful tasks store results correctly (control test)."""
    huey = MemoryHuey(utc=False)
    
    @huey.task(timeout=10)
    def quick_task():
        return 42
    
    consumer = Consumer(
        huey,
        workers=1,
        periodic=False,
        initial_delay=0.001,
        max_delay=0.001,
        worker_type='thread',
        check_worker_health=False
    )
    
    result = quick_task()
    task_id = result.id
    
    worker, _ = consumer.worker_threads[0]
    worker.loop()
    
    assert huey.result_count() == 1, "Result should be stored"
    assert result.get() == 42, "Result should be 42"
    print(f"Successful task test passed: task_id={task_id}, result=42")


if __name__ == '__main__':
    print("Testing thread_timeout feature...")
    print("\n" + "="*60)
    print("Part 1: Basic thread_timeout tests")
    print("="*60)
    
    print("\n1. Testing CPU-bound task timeout...")
    test_thread_timeout_cpu_bound()
    
    print("\n2. Testing task that completes before timeout...")
    test_thread_timeout_completes_before_timeout()
    
    print("\n3. Testing that TaskTimeout is raised...")
    test_thread_timeout_raises_tasktimeout()
    
    print("\n" + "="*60)
    print("Part 2: Huey integration tests")
    print("="*60)
    
    print("\n4. Testing successful task result storage...")
    test_successful_task_result_storage()
    
    print("\n5. Testing timeout task result storage...")
    test_timeout_task_result_storage()
    
    print("\n" + "="*60)
    print("=== All tests passed! ===")
    print("="*60)
