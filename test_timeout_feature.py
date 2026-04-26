import time
import threading
from huey.utils import thread_timeout
from huey.exceptions import TaskTimeout


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


if __name__ == '__main__':
    print("Testing thread_timeout feature...")
    print("\n1. Testing CPU-bound task timeout...")
    test_thread_timeout_cpu_bound()
    
    print("\n2. Testing task that completes before timeout...")
    test_thread_timeout_completes_before_timeout()
    
    print("\n3. Testing that TaskTimeout is raised...")
    test_thread_timeout_raises_tasktimeout()
    
    print("\n=== All tests passed! ===")
