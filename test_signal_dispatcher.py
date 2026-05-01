#!/usr/bin/env python
import sys
sys.path.insert(0, 'g:/fangzheng/solo-dogfeeding/code/16925-huey')

from huey.signals import (
    SignalDispatcher,
    ISignalDispatcher,
    SIGNAL_ENQUEUED,
    SIGNAL_COMPLETE,
    SIGNAL_PRE_EXECUTE,
    SIGNAL_POST_EXECUTE,
    SIGNAL_STARTUP,
    SIGNAL_SHUTDOWN,
    ALL_SIGNALS,
    Signal
)

from huey.api import MemoryHuey
from huey.exceptions import CancelExecution

print('=== Testing SignalDispatcher ===')

print('ISignalDispatcher interface:', ISignalDispatcher)
print('ALL_SIGNALS count:', len(ALL_SIGNALS))
assert len(ALL_SIGNALS) == 17, 'Should have 17 signal types'

print('\\n1. Testing basic connection/emit...')
sd = SignalDispatcher()
received = []

def handler(signal, task, *args):
    received.append((signal, task, args))

sd.connect(handler, SIGNAL_ENQUEUED)
sd.emit(SIGNAL_ENQUEUED, 'test_task')
assert received == [(SIGNAL_ENQUEUED, 'test_task', ())], 'Basic emit failed'
print('   Basic connection/emit: OK')

print('\\n2. Testing hook registration...')
hooks = []
def startup_hook():
    hooks.append('startup')

def shutdown_hook():
    hooks.append('shutdown')

sd.register_hook(SIGNAL_STARTUP, 'my_startup', startup_hook)
sd.register_hook(SIGNAL_SHUTDOWN, 'my_shutdown', shutdown_hook)
sd.execute_startup_hooks()
assert hooks == ['startup'], 'Startup hook failed'
sd.execute_shutdown_hooks()
assert hooks == ['startup', 'shutdown'], 'Shutdown hook failed'
print('   Hook system: OK')

print('\\n3. Testing pre/post execute hooks...')
pre_hooks = []
post_hooks = []

def pre_hook(task):
    pre_hooks.append(task)

def post_hook(task, value, exc):
    post_hooks.append((task, value, exc))

sd.register_hook(SIGNAL_PRE_EXECUTE, 'test_pre', pre_hook)
sd.register_hook(SIGNAL_POST_EXECUTE, 'test_post', post_hook)

sd.execute_pre_execute_hooks('task1')
assert pre_hooks == ['task1'], 'Pre-execute hook failed'

sd.execute_post_execute_hooks('task1', 42, None)
assert post_hooks == [('task1', 42, None)], 'Post-execute hook failed'
print('   Pre/Post execute hooks: OK')

print('\\n4. Testing Huey integration...')
huey = MemoryHuey('test')

@huey.task()
def add(a, b):
    return a + b

signal_state = []

@huey.signal()
def signal_handler(signal, task, *args):
    signal_state.append(signal)

@huey.pre_execute()
def my_pre_hook(task):
    print('      Pre-execute:', task)

@huey.post_execute()
def my_post_hook(task, value, exc):
    print('      Post-execute:', task, value, exc)

print('   Enqueueing task...')
r = add(1, 2)
assert SIGNAL_ENQUEUED in signal_state, 'Enqueue signal missing'
signal_state = []

print('   Executing task...')
result = huey.execute(huey.dequeue())
assert result == 3, 'Task result should be 3'
assert SIGNAL_EXECUTING in signal_state, 'Executing signal missing'
assert SIGNAL_COMPLETE in signal_state, 'Complete signal missing'
print('   Huey integration: OK')

print('\\n5. Testing hook unregistration...')
huey2 = MemoryHuey('test2')
startup_called = []

@huey2.on_startup()
def my_startup():
    startup_called.append(True)

huey2._signal_dispatcher.execute_startup_hooks()
assert startup_called == [True], 'Startup hook should be called'

huey2.unregister_on_startup(my_startup)
huey2._signal_dispatcher.execute_startup_hooks()
assert startup_called == [True], 'Startup hook should not be called after unregister'
print('   Hook unregistration: OK')

print('\\n6. Testing CancelExecution in pre-execute...')
huey3 = MemoryHuey('test3')

@huey3.task()
def will_fail():
    raise ValueError('should not run')

@huey3.pre_execute()
def cancel_hook(task):
    raise CancelExecution()

signal_state2 = []
@huey3.signal()
def handler2(signal, task, *args):
    signal_state2.append(signal)

r = will_fail()
task = huey3.dequeue()
result = huey3.execute(task)
assert result is None, 'Task should be cancelled'
assert SIGNAL_CANCELED in signal_state2, 'Canceled signal missing'
print('   CancelExecution handling: OK')

print('\\n=== All tests passed! ===')
