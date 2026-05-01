import datetime
import time

from huey.api import chord
from huey.signals import *
from huey.tests.base import BaseTestCase


class TestSignals(BaseTestCase):
    def setUp(self):
        super(TestSignals, self).setUp()
        self._state = []

        @self.huey.signal()
        def signal_handle(signal, task, *args):
            self._state.append((signal, task, args))

    def assertSignals(self, expected):
        self.assertEqual([s[0] for s in self._state], expected)
        self._state = []

    def test_signals_simple(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(3)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertEqual(self.execute_next(), 4)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

        r = task_a.schedule((2,), delay=60)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_SCHEDULED])

        r = task_a(None)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR])

    def test_signal_complete_result_ready(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        results = []

        @self.huey.signal(SIGNAL_COMPLETE)
        def on_complete(sig, task, *_):
            results.append(self.huey.result(task.id))

        r = task_a(2)
        self.assertEqual(self.execute_next(), 3)
        self.assertEqual(results, [3])

    def test_signals_on_retry(self):
        @self.huey.task(retries=1)
        def task_a(n):
            return n + 1

        r = task_a(None)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR, SIGNAL_RETRYING,
                            SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR])

        @self.huey.task(retries=1, retry_delay=60)
        def task_b(n):
            return n + 1

        r = task_b(None)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR, SIGNAL_RETRYING,
                            SIGNAL_SCHEDULED])

    def test_signals_revoked(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        task_a.revoke(revoke_once=True)
        r = task_a(2)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_REVOKED])

        r = task_a(3)
        self.assertEqual(self.execute_next(), 4)
        self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING,
                            SIGNAL_COMPLETE])

    def test_signals_locked(self):
        @self.huey.task()
        @self.huey.lock_task('lock-a')
        def task_a(n):
            return n + 1

        r = task_a(1)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertEqual(self.execute_next(), 2)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

        with self.huey.lock_task('lock-a'):
            r = task_a(2)
            self.assertSignals([SIGNAL_ENQUEUED])
            self.assertTrue(self.execute_next() is None)
            self.assertSignals([SIGNAL_EXECUTING, SIGNAL_LOCKED])

    def test_signals_ratelimit(self):
        @self.huey.task()
        @self.huey.rate_limit('rl', limit=1, per=60)
        def task_a():
            return 1

        r = task_a()
        self.assertEqual(self.execute_next(), 1)
        self.assertSignals([
            SIGNAL_ENQUEUED,
            SIGNAL_EXECUTING,
            SIGNAL_COMPLETE])

        r = task_a()
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([
            SIGNAL_ENQUEUED,
            SIGNAL_EXECUTING,
            SIGNAL_RATE_LIMITED,
            SIGNAL_RETRYING,
            SIGNAL_SCHEDULED])

        @self.huey.task()
        @self.huey.rate_limit('rl', limit=1, per=60, retry=False)
        def task_b():
            return 1

        r = task_b()
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([
            SIGNAL_ENQUEUED,
            SIGNAL_EXECUTING,
            SIGNAL_RATE_LIMITED])

        r = task_b(retries=2)
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([
            SIGNAL_ENQUEUED,
            SIGNAL_EXECUTING,
            SIGNAL_RATE_LIMITED,
            SIGNAL_RETRYING,
            SIGNAL_SCHEDULED])

    def test_signal_expired(self):
        @self.huey.task(expires=10)
        def task_a(n):
            return n + 1

        now = datetime.datetime.now()
        expires = now + datetime.timedelta(seconds=15)
        r = task_a(2)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next(expires) is None)
        self.assertSignals([SIGNAL_EXPIRED])

        r = task_a(3)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next(), 4)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    def test_signal_timeout(self):
        @self.huey.task(timeout=0.001, context=True)
        def timeout(task=None):
            time.sleep(0.01)
            task.check_timeout()

        r = timeout()
        self.execute_next()
        self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING, SIGNAL_TIMEOUT])

    def test_signals_chord(self):
        @self.huey.task()
        def prod(n):
            return n + 1
        @self.huey.task()
        def agg(results):
            return sum(results)

        self.huey.enqueue(chord([prod.s(1), prod.s(2)], agg))
        self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_ENQUEUED])
        self.assertEqual([self.execute_next() for _ in range(3)], [2, 3, 5])
        self.assertSignals([
            SIGNAL_EXECUTING, SIGNAL_COMPLETE,
            SIGNAL_EXECUTING, SIGNAL_COMPLETE,
            SIGNAL_ENQUEUED, SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    def test_signals_chord_error(self):
        @self.huey.task()
        def bad():
            raise TestError('fail')

        @self.huey.task()
        def combine(results):
            return results

        self.huey.enqueue(chord([bad.s()], combine.s()))

        self.execute_next()  # bad() fails, exception to chord
        self.execute_next()  # callback

        self.assertSignals([
            SIGNAL_ENQUEUED,
            SIGNAL_EXECUTING, SIGNAL_ERROR,
            SIGNAL_ENQUEUED,
            SIGNAL_EXECUTING, SIGNAL_COMPLETE,
        ])

    def test_specific_handler(self):
        extra_state = []

        @self.huey.signal(SIGNAL_EXECUTING)
        def extra_handler(signal, task):
            extra_state.append(task.args[0])

        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(3)
        self.assertEqual(extra_state, [])
        self.assertEqual(self.execute_next(), 4)
        self.assertEqual(extra_state, [3])
        self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING,
                            SIGNAL_COMPLETE])

        r2 = task_a(1)
        self.assertEqual(self.execute_next(), 2)
        self.assertEqual(extra_state, [3, 1])
        self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING,
                            SIGNAL_COMPLETE])

        self.huey.disconnect_signal(extra_handler, SIGNAL_EXECUTING)
        r3 = task_a(2)
        self.assertEqual(self.execute_next(), 3)
        self.assertEqual(extra_state, [3, 1])
        self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING,
                            SIGNAL_COMPLETE])

    def test_multi_handlers(self):
        state1 = []
        state2 = []

        @self.huey.signal(SIGNAL_EXECUTING, SIGNAL_COMPLETE)
        def handler1(signal, task):
            state1.append(signal)

        @self.huey.signal(SIGNAL_EXECUTING, SIGNAL_COMPLETE)
        def handler2(signal, task):
            state2.append(signal)

        @self.huey.task()
        def task_a(n):
            return n + 1

        r = task_a(1)
        self.assertEqual(self.execute_next(), 2)
        self.assertEqual(state1, ['executing', 'complete'])
        self.assertEqual(state2, ['executing', 'complete'])

        self.huey.disconnect_signal(handler1, SIGNAL_COMPLETE)
        self.huey.disconnect_signal(handler2)

        r2 = task_a(2)
        self.assertEqual(self.execute_next(), 3)
        self.assertEqual(state1, ['executing', 'complete', 'executing'])
        self.assertEqual(state2, ['executing', 'complete'])

    def test_pre_execute_hook_non_cancel_exception_does_not_interrupt_task(self):
        from huey.exceptions import CancelExecution

        task_executed = []
        pre_hook_called = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.pre_execute()
        def flaky_hook(task):
            pre_hook_called.append('flaky')
            raise ValueError('some error')

        @self.huey.pre_execute()
        def normal_hook(task):
            pre_hook_called.append('normal')

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertEqual(result, 6)
        self.assertEqual(task_executed, [5])

        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])
        self.assertEqual(pre_hook_called, ['flaky', 'normal'])

    def test_pre_execute_hook_cancel_execution_interrupts_task(self):
        from huey.exceptions import CancelExecution

        task_executed = []
        pre_hook_called = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.pre_execute()
        def cancel_hook(task):
            pre_hook_called.append('cancel')
            raise CancelExecution()

        @self.huey.pre_execute()
        def after_cancel_hook(task):
            pre_hook_called.append('after')

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertIsNone(result)
        self.assertEqual(task_executed, [])

        # 原代码行为：execute() 先发送 SIGNAL_EXECUTING，然后才调用 _execute()
        # 执行 pre-execute 钩子。所以信号序列应该是 ['executing', 'canceled']
        self.assertIn(SIGNAL_EXECUTING, [s[0] for s in self._state])
        self.assertIn(SIGNAL_CANCELED, [s[0] for s in self._state])
        self._state = []
        self.assertEqual(pre_hook_called, ['cancel'])

    def test_post_execute_hook_exceptions_does_not_affect_result(self):
        task_executed = []
        post_hook_called = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.post_execute()
        def flaky_hook(task, value, exc):
            post_hook_called.append('flaky')
            raise ValueError('post hook error')

        @self.huey.post_execute()
        def normal_hook(task, value, exc):
            post_hook_called.append(('normal', value, exc))

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertEqual(result, 6)
        self.assertEqual(task_executed, [5])

        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])
        self.assertEqual(post_hook_called, [
            'flaky',
            ('normal', 6, None)
        ])

    # Regression coverage for hook ordering/cancel/error edge cases.
    def test_hooks_execution_order(self):
        pre_hooks = []
        post_hooks = []

        @self.huey.task()
        def task_a(n):
            return n + 1

        @self.huey.pre_execute()
        def pre_hook_1(task):
            pre_hooks.append('pre_1')

        @self.huey.pre_execute()
        def pre_hook_2(task):
            pre_hooks.append('pre_2')

        @self.huey.pre_execute()
        def pre_hook_3(task):
            pre_hooks.append('pre_3')

        @self.huey.post_execute()
        def post_hook_1(task, value, exc):
            post_hooks.append('post_1')

        @self.huey.post_execute()
        def post_hook_2(task, value, exc):
            post_hooks.append('post_2')

        @self.huey.post_execute()
        def post_hook_3(task, value, exc):
            post_hooks.append('post_3')

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertEqual(result, 6)

        self.assertEqual(pre_hooks, ['pre_1', 'pre_2', 'pre_3'])
        self.assertEqual(post_hooks, ['post_1', 'post_2', 'post_3'])

    def test_pre_execute_hook_middle_cancel_stops_remaining(self):
        from huey.exceptions import CancelExecution
        pre_hooks = []
        task_executed = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.pre_execute()
        def pre_hook_1(task):
            pre_hooks.append('pre_1')

        @self.huey.pre_execute()
        def pre_hook_2_cancel(task):
            pre_hooks.append('pre_2_cancel')
            raise CancelExecution()

        @self.huey.pre_execute()
        def pre_hook_3(task):
            pre_hooks.append('pre_3')

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertIsNone(result)
        self.assertEqual(task_executed, [])

        self.assertEqual(pre_hooks, ['pre_1', 'pre_2_cancel'])

        self.assertIn(SIGNAL_CANCELED, [s[0] for s in self._state])
        self._state = []

    def test_pre_execute_mixed_exceptions(self):
        from huey.exceptions import CancelExecution
        pre_hooks = []
        task_executed = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.pre_execute()
        def flaky_hook_1(task):
            pre_hooks.append('flaky_1')
            raise ValueError('error 1')

        @self.huey.pre_execute()
        def normal_hook(task):
            pre_hooks.append('normal')

        @self.huey.pre_execute()
        def flaky_hook_2(task):
            pre_hooks.append('flaky_2')
            raise KeyError('error 2')

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertEqual(result, 6)
        self.assertEqual(task_executed, [5])

        self.assertEqual(pre_hooks, ['flaky_1', 'normal', 'flaky_2'])
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    def test_cancel_execution_with_retry(self):
        from huey.exceptions import CancelExecution
        task_executed = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.pre_execute()
        def cancel_with_retry(task):
            raise CancelExecution(retry=True)

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertIsNone(result)
        self.assertEqual(task_executed, [])

        self.assertIn(SIGNAL_CANCELED, [s[0] for s in self._state])
        self._state = []

    def test_cancel_execution_without_retry(self):
        from huey.exceptions import CancelExecution
        task_executed = []

        @self.huey.task()
        def task_a(n):
            task_executed.append(n)
            return n + 1

        @self.huey.pre_execute()
        def cancel_without_retry(task):
            raise CancelExecution(retry=False)

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertIsNone(result)
        self.assertEqual(task_executed, [])

        self.assertIn(SIGNAL_CANCELED, [s[0] for s in self._state])
        self._state = []

    def test_post_execute_hook_receives_task_exception(self):
        post_hook_data = []

        @self.huey.task()
        def failing_task():
            raise ValueError('intentional failure')

        @self.huey.post_execute()
        def post_hook(task, value, exc):
            post_hook_data.append({
                'value': value,
                'exc': exc,
                'exc_type': type(exc).__name__ if exc else None
            })

        self._state = []
        r = failing_task()
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertIsNone(result)

        self.assertEqual(len(post_hook_data), 1)
        self.assertIsNone(post_hook_data[0]['value'])
        self.assertIsNotNone(post_hook_data[0]['exc'])
        self.assertEqual(post_hook_data[0]['exc_type'], 'ValueError')

        self.assertIn(SIGNAL_ERROR, [s[0] for s in self._state])
        self._state = []

    def test_post_execute_multiple_exceptions(self):
        post_hooks = []

        @self.huey.task()
        def task_a(n):
            return n + 1

        @self.huey.post_execute()
        def flaky_1(task, value, exc):
            post_hooks.append('flaky_1')
            raise ValueError('first error')

        @self.huey.post_execute()
        def normal(task, value, exc):
            post_hooks.append(('normal', value))

        @self.huey.post_execute()
        def flaky_2(task, value, exc):
            post_hooks.append('flaky_2')
            raise KeyError('second error')

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertEqual(result, 6)

        self.assertEqual(post_hooks, [
            'flaky_1',
            ('normal', 6),
            'flaky_2'
        ])

        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    def test_post_execute_with_task_returning_none(self):
        post_hook_data = []

        @self.huey.task()
        def return_none_task():
            return None

        @self.huey.post_execute()
        def post_hook(task, value, exc):
            post_hook_data.append({'value': value, 'exc': exc})

        self._state = []
        r = return_none_task()
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertIsNone(result)

        self.assertEqual(len(post_hook_data), 1)
        self.assertIsNone(post_hook_data[0]['value'])
        self.assertIsNone(post_hook_data[0]['exc'])

        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    def test_signal_dispatcher_startup_hooks(self):
        startup_called = []

        @self.huey.on_startup()
        def startup_1():
            startup_called.append('startup_1')

        @self.huey.on_startup()
        def startup_2():
            startup_called.append('startup_2')

        self.huey._signal_dispatcher.execute_startup_hooks()

        self.assertEqual(startup_called, ['startup_1', 'startup_2'])

    def test_signal_dispatcher_startup_hook_exception(self):
        startup_called = []

        @self.huey.on_startup()
        def flaky_startup():
            startup_called.append('flaky')
            raise ValueError('startup error')

        @self.huey.on_startup()
        def normal_startup():
            startup_called.append('normal')

        self.huey._signal_dispatcher.execute_startup_hooks()

        self.assertEqual(startup_called, ['flaky', 'normal'])

    def test_signal_dispatcher_shutdown_hooks(self):
        shutdown_called = []

        @self.huey.on_shutdown()
        def shutdown_1():
            shutdown_called.append('shutdown_1')

        @self.huey.on_shutdown()
        def shutdown_2():
            shutdown_called.append('shutdown_2')

        self.huey._signal_dispatcher.execute_shutdown_hooks()

        self.assertEqual(shutdown_called, ['shutdown_1', 'shutdown_2'])

    def test_hook_unregistration(self):
        from huey.signals import SIGNAL_PRE_EXECUTE
        hooks_called = []

        @self.huey.pre_execute(name='hook_a')
        def hook_a(task):
            hooks_called.append('hook_a')

        @self.huey.pre_execute(name='hook_b')
        def hook_b(task):
            hooks_called.append('hook_b')

        @self.huey.task()
        def task_a(n):
            return n + 1

        self._state = []
        r = task_a(1)
        self.assertSignals([SIGNAL_ENQUEUED])

        self.execute_next()
        self.assertEqual(hooks_called, ['hook_a', 'hook_b'])
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

        result = self.huey.unregister_pre_execute('hook_a')
        self.assertTrue(result)

        self._state = []
        hooks_called = []
        r = task_a(2)
        self.assertSignals([SIGNAL_ENQUEUED])

        self.execute_next()
        self.assertEqual(hooks_called, ['hook_b'])
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

        result = self.huey.unregister_pre_execute('non_existent')
        self.assertFalse(result)

    def test_signal_connect_disconnect(self):
        from huey.signals import SIGNAL_COMPLETE, SIGNAL_ERROR
        signals_received = []

        def handler(signal, task, *args):
            signals_received.append(signal)

        self.huey.signal(SIGNAL_COMPLETE)(handler)

        @self.huey.task()
        def task_a(n):
            return n + 1

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        self.execute_next()
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

        self.huey.disconnect_signal(handler, SIGNAL_COMPLETE)

        self._state = []
        r = task_a(6)
        self.assertSignals([SIGNAL_ENQUEUED])

        self.execute_next()
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    def test_full_execution_pipeline_with_signals_and_hooks(self):
        from huey.exceptions import CancelExecution
        execution_log = []

        @self.huey.pre_execute()
        def pre_hook(task):
            execution_log.append('pre_execute')

        @self.huey.post_execute()
        def post_hook(task, value, exc):
            execution_log.append(('post_execute', value))

        @self.huey.signal(SIGNAL_ENQUEUED)
        def on_enqueue(signal, task, *args):
            execution_log.append('signal_enqueued')

        @self.huey.signal(SIGNAL_EXECUTING)
        def on_executing(signal, task, *args):
            execution_log.append('signal_executing')

        @self.huey.signal(SIGNAL_COMPLETE)
        def on_complete(signal, task, *args):
            execution_log.append('signal_complete')

        @self.huey.task()
        def task_a(n):
            execution_log.append('task_executed')
            return n + 1

        self._state = []
        r = task_a(5)
        self.assertSignals([SIGNAL_ENQUEUED])

        result = self.execute_next()
        self.assertEqual(result, 6)

        self.assertEqual(execution_log, [
            'signal_enqueued',
            'signal_executing',
            'pre_execute',
            'task_executed',
            ('post_execute', 6),
            'signal_complete'
        ])

        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])
