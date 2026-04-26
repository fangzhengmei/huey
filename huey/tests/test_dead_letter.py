import datetime

from huey.api import Task
from huey.exceptions import TaskException
from huey.signals import SIGNAL_DEAD_LETTER
from huey.signals import SIGNAL_ERROR
from huey.signals import SIGNAL_EXECUTING
from huey.signals import SIGNAL_RETRYING
from huey.tests.base import BaseTestCase


class TestError(Exception):
    def __init__(self, m=None):
        self._m = m
    def __repr__(self):
        return 'TestError(%s)' % self._m


class TestDeadLetterQueue(BaseTestCase):
    def setUp(self):
        super(TestDeadLetterQueue, self).setUp()
        self._signals = []

        @self.huey.signal()
        def signal_handle(signal, task, *args):
            self._signals.append((signal, task, args))

    def assertSignals(self, expected):
        self.assertEqual([s[0] for s in self._signals], expected)
        self._signals = []

    def test_task_without_retries_goes_to_dead_letter(self):
        @self.huey.task()
        def bad_task():
            raise TestError('fail')

        self.assertEqual(self.huey.dead_letter_count(), 0)

        r = bad_task()
        self.assertSignals(['enqueued'])

        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR, SIGNAL_DEAD_LETTER])

        self.assertEqual(self.huey.dead_letter_count(), 1)

        dead_letters = self.huey.dead_letter_tasks()
        self.assertEqual(len(dead_letters), 1)

        dl = dead_letters[0]
        self.assertEqual(dl['task_id'], r.id)
        self.assertEqual(dl['task_name'], 'bad_task')
        self.assertEqual(dl['original_retries'], 0)
        self.assertIn('TestError', dl['error'])
        self.assertIn('test_dead_letter', dl['traceback'])

    def test_task_with_retries_after_exhaustion_goes_to_dead_letter(self):
        @self.huey.task(retries=1)
        def bad_task():
            raise TestError('fail')

        self.assertEqual(self.huey.dead_letter_count(), 0)

        r = bad_task()
        self.assertSignals(['enqueued'])

        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR, SIGNAL_RETRYING,
                            'enqueued'])

        self.assertEqual(self.huey.dead_letter_count(), 0)

        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR, SIGNAL_DEAD_LETTER])

        self.assertEqual(self.huey.dead_letter_count(), 1)

    def test_peek_dead_letter(self):
        @self.huey.task()
        def bad_task():
            raise TestError('fail')

        r = bad_task()
        self.execute_next()

        dl = self.huey.peek_dead_letter(r.id)
        self.assertIsNotNone(dl)
        self.assertEqual(dl['task_id'], r.id)

        dl2 = self.huey.peek_dead_letter(r.id)
        self.assertIsNotNone(dl2)
        self.assertEqual(dl['task_id'], dl2['task_id'])

        self.assertEqual(self.huey.dead_letter_count(), 1)

    def test_get_dead_letter(self):
        @self.huey.task()
        def bad_task():
            raise TestError('fail')

        r = bad_task()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)

        dl = self.huey.get_dead_letter(r.id)
        self.assertIsNotNone(dl)
        self.assertEqual(dl['task_id'], r.id)

        self.assertEqual(self.huey.dead_letter_count(), 0)

        dl2 = self.huey.get_dead_letter(r.id)
        self.assertIsNone(dl2)

    def test_delete_dead_letter(self):
        @self.huey.task()
        def bad_task():
            raise TestError('fail')

        r = bad_task()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)

        result = self.huey.delete_dead_letter(r.id)
        self.assertTrue(result)

        self.assertEqual(self.huey.dead_letter_count(), 0)

        result2 = self.huey.delete_dead_letter(r.id)
        self.assertFalse(result2)

    def test_flush_dead_letter(self):
        @self.huey.task()
        def bad_task(n):
            raise TestError('fail-%s' % n)

        r1 = bad_task(1)
        r2 = bad_task(2)
        r3 = bad_task(3)

        self.execute_next()
        self.execute_next()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 3)

        flushed = self.huey.flush_dead_letter()
        self.assertEqual(flushed, 3)

        self.assertEqual(self.huey.dead_letter_count(), 0)

    def test_requeue_dead_letter(self):
        call_count = [0]

        @self.huey.task()
        def failing_task():
            call_count[0] += 1
            if call_count[0] <= 1:
                raise TestError('first attempt fails')
            return 'success'

        r = failing_task()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)
        self.assertEqual(call_count[0], 1)

        new_result = self.huey.requeue_dead_letter(r.id)
        self.assertIsNotNone(new_result)

        self.assertEqual(self.huey.dead_letter_count(), 0)

        result_value = self.execute_next()
        self.assertEqual(result_value, 'success')
        self.assertEqual(call_count[0], 2)

    def test_requeue_dead_letter_with_overrides(self):
        @self.huey.task(retries=0, retry_delay=0)
        def bad_task():
            raise TestError('fail')

        r = bad_task()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)

        dl = self.huey.peek_dead_letter(r.id)
        original_task = self.huey.deserialize_task(dl['task_data'])
        self.assertEqual(original_task.retries, 0)
        self.assertEqual(original_task.retry_delay, 0)

        new_result = self.huey.requeue_dead_letter(
            r.id,
            retries=3,
            retry_delay=60,
            priority=10)

        self.assertIsNotNone(new_result)

        task = self.huey.dequeue()
        self.assertEqual(task.retries, 3)
        self.assertEqual(task.retry_delay, 60)
        self.assertEqual(task.priority, 10)

    def test_requeue_dead_letter_with_eta(self):
        @self.huey.task()
        def bad_task():
            raise TestError('fail')

        r = bad_task()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)
        self.assertEqual(self.huey.scheduled_count(), 0)

        future_time = datetime.datetime.now() + datetime.timedelta(hours=1)
        new_result = self.huey.requeue_dead_letter(r.id, eta=future_time)

        self.assertIsNotNone(new_result)
        self.assertEqual(self.huey.dead_letter_count(), 0)
        self.assertEqual(self.huey.scheduled_count(), 1)

    def test_dead_letter_signal_arguments(self):
        captured = []

        @self.huey.signal(SIGNAL_DEAD_LETTER)
        def on_dead_letter(signal, task, exception, dead_letter_data):
            captured.append({
                'signal': signal,
                'task': task,
                'exception': exception,
                'data': dead_letter_data,
            })

        @self.huey.task()
        def bad_task():
            raise TestError('fail')

        r = bad_task()
        self.execute_next()

        self.assertEqual(len(captured), 1)
        entry = captured[0]

        self.assertEqual(entry['signal'], SIGNAL_DEAD_LETTER)
        self.assertIsInstance(entry['exception'], TestError)
        self.assertEqual(entry['data']['task_id'], r.id)
        self.assertEqual(entry['data']['task_name'], 'bad_task')

    def test_multiple_failures_to_dead_letter(self):
        @self.huey.task()
        def task_a():
            raise TestError('a')

        @self.huey.task(retries=1)
        def task_b():
            raise TestError('b')

        r1 = task_a()
        r2 = task_b()

        self.execute_next()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)

        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 2)

        tasks = self.huey.dead_letter_tasks()
        task_names = set(t['task_name'] for t in tasks)
        self.assertEqual(task_names, {'task_a', 'task_b'})

    def test_dead_letter_with_limit(self):
        @self.huey.task()
        def bad_task(n):
            raise TestError('fail-%s' % n)

        for i in range(5):
            bad_task(i)

        for _ in range(5):
            self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 5)

        tasks_2 = self.huey.dead_letter_tasks(limit=2)
        self.assertEqual(len(tasks_2), 2)

        all_tasks = self.huey.dead_letter_tasks()
        self.assertEqual(len(all_tasks), 5)

    def test_successful_task_does_not_go_to_dead_letter(self):
        @self.huey.task()
        def good_task(n):
            return n + 1

        self.assertEqual(self.huey.dead_letter_count(), 0)

        r = good_task(3)
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 0)
        self.assertEqual(r.get(), 4)

    def test_requeued_dead_letter_fails_again_goes_back(self):
        call_count = [0]

        @self.huey.task()
        def always_fails():
            call_count[0] += 1
            raise TestError('always fails')

        r = always_fails()
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)
        self.assertEqual(call_count[0], 1)

        new_r = self.huey.requeue_dead_letter(r.id)
        self.execute_next()

        self.assertEqual(self.huey.dead_letter_count(), 1)
        self.assertEqual(call_count[0], 2)

        dl = self.huey.dead_letter_tasks()[0]
        self.assertEqual(dl['task_id'], new_r.id)
