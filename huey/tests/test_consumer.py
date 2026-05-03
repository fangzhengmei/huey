import datetime
import time

from huey.api import crontab
from huey.consumer import Consumer
from huey.consumer import Scheduler
from huey.consumer_options import ConsumerConfig
from huey.constants import WORKER_GREENLET
from huey.exceptions import ConfigurationError
from huey.exceptions import TaskException
from huey.exceptions import TaskTimeout
from huey.tests.base import BaseTestCase
from huey.tests.base import slow_test


class TestConsumer(Consumer):
    class _Scheduler(Scheduler):
        def sleep_for_interval(self, current, interval):
            pass
    scheduler_class = _Scheduler


class TestConsumerIntegration(BaseTestCase):
    consumer_class = TestConsumer

    def test_consumer_minimal(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        with self.consumer_context():
            result = task_a(1)
            self.assertEqual(result.get(blocking=True, timeout=2), 2)

    def work_on_tasks(self, consumer, n=1, now=None):
        worker, _ = consumer.worker_threads[0]
        for i in range(n):
            self.assertEqual(len(self.huey), n - i)
            worker.loop(now)

    def schedule_tasks(self, consumer, now=None):
        scheduler = consumer._create_scheduler()
        scheduler._next_loop = time.monotonic() + 60
        scheduler._next_periodic = time.monotonic() - 60
        scheduler.loop(now)

    @slow_test()
    def test_consumer_timeout(self):
        @self.huey.task(timeout=0.1, context=True)
        def t(n, task=None):
            if n:
                for _ in range(100):
                    task.check_timeout()
                    time.sleep(n / 100)
            return n

        r1 = t(0)
        r2 = t(0.2)
        consumer = self.consumer(workers=1)
        self.work_on_tasks(consumer, 2)
        self.assertEqual(r1.get(), 0)
        with self.assertRaises(TaskException):
            r2.get()
        try:
            r2.get()
        except TaskException as exc:
            self.assertEqual(exc.metadata['error'],
                             'TaskTimeout(\'timeout 0.1s\')')

    def test_consumer_schedule_task(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        now = datetime.datetime.now()
        eta = now + datetime.timedelta(days=1)
        r60 = task_a.schedule((2,), delay=60)
        rday = task_a.schedule((3,), eta=eta)

        consumer = self.consumer(workers=1)
        self.work_on_tasks(consumer, 2)  # Process the two messages.

        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.scheduled_count(), 2)

        self.schedule_tasks(consumer, now)
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.scheduled_count(), 2)

        # Ensure that the task that had a delay of 60s is read from schedule.
        later = now + datetime.timedelta(seconds=65)
        self.schedule_tasks(consumer, later)
        self.assertEqual(len(self.huey), 1)
        self.assertEqual(self.huey.scheduled_count(), 1)

        # We can now work on our scheduled task.
        self.work_on_tasks(consumer, 1, later)
        self.assertEqual(r60.get(), 3)

        # Verify the task was run and that there is only one task remaining to
        # be scheduled (in a day).
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.scheduled_count(), 1)

        tomorrow = now + datetime.timedelta(days=1)
        self.schedule_tasks(consumer, tomorrow)
        self.work_on_tasks(consumer, 1, tomorrow)
        self.assertEqual(rday.get(), 4)
        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.huey.scheduled_count(), 0)

    def test_consumer_periodic_tasks(self):
        state = []

        @self.huey.periodic_task(crontab(minute='*/10'))
        def task_p1():
            state.append('p1')

        @self.huey.periodic_task(crontab(minute='0', hour='0'))
        def task_p2():
            state.append('p2')

        consumer = self.consumer(workers=1)
        dt = datetime.datetime(2000, 1, 1, 0, 0)
        self.schedule_tasks(consumer, dt)
        self.assertEqual(len(self.huey), 2)
        self.work_on_tasks(consumer, 2)
        self.assertEqual(state, ['p1', 'p2'])

        dt = datetime.datetime(2000, 1, 1, 12, 0)
        self.schedule_tasks(consumer, dt)
        self.assertEqual(len(self.huey), 1)
        self.work_on_tasks(consumer, 1)
        self.assertEqual(state, ['p1', 'p2', 'p1'])

        task_p1.revoke()
        self.schedule_tasks(consumer, dt)
        self.assertEqual(len(self.huey), 1)  # Enqueued despite being revoked.
        self.work_on_tasks(consumer, 1)
        self.assertEqual(state, ['p1', 'p2', 'p1'])  # No change, not executed.


class TestConsumerConfig(BaseTestCase):
    def test_default_config(self):
        cfg = ConsumerConfig()
        cfg.validate()
        consumer = self.huey.create_consumer(**cfg.values)
        self.assertEqual(consumer.workers, 1)
        self.assertEqual(consumer.worker_type, 'thread')
        self.assertTrue(consumer.periodic)
        self.assertEqual(consumer.default_delay, 0.1)
        self.assertEqual(consumer.scheduler_interval, 1)
        self.assertTrue(consumer._health_check)

    def test_consumer_config(self):
        cfg = ConsumerConfig(workers=3, worker_type='process', initial_delay=1,
                             backoff=2, max_delay=4, check_worker_health=False,
                             scheduler_interval=30, periodic=False)
        cfg.validate()
        consumer = self.huey.create_consumer(**cfg.values)

        self.assertEqual(consumer.workers, 3)
        self.assertEqual(consumer.worker_type, 'process')
        self.assertFalse(consumer.periodic)
        self.assertEqual(consumer.default_delay, 1)
        self.assertEqual(consumer.backoff, 2)
        self.assertEqual(consumer.max_delay, 4)
        self.assertEqual(consumer.scheduler_interval, 30)
        self.assertFalse(consumer._health_check)

    def test_invalid_values(self):
        def assertInvalid(**kwargs):
            cfg = ConsumerConfig(**kwargs)
            self.assertRaises(ValueError, cfg.validate)

        assertInvalid(backoff=0.5)
        assertInvalid(scheduler_interval=90)
        assertInvalid(scheduler_interval=7)
        assertInvalid(scheduler_interval=45)

    def test_create_consumer_invalid_scheduler_interval(self):
        def assertInvalid(**kwargs):
            self.assertRaises(ValueError, self.huey.create_consumer, **kwargs)

        assertInvalid(scheduler_interval=90)
        assertInvalid(scheduler_interval=7)
        assertInvalid(scheduler_interval=45)
        assertInvalid(scheduler_interval=0)

        valid_intervals = [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60]
        for interval in valid_intervals:
            consumer = self.huey.create_consumer(scheduler_interval=interval)
            self.assertEqual(consumer.scheduler_interval, interval)

    def test_create_consumer_max_tasks_requires_health_check(self):
        self.assertRaises(
            ConfigurationError,
            self.huey.create_consumer,
            max_tasks=10,
            check_worker_health=False)

        consumer = self.huey.create_consumer(max_tasks=10)
        self.assertEqual(consumer.max_tasks, 10)
        self.assertTrue(consumer._health_check)

        consumer2 = self.huey.create_consumer(
            max_tasks=100,
            check_worker_health=True)
        self.assertEqual(consumer2.max_tasks, 100)
        self.assertTrue(consumer2._health_check)

    def test_consumer_config_max_tasks_requires_health_check(self):
        cfg = ConsumerConfig(max_tasks=10, check_worker_health=False)
        self.assertRaises(ConfigurationError, cfg.validate)

        cfg2 = ConsumerConfig(max_tasks=10, check_worker_health=True)
        cfg2.validate()
        self.assertEqual(cfg2.max_tasks, 10)

    def test_worker_type_normalization(self):
        cfg = ConsumerConfig(worker_type='gevent')
        self.assertEqual(cfg.worker_type, 'gevent')
        self.assertEqual(cfg.normalized_worker_type, WORKER_GREENLET)

        cfg2 = ConsumerConfig(worker_type=WORKER_GREENLET)
        self.assertEqual(cfg2.worker_type, WORKER_GREENLET)
        self.assertEqual(cfg2.normalized_worker_type, WORKER_GREENLET)

        cfg3 = ConsumerConfig(worker_type='thread')
        self.assertEqual(cfg3.worker_type, 'thread')
        self.assertEqual(cfg3.normalized_worker_type, 'thread')

    def test_values_returns_normalized_worker_type(self):
        cfg = ConsumerConfig(worker_type='gevent')
        values = cfg.values
        self.assertEqual(values['worker_type'], WORKER_GREENLET)
        self.assertNotEqual(values['worker_type'], 'gevent')
