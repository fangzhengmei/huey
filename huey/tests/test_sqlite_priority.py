import datetime
import os
import tempfile
import unittest

from huey.api import SqliteHuey
from huey.api import crontab
from huey.exceptions import TaskException
from huey.storage import SqliteStorage


class TestSqliteStoragePriority(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.mktemp(suffix='.db')
        self.storage = SqliteStorage(filename=self.db_file)

    def tearDown(self):
        self.storage.flush_all()
        self.storage.close()
        if os.path.exists(self.db_file):
            try:
                os.unlink(self.db_file)
            except PermissionError:
                pass

    def test_priority_attribute(self):
        self.assertTrue(self.storage.priority)

    def test_priority_simple_ordering(self):
        priorities = (1, None, 5, None, 3, None, 9, None, 7, 0)
        for i, priority in enumerate(priorities):
            item = 'i%s-%s' % (i, priority)
            self.storage.enqueue(item.encode('utf8'), priority)

        self.assertEqual(self.storage.queue_size(), 10)

        expected = [b'i6-9', b'i8-7', b'i2-5', b'i4-3', b'i0-1',
                    b'i1-None', b'i3-None', b'i5-None', b'i7-None', b'i9-0']
        results = [self.storage.dequeue() for _ in range(10)]
        self.assertEqual(results, expected)
        self.assertEqual(self.storage.queue_size(), 0)

    def test_priority_high_first(self):
        self.storage.enqueue(b'low-1', priority=1)
        self.storage.enqueue(b'high-1', priority=100)
        self.storage.enqueue(b'low-2', priority=1)
        self.storage.enqueue(b'high-2', priority=100)
        self.storage.enqueue(b'medium', priority=50)

        results = [self.storage.dequeue() for _ in range(5)]
        self.assertEqual(results[0], b'high-1')
        self.assertEqual(results[1], b'high-2')
        self.assertEqual(results[2], b'medium')
        self.assertEqual(results[3], b'low-1')
        self.assertEqual(results[4], b'low-2')

    def test_priority_default_zero(self):
        self.storage.enqueue(b'item-1')
        self.storage.enqueue(b'item-2')
        self.storage.enqueue(b'item-3')

        results = [self.storage.dequeue() for _ in range(3)]
        self.assertEqual(results, [b'item-1', b'item-2', b'item-3'])

    def test_priority_mixed_with_default(self):
        self.storage.enqueue(b'default-1')
        self.storage.enqueue(b'high-1', priority=10)
        self.storage.enqueue(b'default-2')
        self.storage.enqueue(b'high-2', priority=10)
        self.storage.enqueue(b'low', priority=-5)
        self.storage.enqueue(b'default-3')

        results = [self.storage.dequeue() for _ in range(6)]
        self.assertEqual(results[0], b'high-1')
        self.assertEqual(results[1], b'high-2')
        self.assertEqual(results[2], b'default-1')
        self.assertEqual(results[3], b'default-2')
        self.assertEqual(results[4], b'default-3')
        self.assertEqual(results[5], b'low')

    def test_priority_enqueued_items_order(self):
        self.storage.enqueue(b'a', priority=1)
        self.storage.enqueue(b'b', priority=3)
        self.storage.enqueue(b'c', priority=2)
        self.storage.enqueue(b'd', priority=3)

        items = self.storage.enqueued_items()
        self.assertEqual(items, [b'b', b'd', b'c', b'a'])

    def test_priority_dequeue_empty(self):
        self.assertIsNone(self.storage.dequeue())
        self.storage.enqueue(b'test')
        self.assertEqual(self.storage.dequeue(), b'test')
        self.assertIsNone(self.storage.dequeue())

    def test_priority_negative_values(self):
        self.storage.enqueue(b'neg10', priority=-10)
        self.storage.enqueue(b'neg5', priority=-5)
        self.storage.enqueue(b'zero', priority=0)
        self.storage.enqueue(b'pos5', priority=5)

        results = [self.storage.dequeue() for _ in range(4)]
        self.assertEqual(results, [b'pos5', b'zero', b'neg5', b'neg10'])

    def test_priority_float_values(self):
        self.storage.enqueue(b'f1.5', priority=1.5)
        self.storage.enqueue(b'f2.3', priority=2.3)
        self.storage.enqueue(b'f0.5', priority=0.5)
        self.storage.enqueue(b'f2.1', priority=2.1)

        results = [self.storage.dequeue() for _ in range(4)]
        self.assertEqual(results, [b'f2.3', b'f2.1', b'f1.5', b'f0.5'])


class TestSqliteStoragePriorityStrictFifo(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.mktemp(suffix='.db')
        self.storage = SqliteStorage(filename=self.db_file, strict_fifo=True)

    def tearDown(self):
        self.storage.flush_all()
        self.storage.close()
        if os.path.exists(self.db_file):
            try:
                os.unlink(self.db_file)
            except PermissionError:
                pass

    def test_priority_attribute_strict_fifo(self):
        self.assertTrue(self.storage.priority)

    def test_priority_simple_ordering_strict_fifo(self):
        priorities = (1, None, 5, None, 3, None, 9, None, 7, 0)
        for i, priority in enumerate(priorities):
            item = 'i%s-%s' % (i, priority)
            self.storage.enqueue(item.encode('utf8'), priority)

        self.assertEqual(self.storage.queue_size(), 10)

        expected = [b'i6-9', b'i8-7', b'i2-5', b'i4-3', b'i0-1',
                    b'i1-None', b'i3-None', b'i5-None', b'i7-None', b'i9-0']
        results = [self.storage.dequeue() for _ in range(10)]
        self.assertEqual(results, expected)
        self.assertEqual(self.storage.queue_size(), 0)

    def test_priority_high_first_strict_fifo(self):
        self.storage.enqueue(b'low-1', priority=1)
        self.storage.enqueue(b'high-1', priority=100)
        self.storage.enqueue(b'low-2', priority=1)
        self.storage.enqueue(b'high-2', priority=100)
        self.storage.enqueue(b'medium', priority=50)

        results = [self.storage.dequeue() for _ in range(5)]
        self.assertEqual(results[0], b'high-1')
        self.assertEqual(results[1], b'high-2')
        self.assertEqual(results[2], b'medium')
        self.assertEqual(results[3], b'low-1')
        self.assertEqual(results[4], b'low-2')

    def test_priority_same_priority_strict_fifo(self):
        for i in range(5):
            self.storage.enqueue(('item-%d' % i).encode('utf8'))

        results = [self.storage.dequeue() for _ in range(5)]
        self.assertEqual(results, [b'item-0', b'item-1', b'item-2', b'item-3', b'item-4'])

    def test_priority_mixed_strict_fifo(self):
        self.storage.enqueue(b'default-1')
        self.storage.enqueue(b'high-1', priority=10)
        self.storage.enqueue(b'default-2')
        self.storage.enqueue(b'high-2', priority=10)
        self.storage.enqueue(b'low', priority=-5)
        self.storage.enqueue(b'default-3')

        results = [self.storage.dequeue() for _ in range(6)]
        self.assertEqual(results[0], b'high-1')
        self.assertEqual(results[1], b'high-2')
        self.assertEqual(results[2], b'default-1')
        self.assertEqual(results[3], b'default-2')
        self.assertEqual(results[4], b'default-3')
        self.assertEqual(results[5], b'low')

    def test_priority_after_deletion_strict_fifo(self):
        for i in range(3):
            self.storage.enqueue(('high-%d' % i).encode('utf8'), priority=10)
        for i in range(3):
            self.storage.enqueue(('low-%d' % i).encode('utf8'), priority=1)

        results = [self.storage.dequeue() for _ in range(6)]
        self.assertEqual(results, [b'high-0', b'high-1', b'high-2', b'low-0', b'low-1', b'low-2'])

        for i in range(3):
            self.storage.enqueue(('new-high-%d' % i).encode('utf8'), priority=10)
        for i in range(3):
            self.storage.enqueue(('new-low-%d' % i).encode('utf8'), priority=1)

        results = [self.storage.dequeue() for _ in range(6)]
        self.assertEqual(results, [b'new-high-0', b'new-high-1', b'new-high-2',
                                   b'new-low-0', b'new-low-1', b'new-low-2'])

    def test_priority_enqueued_items_strict_fifo(self):
        self.storage.enqueue(b'a', priority=1)
        self.storage.enqueue(b'b', priority=3)
        self.storage.enqueue(b'c', priority=2)
        self.storage.enqueue(b'd', priority=3)

        items = self.storage.enqueued_items()
        self.assertEqual(items, [b'b', b'd', b'c', b'a'])


class TestSqliteHueyPriority(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.mktemp(suffix='.db')
        self.huey = SqliteHuey(filename=self.db_file, utc=False)
        self.state = []

        def task_fn(n):
            self.state.append(n)
            return n

        self.task_low = self.huey.task(priority=1, name='task_low')(task_fn)
        self.task_med = self.huey.task(priority=5, name='task_med')(task_fn)
        self.task_high = self.huey.task(priority=10, name='task_high')(task_fn)
        self.task_default = self.huey.task(name='task_default')(task_fn)

    def tearDown(self):
        self.huey.storage.flush_all()
        self.huey.storage.close()
        if os.path.exists(self.db_file):
            try:
                os.unlink(self.db_file)
            except PermissionError:
                pass

    def execute_next(self):
        task = self.huey.dequeue()
        self.assertIsNotNone(task)
        return self.huey.execute(task)

    def test_task_priority_simple(self):
        self.task_default(0)
        self.task_low(10)
        self.task_high(100)
        self.task_default(2)
        self.task_low(12)
        self.task_high(120)

        self.assertEqual(len(self.huey), 6)

        expected = [100, 120, 10, 12, 0, 2]
        for exp in expected:
            self.assertEqual(self.execute_next(), exp)

        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.state, expected)

    def test_task_priority_override(self):
        self.task_default(0)
        self.task_low(10)
        self.task_high(100)
        self.task_default(1, priority=10)
        self.task_low(11, priority=0)
        self.task_high(110, priority=1)

        expected = [100, 1, 10, 110, 0, 11]
        results = []
        for exp in expected:
            result = self.execute_next()
            results.append(result)
            self.assertEqual(result, exp)

        self.assertEqual(results, expected)

    def test_task_priority_retry(self):
        @self.huey.task(priority=10, retries=1)
        def failing_task(n):
            raise ValueError('test error')

        self.task_default(1)
        r = failing_task(100)

        self.assertEqual(len(self.huey), 2)

        task = self.huey.dequeue()
        self.assertEqual(task.priority, 10)
        self.assertEqual(task.retries, 1)

        self.assertRaises(TaskException, self.huey.execute, task)

        self.assertEqual(len(self.huey), 2)

        task = self.huey.dequeue()
        self.assertEqual(task.priority, 10)
        self.assertEqual(task.retries, 0)

        self.assertRaises(TaskException, self.huey.execute, task)

        self.assertEqual(len(self.huey), 1)
        task = self.huey.dequeue()
        self.assertEqual(task.priority, 0)


class TestSqliteHueyPriorityStrictFifo(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.mktemp(suffix='.db')
        self.huey = SqliteHuey(filename=self.db_file, utc=False, strict_fifo=True)
        self.state = []

        def task_fn(n):
            self.state.append(n)
            return n

        self.task_low = self.huey.task(priority=1, name='task_low_strict')(task_fn)
        self.task_med = self.huey.task(priority=5, name='task_med_strict')(task_fn)
        self.task_high = self.huey.task(priority=10, name='task_high_strict')(task_fn)
        self.task_default = self.huey.task(name='task_default_strict')(task_fn)

    def tearDown(self):
        self.huey.storage.flush_all()
        self.huey.storage.close()
        if os.path.exists(self.db_file):
            try:
                os.unlink(self.db_file)
            except PermissionError:
                pass

    def execute_next(self):
        task = self.huey.dequeue()
        self.assertIsNotNone(task)
        return self.huey.execute(task)

    def test_task_priority_simple_strict_fifo(self):
        self.task_default(0)
        self.task_low(10)
        self.task_high(100)
        self.task_default(2)
        self.task_low(12)
        self.task_high(120)

        self.assertEqual(len(self.huey), 6)

        expected = [100, 120, 10, 12, 0, 2]
        for exp in expected:
            self.assertEqual(self.execute_next(), exp)

        self.assertEqual(len(self.huey), 0)
        self.assertEqual(self.state, expected)

    def test_task_priority_override_strict_fifo(self):
        self.task_default(0)
        self.task_low(10)
        self.task_high(100)
        self.task_default(1, priority=10)
        self.task_low(11, priority=0)
        self.task_high(110, priority=1)

        expected = [100, 1, 10, 110, 0, 11]
        results = []
        for exp in expected:
            result = self.execute_next()
            results.append(result)
            self.assertEqual(result, exp)

        self.assertEqual(results, expected)

    def test_task_same_priority_strict_fifo(self):
        for i in range(5):
            self.task_default(i)

        self.assertEqual(len(self.huey), 5)

        expected = [0, 1, 2, 3, 4]
        for exp in expected:
            self.assertEqual(self.execute_next(), exp)

        self.assertEqual(self.state, expected)

    def test_task_priority_retry_strict_fifo(self):
        @self.huey.task(priority=10, retries=1)
        def failing_task(n):
            raise ValueError('test error')

        self.task_default(1)
        r = failing_task(100)

        self.assertEqual(len(self.huey), 2)

        task = self.huey.dequeue()
        self.assertEqual(task.priority, 10)
        self.assertEqual(task.retries, 1)

        self.assertRaises(TaskException, self.huey.execute, task)

        self.assertEqual(len(self.huey), 2)

        task = self.huey.dequeue()
        self.assertEqual(task.priority, 10)
        self.assertEqual(task.retries, 0)

        self.assertRaises(TaskException, self.huey.execute, task)

        self.assertEqual(len(self.huey), 1)
        task = self.huey.dequeue()
        self.assertEqual(task.priority, 0)

    def test_task_priority_mixed_strict_fifo(self):
        self.task_default(0)
        self.task_high(100)
        self.task_default(1)
        self.task_low(10)
        self.task_high(200)
        self.task_default(2)
        self.task_low(20)
        self.task_med(50)

        self.assertEqual(len(self.huey), 8)

        expected = [100, 200, 50, 10, 20, 0, 1, 2]
        for exp in expected:
            self.assertEqual(self.execute_next(), exp)

        self.assertEqual(self.state, expected)


if __name__ == '__main__':
    unittest.main()
