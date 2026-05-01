from functools import partial
import operator

from peewee import *
from playhouse.db_url import connect as db_url_connect

from huey.api import Huey
from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.storage import BaseStorage
from huey.storage import ResultStoreMixin


class BytesBlobField(BlobField):
    def python_value(self, value):
        return value if isinstance(value, bytes) else bytes(value)


class SqlStorage(ResultStoreMixin, BaseStorage):
    def __init__(self, name='huey', database=None, expire_time=None, **kwargs):
        if expire_time is not None:
            super(SqlStorage, self).__init__(name, expire_time=expire_time)
        else:
            super(SqlStorage, self).__init__(name)

        if database is None:
            raise ConfigurationError('Use of SqlStorage requires a '
                                     'database= argument, which should be a '
                                     'peewee database or a connection string.')

        if isinstance(database, Database):
            self.database = database
        else:
            self.database = db_url_connect(database)

        self.KV, self.Schedule, self.Task, self.Counter = self.create_models()
        self.create_tables()

        if isinstance(self.database, PostgresqlDatabase):
            self.for_update = 'FOR UPDATE SKIP LOCKED'
        elif isinstance(self.database, MySQLDatabase):
            self.for_update = 'FOR UPDATE SKIP LOCKED'
            version, = self.database.execute_sql('select version()').fetchone()
            if 'mariadb' in str(version).lower():
                if self.database.server_version < (10, 6):
                    self.for_update = 'FOR UPDATE'
            elif self.database.server_version < (8, 0, 1):
                self.for_update = 'FOR UPDATE'
        else:
            self.for_update = None

    def create_models(self):
        class Base(Model):
            class Meta:
                database = self.database

        class KV(Base):
            queue = CharField()
            key = CharField()
            value = BytesBlobField()
            expire_time = DoubleField(null=True)
            class Meta:
                primary_key = CompositeKey('queue', 'key')

        class Schedule(Base):
            queue = CharField()
            data = BytesBlobField()
            timestamp = TimestampField(resolution=1000)
            class Meta:
                indexes = ((('queue', 'timestamp'), False),)

        class Task(Base):
            queue = CharField()
            data = BytesBlobField()
            priority = FloatField(default=0.0)

        Task.add_index(Task.priority.desc(), Task.id)

        class Counter(Base):
            queue = CharField()
            key = CharField()
            value = IntegerField()
            expire_time = DoubleField(null=True)
            class Meta:
                primary_key = CompositeKey('queue', 'key')

        return (KV, Schedule, Task, Counter)

    def create_tables(self):
        with self.database:
            self.database.create_tables([self.KV, self.Schedule, self.Task,
                                         self.Counter])

    def drop_tables(self):
        with self.database:
            self.database.drop_tables([self.KV, self.Schedule, self.Task,
                                       self.Counter])

    def close(self):
        return self.database.close()

    def tasks(self, *columns):
        return self.Task.select(*columns).where(self.Task.queue == self.name)

    def schedule(self, *columns):
        return (self.Schedule.select(*columns)
                .where(self.Schedule.queue == self.name))

    def kv(self, *columns):
        return self.KV.select(*columns).where(self.KV.queue == self.name)

    def check_conn(self):
        if not self.database.is_connection_usable():
            self.database.close()
            self.database.connect()

    def enqueue(self, data, priority=None):
        self.check_conn()
        self.Task.create(queue=self.name, data=data, priority=priority or 0)

    def dequeue(self):
        self.check_conn()
        query = (self.tasks(self.Task.id, self.Task.data)
                 .order_by(self.Task.priority.desc(), self.Task.id)
                 .limit(1))
        if self.for_update:
            query = query.for_update(self.for_update)

        with self.database.atomic():
            try:
                task = query.get()
            except self.Task.DoesNotExist:
                return

            nrows = self.Task.delete().where(self.Task.id == task.id).execute()
            if nrows == 1:
                return task.data

    def queue_size(self):
        return self.tasks().count()

    def enqueued_items(self, limit=None):
        query = self.tasks(self.Task.data).order_by(self.Task.priority.desc(),
                                                    self.Task.id)
        if limit is not None:
            query = query.limit(limit)
        return list(map(operator.itemgetter(0), query.tuples()))

    def flush_queue(self):
        self.Task.delete().where(self.Task.queue == self.name).execute()

    def add_to_schedule(self, data, timestamp):
        self.check_conn()
        self.Schedule.create(queue=self.name, data=data, timestamp=timestamp)

    def read_schedule(self, timestamp):
        self.check_conn()
        query = (self.schedule(self.Schedule.id, self.Schedule.data)
                 .where(self.Schedule.timestamp <= timestamp)
                 .tuples())
        if self.for_update:
            query = query.for_update(self.for_update)

        with self.database.atomic():
            results = list(query)
            if not results:
                return []

            id_list, data = zip(*results)
            (self.Schedule
             .delete()
             .where(self.Schedule.id.in_(id_list))
             .execute())

            return list(data)

    def schedule_size(self):
        return self.schedule().count()

    def scheduled_items(self, limit=None):
        tasks = (self.schedule(self.Schedule.data)
                 .order_by(self.Schedule.timestamp)
                 .tuples())
        if limit:
            tasks = tasks.limit(limit)
        return list(map(operator.itemgetter(0), tasks))

    def flush_schedule(self):
        (self.Schedule
         .delete()
         .where(self.Schedule.queue == self.name)
         .execute())

    def _clean_expired(self):
        """Lazy cleanup of expired items."""
        self.check_conn()
        now = self._get_now_ts()
        (self.KV
         .delete()
         .where(
             (self.KV.queue == self.name) &
             (self.KV.expire_time.is_null(False)) &
             (self.KV.expire_time < now))
         .execute())

    def put_data(self, key, value, is_result=False):
        self.check_conn()
        expire_ts = self._calculate_expire_ts(is_result)
        if isinstance(self.database, PostgresqlDatabase):
            (self.KV
             .insert(queue=self.name, key=key, value=value, expire_time=expire_ts)
             .on_conflict(
                 conflict_target=[self.KV.queue, self.KV.key],
                 preserve=[self.KV.value, self.KV.expire_time])
             .execute())
        else:
            (self.KV
             .replace(queue=self.name, key=key, value=value, expire_time=expire_ts)
             .execute())

    def peek_data(self, key):
        self.check_conn()
        try:
            kv = (self.kv(self.KV.value, self.KV.expire_time)
                  .where(self.KV.key == key).get())
        except self.KV.DoesNotExist:
            return EmptyData
        else:
            if self._is_expired(kv.expire_time):
                (self.KV
                 .delete()
                 .where(
                     (self.KV.queue == self.name) &
                     (self.KV.key == key))
                 .execute())
                return EmptyData
            return kv.value

    def pop_data(self, key):
        if self._should_destructive_read():
            self.check_conn()
            query = self.kv(self.KV.value, self.KV.expire_time).where(self.KV.key == key)
            if self.for_update:
                query = query.for_update(self.for_update)

            with self.database.atomic():
                try:
                    kv = query.get()
                except self.KV.DoesNotExist:
                    return EmptyData
                else:
                    if self._is_expired(kv.expire_time):
                        (self.KV
                         .delete()
                         .where(
                             (self.KV.queue == self.name) &
                             (self.KV.key == key))
                         .execute())
                        return EmptyData
                    (self.KV
                     .delete()
                     .where(
                         (self.KV.queue == self.name) &
                         (self.KV.key == key))
                     .execute())
                    return kv.value
        else:
            return self.peek_data(key)

    def has_data_for_key(self, key):
        self.check_conn()
        try:
            kv = (self.kv(self.KV.expire_time)
                  .where(self.KV.key == key).get())
        except self.KV.DoesNotExist:
            return False
        else:
            if self._is_expired(kv.expire_time):
                (self.KV
                 .delete()
                 .where(
                     (self.KV.queue == self.name) &
                     (self.KV.key == key))
                 .execute())
                return False
            return True

    def put_if_empty(self, key, value):
        self.check_conn()
        try:
            with self.database.atomic():
                if self.has_data_for_key(key):
                    return False
                (self.KV
                 .insert(queue=self.name, key=key, value=value, expire_time=None)
                 .execute())
        except IntegrityError:
            return False
        else:
            return True

    def delete_data(self, key):
        self.check_conn()
        nrows = (self.KV
                 .delete()
                 .where(
                     (self.KV.queue == self.name) &
                     (self.KV.key == key))
                 .execute())
        return nrows == 1

    def incr(self, key, amount=1):
        self.check_conn()
        with self.database.atomic():
            try:
                counter = (self.Counter
                           .select(self.Counter.value, self.Counter.expire_time)
                           .where(
                               (self.Counter.queue == self.name) &
                               (self.Counter.key == key))
                           .get())
                if self._is_expired(counter.expire_time):
                    val = 0
                else:
                    val = counter.value
            except self.Counter.DoesNotExist:
                val = 0

            new_val = val + amount
            if self._expire_time is not None:
                expire_ts = self._get_now_ts() + self._expire_time
            else:
                expire_ts = None

            if isinstance(self.database, MySQLDatabase):
                self._incr_mysql(key, new_val, expire_ts)
            else:
                self._incr(key, new_val, expire_ts)

            return new_val

    def _incr_mysql(self, key, val, expire_ts):
        (self.Counter
         .insert(queue=self.name, key=key, value=val, expire_time=expire_ts)
         .on_conflict(update={
             self.Counter.value: val,
             self.Counter.expire_time: expire_ts})
         .execute())

    def _incr(self, key, val, expire_ts):
        (self.Counter
         .insert(queue=self.name, key=key, value=val, expire_time=expire_ts)
         .on_conflict(
             conflict_target=(self.Counter.queue, self.Counter.key),
             update={self.Counter.value: val, self.Counter.expire_time: expire_ts})
         .execute())

    def delete_counter(self, key):
        with self.database.atomic():
            self.Counter.delete().where(
                (self.Counter.queue == self.name) &
                (self.Counter.key == key)).execute()

    def result_store_size(self):
        self._clean_expired()
        return self.kv().count()

    def result_items(self):
        self._clean_expired()
        query = self.kv(self.KV.key, self.KV.value).tuples()
        return dict((k, v) for k, v in query.iterator())

    def flush_results(self):
        self.KV.delete().where(self.KV.queue == self.name).execute()

    def flush_counters(self):
        self.Counter.delete().where(self.Counter.queue == self.name).execute()


SqlHuey = partial(Huey, storage_class=SqlStorage)
