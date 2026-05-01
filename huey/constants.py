WORKER_THREAD = 'thread'
WORKER_GREENLET = 'greenlet'
WORKER_PROCESS = 'process'
WORKER_TYPES = (WORKER_THREAD, WORKER_GREENLET, WORKER_PROCESS)


class TaskStatus(object):
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELED = 'canceled'


class EmptyData(object):
    pass
