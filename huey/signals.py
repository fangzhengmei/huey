import itertools
from collections import OrderedDict

from huey.exceptions import CancelExecution


SIGNAL_CANCELED = 'canceled'
SIGNAL_COMPLETE = 'complete'
SIGNAL_ERROR = 'error'
SIGNAL_EXECUTING = 'executing'
SIGNAL_EXPIRED = 'expired'
SIGNAL_LOCKED = 'locked'
SIGNAL_RETRYING = 'retrying'
SIGNAL_REVOKED = 'revoked'
SIGNAL_SCHEDULED = 'scheduled'
SIGNAL_INTERRUPTED = 'interrupted'
SIGNAL_ENQUEUED = 'enqueued'
SIGNAL_TIMEOUT = 'timeout'
SIGNAL_RATE_LIMITED = 'rate-limited'


SIGNAL_PRE_EXECUTE = 'pre-execute'
SIGNAL_POST_EXECUTE = 'post-execute'
SIGNAL_STARTUP = 'startup'
SIGNAL_SHUTDOWN = 'shutdown'


ALL_SIGNALS = (
    SIGNAL_CANCELED,
    SIGNAL_COMPLETE,
    SIGNAL_ERROR,
    SIGNAL_EXECUTING,
    SIGNAL_EXPIRED,
    SIGNAL_LOCKED,
    SIGNAL_RETRYING,
    SIGNAL_REVOKED,
    SIGNAL_SCHEDULED,
    SIGNAL_INTERRUPTED,
    SIGNAL_ENQUEUED,
    SIGNAL_TIMEOUT,
    SIGNAL_RATE_LIMITED,
    SIGNAL_PRE_EXECUTE,
    SIGNAL_POST_EXECUTE,
    SIGNAL_STARTUP,
    SIGNAL_SHUTDOWN,
)


class ISignalDispatcher(object):
    """
    信号分发器接口契约。
    
    定义了信号分发的核心操作，所有实现必须遵循此接口。
    """

    def connect(self, receiver, *signals):
        """
        连接一个信号接收器到指定的信号类型。
        
        :param receiver: 信号处理函数，签名为 receiver(signal, task, *args, **kwargs)
        :param signals: 要监听的信号类型，不指定则监听所有信号
        """
        raise NotImplementedError

    def disconnect(self, receiver, *signals):
        """
        断开指定接收器与信号的连接。
        
        :param receiver: 要断开的接收器函数
        :param signals: 要断开的信号类型，不指定则断开所有信号
        """
        raise NotImplementedError

    def emit(self, signal, task, *args, **kwargs):
        """
        发射一个信号，通知所有注册的接收器。
        
        :param signal: 信号类型
        :param task: 相关任务对象
        :param args: 额外位置参数
        :param kwargs: 额外关键字参数
        """
        raise NotImplementedError

    def register_hook(self, hook_type, name, callback):
        """
        注册一个生命周期钩子。
        
        :param hook_type: 钩子类型（SIGNAL_PRE_EXECUTE 等）
        :param name: 钩子名称
        :param callback: 回调函数
        """
        raise NotImplementedError

    def unregister_hook(self, hook_type, name):
        """
        注销一个生命周期钩子。
        
        :param hook_type: 钩子类型
        :param name: 钩子名称
        :return: 是否成功注销
        """
        raise NotImplementedError

    def get_hooks(self, hook_type):
        """
        获取指定类型的所有钩子。
        
        :param hook_type: 钩子类型
        :return: OrderedDict 包含 {name: callback}
        """
        raise NotImplementedError


class Signal(ISignalDispatcher):
    __slots__ = ('receivers', '_hooks')

    def __init__(self):
        self.receivers = {'any': []}
        self._hooks = {
            SIGNAL_PRE_EXECUTE: OrderedDict(),
            SIGNAL_POST_EXECUTE: OrderedDict(),
            SIGNAL_STARTUP: OrderedDict(),
            SIGNAL_SHUTDOWN: OrderedDict(),
        }

    def connect(self, receiver, *signals):
        if not signals:
            signals = ('any',)
        for signal in signals:
            self.receivers.setdefault(signal, [])
            self.receivers[signal].append(receiver)

    def disconnect(self, receiver, *signals):
        if not signals:
            signals = list(self.receivers)
        for signal in signals:
            try:
                self.receivers[signal].remove(receiver)
            except ValueError:
                pass

    def _send_to_receivers(self, signal, task, *args, **kwargs):
        receivers = itertools.chain(self.receivers.get(signal, ()),
                                    self.receivers['any'])
        for receiver in receivers:
            receiver(signal, task, *args, **kwargs)

    def send(self, signal, task, *args, **kwargs):
        self._send_to_receivers(signal, task, *args, **kwargs)

    def emit(self, signal, task, *args, **kwargs):
        self._send_to_receivers(signal, task, *args, **kwargs)

    def register_hook(self, hook_type, name, callback):
        if hook_type not in self._hooks:
            raise ValueError('Invalid hook type: %s' % hook_type)
        self._hooks[hook_type][name] = callback

    def unregister_hook(self, hook_type, name):
        if hook_type not in self._hooks:
            return False
        return self._hooks[hook_type].pop(name, None) is not None

    def get_hooks(self, hook_type):
        return self._hooks.get(hook_type, OrderedDict()).copy()


class SignalDispatcher(ISignalDispatcher):
    """
    统一的信号分发器。
    
    整合了信号系统和生命周期钩子系统，提供统一的接口：
    - 信号监听/发射：用于任务状态变化通知
    - 钩子注册/注销：用于执行前后、启动/关闭等生命周期事件
    
    所有后端和任务类型都应通过此接口触发信号。
    """

    def __init__(self):
        self._signal = Signal()
        self._logger = None

    def set_logger(self, logger):
        self._logger = logger

    def connect(self, receiver, *signals):
        self._signal.connect(receiver, *signals)

    def disconnect(self, receiver, *signals):
        self._signal.disconnect(receiver, *signals)

    def emit(self, signal, task, *args, **kwargs):
        try:
            self._signal.emit(signal, task, *args, **kwargs)
        except Exception as exc:
            if self._logger:
                self._logger.exception('Error occurred sending signal "%s"', signal)
            else:
                raise

    def register_hook(self, hook_type, name, callback):
        self._signal.register_hook(hook_type, name, callback)

    def unregister_hook(self, hook_type, name):
        return self._signal.unregister_hook(hook_type, name)

    def get_hooks(self, hook_type):
        return self._signal.get_hooks(hook_type)

    def execute_pre_execute_hooks(self, task):
        """
        Execute pre-execute hooks with legacy-compatible semantics:
        - CancelExecution is re-raised to cancel the task.
        - Other exceptions are logged and execution continues.
        """
        hooks = self._signal.get_hooks(SIGNAL_PRE_EXECUTE)
        for name, callback in hooks.items():
            if self._logger:
                self._logger.debug('Pre-execute hook %s for %s.', name, task)
            try:
                callback(task)
            except CancelExecution:
                if self._logger:
                    self._logger.warning('Task %s cancelled by %s (pre-execute).',
                                         task, name)
                raise
            except Exception:
                if self._logger:
                    self._logger.exception(
                        'Unhandled exception calling pre-execute '
                        'hook %s for %s.', name, task)
                # Preserve pre-refactor behavior: non-cancel hook failures do
                # not abort task execution.

    def execute_post_execute_hooks(self, task, task_value, exception):
        """
        执行所有 post-execute 钩子。
        """
        hooks = self._signal.get_hooks(SIGNAL_POST_EXECUTE)
        for name, callback in hooks.items():
            if self._logger:
                self._logger.debug('Post-execute hook %s for %s.', name, task)
            try:
                callback(task, task_value, exception)
            except Exception:
                if self._logger:
                    self._logger.exception(
                        'Unhandled exception calling post-execute '
                        'hook %s for %s.', name, task)

    def execute_startup_hooks(self):
        """
        执行所有 startup 钩子。
        """
        hooks = self._signal.get_hooks(SIGNAL_STARTUP)
        for name, callback in hooks.items():
            if self._logger:
                self._logger.debug('calling startup hook "%s"', name)
            try:
                callback()
            except Exception:
                if self._logger:
                    self._logger.exception('startup hook "%s" failed', name)

    def execute_shutdown_hooks(self):
        """
        执行所有 shutdown 钩子。
        """
        hooks = self._signal.get_hooks(SIGNAL_SHUTDOWN)
        for name, callback in hooks.items():
            if self._logger:
                self._logger.debug('calling shutdown hook "%s"', name)
            try:
                callback()
            except Exception:
                if self._logger:
                    self._logger.exception('shutdown hook "%s" failed', name)
