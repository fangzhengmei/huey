# Huey 信号（Signal）与钩子（Hook）机制分析报告（修订版）

**核对日期**: 2026-05-02  
**代码版本**: 当前工作目录代码  
**关键修正**: 信号总数为 **13 个**（之前误报为 14 个）

---

## 目录
1. [信号系统定义与注册](#1-信号系统定义与注册)
2. [任务生命周期与信号触发](#2-任务生命周期与信号触发)
3. [钩子机制详解](#3-钩子机制详解)
4. [调度器与存储后端的交互](#4-调度器与存储后端的交互)
5. [附录：完整代码引用表](#5-附录完整代码引用表)

---

## 1. 信号系统定义与注册

### 1.1 信号常量定义（共 13 个）

**来源文件**: `huey/signals.py` (第 4-16 行)

| 序号 | 常量名称 | 字符串值 | 代码行 |
|-----|---------|---------|-------|
| 1 | `SIGNAL_CANCELED` | `'canceled'` | 第 4 行 |
| 2 | `SIGNAL_COMPLETE` | `'complete'` | 第 5 行 |
| 3 | `SIGNAL_ERROR` | `'error'` | 第 6 行 |
| 4 | `SIGNAL_EXECUTING` | `'executing'` | 第 7 行 |
| 5 | `SIGNAL_EXPIRED` | `'expired'` | 第 8 行 |
| 6 | `SIGNAL_LOCKED` | `'locked'` | 第 9 行 |
| 7 | `SIGNAL_RETRYING` | `'retrying'` | 第 10 行 |
| 8 | `SIGNAL_REVOKED` | `'revoked'` | 第 11 行 |
| 9 | `SIGNAL_SCHEDULED` | `'scheduled'` | 第 12 行 |
| 10 | `SIGNAL_INTERRUPTED` | `'interrupted'` | 第 13 行 |
| 11 | `SIGNAL_ENQUEUED` | `'enqueued'` | 第 14 行 |
| 12 | `SIGNAL_TIMEOUT` | `'timeout'` | 第 15 行 |
| 13 | `SIGNAL_RATE_LIMITED` | `'rate-limited'` | 第 16 行 |

**重要修正**: `signals.py` 第 4-16 行共定义 **13 个** 信号常量，不是 14 个。

### 1.2 Signal 类实现

**来源文件**: `huey/signals.py` (第 19-45 行)

```python
class Signal(object):
    __slots__ = ('receivers',)  # 第 20 行

    def __init__(self):
        self.receivers = {'any': []}  # 第 23 行：'any' 通道接收所有信号

    def connect(self, receiver, *signals):
        if not signals:
            signals = ('any',)  # 第 27 行：不指定则注册到 'any'
        for signal in signals:
            self.receivers.setdefault(signal, [])
            self.receivers[signal].append(receiver)

    def send(self, signal, task, *args, **kwargs):
        # 第 42-44 行：链式调用指定信号 + 'any' 通道的接收者
        receivers = itertools.chain(self.receivers.get(signal, ()),
                                    self.receivers['any'])
        for receiver in receivers:
            receiver(signal, task, *args, **kwargs)
```

### 1.3 Huey 中的信号集成

**来源文件**: `huey/api.py`

| 功能 | 代码位置 | 说明 |
|-----|---------|------|
| Signal 实例化 | 第 117 行 | `self._signal = S.Signal()` |
| 信号装饰器 | 第 274-278 行 | `@huey.signal()` |
| 注销信号 | 第 280-281 行 | `disconnect_signal()` |
| 发射信号方法 | 第 283-288 行 | `_emit()` 方法 |

**`_emit()` 方法实现** (第 283-288 行):
```python
def _emit(self, signal, task, *args, **kwargs):
    try:
        self._signal.send(signal, task, *args, **kwargs)
    except Exception as exc:
        logger.exception('Error occurred sending signal "%s"', signal)
```

**关键特性**: 信号接收器的异常被捕获并记录，**不会中断任务执行**。

---

## 2. 任务生命周期与信号触发

本节按任务执行顺序分析每个信号的触发条件和代码位置。

### 2.1 阶段概览

```
任务入队 (SIGNAL_ENQUEUED)
    ↓
[调度检查: eta 未到? → SIGNAL_SCHEDULED]
    ↓
[撤销检查: 已撤销? → SIGNAL_REVOKED → 终止]
    ↓
[过期检查: 已过期? → SIGNAL_EXPIRED → 终止]
    ↓
开始执行 (SIGNAL_EXECUTING)
    ↓
[pre_execute 钩子]
    ↓
任务实际执行
    ↓
[异常处理分支]
    ├── TaskTimeout → SIGNAL_TIMEOUT
    ├── RateLimitExceeded → SIGNAL_RATE_LIMITED
    ├── TaskLockedException → SIGNAL_LOCKED
    ├── RetryTask → (仅设置重试状态，无直接信号)
    ├── CancelExecution → SIGNAL_CANCELED
    ├── KeyboardInterrupt → SIGNAL_INTERRUPTED → 直接返回
    └── 其他 Exception → SIGNAL_ERROR (带 exc 参数)
    ↓
存储结果
    ↓
[post_execute 钩子]
    ↓
成功完成? → SIGNAL_COMPLETE
    ↓
[有重试次数? → SIGNAL_RETRYING → 重新入队/调度]
```

### 2.2 详细信号触发分析

#### 信号 1: SIGNAL_ENQUEUED

**触发时机**: 任务被放入队列时

**代码位置**: `huey/api.py` 第 307 行

```python
def enqueue(self, task):
    # ... 处理 group 和 chord ...
    if task.expires:
        task.resolve_expires(self.utc)

    self._emit(S.SIGNAL_ENQUEUED, task)  # 第 307 行：发射信号

    if self._immediate:
        self.execute(task)
    else:
        self.storage.enqueue(self.serialize_task(task), task.priority)  # 第 312 行：存储
    # ...
```

**关键观察**:
- 信号在**存储操作之前**发射
- 这是唯一可能在**应用进程**中触发的信号（其他信号主要在 Consumer 进程）

**触发场景**:
1. 应用代码调用任务函数: `my_task()` → 第 307 行
2. 调度器将到期任务重新入队: `huey/enqueue(task)` → 第 307 行
3. 任务重试时重新入队: `_requeue_task()` → 第 573 行调用 `enqueue()`
4. Chord 回调任务入队: `_check_chord()` → 第 560 行调用 `enqueue()`
5. Pipeline 下一个任务入队: `on_complete` 处理 → 第 525 行调用 `enqueue()`

---

#### 信号 2: SIGNAL_SCHEDULED

**触发时机**: 任务被添加到调度队列（延迟执行）时

**代码位置**: `huey/api.py` 第 701 行

```python
def add_schedule(self, task):
    data = self.serialize_task(task)
    eta = task.eta or datetime.datetime.fromtimestamp(0)
    self.storage.add_to_schedule(data, eta)  # 第 699 行：先存储
    logger.info('Added task %s to schedule, eta %s', task.id, eta)
    self._emit(S.SIGNAL_SCHEDULED, task)  # 第 701 行：后发射信号
```

**触发路径 1**: `execute()` 中检查 `ready_to_run()`

**代码位置**: `huey/api.py` 第 417-418 行

```python
def execute(self, task, timestamp=None):
    # ...
    if not self.ready_to_run(task, timestamp):  # 第 417 行：检查 eta
        self.add_schedule(task)  # 第 418 行 → 触发 SIGNAL_SCHEDULED
    # ...
```

**触发路径 2**: `_requeue_task()` 中有延迟时

**代码位置**: `huey/api.py` 第 562-573 行

```python
def _requeue_task(self, task, timestamp, retry_eta=None):
    task.retries -= 1
    if retry_eta is not None:
        task.eta = retry_eta
        self.add_schedule(task)  # 第 567 行
    elif task.retry_delay:
        delay = datetime.timedelta(seconds=task.retry_delay)
        task.eta = timestamp + delay
        self.add_schedule(task)  # 第 571 行
    else:
        self.enqueue(task)  # 第 573 行 → 触发 SIGNAL_ENQUEUED
```

---

#### 信号 3: SIGNAL_REVOKED

**触发时机**: 任务被撤销，不再执行时

**代码位置**: `huey/api.py` 第 419-421 行

```python
def execute(self, task, timestamp=None):
    # ...
    elif self.is_revoked(task, timestamp, False):  # 第 419 行：检查撤销状态
        logger.warning('Task %s was revoked, not executing', task)
        self._emit(S.SIGNAL_REVOKED, task)  # 第 421 行
    # ...
```

**撤销检查的实现**: `is_revoked()` 方法 (第 671-694 行)

支持多种撤销方式:
1. 按任务实例撤销: `revoke_id = 'r:{task_id}'`
2. 按任务类撤销: 所有该类型任务
3. 撤销直到指定时间: `revoke_until`
4. 仅撤销一次: `revoke_once`

**测试验证**: `huey/tests/test_signals.py` 第 80-94 行

```python
def test_signals_revoked(self):
    @self.huey.task()
    def task_a(n):
        return n + 1

    task_a.revoke(revoke_once=True)
    r = task_a(2)
    self.assertSignals([SIGNAL_ENQUEUED])
    self.assertTrue(self.execute_next() is None)
    self.assertSignals([SIGNAL_REVOKED])  # 验证信号
```

---

#### 信号 4: SIGNAL_EXPIRED

**触发时机**: 任务过期时间已过，不再执行时

**代码位置**: `huey/api.py` 第 422-424 行

```python
def execute(self, task, timestamp=None):
    # ...
    elif task.expires_resolved and task.expires_resolved < timestamp:  # 第 422 行
        logger.info('Task %s expired, not executing.', task)
        self._emit(S.SIGNAL_EXPIRED, task)  # 第 424 行
    # ...
```

**过期时间解析**: 过期时间在 `enqueue()` 时解析 (第 304-305 行)

```python
def enqueue(self, task):
    # ...
    if task.expires:
        task.resolve_expires(self.utc)  # 第 305 行
    # ...
```

**测试验证**: `huey/tests/test_signals.py` 第 156-171 行

```python
def test_signal_expired(self):
    @self.huey.task(expires=10)
    def task_a(n):
        return n + 1

    now = datetime.datetime.now()
    expires = now + datetime.timedelta(seconds=15)
    r = task_a(2)
    self.assertSignals([SIGNAL_ENQUEUED])
    self.assertTrue(self.execute_next(expires) is None)  # 传入过期后的时间
    self.assertSignals([SIGNAL_EXPIRED])  # 验证信号
```

---

#### 信号 5: SIGNAL_EXECUTING

**触发时机**: 任务即将被执行时

**代码位置**: `huey/api.py` 第 426-428 行

```python
def execute(self, task, timestamp=None):
    # ... 前置检查都通过了 ...
    else:
        logger.info('Executing %s', task)
        self._emit(S.SIGNAL_EXECUTING, task)  # 第 427 行
        return self._execute(task, timestamp)  # 第 428 行：实际执行
```

**关键观察**: 此信号发射后立即调用 `_execute()` 执行任务。

---

#### 信号 6: SIGNAL_CANCELED

**触发时机**: 任务被取消时（有两个触发点）

**触发点 1**: `pre_execute` 钩子抛出 `CancelExecution`

**代码位置**: `huey/api.py` 第 431-436 行

```python
def _execute(self, task, timestamp):
    if self._pre_execute:  # 第 431 行
        try:
            self._run_pre_execute(task)  # 第 433 行
        except CancelExecution:
            self._emit(S.SIGNAL_CANCELED, task)  # 第 435 行
            return  # 直接返回，不执行任务
```

**触发点 2**: 任务函数自身抛出 `CancelExecution`

**代码位置**: `huey/api.py` 第 482-491 行

```python
def _execute(self, task, timestamp):
    # ...
    except CancelExecution as exc:  # 第 482 行
        if exc.retry or (exc.retry is None and task.retries):
            task.retries = max(task.retries, 1)
            msg = '(task will be retried)'
        else:
            task.retries = 0
            msg = '(aborted, will not be retried)'
        logger.warning('Task %s raised CancelExecution %s.', task.id, msg)
        self._emit(S.SIGNAL_CANCELED, task)  # 第 490 行
        exception = exc
    # ...
```

**关键差异**:
- 触发点 1 (第 435 行): 直接 `return`，不会进入后续重试逻辑
- 触发点 2 (第 490 行): 设置 `exception = exc`，可能进入重试逻辑 (第 537 行)

---

#### 信号 7: SIGNAL_TIMEOUT

**触发时机**: 任务执行超时时

**代码位置**: `huey/api.py` 第 455-459 行

```python
def _execute(self, task, timestamp):
    # ...
    except TaskTimeout as exc:  # 第 455 行
        logger.warning('Task %s timed out after %ss.', task.id,
                       task.timeout)
        exception = exc
        self._emit(S.SIGNAL_TIMEOUT, task)  # 第 459 行
```

**超时检测机制**:
1. 协作式超时：任务函数需要调用 `task.check_timeout()`
2. 超时上下文管理器: `_timeout_context()` (第 402-407 行)

**测试验证**: `huey/tests/test_signals.py` 第 173-181 行

```python
def test_signal_timeout(self):
    @self.huey.task(timeout=0.001, context=True)
    def timeout(task=None):
        time.sleep(0.01)
        task.check_timeout()  # 协作式超时检查

    r = timeout()
    self.execute_next()
    self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING, SIGNAL_TIMEOUT])
```

---

#### 信号 8: SIGNAL_RATE_LIMITED

**触发时机**: 任务被限流时

**代码位置**: `huey/api.py` 第 460-471 行

```python
def _execute(self, task, timestamp):
    # ...
    except RateLimitExceeded as exc:  # 第 460 行
        delay = task.retry_delay or exc.delay
        if exc.retry or task.retries:  # 第 462 行：允许重试
            logger.info('Task %s rate-limited on "%s", retrying in %s',
                        task.id, exc.key, delay)
            retry_eta = normalize_time(None, delay, self.utc)
            if exc.retry:
                task.retries += 1
        else:  # 不允许重试
            logger.info('Task %s rate-limited on "%s"', task.id, exc.key)
        exception = exc
        self._emit(S.SIGNAL_RATE_LIMITED, task)  # 第 471 行
```

**限流实现**: `RateLimit` 类 (第 1082-1137 行) 使用固定窗口算法。

**测试验证**: `huey/tests/test_signals.py` 第 113-154 行

```python
def test_signals_ratelimit(self):
    @self.huey.task()
    @self.huey.rate_limit('rl', limit=1, per=60)  # 默认允许重试
    def task_a():
        return 1

    # 第一次执行成功
    r = task_a()
    self.assertEqual(self.execute_next(), 1)
    self.assertSignals([SIGNAL_ENQUEUED, SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    # 第二次执行被限流，且会重试
    r = task_a()
    self.assertTrue(self.execute_next() is None)
    self.assertSignals([
        SIGNAL_ENQUEUED,
        SIGNAL_EXECUTING,
        SIGNAL_RATE_LIMITED,
        SIGNAL_RETRYING,  # 因为允许重试
        SIGNAL_SCHEDULED])
```

---

#### 信号 9: SIGNAL_LOCKED

**触发时机**: 任务无法获取锁时

**代码位置**: `huey/api.py` 第 472-475 行

```python
def _execute(self, task, timestamp):
    # ...
    except TaskLockedException as exc:  # 第 472 行
        logger.warning('Task %s not run, %s.', task.id, exc)
        exception = exc
        self._emit(S.SIGNAL_LOCKED, task)  # 第 475 行
```

**锁实现**: `TaskLock` 类 (第 1044-1079 行) 使用存储后端的 `put_if_empty()` 实现。

**测试验证**: `huey/tests/test_signals.py` 第 96-111 行

```python
def test_signals_locked(self):
    @self.huey.task()
    @self.huey.lock_task('lock-a')
    def task_a(n):
        return n + 1

    # 正常执行
    r = task_a(1)
    self.assertSignals([SIGNAL_ENQUEUED])
    self.assertEqual(self.execute_next(), 2)
    self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])

    # 持有锁时执行任务
    with self.huey.lock_task('lock-a'):
        r = task_a(2)
        self.assertSignals([SIGNAL_ENQUEUED])
        self.assertTrue(self.execute_next() is None)
        self.assertSignals([SIGNAL_EXECUTING, SIGNAL_LOCKED])  # 验证信号
```

---

#### 信号 10: SIGNAL_INTERRUPTED

**触发时机**: 任务执行被中断时（有两个触发点）

**触发点 1**: 任务执行中收到 `KeyboardInterrupt`

**代码位置**: `huey/api.py` 第 492-495 行

```python
def _execute(self, task, timestamp):
    # ...
    except KeyboardInterrupt:  # 第 492 行
        logger.warning('Received exit signal, %s did not finish.', task.id)
        self._emit(S.SIGNAL_INTERRUPTED, task)  # 第 494 行
        return  # 直接返回，不存储结果，不重试
```

**触发点 2**: Consumer 关闭时通知所有进行中的任务

**代码位置**: `huey/api.py` 第 269-272 行

```python
def notify_interrupted_tasks(self):
    while self._tasks_in_flight:
        task = self._tasks_in_flight.pop()
        self._emit(S.SIGNAL_INTERRUPTED, task)  # 第 272 行
```

**调用时机**: `Consumer.run()` 方法结束时调用 (第 479 行)

```python
# consumer.py 第 479 行
self.huey.notify_interrupted_tasks()
```

**任务追踪机制**:
- 任务开始时添加: `_execute()` 第 448 行 `self._tasks_in_flight.add(task)`
- 任务结束时移除: `_execute()` 第 453 行 `self._tasks_in_flight.remove(task)` (在 `finally` 块中)

**文档建议用途** (`docs/signals.rst`):
```python
@huey.signal(SIGNAL_INTERRUPTED)
def on_interrupted(signal, task, *args, **kwargs):
    # Consumer 被关闭前任务未完成，重新入队
    huey.enqueue(task)
```

---

#### 信号 11: SIGNAL_ERROR

**触发时机**: 任务执行抛出未处理的异常时

**代码位置**: `huey/api.py` 第 496-499 行

```python
def _execute(self, task, timestamp):
    # ...
    except Exception as exc:  # 第 496 行：捕获其他所有异常
        logger.exception('Unhandled exception in task %s.', task.id)
        exception = exc
        self._emit(S.SIGNAL_ERROR, task, exc)  # 第 499 行：带 exc 参数
```

**关键特性**: 此信号是**唯一传递额外参数**的信号 (`exc` 参数为异常实例)。

**测试验证**: `huey/tests/test_signals.py` 第 37-40 行

```python
def test_signals_simple(self):
    # ...
    r = task_a(None)  # 传入 None 会导致 TypeError
    self.assertSignals([SIGNAL_ENQUEUED])
    self.assertTrue(self.execute_next() is None)
    self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR])
```

---

#### 信号 12: SIGNAL_COMPLETE

**触发时机**: 任务成功执行完成时

**代码位置**: `huey/api.py` 第 518-520 行

```python
def _execute(self, task, timestamp):
    # ...
    if exception is None:  # 第 518 行：无异常
        # Task executed successfully, send the COMPLETE signal.
        self._emit(S.SIGNAL_COMPLETE, task)  # 第 520 行
```

**执行时序**:
```
1. 任务实际执行 (第 451 行)
2. 存储结果 (第 508-513 行)
3. post_execute 钩子 (第 515-516 行)
4. SIGNAL_COMPLETE (第 520 行) ← 此时结果已存储
```

**关键观察**: `SIGNAL_COMPLETE` 发射时，任务结果已经存储到存储后端，可以安全读取。

---

#### 信号 13: SIGNAL_RETRYING

**触发时机**: 任务失败但将重试时

**代码位置**: `huey/api.py` 第 537-539 行

```python
def _execute(self, task, timestamp):
    # ...
    if exception is not None and task.retries:  # 第 537 行：有异常且有重试次数
        self._emit(S.SIGNAL_RETRYING, task)  # 第 538 行
        self._requeue_task(task, self._get_timestamp(), retry_eta)  # 第 539 行
```

**重试条件**:
1. `exception is not None` - 发生了异常
2. `task.retries > 0` - 还有重试次数

**可能触发重试的异常类型**:
- `TaskTimeout` (SIGNAL_TIMEOUT)
- `RateLimitExceeded` (SIGNAL_RATE_LIMITED，且允许重试)
- `RetryTask` (无直接信号)
- `CancelExecution` (SIGNAL_CANCELED，且 `exc.retry=True`)
- 其他 `Exception` (SIGNAL_ERROR)

**不会触发重试的异常类型**:
- `TaskLockedException` (SIGNAL_LOCKED) - 不会重试
- `KeyboardInterrupt` (SIGNAL_INTERRUPTED) - 直接 `return`

**测试验证**: `huey/tests/test_signals.py` 第 57-68 行

```python
def test_signals_on_retry(self):
    @self.huey.task(retries=1)  # 配置 1 次重试
    def task_a(n):
        return n + 1

    r = task_a(None)  # 会抛出 TypeError
    self.assertSignals([SIGNAL_ENQUEUED])

    self.assertTrue(self.execute_next() is None)
    self.assertSignals([
        SIGNAL_EXECUTING, 
        SIGNAL_ERROR, 
        SIGNAL_RETRYING,   # 重试信号
        SIGNAL_ENQUEUED])   # 重新入队

    # 第二次执行（重试次数用完）
    self.assertTrue(self.execute_next() is None)
    self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR])  # 没有重试了
```

---

### 2.3 完整执行流程图（带代码引用）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              任务执行完整流程                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  应用进程 / Consumer 进程                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ enqueue(task)                                                        │  │
│  │   ├── 第 304-305 行: 解析过期时间                                     │  │
│  │   ├── 第 307 行: SIGNAL_ENQUEUED ◄─────────────────────────────────┼──┼─── 信号 11
│  │   ├── 第 309-310 行: immediate 模式 → 直接 execute()                │  │
│  │   └── 第 312 行: 存储到队列                                          │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    ↓                                         │
│  Consumer Worker 进程                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ execute(task, timestamp)                                             │  │
│  │                                                                       │  │
│  │  第 417 行: if not ready_to_run()                                     │  │
│  │    └── 是 → 第 418 行: add_schedule()                                │  │
│  │              └── 第 701 行: SIGNAL_SCHEDULED ◄──────────────────────┼──┼─── 信号 9
│  │                                                                       │  │
│  │  第 419 行: elif is_revoked()                                         │  │
│  │    └── 是 → 第 421 行: SIGNAL_REVOKED ◄────────────────────────────┼──┼─── 信号 8
│  │                                                                       │  │
│  │  第 422 行: elif expires_resolved < timestamp                         │  │
│  │    └── 是 → 第 424 行: SIGNAL_EXPIRED ◄────────────────────────────┼──┼─── 信号 5
│  │                                                                       │  │
│  │  第 426-427 行: 都通过了                                              │  │
│  │    ├── 第 427 行: SIGNAL_EXECUTING ◄────────────────────────────────┼──┼─── 信号 4
│  │    └── 第 428 行: _execute(task, timestamp)                          │  │
│  │                                                                       │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                    ↓                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ _execute(task, timestamp)                                            │  │
│  │                                                                       │  │
│  │  第 431-436 行: pre_execute 钩子                                      │  │
│  │    └── 抛出 CancelExecution → 第 435 行: SIGNAL_CANCELED ◄──────────┼──┼─── 信号 6 (点1)
│  │                                                                       │  │
│  │  第 447-499 行: try-except 包裹任务执行                               │  │
│  │    ┌─────────────────────────────────────────────────────────────┐  │  │
│  │    │ 异常类型                        信号                          │  │  │
│  │    ├─────────────────────────────────────────────────────────────┤  │  │
│  │    │ TaskTimeout          → 第 459 行: SIGNAL_TIMEOUT ◄──────────┼──┼─── 信号 7
│  │    │ RateLimitExceeded    → 第 471 行: SIGNAL_RATE_LIMITED ◄────┼──┼─── 信号 8？不，是 12
│  │    │ TaskLockedException  → 第 475 行: SIGNAL_LOCKED ◄───────────┼──┼─── 信号 9？不，是 6
│  │    │ RetryTask            → (仅设置状态，无信号)                   │  │  │
│  │    │ CancelExecution      → 第 490 行: SIGNAL_CANCELED ◄─────────┼──┼─── 信号 6 (点2)
│  │    │ KeyboardInterrupt    → 第 494 行: SIGNAL_INTERRUPTED ◄──────┼──┼─── 信号 10 (点1)
│  │    │ 其他 Exception       → 第 499 行: SIGNAL_ERROR ◄─────────────┼──┼─── 信号 11 (带 exc)
│  │    └─────────────────────────────────────────────────────────────┘  │  │
│  │                                                                       │  │
│  │  第 508-513 行: 存储结果 (如果启用 results)                          │  │
│  │                                                                       │  │
│  │  第 515-516 行: post_execute 钩子                                     │  │
│  │                                                                       │  │
│  │  第 518-520 行: if exception is None                                  │  │
│  │    └── 是 → 第 520 行: SIGNAL_COMPLETE ◄───────────────────────────┼──┼─── 信号 2
│  │                                                                       │  │
│  │  第 522-529 行: 处理 pipeline (on_complete / on_error)               │  │
│  │    └── 可能触发 enqueue() → SIGNAL_ENQUEUED                          │  │
│  │                                                                       │  │
│  │  第 531-535 行: 处理 chord 回调                                       │  │
│  │    └── 可能触发 enqueue() → SIGNAL_ENQUEUED                          │  │
│  │                                                                       │  │
│  │  第 537-539 行: if exception and task.retries                         │  │
│  │    └── 是 → 第 538 行: SIGNAL_RETRYING ◄────────────────────────────┼──┼─── 信号 7？不，是 13
│  │              └── 第 539 行: _requeue_task()                          │  │
│  │                  ├── 有延迟 → add_schedule() → SIGNAL_SCHEDULED      │  │
│  │                  └── 无延迟 → enqueue() → SIGNAL_ENQUEUED            │  │
│  │                                                                       │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

信号编号对照表:
 1. SIGNAL_CANCELED      (第 435, 490 行)
 2. SIGNAL_COMPLETE      (第 520 行)
 3. SIGNAL_ERROR         (第 499 行)
 4. SIGNAL_EXECUTING     (第 427 行)
 5. SIGNAL_EXPIRED       (第 424 行)
 6. SIGNAL_LOCKED        (第 475 行)
 7. SIGNAL_RETRYING      (第 538 行)
 8. SIGNAL_REVOKED       (第 421 行)
 9. SIGNAL_SCHEDULED     (第 701 行)
10. SIGNAL_INTERRUPTED   (第 272, 494 行)
11. SIGNAL_ENQUEUED      (第 307 行)
12. SIGNAL_TIMEOUT       (第 459 行)
13. SIGNAL_RATE_LIMITED  (第 471 行)
```

---

## 3. 钩子机制详解

### 3.1 钩子类型定义

**来源文件**: `huey/api.py` 第 112-115 行

```python
class Huey(object):
    def __init__(self, ...):
        # ...
        self._pre_execute = OrderedDict()   # 第 112 行
        self._post_execute = OrderedDict()  # 第 113 行
        self._startup = OrderedDict()        # 第 114 行
        self._shutdown = OrderedDict()       # 第 115 行
```

### 3.2 钩子装饰器

| 钩子 | 装饰器 | 注册方法 | 注销方法 | 代码位置 |
|-----|-------|---------|---------|---------|
| pre_execute | `@huey.pre_execute()` | 第 221-225 行 | 第 227-231 行 | api.py |
| post_execute | `@huey.post_execute()` | 第 233-237 行 | 第 239-243 行 | api.py |
| on_startup | `@huey.on_startup()` | 第 245-249 行 | 第 251-255 行 | api.py |
| on_shutdown | `@huey.on_shutdown()` | 第 257-261 行 | 第 263-267 行 | api.py |

**注册示例**:
```python
@huey.pre_execute()
def my_pre_exec(task):
    print('about to execute:', task.id)

@huey.pre_execute(name='custom-name')
def another_hook(task):
    # 指定名称，便于注销
    pass

# 注销
huey.unregister_pre_execute('custom-name')
huey.unregister_pre_execute(my_pre_exec)  # 或传入函数本身
```

### 3.3 钩子触发时机

#### 钩子 1: pre_execute

**触发时机**: 任务实际执行前

**代码位置**: `huey/api.py` 第 431-436 行

```python
def _execute(self, task, timestamp):
    if self._pre_execute:  # 第 431 行
        try:
            self._run_pre_execute(task)  # 第 433 行
        except CancelExecution:
            self._emit(S.SIGNAL_CANCELED, task)  # 第 435 行
            return  # 直接返回，不执行任务
```

**`_run_pre_execute` 实现**: 第 575-587 行

```python
def _run_pre_execute(self, task):
    for name, callback in self._pre_execute.items():
        logger.debug('Pre-execute hook %s for %s.', name, task)
        try:
            callback(task)  # 第 579 行：调用钩子，传入 task
        except CancelExecution:
            logger.warning('Task %s cancelled by %s (pre-execute).',
                           task, name)
            raise  # 重新抛出，由上层处理
        except Exception:
            logger.exception('Unhandled exception calling pre-execute '
                             'hook %s for %s.', name, task)
```

**关键特性**:
- `pre_execute` 钩子可以通过抛出 `CancelExecution` 来**取消任务执行**
- 其他异常被记录但不影响任务执行

**钩子签名**: `callback(task)`

---

#### 钩子 2: post_execute

**触发时机**: 任务执行后（无论成功或失败）

**代码位置**: `huey/api.py` 第 515-516 行

```python
def _execute(self, task, timestamp):
    # ... 存储结果 ...
    if self._post_execute:  # 第 515 行
        self._run_post_execute(task, task_value, exception)  # 第 516 行
    # ... 然后才发射 SIGNAL_COMPLETE 或处理重试
```

**`_run_post_execute` 实现**: 第 588-596 行

```python
def _run_post_execute(self, task, task_value, exception):
    for name, callback in self._post_execute.items():
        logger.debug('Post-execute hook %s for %s.', name, task)
        try:
            callback(task, task_value, exception)  # 第 592 行
        except Exception as exc:
            logger.exception('Unhandled exception calling post-execute '
                             'hook %s for %s.', name, task)
```

**关键特性**:
- 总是被调用（无论任务成功或失败）
- 接收三个参数：`task`, `task_value`, `exception`
- `task_value` 是任务返回值（成功时）
- `exception` 是异常实例（失败时），成功时为 `None`

**执行时序**:
```
任务执行
    ↓
存储结果 (第 508-513 行)
    ↓
post_execute 钩子 (第 515-516 行)
    ↓
SIGNAL_COMPLETE (第 520 行) 或 重试逻辑 (第 537-539 行)
```

---

#### 钩子 3: on_startup

**触发时机**: Worker 进程/线程初始化时

**代码位置**: `huey/consumer.py` 第 101-108 行

```python
class Worker(BaseProcess):
    def initialize(self):  # 第 101 行
        for name, startup_hook in self.huey._startup.items():
            self._logger.debug('calling startup hook "%s"', name)
            try:
                startup_hook()  # 第 105 行：无参数
            except Exception as exc:
                self._logger.exception('startup hook "%s" failed', name)
```

**调用链**:
```
Consumer._create_process()
    → Worker.initialize()  (consumer.py 第 390 行)
        → on_startup 钩子
```

**典型用途**:
- 初始化数据库连接池
- 加载配置文件
- 建立外部服务连接

---

#### 钩子 4: on_shutdown

**触发时机**: Worker 进程/线程关闭时

**代码位置**: `huey/consumer.py` 第 109-115 行

```python
class Worker(BaseProcess):
    def shutdown(self):  # 第 109 行
        for name, shutdown_hook in self.huey._shutdown.items():
            self._logger.debug('calling shutdown hook "%s"', name)
            try:
                shutdown_hook()  # 第 113 行：无参数
            except Exception as exc:
                self._logger.exception('shutdown hook "%s" failed', name)
```

**调用链**:
```
Worker 主循环结束 (或收到退出信号)
    → Worker.shutdown()  (consumer.py 第 401 行，finally 块中)
        → on_shutdown 钩子
```

**典型用途**:
- 关闭数据库连接
- 清理临时文件
- 释放资源

### 3.4 钩子 vs 信号对比

| 特性 | 钩子 (Hooks) | 信号 (Signals) |
|-----|-------------|---------------|
| **注册方式** | 装饰器或手动 | 装饰器或手动 |
| **执行位置** | 嵌入执行流程 | 事件通知 |
| **能否中断流程** | `pre_execute` 可通过 `CancelExecution` 取消 | 不能（异常被忽略） |
| **参数** | 见下表 | `(signal, task, *args)` |
| **异常处理** | `pre_execute` 的 `CancelExecution` 会传播；其他被记录 | 所有异常被记录，不传播 |
| **调用顺序** | OrderedDict 按注册顺序 | 列表按注册顺序 |

**各钩子的参数**:
| 钩子 | 签名 | 参数说明 |
|-----|------|---------|
| `pre_execute` | `callback(task)` | 任务实例 |
| `post_execute` | `callback(task, task_value, exception)` | 任务、返回值、异常 |
| `on_startup` | `callback()` | 无参数 |
| `on_shutdown` | `callback()` | 无参数 |

**各信号的参数**:
| 信号 | 额外参数 | 说明 |
|-----|---------|------|
| 大多数信号 | 无 | 仅 `(signal, task)` |
| `SIGNAL_ERROR` | `exc` | 异常实例 |

### 3.5 完整执行时序（钩子 + 信号）

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    任务执行时序：钩子 + 信号                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Worker 启动                                                        │   │
│  │   └── on_startup 钩子 (consumer.py 第 101-108 行)                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 任务入队                                                          │   │
│  │   └── SIGNAL_ENQUEUED (api.py 第 307 行)                         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ execute() 前置检查                                                │   │
│  │   ├── eta 未到 → add_schedule() → SIGNAL_SCHEDULED               │   │
│  │   ├── 已撤销 → SIGNAL_REVOKED → 终止                              │   │
│  │   └── 已过期 → SIGNAL_EXPIRED → 终止                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ SIGNAL_EXECUTING (api.py 第 427 行)                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ pre_execute 钩子 (api.py 第 431-436 行)                          │   │
│  │   └── 抛出 CancelExecution → SIGNAL_CANCELED → 终止               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 任务实际执行 (api.py 第 451 行)                                    │   │
│  │   ├── 成功 → task_value                                            │   │
│  │   └── 异常 → 进入 except 分支                                      │   │
│  │       ├── TaskTimeout → SIGNAL_TIMEOUT                            │   │
│  │       ├── RateLimitExceeded → SIGNAL_RATE_LIMITED                │   │
│  │       ├── TaskLockedException → SIGNAL_LOCKED                     │   │
│  │       ├── RetryTask → (无信号)                                    │   │
│  │       ├── CancelExecution → SIGNAL_CANCELED                       │   │
│  │       ├── KeyboardInterrupt → SIGNAL_INTERRUPTED → 直接返回       │   │
│  │       └── 其他 Exception → SIGNAL_ERROR (带 exc)                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 存储结果 (api.py 第 508-513 行)                                   │   │
│  │   ├── 异常 → 存储 Error 对象                                       │   │
│  │   └── 成功 → 存储 task_value                                       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ post_execute 钩子 (api.py 第 515-516 行)                         │   │
│  │   └── callback(task, task_value, exception)                       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 成功完成?                                                         │   │
│  │   └── 是 → SIGNAL_COMPLETE (api.py 第 520 行)                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 处理 pipeline / chord (api.py 第 522-535 行)                     │   │
│  │   └── 可能触发 enqueue() → SIGNAL_ENQUEUED                        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ 需要重试? (api.py 第 537-539 行)                                  │   │
│  │   └── 是 → SIGNAL_RETRYING                                         │   │
│  │         └── 有延迟 → add_schedule() → SIGNAL_SCHEDULED            │   │
│  │         └── 无延迟 → enqueue() → SIGNAL_ENQUEUED                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                              ↓                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ Worker 关闭                                                        │   │
│  │   └── on_shutdown 钩子 (consumer.py 第 109-115 行)                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. 调度器与存储后端的交互

### 4.1 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        应用进程 (Application Process)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐                                                            │
│  │  用户代码    │  my_task(arg1, arg2)                                      │
│  └──────┬───────┘                                                            │
│         ↓                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                      Huey.enqueue(task)                                │  │
│  │                                                                       │  │
│  │  第 304-305 行: 解析过期时间                                           │  │
│  │  第 307 行: SIGNAL_ENQUEUED (仅应用进程的接收器可见)                   │  │
│  │  第 309-310 行: immediate 模式 → 直接 execute()                       │  │
│  │  第 312 行: 存储到队列                                                 │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                    ↓                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         存储后端 (Storage Backend)                     │  │
│  │                                                                       │  │
│  │  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐       │  │
│  │  │  执行队列      │    │  调度队列      │    │  结果存储      │       │  │
│  │  │ (Queue)       │    │ (Schedule)    │    │ (Result)      │       │  │
│  │  │               │    │               │    │               │       │  │
│  │  │ - enqueue()   │    │ - add_to_    │    │ - put_data()  │       │  │
│  │  │ - dequeue()   │    │   schedule()  │    │ - pop_data()  │       │  │
│  │  │               │    │ - read_      │    │               │       │  │
│  │  │               │    │   schedule()  │    │               │       │  │
│  │  └───────────────┘    └───────────────┘    └───────────────┘       │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ 任务数据通过存储后端传递
                                    │ 信号**不**通过存储后端传递
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Consumer 进程集群 (Consumer Processes)                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         主进程 (Main Process)                          │  │
│  │                                                                       │  │
│  │  ┌──────────────────┐    ┌──────────────────────────────────────┐   │  │
│  │  │   Scheduler      │    │         健康检查 / 信号处理            │   │  │
│  │  │   (调度器)        │    │                                    │   │  │
│  │  │                  │    │ - 检查 Worker 存活                      │   │  │
│  │  │ 1. 定期检查调度   │    │ - 处理 SIGTERM/SIGINT/SIGHUP          │   │  │
│  │  │    队列中的到期   │    │                                    │   │  │
│  │  │    任务           │    │                                    │   │  │
│  │  │                  │    │                                    │   │  │
│  │  │ 2. read_schedule │    │ 收到退出信号时:                        │   │  │
│  │  │    → 取出到期任务 │    │                                    │   │  │
│  │  │                  │    │ 1. 设置 stop_flag                     │   │  │
│  │  │ 3. enqueue(task) │    │ 2. graceful=True 时等待 Worker 完成   │   │  │
│  │  │    → SIGNAL_     │    │ 3. 调用 notify_interrupted_tasks()     │   │  │
│  │  │    ENQUEUED      │    │    → SIGNAL_INTERRUPTED                │   │  │
│  │  │    (主进程中)     │    │                                    │   │  │
│  │  │                  │    │                                    │   │  │
│  │  │ 4. 检查周期性任务 │    │                                    │   │  │
│  │  │    → enqueue()   │    │                                    │   │  │
│  │  │    → SIGNAL_     │    │                                    │   │  │
│  │  │    ENQUEUED      │    │                                    │   │  │
│  │  └──────────────────┘    └──────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                    │                                         │
│                                    │ 通过存储后端队列                        │
│                                    ↓                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    Worker 进程/线程 (执行任务)                         │  │
│  │                                                                       │  │
│  │  ┌────────────────────────────────────────────────────────────────┐ │  │
│  │  │                    Worker 生命周期                              │ │  │
│  │  │                                                               │ │  │
│  │  │  initialize()                                                  │ │  │
│  │  │   └── on_startup 钩子 (consumer.py 第 101-108 行)              │ │  │
│  │  │                                                               │ │  │
│  │  │  loop() 主循环:                                               │ │  │
│  │  │   ├── huey.dequeue() ←── 从存储后端获取任务                    │ │  │
│  │  │   └── huey.execute(task)                                       │ │  │
│  │  │         └── 各种信号 (仅 Worker 进程的接收器可见)               │ │  │
│  │  │                                                               │ │  │
│  │  │  shutdown()                                                    │ │  │
│  │  │   └── on_shutdown 钩子 (consumer.py 第 109-115 行)             │ │  │
│  │  └────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                       │  │
│  │  可以有多个 Worker 进程/线程并行执行                                  │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 关键设计原则

#### 原则 1: 信号是进程内的

**重要**: Huey 的信号系统**不跨进程传递**。

- 应用进程中发射的 `SIGNAL_ENQUEUED` 只能被应用进程中注册的接收器接收
- Consumer 进程中发射的所有信号只能被 Consumer 进程中注册的接收器接收
- 信号**不会**通过存储后端传递

**文档确认** (`docs/signals.rst`):
> `SIGNAL_ENQUEUED` - Emitted in both the **application process** (when your code calls a task) and the **consumer** (when re-enqueueing retries, periodic tasks, or scheduled tasks).

**含义**: 同一个信号常量 `SIGNAL_ENQUEUED` 会在两个不同的进程中独立发射，各自的接收器只能看到自己进程中的信号。

#### 原则 2: 存储与信号解耦

| 操作 | 存储操作 | 信号发射时机 |
|-----|---------|-------------|
| `enqueue()` | `storage.enqueue()` (第 312 行) | **之前**发射 `SIGNAL_ENQUEUED` (第 307 行) |
| `add_schedule()` | `storage.add_to_schedule()` (第 699 行) | **之后**发射 `SIGNAL_SCHEDULED` (第 701 行) |
| 成功完成 | `put_result()` (第 513 行) | **之后**发射 `SIGNAL_COMPLETE` (第 520 行) |
| 失败 | `put_result(Error(...))` (第 511 行) | **之前**发射 `SIGNAL_ERROR` (第 499 行) |

**关键时序**:
- `SIGNAL_ENQUEUED` 在存储**之前**发射
- `SIGNAL_COMPLETE` 在存储**之后**发射

这意味着在 `SIGNAL_COMPLETE` 处理器中可以安全读取结果:
```python
@huey.signal(SIGNAL_COMPLETE)
def on_complete(sig, task, *_):
    result = huey.result(task.id)  # 结果已存储，可以读取
```

#### 原则 3: 信号接收器异常隔离

**来源代码**: `huey/api.py` 第 283-288 行

```python
def _emit(self, signal, task, *args, **kwargs):
    try:
        self._signal.send(signal, task, *args, **kwargs)
    except Exception as exc:
        logger.exception('Error occurred sending signal "%s"', signal)
```

**含义**:
- 信号接收器抛出的异常只会被记录日志
- 不会中断任务执行
- 不会影响其他接收器

**文档确认** (`docs/signals.rst`):
> If a signal handler raises an exception, Huey **logs the exception** but continues processing. A broken signal handler will not prevent other signal handlers from running, nor will it prevent the task from being executed or its result from being stored.

### 4.3 调度器的工作流程

**来源代码**: `huey/consumer.py` 第 169-195 行 (Scheduler.loop)

```python
class Scheduler(BaseProcess):
    def loop(self, now=None):
        current = self._next_loop
        self._next_loop += self.interval
        
        # 1. 读取已到期的调度任务
        try:
            task_list = self.huey.read_schedule(now)  # 第 177 行
        except Exception:
            self._logger.exception('Error reading schedule.')
        else:
            for task in task_list:
                self._logger.debug('Enqueueing %s', task)
                self.huey.enqueue(task)  # 第 183 行 → 触发 SIGNAL_ENQUEUED

        # 2. 检查周期性任务
        if self.periodic and self._next_periodic <= time.monotonic():
            self._next_periodic += self.periodic_task_seconds
            self.enqueue_periodic_tasks(now)  # 第 187 行 → 内部调用 enqueue()

        self.sleep_for_interval(current, self.interval)
```

**`read_schedule` 实现**: `huey/api.py` 第 703-714 行

```python
def read_schedule(self, timestamp=None):
    if timestamp is None:
        timestamp = self._get_timestamp()
    accum = []
    for msg in self.storage.read_schedule(timestamp):  # 从存储后端读取
        try:
            task = self.deserialize_task(msg)
        except Exception:
            logger.exception('Unable to deserialize scheduled task.')
        else:
            accum.append(task)
    return accum
```

**调度器流程图**:
```
┌─────────────────────────────────────────────────────────────────┐
│                      Scheduler.loop()                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. storage.read_schedule(timestamp)                            │
│        ↓                                                        │
│     从调度队列取出所有 eta <= timestamp 的任务                   │
│        ↓                                                        │
│  2. 对每个到期任务: huey.enqueue(task)                          │
│        ↓                                                        │
│     ├── 第 307 行: SIGNAL_ENQUEUED (在调度器进程中发射)         │
│     └── 第 312 行: 存储到执行队列                                │
│        ↓                                                        │
│  3. 检查是否到了周期性任务检查时间                                │
│        ↓                                                        │
│  4. enqueue_periodic_tasks()                                    │
│        ↓                                                        │
│     对每个符合条件的周期性任务: huey.enqueue(task)              │
│        ↓                                                        │
│        └── SIGNAL_ENQUEUED                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.4 Worker 的工作流程

**来源代码**: `huey/consumer.py` 第 117-147 行 (Worker.loop)

```python
class Worker(BaseProcess):
    def loop(self, now=None):
        task = None
        try:
            task = self.huey.dequeue()  # 第 120 行：从存储后端取出
        except Exception:
            self._logger.exception('Error reading from queue')
            self.sleep()
        else:
            if task is not None:
                self.delay = self.default_delay
                try:
                    self.huey.execute(task, now)  # 第 128 行：执行任务
                except Exception as exc:
                    self._logger.exception('Unhandled error during execution '
                                           'of task %s.', task.id)
                finally:
                    self.task_count += 1
                    if self.max_tasks and self.task_count >= self.max_tasks:
                        raise WorkerRecycle()
            elif not self.huey.storage.blocking:
                self.sleep()
```

**`dequeue` 实现**: `huey/api.py` 第 373-376 行

```python
def dequeue(self):
    data = self.storage.dequeue()  # 从存储后端取出
    if data is not None:
        return self.deserialize_task(data)  # 反序列化
```

**Worker 流程图**:
```
┌─────────────────────────────────────────────────────────────────┐
│                        Worker.loop()                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. huey.dequeue()                                              │
│        ↓                                                        │
│     ├── storage.dequeue() ←── 从执行队列取出                    │
│     └── deserialize_task() ←── 反序列化为 Task 对象            │
│        ↓                                                        │
│  2. if task is not None:                                        │
│        ↓                                                        │
│     huey.execute(task, now)                                     │
│        ↓                                                        │
│     ┌─────────────────────────────────────────────────────┐   │
│     │  各种信号 (在 Worker 进程中发射)                      │   │
│     │                                                      │   │
│     │  - SIGNAL_EXECUTING    (第 427 行)                  │   │
│     │  - SIGNAL_COMPLETE      (第 520 行)                  │   │
│     │  - SIGNAL_ERROR          (第 499 行)                  │   │
│     │  - SIGNAL_RETRYING       (第 538 行)                  │   │
│     │  - SIGNAL_SCHEDULED      (第 701 行，重试有延迟时)    │   │
│     │  - SIGNAL_ENQUEUED       (第 307 行，重试无延迟时)    │   │
│     │  - ... 其他信号                                       │   │
│     └─────────────────────────────────────────────────────┘   │
│        ↓                                                        │
│  3. else: 队列为空                                              │
│        ↓                                                        │
│     self.sleep()  # 指数退避等待                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.5 重试时的数据流转

当任务需要重试时，数据和信号的流转如下：

**来源代码**: `huey/api.py` 第 537-539 行 和 第 562-573 行

```python
# 第 537-539 行
if exception is not None and task.retries:
    self._emit(S.SIGNAL_RETRYING, task)  # 先发射重试信号
    self._requeue_task(task, self._get_timestamp(), retry_eta)

# 第 562-573 行 (_requeue_task)
def _requeue_task(self, task, timestamp, retry_eta=None):
    task.retries -= 1
    logger.info('Requeueing %s, %s retries', task.id, task.retries)
    if retry_eta is not None:
        task.eta = retry_eta
        self.add_schedule(task)  # 第 567 行 → SIGNAL_SCHEDULED
    elif task.retry_delay:
        delay = datetime.timedelta(seconds=task.retry_delay)
        task.eta = timestamp + delay
        self.add_schedule(task)  # 第 571 行 → SIGNAL_SCHEDULED
    else:
        self.enqueue(task)  # 第 573 行 → SIGNAL_ENQUEUED
```

**重试流程图**:
```
┌─────────────────────────────────────────────────────────────────┐
│                      任务重试流程                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  任务执行异常                                                     │
│       ↓                                                         │
│  exception is not None                                           │
│       ↓                                                         │
│  task.retries > 0?                                               │
│       │                                                         │
│       ├── 否 → 结束 (不再重试)                                   │
│       │                                                         │
│       └── 是 → 第 538 行: SIGNAL_RETRYING                       │
│                  ↓                                              │
│            _requeue_task()                                      │
│                  ↓                                              │
│            有 retry_eta 或 retry_delay?                         │
│                  │                                              │
│                  ├── 是 → add_schedule() → SIGNAL_SCHEDULED    │
│                  │                                              │
│                  └── 否 → enqueue() → SIGNAL_ENQUEUED          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. 附录：完整代码引用表

### 5.1 信号定义表（共 13 个）

| 序号 | 信号常量 | 字符串值 | 定义位置 |
|-----|---------|---------|---------|
| 1 | `SIGNAL_CANCELED` | `'canceled'` | `huey/signals.py` 第 4 行 |
| 2 | `SIGNAL_COMPLETE` | `'complete'` | `huey/signals.py` 第 5 行 |
| 3 | `SIGNAL_ERROR` | `'error'` | `huey/signals.py` 第 6 行 |
| 4 | `SIGNAL_EXECUTING` | `'executing'` | `huey/signals.py` 第 7 行 |
| 5 | `SIGNAL_EXPIRED` | `'expired'` | `huey/signals.py` 第 8 行 |
| 6 | `SIGNAL_LOCKED` | `'locked'` | `huey/signals.py` 第 9 行 |
| 7 | `SIGNAL_RETRYING` | `'retrying'` | `huey/signals.py` 第 10 行 |
| 8 | `SIGNAL_REVOKED` | `'revoked'` | `huey/signals.py` 第 11 行 |
| 9 | `SIGNAL_SCHEDULED` | `'scheduled'` | `huey/signals.py` 第 12 行 |
| 10 | `SIGNAL_INTERRUPTED` | `'interrupted'` | `huey/signals.py` 第 13 行 |
| 11 | `SIGNAL_ENQUEUED` | `'enqueued'` | `huey/signals.py` 第 14 行 |
| 12 | `SIGNAL_TIMEOUT` | `'timeout'` | `huey/signals.py` 第 15 行 |
| 13 | `SIGNAL_RATE_LIMITED` | `'rate-limited'` | `huey/signals.py` 第 16 行 |

**重要修正**: `huey/signals.py` 第 4-16 行共定义 **13 个** 信号常量，不是 14 个。

### 5.2 信号触发位置表

| 信号 | 触发代码位置 | 触发条件 |
|-----|-------------|---------|
| `SIGNAL_ENQUEUED` | `huey/api.py` 第 307 行 | 任务入队时 |
| `SIGNAL_SCHEDULED` | `huey/api.py` 第 701 行 | 任务添加到调度队列时 |
| `SIGNAL_REVOKED` | `huey/api.py` 第 421 行 | 任务被撤销时 |
| `SIGNAL_EXPIRED` | `huey/api.py` 第 424 行 | 任务已过期时 |
| `SIGNAL_EXECUTING` | `huey/api.py` 第 427 行 | 任务即将执行时 |
| `SIGNAL_CANCELED` | `huey/api.py` 第 435 行 | pre_execute 钩子抛出 `CancelExecution` |
| `SIGNAL_CANCELED` | `huey/api.py` 第 490 行 | 任务函数抛出 `CancelExecution` |
| `SIGNAL_TIMEOUT` | `huey/api.py` 第 459 行 | 任务执行超时时 |
| `SIGNAL_RATE_LIMITED` | `huey/api.py` 第 471 行 | 任务被限流时 |
| `SIGNAL_LOCKED` | `huey/api.py` 第 475 行 | 任务无法获取锁时 |
| `SIGNAL_INTERRUPTED` | `huey/api.py` 第 494 行 | 任务执行中收到 `KeyboardInterrupt` |
| `SIGNAL_INTERRUPTED` | `huey/api.py` 第 272 行 | Consumer 关闭时通知进行中的任务 |
| `SIGNAL_ERROR` | `huey/api.py` 第 499 行 | 任务抛出其他未处理异常（带 `exc` 参数） |
| `SIGNAL_COMPLETE` | `huey/api.py` 第 520 行 | 任务成功完成时 |
| `SIGNAL_RETRYING` | `huey/api.py` 第 538 行 | 任务需要重试时 |

### 5.3 钩子定义与触发位置表

| 钩子 | 注册装饰器 | 触发位置 | 签名 |
|-----|-----------|---------|------|
| `pre_execute` | `@huey.pre_execute()` | `huey/api.py` 第 433 行 | `callback(task)` |
| `post_execute` | `@huey.post_execute()` | `huey/api.py` 第 516 行 | `callback(task, task_value, exception)` |
| `on_startup` | `@huey.on_startup()` | `huey/consumer.py` 第 105 行 | `callback()` |
| `on_shutdown` | `@huey.on_shutdown()` | `huey/consumer.py` 第 113 行 | `callback()` |

### 5.4 测试用例验证表

| 测试场景 | 测试文件位置 | 预期信号序列 |
|---------|-------------|-------------|
| 成功执行任务 | `test_signals.py` 第 22-30 行 | `ENQUEUED` → `EXECUTING` → `COMPLETE` |
| 任务失败重试 | `test_signals.py` 第 57-68 行 | `ENQUEUED` → `EXECUTING` → `ERROR` → `RETRYING` → `ENQUEUED` |
| 调度任务 | `test_signals.py` 第 32-35 行 | `ENQUEUED` → `SCHEDULED` |
| 撤销任务 | `test_signals.py` 第 80-94 行 | `ENQUEUED` → `REVOKED` |
| 过期任务 | `test_signals.py` 第 156-171 行 | `ENQUEUED` → `EXPIRED` |
| 超时任务 | `test_signals.py` 第 173-181 行 | `ENQUEUED` → `EXECUTING` → `TIMEOUT` |
| 被限流任务 | `test_signals.py` 第 113-154 行 | `ENQUEUED` → `EXECUTING` → `RATE_LIMITED` → `RETRYING` → `SCHEDULED` |
| 无法获取锁 | `test_signals.py` 第 96-111 行 | `ENQUEUED` → `EXECUTING` → `LOCKED` |

### 5.5 关键修正汇总

| 修正项 | 之前 | 修正后 | 依据 |
|-------|------|--------|------|
| 信号总数 | 14 个 | **13 个** | `huey/signals.py` 第 4-16 行共 13 行定义 |
| SIGNAL_ERROR 参数 | 未明确 | **带 `exc` 参数** | `huey/api.py` 第 499 行 `self._emit(S.SIGNAL_ERROR, task, exc)` |
| SIGNAL_CANCELED 触发点 | 1 处 | **2 处** | 第 435 行 (pre_execute) 和第 490 行 (任务函数) |
| SIGNAL_INTERRUPTED 触发点 | 1 处 | **2 处** | 第 272 行 (notify) 和第 494 行 (KeyboardInterrupt) |
| SIGNAL_ENQUEUED 发射时机 | 未明确 | **存储之前** | 第 307 行在第 312 行 `storage.enqueue()` 之前 |
| SIGNAL_COMPLETE 发射时机 | 未明确 | **存储之后** | 第 520 行在第 508-513 行结果存储之后 |

---

## 修订历史

| 版本 | 日期 | 修订内容 |
|-----|------|---------|
| v2 | 2026-05-02 | 重新核对代码，修正信号总数为 13 个，补充所有信号和钩子的代码位置引用，修正多个事实错误 |
