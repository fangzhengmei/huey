# Huey 信号（Signal）与钩子（Hook）机制深度分析

## 目录
1. [信号系统概述](#信号系统概述)
2. [任务生命周期与信号触发](#任务生命周期与信号触发)
3. [钩子机制详解](#钩子机制详解)
4. [信号流转架构](#信号流转架构)
5. [附录：完整信号触发代码位置](#附录完整信号触发代码位置)

---

## 信号系统概述

### 1.1 信号定义

Huey 的信号系统在 `huey/signals.py` 中定义，共包含 **14 种信号类型**：

| 信号常量 | 字符串值 | 触发时机 |
|---------|---------|---------|
| `SIGNAL_ENQUEUED` | `'enqueued'` | 任务被放入队列时 |
| `SIGNAL_EXECUTING` | `'executing'` | 任务即将被 Worker 执行时 |
| `SIGNAL_COMPLETE` | `'complete'` | 任务成功执行完成时 |
| `SIGNAL_ERROR` | `'error'` | 任务执行期间抛出未处理异常时 |
| `SIGNAL_CANCELED` | `'canceled'` | 任务被取消（pre_execute 钩子或任务自身抛出 `CancelExecution`） |
| `SIGNAL_RETRYING` | `'retrying'` | 任务失败但将重试时 |
| `SIGNAL_SCHEDULED` | `'scheduled'` | 任务被添加到调度队列（延迟执行）时 |
| `SIGNAL_REVOKED` | `'revoked'` | 任务被撤销，不再执行 |
| `SIGNAL_EXPIRED` | `'expired'` | 任务过期时间已过，不再执行 |
| `SIGNAL_LOCKED` | `'locked'` | 任务无法获取锁时 |
| `SIGNAL_TIMEOUT` | `'timeout'` | 任务执行超时时 |
| `SIGNAL_RATE_LIMITED` | `'rate-limited'` | 任务被限流时 |
| `SIGNAL_INTERRUPTED` | `'interrupted'` | Consumer 被关闭时任务仍在执行中 |

### 1.2 Signal 类实现

信号系统的核心是 `Signal` 类，其设计简洁高效：

```python
class Signal(object):
    __slots__ = ('receivers',)

    def __init__(self):
        self.receivers = {'any': []}  # 'any' 通道接收所有信号

    def connect(self, receiver, *signals):
        if not signals:
            signals = ('any',)
        for signal in signals:
            self.receivers.setdefault(signal, [])
            self.receivers[signal].append(receiver)

    def disconnect(self, receiver, *signals):
        # 从指定信号通道移除接收器

    def send(self, signal, task, *args, **kwargs):
        # 链式调用：指定信号的接收者 + 'any' 通道的接收者
        receivers = itertools.chain(self.receivers.get(signal, ()),
                                    self.receivers['any'])
        for receiver in receivers:
            receiver(signal, task, *args, **kwargs)
```

**设计要点**：
- 使用 `__slots__` 优化内存占用
- `'any'` 特殊通道：注册到 `'any'` 的接收器会接收所有信号
- `send` 方法同时调用指定信号和 `'any'` 通道的接收者
- 信号处理器异常会被捕获并记录日志，**不会中断任务执行**

### 1.3 信号注册方式

在 `Huey` 类中通过 `_signal` 属性持有 `Signal` 实例：

```python
# huey/api.py:117
self._signal = S.Signal()
```

**装饰器方式注册**：
```python
@huey.signal()  # 不指定信号 → 注册到 'any' 通道
def all_signal_handler(signal, task, exc=None):
    print('%s - %s' % (signal, task.id))

@huey.signal(SIGNAL_ERROR, SIGNAL_LOCKED)  # 注册多个特定信号
def error_handler(signal, task, exc=None):
    pass
```

**手动注册/注销**：
```python
huey.disconnect_signal(handler)           # 从所有信号注销
huey.disconnect_signal(handler, SIGNAL_ERROR)  # 从指定信号注销
```

---

## 任务生命周期与信号触发

### 2.1 核心执行流程

任务的完整生命周期在 `huey/api.py` 的 `execute()` 和 `_execute()` 方法中处理：

```
┌─────────────────────────────────────────────────────────────────┐
│                      Huey.execute() 入口                         │
├─────────────────────────────────────────────────────────────────┤
│  1. 检查任务是否 ready_to_run()                                   │
│     ├── 否 → add_schedule() → SIGNAL_SCHEDULED                   │
│     └── 是 → 继续                                                  │
│                                                                  │
│  2. 检查任务是否被撤销 (is_revoked)                               │
│     └── 是 → SIGNAL_REVOKED → 终止                               │
│                                                                  │
│  3. 检查任务是否过期 (expires_resolved < timestamp)              │
│     └── 是 → SIGNAL_EXPIRED → 终止                               │
│                                                                  │
│  4. 发出 SIGNAL_EXECUTING                                         │
│                                                                  │
│  5. 调用 _execute() 实际执行任务                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 各阶段信号触发详解

#### 阶段一：任务入队 (Enqueue)

**触发信号**：`SIGNAL_ENQUEUED`

**代码位置**：`huey/api.py:297-325`

```python
def enqueue(self, task):
    # ... 处理 group 和 chord ...
    
    # 解析过期时间
    if task.expires:
        task.resolve_expires(self.utc)

    # 发出入队信号
    self._emit(S.SIGNAL_ENQUEUED, task)  # 第 307 行

    if self._immediate:
        self.execute(task)
    else:
        # 序列化任务并存入存储后端
        self.storage.enqueue(self.serialize_task(task), task.priority)
    # ...
```

**关键观察**：
- `SIGNAL_ENQUEUED` 在**存储之前**发出
- 这是**唯一可能在应用进程**中触发的信号（其他信号主要在 Consumer 进程中）
- 触发场景包括：
  - 应用代码调用任务函数：`my_task()`
  - Consumer 重试任务时重新入队
  - 调度器将定时任务从调度队列移到执行队列
  - Chord 的回调任务入队

#### 阶段二：任务调度 (Scheduled)

**触发信号**：`SIGNAL_SCHEDULED`

**代码位置**：`huey/api.py:696-701`

```python
def add_schedule(self, task):
    data = self.serialize_task(task)
    eta = task.eta or datetime.datetime.fromtimestamp(0)
    self.storage.add_to_schedule(data, eta)  # 存入调度存储
    logger.info('Added task %s to schedule, eta %s', task.id, eta)
    self._emit(S.SIGNAL_SCHEDULED, task)  # 第 701 行
```

**触发时机**：
1. 任务有 `eta` 参数（指定执行时间）
2. 任务有 `delay` 参数（延迟执行）
3. 任务重试时设置了 `retry_delay`
4. 任务被限流时设置了重试延迟

**调度器的角色**：
```python
# huey/consumer.py:169-195 (Scheduler.loop)
def loop(self, now=None):
    # 1. 读取已到期的调度任务
    task_list = self.huey.read_schedule(now)
    
    # 2. 将到期任务重新入队（触发 SIGNAL_ENQUEUED）
    for task in task_list:
        self.huey.enqueue(task)  # 这里会再次触发 SIGNAL_ENQUEUED!
    
    # 3. 处理周期性任务
    if self.periodic:
        self.enqueue_periodic_tasks(now)
```

#### 阶段三：任务执行前检查

在 `execute()` 方法中，执行前有三个检查点：

| 检查项 | 失败时信号 | 代码位置 |
|-------|-----------|---------|
| `ready_to_run()` (eta 未到) | `SIGNAL_SCHEDULED` | api.py:417-418 |
| `is_revoked()` | `SIGNAL_REVOKED` | api.py:419-421 |
| `expires_resolved < timestamp` | `SIGNAL_EXPIRED` | api.py:422-424 |

**撤销检查的细节**：
```python
# api.py:671-694 (is_revoked)
def is_revoked(self, task, timestamp=None, peek=True):
    # 支持多种撤销方式：
    # 1. 按任务实例撤销 (revoke_id = 'r:{task_id}')
    # 2. 按任务类撤销 (所有该类型任务)
    # 3. 撤销直到指定时间 (revoke_until)
    # 4. 仅撤销一次 (revoke_once)
```

#### 阶段四：任务执行中 (Executing)

**触发信号**：`SIGNAL_EXECUTING`

**代码位置**：`huey/api.py:427`

```python
def execute(self, task, timestamp=None):
    # ... 前置检查 ...
    else:
        logger.info('Executing %s', task)
        self._emit(S.SIGNAL_EXECUTING, task)  # 第 427 行
        return self._execute(task, timestamp)
```

**关键观察**：
- `SIGNAL_EXECUTING` 发出后立即调用 `_execute()`
- 此时任务已经被 Worker 从队列中取出

#### 阶段五：任务执行内部 (`_execute`)

这是最复杂的阶段，包含多个异常处理分支：

```
┌─────────────────────────────────────────────────────────────────┐
│                    Huey._execute() 核心逻辑                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 1. 运行 pre_execute 钩子                                  │   │
│  │    └── 抛出 CancelExecution → SIGNAL_CANCELED → 返回     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 2. 任务执行（try-except 包裹）                            │   │
│  │                                                           │   │
│  │ 异常类型                      → 触发信号                   │   │
│  │ ┌─────────────────────────────────────────────────────┐  │   │
│  │ │ TaskTimeout              → SIGNAL_TIMEOUT           │  │   │
│  │ │ RateLimitExceeded        → SIGNAL_RATE_LIMITED      │  │   │
│  │ │ TaskLockedException      → SIGNAL_LOCKED            │  │   │
│  │ │ RetryTask                → (记录, 准备重试)          │  │   │
│  │ │ CancelExecution          → SIGNAL_CANCELED           │  │   │
│  │ │ KeyboardInterrupt        → SIGNAL_INTERRUPTED        │  │   │
│  │ │ 其他 Exception            → SIGNAL_ERROR (带 exc)     │  │   │
│  │ └─────────────────────────────────────────────────────┘  │   │
│  │                                                           │   │
│  │ 无异常 → 继续后续流程                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 3. 存储结果（如果启用 results）                            │   │
│  │    - 异常 → 存储 Error 对象                                │   │
│  │    - 成功 → 存储返回值                                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 4. 运行 post_execute 钩子                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 5. 成功完成 → SIGNAL_COMPLETE                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 6. 处理 pipeline (on_complete / on_error)                │   │
│  │    - 成功且有 on_complete → 入队下一个任务                │   │
│  │    - 失败且有 on_error → 入队错误处理任务                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 7. 处理 Chord 回调                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              ↓                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 8. 需要重试 → SIGNAL_RETRYING → 重新入队/调度            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### 阶段六：任务完成/失败后的信号

| 场景 | 信号 | 额外参数 | 触发条件 |
|-----|------|---------|---------|
| 成功完成 | `SIGNAL_COMPLETE` | 无 | 无异常且结果已存储 |
| 执行异常 | `SIGNAL_ERROR` | `exc` | 任务抛出未处理异常 |
| 需要重试 | `SIGNAL_RETRYING` | 无 | 有 `retries` 剩余次数 |
| 超时时 | `SIGNAL_TIMEOUT` | 无 | 超过 `timeout` 时间 |
| 被限流 | `SIGNAL_RATE_LIMITED` | 无 | 触发 `RateLimitExceeded` |
| 无法获取锁 | `SIGNAL_LOCKED` | 无 | 触发 `TaskLockedException` |
| 被取消 | `SIGNAL_CANCELED` | 无 | 触发 `CancelExecution` |
| 被中断 | `SIGNAL_INTERRUPTED` | 无 | Consumer 收到 `SIGTERM` |

### 2.3 典型场景的信号序列

根据 `docs/signals.rst` 和测试代码，以下是典型场景的信号顺序：

#### 场景 1：成功执行的任务

```
1. SIGNAL_ENQUEUED    (应用进程中，任务入队)
2. SIGNAL_EXECUTING   (Consumer 中，Worker 开始执行)
3. SIGNAL_COMPLETE    (任务成功完成)
4. [如有 on_complete] → SIGNAL_ENQUEUED (下一个任务入队)
```

**测试验证** (`test_signals.py:22-30`):
```python
def test_signals_simple(self):
    @self.huey.task()
    def task_a(n):
        return n + 1
    
    r = task_a(3)
    self.assertSignals([SIGNAL_ENQUEUED])  # 入队时
    
    self.assertEqual(self.execute_next(), 4)
    self.assertSignals([SIGNAL_EXECUTING, SIGNAL_COMPLETE])  # 执行时
```

#### 场景 2：任务失败并重试

```
1. SIGNAL_ENQUEUED
2. SIGNAL_EXECUTING
3. SIGNAL_ERROR      (异常发生，exc 作为参数)
4. SIGNAL_RETRYING   (决定重试)
5. [有 retry_delay]  → SIGNAL_SCHEDULED
   [无 retry_delay]  → SIGNAL_ENQUEUED (立即重试)
   
   (重试执行...)
6. SIGNAL_EXECUTING  (第二次执行)
7. SIGNAL_COMPLETE 或 SIGNAL_ERROR
```

**测试验证** (`test_signals.py:57-68`):
```python
def test_signals_on_retry(self):
    @self.huey.task(retries=1)  # 1 次重试
    def task_a(n):
        return n + 1  # 当 n=None 时会抛出 TypeError
    
    r = task_a(None)
    self.assertSignals([SIGNAL_ENQUEUED])
    
    self.assertTrue(self.execute_next() is None)
    self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR, 
                        SIGNAL_RETRYING, SIGNAL_ENQUEUED])  # 重试时重新入队
    
    # 第二次执行（重试用完）
    self.assertTrue(self.execute_next() is None)
    self.assertSignals([SIGNAL_EXECUTING, SIGNAL_ERROR])  # 没有重试了
```

#### 场景 3：调度任务（延迟执行）

```
1. SIGNAL_ENQUEUED      (应用进程，初次入队)
2. SIGNAL_SCHEDULED     (Worker 发现 eta 未到，加入调度)
   (等待调度器...)
3. SIGNAL_ENQUEUED      (调度器中，重新入队 - Consumer 进程!)
4. SIGNAL_EXECUTING
5. SIGNAL_COMPLETE
```

#### 场景 4：被撤销的任务

```
1. SIGNAL_ENQUEUED
2. SIGNAL_REVOKED   (不再发出其他信号)
```

#### 场景 5：限流任务

```
1. SIGNAL_ENQUEUED
2. SIGNAL_EXECUTING
3. SIGNAL_RATE_LIMITED
4. [允许重试] → SIGNAL_RETRYING → SIGNAL_SCHEDULED
   [不允许重试] → 结束
```

### 2.4 特殊信号：SIGNAL_INTERRUPTED

这个信号有两个触发点：

1. **任务执行中收到 KeyboardInterrupt** (`api.py:492-495`):
```python
except KeyboardInterrupt:
    logger.warning('Received exit signal, %s did not finish.', task.id)
    self._emit(S.SIGNAL_INTERRUPTED, task)
    return  # 直接返回，不触发其他信号
```

2. **Consumer 关闭时通知所有进行中的任务** (`api.py:269-272`):
```python
def notify_interrupted_tasks(self):
    while self._tasks_in_flight:
        task = self._tasks_in_flight.pop()
        self._emit(S.SIGNAL_INTERRUPTED, task)
```

**设计用途**：文档中建议用此信号实现任务重新入队：
```python
@huey.signal(SIGNAL_INTERRUPTED)
def on_interrupted(signal, task, *args, **kwargs):
    # Consumer 被关闭前任务未完成，重新入队
    huey.enqueue(task)
```

---

## 钩子机制详解

Huey 提供了四种生命周期钩子，与信号系统相互补充：

### 3.1 钩子类型概览

| 钩子装饰器 | 存储位置 | 触发时机 | 签名 |
|-----------|---------|---------|------|
| `@huey.pre_execute()` | `_pre_execute` (OrderedDict) | 任务执行前 | `callback(task)` |
| `@huey.post_execute()` | `_post_execute` (OrderedDict) | 任务执行后（无论成功失败） | `callback(task, task_value, exception)` |
| `@huey.on_startup()` | `_startup` (OrderedDict) | Worker 进程初始化时 | `callback()` |
| `@huey.on_shutdown()` | `_shutdown` (OrderedDict) | Worker 进程关闭时 | `callback()` |

### 3.2 Pre/Post Execute 钩子

这些钩子在 `_execute()` 方法中被调用：

```python
# api.py:430-436 (pre_execute)
def _execute(self, task, timestamp):
    if self._pre_execute:
        try:
            self._run_pre_execute(task)
        except CancelExecution:
            self._emit(S.SIGNAL_CANCELED, task)  # 钩子可以取消任务!
            return
    # ...

# api.py:515-516 (post_execute)
if self._post_execute:
    self._run_post_execute(task, task_value, exception)
```

**关键特性**：
- `pre_execute` 钩子可以通过抛出 `CancelExecution` 来**取消任务执行**
- `post_execute` 钩子总是会被调用（无论任务成功或失败）
- 钩子异常会被记录日志，但**不会中断任务流程**

**执行顺序**：
```
pre_execute 钩子
    ↓
任务实际执行
    ↓
存储结果
    ↓
post_execute 钩子
    ↓
SIGNAL_COMPLETE 或 其他结束信号
```

### 3.3 Startup/Shutdown 钩子

这些钩子在 `Worker` 类的生命周期中调用：

```python
# consumer.py:101-107 (Worker.initialize)
def initialize(self):
    for name, startup_hook in self.huey._startup.items():
        self._logger.debug('calling startup hook "%s"', name)
        try:
            startup_hook()
        except Exception as exc:
            self._logger.exception('startup hook "%s" failed', name)

# consumer.py:109-115 (Worker.shutdown)
def shutdown(self):
    for name, shutdown_hook in self.huey._shutdown.items():
        self._logger.debug('calling shutdown hook "%s"', name)
        try:
            shutdown_hook()
        except Exception as exc:
            self._logger.exception('shutdown hook "%s" failed', name)
```

**触发时机**：
- `initialize()` 在 Worker 进程/线程启动时调用一次
- `shutdown()` 在 Worker 进程/线程退出时调用一次

**典型用途**：
- 初始化数据库连接池
- 加载配置文件
- 清理资源
- 关闭连接

### 3.4 钩子 vs 信号：对比与选择

| 特性 | 钩子 (Hooks) | 信号 (Signals) |
|-----|-------------|---------------|
| **执行时机** | 嵌入执行流程 | 事件通知 |
| **能否中断流程** | pre_execute 可以通过 `CancelExecution` 取消 | 不能（信号处理器异常被忽略） |
| **传递信息** | task, task_value, exception | signal, task, *args |
| **注册方式** | 装饰器或手动 | 装饰器或手动 |
| **典型用途** | 资源管理、任务拦截 | 监控、日志、通知 |

---

## 信号流转架构

### 4.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          应用进程 (Application Process)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐                                                            │
│  │  用户代码调用  │  my_task(arg1, arg2)                                      │
│  └──────┬───────┘                                                            │
│         ↓                                                                    │
│  ┌──────────────┐     SIGNAL_ENQUEUED     ┌─────────────────┐              │
│  │ Huey.enqueue │ ─────────────────────→  │ Signal.send()   │              │
│  │              │                          │ (同步调用所有接收器) │              │
│  └──────┬───────┘                          └─────────────────┘              │
│         ↓                                                                    │
│  ┌──────────────┐                                                            │
│  │ 存储后端      │  storage.enqueue(serialized_task)                         │
│  │ (Redis等)    │                                                            │
│  └──────────────┘                                                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ 任务数据通过存储后端传递
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Consumer 进程 (主进程 + Worker 子进程)                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                           主进程 (Main Process)                         │  │
│  │                                                                       │  │
│  │  ┌──────────────┐      ┌──────────────┐      ┌──────────────────┐   │  │
│  │  │  Scheduler   │      │  HealthCheck │      │  Signal Handler  │   │  │
│  │  │ (调度器)      │      │ (健康检查)    │      │ (处理 SIGTERM等) │   │  │
│  │  │              │      │              │      │                  │   │  │
│  │  │ 定期检查调度  │      │ 监控 Worker   │      │ 触发 stop()     │   │  │
│  │  │ 队列，到期    │      │ 存活状态      │      │                  │   │  │
│  │  │ 任务重新入队  │      │              │      │                  │   │  │
│  │  │              │      │              │      │                  │   │  │
│  │  │ enqueue(task)│─────→│ 触发 SIGNAL  │      │                  │   │  │
│  │  │ (在主进程)   │      │ _ENQUEUED    │      │                  │   │  │
│  │  └──────────────┘      └──────────────┘      └──────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                    │                                          │
│                                    │ 通过存储后端队列                          │
│                                    ↓                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         Worker 进程/线程 (执行任务)                      │  │
│  │                                                                       │  │
│  │  ┌────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    Worker.loop() 主循环                          │  │  │
│  │  │                                                                 │  │  │
│  │  │  1. dequeue() ←── 从存储后端获取任务                            │  │  │
│  │  │       ↓                                                         │  │  │
│  │  │  2. huey.execute(task)                                          │  │  │
│  │  │       ↓                                                         │  │  │
│  │  │  ┌──────────────────────────────────────────────────────────┐ │  │  │
│  │  │  │              Huey.execute() / _execute()                  │ │  │  │
│  │  │  │                                                           │ │  │  │
│  │  │  │  信号发射点 (通过 _emit() → Signal.send())                │ │  │  │
│  │  │  │  ┌────────────────────────────────────────────────────┐  │ │  │  │
│  │  │  │  │ SIGNAL_EXECUTING  (执行前)                          │  │ │  │  │
│  │  │  │  │ SIGNAL_COMPLETE    (成功后)                          │  │ │  │  │
│  │  │  │  │ SIGNAL_ERROR       (异常时，带 exc)                  │  │ │  │  │
│  │  │  │  │ SIGNAL_RETRYING    (重试前)                          │  │ │  │  │
│  │  │  │  │ SIGNAL_LOCKED      (无法获取锁)                       │  │ │  │  │
│  │  │  │  │ SIGNAL_TIMEOUT     (超时)                             │  │ │  │  │
│  │  │  │  │ SIGNAL_RATE_LIMITED (被限流)                         │  │ │  │  │
│  │  │  │  │ SIGNAL_CANCELED    (被钩子取消)                       │  │ │  │  │
│  │  │  │  │ SIGNAL_INTERRUPTED (被中断)                          │  │ │  │  │
│  │  │  │  │ SIGNAL_SCHEDULED    (加入调度)                        │  │ │  │  │
│  │  │  │  │ SIGNAL_REVOKED      (被撤销)                          │  │ │  │  │
│  │  │  │  │ SIGNAL_EXPIRED      (已过期)                          │  │ │  │  │
│  │  │  │  └────────────────────────────────────────────────────┘  │ │  │  │
│  │  │  │                                                           │ │  │  │
│  │  │  │  存储交互:                                                 │ │  │  │
│  │  │  │  - put_result() → 存储结果                                │ │  │  │
│  │  │  │  - add_schedule() → 存储到调度队列                        │ │  │  │
│  │  │  │  - enqueue() → 重试时重新入队                             │ │  │  │
│  │  │  └──────────────────────────────────────────────────────────┘ │  │  │
│  │  └────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                       │  │
│  │  ┌────────────────────────────────────────────────────────────────┐  │  │
│  │  │              Worker 初始化/关闭 (Startup/Shutdown)              │  │  │
│  │  │                                                                 │  │  │
│  │  │  initialize() → 调用所有 on_startup 钩子                        │  │  │
│  │  │  shutdown()   → 调用所有 on_shutdown 钩子                       │  │  │
│  │  └────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 信号发射的核心机制：`_emit` 方法

所有信号都通过 `Huey._emit()` 方法发射：

```python
# api.py:283-288
def _emit(self, signal, task, *args, **kwargs):
    try:
        self._signal.send(signal, task, *args, **kwargs)
    except Exception as exc:
        logger.exception('Error occurred sending signal "%s"', signal)
```

**关键设计决策**：
1. **异常隔离**：信号接收器的异常被捕获并记录，**不会影响任务执行**
2. **同步调用**：`send()` 是同步的，接收器在当前线程/进程中执行
3. **无返回值**：信号是"发射后不管"的通知机制

### 4.3 信号与存储后端的交互

信号系统和存储后端是**完全解耦**的两个系统：

| 操作 | 存储后端交互 | 信号发射 |
|-----|-------------|---------|
| 入队 | `storage.enqueue()` | **之前**发射 `SIGNAL_ENQUEUED` |
| 调度 | `storage.add_to_schedule()` | **之后**发射 `SIGNAL_SCHEDULED` |
| 存储结果 | `storage.put_data()` | **之后**才发射 `SIGNAL_COMPLETE` |
| 重试入队 | `storage.enqueue()` 或 `add_to_schedule()` | 触发 `SIGNAL_RETRYING` 后进行 |

**重要时序**：
- `SIGNAL_ENQUEUED` 在存储**之前**发射
- `SIGNAL_COMPLETE` 在结果存储**之后**发射
- `SIGNAL_ERROR` 在错误结果存储**之后**发射（见 `api.py:508-513`）

```python
# api.py:508-520
if self.results and not isinstance(task, PeriodicTask):
    if exception is not None:
        error_data = self.build_error_result(task, exception)
        self.put_result(task.id, Error(error_data))  # 先存储错误结果
    elif task_value is not None or self.store_none:
        self.put_result(task.id, task_value)  # 先存储成功结果

if self._post_execute:
    self._run_post_execute(task, task_value, exception)

if exception is None:
    self._emit(S.SIGNAL_COMPLETE, task)  # 后发射信号
```

这意味着在 `SIGNAL_COMPLETE` 处理器中，任务结果已经可以从存储中读取：

```python
@huey.signal(SIGNAL_COMPLETE)
def on_complete(sig, task, *_):
    result = huey.result(task.id)  # 可以安全地读取结果
    # ...
```

### 4.4 跨进程信号考量

**重要**：Huey 的信号系统是**进程内**的：

1. **应用进程**中发射的 `SIGNAL_ENQUEUED` 只在应用进程的接收器中触发
2. **Consumer 进程**中发射的所有信号只在 Consumer 进程的接收器中触发
3. 信号**不会**通过存储后端传递

**这意味着**：
- 如果在应用进程中注册信号处理器，它只能收到 `SIGNAL_ENQUEUED`（和 immediate 模式下的其他信号）
- 如果要接收所有信号，信号处理器必须在 Consumer 进程中注册

**文档说明**：
> `SIGNAL_ENQUEUED` - Emitted in both the **application process** (when your code calls a task) and the **consumer** (when re-enqueueing retries, periodic tasks, or scheduled tasks).

### 4.5 Immediate 模式的特殊处理

当 `huey.immediate = True` 时：

```python
# api.py:309-310
if self._immediate:
    self.execute(task)  # 同步执行，不经过存储
else:
    self.storage.enqueue(...)
```

此时：
- 所有信号都在**同一进程**中发射
- 任务同步执行
- 不经过存储后端（除非 `immediate_use_memory=False`）

这使得测试信号处理器变得简单：

```python
huey.immediate = True

state = []
@huey.signal(SIGNAL_COMPLETE)
def on_complete(signal, task):
    state.append(task.id)

result = add(1, 2)  # 立即执行，发射信号
assert len(state) == 1
assert state[0] == result.id
```

---

## 附录：完整信号触发代码位置

以下是所有信号在 `huey/api.py` 中的精确触发位置：

| 信号 | 代码行 | 触发条件 |
|-----|-------|---------|
| `SIGNAL_ENQUEUED` | 307 | `enqueue()` 方法中，任务入队时 |
| `SIGNAL_EXECUTING` | 427 | `execute()` 方法中，任务即将执行时 |
| `SIGNAL_REVOKED` | 421 | 任务被撤销时 |
| `SIGNAL_EXPIRED` | 424 | 任务已过期时 |
| `SIGNAL_CANCELED` | 435 | `pre_execute` 钩子抛出 `CancelExecution` |
| `SIGNAL_CANCELED` | 490 | 任务自身抛出 `CancelExecution` |
| `SIGNAL_TIMEOUT` | 459 | 任务执行超时时 |
| `SIGNAL_RATE_LIMITED` | 471 | 任务被限流时 |
| `SIGNAL_LOCKED` | 475 | 任务无法获取锁时 |
| `SIGNAL_INTERRUPTED` | 494 | 执行中收到 `KeyboardInterrupt` |
| `SIGNAL_ERROR` | 499 | 任务抛出其他未处理异常 |
| `SIGNAL_COMPLETE` | 520 | 任务成功完成 |
| `SIGNAL_RETRYING` | 538 | 任务需要重试时 |
| `SIGNAL_SCHEDULED` | 701 | `add_schedule()` 方法中 |
| `SIGNAL_INTERRUPTED` | 272 | `notify_interrupted_tasks()` 中 |

---

## 总结

### 核心设计原则

1. **信号是通知机制**：不能中断任务流程，异常被隔离
2. **钩子是拦截机制**：`pre_execute` 可以取消任务
3. **存储与信号解耦**：信号不经过存储，纯进程内通信
4. **时序保证**：`SIGNAL_COMPLETE` 保证结果已存储，`SIGNAL_ENQUEUED` 在存储前发射

### 最佳实践

1. **监控/日志**：使用信号系统
2. **资源管理**：使用 `on_startup`/`on_shutdown` 钩子
3. **任务拦截**：使用 `pre_execute` 钩子 + `CancelExecution`
4. **结果后处理**：使用 `post_execute` 钩子或 `SIGNAL_COMPLETE` 信号
5. **测试信号处理器**：使用 `immediate=True` 模式

### 与存储后端的关系

```
任务数据 (通过存储后端传递)
    ↓
应用进程入队 → [存储队列] → Worker 取出执行
                    ↓
信号 (进程内，不通过存储)
    ↓
应用进程 SIGNAL_ENQUEUED (仅应用进程接收器可见)
Consumer 进程各种信号 (仅 Consumer 进程接收器可见)
```

理解这种分离对于正确设计信号处理器至关重要。
