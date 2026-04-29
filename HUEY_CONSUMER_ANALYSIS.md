# Huey 消费者进程调度机制分析报告

> 分析版本：基于 `huey/consumer.py`、`huey/api.py`、`huey/utils.py`、`huey/registry.py`

---

## 目录

1. [三种 Worker 模式的启动流程与任务交接](#1-三种-worker-模式的启动流程与任务交接)
2. [Crontab 周期任务触发机制](#2-crontab-周期任务触发机制)
3. [停止信号处理与优雅退出](#3-停止信号处理与优雅退出)

---

## 1. 三种 Worker 模式的启动流程与任务交接

### 1.1 统一的 Environment 抽象层

Huey 通过 `Environment` 基类定义了统一的并发环境接口，为三种不同的并发模型提供了一致的抽象：

```
┌─────────────────────────────────────────────────────────────┐
│                    Environment (基类接口)                     │
├─────────────────────────────────────────────────────────────┤
│  get_stop_flag()      → 返回事件对象用于信号同步              │
│  create_process()     → 创建执行单元 (线程/进程/协程)         │
│  is_alive()           → 检查执行单元是否存活                   │
│  set_timeout_handler()→ 设置超时处理函数                       │
└─────────────────────────────────────────────────────────────┘
         ▲                    ▲                    ▲
         │                    │                    │
┌────────┴────────┐  ┌────────┴────────┐  ┌────────┴────────┐
│ ThreadEnvironment│ │GreenletEnvironment│ │ProcessEnvironment │
│  (线程模式)       │ │   (协程模式)       │ │   (进程模式)       │
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│threading.Event  │  │  GreenEvent     │  │  ProcessEvent   │
│threading.Thread │  │  Greenlet       │  │  Process        │
│thread_timeout   │  │greenlet_timeout │  │process_timeout  │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

**核心实现位置**：`huey/consumer.py:198-266`

#### 三种 Environment 的详细对比

| 特性 | ThreadEnvironment | GreenletEnvironment | ProcessEnvironment |
|------|-------------------|---------------------|---------------------|
| **停止标志** | `threading.Event()` | `GreenEvent()` (gevent) | `ProcessEvent()` (multiprocessing) |
| **执行单元** | `threading.Thread` | `gevent.Greenlet` | `multiprocessing.Process` |
| **超时处理** | `thread_timeout` (空实现，依赖协作式检查) | `greenlet_timeout` (gevent.Timeout) | `process_timeout` (SIGALRM 信号) |
| **daemon 标记** | 是 (True) | - | 是 (True) |
| **内存共享** | 共享内存空间 | 共享内存空间 | 独立内存空间 |

### 1.2 Consumer 的初始化与启动流程

#### 初始化阶段 (`Consumer.__init__`)

```
┌──────────────────────────────────────────────────────────────┐
│                    Consumer 初始化流程                         │
├──────────────────────────────────────────────────────────────┤
│  1. 验证配置参数                                               │
│     ├── 检查 immediate 模式                                    │
│     ├── 验证 scheduler_interval 必须是 60 的因数               │
│     └── 验证 worker_type 合法性                                │
│                                                               │
│  2. 创建 Environment 实例                                      │
│     └── self.environment = WORKER_TO_ENVIRONMENT[worker_type]│
│                                                               │
│  3. 安装环境特定的超时处理器                                    │
│     └── self.environment.set_timeout_handler(self.huey)      │
│                                                               │
│  4. 创建停止标志事件                                           │
│     └── self.stop_flag = self.environment.get_stop_flag()    │
│                                                               │
│  5. 预创建 Scheduler 和 Workers (未启动)                       │
│     ├── self.scheduler = self._create_process(scheduler)     │
│     └── self.worker_threads = [(worker, process), ...]       │
└──────────────────────────────────────────────────────────────┘
```

**代码位置**：`huey/consumer.py:269-402`

#### 启动阶段 (`Consumer.start`)

```python
def start(self):
    # 1. 检查 gevent monkey-patch (如果是 greenlet 模式)
    if self.worker_type == WORKER_GREENLET:
        if not monkey.is_module_patched('socket'):
            self._logger.warning('Gevent monkey-patch has not been applied')
    
    # 2. 启动 Scheduler
    self.scheduler.start()
    
    # 3. 启动所有 Worker 进程
    for _, worker_process in self.worker_threads:
        worker_process.start()
    
    # 4. 为主进程注册信号处理器
    self._set_signal_handlers()
```

**代码位置**：`huey/consumer.py:404-442`

### 1.3 Worker 类的核心循环

#### Worker 的生命周期

```
┌──────────────────────────────────────────────────────────────┐
│                      Worker 生命周期                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐           │
│   │initialize│────▶│   loop   │────▶│ shutdown │           │
│   └──────────┘     └────┬─────┘     └──────────┘           │
│                          │                                     │
│                          │ 循环直到 stop_flag.is_set()         │
│                          ▼                                     │
│                   ┌──────────────┐                             │
│                   │ 从队列获取任务 │                             │
│                   │huey.dequeue()│                             │
│                   └──────┬───────┘                             │
│                          │                                       │
│              ┌───────────┴───────────┐                        │
│              │                       │                        │
│         task ≠ None             task is None                  │
│              │                       │                        │
│              ▼                       ▼                        │
│    ┌────────────────┐       ┌────────────────┐               │
│    │ 执行任务        │       │ 退避睡眠        │               │
│    │huey.execute()  │       │self.sleep()    │               │
│    └────────────────┘       └────────────────┘               │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

#### Worker.loop 的详细实现

```python
def loop(self, now=None):
    task = None
    try:
        # 1. 从存储层获取任务
        task = self.huey.dequeue()
    except Exception:
        self._logger.exception('Error reading from queue')
        self.sleep()
    else:
        if task is not None:
            # 2. 有任务，重置退避延迟
            self.delay = self.default_delay
            try:
                # 3. 执行任务
                self.huey.execute(task, now)
            except Exception as exc:
                self._logger.exception('Unhandled error during execution of task %s.', task.id)
            finally:
                # 4. 检查是否需要回收 Worker (max_tasks 限制)
                self.task_count += 1
                if self.max_tasks and self.task_count >= self.max_tasks:
                    self._logger.info('Worker reached max tasks (%d), exiting.', self.max_tasks)
                    raise WorkerRecycle()
        elif not self.huey.storage.blocking:
            # 5. 队列为空且非阻塞存储，执行退避睡眠
            self.sleep()
```

**代码位置**：`huey/consumer.py:117-147`

#### 退避策略

当队列为空时，Worker 使用指数退避策略减少轮询频率：

```python
def sleep(self):
    # 限制最大延迟
    if self.delay > self.max_delay:
        self.delay = self.max_delay
    
    time.sleep(self.delay)
    
    # 指数增长：delay *= backoff
    self.delay *= self.backoff
```

**典型配置**：
- `default_delay` = 0.1 秒 (初始轮询间隔)
- `max_delay` = 10.0 秒 (最大轮询间隔)
- `backoff` = 1.15 (退避因子)

### 1.4 Scheduler 类的调度逻辑

#### Scheduler 的职责

Scheduler 负责两类任务的调度：

1. **定时任务 (Scheduled Tasks)**：有明确 `eta` (预计执行时间) 的任务
2. **周期性任务 (Periodic Tasks)**：按 crontab 表达式重复执行的任务

#### Scheduler.loop 的详细实现

```python
def loop(self, now=None):
    current = self._next_loop
    self._next_loop += self.interval
    
    # 防跳跃检查：如果当前时间已经超过下一次调度时间，跳过本次
    if self._next_loop < time.monotonic():
        self._logger.debug('scheduler skipping iteration to avoid race.')
        return
    
    try:
        # 1. 读取调度队列中到期的任务
        task_list = self.huey.read_schedule(now)
    except Exception:
        self._logger.exception('Error reading schedule.')
    else:
        # 2. 将到期任务加入执行队列
        for task in task_list:
            self._logger.debug('Enqueueing %s', task)
            self.huey.enqueue(task)
    
    # 3. 检查是否需要处理周期性任务 (每 60 秒)
    if self.periodic and self._next_periodic <= time.monotonic():
        self._next_periodic += self.periodic_task_seconds  # += 60
        self.enqueue_periodic_tasks(now)
    
    # 4. 精确睡眠到下一次调度时间
    self.sleep_for_interval(current, self.interval)
```

**代码位置**：`huey/consumer.py:169-189`

### 1.5 任务交接机制

#### 任务流全景图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           任务流全景图                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────┐                    ┌──────────────┐                       │
│  │ Producer │                    │  Storage     │                       │
│  │ (生产者) │                    │ (存储层)     │                       │
│  └────┬─────┘                    └──────┬───────┘                       │
│       │                                   │                               │
│       │ 1. huey.enqueue(task)            │                               │
│       │                                   │                               │
│       ▼                                   │                               │
│  ┌────────────────────────────────────────┴──────────────────────────┐  │
│  │                         任务存储                                    │  │
│  ├─────────────────────────────────────────────────────────────────────┤  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌──────────────────────┐  │  │
│  │  │  执行队列   │    │  调度队列   │    │  周期性任务注册       │  │  │
│  │  │ (Queue)    │    │(Schedule)   │    │  (_periodic_tasks)   │  │  │
│  │  └──────┬──────┘    └──────┬──────┘    └──────────┬───────────┘  │  │
│  └─────────┼──────────────────┼────────────────────────┼──────────────┘  │
│            │                  │                        │                  │
│            │                  │                        │                  │
│            │ 2. dequeue()     │ 3. read_schedule()    │ 4. read_periodic()│
│            │                  │                        │                  │
│            ▼                  ▼                        ▼                  │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                         消费者进程                                    │  │
│  ├─────────────────────────────────────────────────────────────────────┤  │
│  │  ┌──────────────────────────────────────────────────────────────┐  │  │
│  │  │                      Scheduler (调度器)                        │  │  │
│  │  │  ┌───────────────┐      ┌────────────────────────────────┐  │  │  │
│  │  │  │ 处理调度队列  │      │ 处理周期性任务 (每60秒)         │  │  │  │
│  │  │  │ read_schedule │─────▶│  read_periodic → validate_datetime│  │  │  │
│  │  │  └───────────────┘      └────────────────────────────────┘  │  │  │
│  │  │                            │                                  │  │  │
│  │  │                            ▼                                  │  │  │
│  │  │                   ┌──────────────────┐                       │  │  │
│  │  │                   │ huey.enqueue()   │                       │  │  │
│  │  │                   │ (重新入队到执行队列)│                       │  │  │
│  │  │                   └────────┬─────────┘                       │  │  │
│  │  └────────────────────────────┼──────────────────────────────────┘  │  │
│  │                               │                                     │  │
│  └───────────────────────────────┼─────────────────────────────────────┘  │
│                                  │                                          │
│                                  ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                      Workers (工作节点)                                │  │
│  ├─────────────────────────────────────────────────────────────────────┤  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │  │
│  │  │  Worker-1   │  │  Worker-2   │  │  Worker-N   │                │  │
│  │  ├─────────────┤  ├─────────────┤  ├─────────────┤                │  │
│  │  │dequeue()    │  │dequeue()    │  │dequeue()    │                │  │
│  │  │execute()    │  │execute()    │  │execute()    │                │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 任务交接的关键接口

| 接口 | 调用者 | 功能 | 代码位置 |
|------|--------|------|----------|
| `huey.enqueue(task)` | Producer / Scheduler | 将任务序列化后加入执行队列 | `huey/api.py:297-325` |
| `huey.dequeue()` | Worker | 从执行队列取出并反序列化任务 | `huey/api.py:373-377` |
| `huey.add_schedule(task)` | execute() (当 task.eta > now) | 将任务加入调度队列 | `huey/api.py:696-701` |
| `huey.read_schedule(timestamp)` | Scheduler | 从调度队列读取到期的任务 | `huey/api.py:703-714` |
| `huey.read_periodic(timestamp)` | Scheduler | 筛选符合当前时间的周期性任务 | `huey/api.py:716-720` |
| `huey.execute(task, timestamp)` | Worker | 执行任务 (包含撤销/过期检查) | `huey/api.py:413-541` |

---

## 2. Crontab 周期任务触发机制

### 2.1 crontab 函数的设计与实现

#### 功能概述

`crontab()` 函数将类 Unix crontab 风格的参数转换为一个验证函数，该函数接收一个 `datetime` 对象，返回布尔值表示该时间是否匹配 crontab 表达式。

**代码位置**：`huey/api.py:1343-1429`

#### 支持的语法

| 语法 | 示例 | 含义 |
|------|------|------|
| `*` | `minute='*'` | 每一分钟 |
| `*/n` | `hour='*/4'` | 每 4 小时 (0, 4, 8, 12, 16, 20) |
| `m-n` | `day='1-15'` | 每月 1 号到 15 号 |
| `m,n` | `day_of_week='1,3,5'` | 周一、周三、周五 |
| 组合 | `minute='0,30'`, `hour='9-17'` | 工作日的 9:00-17:00 每半小时 |

#### 核心实现解析

```python
def crontab(minute='*', hour='*', day='*', month='*', day_of_week='*', strict=False):
    # 定义验证顺序和各自的有效范围
    validation = (
        ('m', month, range(1, 13)),      # 月份: 1-12
        ('d', day, range(1, 32)),         # 日期: 1-31
        ('w', day_of_week, range(8)),     # 星期: 0-6, 7 也表示周日
        ('H', hour, range(24)),            # 小时: 0-23
        ('M', minute, range(60))           # 分钟: 0-59
    )
    
    cron_settings = []
    
    # 解析每个时间分量，生成允许值的集合
    for (date_str, value, acceptable) in validation:
        settings = set([])
        
        if isinstance(value, int):
            value = str(value)
        
        # 支持逗号分隔的多个值
        for piece in value.split(','):
            if piece == '*':
                # 通配符：包含所有有效值
                settings.update(acceptable)
                continue
            
            if piece.isdigit():
                # 单个数值
                piece = int(piece)
                if piece not in acceptable:
                    raise ValueError('%d is not a valid input' % piece)
                elif date_str == 'w':
                    # 星期特殊处理：7 也表示周日 (0)
                    piece %= 7
                settings.add(piece)
                continue
            
            # 范围匹配: m-n
            dash_match = dash_re.match(piece)
            if dash_match:
                lhs, rhs = map(int, dash_match.groups())
                if lhs not in acceptable or rhs not in acceptable:
                    raise ValueError('%s is not a valid input' % piece)
                elif date_str == 'w':
                    lhs %= 7
                    rhs %= 7
                settings.update(range(lhs, rhs + 1))
                continue
            
            # 间隔匹配: */n
            every_match = every_re.match(piece)
            if every_match:
                if date_str == 'w':
                    raise ValueError('Cannot perform this kind of matching on day-of-week.')
                interval = int(every_match.groups()[0])
                # 从第一个元素开始，每隔 interval 个取一个
                settings.update(acceptable[::interval])
                continue
            
            # 无法解析的格式
            if strict:
                raise ValueError('%s is not a valid input' % piece)
        
        cron_settings.append(sorted(list(settings)))
    
    # 返回验证闭包
    def validate_date(timestamp):
        # 从时间戳提取各分量
        # timetuple() 返回: (year, month, day, hour, minute, second, weekday, ...)
        _, m, d, H, M, _, w, _, _ = timestamp.timetuple()
        
        # 修正 weekday：Python weekday() 中 0=周一, 6=周日
        # 但 crontab 约定 0=周日, 6=周六
        # 所以 (w + 1) % 7 进行转换
        w = (w + 1) % 7
        
        # 依次检查每个分量是否在允许集合中
        # 检查顺序: month, day, weekday, hour, minute
        for (date_piece, selection) in zip((m, d, w, H, M), cron_settings):
            if date_piece not in selection:
                return False
        
        return True
    
    return validate_date
```

#### 正则表达式解析

```python
# 用于匹配范围表达式如 "9-17"
dash_re = re.compile(r'(\d+)-(\d+)')

# 用于匹配间隔表达式如 "*/4"
every_re = re.compile(r'\*\/(\d+)')
```

**代码位置**：`huey/api.py:1339-1340`

#### 便利封装

```python
# 每小时 (分钟=0)
crontab.hourly = partial(crontab, minute='0')

# 每天 (分钟=0, 小时=0)
crontab.daily = partial(crontab, minute='0', hour='0')
```

**代码位置**：`huey/api.py:1433-1435`

### 2.2 周期性任务的注册流程

#### 使用示例

```python
from huey import crontab

@huey.periodic_task(crontab(minute='0', hour='2'))
def nightly_report():
    """每天凌晨 2:00 执行"""
    generate_nightly_report()

@huey.periodic_task(crontab(day_of_week='1,3,5', hour='9', minute='0'))
def weekday_morning_task():
    """周一、周三、周五早上 9:00 执行"""
    pass
```

#### 注册机制详解

```python
def periodic_task(self, validate_datetime, retries=0, retry_delay=0,
                  priority=None, context=False, name=None, expires=None,
                  timeout=None, **kwargs):
    TaskWrapper = self.task_wrapper_class
    
    def decorator(func):
        # 创建 validate_datetime 方法，绑定到任务类
        def method_validate(self, timestamp):
            return validate_datetime(timestamp)
        
        # 创建 TaskWrapper，指定：
        # 1. task_base = PeriodicTask (而非普通 Task)
        # 2. validate_datetime = method_validate
        return TaskWrapper(
            self,
            func.func if isinstance(func, TaskWrapper) else func,
            context=context,
            name=name,
            default_retries=retries,
            default_retry_delay=retry_delay,
            default_priority=priority,
            default_expires=expires,
            default_timeout=timeout,
            validate_datetime=method_validate,  # 关键：添加时间验证方法
            task_base=PeriodicTask,              # 关键：使用 PeriodicTask 基类
            **kwargs)
    
    return decorator
```

**代码位置**：`huey/api.py:183-205`

#### PeriodicTask 基类

```python
class PeriodicTask(Task):
    """周期性任务基类，添加 validate_datetime 方法"""
    
    def validate_datetime(self, timestamp):
        """默认返回 False，实际实现由装饰器注入"""
        return False
```

**代码位置**：`huey/api.py:929-931`

#### Registry 中的注册

当 `TaskWrapper` 被创建时，它会自动将任务类注册到 `Registry`：

```python
class TaskWrapper(object):
    def __init__(self, huey, func, ..., **settings):
        # ...
        # 动态创建任务类
        self.task_class = self.create_task(func, context, name, **settings)
        # 注册到 huey 的注册表
        self.huey._registry.register(self.task_class)
```

**代码位置**：`huey/api.py:934-952`

```python
class Registry(object):
    def register(self, task_class):
        task_str = self.task_to_string(task_class)
        
        # 检查重复注册
        if task_str in self._registry:
            raise ValueError('Attempting to register a task with the same '
                             'identifier as existing task...')
        
        self._registry[task_str] = task_class
        
        # 关键：如果有 validate_datetime 方法，加入周期性任务列表
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        
        return True
```

**代码位置**：`huey/registry.py:26-36`

### 2.3 周期性任务的调度派发

#### 调度时序图

```
时间轴 (秒) ──────────────────────────────────────────────────────────▶

┌──────────────────────────────────────────────────────────────────────┐
│                     Scheduler 调度循环                                 │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  T=0    _next_loop = 0, _next_periodic = 0                          │
│         ┌─────────────┐                                               │
│         │ loop() 执行 │                                               │
│         ├─────────────┤                                               │
│         │ 1. read_schedule()                                          │
│         │    → 处理到期的定时任务                                     │
│         │                                                             │
│         │ 2. 检查周期性任务: _next_periodic (0) <= now (0)? ✓       │
│         │    ├── _next_periodic += 60 → 变为 60                     │
│         │    └── enqueue_periodic_tasks(now)                        │
│         │        ├── read_periodic(now)                              │
│         │        │   └── 对每个 PeriodicTask 调用 validate_datetime()│
│         │        │       └── 匹配的任务入队                          │
│         │        └── huey.enqueue(task)                              │
│         │                                                             │
│         │ 3. sleep_for_interval(0, scheduler_interval=1)            │
│         │    → 睡眠约 1 秒                                            │
│         └─────────────┘                                               │
│                                                                       │
│  T=1    _next_loop = 1, _next_periodic = 60                         │
│         ┌─────────────┐                                               │
│         │ loop() 执行 │                                               │
│         ├─────────────┤                                               │
│         │ 1. read_schedule()                                          │
│         │                                                             │
│         │ 2. 检查周期性任务: _next_periodic (60) <= now (1)? ✗      │
│         │    → 跳过，不执行周期性任务检查                              │
│         │                                                             │
│         │ 3. sleep_for_interval(1, 1)                                │
│         └─────────────┘                                               │
│                                                                       │
│  ... (重复 58 次) ...                                                 │
│                                                                       │
│  T=59   _next_loop = 59, _next_periodic = 60                        │
│         ┌─────────────┐                                               │
│         │ loop() 执行 │                                               │
│         ├─────────────┤                                               │
│         │ 1. read_schedule()                                          │
│         │                                                             │
│         │ 2. 检查周期性任务: _next_periodic (60) <= now (59)? ✗     │
│         │    → 跳过                                                   │
│         │                                                             │
│         │ 3. sleep_for_interval(59, 1)                               │
│         └─────────────┘                                               │
│                                                                       │
│  T=60   _next_loop = 60, _next_periodic = 60                        │
│         ┌─────────────┐                                               │
│         │ loop() 执行 │                                               │
│         ├─────────────┤                                               │
│         │ 1. read_schedule()                                          │
│         │                                                             │
│         │ 2. 检查周期性任务: _next_periodic (60) <= now (60)? ✓      │
│         │    ├── _next_periodic += 60 → 变为 120                    │
│         │    └── enqueue_periodic_tasks(now)  ← 再次检查!           │
│         │                                                             │
│         │ 3. sleep_for_interval(60, 1)                               │
│         └─────────────┘                                               │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

#### 关键代码详解

##### Scheduler.loop 中的周期性任务检查

```python
def loop(self, now=None):
    current = self._next_loop
    self._next_loop += self.interval  # self.interval 默认为 1 秒
    
    # ... 处理定时任务 (read_schedule) ...
    
    # 检查是否需要处理周期性任务
    # _next_periodic 初始化为 time.monotonic()
    # periodic_task_seconds = 60 (硬编码常量)
    if self.periodic and self._next_periodic <= time.monotonic():
        # 更新下一次检查时间
        self._next_periodic += self.periodic_task_seconds  # += 60
        
        # 执行周期性任务检查和入队
        self.enqueue_periodic_tasks(now)
    
    # ...
```

**代码位置**：`huey/consumer.py:169-189`

##### enqueue_periodic_tasks 方法

```python
def enqueue_periodic_tasks(self, now):
    self._logger.debug('Checking periodic tasks')
    
    # 调用 huey.read_periodic 获取符合当前时间的任务
    for task in self.huey.read_periodic(now):
        self._logger.info('Enqueueing periodic task %s.', task)
        # 将任务加入执行队列
        self.huey.enqueue(task)
```

**代码位置**：`huey/consumer.py:191-195`

##### read_periodic 方法

```python
def read_periodic(self, timestamp):
    if timestamp is None:
        timestamp = self._get_timestamp()
    
    # 遍历所有注册的周期性任务
    # _registry.periodic_tasks 会实例化每个任务类
    return [task for task in self._registry.periodic_tasks
            if task.validate_datetime(timestamp)]  # 关键：调用验证函数
```

**代码位置**：`huey/api.py:716-720`

##### Registry.periodic_tasks 属性

```python
@property
def periodic_tasks(self):
    # 将任务类实例化为任务对象
    return [task_class() for task_class in self._periodic_tasks]
```

**代码位置**：`huey/registry.py:127-129`

### 2.4 调度时间的精确控制

#### sleep_for_interval 方法

Scheduler 使用 `sleep_for_interval` 方法来保证调度的时间精度：

```python
def sleep_for_interval(self, start_ts, nseconds):
    """
    基于开始时间戳精确睡眠指定间隔
    
    例如：start_ts=1337, nseconds=10
          如果当前时间是 1340，实际只睡眠 7 秒 (1340+7=1347=1337+10)
    """
    # 计算需要睡眠的时间
    sleep_time = nseconds - (time.monotonic() - start_ts)
    
    if sleep_time <= 0:
        return  # 时间已过，不睡眠
    
    self._logger.debug('Sleeping for %s', sleep_time)
    
    # 重新计算以提高精度 (防止日志记录导致的延迟)
    sleep_time = nseconds - (time.monotonic() - start_ts)
    
    if sleep_time > 0:
        time.sleep(sleep_time)
```

**代码位置**：`huey/consumer.py:48-65`

#### 防跳跃机制

Scheduler 包含一个重要的防跳跃保护：

```python
def loop(self, now=None):
    current = self._next_loop
    self._next_loop += self.interval
    
    # 关键：如果当前时间已经超过了下一次调度时间，说明上一次
    # 循环执行时间过长，跳过本次调度以避免累积延迟
    if self._next_loop < time.monotonic():
        self._logger.debug('scheduler skipping iteration to avoid race.')
        return
    
    # ... 继续执行调度逻辑 ...
```

**代码位置**：`huey/consumer.py:169-174`

### 2.5 周期任务触发机制总结

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    周期任务触发机制总览                                    │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      注册阶段 (应用启动时)                            │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  @huey.periodic_task(crontab(minute='0', hour='2'))                │ │
│  │  def my_task(): ...                                                  │ │
│  │                                                                       │ │
│  │  1. crontab() 返回 validate_date 闭包                               │ │
│  │  2. periodic_task() 装饰器创建 PeriodicTask 子类                    │ │
│  │  3. TaskWrapper 将任务类注册到 Registry._periodic_tasks             │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      调度阶段 (运行时)                                │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │                                                                       │ │
│  │  Scheduler 主循环 (每秒执行一次):                                     │ │
│  │  ┌──────────────────────────────────────────────────────────────┐  │ │
│  │  │ 1. _next_loop += interval                                      │  │ │
│  │  │ 2. 检查 _next_loop < now? 是则跳过 (防跳跃)                    │  │ │
│  │  │ 3. read_schedule() → 处理定时任务                               │  │ │
│  │  │ 4. 检查 _next_periodic <= now?                                 │  │ │
│  │  │    ├── 否: 跳过                                                │  │ │
│  │  │    └── 是:                                                     │  │ │
│  │  │         ├── _next_periodic += 60                              │  │ │
│  │  │         └── enqueue_periodic_tasks(now)                       │  │ │
│  │  │              └── read_periodic(now)                            │  │ │
│  │  │                   └── [t for t in periodic_tasks              │  │ │
│  │  │                        if t.validate_datetime(now)]            │  │ │
│  │  │                             ↓                                   │  │ │
│  │  │              └── huey.enqueue(task) ← 匹配的任务入队执行       │  │ │
│  │  └──────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      执行阶段                                        │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  Worker 从执行队列 dequeue() 获取任务，调用 huey.execute() 执行    │ │
│  │  注意：周期性任务每次执行都是新的任务实例，状态不保留                  │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 停止信号处理与优雅退出

### 3.1 信号处理架构

Huey 的信号处理设计需要考虑以下因素：

1. **不同并发模型的差异**：线程、进程、协程对信号的处理方式不同
2. **父子进程的协调**：进程模式下，子进程需要特殊处理
3. **退出类型区分**：
   - **优雅退出 (Graceful)**：等待当前任务完成
   - **立即退出 (Immediate)**：中断当前任务
   - **重启 (Restart)**：优雅退出后重启进程

#### 信号映射表

| 信号 | 处理函数 | 退出类型 | 行为描述 |
|------|----------|----------|----------|
| `SIGTERM` | `_handle_stop_signal` | 立即退出 | 设置 `_graceful=False`，不等待任务 |
| `SIGINT` (Ctrl+C) | 根据 worker 类型 | 优雅退出 (多数情况) | 等待当前任务完成 |
| `SIGHUP` | `_handle_restart_signal` | 优雅退出 + 重启 | 退出后重新执行自身 |

### 3.2 主进程信号处理器

#### 信号处理器安装

```python
def _set_signal_handlers(self):
    # SIGTERM: 终止信号，总是非优雅退出
    signal.signal(signal.SIGTERM, self._handle_stop_signal)
    
    # SIGINT: 中断信号 (Ctrl+C)，处理方式取决于 worker 类型
    if self.worker_type in (WORKER_GREENLET, WORKER_THREAD):
        # Greenlet/Thread 模式：使用自定义处理器
        # 原因：
        # 1. gevent 默认会在非主 hub 的 greenlet 中抛出 KeyboardInterrupt
        # 2. 线程模式确保 SIGHUP 后 SIGINT 能正确响应
        signal.signal(signal.SIGINT, self._handle_interrupt_signal_gevent)
    else:
        # Process 模式：使用默认处理器
        # 默认行为是抛出 KeyboardInterrupt
        signal.signal(signal.SIGINT, signal.default_int_handler)
    
    # SIGHUP: 挂起信号，用于重启
    if hasattr(signal, 'SIGHUP'):  # Windows 可能没有
        signal.signal(signal.SIGHUP, self._handle_restart_signal)
```

**代码位置**：`huey/consumer.py:549-564`

#### 各信号处理函数详解

##### _handle_stop_signal (SIGTERM)

```python
def _handle_stop_signal(self, sig_num, frame):
    self._logger.info('Received SIGTERM')
    
    # 标记已收到信号
    self._received_signal = True
    # 不重启
    self._restart = False
    # 非优雅退出：不等待任务完成
    self._graceful = False
    
    # Greenlet 模式特殊处理：发送 KeyboardInterrupt 到所有 worker
    if self.worker_type == WORKER_GREENLET:
        def kill_workers():
            gevent.killall([t for _, t in self.worker_threads],
                           KeyboardInterrupt)
        gevent.spawn(kill_workers)
```

**代码位置**：`huey/consumer.py:572-581`

##### _handle_interrupt_signal_gevent (SIGINT for Thread/Greenlet)

```python
def _handle_interrupt_signal_gevent(self, sig_num, frame):
    self._logger.info('Received SIGINT')
    
    self._received_signal = True
    self._restart = False
    self._graceful = True  # 优雅退出
    
    # 恢复默认处理器：再次 Ctrl+C 将立即终止
    signal.signal(signal.SIGINT, signal.default_int_handler)
```

**代码位置**：`huey/consumer.py:565-570`

##### _handle_restart_signal (SIGHUP)

```python
def _handle_restart_signal(self, sig_num, frame):
    self._logger.info('Received SIGHUP, will restart')
    
    self._received_signal = True
    self._restart = True        # 需要重启
    self._graceful = True       # 优雅退出：等待任务完成
```

**代码位置**：`huey/consumer.py:583-587`

### 3.3 子进程信号处理 (Process 模式)

在 `worker_type=WORKER_PROCESS` 模式下，子进程有独立的信号处理策略：

#### 子进程信号处理器安装

```python
def _set_child_signal_handlers(self):
    """
    在子进程中安装信号处理器
    
    策略：
    - 忽略 SIGHUP (重启信号由主进程处理)
    - 忽略 SIGINT (中断信号由主进程处理)
    - 收到 SIGTERM 时抛出 KeyboardInterrupt，由循环捕获
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)   # 忽略
    signal.signal(signal.SIGTERM, self._handle_stop_signal_worker)
    
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)  # 忽略
```

**代码位置**：`huey/consumer.py:589-599`

#### 子进程 SIGTERM 处理

```python
def _handle_stop_signal_worker(self, sig_num, frame):
    """
    子进程收到 SIGTERM 时的处理
    
    抛出 KeyboardInterrupt，这将：
    1. 被 _create_process 中的 try/except 捕获
    2. 或者被 huey.execute() 中的 try/except 捕获
    """
    raise KeyboardInterrupt
```

**代码位置**：`huey/consumer.py:600-602`

#### 子进程信号处理流程图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    Process 模式父子进程信号处理                            │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                         主进程 (Consumer)                             │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  信号处理器：                                                          │ │
│  │  ├── SIGTERM → _handle_stop_signal (立即退出)                        │ │
│  │  ├── SIGINT  → default_int_handler (抛出 KeyboardInterrupt)          │ │
│  │  └── SIGHUP  → _handle_restart_signal (优雅退出+重启)                │ │
│  │                                                                       │ │
│  │  行为：收到信号后设置 stop_flag，然后 stop(graceful=?)                │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    │                                      │
│                                    │ stop_flag.set()                      │
│                                    │ + worker_process.join() (优雅模式)   │
│                                    ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                      子进程 (Worker/Scheduler)                        │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │  信号处理器：                                                          │ │
│  │  ├── SIGINT  → SIG_IGN (忽略，不响应 Ctrl+C)                         │ │
│  │  ├── SIGHUP  → SIG_IGN (忽略，不响应重启)                            │ │
│  │  └── SIGTERM → _handle_stop_signal_worker (抛出 KeyboardInterrupt)   │ │
│  │                                                                       │ │
│  │  行为：                                                                │ │
│  │  1. 检查 stop_flag.is_set() (由主进程通过共享内存设置)                 │ │
│  │  2. 或者：主进程调用 terminate() 发送 SIGTERM                         │ │
│  │     → 抛出 KeyboardInterrupt                                          │ │
│  │     → 被 _create_process 的 try/except 捕获                          │ │
│  │     → 执行 shutdown() 钩子                                            │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.4 退出流程详解

#### 主循环退出检查

Consumer 的主循环在 `loop()` 方法中检查退出条件：

```python
def loop(self, health_check_ts=None):
    try:
        # 等待 stop_flag，超时后继续检查
        # _stop_flag_timeout = 0.1 秒
        self.stop_flag.wait(timeout=self._stop_flag_timeout)
    except KeyboardInterrupt:
        # Process 模式下 SIGINT 的默认处理
        self._logger.info('Received SIGINT')
        self.stop(graceful=True)
    except:
        self._logger.exception('Error in consumer.')
        self.stop()
    else:
        # 检查是否通过信号处理器设置了 _received_signal
        if self._received_signal:
            self.stop(graceful=self._graceful)
    
    # 如果 stop_flag 已设置，抛出异常终止主循环
    if self.stop_flag.is_set():
        raise ConsumerStopped
    
    # 健康检查逻辑...
    # ...
    
    return health_check_ts
```

**代码位置**：`huey/consumer.py:492-515`

#### run() 方法的完整流程

```python
def run(self):
    # 1. 启动所有进程
    self.start()
    
    health_check_ts = time.monotonic()
    
    # 2. 主循环
    while True:
        try:
            health_check_ts = self.loop(health_check_ts)
        except ConsumerStopped:
            break  # 退出循环
    
    # 3. 任务中断通知
    self.huey.notify_interrupted_tasks()
    
    # 4. 检查是否需要重启
    if self._restart:
        self._logger.info('Consumer will restart.')
        python = sys.executable
        if not python:
            self._logger.error('Could not determine Python executable, '
                               'unable to restart.')
        else:
            # 用相同参数重新执行自身
            os.execl(python, python, *sys.argv)
    else:
        self._logger.info('Consumer exiting.')
```

**代码位置**：`huey/consumer.py:466-491`

#### stop() 方法实现

```python
def stop(self, graceful=False):
    """
    设置停止标志
    
    graceful=True: 阻塞直到所有 worker 完成当前任务
    graceful=False: 立即返回，不等待
    """
    # 1. 设置停止标志
    self.stop_flag.set()
    
    if graceful:
        self._logger.info('Shutting down gracefully...')
        try:
            # 2. 等待所有 worker 进程完成
            for _, worker_process in self.worker_threads:
                worker_process.join()
            
            # 3. 等待 scheduler 完成
            self.scheduler.join()
        except KeyboardInterrupt:
            # 再次收到中断信号，放弃等待
            self._logger.info('Received request to shut down now.')
            self._restart = False
        else:
            self._logger.info('All workers have stopped.')
    else:
        self._logger.info('Shutting down')
```

**代码位置**：`huey/consumer.py:444-465`

### 3.5 进程封装层 (_create_process)

所有 Scheduler 和 Worker 都通过 `_create_process` 方法封装，这个方法定义了执行单元的生命周期：

```python
def _create_process(self, process, name):
    """
    封装 process 的 loop() 方法，添加生命周期管理
    
    Args:
        process: Worker 或 Scheduler 实例
        name: 进程名称 (用于日志)
    
    Returns:
        由 Environment.create_process 创建的执行单元
    """
    
    def _run():
        # Process 模式下，设置子进程信号处理器
        if self.worker_type == WORKER_PROCESS:
            self._set_child_signal_handlers()
        
        # 1. 初始化 (调用 startup 钩子)
        process.initialize()
        
        try:
            # 2. 主循环：直到 stop_flag 被设置
            while not self.stop_flag.is_set():
                process.loop()
        
        except KeyboardInterrupt:
            # 捕获键盘中断，静默处理
            pass
        
        except WorkerRecycle:
            # Worker 达到 max_tasks 限制，需要重启
            self._logger.info('Process %s restarting (max tasks).', name)
        
        except:
            # 其他异常：记录日志，进程退出
            self._logger.exception('Process %s died!', name)
        
        finally:
            # 3. 清理 (调用 shutdown 钩子)
            process.shutdown()
    
    # 创建执行单元 (线程/进程/greenlet)
    return self.environment.create_process(_run, name)
```

**代码位置**：`huey/consumer.py:381-402`

### 3.6 资源清理机制

#### Worker 生命周期钩子

```python
class Worker(BaseProcess):
    def initialize(self):
        """
        Worker 启动时调用
        
        执行所有注册的 startup 钩子
        """
        for name, startup_hook in self.huey._startup.items():
            self._logger.debug('calling startup hook "%s"', name)
            try:
                startup_hook()
            except Exception as exc:
                self._logger.exception('startup hook "%s" failed', name)

    def shutdown(self):
        """
        Worker 退出时调用
        
        执行所有注册的 shutdown 钩子
        """
        for name, shutdown_hook in self.huey._shutdown.items():
            self._logger.debug('calling shutdown hook "%s"', name)
            try:
                shutdown_hook()
            except Exception as exc:
                self._logger.exception('shutdown hook "%s" failed', name)
```

**代码位置**：`huey/consumer.py:101-115`

#### 用户注册钩子的方式

```python
@huey.on_startup()
def open_database_connection():
    """启动时打开数据库连接"""
    global db_conn
    db_conn = create_connection()

@huey.on_shutdown()
def close_database_connection():
    """退出时关闭数据库连接"""
    global db_conn
    if db_conn:
        db_conn.close()
```

#### 任务执行中的中断处理

在任务执行过程中，如果收到中断信号，会有特殊处理：

```python
def _execute(self, task, timestamp):
    # ...
    
    try:
        self._tasks_in_flight.add(task)
        try:
            with self._timeout_context(task) as check_timeout:
                task_value = task.execute()
        finally:
            self._tasks_in_flight.remove(task)
            duration = time.monotonic() - start
    
    # ... 其他异常处理 ...
    
    except KeyboardInterrupt:
        # 捕获键盘中断
        logger.warning('Received exit signal, %s did not finish.', task.id)
        # 发送中断信号
        self._emit(S.SIGNAL_INTERRUPTED, task)
        # 立即返回，不重试
        return
    
    # ...
```

**代码位置**：`huey/api.py:430-541`

#### 中断任务通知

```python
def notify_interrupted_tasks(self):
    """
    Consumer 退出时调用，通知所有执行中的任务
    
    遍历 _tasks_in_flight 集合，发送 SIGNAL_INTERRUPTED 信号
    """
    while self._tasks_in_flight:
        task = self._tasks_in_flight.pop()
        self._emit(S.SIGNAL_INTERRUPTED, task)
```

**代码位置**：`huey/api.py:269-273`

#### 启动时的锁清理

Consumer 启动时可以选择清理可能遗留的锁：

```python
class Consumer(object):
    def __init__(self, ..., flush_locks=False, extra_locks=None, ...):
        # ...
        
        # 启动时清理锁
        if flush_locks or extra_locks:
            lock_names = extra_locks.split(',') if extra_locks else ()
            self.flush_locks(*lock_names)
    
    def flush_locks(self, *names):
        self._logger.debug('Flushing locks before starting up.')
        flushed = self.huey.flush_locks(*names)
        if flushed:
            self._logger.warning('Found stale locks: %s' % (
                ', '.join(key for key in flushed)))
```

**代码位置**：`huey/consumer.py:334-359`

### 3.7 优雅退出流程图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         优雅退出完整流程                                   │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        阶段 1: 信号接收                              │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │                                                                       │ │
│  │  用户发送信号:                                                        │ │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐            │ │
│  │  │  SIGINT     │    │  SIGHUP     │    │  SIGTERM    │            │ │
│  │  │  (Ctrl+C)   │    │             │    │             │            │ │
│  │  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘            │ │
│  │         │                  │                  │                     │ │
│  │         ▼                  ▼                  ▼                     │ │
│  │  ┌──────────────────────────────────────────────────────────────┐  │ │
│  │  │                    信号处理器设置标志                           │  │ │
│  │  ├──────────────────────────────────────────────────────────────┤  │ │
│  │  │  _received_signal = True                                       │  │ │
│  │  │  _graceful = ?  (SIGINT/SIGHUP=True, SIGTERM=False)          │  │ │
│  │  │  _restart = ?   (SIGHUP=True, 其他=False)                      │  │ │
│  │  └──────────────────────────────────────────────────────────────┘  │ │
│  │                                                                       │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    │                                      │
│                                    ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        阶段 2: 主循环检测                            │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │                                                                       │ │
│  │  Consumer.loop() 检测:                                                │ │
│  │  1. stop_flag.wait(timeout=0.1) → 超时返回                          │ │
│  │  2. 检查 _received_signal → 是                                        │ │
│  │  3. 调用 self.stop(graceful=self._graceful)                          │ │
│  │                                                                       │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    │                                      │
│                                    ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        阶段 3: stop() 执行                           │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │                                                                       │ │
│  │  1. self.stop_flag.set()  ← 设置停止标志                             │ │
│  │                                                                       │ │
│  │  2. 如果 graceful=True:                                               │ │
│  │     ├── for worker_process in workers:                                │ │
│  │     │       worker_process.join()  ← 等待完成                        │ │
│  │     └── scheduler.join()                                              │ │
│  │                                                                       │ │
│  │     ┌──────────────────────────────────────────────────────────┐   │ │
│  │     │              Worker/Scheduler 检测到 stop_flag             │   │ │
│  │     ├──────────────────────────────────────────────────────────┤   │ │
│  │     │  while not self.stop_flag.is_set():  ← 条件变为 False     │   │ │
│  │     │      process.loop()  ← 退出循环                            │   │ │
│  │     │                                                             │   │ │
│  │     │  进入 finally 块:                                           │   │ │
│  │     │  process.shutdown()                                         │   │ │
│  │     │  └── 执行 shutdown 钩子                                      │   │ │
│  │     └──────────────────────────────────────────────────────────┘   │ │
│  │                                                                       │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    │                                      │
│                                    ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        阶段 4: 清理与退出                            │ │
│  ├────────────────────────────────────────────────────────────────────┤ │
│  │                                                                       │ │
│  │  Consumer.run() 中:                                                   │ │
│  │                                                                       │ │
│  │  1. 捕获 ConsumerStopped 异常                                         │ │
│  │  2. huey.notify_interrupted_tasks()                                   │ │
│  │     └── 向所有 _tasks_in_flight 发送 SIGNAL_INTERRUPTED             │ │
│  │                                                                       │ │
│  │  3. 检查 _restart:                                                    │ │
│  │     ├── True: os.execl(python, python, *sys.argv) ← 重启           │ │
│  │     └── False: 正常退出                                               │ │
│  │                                                                       │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.8 非优雅退出 (SIGTERM) 的特殊处理

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    非优雅退出 (SIGTERM) 流程                              │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  与优雅退出的主要区别:                                                     │
│  ├── _graceful = False                                                   │
│  ├── stop() 中不调用 join()，立即返回                                    │
│  └── Greenlet 模式额外发送 KeyboardInterrupt                             │
│                                                                           │
│  Greenlet 模式的特殊处理:                                                  │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  def _handle_stop_signal(self, sig_num, frame):                      │ │
│  │      # ...                                                            │ │
│  │      if self.worker_type == WORKER_GREENLET:                         │ │
│  │          def kill_workers():                                          │ │
│  │              gevent.killall(                                          │ │
│  │                  [t for _, t in self.worker_threads],                │ │
│  │                  KeyboardInterrupt)                                    │ │
│  │          gevent.spawn(kill_workers)                                   │ │
│  │                                                                       │ │
│  │  这会:                                                                 │ │
│  │  1. 在所有 worker greenlet 中抛出 KeyboardInterrupt                   │ │
│  │  2. 如果任务正在执行，异常被 huey._execute() 捕获                     │ │
│  │  3. 发送 SIGNAL_INTERRUPTED 信号                                      │ │
│  │  4. 任务不重试，立即退出                                               │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  Process 模式的非优雅退出:                                                 │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  主进程可能调用:                                                        │ │
│  │  worker_process.terminate()  ← 发送 SIGTERM                          │ │
│  │                                                                       │ │
│  │  子进程收到 SIGTERM:                                                   │ │
│  │  signal.signal(SIGTERM, _handle_stop_signal_worker)                  │ │
│  │  └── 抛出 KeyboardInterrupt                                            │ │
│  │                                                                       │ │
│  │  效果:                                                                 │ │
│  │  - 如果在 process.loop() 中：被 _run() 的 try/except 捕获            │ │
│  │  - 如果在 huey.execute() 中：被 _execute() 的 try/except 捕获        │ │
│  │    → 发送 SIGNAL_INTERRUPTED                                          │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 附录：关键代码位置速查表

| 功能模块 | 文件 | 行号 |
|----------|------|------|
| **三种 Environment 类** | `huey/consumer.py` | 212-266 |
| **Worker 类** | `huey/consumer.py` | 83-147 |
| **Scheduler 类** | `huey/consumer.py` | 149-196 |
| **Consumer 类** | `huey/consumer.py` | 269-602 |
| **crontab 函数** | `huey/api.py` | 1343-1429 |
| **Huey.execute()** | `huey/api.py` | 413-541 |
| **Huey._execute()** | `huey/api.py` | 430-541 |
| **信号处理器** | `huey/consumer.py` | 549-602 |
| **stop() 方法** | `huey/consumer.py` | 444-465 |
| **Registry 类** | `huey/registry.py` | 18-129 |
| **超时处理** | `huey/utils.py` | 184-212 |

---

## 总结

### 核心设计理念

1. **统一抽象**：通过 `Environment` 类统一三种并发模型的差异，上层代码无需关心底层实现
2. **存储层解耦**：所有任务通过存储层传递，实现了 Producer、Scheduler、Worker 之间的完全解耦
3. **精确调度**：Scheduler 使用时间戳追踪而非简单睡眠，保证调度精度
4. **分层信号处理**：主进程负责决策，子进程负责执行，信号处理策略因角色而异
5. **双模式退出**：支持优雅退出和立即退出，满足不同场景需求

### 关键数据结构

- `stop_flag`：跨执行单元的同步事件，用于协调退出
- `cron_settings`：crontab 解析后的允许值集合列表
- `_periodic_tasks`：注册表中保存的周期性任务类列表
- `_tasks_in_flight`：当前正在执行的任务集合，用于中断通知

### 时间线

- **Scheduler 调度间隔**：1 秒 (可配置，必须是 60 的因数)
- **周期性任务检查间隔**：60 秒 (硬编码)
- **stop_flag 检查间隔**：0.1 秒 (硬编码)
- **Worker 退避初始延迟**：0.1 秒
- **Worker 最大退避延迟**：10.0 秒

---

*报告生成时间：2026-04-29*
