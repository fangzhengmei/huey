# Huey 存储后端架构分析报告

## 一、存储层抽象定义

### 1.1 核心操作契约

`BaseStorage` 类定义了完整的存储层接口契约，所有后端必须实现这些方法：

```python
class BaseStorage(object):
    blocking = False  # dequeue() 是否阻塞
    priority = True   # 是否支持优先级
```

#### 队列操作（Queue Operations）

| 方法 | 功能描述 | 原子性要求 |
|------|----------|------------|
| `enqueue(data, priority=None)` | 将任务数据加入队列 | 否 |
| `dequeue()` | 原子性地从队列移除并返回数据 | **是** |
| `queue_size()` | 返回队列中任务数量 | 否 |
| `enqueued_items(limit=None)` | 非破坏性读取队列中的任务 | 否 |
| `flush_queue()` | 清空队列 | 否 |

#### 调度操作（Schedule Operations）

| 方法 | 功能描述 | 原子性要求 |
|------|----------|------------|
| `add_to_schedule(data, ts)` | 添加定时任务到调度表 | 否 |
| `read_schedule(ts)` | 原子性读取并移除以 `ts` 为截止时间的任务 | **是** |
| `schedule_size()` | 返回调度表中的任务数量 | 否 |
| `scheduled_items(limit=None)` | 非破坏性读取调度任务 | 否 |
| `flush_schedule()` | 清空调度表 | 否 |

#### 键值存储操作（Key-Value Operations）

| 方法 | 功能描述 | 原子性要求 |
|------|----------|------------|
| `put_data(key, value, is_result=False)` | 存储键值对 | 否 |
| `peek_data(key)` | 非破坏性读取值 | 否 |
| `pop_data(key)` | 破坏性读取（读取并删除） | **是** |
| `delete_data(key)` | 删除键值对 | 否 |
| `has_data_for_key(key)` | 检查键是否存在 | 否 |
| `put_if_empty(key, value)` | 原子性地仅当键不存在时写入 | **是** |
| `wait_result(key, timeout, backoff, max_delay)` | 阻塞等待结果可用 | 否 |

#### 计数器操作（Counter Operations）

| 方法 | 功能描述 | 原子性要求 |
|------|----------|------------|
| `incr(key, amount=1)` | 原子性递增计数器 | **是** |
| `delete_counter(key)` | 删除计数器 | 否 |
| `flush_counters()` | 清空所有计数器 | 否 |

#### 结果存储操作

| 方法 | 功能描述 |
|------|----------|
| `result_store_size()` | 返回结果存储中的键值对数量 |
| `result_items()` | 返回所有结果键值对 |
| `flush_results()` | 清空结果存储 |
| `flush_all()` | 清空所有数据（队列+调度+结果+计数器） |

---

## 二、三种后端实现分析

### 2.1 MemoryStorage（内存存储）

**文件位置**: `storage.py:308-403`

#### 数据结构

```python
class MemoryStorage(BaseStorage):
    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)
        self._c = 0  # FIFO 顺序计数器
        self._queue = []       # heapq 实现的优先级队列
        self._results = {}     # 结果存储字典
        self._schedule = []    # heapq 实现的时间排序堆
        self._counters = {}    # 计数器字典
        self._lock = threading.RLock()  # 可重入锁
```

#### 关键实现细节

##### 队列操作

```python
def enqueue(self, data, priority=None):
    with self._lock:
        self._c += 1
        priority = 0 if priority is None else -priority
        heapq.heappush(self._queue, (priority, self._c, data))

def dequeue(self):
    try:
        _, _, data = heapq.heappop(self._queue)  # ⚠️ 无锁保护！
    except IndexError:
        pass
    else:
        return data
```

**问题**: `dequeue()` 方法**没有获取锁**，存在竞态条件风险。

##### 调度操作

```python
def add_to_schedule(self, data, ts):
    heapq.heappush(self._schedule, (ts, data))  # ⚠️ 无锁

def read_schedule(self, ts):
    with self._lock:  # 有锁保护
        accum = []
        while self._schedule:
            sts, data = heapq.heappop(self._schedule)
            if sts <= ts:
                accum.append(data)
            else:
                heapq.heappush(self._schedule, (sts, data))
                break
    return accum
```

##### 键值和计数器

```python
def put_if_empty(self, key, value):
    if self.has_data_for_key(key):  # ⚠️ 非原子！先检查后写入
        return False
    self.put_data(key, value)
    return True

def incr(self, key, amount=1):
    with self._lock:  # 有锁保护
        self._counters[key] = self._counters.get(key, 0) + amount
    return self._counters[key]
```

---

### 2.2 RedisStorage（Redis 存储）

**文件位置**: `storage.py:417-598`

#### 数据结构映射

| 逻辑概念 | Redis 数据结构 | Key 命名 |
|----------|----------------|----------|
| 队列 | List | `huey.redis.{name}` |
| 调度表 | Sorted Set | `huey.schedule.{name}` |
| 结果存储 | Hash | `huey.results.{name}` |
| 计数器 | Hash | `huey.counters.{name}` |
| 错误存储 | Hash | `huey.errors.{name}` |

#### 关键实现细节

##### 队列操作

```python
def enqueue(self, data, priority=None):
    if priority:
        raise NotImplementedError('Task priorities are not supported')
    self.conn.lpush(self.queue_key, data)

def dequeue(self):
    if self.blocking:
        try:
            return self.conn.brpop(
                self.queue_key,
                timeout=self.read_timeout)[1]  # 阻塞式弹出
        except (ConnectionError, TimeoutError, TypeError, IndexError):
            return None
    else:
        return self.conn.rpop(self.queue_key)  # 非阻塞
```

**优先级变体** (`PriorityRedisStorage`):

```python
def enqueue(self, data, priority=None):
    priority = 0 if priority is None else -priority
    prefix = struct.pack('>Q', int(time.time() * 1e6))  # 微秒时间戳前缀
    self.conn.zadd(self.queue_key, {prefix + data: priority})

def dequeue(self):
    if self.blocking:
        _, res, _ = self.conn.bzpopmin(self.queue_key, timeout=self.read_timeout)
        return res[8:]  # 移除 8 字节前缀
    else:
        items = self.conn.zpopmin(self.queue_key, count=1)
        return items[0][0][8:] if items else None
```

##### 调度操作 - **Lua 脚本保证原子性**

```python
SCHEDULE_POP_LUA = """\
local unix_ts = tonumber(ARGV[1])
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""

def read_schedule(self, ts):
    unix_ts = self.convert_ts(ts)
    tasks = self._pop(keys=[self.schedule_key], args=[unix_ts])
    return [] if tasks is None else tasks
```

**设计要点**: Lua 脚本在 Redis 中原子执行，确保"读取+删除"操作不会被其他客户端打断。

##### 原子操作

```python
def put_if_empty(self, key, value):
    return self.conn.hsetnx(self.result_key, key, value)  # Redis 原生原子

def incr(self, key, amount=1):
    return self.conn.hincrby(self.counter_key, key, amount)  # Redis 原生原子

def pop_data(self, key):
    pipe = self.conn.pipeline()
    pipe.hexists(self.result_key, key)
    pipe.hget(self.result_key, key)
    pipe.hdel(self.result_key, key)
    exists, val, n = pipe.execute()  # Pipeline 但非原子！
    return EmptyData if not exists else val
```

**注意**: `pop_data` 使用 pipeline 但不是原子事务。如果需要原子性，应该使用 Lua 脚本或 MULTI/EXEC。

---

### 2.3 SqliteStorage（SQLite 存储）

**文件位置**: `storage.py:794-1004`

#### 表结构设计

```sql
-- 键值存储表
create table if not exists kv (
    queue text not null, 
    key text not null, 
    value blob not null, 
    primary key(queue, key)
)

-- 调度表
create table if not exists schedule (
    id integer not null primary key, 
    queue text not null, 
    data blob not null, 
    timestamp real not null
)
create index if not exists schedule_queue_timestamp on schedule (queue, timestamp)

-- 任务队列表
create table if not exists task (
    id integer not null primary key, 
    queue text not null, 
    data blob not null, 
    priority real not null default 0.0
)
create index if not exists task_priority_id on task (priority desc, id asc)

-- 计数器表
create table if not exists counter (
    queue text not null, 
    key text not null, 
    value integer not null default 0, 
    primary key(queue, key)
)
```

#### 并发控制机制

```python
class BaseSqlStorage(BaseStorage):
    begin_sql = 'begin'  # 或 'begin exclusive'
    
    def __init__(self, *args, **kwargs):
        super(BaseSqlStorage, self).__init__(*args, **kwargs)
        self.lock = threading.Lock()  # 应用层线程锁
        self._conn = None

class SqliteStorage(BaseSqlStorage):
    begin_sql = 'begin exclusive'  # 排他事务
```

#### 上下文管理器设计

```python
@contextlib.contextmanager
def db(self, commit=False, close=False):
    with self.lock:  # 1. 先获取应用层锁
        conn = self.conn
        cursor = conn.cursor()
        try:
            if commit: cursor.execute(self.begin_sql)  # 2. 开始数据库事务
            yield cursor
        except Exception:
            if commit: conn.rollback()
            raise
        else:
            if commit: conn.commit()
        finally:
            cursor.close()
            if close:
                conn.close()
                self._conn = None
```

**双重保护**: 
1. `threading.Lock()` - 防止同一进程内多线程竞争
2. `BEGIN EXCLUSIVE` - 防止其他数据库连接（可能是其他进程）写入

#### 关键实现细节

##### 队列操作

```python
def enqueue(self, data, priority=None):
    self.sql('insert into task (queue, data, priority) values (?, ?, ?)',
             (self.name, self.to_blob(data), priority or 0), commit=True)

def dequeue(self):
    with self.db(commit=True) as curs:
        # 1. 查询最高优先级的任务
        curs.execute('select id, data from task where queue = ? '
                     'order by priority desc, id limit 1', (self.name,))
        result = curs.fetchone()
        if result is not None:
            tid, data = result
            # 2. 删除该任务
            curs.execute('delete from task where id = ?', (tid,))
            # 3. 验证删除成功（防止竞态）
            if curs.rowcount == 1:
                return data
```

**原子性保证**: 
- 整个操作在 `BEGIN EXCLUSIVE` 事务中
- `rowcount == 1` 检查确保确实删除了一行

##### 调度操作

```python
def read_schedule(self, ts):
    with self.db(commit=True) as curs:
        params = (self.name, ts.timestamp())
        # 1. 查询所有到期任务
        curs.execute('select id, data from schedule where '
                     'queue = ? and timestamp <= ?', params)
        id_list, data = [], []
        for task_id, task_data in curs.fetchall():
            id_list.append(task_id)
            data.append(task_data)
        # 2. 批量删除
        if id_list:
            plist = ','.join('?' * len(id_list))
            curs.execute('delete from schedule where id IN (%s)' % plist,
                         id_list)
        return data
```

##### 原子操作实现

```python
def put_if_empty(self, key, value):
    try:
        with self.db(commit=True) as curs:
            curs.execute('insert or abort into kv '  # 冲突则回滚
                         '(queue, key, value) values (?, ?, ?)',
                         (self.name, key, self.to_blob(value)))
    except sqlite3.IntegrityError:
        return False
    else:
        return True

def incr(self, key, amount=1):
    with self.db(commit=True) as curs:
        if sqlite3.sqlite_version_info >= (3, 35, 0):
            # SQLite 3.35+ 支持 RETURNING 子句
            curs.execute('insert into counter (queue, key, value) '
                         'values (?, ?, ?) on conflict (queue, key) '
                         'do update set value = value + ? '
                         'returning value',
                         (self.name, key, amount, amount))
            value, = curs.fetchone()
        elif sqlite3.sqlite_version_info >= (3, 24, 0):
            # SQLite 3.24+ 支持 UPSERT 但无 RETURNING
            curs.execute('insert into counter (queue, key, value) '
                         'values (?, ?, ?) on conflict (queue, key) '
                         'do update set value = value + ?',
                         (self.name, key, amount, amount))
            curs.execute('select value from counter where queue = ? and key = ?',
                         (self.name, key))
            value, = curs.fetchone()
        else:
            raise NotImplementedError('SQLite 3.24 or newer is required.')
    return value
```

---

## 三、原子性与并发处理对比

### 3.1 原子性保证对比

| 操作类型 | MemoryStorage | RedisStorage | SqliteStorage |
|----------|---------------|--------------|---------------|
| **dequeue** | ❌ 无锁保护，heapq 操作非原子 | ✅ Redis 命令原生原子 | ✅ 事务 + rowcount 验证 |
| **read_schedule** | ✅ 有锁保护 | ✅ Lua 脚本原子执行 | ✅ 事务保护 |
| **put_if_empty** | ❌ 先检查后写入，非原子 | ✅ HSETNX 原生原子 | ✅ INSERT OR ABORT + IntegrityError |
| **incr** | ✅ 有锁保护 | ✅ HINCRBY 原生原子 | ✅ UPSERT 原子 |
| **pop_data** | ⚠️ dict.pop 是原子的 | ⚠️ Pipeline 非原子 | ✅ 事务保护（3.35+ 用 RETURNING） |

### 3.2 并发处理机制

#### MemoryStorage

```
┌─────────────────────────────────────────────────────────┐
│                     进程内多线程                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                │
│  │ Thread1 │  │ Thread2 │  │ Thread3 │                │
│  └────┬────┘  └────┬────┘  └────┬────┘                │
│       │            │            │                        │
│       ▼            ▼            ▼                        │
│  ┌──────────────────────────────────────┐               │
│  │    threading.RLock() [部分使用]       │               │
│  │  - enqueue: 有锁                       │               │
│  │  - dequeue: 无锁 ⚠️                    │               │
│  │  - incr: 有锁                          │               │
│  └──────────────────────────────────────┘               │
│                          │                               │
│                          ▼                               │
│  ┌──────────────────────────────────────┐               │
│  │  Python 原生数据结构（非共享内存）      │               │
│  │  - heapq 堆                            │               │
│  │  - dict 字典                           │               │
│  └──────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────┘
                        不支持多进程
```

**问题**: `dequeue()` 方法没有使用锁：

```python
def dequeue(self):
    try:
        _, _, data = heapq.heappop(self._queue)  # 多线程同时调用会崩溃！
    except IndexError:
        pass
    else:
        return data
```

`heapq.heappop` 包含多个操作：
1. 取出堆顶元素
2. 将最后一个元素移到堆顶
3. 执行下沉操作维护堆性质

多线程同时执行时，堆结构可能被破坏。

#### RedisStorage

```
┌─────────────────────────────────────────────────────────────────┐
│                        Redis 服务器（单线程）                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    命令队列（串行执行）                     │   │
│  │  Cmd1 → Cmd2 → Cmd3 → Lua Script → Cmd4 → ...          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │
│  │  List 队列   │  │ Sorted Set  │  │  Hash 哈希表         │   │
│  │  (lpush/rpop)│  │ (zadd/zpop) │  │ (hset/hget/hincrby) │   │
│  └─────────────┘  └─────────────┘  └─────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
          ▲                    ▲                    ▲
          │                    │                    │
    ┌─────┴─────┐        ┌─────┴─────┐        ┌─────┴─────┐
    │  Client1  │        │  Client2  │        │  Client3  │
    │ (进程/线程)│        │ (进程/线程)│        │ (进程/线程)│
    └───────────┘        └───────────┘        └───────────┘
```

**优势**:
- 所有命令在 Redis 服务器中单线程串行执行
- Lua 脚本可以包含多个命令，原子执行
- 支持多进程、多客户端并发

#### SqliteStorage

```
┌─────────────────────────────────────────────────────────────────┐
│                    应用层 (Python 进程)                           │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐                  │
│  │  Thread1  │  │  Thread2  │  │  Thread3  │                  │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘                  │
│        │              │              │                          │
│        ▼              ▼              ▼                          │
│  ┌───────────────────────────────────────┐                     │
│  │      threading.Lock() [同一进程内]      │                     │
│  │      确保同一时间只有一个线程操作数据库    │                     │
│  └───────────────────────────────────────┘                     │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              SQLite 数据库连接                           │   │
│  │  ┌─────────────────────────────────────────────────┐    │   │
│  │  │  BEGIN EXCLUSIVE 事务                            │    │   │
│  │  │  - 阻止其他连接写入                               │    │   │
│  │  │  - 允许其他连接读取（取决于锁级别）                │    │   │
│  │  └─────────────────────────────────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  SQLite 数据库文件 │
                    │  (可被多进程访问)  │
                    └─────────────────┘
```

**双重锁机制**:
1. `threading.Lock()` - 防止同一进程内多线程竞争
2. `BEGIN EXCLUSIVE` - SQLite 级别的排他锁，防止其他进程写入

**WAL 模式配置**:

```python
def _create_connection(self):
    conn = sqlite3.connect(self.filename, timeout=self._timeout,
                           check_same_thread=False,
                           **self._conn_kwargs)
    conn.isolation_level = None  # 自动提交模式
    conn.execute('pragma journal_mode="%s"' % self._journal_mode)  # 默认 WAL
    if self._cache_mb:
        conn.execute('pragma cache_size=%s' % (-1000 * self._cache_mb))
    conn.execute('pragma synchronous=%s' % (2 if self._fsync else 0))
    return conn
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `journal_mode` | `wal` | Write-Ahead Logging，支持更高并发 |
| `synchronous` | `0` (OFF) | 不等待 fsync，性能高但崩溃可能丢数据 |
| `timeout` | `5` 秒 | 锁等待超时时间 |

---

## 四、任务可靠性分析

### 4.1 崩溃场景对比

#### MemoryStorage - 最不可靠

```
崩溃场景分析：

正常运行状态：
┌─────────────────────────────────────────┐
│           Python 进程内存                 │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │ 队列    │ │ 调度表  │ │ 结果    │   │
│  │ (heapq) │ │ (heapq) │ │ (dict)  │   │
│  └─────────┘ └─────────┘ └─────────┘   │
└─────────────────────────────────────────┘

崩溃后：
┌─────────────────────────────────────────┐
│           进程已终止，内存已释放          │
│  ┌─────────────────────────────────┐    │
│  │  所有数据完全丢失！无法恢复       │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

**风险评估**:
- **进程崩溃**: 100% 数据丢失
- **机器重启**: 100% 数据丢失
- **恢复能力**: 无
- **适用场景**: 开发测试、临时任务、可重复执行的任务

#### RedisStorage - 取决于持久化配置

```
持久化策略对比：

┌────────────────────────────────────────────────────────────┐
│                      RDB 快照模式                            │
│  时机：定期执行（如 save 900 1）                            │
│                                                             │
│  T0: save 完成 → 数据安全                                    │
│  T1: 新任务入队 → 仅在内存 ⚠️                               │
│  T2: 进程崩溃 → T1 的任务丢失！                              │
│  T3: Redis 重启 → 从 T0 的 RDB 恢复                         │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                      AOF 追加模式                            │
│  appendfsync 选项：                                          │
│  - always: 每个命令都 fsync → 最安全，性能最低              │
│  - everysec: 每秒 fsync → 平衡，可能丢 1 秒数据            │
│  - no: 由 OS 决定何时刷盘 → 性能最高，可能丢更多数据        │
└────────────────────────────────────────────────────────────┘
```

**风险评估**:

| 配置 | 崩溃后数据丢失 | 恢复能力 |
|------|---------------|---------|
| RDB 默认 | 可能丢数分钟数据 | 从 RDB 文件恢复 |
| AOF + appendfsync=always | 几乎不丢 | 从 AOF 文件重放 |
| AOF + appendfsync=everysec | 可能丢 1 秒 | 从 AOF 文件重放 |
| 无持久化 | 100% 丢失 | 无法恢复 |

**主从复制额外风险**:
- 异步复制：主节点崩溃时，从节点可能尚未同步最新数据
- 故障转移可能导致数据丢失

#### SqliteStorage - 取决于同步配置

```
synchronous 配置对比：

┌────────────────────────────────────────────────────────────┐
│  synchronous = 0 (OFF) - 默认值                            │
│                                                             │
│  应用写入 → OS 页缓存 → [不确定何时] → 磁盘                │
│                       ↑                                     │
│              崩溃则丢失这部分数据 ⚠️                        │
│                                                             │
│  风险：OS 崩溃或机器掉电可能丢失数据                        │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│  synchronous = 2 (FULL) - 最安全                           │
│                                                             │
│  应用写入 → OS 页缓存 → 立即 fsync → 磁盘                  │
│                                          │                  │
│                                    写入完成确认              │
│                                                             │
│  风险：几乎无数据丢失风险，但性能降低                       │
└────────────────────────────────────────────────────────────┘
```

**WAL 模式恢复机制**:
- WAL 文件在 checkpoint 之前包含所有变更
- 崩溃后重启时，SQLite 会自动检查并重放 WAL
- 只要 `synchronous=FULL`，数据在提交时已落盘

### 4.2 并发写场景分析

#### MemoryStorage - 存在竞态

```python
# 问题代码：dequeue 无锁
def dequeue(self):
    try:
        # heapq.heappop 不是线程安全的！
        _, _, data = heapq.heappop(self._queue)
    except IndexError:
        pass
    else:
        return data
```

**竞态场景**:

```
时间线：
Thread1: 检查堆非空 → 取出堆顶元素 (index=0)
Thread2: 同时也在执行 heappop → 可能读取到不一致的状态
Thread1: 移动最后一个元素到堆顶，开始下沉
Thread2: 堆结构已被破坏，可能引发 IndexError 或数据错乱

可能的后果：
1. 同一任务被多个线程获取（重复执行）
2. 堆结构破坏，某些任务永远无法被取出
3. Python 异常崩溃
```

#### RedisStorage - 天然安全

```
Redis 单线程模型：

Client1: LPUSH queue task1
Client2: LPUSH queue task2
Client1: BRPOP queue 0
Client2: BRPOP queue 0

执行顺序（Redis 内部串行化）：
1. Client1 LPUSH → queue: [task1]
2. Client2 LPUSH → queue: [task2, task1]
3. Client1 BRPOP → 获取 task1，queue: [task2]
4. Client2 BRPOP → 获取 task2，queue: []

结果：
- 每个任务只被一个客户端获取
- 无竞态，无重复，无丢失
```

#### SqliteStorage - 双重保护

```python
def dequeue(self):
    with self.db(commit=True) as curs:  # 1. 获取 threading.Lock()
        # 2. BEGIN EXCLUSIVE 事务
        curs.execute('select id, data from task where queue = ? '
                     'order by priority desc, id limit 1', (self.name,))
        result = curs.fetchone()
        if result is not None:
            tid, data = result
            curs.execute('delete from task where id = ?', (tid,))
            # 3. 验证删除成功
            if curs.rowcount == 1:
                return data
```

**多进程场景**:

```
进程 A: BEGIN EXCLUSIVE → 获取数据库写锁
进程 B: BEGIN EXCLUSIVE → 等待锁 (busy)
进程 A: SELECT → 获取任务 X
进程 A: DELETE 任务 X → rowcount = 1
进程 A: COMMIT → 释放锁
进程 B: 获取锁 → BEGIN EXCLUSIVE
进程 B: SELECT → 获取任务 Y (X 已被删除)
进程 B: DELETE 任务 Y
进程 B: COMMIT
```

### 4.3 极端场景应对策略

#### 场景 1：消费者崩溃，任务正在执行中

```
任务执行流程：
1. dequeue() → 任务从队列移除
2. 消费者开始执行任务
3. 消费者崩溃 ⚠️

后果：
- 任务已从队列移除
- 任务未执行完成
- 任务结果未写入
```

**各后端表现**:

| 后端 | 任务状态 | 能否自动恢复 |
|------|---------|-------------|
| MemoryStorage | 丢失 | 否 |
| RedisStorage | 丢失 | 否（除非使用 RPOPLPUSH 模式） |
| SqliteStorage | 丢失 | 否 |

**Huey 的设计选择**: 
- 采用"至少一次"（at-least-once）语义
- 任务出队后即从队列删除
- 依赖重试机制（retries 参数）而非事务性出队

**注意**: 某些消息队列（如 RabbitMQ）采用"确认模式"，任务出队后不立即删除，等待消费者 ack。Huey 没有采用这种设计。

#### 场景 2：高并发入队

```python
# 1000 个线程同时入队 10000 个任务

# MemoryStorage
def enqueue(self, data, priority=None):
    with self._lock:  # 锁竞争成为瓶颈
        self._c += 1
        priority = 0 if priority is None else -priority
        heapq.heappush(self._queue, (priority, self._c, data))

# RedisStorage
def enqueue(self, data, priority=None):
    self.conn.lpush(self.queue_key, data)  # 无锁，Redis 处理并发

# SqliteStorage
def enqueue(self, data, priority=None):
    self.sql('insert into task ...', commit=True)  # 锁竞争 + 事务开销
```

**性能对比（估算）**:

| 后端 | 单线程入队 (qps) | 100 线程入队 (qps) | 瓶颈 |
|------|------------------|-------------------|------|
| MemoryStorage | 极高 (~100k) | 低 (~1k) | GIL + 锁竞争 |
| RedisStorage | 高 (~10k) | 中高 (~5k) | 网络 + Redis 单线程 |
| SqliteStorage | 中 (~1k) | 低 (~100) | 锁竞争 + 磁盘 IO |

#### 场景 3：批量任务同时到期

```python
# read_schedule 的原子性保证

# Redis - Lua 脚本
SCHEDULE_POP_LUA = """
local unix_ts = tonumber(ARGV[1])
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end"""

# 两个消费者同时调用 read_schedule(now)：
# Consumer A: 执行 Lua 脚本 → 获取 [task1, task2, task3] → 删除这三个
# Consumer B: 执行 Lua 脚本 → 集合已空 → 返回 []
# 结果：无重复，无丢失

# Sqlite - 事务
def read_schedule(self, ts):
    with self.db(commit=True) as curs:
        # BEGIN EXCLUSIVE 确保只有一个连接能执行
        curs.execute('select id, data from schedule where ...')
        # ... 处理 ...
        curs.execute('delete from schedule where id IN (...)')
```

#### 场景 4：任务重复执行检测

```python
# put_if_empty 的使用场景 - 分布式锁

class TaskLock(object):
    def acquire(self):
        # 原子性：仅当锁不存在时获取
        if not self._huey.put_if_empty(self._key, '1'):
            raise TaskLockedException('unable to acquire lock %s' % self._name)
        return True
```

**各后端 put_if_empty 实现对比**:

| 后端 | 实现方式 | 原子性 | 多进程安全 |
|------|---------|--------|-----------|
| MemoryStorage | `if not has_data_for_key(key): put_data(key, value)` | ❌ 非原子 | ❌ 不适用 |
| RedisStorage | `HSETNX` (Redis 原生命令) | ✅ 原子 | ✅ 安全 |
| SqliteStorage | `INSERT OR ABORT` + 捕获 `IntegrityError` | ✅ 原子 | ✅ 安全 |

---

## 五、后端选择建议

### 5.1 决策树

```
                    开始
                      │
                      ▼
           ┌──────────────────────┐
           │   需要持久化存储吗？   │
           └──────────────────────┘
              │              │
             否              是
              │              │
              ▼              ▼
    ┌────────────────┐ ┌────────────────────┐
    │  MemoryStorage │ │   需要多进程吗？    │
    │  (开发测试)    │ └────────────────────┘
    └────────────────┘    │            │
                         否            是
                          │            │
                          ▼            ▼
                ┌──────────────┐ ┌──────────────────┐
                │ SqliteStorage│ │   Redis 可用？    │
                │ (单进程生产)  │ └──────────────────┘
                └──────────────┘    │          │
                                   否          是
                                    │          │
                                    ▼          ▼
                            ┌──────────┐ ┌─────────────┐
                            │Sqlite或  │ │ RedisStorage│
                            │考虑其他  │ │ (多进程生产) │
                            │方案      │ └─────────────┘
                            └──────────┘
```

### 5.2 详细对比表

| 维度 | MemoryStorage | RedisStorage | SqliteStorage |
|------|---------------|--------------|---------------|
| **持久化** | ❌ 无 | ✅ 可配置 (RDB/AOF) | ✅ 天然持久化 |
| **多进程** | ❌ 不支持 | ✅ 完美支持 | ⚠️ 支持但有锁竞争 |
| **原子性** | ⚠️ 部分操作有缺陷 | ✅ 原生原子 | ✅ 事务保证 |
| **并发性能** | ⚠️ 锁竞争瓶颈 | ✅ 优秀 | ⚠️ 一般 |
| **崩溃恢复** | ❌ 无法恢复 | ⚠️ 取决于配置 | ⚠️ 取决于 synchronous |
| **部署复杂度** | ✅ 零依赖 | ⚠️ 需维护 Redis 实例 | ✅ 零依赖（文件） |
| **适用场景** | 开发测试 | 高并发生产环境 | 中小规模单进程 |

### 5.3 配置最佳实践

#### RedisStorage 生产配置

```python
from huey import RedisHuey

# 推荐配置
huey = RedisHuey(
    'my-app',
    url='redis://localhost:6379/0',
    blocking=True,           # 使用阻塞式 dequeue，减少轮询
    read_timeout=1,           # 阻塞超时时间
    notify_result=True,       # 结果通知，降低等待延迟
    notify_result_ttl=86400,  # 通知 TTL
)

# Redis 服务端配置建议 (redis.conf):
# appendonly yes
# appendfsync everysec  # 或 always 追求最高安全
# aof-use-rdb-preamble yes
```

#### SqliteStorage 生产配置

```python
from huey import SqliteHuey

# 最高可靠性配置
huey = SqliteHuey(
    'my-app',
    filename='huey_tasks.db',
    cache_mb=64,              # 增大缓存
    fsync=True,                # 启用 FULL 同步模式 ⚠️
    journal_mode='wal',        # WAL 模式
    timeout=10,                # 锁等待超时
    strict_fifo=True,          # 严格 FIFO 顺序
)

# 注意：fsync=True 会显著降低写入性能
# 如果任务可以接受少量丢失风险，可保持 fsync=False（默认）
```

---

## 六、代码缺陷与改进建议

### 6.1 MemoryStorage dequeue 竞态问题

**问题代码** (`storage.py:324-330`):

```python
def dequeue(self):
    try:
        _, _, data = heapq.heappop(self._queue)  # ⚠️ 无锁！
    except IndexError:
        pass
    else:
        return data
```

**修复建议**:

```python
def dequeue(self):
    with self._lock:
        try:
            _, _, data = heapq.heappop(self._queue)
        except IndexError:
            pass
        else:
            return data
```

### 6.2 MemoryStorage put_if_empty 非原子问题

**问题代码** (`storage.py:217-228`, 继承自 BaseStorage):

```python
def put_if_empty(self, key, value):
    if self.has_data_for_key(key):  # 检查
        return False
    self.put_data(key, value)        # 写入 ⚠️ 竞态窗口
    return True
```

**修复建议**:

```python
def put_if_empty(self, key, value):
    with self._lock:
        if self.has_data_for_key(key):
            return False
        self.put_data(key, value)
        return True
```

### 6.3 RedisStorage pop_data 非原子问题

**问题代码** (`storage.py:545-551`):

```python
def pop_data(self, key):
    pipe = self.conn.pipeline()
    pipe.hexists(self.result_key, key)
    pipe.hget(self.result_key, key)
    pipe.hdel(self.result_key, key)
    exists, val, n = pipe.execute()  # Pipeline 不是事务！
    return EmptyData if not exists else val
```

**问题**: 两个客户端同时调用 `pop_data` 可能都获取到相同的值。

**修复建议**: 使用 Lua 脚本或 MULTI/EXEC 事务：

```python
POP_DATA_LUA = """
local exists = redis.call('hexists', KEYS[1], KEYS[2])
if exists == 1 then
    local val = redis.call('hget', KEYS[1], KEYS[2])
    redis.call('hdel', KEYS[1], KEYS[2])
    return val
else
    return nil
end"""

def pop_data(self, key):
    result = self._pop_data_script(keys=[self.result_key, key])
    return EmptyData if result is None else result
```

---

## 七、总结

Huey 的存储层设计体现了良好的抽象与实现分离：

1. **抽象层** (`BaseStorage`) 定义了清晰的接口契约，涵盖队列、调度、KV 存储、计数器四大功能域。

2. **三个实现**各有取舍：
   - **MemoryStorage**: 最简单但最不可靠，适合开发测试
   - **RedisStorage**: 最健壮，适合高并发生产环境
   - **SqliteStorage**: 零依赖，适合中小规模单进程部署

3. **原子性保证**差异显著：
   - Redis 依赖单线程模型和 Lua 脚本
   - SQLite 依赖事务和锁
   - MemoryStorage 存在已知的线程安全缺陷

4. **可靠性**取决于：
   - 持久化机制（无 vs RDB/AOF vs 磁盘文件）
   - 同步策略（fsync 配置）
   - 原子操作的正确实现

选择后端时应根据实际需求权衡：如果追求简单和零依赖，选 SQLite；如果追求高并发和多进程，选 Redis；如果只是开发测试，MemoryStorage 足够。
