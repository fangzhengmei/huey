# Huey 组合任务编排机制分析报告

## 目录

1. [概述](#概述)
2. [Group 机制](#group-机制)
3. [Chord 机制](#chord-机制)
4. [Pipeline 机制](#pipeline-机制)
5. [三种机制对比总结](#三种机制对比总结)
6. [附录：核心代码位置](#附录核心代码位置)

---

## 概述

Huey 作为一个轻量级的 Python 任务队列库，提供了三种强大的组合任务编排机制：

| 机制 | 执行模式 | 依赖关系 | 结果聚合 | 主要用途 |
|------|----------|----------|----------|----------|
| **Group** | 并行执行 | 无依赖 | 独立结果 | 批量独立任务 |
| **Chord** | 并行 + 回调 | 全部完成后触发 | 聚合到回调 | Map-Reduce 模式 |
| **Pipeline** | 串行执行 | 前序完成后触发 | 链式传递 | 工作流编排 |

这三种机制可以相互组合，构建复杂的任务执行图。

---

## Group 机制

### 核心概念

**Group** 是最简单的任务组合方式，用于将多个独立的任务打包成一个逻辑单元执行。Group 中的任务之间**没有任何依赖关系**，它们会被并行地加入任务队列，由消费者独立执行。

### 执行依赖关系表达

Group 的依赖关系非常简单：**无依赖**。

```python
class group(object):
    def __init__(self, tasks):
        self.tasks = tasks  # 只是简单地存储任务列表
```

**入队逻辑** (`api.py:298-299`)：
```python
if isinstance(task, group):
    return ResultGroup([self.enqueue(t) for t in task.tasks])
```

当 Group 被入队时，Huey 只是简单地遍历其内部的任务列表，将每个任务独立地加入队列。

### 依赖跟踪机制

Group **不跟踪**任务之间的依赖关系。每个任务：
- 拥有独立的任务 ID
- 独立的执行状态
- 独立的结果存储

唯一的关联是通过 `ResultGroup` 提供的统一接口来获取所有任务的结果。

### 结果聚合传递

Group 的结果通过 `ResultGroup` 类进行管理：

```python
class ResultGroup(object):
    def __init__(self, results):
        self._results = results  # 存储 Result 对象列表
    
    def get(self, *args, **kwargs):
        return [result.get(*args, **kwargs) for result in self._results]
```

**结果获取方式**：
1. **阻塞获取**：`result_group.get(blocking=True)` - 等待所有任务完成
2. **迭代获取**：`result_group.as_completed()` - 按完成顺序获取结果
3. **索引访问**：`result_group[0]` - 阻塞获取指定索引的结果

**结果传递特点**：
- 任务结果之间**互不影响**
- 一个任务失败不会阻止其他任务执行
- 获取结果时，如果某个任务失败会抛出 `TaskException`

### 错误传播机制

Group 的错误传播是**独立的**：

```python
def error(self, *args, **kwargs):
    # Apply error handler to all tasks.
    for task in self.tasks:
        task.error(*args, **kwargs)
    return self
```

**错误传播规则**：
1. **任务级隔离**：每个任务的错误是独立的，不会影响其他任务
2. **错误处理器**：通过 `.error()` 方法可以为 Group 中的**所有任务**注册相同的错误处理器
3. **结果获取时抛出**：当调用 `ResultGroup.get()` 时，如果任何一个任务失败，会立即抛出 `TaskException`

**示例场景**：
```python
g = group([task_a.s(1), task_a.s(-1), task_a.s(3)])
g.error(on_err)  # 为所有任务注册错误处理器
rg = huey.enqueue(g)

# 执行顺序：
# 1. task_a(1) 成功执行
# 2. task_a(-1) 失败，触发 on_err
# 3. task_a(3) 继续成功执行
# 4. on_err 被调用处理错误
```

---

## Chord 机制

### 核心概念

**Chord** 实现了经典的 **Map-Reduce** 模式：
- **Map 阶段**：一组并行执行的任务（称为 chord members）
- **Reduce 阶段**：一个回调任务，只有当所有 Map 任务完成后才会执行

Chord 是三种机制中**最复杂**的，因为它需要：
1. 跟踪多个并行任务的完成状态
2. 收集所有任务的结果
3. 原子性地触发回调任务

### 执行依赖关系表达

Chord 的依赖关系是：**回调任务依赖于所有成员任务的完成**。

```python
class chord(object):
    def __init__(self, tasks, callback):
        if isinstance(callback, TaskWrapper):
            callback = callback.s()
        self.tasks = tasks      # Map 阶段任务列表
        self.callback = callback  # Reduce 阶段回调任务
```

**关键依赖表达**：通过 `ChordConfig` 配置对象将成员任务与回调关联起来：

```python
class ChordConfig:
    def __init__(self, cid, size, idx, callback):
        self.cid = cid        # Chord 唯一标识 UUID
        self.size = size      # 成员任务总数
        self.idx = idx        # 当前任务在 chord 中的索引
        self.callback = callback  # 回调任务引用
```

### 依赖跟踪机制

Chord 的依赖跟踪是通过**存储层的计数器**实现的，这是一个相对复杂的机制。

#### 入队时的准备 (`api.py:327-341`)

```python
def _enqueue_chord(self, chord_obj):
    cid = str(uuid.uuid4())  # 为 chord 生成唯一 ID
    size = len(chord_obj.tasks)
    results = []
    for i, task in enumerate(chord_obj.tasks):
        # 为每个成员任务分配 ChordConfig
        config = ChordConfig(cid, size, i, chord_obj.callback)
        results.append(self._enqueue_chord_member(task, config))
    # ...
```

#### 成员任务处理 (`api.py:343-359`)

对于每个成员任务，Huey 会找到其**任务链的尾部**，将 `chord_config` 绑定到尾部任务：

```python
def _enqueue_chord_member(self, task, config):
    if isinstance(task, chord):
        head = task.callback
    else:
        head = task
    
    # 找到任务链的尾部
    tail = head
    while tail.on_complete is not None:
        tail = tail.on_complete
    
    # 将 chord_config 绑定到尾部任务
    tail.chord_config = config
    # ...
```

**设计意图**：这样即使成员任务本身是一个 pipeline（链式任务），也能确保只有当整个 pipeline 完成后才会通知 chord。

#### 完成检测机制 (`api.py:543-560`)

当任务执行完成后，会检查是否有 `chord_config`：

```python
def _check_chord(self, task, value):
    cc = task.chord_config
    chord_key = 'chord:%s' % cc.cid
    result_key = 'chord:%s:%s' % (cc.cid, cc.idx)
    
    # 1. 存储当前任务的结果（使用特殊的 chord 键）
    self.put_result(result_key, value)
    
    # 2. 原子性地增加完成计数器
    if self.storage.incr(chord_key) == cc.size:
        # 3. 如果是最后一个完成的任务
        self.storage.delete_counter(chord_key)
        
        # 4. 收集所有任务的结果
        results = []
        for idx in range(cc.size):
            result = self.get('chord:%s:%s' % (cc.cid, idx))
            results.append(result)
        
        # 5. 将结果聚合后传递给回调任务
        callback = cc.callback
        callback.extend_data((results,))  # 结果作为元组传入
        self.enqueue(callback)
```

**依赖跟踪的关键点**：

| 组件 | 作用 | 实现方式 |
|------|------|----------|
| `cid` | Chord 唯一标识 | UUID4 |
| `chord_key` | 完成计数器键 | `'chord:{cid}'` |
| `result_key` | 成员结果存储键 | `'chord:{cid}:{idx}'` |
| `storage.incr()` | 原子计数操作 | 存储层提供 |

**为什么使用存储层计数器？**

这是一个**分布式安全**的设计：
- 多个消费者可能并发执行 chord 的成员任务
- 存储层的 `incr()` 操作通常是原子性的（如 Redis 的 INCR）
- 确保只有一个消费者会触发回调任务

### 结果聚合传递

Chord 的结果聚合是**显式的**，分为两个阶段：

#### 阶段 1：成员任务结果存储

每个成员任务完成后，结果会存储在**特殊的键**下，而不是常规的任务结果键：

```python
result_key = 'chord:%s:%s' % (cc.cid, cc.idx)
self.put_result(result_key, value)
```

#### 阶段 2：回调任务结果传递

当所有成员完成后，结果被**收集并聚合**为一个列表，传递给回调任务：

```python
results = []
for idx in range(cc.size):
    result = self.get('chord:%s:%s' % (cc.cid, idx))
    results.append(result)

callback.extend_data((results,))  # 作为位置参数传递
self.enqueue(callback)
```

**结果传递的关键细节**：

1. **参数格式**：结果列表被包装成元组 `(results,)`，然后通过 `extend_data()` 添加到回调任务的 `args` 中
2. **索引保持**：结果顺序与成员任务在 chord 中的定义顺序一致，**不依赖执行顺序**
3. **异常保留**：如果成员任务失败，异常对象（`Error` 包装）会被包含在结果列表中

**索引保持的实现**：
```python
# 入队时按顺序分配 idx
for i, task in enumerate(chord_obj.tasks):
    config = ChordConfig(cid, size, i, chord_obj.callback)
    # ...

# 收集时按 idx 顺序读取
for idx in range(cc.size):
    result = self.get('chord:%s:%s' % (cc.cid, idx))
    results.append(result)
```

**ChordResult 类**：

```python
class ChordResult(object):
    def __init__(self, results, callback_result, pipeline=None):
        self.results = ResultGroup(results)  # 成员任务结果
        self.callback = callback_result       # 回调任务结果
        self.pipeline_results = pipeline      # 回调后续的 pipeline
    
    def get(self, *args, **kwargs):
        return self.callback.get(*args, **kwargs)  # 默认获取回调结果
```

**结果获取方式**：
- `chord_result()` 或 `chord_result.get()`：获取回调任务的结果
- `chord_result.results()`：获取所有成员任务的结果列表
- `chord_result.pipeline_results()`：获取回调后续 pipeline 的结果

### 错误传播机制

Chord 的错误传播是三种机制中**最复杂**的，需要考虑多个场景。

#### 场景 1：成员任务失败但有重试

**规则**：如果成员任务有重试次数，**失败不会立即通知 chord**。

```python
# api.py:531-535
if task.chord_config is not None:
    if exception is None:
        self._check_chord(task, task_value)      # 成功：通知 chord
    elif not task.retries:
        self._check_chord(task, exception)        # 失败且无重试：通知 chord
```

**设计意图**：只有当任务**彻底失败**（重试次数耗尽）时，才会将异常传递给 chord。

#### 场景 2：成员任务彻底失败

当任务彻底失败后，**异常对象会被存储为结果**：

```python
# 存储阶段
self.put_result(result_key, value)  # value 可能是 Error 对象

# 收集阶段
result = self.get('chord:%s:%s' % (cc.cid, idx))
# 此时 result 可能是 Error 包装的异常
```

#### 场景 3：回调任务接收异常

回调任务会收到包含异常的结果列表，**由回调任务自己决定如何处理**：

```python
@huey.task()
def agg(ns):
    # ns 可能包含异常对象
    if any(isinstance(n, Exception) for n in ns):
        return -1  # 自定义错误处理
    return sum(ns)
```

**测试用例验证** (`test_api.py:1687-1704`)：
```python
def test_chord_error(self):
    c = chord([prod.s(i) for i in (1, None, 2)], agg)
    r = self.huey.enqueue(c)
    
    # 执行：
    # 1. prod(1) 成功 -> 2
    # 2. prod(None) 失败（有重试）-> 重新入队
    # 3. prod(2) 成功 -> 3
    # 4. prod(None) 重试失败（无重试了）-> 通知 chord
    # 5. agg([2, Error, 3]) 执行，返回 -1
    
    self.assertEqual(r(), -1)  # 回调检测到异常
```

#### 场景 4：成员任务的独立错误处理器

成员任务可以有自己的 `on_error` 处理器，这与 chord 的错误处理**并行执行**：

```python
# test_api.py:1792-1830
tasks = [ident.s(1), ident.s(-1).error(on_err)]
result = self.huey.enqueue(chord(tasks, agg.s()))

# 执行流程：
# 1. ident(1) 成功
# 2. ident(-1) 失败：
#    a. 触发 on_err 错误处理器（独立执行）
#    b. 同时通知 chord（因为无重试）
# 3. agg 收到 [1, TestError]
# 4. on_err 也被调用，记录 'caught'
```

**关键点**：成员任务的 `on_error` 和 chord 的回调是**两个独立的机制**：
- `on_error`：任务级别的错误处理
- chord 回调：聚合级别的结果处理

#### 场景 5：回调任务自身失败

如果回调任务失败，可以通过 chord 的 `.error()` 方法注册错误处理器：

```python
c = chord([prod.s(1), prod.s(2)], fail).error(on_err)
res = self.huey.enqueue(c)

# 执行流程：
# 1. prod(1), prod(2) 成功
# 2. fail() 执行，抛出 ValueError
# 3. on_err 被调用，接收异常
```

---

## Pipeline 机制

### 核心概念

**Pipeline**（也称为任务链）实现了**串行执行**的工作流模式。任务之间通过 `then()` 方法链接，前一个任务完成后，将结果传递给下一个任务执行。

Pipeline 的核心是：**顺序依赖 + 结果传递**。

### 执行依赖关系表达

Pipeline 的依赖关系是**链式的**：`A → B → C`，其中 B 依赖 A 的完成，C 依赖 B 的完成。

#### 依赖表达的核心：`on_complete` 属性

```python
class Task(object):
    def __init__(self, ...):
        self.on_complete = on_complete  # 指向后续任务
        self.on_error = on_error        # 指向错误处理任务
```

#### `then()` 方法：构建依赖链 (`api.py:891-901`)

```python
def then(self, task, *args, **kwargs):
    if self.on_complete:
        # 如果已有后续任务，递归添加到链的尾部
        self.on_complete.then(task, *args, **kwargs)
    else:
        if isinstance(task, Task):
            if args: task.extend_data(args)
            if kwargs: task.extend_data(kwargs)
        else:
            task = task.s(*args, **kwargs)  # 转换为 Task 实例
        self.on_complete = task
    return self
```

**链式构建示例**：
```python
# 构建 A → B → C 的 pipeline
task_a.s().then(task_b).then(task_c)

# 内部结构：
# task_a.on_complete = task_b
# task_b.on_complete = task_c
# task_c.on_complete = None
```

### 依赖跟踪机制

Pipeline 的依赖跟踪是**运行时动态触发**的，不需要存储层计数器。

#### 执行时的依赖触发 (`api.py:522-525`)

```python
if task.on_complete and exception is None:
    next_task = task.on_complete
    next_task.extend_data(task_value)  # 传递结果
    self.enqueue(next_task)             # 入队后续任务
```

**依赖跟踪的关键点**：

1. **惰性触发**：只有当前一个任务**成功完成**时，才会将后续任务入队
2. **结果传递**：前一个任务的返回值通过 `extend_data()` 传递给后续任务
3. **失败中断**：如果前一个任务失败，后续任务**不会被入队**

#### 与 Chord 结合的特殊处理

当 pipeline 作为 chord 的成员时，需要特殊处理以确保**整个 pipeline 完成**后才通知 chord：

```python
# api.py:343-352
def _enqueue_chord_member(self, task, config):
    # ...
    tail = head
    while tail.on_complete is not None:
        tail = tail.on_complete  # 找到链的尾部
    
    tail.chord_config = config  # 将配置绑定到尾部
```

**设计意图**：
- 只有当 pipeline 的**最后一个任务**完成时，才会触发 chord 的回调
- 如果 pipeline 中间某个任务失败，后续任务不会执行，chord 会收到异常

### 结果聚合传递

Pipeline 的结果传递是**链式的、渐进的**，每个任务的结果会作为下一个任务的输入。

#### `extend_data()` 方法：结果传递的核心 (`api.py:877-889`)

```python
def extend_data(self, data):
    if data is None or data == ():
        return
    
    if isinstance(data, tuple):
        self.args += data           # 作为位置参数追加
    elif isinstance(data, dict):
        for key, value in data.items():
            self.kwargs.setdefault(key, value)  # 作为关键字参数追加
    else:
        self.args = self.args + (data,)  # 单个值作为位置参数
```

**参数传递规则**：

| 前一个任务返回值 | 传递方式 | 后续任务接收到 |
|------------------|----------|----------------|
| `None` 或 `()` | 不传递 | 原始参数 |
| 单个值 `x` | 追加到 args | `(*original_args, x)` |
| 元组 `(a, b)` | 展开追加 | `(*original_args, a, b)` |
| 字典 `{'k': v}` | 合并到 kwargs | 原始 kwargs + 新键值 |

**传递示例**：
```python
@huey.task()
def add(a, b):
    return a + b

@huey.task()
def multiply(x, y=2):
    return x * y

# 构建 pipeline：add(2, 3) → multiply(?, y=10)
pipeline = add.s(2, 3).then(multiply, y=10)
result = huey.enqueue(pipeline)

# 执行流程：
# 1. add(2, 3) 执行，返回 5
# 2. 5 被传递给 multiply 的 args
# 3. multiply 实际参数为：args=(5,), kwargs={'y': 10}
# 4. multiply(5, y=10) 执行，返回 50
```

#### 多任务结果传递

当多个任务通过 pipeline 链接时，结果会**累积传递**：

```python
@huey.task()
def step1():
    return 1

@huey.task()
def step2(a):
    return a + 2

@huey.task()
def step3(b):
    return b * 3

pipeline = step1.s().then(step2).then(step3)
r1, r2, r3 = huey.enqueue(pipeline)

# 执行流程：
# 1. step1() 返回 1
# 2. step2(1) 执行，返回 3
# 3. step3(3) 执行，返回 9
#
# 结果：
# r1.get() = 1
# r2.get() = 3
# r3.get() = 9
```

### 错误传播机制

Pipeline 的错误传播是**中断式**的，同时支持独立的错误处理链。

#### 场景 1：正常错误传播（中断）

如果 pipeline 中的某个任务失败，**后续任务不会被执行**：

```python
# api.py:522-529
if task.on_complete and exception is None:
    # 只有成功时才触发后续任务
    next_task.extend_data(task_value)
    self.enqueue(next_task)
elif task.on_error and exception is not None:
    # 失败时触发错误处理任务
    next_task = task.on_error
    next_task.extend_data(exception)  # 传递异常对象
    self.enqueue(next_task)
```

**执行流程图**：
```
成功路径：A → [成功] → B → [成功] → C
失败路径：A → [失败] → on_error_A（如果有）
                ↓
              B 和 C 不会执行
```

#### 场景 2：独立错误处理链

每个任务都可以通过 `.error()` 方法注册自己的错误处理器：

```python
def error(self, task, *args, **kwargs):
    if self.on_error:
        self.on_error.error(task, *args, **kwargs)
    else:
        # ... 类似 then() 的处理
        self.on_error = task
    return self
```

**错误处理链示例**：
```python
@huey.task()
def risky_task(n):
    if n < 0:
        raise ValueError("negative")
    return n * 2

@huey.task()
def handle_error(exc):
    return f"caught: {type(exc).__name__}"

@huey.task()
def normal_task(x):
    return x + 1

# 构建带错误处理的 pipeline
pipeline = (risky_task.s(-1)
            .error(handle_error)
            .then(normal_task))

result = huey.enqueue(pipeline)

# 执行流程：
# 1. risky_task(-1) 抛出 ValueError
# 2. 因为有 on_error，handle_error 被入队
# 3. handle_error(ValueError) 执行，返回 "caught: ValueError"
# 4. normal_task 不会被执行（因为 risky_task 失败）
```

#### 场景 3：错误处理器的后续执行

错误处理器执行完成后，**可以有自己的 `then()` 链**：

```python
pipeline = (risky_task.s(-1)
            .error(handle_error).then(recover_task)
            .then(normal_task))
```

**执行流程**：
```
risky_task(-1) → [失败]
       ↓
handle_error(exc) → [成功，返回 "recovered"]
       ↓
recover_task("recovered") → [继续执行]
       ↓
normal_task(...) → [继续执行]
```

**注意**：错误处理链和正常执行链是**互斥的**，不会同时触发。

---

## 三种机制对比总结

### 核心特性对比

| 特性 | Group | Chord | Pipeline |
|------|-------|-------|----------|
| **执行模式** | 完全并行 | 并行 + 串行回调 | 完全串行 |
| **依赖关系** | 无 | 全部完成 → 回调 | 前序完成 → 后序 |
| **依赖跟踪** | 无 | 存储层计数器 | 运行时触发 |
| **结果聚合** | 独立获取 | 聚合到回调 | 链式传递 |
| **错误隔离** | 完全隔离 | 部分隔离（回调可处理） | 中断式传播 |
| **适用场景** | 批量独立任务 | Map-Reduce | 工作流编排 |

### 依赖关系表达对比

#### Group
```python
# 表达：无依赖
group([task_a.s(), task_b.s(), task_c.s()])

# 执行图：
# task_a → [独立]
# task_b → [独立]
# task_c → [独立]
```

#### Chord
```python
# 表达：全部完成后触发回调
chord([task_a.s(), task_b.s()], callback.s())

# 执行图：
# task_a ──┐
#          ├──→ 全部完成检测 → callback([a_result, b_result])
# task_b ──┘
```

#### Pipeline
```python
# 表达：链式依赖
task_a.s().then(task_b).then(task_c)

# 执行图：
# task_a → [成功则传递结果] → task_b(a_result) → [成功则传递] → task_c(b_result)
#          ↓
#        [失败则中断，触发 on_error]
```

### 结果传递对比

| 机制 | 结果存储 | 结果获取 | 结果传递 |
|------|----------|----------|----------|
| **Group** | 各任务独立的 result key | `ResultGroup.get()` | 无传递，独立获取 |
| **Chord** | 特殊的 chord key (`chord:{cid}:{idx}`) | `ChordResult.results` | 聚合为列表传递给回调 |
| **Pipeline** | 各任务独立的 result key | `ResultGroup.get()` (按顺序) | 前序结果作为后序参数 |

### 错误传播对比

#### Group 的错误传播
```
任务 A [失败] → 触发 A 的 on_error（如果有）
任务 B [成功] → 正常执行
任务 C [失败] → 触发 C 的 on_error（如果有）

获取结果时：如果任何任务失败，get() 抛出异常
```

#### Chord 的错误传播
```
任务 A [成功] → 存储结果，计数器 +1
任务 B [失败，无重试] → 存储 Error 对象，计数器 +1
任务 C [成功] → 存储结果，计数器 +1

计数器 == 3 → 触发 callback([A_result, Error, C_result])

callback 可以决定如何处理包含异常的结果列表
```

#### Pipeline 的错误传播
```
任务 A [失败]
    ↓
任务 B 不会被入队（中断）
任务 C 不会被入队（中断）
    ↓
如果 A 有 on_error → 错误处理器被执行
```

### 组合使用示例

三种机制可以相互嵌套，构建复杂的执行图：

```python
# 示例：嵌套 Chord
c = chord([
    chord([fetch.s('a'), fetch.s('b')], combine),  # 内层 chord 1
    chord([
        chord([fetch.s('c'), fetch.s('d')], combine),  # 内层 chord 2
        chord([fetch.s('e'), fetch.s('f')], combine),  # 内层 chord 3
    ], combine),  # 中层 chord
    chord([fetch.s('g'), fetch.s('h')], combine),  # 内层 chord 4
], combine)  # 外层 chord

# 执行图：
# a, b ──→ combine1
# c, d ──→ combine2 ──┐
# e, f ──→ combine3 ──┴──→ combine_mid ──┐
# g, h ──→ combine4 ─────────────────────┴──→ combine_final
```

```python
# 示例：Pipeline 与 Chord 组合
c = chord([
    step1.s(1).then(step2).then(step3),  # pipeline 作为成员
    step1.s(2).then(step2).then(step3),  # pipeline 作为成员
], combine)

# 执行图：
# 1 → step1 → step2 → step3 (尾部绑定 chord_config) ──┐
#                                                         ├──→ combine
# 2 → step1 → step2 → step3 (尾部绑定 chord_config) ──┘
#
# 只有当两个 pipeline 都完全执行完成后，combine 才会被触发
```

---

## 附录：核心代码位置

### Group 相关
| 功能 | 文件 | 行号 |
|------|------|------|
| `group` 类定义 | `huey/api.py` | 1140-1153 |
| Group 入队逻辑 | `huey/api.py` | 298-299 |
| `ResultGroup` 类定义 | `huey/api.py` | 1297-1323 |

### Chord 相关
| 功能 | 文件 | 行号 |
|------|------|------|
| `chord` 类定义 | `huey/api.py` | 1156-1169 |
| `ChordConfig` 类定义 | `huey/utils.py` | - |
| Chord 入队逻辑 | `huey/api.py` | 327-341 |
| Chord 成员处理 | `huey/api.py` | 343-359 |
| Chord 完成检测 | `huey/api.py` | 543-560 |
| `ChordResult` 类定义 | `huey/api.py` | 1325-1337 |

### Pipeline 相关
| 功能 | 文件 | 行号 |
|------|------|------|
| `Task.then()` 方法 | `huey/api.py` | 891-901 |
| `Task.error()` 方法 | `huey/api.py` | 903-913 |
| `Task.extend_data()` 方法 | `huey/api.py` | 877-889 |
| Pipeline 执行触发 | `huey/api.py` | 522-529 |

### 测试用例
| 功能 | 文件 | 测试方法 |
|------|------|----------|
| Group 基本功能 | `huey/tests/test_api.py` | `TestGroupPrimitive` 类 |
| Chord 基本功能 | `huey/tests/test_api.py` | `TestChordPrimitive` 类 |
| Pipeline 基本功能 | `huey/tests/test_api.py` | `test_schedule_s`, `test_chord_task_cb` 等 |

---

## 总结

Huey 的三种组合任务编排机制各有侧重：

1. **Group**：最简单的组合方式，适用于**完全独立**的批量任务。任务之间没有依赖，错误完全隔离。

2. **Chord**：最强大的聚合模式，实现了 **Map-Reduce** 范式。通过存储层计数器实现分布式安全的完成检测，结果会被聚合传递给回调任务。错误处理灵活，回调可以决定如何处理部分失败的情况。

3. **Pipeline**：最常用的工作流模式，实现了**串行执行**和**结果传递**。依赖关系通过 `on_complete` 属性在运行时动态触发，错误传播是中断式的，但支持独立的错误处理链。

理解这三种机制的特性和差异，是构建复杂任务编排系统的基础。通过合理组合这些机制，可以构建出表达能力强、可靠性高的异步任务执行图。
