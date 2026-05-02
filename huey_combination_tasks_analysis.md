# Huey 组合任务错误处理机制深度分析

## 核心修正：正常链与错误链是互斥的

### 关键代码分析

```python
# api.py:522-529
if task.on_complete and exception is None:
    next_task = task.on_complete
    next_task.extend_data(task_value)
    self.enqueue(next_task)
elif task.on_error and exception is not None:
    next_task = task.on_error
    next_task.extend_data(exception)
    self.enqueue(next_task)
```

**重要发现**：使用 `if...elif` 结构，确保两个链**完全互斥**，不会同时触发。

| 执行结果 | 触发的链 | 说明 |
|----------|----------|------|
| 成功 (`exception is None`) | `on_complete` 链 | 正常执行链继续 |
| 失败 (`exception is not None`) | `on_error` 链 | 错误处理链触发，正常链中断 |

### 链式结构说明

每个 Task 有两个独立的链：
```python
class Task:
    on_complete = None  # 正常执行链（成功时触发）
    on_error = None     # 错误处理链（失败时触发）
```

**链的构建方式**：
- `then()` 方法构建 `on_complete` 链
- `error()` 方法构建 `on_error` 链

---

## Group 机制的错误处理

### 核心特性

| 特性 | 行为 |
|------|------|
| 任务独立性 | 成员任务完全独立，互不影响 |
| 错误隔离 | 一个任务失败，其他任务继续执行 |
| 错误处理器作用域 | `group.error()` 为**每个成员任务**注册错误处理器 |

### `group.error()` 的实现

```python
# api.py:1149-1153
def error(self, *args, **kwargs):
    # Apply error handler to all tasks.
    for task in self.tasks:
        task.error(*args, **kwargs)
    return self
```

**关键点**：遍历所有成员任务，为**每个任务**单独注册错误处理器。

### 执行流程图解

```python
g = group([task_a.s(1), task_a.s(-1), task_a.s(3)])
g.error(on_err)
```

```
入队后队列：[task_a(1), task_a(-1), task_a(3)]

执行流程：
1. task_a(1) 成功 → 无 on_complete，无后续
2. task_a(-1) 失败 → 触发 on_err（入队）
3. task_a(3) 成功 → 无 on_complete，无后续
4. on_err 执行 → 处理异常

结果：
- task_a(1): 成功完成
- task_a(-1): 失败，错误处理器执行
- task_a(3): 成功完成
```

### 重要发现：`group.then()` 会转换为 chord

```python
# api.py:1144-1147
def then(self, task, *args, **kwargs):
    if not isinstance(task, Task):
        task = task.s(*args, **kwargs)
    return chord(self.tasks, task)  # 关键：返回 chord！
```

**测试用例验证** (`test_api.py:1532-1553`)：
```python
result = self.huey.enqueue(
    group([fetch.s(2), fetch.s(3)])
    .error(on_err)
    .then(combine))  # then() 触发转换为 chord

# 实际等同于：
# chord([
#     fetch.s(2).error(on_err), 
#     fetch.s(3).error(on_err)
# ], combine)
```

**执行流程**：
```
1. fetch(2) 成功
2. fetch(3) 成功
3. 所有成员完成 → 触发 combine([2, 3])
4. combine 返回 5
```

### Group 失败后是否继续执行的结论

| 场景 | 是否继续执行 | 说明 |
|------|-------------|------|
| 单个成员任务失败 | **是** | 其他成员任务继续执行 |
| 成员任务有 on_error | **是** | 错误处理器被触发，独立执行 |
| group 有 `.then()` | 触发 chord 逻辑 | group 被转换为 chord |

---

## Chord 机制的错误处理

### 核心特性

| 特性 | 行为 |
|------|------|
| 成员独立性 | 成员任务之间独立执行 |
| 聚合触发 | 所有成员完成后（无论成功失败）触发回调 |
| 回调链 | 回调任务有自己的 `on_complete` 和 `on_error` 链 |

### `chord.then()` 和 `chord.error()` 的实现

```python
# api.py:1163-1169
def then(self, task, *args, **kwargs):
    self.callback.then(task, *args, **kwargs)  # 操作回调的 on_complete
    return self

def error(self, task, *args, **kwargs):
    self.callback.error(task, *args, **kwargs)  # 操作回调的 on_error
    return self
```

**关键点**：Chord 的 `then()` 和 `error()` 操作的是**回调任务**的链，不是成员任务的。

### 成员任务失败的处理

#### 有重试的情况

```python
# api.py:531-535
if task.chord_config is not None:
    if exception is None:
        self._check_chord(task, task_value)      # 成功：通知 chord
    elif not task.retries:
        self._check_chord(task, exception)        # 失败且无重试：通知 chord
```

**规则**：
- 成员任务失败但**有重试** → 不通知 chord，等待重试结果
- 成员任务失败且**无重试** → 立即通知 chord（异常作为结果）

#### 无重试的情况

当所有成员完成后（包括失败的）：

```python
# api.py:543-560
def _check_chord(self, task, value):
    cc = task.chord_config
    result_key = 'chord:%s:%s' % (cc.cid, cc.idx)
    self.put_result(result_key, value)  # value 可能是异常对象
    
    if self.storage.incr(chord_key) == cc.size:
        # 收集所有结果（包括异常）
        results = []
        for idx in range(cc.size):
            result = self.get('chord:%s:%s' % (cc.cid, idx))
            results.append(result)
        
        # 传递给回调
        callback = cc.callback
        callback.extend_data((results,))
        self.enqueue(callback)
```

### 完整执行流程图解

**场景 1：成员任务部分失败**
```python
c = chord([prod.s(1), prod.s(None), prod.s(2)], agg)
# prod 有 retries=1
```

```
执行流程：
1. prod(1) 成功 → 结果 2 存储，计数器 +1
2. prod(None) 第1次失败 → 有重试，不通知 chord，重新入队
3. prod(2) 成功 → 结果 3 存储，计数器 +1
4. prod(None) 第2次失败 → 无重试，异常存储，计数器 +1
5. 计数器 == 3 → 收集结果 [2, Error, 3]，触发 agg
6. agg([2, Error, 3]) 执行，返回 -1（检测到异常）
```

**场景 2：回调任务失败**
```python
c = chord([prod.s(1), prod.s(2)], fail).error(on_err)
# fail 任务会抛出 ValueError
```

```
执行流程：
1. prod(1) 成功 → 2
2. prod(2) 成功 → 3
3. 触发 fail([2, 3]) → 抛出 ValueError
4. fail 失败 → 触发 on_err（因为 fail.on_error = on_err）
5. on_err(ValueError) 执行，返回 'done'
```

**场景 3：回调的正常链和错误链**
```python
c = chord([incr.s(1), incr.s(2)], agg).then(finished).error(err)
```

**内部结构**：
```
chord.callback = agg
  ├── agg.on_complete = finished  (chord.then() 设置)
  └── agg.on_error = err          (chord.error() 设置)
```

**成功路径**：
```
1. incr(1), incr(2) 成功
2. agg([2, 3]) 成功 → 返回 5
3. agg 成功 → 触发 finished（agg.on_complete）
4. finished(5) 执行 → 返回 50
```

**失败路径**：
```
1. incr(1) 成功, incr(None) 失败
2. agg([2, Error]) 执行 → 可能抛出异常
3. agg 失败 → 触发 err（agg.on_error）
4. err(exception) 执行 → 返回 -1
```

### 成员任务的独立错误处理器

成员任务可以有自己的 `on_error`，这与 chord 回调**独立执行**：

```python
tasks = [ident.s(1), ident.s(-1).error(on_err)]
result = self.huey.enqueue(chord(tasks, agg.s()))
```

```
执行流程：
1. ident(1) 成功 → 结果 1 存储
2. ident(-1) 失败：
   a. 触发 on_err（ident(-1).on_error）→ on_err 入队
   b. 无重试 → 异常存储，通知 chord
3. on_err 执行 → 记录 'caught'
4. 所有成员完成 → 触发 agg([1, TestError])
5. agg 执行 → 返回 [1, TestError]
```

**关键点**：
- `on_err`（成员的错误处理器）和 `agg`（chord 回调）**都会执行**
- 两者是独立的机制，互不干扰

### Chord 失败后是否继续执行的结论

| 场景 | 是否继续执行 | 说明 |
|------|-------------|------|
| 成员任务失败（有重试） | **等待重试** | 不通知 chord，等待重试结果 |
| 成员任务失败（无重试） | **触发回调** | 异常作为结果，通知 chord |
| 成员有独立 on_error | **并行执行** | on_error 和 chord 回调都执行 |
| 回调任务成功 | **触发回调的 on_complete** | 正常链继续 |
| 回调任务失败 | **触发回调的 on_error** | 错误处理链触发 |

---

## Pipeline 机制的错误处理

### 核心特性

| 特性 | 行为 |
|------|------|
| 串行依赖 | 前序任务完成后才执行后序 |
| 中断机制 | 前序失败 → 正常链中断 |
| 错误恢复 | 错误处理器成功后，可触发其正常链 |

### 中断机制分析

```python
# api.py:522-529
if task.on_complete and exception is None:
    # 成功时才触发 on_complete
    next_task.extend_data(task_value)
    self.enqueue(next_task)
elif task.on_error and exception is not None:
    # 失败时触发 on_error
    next_task.extend_data(exception)
    self.enqueue(next_task)
```

**关键规则**：
- **正常链中断**：任务失败 → `on_complete` 链不会被触发
- **错误链触发**：任务失败 → `on_error` 链会被触发（如果有）

### 执行流程图解

**场景 1：正常执行**
```python
pipe = step1.s().then(step2).then(step3)
```

```
内部结构：
step1.on_complete = step2
step2.on_complete = step3
step3.on_complete = None
```

```
执行流程：
1. step1 成功 → 触发 step2（step1.on_complete）
2. step2 成功 → 触发 step3（step2.on_complete）
3. step3 成功 → 完成
```

**场景 2：中间失败（无错误处理器）**
```python
pipe = step1.s().then(step2_fail).then(step3)
# step2_fail 会抛出异常
```

```
执行流程：
1. step1 成功 → 触发 step2_fail
2. step2_fail 失败：
   - 无 on_error → 无错误处理器
   - 不触发 on_complete → step3 不会被入队
3. 执行中断，step3 永远不会执行
```

**场景 3：中间失败（有错误处理器）**
```python
pipe = step1.s().then(step2_fail).error(on_err).then(recover)
```

**内部结构**：
```
step1.on_complete = step2_fail
step2_fail.on_error = on_err
step2_fail.on_complete = recover  # 注意：then() 加在 step2_fail 上！
```

```
执行流程：
1. step1 成功 → 触发 step2_fail
2. step2_fail 失败：
   - 不触发 on_complete → recover 不会执行
   - 触发 on_error → on_err 入队
3. on_err 执行：
   - 如果 on_err 成功 → 触发 on_err.on_complete（如果有）
   - 如果 on_err 失败 → 触发 on_err.on_error（如果有）
```

### 错误恢复机制

错误处理器本身也是 Task，执行时遵循同样的规则：

```python
# 构建：风险任务 → 错误处理器 → 恢复任务
risky_task.s().error(handle_err.then(recover_task))
```

**内部结构**：
```
risky_task.on_error = handle_err
handle_err.on_complete = recover_task
```

```
执行流程：
1. risky_task 失败 → 触发 handle_err
2. handle_err 成功 → 触发 recover_task（handle_err.on_complete）
3. recover_task 执行 → 从错误中恢复
```

**关键发现**：错误处理链可以有自己的正常链，实现从错误中**恢复执行**。

### 完整示例

```python
@huey.task()
def risky(n):
    if n < 0:
        raise ValueError("negative")
    return n * 2

@huey.task()
def handle_err(exc):
    return 0  # 从错误中恢复，返回默认值

@huey.task()
def finalize(value):
    return value + 100

# 构建：risky(-1) 失败 → handle_err 恢复 → finalize 继续
pipe = risky.s(-1).error(handle_err.then(finalize))
```

```
执行流程：
1. risky(-1) 失败 → 触发 handle_err
2. handle_err 成功（返回 0）→ 触发 finalize（handle_err.on_complete）
3. finalize(0) 执行 → 返回 100

结果：最终返回 100，从错误中成功恢复
```

### Pipeline 失败后是否继续执行的结论

| 场景 | 是否继续执行 | 说明 |
|------|-------------|------|
| 前序任务成功 | **是** | 触发 on_complete 链 |
| 前序任务失败（无 on_error） | **否** | 正常链中断，无后续 |
| 前序任务失败（有 on_error） | **错误链继续** | 触发 on_error 链，正常链中断 |
| on_error 任务成功 | **是** | 触发 on_error 的 on_complete 链（可恢复） |
| on_error 任务失败 | **错误链继续** | 触发 on_error 的 on_error 链 |

---

## 三种机制对比总结

### 错误处理行为对比

| 维度 | Group | Chord | Pipeline |
|------|-------|-------|----------|
| **正常链与错误链关系** | 互斥（每个任务独立） | 互斥（回调独立） | **互斥（关键修正）** |
| **失败后的继续行为** | 其他任务独立继续 | 回调在所有成员完成后触发 | 正常链中断，错误链触发 |
| **错误处理器作用域** | 每个成员独立 | 回调独立，成员可独立注册 | 链式传递 |
| **结果传递** | 独立获取 | 聚合到回调 | 链式传递 |

### 关键代码位置总结

| 功能 | 文件 | 行号 |
|------|------|------|
| 核心执行逻辑（互斥链） | `huey/api.py` | 522-529 |
| `then()` 构建 on_complete 链 | `huey/api.py` | 891-901 |
| `error()` 构建 on_error 链 | `huey/api.py` | 903-913 |
| `group.error()` 实现 | `huey/api.py` | 1149-1153 |
| `group.then()` 转换为 chord | `huey/api.py` | 1144-1147 |
| `chord.then()/error()` 实现 | `huey/api.py` | 1163-1169 |
| Chord 完成检测 | `huey/api.py` | 543-560 |

---

## 常见误区纠正

### 误区 1：错误处理链和正常链并行执行

**纠正**：两个链是**互斥**的，使用 `if...elif` 确保不会同时触发。

### 误区 2：`group.then()` 是 group 的特性

**纠正**：`group.then()` 会**将 group 转换为 chord**，不再是纯 group 行为。

### 误区 3：Pipeline 失败后完全停止

**纠正**：正常链停止，但**错误处理链会触发**。如果错误处理器成功，它的正常链会继续执行（可实现恢复）。

### 误区 4：Chord 成员失败会阻止回调

**纠正**：成员失败（无重试）时，**异常会作为结果存储**，回调仍会触发，由回调决定如何处理包含异常的结果列表。

---

## 附录：测试用例验证

### 测试用例位置

| 测试场景 | 测试类/方法 |
|----------|-------------|
| Group 错误处理器 | `TestGroupPrimitive.test_group_error_handler` |
| Group 链式调用 | `TestGroupPrimitive.test_group_error_chaining` |
| Chord 回调失败 | `TestChordPrimitive.test_chord_callback_err` |
| Chord 成员错误 | `TestChordPrimitive.test_chord_error` |
| Chord 成员独立错误处理器 | `TestChordPrimitive.test_chord_member_error_callback_independent` |
| Chord 回调的 then/error 链 | `TestChordPrimitive.test_nested_chord_callback_pipeline_tail_walking` (约 2045 行) |
| Pipeline 基本链式 | `TestTaskChaining` |

### 关键测试验证

**验证 1：正常链与错误链互斥** (`test_api.py:1647-1669`)
```python
c = chord([prod.s(1), prod.s(2)], fail).error(on_err)
# fail 会抛出 ValueError
# 执行后：fail 失败 → 只触发 on_err，不触发其他
```

**验证 2：Group.then() 转换为 chord** (`test_api.py:1532-1553`)
```python
group([fetch.s(2), fetch.s(3)]).error(on_err).then(combine)
# 实际执行：
# 1. fetch(2), fetch(3) 成功
# 2. 触发 combine([2, 3]) → chord 行为
```

**验证 3：Pipeline 中断与恢复**
- 正常链：`then()` 构建的链只在成功时触发
- 错误链：`error()` 构建的链只在失败时触发
- 恢复机制：错误处理器成功后，其 `on_complete` 会触发
