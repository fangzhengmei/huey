# Huey 组合任务错误处理器绑定归属与触发顺序分析

## 核心修正：链式调用的绑定归属

### 关键发现

**重要修正**：`then()` 和 `error()` 方法都返回 `self`，所以链式调用时，后续方法都作用于**同一个原始对象**，不是链式绑定。

```python
# api.py:891-913
def then(self, task, *args, **kwargs):
    if self.on_complete:
        self.on_complete.then(task, *args, **kwargs)  # 递归添加到链尾
    else:
        self.on_complete = task
    return self  # 关键：返回 self！

def error(self, task, *args, **kwargs):
    if self.on_error:
        self.on_error.error(task, *args, **kwargs)  # 递归添加到错误链尾
    else:
        self.on_error = task
    return self  # 关键：返回 self！
```

---

## Task 级别的绑定分析

### 基础示例：`task_a.s().then(task_b).error(task_err)`

**执行顺序分析**：

```
1. task_a.s()
   → 返回 t_a（task_a 的 Task 实例）
   → t_a.on_complete = None
   → t_a.on_error = None

2. t_a.then(task_b)
   → 因为 t_a.on_complete 是 None
   → 设置 t_a.on_complete = task_b
   → 返回 t_a（关键！）

3. t_a.error(task_err)
   → 因为 t_a.on_error 是 None
   → 设置 t_a.on_error = task_err
   → 返回 t_a
```

**最终绑定结果**：

```
t_a.on_complete = task_b
t_a.on_error = task_err

task_b.on_complete = None
task_b.on_error = None

task_err.on_complete = None
task_err.on_error = None
```

**关键修正**：
- ❌ 错误理解：`then()` 和 `error()` 是链式绑定（`task_b` 的 `on_error` = `task_err`）
- ✅ 正确理解：`then()` 和 `error()` 是**平行**关系，都绑定到**同一个原始任务** `t_a`

### 执行路径分析

| 场景 | 触发的任务 | 说明 |
|------|-----------|------|
| `t_a` 成功 | `task_b` | 触发 `t_a.on_complete` |
| `t_a` 失败 | `task_err` | 触发 `t_a.on_error`，`task_b` 不会执行 |
| `task_b` 成功 | 无 | `task_b.on_complete` 是 None |
| `task_b` 失败 | 无 | `task_b.on_error` 是 None |

---

## 更复杂的链式调用分析

### 示例：`task_a.s().then(task_b).then(task_c).error(task_err)`

**执行顺序分析**：

```
1. task_a.s() → t_a

2. t_a.then(task_b)
   → t_a.on_complete = task_b（因为是 None）
   → 返回 t_a

3. t_a.then(task_c)
   → 因为 t_a.on_complete 不是 None（是 task_b）
   → 递归调用：t_a.on_complete.then(task_c) → task_b.then(task_c)
   → task_b.on_complete = task_c（因为 task_b.on_complete 是 None）
   → 返回 t_a

4. t_a.error(task_err)
   → t_a.on_error = task_err（因为是 None）
   → 返回 t_a
```

**最终绑定结果**：

```
t_a.on_complete = task_b
t_a.on_error = task_err

task_b.on_complete = task_c
task_b.on_error = None

task_c.on_complete = None
task_c.on_error = None

task_err.on_complete = None
task_err.on_error = None
```

**执行路径图**：

```
成功路径：
t_a 成功 → task_b 成功 → task_c 成功 → 完成
     ↓           ↓
  [on_complete] [on_complete]

失败路径 1：
t_a 失败 → task_err 执行 → 完成
     ↓
  [on_error]
  （task_b 和 task_c 不会执行）

失败路径 2：
t_a 成功 → task_b 失败 → 无后续
     ↓           ↓
  [on_complete] [无 on_error]
  （task_c 不会执行）
```

### 关键发现总结

| 调用顺序 | 绑定位置 | 原因 |
|----------|----------|------|
| 第一个 `then()` | `t_a.on_complete` | `t_a.on_complete` 是 None |
| 后续 `then()` | 链尾任务的 `on_complete` | 递归添加 |
| 任何 `error()` | `t_a.on_error` | 返回 `t_a`，`t_a.on_error` 是 None |

---

## Group 机制的绑定分析

### `group.error()` 的实现

```python
# api.py:1149-1153
def error(self, *args, **kwargs):
    # Apply error handler to all tasks.
    for task in self.tasks:
        task.error(*args, **kwargs)  # 为每个成员注册
    return self  # 返回 self (group)
```

### `group.then()` 的实现（特殊！）

```python
# api.py:1144-1147
def then(self, task, *args, **kwargs):
    if not isinstance(task, Task):
        task = task.s(*args, **kwargs)
    return chord(self.tasks, task)  # 关键：返回 chord，不是 self！
```

### 示例：`group([a, b]).error(on_err).then(combine)`

**执行顺序分析**：

```
1. group([a, b])
   → 创建 g，g.tasks = [t_a, t_b]

2. g.error(on_err)
   → 遍历 g.tasks：
     - t_a.error(on_err) → t_a.on_error = on_err
     - t_b.error(on_err) → t_b.on_error = on_err
   → 返回 g（关键！）

3. g.then(combine)
   → 因为 group.then() 特殊
   → 返回 chord([t_a, t_b], combine)
```

**最终绑定结果**：

```
这是一个 chord！
chord.callback = combine

t_a.on_error = on_err
t_b.on_error = on_err

t_a.on_complete = None
t_b.on_complete = None
```

**关键修正**：
- `group.error()` 为**每个成员任务**注册错误处理器
- `group.then()` 会**将 group 转换为 chord**，不再是纯 group
- 链式调用中，`error()` 和 `then()` 的顺序可能影响最终类型

### 执行路径分析

**成员任务独立执行**：
- `t_a` 成功/失败 → 独立处理
- `t_b` 成功/失败 → 独立处理

**错误处理器触发**：
- 如果 `t_a` 失败 → 触发 `t_a.on_error = on_err`
- 如果 `t_b` 失败 → 触发 `t_b.on_error = on_err`
- 两个 `on_err` 会**独立执行**（如果两个任务都失败）

**Chord 回调触发**：
- 所有成员完成后（无论成功失败）→ 触发 `combine`
- `combine` 收到的结果列表可能包含异常

---

## Chord 机制的绑定分析

### `chord.then()` 和 `chord.error()` 的实现

```python
# api.py:1163-1169
def then(self, task, *args, **kwargs):
    self.callback.then(task, *args, **kwargs)  # 操作 callback 的 on_complete
    return self  # 返回 self (chord)

def error(self, task, *args, **kwargs):
    self.callback.error(task, *args, **kwargs)  # 操作 callback 的 on_error
    return self  # 返回 self (chord)
```

### 示例：`chord([a, b], callback).then(finished).error(err)`

**执行顺序分析**：

```
1. chord([a, b], callback)
   → 创建 c，c.callback = callback

2. c.then(finished)
   → c.callback.then(finished) → callback.then(finished)
   → callback.on_complete = finished
   → 返回 c（关键！）

3. c.error(err)
   → c.callback.error(err) → callback.error(err)
   → callback.on_error = err
   → 返回 c
```

**最终绑定结果**：

```
c.callback = callback

callback.on_complete = finished
callback.on_error = err

t_a.on_complete = None（除非单独设置）
t_a.on_error = None（除非单独设置）
t_b.on_complete = None（除非单独设置）
t_b.on_error = None（除非单独设置）
```

**关键修正**：
- `chord.then()` 操作的是 **callback 的 `on_complete`**
- `chord.error()` 操作的是 **callback 的 `on_error`**
- 成员任务的绑定不受影响（除非单独设置）

### 执行路径分析

**成员任务阶段**：
- `t_a` 和 `t_b` 独立执行
- 如果成员有自己的 `on_error`，失败时会触发

**回调触发条件**：
- 所有成员完成（无论成功失败）
- 成员失败但有重试 → 等待重试结果
- 成员失败且无重试 → 异常作为结果

**回调执行阶段**：

| 场景 | 触发的任务 |
|------|-----------|
| `callback` 成功 | `callback.on_complete = finished` |
| `callback` 失败 | `callback.on_error = err` |

### 测试用例验证

**测试用例**：`test_nested_chord_callback_pipeline_tail_walking` (`test_api.py:2045-2070`)

```python
@self.huey.task()
def incr(n):
    return n + 1

@self.huey.task()
def agg(ns):
    return sum(ns)  # 没有异常检测！

@self.huey.task()
def finished(res):
    return res * 10

@self.huey.task()
def err(exc):
    state.append(99)
    return -1

# 构建
c = chord([incr.s(i) for i in range(2)], agg).then(finished).error(err)
```

**绑定结果**：
```
chord.callback = agg
agg.on_complete = finished
agg.on_error = err
```

**成功场景执行**：
```
1. incr(0) 成功 → 1
2. incr(1) 成功 → 2
3. 所有成员完成 → 触发 agg([1, 2])
4. agg([1, 2]) 成功 → sum([1, 2]) = 3
5. agg 成功 → 触发 agg.on_complete = finished
6. finished(3) 执行 → 30
```

**失败场景执行**：
```
1. incr(1) 成功 → 2
2. incr(None) 失败 → TypeError（None + 1）
3. 所有成员完成 → 触发 agg([2, TypeError])
4. agg([2, TypeError]) 执行 → sum([2, TypeError]) 抛出 TypeError
5. agg 失败 → 触发 agg.on_error = err
6. err(TypeError) 执行 → state.append(99)，返回 -1
```

**测试用例期望**：
- 成功场景：`r.pipeline_results()` = `[3, 30]`
- 失败场景：`r()` 抛出 TaskException，`state = [99]`，最后返回 `-1`

这与测试用例完全一致！

---

## 绑定归属总结表

### 方法绑定目标

| 方法 | 绑定目标 | 返回值 |
|------|----------|--------|
| `Task.then()` | `self.on_complete`（无则设置，有则递归到链尾） | `self` |
| `Task.error()` | `self.on_error`（无则设置，有则递归到错误链尾） | `self` |
| `group.then()` | 创建新 `chord`，`chord.callback` = task | `chord` |
| `group.error()` | 每个成员的 `on_error` | `self` (group) |
| `chord.then()` | `callback.on_complete` | `self` (chord) |
| `chord.error()` | `callback.on_error` | `self` (chord) |

### 链式调用的绑定位置

| 调用示例 | `then()` 绑定位置 | `error()` 绑定位置 |
|----------|-------------------|-------------------|
| `task_a.s().then(b).error(e)` | `task_a.on_complete = b` | `task_a.on_error = e` |
| `task_a.s().error(e).then(b)` | `task_a.on_complete = b` | `task_a.on_error = e` |
| `task_a.s().then(b).then(c).error(e)` | `task_a.on_complete = b`<br>`b.on_complete = c` | `task_a.on_error = e` |
| `group([a,b]).error(e).then(c)` | 转换为 chord，`callback = c` | `a.on_error = e`<br>`b.on_error = e` |
| `chord([a,b],cb).then(f).error(e)` | `cb.on_complete = f` | `cb.on_error = e` |

---

## 失败后的执行路径分析

### 核心执行逻辑

```python
# api.py:522-529
if task.on_complete and exception is None:
    # 成功：触发 on_complete
    next_task.extend_data(task_value)
    self.enqueue(next_task)
elif task.on_error and exception is not None:
    # 失败：触发 on_error
    next_task.extend_data(exception)
    self.enqueue(next_task)
```

**关键规则**：
1. 成功时：只触发 `on_complete` 链
2. 失败时：只触发 `on_error` 链
3. 两个链**互斥**，不会同时触发

### Pipeline 的失败路径

#### 示例 1：`task_a.s().then(task_b).then(task_c).error(task_err)`

**绑定结果**：
```
task_a.on_complete = task_b
task_a.on_error = task_err
task_b.on_complete = task_c
```

**执行路径矩阵**：

| 执行节点 | 结果 | 后续执行 | 说明 |
|----------|------|----------|------|
| `task_a` | 成功 | 触发 `task_b` | `task_a.on_complete` |
| `task_a` | 失败 | 触发 `task_err` | `task_a.on_error`，`task_b`/`task_c` 不执行 |
| `task_b` | 成功 | 触发 `task_c` | `task_b.on_complete` |
| `task_b` | 失败 | 无后续 | `task_b.on_error` 是 None，`task_c` 不执行 |
| `task_c` | 成功/失败 | 无后续 | `task_c.on_complete/on_error` 是 None |

#### 示例 2：每个任务都有错误处理器

```python
task_a.s()
    .error(task_err_a)
    .then(task_b.error(task_err_b))
    .then(task_c.error(task_err_c))
```

**绑定结果**：
```
task_a.on_error = task_err_a
task_a.on_complete = task_b
task_b.on_error = task_err_b
task_b.on_complete = task_c
task_c.on_error = task_err_c
```

**执行路径矩阵**：

| 执行节点 | 结果 | 后续执行 |
|----------|------|----------|
| `task_a` | 成功 | 触发 `task_b` |
| `task_a` | 失败 | 触发 `task_err_a` |
| `task_b` | 成功 | 触发 `task_c` |
| `task_b` | 失败 | 触发 `task_err_b` |
| `task_c` | 成功 | 无后续 |
| `task_c` | 失败 | 触发 `task_err_c` |

### Chord 的失败路径

#### 成员任务失败

```python
chord([
    task_a.s().error(err_a),
    task_b.s().error(err_b),
], callback)
```

**执行路径**：
1. `task_a` 失败 → 触发 `err_a`（独立执行）
2. `task_b` 成功 → 正常完成
3. 所有成员完成 → 触发 `callback([Error, b_result])`

**关键点**：
- 成员的 `on_error` 和 chord 回调**独立执行**
- 回调收到的结果列表包含异常对象

#### 回调任务失败

```python
chord([a, b], callback).error(err_handler)
```

**执行路径**：
1. `a`, `b` 完成 → 触发 `callback`
2. `callback` 失败 → 触发 `err_handler`
3. `err_handler` 成功/失败 → 根据其 `on_complete/on_error` 继续

### 错误恢复机制

错误处理器本身也是 Task，执行时遵循同样的规则：

```python
# 构建：风险任务 → 错误处理器 → 恢复任务
risky.s().error(handle_err.then(recover_task))
```

**绑定结果**：
```
risky.on_error = handle_err
handle_err.on_complete = recover_task
```

**执行路径**：
1. `risky` 失败 → 触发 `handle_err`
2. `handle_err` 成功 → 触发 `recover_task`（`handle_err.on_complete`）
3. `handle_err` 失败 → 触发 `handle_err.on_error`（如果有）

**关键发现**：错误处理链可以有自己的正常链，实现从错误中**恢复执行**。

---

## 常见误区纠正

### 误区 1：`then()` 和 `error()` 是链式绑定

**纠正**：两个方法都返回 `self`，所以链式调用时，后续方法都作用于**同一个原始对象**。

```python
# ❌ 错误理解
task_a.then(task_b).error(task_err)
# 认为：task_a.on_complete = task_b, task_b.on_error = task_err

# ✅ 正确理解
# 实际：task_a.on_complete = task_b, task_a.on_error = task_err
```

### 误区 2：`group.then()` 是 group 的特性

**纠正**：`group.then()` 会**创建新的 chord**，返回的是 chord 实例，不是 group。

```python
g = group([a, b]).then(c)
# type(g) 是 chord，不是 group！
```

### 误区 3：`chord.error()` 绑定到成员任务

**纠正**：`chord.error()` 绑定到 **callback 的 `on_error`**，不是成员任务。

```python
chord([a, b], callback).error(err)
# err 绑定到 callback.on_error，不是 a.on_error 或 b.on_error
```

### 误区 4：Pipeline 中间任务失败会触发第一个任务的 error

**纠正**：每个任务的 `on_error` 只在**自己失败**时触发。

```python
task_a.s().then(task_b).error(task_err)
# task_err 绑定到 task_a.on_error
# 如果 task_b 失败，不会触发 task_err（因为 task_b.on_error 是 None）
```

---

## 附录：完整示例分析

### 示例 1：复杂 Pipeline

```python
@huey.task()
def step1(n):
    return n + 1

@huey.task()
def step2(n):
    if n > 10:
        raise ValueError("too big")
    return n * 2

@huey.task()
def step3(n):
    return n - 3

@huey.task()
def err1(exc):
    return "recovered from step1"

@huey.task()
def err2(exc):
    return "recovered from step2"

# 构建
pipe = (step1.s(5)
         .error(err1)
         .then(step2.error(err2))
         .then(step3))
```

**绑定分析**：
```
1. step1.s(5) → t1
2. t1.error(err1) → t1.on_error = err1，返回 t1
3. t1.then(step2.error(err2))
   → step2.error(err2) 先执行：step2.on_error = err2
   → t1.then(step2)：t1.on_complete = step2
   → 返回 t1
4. t1.then(step3)
   → t1.on_complete.then(step3) → step2.then(step3)
   → step2.on_complete = step3
   → 返回 t1
```

**最终绑定**：
```
t1.on_complete = step2
t1.on_error = err1

step2.on_complete = step3
step2.on_error = err2

step3.on_complete = None
step3.on_error = None

err1.on_complete = None
err1.on_error = None

err2.on_complete = None
err2.on_error = None
```

**执行路径**：

| 场景 | 执行流程 | 最终结果 |
|------|----------|----------|
| 全部成功 | `step1(5)=6` → `step2(6)=12` → `step3(12)=9` | `9` |
| `step2` 失败 | `step1=6` → `step2(6)` 抛异常 → `err2` 执行 | `"recovered from step2"` |
| `step1` 失败 | `step1` 抛异常 → `err1` 执行 | `"recovered from step1"` |

### 示例 2：嵌套 Chord

```python
@huey.task()
def fetch(n):
    return n

@huey.task()
def combine(results):
    return sum(results)

@huey.task()
def post_process(n):
    return n * 100

@huey.task()
def handle_error(exc):
    return -1

# 构建
c = (chord([
         chord([fetch.s('a'), fetch.s('b')], combine),
         chord([fetch.s('c'), fetch.s('d')], combine),
     ], combine)
     .then(post_process)
     .error(handle_error))
```

**绑定分析**：
```
外层 chord.callback = combine
combine.on_complete = post_process
combine.on_error = handle_error
```

**执行路径**：
1. 内层 4 个 `fetch` 并行执行
2. 两个内层 `combine` 分别执行
3. 外层 `combine` 执行（聚合两个内层结果）
4. 如果外层 `combine` 成功 → 触发 `post_process`
5. 如果外层 `combine` 失败 → 触发 `handle_error`

---

## 附录：关键代码位置

| 功能 | 文件 | 行号 |
|------|------|------|
| `Task.then()` 实现 | `huey/api.py` | 891-901 |
| `Task.error()` 实现 | `huey/api.py` | 903-913 |
| `group.then()` 实现 | `huey/api.py` | 1144-1147 |
| `group.error()` 实现 | `huey/api.py` | 1149-1153 |
| `chord.then()` 实现 | `huey/api.py` | 1163-1165 |
| `chord.error()` 实现 | `huey/api.py` | 1167-1169 |
| 核心执行逻辑（互斥链） | `huey/api.py` | 522-529 |
| 测试用例：chord 回调的 then/error 链 | `huey/tests/test_api.py` | 2045-2070 |
