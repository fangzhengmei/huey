# Huey 消费者进程停止机制摘要

---

## 结论

### 1. 非优雅退出时在途任务中断保证

| Worker 模式 | 主动中断机制 | 是否必然上报 `SIGNAL_INTERRUPTED` |
|-------------|--------------|-----------------------------------|
| **Greenlet** | `gevent.killall()` | ✅ 是 |
| **Process** | 依赖 OS 发 SIGTERM 到 daemon 子进程 | ⚠️ **非必然**（取决于任务时长、主进程退出速度） |
| **Thread** | 无任何中断 | ❌ 否 |

### 2. 主流程停止 vs 子执行单元中断

- **主流程停止**：收到信号 → `stop_flag.set()` → 优雅模式 `join()` 等待，非优雅模式立即返回
- **子执行单元中断**：只有 Greenlet 主动中断；Process 模式主进程**不主动发信号**，依赖 daemon 被 OS 发送 SIGTERM；Thread 模式无中断

### 3. 周期任务触发机制

- **不预计算**未来触发点
- **固定节奏轮询**：每 60 秒检查**当前时间**是否匹配 crontab
- **错过检查点直接跳过**，无补偿机制

---

## 证据

### 证据 1：只有 Greenlet 主动中断

**位置：`huey/consumer.py:572-581`**

```python
def _handle_stop_signal(self, sig_num, frame):
    # ...
    if self.worker_type == WORKER_GREENLET:  # 只有 Greenlet 模式
        def kill_workers():
            gevent.killall([t for _, t in self.worker_threads],
                           KeyboardInterrupt)
        gevent.spawn(kill_workers)
```

### 证据 2：Process 模式非优雅退出不 join()

**位置：`huey/consumer.py:444-465`**

```python
def stop(self, graceful=False):
    self.stop_flag.set()
    if graceful:
        worker_process.join()  # 优雅模式：等待
    else:
        self._logger.info('Shutting down')  # 非优雅模式：不等待！
```

### 证据 3：stop_flag 只在循环开头检查

**位置：`huey/consumer.py:386-402`**

```python
while not self.stop_flag.is_set():  # 只在这里检查
    process.loop()  # execute() 期间不再检查
```

### 证据 4：周期任务检查当前时间

**位置：`huey/api.py:716-720`**

```python
def read_periodic(self, timestamp):
    return [task for task in self._registry.periodic_tasks
            if task.validate_datetime(timestamp)]  # 检查当前时间戳
```

---

## 边界

### 边界 1：Process 模式非优雅退出的不确定性

| 子进程状态 | 结果 |
|------------|------|
| 正在 `huey.execute()` 中 | 继续执行，直到任务完成或主进程退出后被 OS 发送 SIGTERM |
| 任务很长、主进程很快退出 | 可能收到 OS 的 SIGTERM → 抛出 `KeyboardInterrupt` → 上报 `SIGNAL_INTERRUPTED` |
| 任务很短、主进程退出慢 | 任务可能正常完成 → 不上报 |

### 边界 2：Thread 模式的风险

- 非优雅退出时，daemon 线程被强制终止，**无任何异常或信号**
- 在途任务状态完全不确定

### 边界 3：周期任务错过检查点

- 检查点间隔：60 秒
- 检查时机：`_next_periodic <= time.monotonic()` 时
- 如果系统在检查点之间休眠/阻塞，该时间点的任务**不执行**
- `_next_periodic += 60` 只前进 60 秒，不补偿

---

*文档版本：4.0（极简版）| 生成时间：2026-04-29*
