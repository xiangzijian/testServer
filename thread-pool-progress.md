# 线程管理 / 线程池 建设进度

> 本文件记录线程池模块的目标、方案和进度。每完成一步或方案有变化时更新。
> 代码位置：`test-common/src/main/java/com/slg/common/thread/`
> 关联设计：`slg-server-design.md` 第 6 节（线程与并发模型）、第 7 节（定时事件引擎）

最后更新：2026-11-13

---

## 一、最初目标

为项目搭建统一的线程管理和线程池执行基础，提供三种线程池：

| 线程池 | 语义 | 典型用途 |
| --- | --- | --- |
| **串行业务线程池** | 按 id 固定分配到某一条线程，同一个 id 的任务严格按提交顺序执行 | 玩家消息处理、会话清理 |
| **唯一性线程池** | 同样按 id 固定分配到某一条线程；任务带唯一标识 `uniqueKey`，队列里已有同一个 key 的任务在等待时，新任务替换旧任务 | 玩家数据落库、排行榜上报、视野推送、战力重算 |
| **异步优先级线程池** | 多线程并行执行，不保证顺序，高优先级先执行 | 阻塞 IO（渠道验单、外部 HTTP）、计算类任务 |

另外由 `ThreadManage` 统一负责：创建、命名、登记、监控、按顺序关闭所有线程池。

工作方式：由我（开发者）动手实现，AI 负责给方案、讲 API、做 review。

---

## 二、整体架构

```
ThreadManage（创建 / 登记 / 监控 / 有序关闭）
│
├── MailboxExecutorSupport（邮箱模型公共部分：线程池、邮箱注册表、状态、全局计数、回收、关闭）
│     ├── SerialExecutor        普通任务 execute(id, task)，SerialMailbox 无锁（WIP 计数） → 业务线程池
│     └── UniqueSerialExecutor  额外支持 execute(id, uniqueKey, task)，UniqueMailbox 加锁   → 落库 / 缓存刷新等
│         （两者邮箱独立，同一 id 的任务互不保证顺序；唯一任务执行器不能修改个体内存状态）
│
└── PriorityAsyncExecutor（共享队列 + 多线程 + 分档优先级）
```

公共基础件：`NamedThreadFactory`、任务包装（异常兜底、排队耗时和执行耗时统计、慢任务告警、MDC 传递）、指标统计、统一关闭。

### 关键设计约定

- 业务状态只在该 id 的任务里修改，其他地方只能投递任务；异步结果通过 `serial.execute(id, ...)` 回投。
- 步骤 4b 起：同一个 id 的任务严格串行、按提交顺序执行，但**不保证固定在同一条线程**；不能用按线程存放的 `ThreadLocal` 保存个体状态。
- （步骤 3、4 的槽位实现，留档）线程数向上取整到 2 的幂，路由规则 `(int) ((id * 0x9E3779B97F4A7C15L) >>> 32) & mask`。
- 所有队列都必须有界；提交时用 `offer`，不用 `put`，避免阻塞 Netty IO 线程；队列满时抛 `RejectedExecutionException`。
- 禁止使用 `CallerRunsPolicy`，否则提交任务的业务线程会自己去执行阻塞操作。
- 任务异常在任务包装层用 `try/catch Throwable` 捕获，线程不能死；`UncaughtExceptionHandler` 只做最后兜底。

### 唯一性任务的规则

- 被替换的旧任务从队列删除，新任务**排到队尾**（开发者决定：唯一任务多为快照或时效性数据，应按时间顺序排列；代价是积压时高频更新的 key 可能迟迟轮不到）。
- 正在执行的任务不参与替换，再提交同一个 key 时正常排队。
- `uniqueKey` 的作用范围限定在单个 id 内，接口为 `execute(id, uniqueKey, task)`；邮箱模型下每个 id 一个 Mailbox，key 天然只在本 id 内生效。
- 默认“最后提交的胜出”，可选 `merge(old, new)`。
- 带返回结果的任务被替换时，它的 Future 必须有明确结局，不能永远不完成。
- 关闭时必须把剩余任务执行完（落库任务不能丢）。

### 异步优先级池的规则

- 不使用 `ThreadPoolExecutor` 加 `PriorityBlockingQueue`（无界、`submit` 时会抛 `ClassCastException`、同优先级不保证先进先出、低优先级会饿死）。
- 改用固定档位（HIGH / NORMAL / LOW）的多级有界队列，按权重轮询（比如 6:3:1），每档单独设置拒绝策略。
- 按资源拆分成不同的池（DB、HTTP 各一个），不靠优先级来隔离资源。
- 关闭时 HIGH 和 NORMAL 档排空，LOW 档可以按配置丢弃。

### 关闭顺序

1. Netty 停止接收新连接和新请求
2. 定时器停止触发
3. 业务槽位执行器排空队列（设置超时）
4. 触发全量落库，等 DB 池排空
5. 关闭 IO 池
6. 关闭 Netty EventLoop

---

## 三、路线图与进度

状态说明：✅ 完成　🔶 部分完成或有待办　⏳ 进行中　⬜ 未开始　⏭ 跳过

| 步骤 | 内容 | 状态 |
| --- | --- | --- |
| 0 | 修复 `NamedThreadFactory` 迁移包路径后的 import | ✅ |
| 1 | 完善 `NamedThreadFactory`，做异常去向的实验（`execute` / `submit`） | ✅ |
| 2 | 并发基础（`volatile` / `Atomic` / 并发容器） | ⏭ 已掌握，跳过 |
| 3 | `Worker` + `SlotExecutor`（串行池）+ 优雅关闭 + 替换 `KeyedExecutor` | ✅ |
| 4 | `SlotExecutor` 增加唯一任务 `execute(id, uniqueKey, task)`（在 Worker 队列层实现，普通任务与唯一任务共用一个队列） | ✅ 完成（2026-11-16，17 个测试通过） |
| 4b | `SlotExecutor` 内部改为邮箱模型（每个 id 一个 Mailbox，注册到共享线程池执行），对外接口基本不变 | ✅ 完成（2026-11-16，AI 实现，23 个测试连续 5 轮通过） |
| 4c | 拆分为 `SerialExecutor`（普通，无锁邮箱）和 `UniqueSerialExecutor`（唯一任务，加锁邮箱），公共部分抽到 `MailboxExecutorSupport` | ✅ 完成（2026-11-16，AI 实现，40 个测试连续 5 轮通过） |
| 5 | `PriorityAsyncExecutor`（异步优先级池）+ 异步结果回投 | ⬜ |
| 6 | 定时器（先用 `ScheduledExecutorService`，M1 阶段换成分层时间轮） | ⬜ |
| 7 | `ThreadManage` 统一管理 + 监控指标 | ⬜ |

### 当前步骤（步骤 4b）：`SlotExecutor` 内部改为邮箱模型

**为什么改**：哈希槽位模型下，一个 id 的慢任务会卡住同槽位的所有 id（队头阻塞），热点 id 所在槽位排长队时其他线程却空闲；唯一任务需要全局 map 且 key 必须带 routeId；锁的粒度是整个槽位。邮箱模型让每个逻辑个体（玩家、城池、联盟等）拥有自己的任务队列，有任务时才把队列交给共享线程池执行，同一个 id 依然严格串行。

**放弃的方案**：底层换成 Netty `DefaultEventExecutor`。它的价值在于“一个执行器绑定一条线程”，邮箱模型不需要绑定线程；且它的队列同样有锁（`LinkedBlockingQueue`）、最小容量 16、无法从队列中间删除任务。

#### 1. 组成

| 组件 | 职责 |
| --- | --- |
| `SlotExecutor`（类名暂不改，避免改调用方） | 对外入口：持有共享线程池、邮箱注册表、状态和全局计数；负责提交、关闭、回收空闲邮箱 |
| `Mailbox`（包内可见，`implements Runnable`） | 单个 id 的任务队列：`ArrayDeque<MailTask>` + 唯一任务索引 `HashMap<Long, MailTask>` + `scheduled` 标志 + 一把锁；被线程池执行时按批次运行自己的任务 |
| `MailTask`（取代 `SlotTask`） | 包装用户任务：`id`、`uniqueKey`（可为 null）、`Runnable`；负责 `catch Throwable`、清中断标志、慢任务日志 |
| 共享线程池 | 固定 N 条线程的 `ThreadPoolExecutor`，`NamedThreadFactory(name, false)` 命名，非守护 |
| 删除 | `Worker.java`、`SlotTask.java`、`UniqueKey` |

线程池的队列放的是 **Mailbox 而不是业务任务**，每个邮箱同一时刻最多在池队列里出现一次，所以池队列长度不超过活跃邮箱数，用无界 `LinkedBlockingQueue` 可以接受；真正的容量限制在邮箱上。

#### 2. 提交流程 `execute(id, uniqueKey, task)`

1. 读锁内检查状态，不是 RUNNING 就拒绝（读写锁的作用见第 5 点）
2. `mailbox = mailboxes.computeIfAbsent(id, Mailbox::new)`
3. 进入该邮箱的锁：
   - 邮箱已被回收（`retired == true`）：释放锁，回到第 2 步重新取（见第 4 点）
   - 唯一任务先查 `old = uniqueIndex.get(uniqueKey)`。索引和队列在同一把锁下维护，**索引里有就一定还在队列里**（取出时会同时删索引），所以替换不需要额外容量
   - `old == null` 且（队列长度达到单邮箱容量，或全局待执行数达到上限）：拒绝，此时什么都没改动
   - `old != null`：`queue.remove(old)`，合并计数 +1，全局待执行数不变
   - 新任务加到队尾；唯一任务写入 `uniqueIndex`；没有替换时全局待执行数 +1
   - `scheduled` 为 false 时置为 true，并记下“需要调度”
4. 释放邮箱锁后，如果需要调度就 `pool.execute(mailbox)`
5. 拒绝时抛 `RejectedExecutionException`，信息带 name、id、邮箱长度、全局待执行数、running

邮箱的锁只被“提交到这个 id 的线程”和“正在执行这个邮箱的线程”争用，几乎没有竞争。

#### 3. 执行流程 `Mailbox.run()`

```
for i in 0 until BATCH(16):
    加锁：task = queue.poll()
          task 为空：scheduled = false，解锁，返回
          唯一任务：uniqueIndex.remove(uniqueKey, task)
    解锁
    CURRENT.set(this)；task.run()（包装层兜底异常）；CURRENT.remove()
    全局待执行数 -1
批次跑满：加锁，队列为空则 scheduled = false，否则保持 true；解锁后若仍有任务，pool.execute(this) 重新排到池队列末尾
```

- `scheduled` 只在邮箱锁内读写，不存在“清标志前刚入队的任务没人调度”的问题
- 每批最多 16 个任务后让出线程，忙碌的个体不会长期霸占线程（公平性）
- 锁保证了前后两批在不同线程上执行时，状态修改对下一批可见
- `CURRENT` 是 `ThreadLocal<Mailbox>`，只在任务执行期间有值，用于 `inSlot(id)` / `assertInSlot(id)`：判断当前是否正在执行这个 executor 中该 id 的任务

#### 4. 邮箱回收

- 邮箱为空不立即删除（同一个玩家的任务很频繁，反复创建浪费）
- 后台每 60 秒扫描一次：在邮箱锁内，如果队列为空、`scheduled == false`、空闲超过 5 分钟，就把 `retired` 置为 true，然后 `mailboxes.remove(id, mailbox)`
- 提交方可能拿到了即将被回收的邮箱引用，所以第 2 步遇到 `retired` 要重新 `computeIfAbsent`，保证任务不会投进已回收的邮箱
- 扫描暂时用 `SlotExecutor` 内部的单线程 `ScheduledExecutorService`（守护线程），第 6 步完成后改由统一定时器触发

#### 5. 关闭 `shutdown(timeout, unit)`

1. 写锁内把状态改为 SHUTDOWN。读写锁保证：所有已通过状态检查的提交都已完成入队和调度，之后的提交全部拒绝；提交方之间只共享读锁，互不阻塞
2. 用同一个 deadline 等待全局待执行数降到 0（每 10ms 检查一次即可，只在关闭时发生）
3. `pool.shutdown()`，再 `pool.awaitTermination(剩余时间)`；停止回收扫描线程
4. 超时返回 false 并记录剩余任务数；被中断时恢复中断标志并返回 false

注意：关闭后，正在执行的任务再向其他 id 投递的后续任务会被拒绝。需要保证执行的收尾工作（如全量落库）要在调用 `shutdown` 之前投递，或者交给 DB 池处理（对应“关闭顺序”第 3、4 步）。

#### 6. 唯一任务语义（与步骤 4 相同）

- 等待中的旧任务被删除，新任务排到队尾
- 已被取出执行的任务不受影响
- 队列满时同 key 替换仍然成功
- 契约：提交成功的任务一定会被执行，或者被同 key 的新任务取代；被拒绝的任务一定没有执行，且不影响已有任务
- 因为索引只在单个邮箱内，key 不再需要带 routeId，删除旧任务也只遍历这个 id 自己的短队列

#### 7. 对外接口变化

| 接口 | 变化 |
| --- | --- |
| 构造 `(name, threads, queueCapacity)` | `queueCapacity` 改为**单个邮箱**的容量；新增可选的全局待执行上限（默认 `queueCapacity × threads × 16` 之类的值，接入时在配置里给出）；线程数不再取整到 2 的幂 |
| `execute` 两个重载、`start`、`shutdown`、`name`、`mergedCount`、`pendingTasks` | 不变 |
| `inSlot(id)` / `assertInSlot(id)` | 语义改为“当前正在执行该 id 的任务”，方法名暂不改 |
| `size()` | 返回线程数（`BackendServer` 启动日志在用） |
| `slotOf(id)` | 删除 |
| `queueSize(int slot)` | 改为 `queueSize(long id)` |
| 新增 `mailboxCount()` | 当前邮箱数量，供第 7 步监控使用 |

`BackendServer` 和 `MessageDispatcher` 只用到 `execute`、`start`、`shutdown`、`size`、`pendingTasks`，不需要改。

#### 8. 测试调整

`SlotExecutorTest`
- 删除 `threadCountRoundsUpToPowerOfTwo`
- `sameIdAlwaysOnSameThreadAndIdsCoverAllSlots` 改为：同一个 id 的任务从不并发执行（用计数器检测同时进入的个数）且按顺序执行
- `failingTaskDoesNotKillSlotThread`：线程名断言改为以 `fail-` 开头，后续任务仍能执行
- `rejectsWhenNotStartedOrQueueFull`：卡住 id 1 后，id 1 再提交 1 个成功、第 2 个被拒绝；**新增断言**：此时 id 2 仍然可以提交并执行
- `inSlotOnlyOnOwningThread`：不再需要找不同槽位的 id，任意另一个 id 即可
- 其余用例不变

`SlotExecutorUniqueTest`
- 用 1 条线程的执行器，卡住线程的任务就能让所有邮箱排队，现有 8 个场景基本不用改

新增 `SlotExecutorMailboxTest`
1. 队头阻塞消除：2 条线程，id A 的任务卡住时，id B 的任务照常执行完
2. 公平性：1 条线程，先给 id A 提交 1000 个任务，再给 id B 提交 1 个，B 在 A 全部执行完之前就执行了
3. 线程切换后的可见性：同一个 id 交替穿插其他 id 的任务，用普通字段（非 volatile）累加 10 万次，结果正确
4. 回收竞态：把空闲阈值调到很小，多线程不停提交的同时反复触发回收，成功提交数 == 执行数
5. 全局上限：达到全局待执行上限后，任何 id 的提交都被拒绝
6. 关闭：已接收的任务全部执行，之后的提交被拒绝，`mailboxCount` 之后不再增长

全部通过后连续跑 5 轮。

#### 9. 实施顺序

1. `MailTask`（从 `SlotTask` 和 `Worker.runTask` 的内容迁移）
2. `Mailbox`：提交、执行、唯一任务、回收标记
3. `SlotExecutor` 内部替换：线程池、注册表、读写锁、全局计数、关闭、回收扫描
4. 删除 `Worker`、`SlotTask`
5. 调整原有测试、新增 `SlotExecutorMailboxTest`，跑 5 轮
6. 更新本文档（第二节架构、待办、Review 记录）

#### 10. 之后的优化方向（压测证明有瓶颈再做）

- 邮箱锁换成无锁的“MPSC 队列 + CAS 调度标志”（唯一任务届时改为作废标记）
- 读写锁换成 `LongAdder` 统计“正在提交的线程数”，关闭时先改状态再等计数归零
- 线程池换成 `ForkJoinPool`（`asyncMode = true`）获得工作窃取

### 步骤 4（已完成，留档）：在 `SlotExecutor` 的队列层实现唯一任务

**设计调整（2026-11-13）**：最初的设计是“一种队列一个实例”，另外包一层 `UniqueSlotExecutor`。开发者提议改成**一个 Executor、一种队列同时支持普通任务和唯一任务**，采纳。理由：
- 方案 A 和方案 B 的区别在于“这个池怎么用”，而不在于“这个池有没有去重能力”。同一个类，业务池实例上用唯一任务就是方案 B（可以修改玩家状态），落库池实例上用就是方案 A（只处理快照）
- 同一个 id 的普通任务和唯一任务在同一个队列里，先后顺序有保证（例如先“删除角色”再“保存角色”）
- 不需要 `SlotQueue` 接口、`FifoSlotQueue`、`UniqueSlotExecutor`，代码更少
- 普通任务的额外开销只有一个条目对象和一次原子操作，大约几十纳秒，可以忽略

**语义（2026-11-13 由开发者调整）**：`execute(id, uniqueKey, task)` 提交时，如果队列里已有同一个 `(id, uniqueKey)` 的任务在等待，就**删除旧任务，新任务排到队尾**。理由是唯一任务一般是快照或时效性数据，应该按最后一次提交的时间和其他任务排序。正在执行（或已被 Worker 取出）的任务不受影响；普通任务 `execute(id, task)` 永远不会被合并。

- 已知代价：系统积压时，提交间隔比排队等待时间还短的 key 会一直被挪到队尾，积压期间可能一直执行不到。目前接受；以后如果出现，可以加兜底规则：旧任务已等待超过阈值（比如 5 秒）时原地替换，不再挪到队尾
- `queue.remove(Object)` 是 O(n)，容量 3000 时每次约几微秒，目前可以接受；需要优化时可以改为“标记旧条目作废、Worker 跳过”的延迟删除

**按“排到队尾”语义的实现要点**（取代下面第 1、2 点里“原地替换”的写法）：
- `offer` 中的唯一任务分支：`old = map.get(key)`；`replaced = old != null && queue.remove(old)`；`queue.offer(task)` 失败就返回 false；成功就 `map.put(key, task)`，`replaced` 为真时合并计数加 1
- Worker 在 `runTask` 开头对唯一任务调用 `map.remove(key, task)`（两个参数的版本）
- `Key = record(routeId, uniqueKey)`，`uniqueKey` 的类型用 `Object`
- 旧任务已被 Worker 取进 batch 但还没执行时，`remove` 会失败，新旧任务各执行一次，对快照数据可以接受

**1. `SlotTask` 改为队列条目（包内 final class）**
- `final long routeId`、`final Object uniqueKey`（普通任务为 null）、`final AtomicReference<Runnable> task`
- 哨兵 `static final Runnable TAKEN`，表示“已被 Worker 取走”
- 方法：`boolean replace(Runnable newTask)`（当前值不是 TAKEN 时 CAS 替换，成功返回 true）、`Runnable take()`（`getAndSet(TAKEN)`）

**2. `Worker` 的队列**
- 保留 `ArrayBlockingQueue<SlotTask> queue` 记录顺序（容量只按条目数计算）
- 新增 `ConcurrentHashMap<Key, SlotTask> pending`，`Key` 是 `(routeId, uniqueKey)` 的 record。只用 uniqueKey 的话，同一个槽位上不同玩家的同名 key 会被误合并
- `offer(SlotTask)`（本来就在锁内）：
  - 普通任务：和现在一样直接入队
  - 唯一任务：先在 `pending` 里找同 key 的条目，找到并且 `replace` 成功，就合并计数加 1 并返回 true。**替换不占用容量，队列满了也能成功**。否则新建条目，入队失败就返回 false，成功就放进 `pending`
- 取任务（`runLoop` 中）：对每个条目调用 `take()` 拿到最新的任务；如果是唯一任务，再调用 `pending.remove(key, entry)`（**必须用两个参数的版本**，避免误删同一个 key 新建的条目）
- 思考题：生产者的 `replace`（CAS）和 Worker 的 `take()`（getAndSet）同时发生时，为什么任务既不会丢，也不会执行两次？
- 合并次数用 `LongAdder` 统计

**3. `SlotExecutor` 新增**
- `execute(long id, Object uniqueKey, Runnable task)`：uniqueKey 不能为 null，并且要正确实现 `equals` / `hashCode`
- `mergedCount()`：所有槽位合并次数之和
- javadoc 写明：被替换时只保留最后一次提交，所以唯一任务必须携带完整状态或完整快照，不能是增量；在落库这类非业务线程池上使用时，任务只能处理快照

**步骤 4 第一版 review 待修问题（2026-11-13）**
- [x] 普通任务的 `uniqueKey` 为 null 时 NPE（第二版已修：先判断是否为 null）
- [x] `uniqueMap` 只在“已存在”时才 `put`（第三版已修：每个唯一任务都会 put）
- [x] 任务执行后没有从 map 里删除（第三版已加删除，但写法有问题，见下）
- [x] 第三版：普通任务执行 `uniqueMap.remove(null)` 抛 NPE（第四版已修：`runTask` 中只对唯一任务调用 remove）
- [x] 第三版：检查和删除分两步，会丢最新快照（第四版已修：改用 `uniqueMap.remove(key, task)`）
- [x] 第五版：`offer` 返回值已修正，`SlotExecutorTest` 9 个全部通过；put 放在入队成功之后
- [x] 第五版：先 `queue.offer(new)` 再删旧任务，队列满时即使有同 key 的旧任务也会直接拒绝（不出错，但积压时最需要合并的场景反而合并不了）；改为先 `remove(old)` 再 offer。另外用局部变量 `old` 只查一次 map
- [ ] ~~第四版：`offer` 的返回值反了~~（已修），原描述：入队成功返回 false（调用方收到拒绝，但任务照样执行）、入队失败返回 true（任务没入队却报告已接收）；唯一任务的处理只在入队失败的分支里才执行。`SlotExecutorTest` 9 个里 7 个失败。正确顺序：检查 running → 普通任务直接 offer → 唯一任务先 `remove(old)` 腾位置 → `queue.offer(task)`，失败返回 false → 成功后 `put`（完整代码见 2026-11-16 review）
- [x] 被跳过的旧任务应计入 `mergedCount`；Worker 类注释的约定改为“offer 返回 true 的任务一定会被执行，或者被同 key 的新任务取代”
- 说明（第七版修订）：旧任务在提交时直接从队列删除，Worker 不再跳过任务，取走的任务一律执行，只调用 `uniqueMap.remove(key, task)` 清理登记；`offer` 必须先 put 再入队，入队失败时 `remove(key, task)` 撤销。原因：Worker 不持锁，若入队后再 put，Worker 可能先取走新任务，导致最新版本被误判为过期而跳过（并发测试复现：最后执行的是 v499 而不是 v500）
- [x] Worker 退出时顺带 `uniqueMap.clear()`
- [x] 在检查 `running` 之前就删掉了旧任务（第二版已修：先检查 running）
- [x] 排到队尾还是保留位置：开发者决定排到队尾（见上面的“语义”）
- [x] map 的 key 只用了 uniqueKey，没有带 routeId，同一个槽位上不同玩家的同名 key 会被误合并（第四版改成了 `String`，但仍然没有和 routeId 组合）；用 `record Key(long routeId, Object uniqueKey)`
- [x] 删除未使用的 `SlotQueue.java`（`CoalescingSlotQueue.java` 已删除）
- [x] `mergedCount()`（Worker 用 LongAdder 计数，SlotExecutor 汇总）
- [x] 唯一任务的正式验收测试（`SlotExecutorUniqueTest` 8 个 + 原有 9 个）

**4. 验收测试（加在 `SlotExecutorTest`，或新建 `SlotExecutorUniqueTest`）**
1. 等待中被替换：先用 latch 卡住槽位，再对同一个 key 提交 v1 到 v100，放行后这个 key 只执行一次，执行的是 v100
2. 替换后排到队尾：卡住槽位后依次提交 A1、B1、A2，执行顺序为 B1、A2
3. 正在执行的任务不被替换：key K 的任务执行中再提交 K，会再执行一次
4. 同一槽位、不同 routeId、相同 uniqueKey 时不合并
5. 容量：容量为 1 且槽位被卡住，提交 K 成功；再提交 K 成功（替换）；提交 L 被拒绝
6. 普通任务和唯一任务混合：同一个 id 依次提交普通 N1、唯一 K1、普通 N2、唯一 K2，执行顺序为 N1、N2、K2；普通任务永远不会被合并
7. 并发：8 个生产者各自负责一批 key 并递增版本号，最后每个 key 执行的都是最大版本；成功提交数 == 实际执行数 + 合并数
8. 关闭时执行完已接收的任务，之后的提交被拒绝
9. 原有的 9 个 `SlotExecutorTest` 依然全部通过

### 步骤 3 要求（已完成，留档）

**`Worker`（包内可见）**
- 字段：`Thread thread`、`BlockingQueue<SlotTask> queue`（有界的 `LinkedBlockingQueue`）、`volatile boolean running`、`int index`
- `boolean offer(SlotTask)`：`running` 为 false 或队列已满时返回 false
- `runLoop()`：用 `poll(100ms)` 取任务；退出条件是 `!running && queue.isEmpty()`；每个任务单独 `try/catch Throwable`；超过 20ms 打慢任务日志；正确处理 `InterruptedException`
- `start()`、`stop()`、`awaitTermination(timeoutMs)`、`inThisThread()`
- 线程名用 `name + "-" + index`，和槽位号保持一致

**`SlotTask`（包内可见）**：目前只包一个 `Runnable`，步骤 4 会在里面加 `uniqueKey` 和替换逻辑。

**`SlotExecutor`（公开）**
- 构造器 `SlotExecutor(String name, int threads, int queueCapacity)`：只创建 Worker，不启动线程
- `start()`、`execute(long id, Runnable)`、`slotOf(long id)`、`inSlot(long id)`、`assertInSlot(long id)`
- `shutdown(timeout, unit)`：先对所有 Worker 调用 stop，再按同一个截止时间等待它们结束
- `size()`、`queueSize(int slot)`

**验收测试 `SlotExecutorTest`**
1. 顺序性：同一个 id 提交 10000 个任务往普通 `ArrayList` 里写序号，结果严格递增且没有丢失
2. 路由稳定：同一个 id 总在同一个线程上执行；1000 个不同 id 能覆盖所有槽位
3. 异常隔离：抛异常的任务之后，下一个任务还在同一个线程上执行
4. 拒绝：容量为 1、线程被卡住时，第 3 次提交抛 `RejectedExecutionException`
5. 排空关闭：提交 1000 个任务后立刻 shutdown，全部执行完；shutdown 之后再提交会被拒绝

**接入**：在 `MessageDispatcher`、`BackendServer` 中用 `SlotExecutor` 替换 `KeyedExecutor`，然后删除 `dispatch/KeyedExecutor.java`。

**思考题**：`offer` 检查完 `running == true` 之后，`shutdown` 把它设为 false，Worker 发现队列为空于是退出，接着任务才入队。这个任务就丢了。你打算怎么解决？

---

## 四、待办与遗留问题

- [x] `NamedThreadFactory`：默认 handler 改成 slf4j 的 `log.error("...", thread.getName(), ex)`，不要用 `System.err`
- [x] `NamedThreadFactory`：删掉 `newThread(Runnable, UncaughtExceptionHandler)` 重载，改为通过构造器传入 handler
- [x] `NamedThreadFactory`：把 `import java.util.concurrent.*` 改成具体导入
- [x] 步骤 3 思考题：offer 和 shutdown 之间的竞态（已采用方案一并通过并发测试）
  - 之前提的“Worker 退出循环后再 poll 一遍”**不正确**：只是缩小了时间窗口，offer 线程在检查 running 和入队之间可能被挂起任意长时间
  - 正确方案一（推荐）：每个 Worker 一把锁，`offer` 的“检查 running + 入队”和 `shutdown` 的“running = false”都在锁内执行
  - 正确方案二（JDK `ThreadPoolExecutor` 的做法）：入队后再检查一次 running，如果已关闭并且 `queue.remove(task)` 成功，就返回拒绝
  - 不论哪种方案，循环条件都必须**先读 running，再读 isEmpty**
  - 要保证的约定：offer 返回 true 的任务一定会被执行，返回 false 的任务一定没有被执行
  - 性能对比结论：两种方案的差别在几十纳秒，可以忽略（`ArrayBlockingQueue` 内部本来就有锁，每个槽位一把锁，竞争很小）。**选方案一**，因为步骤 4 的 `executeUnique` 需要同一把锁来保护“检查 + 查 map + 入队”这个组合操作。真正的性能优化方向是：消费端用 `drainTo` 批量取任务、换 `LinkedBlockingQueue`（入队出队分两把锁），最终换成 MPSC 无锁队列。压测证明有瓶颈之后再做
  - 验收测试第 5 条加强为：8 个线程并发 execute 时调用 shutdown，断言成功提交的总数 == 实际执行的任务数

### 步骤 3 初版 review 待修问题（2026-11-13）

已修复（第三版确认）：
- [x] `Worker.t` 已赋值；`start` 不再直接返回；`awaitTermination` 返回 `!isAlive()`
- [x] 删掉了 `runLoop` 开头的 `inThisThread` 判断，`runLoop` 改为 private
- [x] 改为 `catch (Throwable)`
- [x] 队列改为有界的 `ArrayBlockingQueue(queueCapacity)`
- [x] Worker 在构造器里创建
- [x] `mask` 和数组长度改用 `this.threads`
- [x] `Worker` 不再继承 `Thread`，改为组合
- [x] 路由改为 `slotOf(long id)`，使用黄金分割常数打散
- [x] 对外接口改为 `execute(long id, Runnable)`，`SlotTask` 的字段改为 final
- [x] offer 和 shutdown 的竞态：采用方案一，加锁

已修复（第四版确认）：
- [x] 构造器里创建 Worker 的循环改用 `this.threads`
- [x] `shutdown` 不再把绝对时间戳传给 `join`，`result` 也会如实返回
- [x] 去掉了 `sleep(100)`，改为先 `poll(100ms)` 等第一个任务，再 `drainTo(batch, 63)`；批量容器用的是循环外创建的 `ArrayDeque`
- [x] 线程已命名（`name--index`）
- [x] 慢任务日志打印的是耗时，不再是 `endTime`

已修复（第五版，2026-11-13 由 AI 按清单直接修改）：
- [x] `shutdown(long timeout, TimeUnit unit)`：所有 Worker 共用一个截止时间，剩余时间小于等于 0 时只检查 `isAlive()`；被中断时恢复中断标志并返回 false
- [x] `runLoop` 等待任务时收到中断：打 error 日志后退出；退出时（`finally` 中）把 running 置为 false，保证之后的 offer 都被拒绝，并记录丢弃的任务数
- [x] 每个任务执行完后调用 `Thread.interrupted()`，清除任务残留的中断标志
- [x] 线程显式 `setDaemon(false)`，设置 `NamedThreadFactory.LOGGING_HANDLER`；线程名为 `name-index`
- [x] 慢任务阈值改为 20ms，计时改用 `nanoTime`；删掉了“队列长度 > 100”告警
- [x] 拒绝异常信息改为普通拼接，带上 name、slot、routeId、队列长度、running
- [x] 字段改为 `private final`；`Worker` 改为包内可见的 final 类；`SlotTask` 改为包内可见的 record
- [x] `start()` 去掉参数；重复 start（包括 shutdown 后再 start）抛 `IllegalStateException`
- [x] 实现 `inSlot(long id)` / `assertInSlot(long id)`
- [x] `size()` 返回槽位数；新增 `pendingTasks()` 返回排队任务总数；`execute` 对 null 任务直接抛 NPE；`queueCapacity <= 0` 时构造器抛 `IllegalArgumentException`
- [x] `SlotExecutorTest` 9 个测试全部通过：顺序性、线程数取整、路由稳定、异常隔离（含 Error 和残留中断）、未启动或队列满时拒绝、排空关闭、并发关闭（20 轮，成功提交数 == 实际执行数）、`inSlot`、禁止重启

接入（2026-11-13 完成）：
- [x] `BackendServer`：`bizThreads <= 0` 时取 CPU 核数；新增配置 `server.bizQueueCapacity`（默认 3000）；先 `executor.start()` 再启动网络；关闭顺序改为注销注册、停网络、等业务线程（10 秒，超时打 warn）
- [x] `MessageDispatcher.dispatch`：捕获 `RejectedExecutionException`，回复新增的 `ErrorCodes.SERVER_BUSY = 6`，不把异常抛进 Netty pipeline
- [x] `InternalServer` 会话关闭清理任务被拒绝时打 error 日志
- [x] 删除 `dispatch/KeyedExecutor.java`
- 慢任务日志暂时保留两处：`MessageDispatcher`（带 msgId）和 `Worker`（带 routeId），同一个慢消息会打两条；第 7 步加入任务名后合并

### 构建环境问题（与线程池无关，但会导致整个模块构建失败）

- [x] protobuf 代码生成：已决定交给 Maven 插件自动生成。protoc 安装包（`include/`、`bin/`、`protoc.exe`、zip、readme、手动生成的 `java/`）已从 `src/main/proto` 移到 `tools/protoc-36.2/`（已加入 `.gitignore`）；删除了 `src/main/java/com/slg/common/proto/` 下手动生成的 20 个文件（备份在 `tools/protoc-36.2/java/`）。`src/main/proto` 现在只剩 `client.proto` 和 `internal.proto`
- [x] logback 版本冲突：去掉了根 pom 里的 `logback.version` 和 `logback-classic` 版本覆盖，改由 Spring Boot BOM 统一管理（classic 和 core 同为 1.5.38）
- [ ] 终端默认 JDK 是 1.8，不认识 record；本机可用 `C:\Users\EYU\.jdks\graalvm-jdk-21.0.7`（以 `--release 17` 编译），建议把 `JAVA_HOME` 指向 JDK 17 或 21
- 当前构建命令（clean 构建，不跳过 protoc，13 个测试全部通过）：
  ```
  $env:JAVA_HOME="C:\Users\EYU\.jdks\graalvm-jdk-21.0.7"
  mvn -pl test-common,test-gateway,test-world,test-game -am clean test "-Denforcer.skip=true"
  ```

## 五、待决策事项

- [x] 唯一性任务要不要直接修改玩家状态？**最初决定方案 A**（单独建池、只处理快照）；之后调整为：唯一任务作为 `SlotExecutor` 本身的能力，在业务池实例上使用就是方案 B，在落库池实例上使用就是方案 A，由使用方选择
- [ ] `ThreadManage` 用静态单例，还是交给 Spring 管理？
- [x] 串行池最终是否升级成 Actor 邮箱模型？**2026-11-16 决定升级**（步骤 4b）；曾考虑的 Netty `DefaultEventExecutor` 方案放弃，理由见步骤 4b
- [x] 步骤 4b 由谁实现？由 AI 按方案实现
- [x] `MessageDispatcher` 用 `playerId != 0 ? playerId : sessionId` 作为邮箱 id，两者共用 id 空间：**决定不处理**。业务分发不使用唯一任务，碰撞的唯一后果是两个邮箱被强制串行；sessionId 是网关的小计数值，playerId 是全局 ID，实际几乎不会相等
- [ ] 全局待执行上限的默认值，以及邮箱空闲回收阈值（暂定 5 分钟）是否合适？

---

## 六、Review 记录

| 日期 | 步骤 | 结论 |
| --- | --- | --- |
| 2026-11-12 | 0 | import 已全部改为 `com.slg.common.thread.NamedThreadFactory` |
| 2026-11-12 | 1 | 已删除 `main`；结论“异常应在任务内部捕获”正确；handler 和重载方法的问题挪到待办 |
| 2026-11-13 | 3 | 初版 `Worker`、`SlotExecutor`、`SlotTask` 已写，结构方向正确（每槽位一个队列、poll 带超时、volatile 标志、退出前排空队列）；发现 7 个会直接出错的 bug 和若干设计问题，详见第四节；还没有测试 |
| 2026-11-13 | 3 | 讨论 offer 和 shutdown 的竞态：纠正了之前“退出后再 poll 一遍”的方案，改为加锁或入队后再检查；第二版 Worker 改成了组合、改用有界队列，但新引入了 `inThisThread` 直接返回和 `catch (Error)` 两个问题 |
| 2026-11-13 | 3 | 第三版：加锁方案正确，关闭竞态已解决；`catch (Throwable)`、`slotOf(long)`、`execute(long, Runnable)` 都已改好。还剩构造器循环越界、shutdown 把时间戳当成时长、shutdown 结果被忽略、`sleep(100)` 带来的延迟这 4 个必须修的问题 |
| 2026-11-13 | 3 | 第四版：上面 4 个问题修了 3 个（构造器循环、批量取任务的延迟、慢任务日志），`shutdown` 还剩等待时长不可控和吞掉中断两个问题；Worker 的中断处理和线程属性待完善；测试和接入还没开始 |
| 2026-11-13 | 1、3 | 应要求由 AI 直接修改：重写 `Worker`、`SlotExecutor`，`SlotTask` 改为 record，修好 `NamedThreadFactory`；新增 `SlotExecutorTest`，9 个测试全部通过。步骤 3 只剩接入现有代码 |
| 2026-11-13 | 3 | 接入完成（AI 修改）：`BackendServer` 启动、线程数、关闭顺序，`MessageDispatcher` 拒绝时回复 `SERVER_BUSY`；修好 logback 版本冲突；test-common 13 个测试通过，gateway、world、game 编译通过。步骤 3 完成，进入步骤 4（方案 A） |
| 2026-11-13 | 4 | 设计调整：采纳开发者“一个 Executor、在队列层实现唯一任务”的提议，取消 `SlotQueue` / `UniqueSlotExecutor`，改为 `SlotTask` 带 uniqueKey + `AtomicReference`，Worker 加 `pending` map，`SlotExecutor` 新增 `execute(id, uniqueKey, task)` |
| 2026-11-13 | 4 | 第一版：`SlotExecutor.execute(id, uniqueKey, task)` 接口方向正确，但普通任务 NPE（7 个原有测试失败）、map 从未写入、用“删除后重新入队”替换（丢失排队位置且是 O(n)）、关闭时会丢旧任务、key 没带 routeId；已给出原地替换的骨架 |
| 2026-11-13 | 4 | 第二版：NPE 和 running 检查顺序已修；开发者决定替换后排到队尾（已记录积压时可能执行不到的代价）。还剩：map 只在已存在时才 put（合并永远不会发生）、执行后没有从 map 删除（内存泄漏）、key 没带 routeId（不同玩家互相覆盖，会丢数据） |
| 2026-11-13 | 4 | 第三版：map 的 put 和 remove 都补上了；Worker 跳过“不是 map 里最新版本”的任务，思路很好（解决了新旧任务各执行一次的问题）。但普通任务调用 `remove(null)` 抛 NPE，会让槽位线程退出；检查和删除分两步，会丢最新快照；先 put 后 offer 在队列满时会丢旧任务；key 仍然没带 routeId |
| 2026-11-16 | 4 | 第四版：`runTask` 改用 `remove(key, task)`，NPE 和丢失最新快照的问题已解决；但 `offer` 返回值写反（7 个原有测试失败），唯一任务的处理放在了入队失败的分支里；key 仍然没带 routeId。已给出 `offer` 的完整写法，建议先补唯一任务的测试再继续改 |
| 2026-11-16 | 4 | 第五版：`offer` 返回值已修正，原有 9 个测试全部通过，并发逻辑正确。还剩：key 没带 routeId（唯一会丢数据的问题）、队列满时无法替换（建议先删后入队）、合并计数、类注释、退出时清空 map、删除 `SlotQueue.java`、补唯一任务测试 |
| 2026-11-16 | 4 | 第六版（AI 按要求修改 `Worker.offer`）：Worker 内部 key 改为 `record UniqueKey(routeId, uniqueKey)`；offer 改为先 `remove(old)` 腾容量、再 offer、成功后才 put；跳过的旧任务计入 `mergedCount`；退出时 `uniqueMap.clear()`；类注释写明 offer 约定。开发者同时把 `execute` 的 uniqueKey 改成 `Long` 并拼成 `id-key` 字符串，和 Worker 的 routeId 组合重复但无害。临时测试验证（同槽不同玩家不合并、队列满时替换、8 线程并发下 提交数 == 执行数 + 合并数）与原有 9 个测试共 12 个全部通过，临时测试已删除。剩余：正式唯一任务测试 |
| 2026-11-16 | 4 | 第七版（AI 编写 `SlotExecutorUniqueTest`）：8 个场景测试。并发测试发现第六版的竞态：先入队后 put，Worker 取走新任务时 map 还没登记，最新版本被当作过期跳过。修复为先 put 再入队、失败撤销，Worker 不再跳过任务、只清理登记，`mergedCount` 只在提交时从队列删掉旧任务时计数。开发者完成了 uniqueKey 改为 `Long`（`SlotTask`、`UniqueKey`），去掉了字符串拼接。17 个测试连续多轮通过，步骤 4 完成 |
| 2026-11-16 | 4b | 方案调整（仅修改文档）：讨论了换成 Netty `DefaultEventExecutor` 的方案（已查证其队列为 `LinkedBlockingQueue`、最小容量 16），之后开发者提出“每个个体一个任务队列，注册到共享线程池执行”，确定改为邮箱模型。方案见步骤 4b：Mailbox 内锁 + 唯一任务索引、批次执行保证公平、空闲回收与 retired 重试、读写锁 + 全局待执行数实现关闭、接口变化与测试调整 |
| 2026-11-16 | 4b | 实现完成（AI）：新增 `MailTask`（用类而非 record，按引用删除）、`Mailbox`（邮箱锁内维护队列、唯一任务索引、scheduled、retired；每批 16 个）；重写 `SlotExecutor`（`ThreadPoolExecutor` + 邮箱注册表 + 读写锁 + 全局待执行数 + 每 60 秒回收空闲邮箱，新增 5 参数构造器、`mailboxCount()`、`queueSize(long id)`，删除 `slotOf`）；删除 `Worker`、`SlotTask`。`SlotExecutorTest` 按方案调整，唯一任务 8 个测试未改动，新增 `SlotExecutorMailboxTest` 6 个；共 23 个测试连续 5 轮通过，gateway、world、game 编译通过。`ThreadUtil.nextPowerOfTwo` 已无人使用，可删除 |
| 2026-11-16 | 4c | 开发者提出按“是否带唯一任务”拆成两个执行器（AI 实现）：`MailboxExecutorSupport<M, T>` 放公共部分；`SerialExecutor` + `SerialMailbox` 用 WIP 计数实现无锁调度（先入队后计数，0→1 的提交方负责调度，执行方减到 0 才退出；回收为 CAS 0→RETIRED，提交方入队后发现已回收则按引用删掉自己的任务并重试，删不掉说明已被执行）；`UniqueSerialExecutor` + `UniqueMailbox` 保留加锁实现和 `mergedCount`；`AbstractMailbox` 放当前邮箱标记和任务包装。`BackendServer`、`MessageDispatcher` 改用 `SerialExecutor`。测试改为抽象契约测试 `MailboxExecutorContractTest`（15 个），两个执行器各跑一遍；`SerialExecutorTest` 另加同 id 多生产者、回收竞态 2 个压力测试；`UniqueSerialExecutorTest` 保留唯一任务 8 个。共 40 个测试连续 5 轮通过，全量构建通过。读写锁和全局 `pending` 计数仍是全局热点，留作后续优化 |
