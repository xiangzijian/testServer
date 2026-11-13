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
├── SlotExecutor（槽位执行器：N 个 Worker 线程，id → 固定槽位）
│     ├── 普通任务 execute(id, task)                 → 串行业务线程池
│     └── 可替换任务 executeUnique(id, key, task)    → 唯一性线程池
│
└── PriorityAsyncExecutor（共享队列 + 多线程 + 分档优先级）
```

公共基础件：`NamedThreadFactory`、任务包装（异常兜底、排队耗时和执行耗时统计、慢任务告警、MDC 传递）、指标统计、统一关闭。

### 关键设计约定

- 业务状态只在槽位线程上修改，其他线程只能投递任务；异步结果通过 `serial.execute(id, ...)` 回投到槽位线程。
- 槽位线程数在启动时固定，向上取整到 2 的幂，**不支持运行中扩缩容**，否则 id 和线程的对应关系会变。
- 路由规则：`(int) ((id * 0x9E3779B97F4A7C15L) >>> 32) & mask`。
- 所有队列都必须有界；提交时用 `offer`，不用 `put`，避免阻塞 Netty IO 线程；队列满时抛 `RejectedExecutionException`。
- 禁止使用 `CallerRunsPolicy`，否则提交任务的业务线程会自己去执行阻塞操作。
- 任务异常在 Worker 循环里用 `try/catch Throwable` 捕获，线程不能死；`UncaughtExceptionHandler` 只做最后兜底。

### 唯一性任务的规则

- 被替换的任务**保留原来的排队位置**，只替换任务内容，防止高频更新的 key 被饿死。
- 正在执行的任务不参与替换，再提交同一个 key 时正常排队。
- `uniqueKey` 的作用范围限定在单个 routeId 内，接口为 `executeUnique(routeId, uniqueKey, task)`。
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
| 4 | `SlotExecutor` 增加唯一任务 `execute(id, uniqueKey, task)`（在 Worker 队列层实现，普通任务与唯一任务共用一个队列） | ⏳ 当前步骤，设计已给出，由开发者实现 |
| 5 | `PriorityAsyncExecutor`（异步优先级池）+ 异步结果回投 | ⬜ |
| 6 | 定时器（先用 `ScheduledExecutorService`，M1 阶段换成分层时间轮） | ⬜ |
| 7 | `ThreadManage` 统一管理 + 监控指标 | ⬜ |

### 当前步骤（步骤 4）：在 `SlotExecutor` 的队列层实现唯一任务

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
- [ ] 第三版：`runLoop` 中对普通任务执行了 `uniqueMap.remove(null)`，抛 NPE，并且这个异常不在 `runTask` 的 try 里，**会让槽位线程直接退出**（测试已复现）
- [ ] 第三版：“检查 `task == map.get(key)`”和“`map.remove(key)`”是两步操作，中间生产者 put 了新任务的话，新任务的映射会被删掉，之后新任务被当成旧任务跳过，**最新快照丢失**。改为 `if (key != null && !uniqueMap.remove(key, task)) continue;`，原子地完成检查和删除，同时解决上一条 NPE
- [ ] 第三版：`offer` 中先 `put` 后 `queue.offer`。旧任务已被取进 batch 而且队列满时，新任务被拒，旧任务又因为“不是 map 里的最新版本”被跳过，两个都没执行。改为 offer 成功后再 put
- [ ] 被跳过的旧任务应计入 `mergedCount`；Worker 类注释的约定改为“offer 返回 true 的任务一定会被执行，或者被同 key 的新任务取代”
- 说明：Worker 跳过“不是 map 里最新版本”的任务以后，`queue.remove(old)` 已经不是保证正确性所必需的，作用只是及时释放队列容量
- [ ] Worker 退出时顺带 `uniqueMap.clear()`
- [x] 在检查 `running` 之前就删掉了旧任务（第二版已修：先检查 running）
- [x] 排到队尾还是保留位置：开发者决定排到队尾（见上面的“语义”）
- [ ] map 的 key 只用了 uniqueKey，没有带 routeId，同一个槽位上不同玩家的同名 key 会被误合并；`uniqueKey` 的类型建议从 `Long` 改为 `Object`
- [ ] 删除未使用的 `SlotQueue.java`、`CoalescingSlotQueue.java`（旧设计遗留，而且没有合并逻辑）
- [ ] 还没有 `mergedCount()` 和唯一任务的测试

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
- [ ] 串行池最终是否升级成 Actor 邮箱模型（设计文档 6.2 节）？

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
