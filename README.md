# testServer —— SLG 服务端网络骨架（gateway / world / game）

依据 `docs/slg-server-design.md`（第 5 节通信协议、第 6 节线程模型、第 15 节模块划分、2.1 技术栈）搭建的**最基础骨架**：只有网络通信与消息分发，没有任何游戏业务逻辑。

- Java 17（`maven.compiler.release=17`，未使用任何 Java 21 特性），Maven 多模块
- Netty 4.1.115.Final + Protobuf 3.25.5（protoc 由 Maven 插件按平台自动下载，Windows/Linux/macOS 通用）
- 日志 SLF4J 2 + Logback 1.5；测试 JUnit 5
- 未接数据库、Redis、Kafka、Nacos、Actor、时间轮（代码里以 `TODO` 标注预留位置）

## 目录结构

```
testServer/
├── pom.xml                      # 父 POM：版本管理、release=17、enforcer(JDK17)、shade、protobuf 插件
├── bin/                         # 启动脚本（.bat 给 Windows，.sh 给 Linux/macOS）
│   ├── build.bat / build.sh
│   ├── start-all.bat / start-all.sh / stop-all.sh
│   ├── start-world.bat  start-game.bat  start-gateway.bat
│   ├── run-robot.bat            # 测试客户端
│   ├── _run.bat / run.sh        # 公共启动逻辑
├── slg-common/                  # 公共库（所有服务依赖）
│   └── src/main/
│       ├── proto/client.proto   # 客户端协议 body：ErrorResp / Heartbeat / PingReq / PingResp
│       ├── proto/internal.proto # 内部控制消息：握手 / 心跳 / 会话关闭
│       ├── resources/logback.xml
│       └── java/com/slg/common/
│           ├── config/          # ConfigLoader（properties 加载）、ServerConfig
│           ├── discovery/       # ServiceType、ServiceInstance、ServiceDiscovery 接口、StaticServiceDiscovery
│           ├── msg/             # MsgIds（消息 ID）、RouteTable（消息 ID→服务）、ErrorCodes
│           ├── net/client/      # 客户端帧：ClientMessage、ClientCodec
│           ├── net/internal/    # 内部协议：InternalPacket、InternalCodec、InternalServer、InternalClient(自动重连)
│           ├── dispatch/        # MessageDispatcher（msgId→handler）、MessageHandler、HandlerContext、KeyedExecutor
│           ├── server/          # BackendServer：world/game 共用的启动骨架
│           └── util/
├── slg-gateway/                 # 网关
│   └── com/slg/gateway/
│       ├── GatewayServer.java   # 入口
│       ├── net/                 # ClientServer（客户端监听）、ClientHandler（心跳/去重/路由）
│       ├── session/             # GatewaySession、SessionManager
│       └── backend/             # BackendManager（到 world/game 的连接、转发、回包回投）
├── slg-world/                   # 世界服：WorldServer + handler/WorldPingHandler
├── slg-game/                    # 主城服：GameServer + handler/GamePingHandler
└── slg-robot/                   # 测试客户端 RobotMain/RobotClient；test 里有进程内端到端测试
```

## 端口

| 服务 | 端口 | 用途 | 配置项 |
| --- | --- | --- | --- |
| gateway | 9001 | 客户端 TCP 长连接 | `gateway.properties` → `client.port` |
| world | 9101 | 内部 TCP（gateway 连入） | `world.properties` → `server.port` |
| game | 9201 | 内部 TCP（gateway 连入） | `game.properties` → `server.port` |

gateway 通过 `discovery.world.nodes` / `discovery.game.nodes`（格式 `nodeId@host:port`，逗号分隔多个）找到后端。

## 构建

前提：JDK 17、Maven 3.6.3+，且 `java`、`mvn` 在 PATH 中（或设置了 `JAVA_HOME`）。首次构建需联网下载依赖与 protoc。

```bat
cd /d E:\testServer
bin\build.bat                 :: = mvn -B clean package，包含单元测试和端到端测试
bin\build.bat -DskipTests     :: 跳过测试
```

产物（可直接 `java -jar`）：`slg-gateway\target\slg-gateway-all.jar`、`slg-world\target\slg-world-all.jar`、`slg-game\target\slg-game-all.jar`、`slg-robot\target\slg-robot-all.jar`。

> 父 POM 的 `maven-enforcer-plugin` 按设计文档要求只允许 JDK 17 构建。如果本机只有更高版本 JDK，可临时加 `-Denforcer.skip=true`（`release=17` 仍会阻止使用 17 以上的 API）。

## 启动顺序

建议顺序：**world → game → gateway → robot**。gateway 到后端的连接带自动重连（1s 起指数退避，上限 30s），所以顺序不严格：先起 gateway 也可以，后端起来后会自动连上；后端重启也会自动恢复。

Windows：

```bat
bin\start-all.bat             :: 在三个独立窗口中启动 world / game / gateway
bin\run-robot.bat             :: 默认连 127.0.0.1:9001，跑 3 轮
bin\run-robot.bat 127.0.0.1 9001 10
```

也可以分别执行 `bin\start-world.bat`、`bin\start-game.bat`、`bin\start-gateway.bat`。关闭窗口或 Ctrl+C 停止。

Linux/macOS：

```bash
bin/start-all.sh              # 后台启动，输出在 logs/<app>.out
bin/run.sh robot 127.0.0.1 9001 3
bin/stop-all.sh
```

测试客户端成功时输出类似：

```
connected to gateway 127.0.0.1:9001
[heartbeat] serverTime=1793784380379
[game] round 1 seq=2 pong from game-1 content=game-ping-1 rtt=7.83ms
[world] round 1 seq=3 pong from world-1 content=world-ping-1 rtt=5.56ms
...
RESULT: 7/7 ok -> PASS
```

退出码 0 表示全部成功。

### 配置与日志

- 默认配置打包在各 jar 内（`src/main/resources/*.properties`）。
- 覆盖方式（优先级从高到低）：
  1. 单项覆盖：`set JAVA_OPTS=-Dclient.port=9002` 后再运行启动脚本（任意配置 key 都可以用 `-Dkey=value` 覆盖）；
  2. 指定文件：`-Dslg.config=E:\testServer\conf\gateway.properties`；
  3. 工作目录下的 `config\<服务>.properties`（脚本以工程根目录为工作目录，即 `E:\testServer\config\gateway.properties`）。可从 `src/main/resources` 复制一份修改。
- 日志：控制台 + `logs\<app>.log`（按天/100MB 滚动）。
- JVM 参数：环境变量 `JAVA_OPTS`，默认 `-Xms256m -Xmx512m -XX:+UseG1GC`。

## 通信协议

### 客户端 ↔ gateway

```
| length(4B) | msgId(4B) | seq(4B) | flags(1B) | body(Protobuf) |
```

- 大端序；`length` = 其后所有字节数（9 + body 长度），单帧上限 `client.maxFrameLength`（默认 1MB）。
- `seq`：客户端自增，回包原样带回；gateway 丢弃重复/回退的 seq（`seq=0` 不参与去重）；服务端推送 `seq=0`。
- `flags`：bit0 压缩、bit1 加密——已预留，未实现。
- 心跳：客户端定期发 `HEARTBEAT_REQ(10001)`，gateway 本地回 `HEARTBEAT_RESP(10002)`；超过 `client.readerIdleSeconds`（默认 60s）无任何上行则断开。
- 出错时回 `ERROR_RESP(10099)`，seq 与请求一致，`code` 见 `ErrorCodes`（1 未知消息、2 后端不可用、3 body 解析失败、4 服务器异常、5 无 handler）。

### 消息号段与路由（`RouteTable.defaults()`）

| 号段 | 目标 | 当前消息 |
| --- | --- | --- |
| 1xxxx | gateway 本地处理 | 10001/10002 心跳，10099 错误 |
| 2xxxx | game | 20001 GAME_PING_REQ / 20002 GAME_PING_RESP |
| 3xxxx | world | 30001 WORLD_PING_REQ / 30002 WORLD_PING_RESP |

约定：请求为奇数，回包 = 请求 + 1。号段外可用 `RouteTable.route(msgId, service)` 做单条精确路由。

### gateway ↔ world/game（内部协议）

```
| length(4B) | kind(1B) | msgId(4B) | seq(4B) | sessionId(8B) | playerId(8B) | kingdomId(4B) | traceId(8B) | body |
```

- `kind`：1 FORWARD（gateway→后端，转发客户端请求）、2 RESPONSE（回包）、3 PUSH（主动推送）、4 CONTROL（握手/心跳/会话关闭）。
- 连接建立后 gateway 发 `HandshakeReq`，后端回 `HandshakeResp`，之后链路才可用。
- gateway 写空闲 `backend.heartbeatSeconds`（默认 10s）发心跳，3 倍时间无回包断开重连；后端 `server.readerIdleSeconds`（默认 60s）无数据断开。
- 客户端断线时 gateway 向所有后端发 `SessionClosed`，后端在该会话的业务线程上回调 `SessionListener`。

### 线程模型

- Netty IO 线程只做编解码与转发，不执行业务。
- world/game 收到 FORWARD 后由 `MessageDispatcher` 投递到 `KeyedExecutor`：按 `playerId`（未登录用 `sessionId`）哈希到固定单线程，同一玩家的消息严格串行。这是 Actor 调度的占位实现，后续替换为设计文档 6.2 的自研 Actor 时只需替换 `KeyedExecutor`。

## 如何新增消息和 handler

以给 game 增加「查询城市信息」为例：

1. **定义协议**：在 `slg-common/src/main/proto/client.proto` 加消息：

   ```proto
   message CityInfoReq {}
   message CityInfoResp { int32 level = 1; }
   ```

2. **分配消息 ID**：在 `MsgIds` 对应号段加常量（2xxxx 自动路由到 game，无需改路由表）：

   ```java
   public static final int CITY_INFO_REQ = 20011;
   public static final int CITY_INFO_RESP = 20012;
   ```

3. **写 handler**：在 `slg-game/.../handler/` 下：

   ```java
   public final class CityInfoHandler implements MessageHandler<CityInfoReq> {
       @Override
       public void handle(HandlerContext ctx, CityInfoReq req) {
           ctx.reply(MsgIds.CITY_INFO_RESP, CityInfoResp.newBuilder().setLevel(1).build());
       }
   }
   ```

   `HandlerContext` 提供 `sessionId()/playerId()/kingdomId()/traceId()`、`reply(...)`、`replyError(...)`、`push(...)`。handler 在业务线程执行，禁止阻塞调用。

4. **注册**：在 `GameServer.registerHandlers()` 中：

   ```java
   .register(MsgIds.CITY_INFO_REQ, CityInfoReq.parser(), new CityInfoHandler())
   ```

5. 重新 `bin\build.bat`，客户端按帧格式发送 `msgId=20011` 即可。

新增一个号段（如 4xxxx 联盟服）：在 `ServiceType` 加枚举、`RouteTable.defaults()` 加 `range(40000, 49999, ServiceType.ALLIANCE)`、`BackendManager.BACKENDS` 加上该类型，并在 `gateway.properties` 配置 `discovery.alliance.nodes`；新服务本身仿照 `WorldServer` 用 `BackendServer` 启动即可。

gateway 本地处理的消息（1xxxx）在 `ClientHandler.handleLocal` 中添加。

## 测试

- `slg-common`：`CodecTest`（客户端帧/内部包编解码，含半包）、`RouteTableTest`。
- `slg-robot`：`EndToEndTest` 在同一 JVM 内拉起三服，验证：后端未启动时返回 `BACKEND_UNAVAILABLE` → 后端启动后自动连上 → game/world ping-pong → game 重启后自动重连恢复 → 未知消息返回 `UNKNOWN_MSG`。

## 预留 / TODO（均未实现）

- 服务发现：`ServiceDiscovery` 接口已定义，当前为 `StaticServiceDiscovery`；接 Nacos/etcd 时新增实现即可。
- gateway：Token 鉴权与登录绑定 `playerId`、限流、压缩/加密、断线重连会话保持与补发、推送合并、按王国/一致性哈希选节点、WebSocket 接入。
- 后端：Actor 框架、时间轮、持久化、gRPC、Kafka、监控指标。
- 内部协议：`deadline` 字段。
