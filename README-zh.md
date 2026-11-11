# monarch-persistence

[English](README.md) | 中文

## 当前 R1/R2 入口

当前 R2 候选为 `0.3.0-dev.r2-20261106-dc168f96fa62`，配对固定的 `monarch-core` `0.2.1` 制品。这是冻结制品对，不表示正式 `0.3.0` 已发布。

- R1：`PersistentMonarch.managedBehavior` 提供可恢复外部操作接线。
- R2：`WorkflowHost` / `WorkflowProgram` 与 `PersistentInbox` 提供有限作业、限定资源协调和耐久子任务交接，复用已有阶段引擎，不替换它。

详见 [R1 指南](docs/RECOVERABLE_OPERATIONS.zh.md)、[R2 指南](docs/DURABLE_WORKFLOWS.zh.md)、[R1/R2 验收记录](docs/ACCEPTANCE.zh.md)和[示例指南](examples/README.md)。

下方教程只介绍 **legacy `PersistentMonarch.behavior` / `behaviorWithSettings` checkpoint 包装层**，不是 R2 工作流 API。历史 `0.2.1` 测试数量与发布示例不是本次新增的 R1/R2 测试结果，也不是替换冻结制品对的操作指令。指向 sibling core 教程的链接是需要另行检出该仓库的工作区参考，不保证只干净检出本仓即可使用。

## Legacy checkpoint 包装层教程

为不依赖 Akka 的 [monarch-core](../monarch-core/README-zh.md) 提供 Akka Persistence 支持。core 把小进度卡依次交给各阶段；这个包装层把卡和已接受进度记进 journal，让恢复后的 actor 可以继续。一个实体拥有一次运行。

**历史 0.2.1 基线，不是当前 R2 总览或远程发布声明。** 当时增加了等待持久化确认的 checkpoint、分类失败处置状态的持久化、严格的 JVM 内运行所有权、Protobuf 持久化格式和可选快照。已有验收记录为 core 34 个测试、persistence 33 个常规测试通过，两仓对已发布 0.2 的 MiMa 兼容检查通过；本次文档修改没有重跑。范围和测量见 [VALIDATION.md](VALIDATION.md)，这是库级检查，不是完整银行/Fab 业务验收或生产性能承诺。

## 快速开始

如果还不熟悉 Stage、State 和解释器，先看 [core 的完整控制台例子](../monarch-core/README-zh.md#先跑通一次控制台流程)。这里沿用同一批充值：准备文件、提交充值批、整理摘要。包装层负责耐久进度，不负责充值实现或子批调度。

下面是**应用接线伪代码**，不是独立程序。`RechargeConsole` 是 core 指南里的应用对象；`progressToBytes`、`progressFromBytes` 是宿主实现的业务状态编码/解码函数，需兼容历史版本。在 actor 线程构造 behavior、捕获执行上下文，并将它启动为 actor（或注册到 Cluster Sharding），之后才能发送 `StartRun`。

```scala
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.PersistenceId
import net.imadz.monarch.persistence._
import scala.concurrent.ExecutionContext
import RechargeConsole.{Progress, PrepareFile, SubmitBatch, Summarize, runStage}

// parentBehavior 本身也需要由应用的 ActorSystem 启动。
val parentBehavior = Behaviors.setup[Nothing] { context =>
  implicit val ec: ExecutionContext = context.executionContext
  val runId = "recharge-001"
  val behavior = PersistentMonarch.behavior[RechargeConsole.Stage, Progress](
    persistenceId = PersistenceId("recharge-run", runId),
    stages = Vector(PrepareFile, SubmitBatch, Summarize),
    interpreter = (stage, state) => runStage(stage, state, rejectBatch = false),
    stateCodec = StateCodec[Progress](progressToBytes, progressFromBytes),
    stageName = stage => stage.name
  )
  val runActor = context.spawn(behavior, "recharge-run")
  val replies = context.spawn(Behaviors.setup[PersistentMonarch.RunReply] { replyContext =>
    Behaviors.receiveMessage[PersistentMonarch.RunReply] { reply =>
      replyContext.log.info("Run start reply: {}", reply)
      Behaviors.same
    }
  }, "run-replies")
  runActor ! PersistentMonarch.StartRun(runId, Progress(runId, requestCount = 5L), replies)
  Behaviors.empty
}
```

启动 ActorSystem 前，需要配置并初始化 Akka Persistence 的 **journal 插件**及其存储。添加依赖不会自动安装数据库或完成初始化。若启用快照，还要配置 snapshot store；快照默认关闭。示例的 `demo://` 引用仍是模拟，真实恢复需要可访问的耐久产物。解释器的 Future 回调不要操作 ActorContext，所需 ActorRef 应在 actor 线程捕获。

`Accepted` 只在 `RunStarted` 提交后回复，表示“起始进度已经记住”，**不表示充值批已经成功**。已经进入终态的实体会拒绝另一个 `StartRun`；新的运行用新的 `persistenceId`。

## 明确拒绝后，带着处置状态继续收尾

在应用代码中定义下面的策略，再给 `behavior` 增加 `failurePolicy = Some(reviewRejectedSubmission)` 参数。要在模拟例子中看到拒绝，还需把解释器的 `rejectBatch` 改为 `true`：

```scala
import net.imadz.monarch.{StageError, StageFailedException}
import scala.concurrent.Future

val reviewRejectedSubmission: (String, StageError, Progress) => Future[Progress] =
  (_, error, before) =>
    if (error.code.contains("BATCH_REJECTED"))
      Future.successful(before.copy(reviewRequired = true))
    else Future.failed(StageFailedException(error))
```

策略拿到的是**失败阶段执行前**的状态。成功返回后写入 **`StageResolved`**，而不是正常完成的 `StageDone`。该 checkpoint 提交后，`Summarize` 收到“需要人工检查”的卡；提交阶段不会自动重试。正常完成与处置完成都推进连续已接受前缀，并携带恰好匹配的状态。

应用仍可分类失败项、拆批、返回耐久子任务引用，再交给独立有界调度器。legacy `PersistentMonarch.behavior` 包装层不提供这些业务规则，不创建子任务，也不往本次运行动态插入阶段。R2 的有限作业与耐久子任务交接是独立 API，详见 [R2 指南](docs/DURABLE_WORKFLOWS.zh.md)，不能与这个旧包装层混为一谈。外部副作用和策略都必须能承受重跑：尚未确认的阶段或处置过程可能在恢复后再次执行。

checkpoint、codec、journal 的基础设施故障**不会进入业务 `failurePolicy`**。checkpoint 超时可能是提交结果未知，不能据此自动拆批、补偿或重试未知写入。

## 恢复与运行所有权

沿用充值例子：若 `prepare-file#0` 和 `submit-batch#1` 已提交，恢复时取出提交后的卡，验证这两个游标是计划的连续前缀，再运行 `summarize#2`。如果提交经过人工检查处置，恢复的是 `StageResolved` 里的卡。提交的 checkpoint 未落盘时可能重跑；已经落盘但确认丢失时，回放找到它，就会跳过该阶段。

包装层每次用传入的阶段列表建立新的 core 引擎，从零分配位置。宿主要保持顺序、命名与业务含义稳定。core 的可变计划方法不会改写此 actor 的记录计划，也不会向当前 run 插入工作。下面允许的尾部扩展是执行中实体**恢复时的配置变化**，不是运行中的调度 API。

主要边界如下：

- `RunStarted` 提交后才回复 `Accepted` 并启动阶段。
- 每次运行最多有一个未确认的 checkpoint。下一阶段等待当前 checkpoint 的持久化确认；`RunCompleted` 也必须等待最后一个阶段的确认。
- 未提交 checkpoint 的阶段可能在恢复后重跑。已经提交但确认丢失的 checkpoint 会在回放时被识别，其阶段不会再次执行。
- 启动、恢复时获取严格的 `RunLease`；停止、重启、进入终态或 checkpoint 基础设施故障时撤销所有权。内部结果携带运行代际标识，接收端也会校验，不能只依赖发送前检查。
- 所有权保护仅在 **JVM 内**有效。跨 JVM 的集群所有权、外部系统 fencing 和副作用幂等性仍由宿主负责；已经发出的 I/O 不会被自动取消。
- 恢复时校验已记录计划和已完成 cursor，要求进度恰好是当前计划的连续前缀。允许在计划尾部追加阶段，扩展通过 `RunPlanExtended` 落盘后才执行。
- 不允许删除、重排或改写已记录计划；重复、乱序或有缺口的历史进度会在启动 interpreter 前被拒绝。
- 对旧版未持久化 resolved 进度造成的历史缺口，不能拿后续状态推测补齐。实体保持不执行状态，记录需要人工恢复的诊断，不改写历史。名称和位置校验不能识别同名阶段的业务语义变化，宿主必须保证计划语义稳定。
- 已进入终态的实体拒绝新的 `StartRun`；新的运行应使用新的 `persistenceId`。

阶段执行完成、checkpoint 已提交、外部业务副作用成功是不同的概念，不能据此推断整条链路具有 exactly-once 语义。

## Protobuf 与 0.2 兼容

[`src/main/protobuf/monarch.proto`](src/main/protobuf/monarch.proto) 定义持久化事件、运行状态和完整快照 envelope。ScalaPB 生成托管源码，`MonarchProtoConverters` 在保持现有 Scala API 的前提下完成转换，`MonarchSerializer` 负责 wire bytes。

- serializer identifier 仍为 `77102121`。
- 当前写入 manifest：`E2` 用于 `VersionedRunStarted` / `RunAwaiting`，`E1` 用于其他事件，`S2` 用于 `DurableState`，`R1` 用于 `RunState`；读取仍接受旧 `S1` 快照。
- 演进协议时不能复用已有字段编号或 oneof 分支编号。不支持的 schema 版本和空分支会明确失败，不会静默丢弃历史。

库的 [`reference.conf`](src/main/resources/reference.conf) 将新的事件、状态及快照写入绑定到 Protobuf serializer。**`StateCodec[S]` 仍只负责业务状态 bytes**，包括业务状态格式的版本兼容和旧数据解码；它不是外层事件 serializer，也不是远程命令 serializer。业务状态引用的外部产物还必须在恢复时可用。

### 新写 Protobuf，旧 journal 继续读取

0.2 的 Jackson-CBOR/JSON 记录仍按其已存储的 serializer identifier 和类 manifest 解码。Jackson 读取路径保留对已发布四种旧事件的支持，新写入使用 Protobuf。正常旧 journal 恢复、旧 Jackson 与新 Protobuf 混合回放已经测试。

如果宿主配置了更具体的事件 serialization binding，需要核对实际 writer 格式，并保留旧 reader。**不需要删除、覆盖或重新发布 0.2**；不要让新旧 writer 同时写同一个 `persistenceId`，也不要在写入新事件或快照后无迁移方案地降级到 0.2。

### 泛型命令与远程通信

内部 checkpoint 命令携带本地 `Promise`，不写入 journal，也不参与远程序列化。跨节点发送 `StartRun[S]` 时，宿主必须提供覆盖 `S` 和 `replyTo` 的显式命令 serializer/binding，库不能自动编码任意泛型业务状态。

`RemoteCommandCompatibilitySpec` 验证了代表性业务状态及可用回复 `ActorRef` 的序列化往返，但它**不是多节点网络通信测试**。

## 配置与可观测性

原 `behavior` 签名保持不变，默认设置为单个未确认 checkpoint、有限超时和关闭快照。需要配置时使用新增的 `behaviorWithSettings`。下例是宿主接线伪代码：`persistenceId`、`stages`、`interpreter`、`codec` 和隐式 `ExecutionContext` 都来自应用。

```scala
import net.imadz.monarch.persistence.{PersistentMonarch, PersistentMonarchSettings}
import scala.concurrent.duration._

val configured = PersistentMonarch.behaviorWithSettings(
  persistenceId, stages, interpreter, codec,
  PersistentMonarchSettings(
    checkpointTimeout = 30.seconds,
    snapshotEvery = Some(1000),
    keepSnapshots = 2
  )
)
```

### checkpoint 超时

默认超时为 **30 秒**，应由宿主根据 journal 的服务级别要求调整。超时机制会独立使未完成确认失败并撤销执行所有权，即使 actor 正在等待 journal I/O，也能解除引擎对该确认的等待。

actor 在能够处理故障消息时停止；库不重试未知写入，也不伪造业务失败终态。之后必须通过受控恢复确认事件是否已经提交。**永久挂起的 journal 仍需要宿主运维处理，不能保证 actor 立即停止。**

### 快照

快照默认关闭。启用后保存完整 envelope，包括计划、运行状态、连续进度、业务状态 bytes 和校验版本。已验证快照加 journal 尾部回放，以及快照保存失败后从完整 journal 恢复。

快照减少恢复回放量，**不会减少每阶段事件写入的成本或写放大**。快照保留策略可能清理旧快照，但此配置**不启用 journal 事件删除**。

### Observer

`RunObserver` 是非阻塞通知接口。observer 的 `NonFatal` 异常会被记录并隔离，不阻断执行控制；回调必须由宿主保持非阻塞。

- `onStageComplete`：阶段执行完成，不表示 checkpoint 已被持久化接受。
- `DurableRunObserver.onCheckpointCommitted`：正常阶段的 checkpoint 已提交。
- `DurableRunObserver.onResolutionCommitted`：分类恢复状态的 checkpoint 已提交。
- `onCompleted`：运行终态事件提交后通知。

这些通知都不是 exactly-once 的业务副作用钩子，不应直接用作无幂等保护的外部交易提交机制。

## 大数据量与流批一体使用边界

Stage 应代表有意义的流程、事务或恢复边界。在阶段内部使用有界流或分块处理，业务状态 `S` 只保留紧凑的 offset、计数器和耐久产物引用。

不要将每条记录建模成一个 Stage，也不要在每次 checkpoint 中保存全部历史记录。计划和 cursor 历史仍为 **O(N)**；`window = 1` 只限制 checkpoint 积压，**不保证应用总内存为 O(1)，也不限制不同运行之间的全局并发**。

紧凑状态可以包含来源分区与 offset、sink 幂等键以及耐久输出产物引用。阶段 interpreter 实现有界读取和 sink 写入，失败策略提供拆批与重新调度引用。应用还需负责分块、资源准入、跨运行并发控制及外部副作用一致性，库不能从这些状态字段自动推导出 exactly-once 的 source/sink 事务。

## 构建、测试与本地候选包

使用 JDK 11、Scala 2.13.14、sbt 1.4.9。**命令必须在对应仓库执行，不在工作区根目录执行。**

此前验收记录两仓的 `Test / fork` 均为 `false`，可用 `sbt 'show Test / fork'` 核对。fork、测试并行与 actor/Future dispatcher 是不同配置；`fork = false` 不表示所有测试或 Future 回调都在同一个线程运行。以下命令供后续构建使用，本次文档修改未执行它们。

```bash
sbt test mimaReportBinaryIssues
```

### 历史 0.2.1 候选的隔离解析（仅供追溯）

以下是历史 `0.2.1` 候选接线，不是重建或替换当前冻结 R2/core 制品对的操作指令。当时 persistence 依赖实际的 core `0.2.1` artifact，并不直接引用 sibling 源码；在 core `0.2.1` 远程发布前，验收使用隔离的本地 Maven 目录，避免覆盖共享的 `~/.ivy2/local`。

历史顺序为先在 **monarch-core 仓库**生成候选包：

```bash
sbt 'set version := "0.2.1"' \
  'set publishTo := Some(Resolver.file("candidate", file("target/isolated-maven"))(Resolver.mavenStylePatterns))' publish
```

再在 **monarch-persistence 仓库**提供该目录的绝对 file URL：

```bash
sbt -Dmonarch.core.repository=file:///absolute/path/to/monarch-core/target/isolated-maven/ \
  test mimaReportBinaryIssues
```

这里的 `publish` 仅写入明确指定的本地目录，不发布到 Maven Central。可检查 `Compile / dependencyClasspath` 确认解析到的实际候选 jar。`monarch.core.repository` 是显式启用的 resolver，不会静默替换成 sibling 源码依赖。

### 验证范围与容量测试

历史 0.2.1 候选验收结果：Core **34** 个测试、Persistence **33** 个常规测试通过，两仓对已发布 0.2 的 MiMa 二进制兼容检查通过。常规测试包含实际序列化、受控 journal 确认、停止/重启、旧 journal 恢复、快照，以及使用测试专用耐久文件 journal 强制终止并重启真实 JDK 11 子进程的恢复测试。

这些是库契约验证，不是生产数据库插件或完整业务验收。容量测试必须显式开启，常规测试运行中会明确取消该用例，不计入常规通过数量：

```bash
sbt -Dmonarch.core.repository=file:///absolute/candidate/repository/ \
  -Dmonarch.capacity=true 'testOnly net.imadz.monarch.persistence.PersistenceCapacitySpec'
# 可选样本参数：
# -Dmonarch.capacity.stages=1000 -Dmonarch.capacity.bytes=16384
# -Dmonarch.capacity.stages=100 -Dmonarch.capacity.bytes=1048576
```

三个已通过样本分别为 10,000 个 1 KiB、1,000 个 16 KiB、100 个 1 MiB 状态 checkpoint；均验证最大未确认 checkpoint 数量为 1 和连续恢复状态。测试 journal 的调度延迟不能当作生产数据库延迟或吞吐承诺。

完整测量、复现命令和限制见 [VALIDATION.md](VALIDATION.md)。Core 的百万个立即完成阶段单样本中，候选版执行时间约慢 32%，大计划装载时间则降低；这不是 JMH 稳态基准，不能据此宣称整体性能提升。

**未验证或实现：**生产 MongoDB/JDBC journal 压测、多节点集群所有权、exactly-once sink、完整半导体或银行业务、JMH/JFR/保留堆分析，以及无限动态运行队列。

## 发布边界

版本由 tag 派生。现有 tag 触发发布流程以测试和对 0.2 的 MiMa 检查作为门禁。先发布 core 0.2.1、再发布 persistence 0.2.1 的顺序仅指历史 0.2.1 基线，不适用于替换当前冻结的 R2/core 制品对。

不得覆盖冻结制品对或以原版本重新发布。本次文档提交不授权打 tag、推送、发布制品、升级应用或迁移 journal，也不更新其他运行库；这些操作仍需单独授权。

版本说明建议用 **CHANGELOG** 保存逐版本变更，README 只放简短入口，**GitHub Release** 写面向用户的本版摘要与升级提示。[VALIDATION.md](VALIDATION.md)提供验收证据，不替代发布说明。本次不创建 CHANGELOG 或 GitHub Release，等明确请求后再做。
