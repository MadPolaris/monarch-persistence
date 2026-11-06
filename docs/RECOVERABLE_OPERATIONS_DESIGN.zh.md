# Monarch 一步一脚印：银行与 Fab 能力 Gap、加固设计及实施路线

- 设计日期：2026-11-06
- 状态：原始设计已批准；R1 已实现/提交，用户随后授权继续 G2–G4。R2 实现和验收进度以 [ACCEPTANCE.zh.md](ACCEPTANCE.zh.md) 为准。
- 实施分支：`feature/monarch-recoverable-operations`，基于 `monarch-persistence@e6a6031`。
- 下文保留原始目标设计与当时的分门授权，不回写历史为测试结果；最终实际 API/范围见 [DURABLE_WORKFLOWS.zh.md](DURABLE_WORKFLOWS.zh.md)。

## Context

用户已决定暂不采用 Graph／Akka Streams 路线，集中在 Monarch 上，以最小成本、最小风险承接原 Java PersistentFSM 的能力：**先记录意图，明确进行中状态；重启先查证，结果确证并提交后才推进；跨资源交接、部分重试及业务收尾也必须可恢复。** 允许从 `dag-engine-core`、银行与 Fab 项目提炼通用能力进入 Monarch，银行与 Fab 都是验收场景。

现有 demo **有阶段持久化**，不是全内存程序；缺口是外部操作暂态、调度与业务收尾的持久化覆盖。历史核查还发现，pre-Monarch 的通用 Future 流程已采用整链重跑，不能把问题归因于纯 core 与 persistence 分层，也不能搬回旧执行器解决。

**推荐单一方案：保留 core 执行语义；在同一 `monarch-persistence` 项目增加通用可恢复操作、资源准入、可靠交接与最小批调度。先证明单操作，再证明两资源区，再增加批量与业务策略，最后迁移页面。** 不另建引擎、不建长期 fork、不以界面流畅代替恢复认证。

本文件只包含当前 Monarch 方案。先前的 Graph／Akka Streams 方案另行保留为历史讨论记录，**其模块、API、分支与实施步骤均不再执行**。原设计落盘时尚未实施；后续已按G0→G1验收，获得继续授权后实施G2–G4，各门实际结果单独记录。

**首轮实施边界：先执行 G0→G1，交付 R1 的代码、银行/Fab 单操作故障证据和剩余限制；在 G1 验收点停下汇报。G2–G4 是后续已设计路线，不与首轮同时铺开。** 不以计划获批代替各门测试通过，也不默认获得消费者 dirty 文件覆盖、commit/merge/push 或外部发布授权。

## 1. 目标能力与保证边界

### 两个可独立交付的终点

- **R1：可靠步骤执行。** 银行外部任务和 Fab 加工任务在 `Dispatching/Ongoing/Unknown` 中发生进程故障，恢复后按同一操作身份查证；既不跳下一步，也不盲目重发。完成后能够可靠查询结果。即使后续暂缓，R1 仍直接改善现有 Monarch。
- **R2：最小完整长批流程。** 在 R1 上加入两资源区交接、资源级成批与准入、逐项分类／有限返工、耐久待处理／重试及真实业务排空；银行与 Fab 小型端到端场景通过同一套公共库，不为每个业务另写恢复 FSM。

### 必须守住的不变量

1. 外部副作用前已有持久意图、稳定身份和不可变请求；恢复不换 ID。
2. `Ongoing/Unknown` 不是成功，也不自动转成终态失败或安全重试；没有证据就保持待查证／阻塞。
3. 操作确证结果提交 → Monarch 阶段 checkpoint 提交 → 后续步骤，严格按此顺序。
4. 封定批次的成员、顺序、请求、策略／codec 版本不因重启而变化。
5. 输入项不能丢失或重复处置；部分成功不能因整批重试再次生效。
6. 下游“已耐久接收”、下游“业务完成”、上游“全部结算”是三个不同事实。
7. `Queued` 不是资源许可；超时／lease 失效不是物理任务已停止，未知占用不贸然释放。

**认证边界：** 首轮验证单一有效 owner、可靠 journal 以及独立外部任务服务下的进程崩溃恢复。外部 exactly-once 取决于幂等／权威查询／事务条件；不承诺无条件自动治愈。真实断电、存储损坏、脑裂、生产吞吐 SLA 与真实设备安全认证另设门，不以 JVM kill 或本地 `force(true)` 冒充。

## 2. 能力 Gap 与归属

设计核对基线：`monarch-core@376b152`、`monarch-persistence@e6a6031`；旧 `playAkkaCQRS@8ab0fb1` 消费 core 0.2 并自写持久化 Actor；Fab standalone 的结论包含核对时的 dirty 工作树，不等同于纯提交状态。实施前再次核对工作树边界，不将这些基线描述当作持续更新的运行状态。

| 能力 | 当前已有／缺口 | 推荐归属与处理 | 验收门 |
|---|---|---|---|
| 顺序 stage、游标、完成确认 | core 已有 Future 阶段；当前 persistence 已等待 checkpoint ACK | **复用，不改 core 执行模型**，不搬回旧 Unit 回调宿主 | G0/G1 |
| 意图、进行中、外部任务查证 | 当前只恢复未完成 stage；缺通用 operation FSM | persistence 新增 `PersistentOperation`，宿主提供 submit/query 与证据验证 | G1 |
| Unknown 阻塞 | core 成功 State 会推进，异常可能终态失败；Fab 有局部 RecoveryRequired | managed adapter 吸收不确定性，持久挂起／查证，不走普通 failure interceptor | G1 |
| 幂等启动、挂接、结果查询 | 只有 StartRun；重复拒绝；终态丢结果 bytes | 原 `PersistentMonarch` 加 StartOrAttach/GetRun、请求指纹、终态保留 | G1 |
| owner 与等待生命周期 | checkpoint Promise 有 guard，operation waiter/timer 尚无 | 新 `ManagedRunScope` 受现有 lease 撤销控制；清理本地等待，不取消已发外部效果 | G1 |
| 版本化 wire、尺寸边界 | 有 PB/state codec；无新命令 serializer、完整限额 | 同步模型/PB/converter/serializer；新消息使用具体 bytes DTO 与尺寸限制 | G1 |
| 跨区／跨子批可靠交接 | demo 完成 observer 中 fire-and-forget 收尾 | 显式 PrepareDelivery/Deliver/AwaitChild 步骤＋接收方耐久 inbox | G2 |
| 资源容量与任务准入 | 部分逻辑容量／内存设备队列；无完整 reservation 协议 | 通用 AdmissionPort＋限定资源域 ResourceCoordinator，业务资源语义留宿主 | G2 |
| 资源级成批 | AreaScheduler 有策略雏形，队列不耐久、切分复用 ID | 提炼纯 BatchPolicy，新增冻结 manifest 与最小耐久 BatchCoordinator | G3 |
| 逐项分类守恒 | 三分类可用，但 flatMap/zip 会漏项；未知可转失败 | 按稳定 ItemId 校验覆盖与冲突，记录四类处置及策略版本 | G3 |
| 逐项重试／返工 | 计数常为整批；延期 timer/路由意图在内存 | 持久化 item attempt、nextDue、动作与 lineage；只有明确安全项能重执行 | G3 |
| 输入关闭、业务排空、业务收尾 | 内存 queue/running、余额/持仓、申购衔接不构成完整恢复 | JobClosing/Drained，纳入未定项、交接、子任务及结果确认 | G2–G4 |
| 页面与运行状态 | 部分内存指标，Actor crash 不能证明 JVM 恢复 | 页面消费权威 Run/Operation/Job 查询及可重建视图；observer 只做展示 | G4 |

**职责分界：** 框架负责持久状态、身份、重试／交接协议及有界执行；连接器负责外部状态含义和证据；银行／Fab 宿主负责账务、工艺、质量判定、资源需求与业务返工决策。Saga 仍负责需要的跨聚合事务，不与本次操作恢复／流程调度混成一种机制。

## 3. 模块与抽取策略：尽量不增加发布面

```text
monarch-core                         不变：Stage × State → Future[State]
        ▲
monarch-persistence                  单一新候选 artifact
  PersistentMonarch                  流程进度、checkpoint、查询
  operation/                         外部操作意图、查证、终态
  resource/                          限定资源域的准入与占用交接
  batch/                             manifest、逐项处置、可靠派发与排空
  workflow/                          纯模型／策略，不 import Akka
        ▲
examples（不发布）                    银行 + Fab，消费公开 JAR
        ▲
Bank / Fab 薄适配与页面               后置迁移，不另写恢复内核
```

- 新纯模型暂放同 artifact 的独立包，避免首轮为了模块整齐就同时发布多个新库。core 不增加 Akka、Streams 或银行/Fab 依赖；无需改 core 时不发新 core。
- `PersistentOperation` 是一个通用外部操作 FSM，不是第二个阶段引擎；所有业务用参数化 adapter，不再为每种业务复制 actor 状态机。
- 一个事实一个权威位置：Run 管流程进度；Operation 管外部执行证据；Reservation 管逻辑资源归属；Job/Inbox 管工作接管。记录相互引用，不复制整套父／子 journal 再自行猜测一致性。

| 来源 | 提炼进入 Monarch 的内容 | 必须修正／留在来源的内容 |
|---|---|---|
| salary-saving Java FSM | intent-before-effect、暂态与结果分离、恢复处理器、剩余待查证项 | 不照搬恢复即重发、事件应用中的副作用及业务 DAO |
| dag-engine-core BankChain/SubBatchPipeline | 明确步骤和可替换通信端口、阶段中间值 | FTP、账务、交易协议仍是宿主；旧 ChainExecutionActor 不迁入 |
| ResultClassifier/ReconfirmHandler/ReBatchRouter | 分类／查证／决策分离、显式下一步动作 | 去掉 Any、按位置配对、漏项和 Unknown 默认重试；保留领域策略在宿主 |
| AreaScheduler | 数量／等待／兼容键等纯组批决策 | 不搬可变内存队列、切分复用 ID、未实际执行的资源参数 |
| Fab adapter / PipelineStages | Busy、Unknown、Unavailable 的语义分离；Queued≠许可；回执身份校验 | 不搬匿名 ProcessRecipe、随机 jobId、内存 registry、看 Idle 推断未执行 |
| Fab 持久宿主 | RecoveryRequired 的保守边界与 durable callback 用法 | 不复制父子双份 checkpoint 协调；OCAP、recipe、Lot/Saga 保留 Fab |

迁移采用“库先实现并验证 → 原调用点薄适配 → 旧类型兼容转发／删除”的顺序。最终只保留一份通用实现；当前 dirty 源文件不能直接搬空或覆盖。先做一个银行消费者与一个 Fab 消费者，不同时重构所有同类 demo。

## 4. R1 设计：操作恢复与流程恢复分层

### 4.1 通用 `PersistentOperation`，不是再写一套业务 FSM

宿主注册一个版本化 `ExternalOperationPort[Request, Result]`：`submit`、`query`、回执／证据校验。框架保存操作状态、调度查证和幂等返回结果；FTP、交易系统、设备、天车只实现端口，不各自复制恢复状态机。

```text
Prepared ──提交发送意图──> Dispatching ──对方确认接受──> Ongoing
                              │                         │
                     结果不明／恢复                 轮询／恢复
                              └─────> Reconciling <─────┘
                                          ├─ Completed(evidence)
                                          ├─ KnownFailed(evidence)
                                          ├─ Running → 安排下一次查证
                                          ├─ SafeToSubmit(evidence) → 同身份提交
                                          └─ Unknown → 继续查证／Blocked
```

- `Prepared` 已保存稳定 operation key、请求 bytes／不可变 artifact 引用及摘要、步骤／连接器版本、资源 reservation 引用；未记录 Dispatching 前不得调用外部。
- `Dispatching` 表示“可能已经发出”，不是“对方已接受”。恢复该状态默认查询原操作；只有对端支持相同 key 幂等提交，或有可靠的“未接受且旧调用不可能再生效”证据，才允许重发。
- `Running/Completed/Rejected/KnownFailed/Unknown` 是类型化观察结果。普通 `NotFound`、设备 Idle、查询超时或 registry 丢失都不等于 `SafeToSubmit`。
- 外部 taskId、服务／设备 incarnation、请求摘要与回执关联必须验证；旧实例的迟到结果不能完成新操作。终态单调，冲突证据进入诊断／阻塞，不回退为 Running。
- `nextCheckAt`、查证预算、阻塞原因均持久化。恢复后重建定时动作；事件 application 只演进状态，不调用外部、不发送消息。
- 已确证的结果先持久化，再回复等待者。网络查询暂时失败不直接成为业务失败；Unknown 自动预算耗尽时停止自动查询并显式 Blocked，可通过 `Reconcile` 恢复查证同一操作。
- 不提供无证据的 `forceSuccess/forceRetry`。真实人工裁决需要领域证据与审计入口，首轮只支持补充证据后继续查证，不实现完整审批平台。

**身份规则：** 技术重试／重新挂接保持 `OperationKey`；新业务执行或明确返工才使用新的执行序号，并关联原 ItemId／父操作。`EffectKey` 表示业务副作用身份：同一扣款重试保持不变，明确的再次加工是新执行。不能用随机 run generation、批次重新切分 ID 或调用计数决定业务幂等性。重复 Stage 通过持久计划中的显式稳定 stepId 区分。

### 4.2 保持 `Future[State]`，增加受管理的解释器适配

新增 persistence 侧 `managedBehavior`，其解释器接受 `ManagedStageScope`；旧 behavior／core 接口继续可用。scope 在每个 run owner／阶段调用时创建，带已知 cursor 与 generation，不反射 builder 的私有字段。

目标调用形态（拟议 API，G1 编译验收固定）：

```scala
PersistentMonarch.managedBehavior(
  persistenceId = id,
  definition = frozenDefinition,
  stateCodec = codec,
  interpreter = (step, state, scope) => step match {
    case SubmitAndWait(stepId) =>
      scope.operations
        .awaitTerminal(requestFor(stepId, state))
        .map(result => state.withConfirmedResult(result))
    case Classify(stepId) =>
      Future.successful(classifyWithVersion(stepId, state))
  }
)
```

- `awaitTerminal` 不是持久 Promise：耐久事实在 Operation。scope 只维护有界的本地等待／轮询／订阅，未知时 Future 不成功、不泄漏为普通失败。确定业务失败如何终止或分类由宿主显式映射。
- 在 Run 中持久保存薄的 `AwaitingRef(cursor, operationKey)` 以便查询／恢复定位；Operation 保存实际状态与证据。引用提交后再调用／挂接，checkpoint 后清除引用，不复制整份 operation 状态。
- scope 的 cursor 由 persistence 中既有的“已提交前缀 ordinal＋冻结计划”确定，不能依赖运行时调用次数；新增等待引用的提交确认也校验 owner generation 与当前 cursor。OperationKey 由冻结的业务身份构造，不包含 generation。这样无需给 core 的 StageInterpreter 增加 Akka 或恢复参数。
- 旧 owner 撤销、`PreRestart/PostStop`、checkpoint 基础设施失败时，清理 scope 的本地 timer／waiter并以 `StaleRun` 结束旧等待；不把 Operation 标失败、不宣称撤销外部调用。
- 不使用一个数小时的 ask；查询使用有限超时，并由 scope／Operation 的持久恢复协议继续。每 run 等待数量、查询频率与全局执行准入有界。
- 首轮不新增 core Pause ADT。若 G1 证明受管理等待无法在目标规模下控制资源，再在同门内评估显式挂起接口，不能边带泄漏边进入批调度。

### 4.3 Run 的幂等挂接、查询与序列化

直接扩展原 `PersistentMonarch`：

- `StartOrAttach`：同 runId、初始请求摘要、计划／codec 版本返回同一已提交视图；冲突拒绝；非 Idle 挂接不重新 acquireOwner 或调用第二次 runEngine。
- `GetRun`：返回已提交 frontier、最后状态、AwaitingRef、Completed／KnownFailed 或 RecoveryBlocked。状态查询不会把日志里的 Executing 直接解释为“当前正在健康运行”；查询超时返回不可用，不伪造业务失败。
- 内部 `DurableState` 保留初始身份和终态最后提交 bytes；尽量不改旧公开 `Done/Failed` 构造器。失败时的最后 checkpoint 不冒充失败操作结果，后者由 Operation 查询。
- 新命令／回复用具体版本化 bytes DTO 和专用 serializer；不绑定整个 `Command[S]`，避免把含 Promise 的本地消息当作 wire 协议。使用 ActorRefResolver，并复制／使用不可变 bytes 防止调用方修改。
- 同步 Scala、proto、converter、serializer、bindings、fixtures；旧 E1/S1/R1 仍可读，新语义明确版本化。旧终态快照缺结果时返回 unavailable，不能重新执行副作用来补数据。
- 新写入限制 request/state/result/envelope 大小、plan/metadata 数量及等待数量。业务大文件使用 immutable manifest/ref＋hash/size，路径可恢复性由宿主保证。
- `Effect.stop` 不是自动恢复器。示例宿主负责按稳定身份重新创建 actor，区分停止／基础设施不可用／不兼容阻塞；不无上限重启损坏 journal。

## 5. R2 设计：可靠交接、资源与批处理

### 5.1 先用显式步骤完成交接，不新建全功能 outbox 平台

```text
Classify/PrepareDelivery
  → checkpoint 保存结果、去向、DeliveryId、childId、payload 摘要
Deliver
  → 向目标 inbox 重发同一 DeliveryId
  → 下游持久化 Accepted 后才完成本步骤
AwaitChild（业务要求时）
  → 查询 childId，等待其确证完成／结算
Finish
```

- 上游 Run 的状态就是这条有限交接的 outbox；下游 Job/Inbox 以 DeliveryId＋payload 摘要去重，并保存启动子 run 的意图。不在 `observer.onCompleted` 中唯一地启动后继业务。
- 接收 ACK 丢失重发同一身份；下游 accepted 是承担执行责任，不是执行成功。父作业按合同等待必要子任务终态；通知只是加速，正确性依赖查询／已提交 receipt。
- `RunCompleted` 表示该段步骤完成；`JobDrained` 必须包含本作业承诺的子任务、未确认交接和逐项处置。业务记账是显式可恢复步骤或幂等结果接收端，不放在 UI observer。
- V1 只做预先注册的有限顺序环节与有界重试／返工；每项下一步为一个明确目标，不实现任意 fork/join、图拓扑编辑或循环调度语言。

### 5.2 资源准入：成批成功不等于可以执行

新增 `ResourceAdmissionPort`：`reserve(reservationId, bundle)`、`query`、`release/transfer`，结果区分 `Granted(token)`、`Queued(ticket)`、`Rejected`、`Unknown`。

- V1 提供一个**限定资源域**的持久 `ResourceCoordinator`：同一 actor 管理静态已知的设备／槽位集合，整个 bundle 原子地全获批或全部不占用，避免逐资源 hold-and-wait。跨多个独立资源系统的原子协调不在 V1，不能假装等同 Saga。
- 稳定 reservationId、需求摘要、owner operationKey、grant 与释放／移交都提交后才回执；重复申请幂等。Queued 不允许 dispatch。
- 区分短期执行许可与持续占位：加工完成可释放执行许可，但到站的 FOUP 仍占目的端口；占位必须根据证据释放或转交下一步骤，不能一到 JobCompleted 就全部清空。
- 资源超时、lease 失效、宿主重启不能证明物理任务停止。Unknown 时保留或隔离该占用，核对实际设备／载具证据后再调度。
- 真实设备控制系统仍是物理事实来源。本地 coordinator 只有在独占控制域成立时能承担准入，V1 不声称防止未纳管外部操作者或脑裂写者。

### 5.3 最小 `BatchCoordinator` 与纯策略

先提炼 `BatchManifest`、`BatchPolicy`、`ItemDisposition` 等纯类型，再实现耐久 coordinator，不把旧 AreaScheduler 的可变队列搬进来。

- Manifest 固定 ItemId、请求／artifact 引用、摘要、兼容 key、目标资源／步骤、版本和逐项执行次数；拆批分配新稳定 childId 并记录父子关系，不沿用不同成员集合的同一 batchId。
- 内置 `single`、`countOrWait(maxItems, maxWait)` 及业务兼容键；deadline 从首项入批算起。资源容量／装载规则是硬约束，时间到不能绕过；尾批无法满足硬约束时明确等待条件／人工处置，不能无声丢弃或永久隐藏。
- Coordinator 持久化 Accepted、BatchSealed、DispatchIntent、RetryScheduled、DeliveryAccepted、ItemSettled、InputClosed；再由现有 Monarch 执行单批链。dispatch 重试挂接同 runId，不新建一个未知副作用的执行。
- 按 ItemId 校验输入和结果完整性：成功、明确失败、待查证及人工持有都要显式归属；缺项／重复／陌生或冲突响应不能通过 flatMap/zip 消失。
- 分类与路由决策带 policyVersion、证据引用和动作，一经提交恢复不重新用新策略改判。TIMEOUT 默认 Unknown；返工／换设备仅用于确证业务条件，保留逐项预算和原副作用身份。
- 只重试明确可安全重执行的 subset；成功项不重做，未知项进入单独查证工作。新增业务返工记录新执行号，不等同传输重试。
- 限制未结算 item/bytes、活跃 key／批、正在执行数及输出／派生任务数量。信用覆盖等待、在途、重试和待确认输出；不能通过队列转移提前释放。准入满时显式 Backpressured／可重试拒绝，不无界积累 Future。
- EOF/CloseInput 关闭外部输入，不取消内部重试与必要后继。只有 pending/open/running/unknown/retrying/unacked/必需child 均闭合才 Drained；否则返回 ClosingBlocked 的明确原因。
- 活跃内存数据结算后可压缩为 frontier/计数，已完成 actor 可停止；历史 journal 默认保留，磁盘增长与清理策略独立说明。

配置形态保持普通 Scala builder，不使用 Streams：

```scala
val ftpArea = AreaDefinition("bank-ftp")
  .batch(BatchPolicy.countOrWait(500, 2.seconds))
  .requires(ResourceBundle.single("ftp-channel", 1))
  .execute(rechargeRunDefinition)
  .classify(rechargePolicyV1)

val lithoArea = AreaDefinition("fab-litho")
  .batch(BatchPolicy.compatibleBy(recipeAndCarrier).upTo(25))
  .requires(lithoResources)
  .execute(lithoRunDefinition)
  .classify(qualityPolicyV1)
```

这是 G3 的薄配置入口，先固定协议再确定最终签名；其中 batch 大小只是示例配置，不代替真实设备工艺约束。一次性文件输入和持续到达的数据均通过有身份的 `offer/closeInput` 接入，同一耐久队列处理，不需要两个执行引擎。

## 6. 银行与 Fab 的共同验收样本

| 样本 | 必须保留的差异 | 共用协议的必验点 |
|---|---|---|
| 银行提交／查询任务 | 同一业务请求不能因超时重复扣款 | 接收回执丢失、Ongoing、外部成功但本方未记结果、Unknown 不误重试 |
| FTP 文件交付＋响应 | 文件存在只证明交付，不证明银行业务成功；manifest/hash 和结果关联独立 | 同名不同内容拒绝、部分上传、原交付身份查证、文件响应逐项守恒 |
| 充值成功子集 → 申购 | A 完成不等于 B 已接管／完成；账务和结果交付需幂等 | 成功 subset 恰好接管一次、丢 ACK 重发、未定项不进入申购 |
| Fab 加工 | 设备 taskId/incarnation、recipe、wafer IDs 必须匹配 | 两工单争容量 1；Queued 不下发；Unknown 不触发硬件失败/OCAP/换设备 |
| Fab 天车／搬运 → 量测区 | FOUP、源／目的端口、到站和占位状态都有业务含义 | 错 FOUP／目的端拒绝；天车与目的位原子准入；到站后占位不提前释放 |
| Fab 量测与返工 | 正常返工是新物理执行；重复回复不是新执行 | 按 waferId 处理缺项／冲突；仅明确质量不合格项返工；预算持久化 |

正向故障测试的外部服务／设备模拟器必须在独立进程中保留稳定 operationId 的接受历史和终态证据；宿主被杀后它仍可继续工作。另加反例：模拟器历史丢失／incarnation 变化后即使报告 Idle，也只能得到 Unknown／阻塞，不能假装自动恢复成功。

## 7. 实施顺序、文件边界与成本

S/M/L 表示相对增量，不是工期承诺。G1 后根据实际实现和故障测试结果校准后续成本；若协议不能收敛则停在该门修正，不继续堆 UI 或调度特性。

| 门 | 交付内容 | 通过标准／停止条件 | 量级 |
|---|---|---|---|
| G0 基线与合同 | 新短期分支、基线测试、身份／证据／版本合同；定义提炼来源与不迁入内容 | 明确外部何谓 Accepted/Completed/可安全重发；记录 artifact 与 dirty 边界；原有失败如实报告 | S |
| G1 可靠步骤＝R1 | Operation FSM、managed scope、Run attach/query/终态留存、wire 与 limits | 银行＋Fab 单操作覆盖所有故障窗口；Unknown 不推进；旧 waiter 清理；结果提交前不推进；不能做到则不进 G2 | L |
| G2 两资源区 | 显式交接步骤、耐久 inbox、子 run 启动意图与查询；限定资源域的原子准入／占用移交 | A/B 丢接收 ACK 仍同一任务；接收与完成区分；Fab 天车/目的位及 Unknown 占用不超配 | M–L |
| G3 批策略与逐项结果 | manifest、纯分类／路由、耐久 BatchCoordinator、subset retry/返工、关闭与排空 | 批成员冻结、逐项守恒、不同 attempt 合批不丢预算、无成功重跑、无容量死锁／提前 Drained | L |
| G4 消费与薄迁移＝R2 | 独立公共 JAR 示例；选定银行/Fab 消费者迁移；页面只观察事实 | 整 JVM 恢复正确，业务收尾不在 observer；正常／故障路径可重现；无两套通用恢复实现 | M，按工作树边界校准 |

### 关键文件

以下路径以工作区及对应子仓为范围；构建、测试和 Git 操作始终在对应子仓执行。

**保留／只增验收：**
- `monarch-core/src/main/scala/net/imadz/monarch/{Monarch,MonarchTypes,MonarchBuilder,AsyncCheckpointHooks,RunLease}.scala`：核心不改；补 managed adapter 不误推进的集成断言，必要文档单独新增而不覆盖 dirty README。

**G1 修改与新增：**
- `monarch-persistence/src/main/scala/net/imadz/monarch/persistence/PersistentMonarch.scala`
- 同目录新增 `ManagedRunScope.scala`、`RunProtocol.scala`／专用 serializer、`ProtocolLimits.scala`
- 新增 `operation/{OperationProtocol,OperationState,PersistentOperation,OperationClient,ExternalOperationPort}.scala`
- `src/main/protobuf/monarch.proto`、新增 operation/wire proto；同步 `MonarchProtoConverters.scala`、`MonarchSerializer.scala`、`src/main/resources/reference.conf`

**G2/G3 新增（不复制多个业务版本）：**
- `resource/{ResourceAdmissionPort,ResourceCoordinator}.scala`
- `workflow/{BatchManifest,BatchPolicy,ItemDisposition,RoutingDecision}.scala`（纯模型／策略）
- `batch/{BatchCoordinator,BatchProtocol,DeliveryProtocol}.scala` 及对应 proto/converters/tests
- `build.sbt`：维持原根坐标，新增 `publish / skip := true` 的 examples 子项目；不引入 Streams，不造成 aggregate/dependsOn 循环。

**验收与资料：**
- 复用 `src/test/.../{ControlledJournal,SerializedValidationJournal,DurableJvmRecoveryValidationSpec,RemoteCommandCompatibilitySpec}.scala` 的模式，扩展事件种类 gate，不重写 journal；测试夹具不进入产品 artifact。
- 新增 `OperationRecoverySpec`、`OperationJvmRecoverySpec`、`RunQuerySpec`、`DeliveryRecoverySpec`、`ResourceAdmissionSpec`、`BatchConservationSpec` 等聚焦 suite。
- `examples/`：银行与 Fab host、独立外部任务 fixture、小型耐久效果账本和运行入口；只调用公开库 API。
- 后续新增 `docs/RECOVERABLE_OPERATIONS.zh.md`、`docs/CAPABILITY_GAPS.zh.md`、`docs/ACCEPTANCE.zh.md`，承载实际 API 指南、能力状态与验收证据；本设计文档不冒充验收报告。API 样例由实际编译源码生成或引用，避免伪代码漂移。

### 消费者迁移边界

- 原 Java `salary-saving` 只读作语义来源，不重构旧工程。
- 默认首个银行迁移目标为当前已有现代 persistence 接线的 `bank-batch-transactor-demo`；原 `playAkkaCQRS/dag-engine-core` 保留为参考，不同时升级两套银行 demo。若存在未归属 dirty 文件，先完成隔离示例，冻结迁移改动清单并协调 owner 后再改。
- Fab 迁移通过 adapter／host 薄接线替代重复恢复逻辑；不搬空其 OCAP、工艺、Lot/Saga、projection。现有 dirty 改动逐文件核对，不能覆盖。
- 用户已授权提炼通用能力，仍不包含覆盖他人修改、改生成应用、重新发布冻结制品或自动 commit/merge/push。

## 8. 验证矩阵：每一步用持久证据验收

| 注入位置／输入 | 必须观察到的结果 |
|---|---|
| intent 尚未提交 | 无外部调用；没有虚假接收确认 |
| intent 已提交，发送与否不明 | 相同 key 查证／受约束幂等提交，不生成另一请求 |
| 外部已接受，acceptance 回复丢失 | 能按客户 operationId 查到原任务，不依赖尚未收到的 taskId |
| Ongoing 时宿主 JVM 被杀 | 外部任务继续；新 JVM 先查证，未确证不执行后继 |
| 外部成功，本方 terminal 未提交 | 读取既有成功证据再提交；实际业务效果不重复 |
| Operation terminal 已提交，stage checkpoint 丢 ACK | attach 返回同一结果；checkpoint 恢复不重做效果 |
| Unknown／历史丢失／错误 incarnation | 持久 Blocked，不默认失败重试；资源不自动释放 |
| 新 owner 建立，旧 Future/回复晚到 | 不污染新状态、不产生第二次交接，本地 waiter/timer 可回收 |
| 下游接收已提交但 ACK 丢失 | 同一 DeliveryId 重发，保持同一子任务；没有 orphan |
| 批响应缺项／重复／冲突 | 显式 unresolved／协议错误；无 silent drop、无错误完成计数 |
| 部分结果＋延迟返工＋EOF | 成功项不重跑、逐项预算保留；重试／子任务未闭合不报告 Drained |
| 多资源竞争／错误目的端／未知占用 | 不超配、不把 Queued 当 Granted、不把错到站当完成 |
| wire／尺寸／版本不兼容 | 显式拒绝或阻塞；Java serialization 关闭；旧缺字段不伪造结果 |
| 慢外部／慢接收方／满队列 | 内存与待请求有界，timer/确认仍可推进，无先占满后无法封尾批的死锁 |

执行层次：

1. 纯模型测试＋Akka Persistence testkit：确定状态转换、守恒、配置验证；可控时钟替代猜测性 sleep。
2. 可控真实序列化 journal：对 intent/dispatch/terminal/inbox/grant/release 分别控制“提交前”和“已 force 但 ACK 未到”。既有仅持有 StageDone/Resolved 的 gate 扩展为通用事件选择；不依赖内存 observer 计数证明正确。
3. 真实两 JVM／外部任务进程：进程身份、独占存储、稳定 key 和耐久业务账本为依据。先确认旧 host 已退出，再接管同目录；设备历史持久与丢失两种模式都测试。
4. examples 通过公开 Maven 坐标消费双 JAR：可从冻结基线开始验证旧契约，再切新版本；记录实际 CodeSource/classpath/SHA，不能仅凭 build 声明。候选只写新的隔离本地目录，空缓存可复现步骤单独验证。
5. 最后驱动真实 CLI／页面的正常和故障路径；页面 Actor crash 是较弱演示，不代替 JVM 故障门。尚未运行的项目明确标待验收，不写 PASS。

工具链保持 Scala 2.13.14、JDK 11、sbt 1.4.9、Akka 2.6.20。单元优先 testkit；故障测试使用独占文件 journal／必要的独占 LevelDB。snapshot 内存 fixture 不能冒充磁盘 snapshot。真实生产 journal/断电认证另做，未经协调不启动或重启共享服务。

## 9. 单一主线、版本与交付约束

- 设计核对时，core main/hardening 同为 `376b152`，无须再合并；persistence `main=431c914` 落后 hardening 两个提交（`9facddd`、`e6a6031`），已有加固值得独立验收后收回 main。
- 本轮短期分支为 `feature/monarch-recoverable-operations`，基于包含上述加固的 `e6a6031`，与 main 保持同一演进谱系；旧加固与新功能分别评审。实际 merge/commit/push 仍需明确授权，不自动执行。
- 新 persistence 候选采用独立不可变版本与目录（例如 `0.3.0-dev.<revision>`）；core 未变则固定已验证版本。保护普通 0.2.1、P23、历史候选及回执，不就地重发。
- 旧 0.2 无消费者的 MiMa 要求不扩大成本；当前序列化数据、公共命令与所选消费者的正确性必须验证。兼容性变化如实说明，不用静默数据丢失换“兼容”。
- R1 通过即可独立交付可靠操作能力；R2 在限定银行/Fab 流程通过后结束。真实连接器的安全证据不足时，交付“可靠阻塞”的边界，不宣称自动恢复；不因此无止境扩张成完整 MES、数据流引擎或任意工作流平台。

**总体取舍：低成本来自复用现有 Monarch 和从业务中提炼同一套协议，不是省略暂态、查证、交接或故障验证。**
