# R2：有限耐久批作业、资源与可靠交接

R2 在 R1 的外部操作和原 `PersistentMonarch` 上增加资源准入、耐久 inbox、冻结批次和逐项结算。没有引入 GraphDSL/Streams 调度层，也没有第二个阶段执行器。R1 的细节见 [RECOVERABLE_OPERATIONS.zh.md](RECOVERABLE_OPERATIONS.zh.md)，测试状态见 [ACCEPTANCE.zh.md](ACCEPTANCE.zh.md)。

## 1. 一条工作流，五类事实

| 权威实体 | 保存什么 | 不代表什么 |
|---|---|---|
| BatchCoordinator | 接受的输入、封定 manifest、交接/分类意图、逐项 execution/lineage、EOF 与结算 | 本地队列空不等于业务排空 |
| ResourceCoordinator | 一个限定资源域的 bundle、grant token、释放和占位移交 | Queued 不是许可，timeout 不是物理资源空闲 |
| PersistentInbox | DeliveryId、不可变子 Run 请求、接管和子任务终态 | Accepted 不是业务完成 |
| PersistentMonarch | 原顺序阶段及其 checkpoint ACK | Run 终态不是全部父作业已结算 |
| PersistentOperation | 外部意图、进行中/未知、查证与确证结果 | NotFound/Idle 不是可安全重发证据 |

单批的耐久顺序：

```text
InputAccepted → Sealed manifest → resource Granted
  → 冻结 RunSubmission / Delivery → inbox Accepted → ChildStartIntent
  → 原 managedBehavior → 外部操作确证提交 → 原 checkpoint / Run 终态
  → inbox ChildTerminalRecorded → 提交分类决策
  → 释放旧占位 / 释放本步执行许可或 Transfer 新占位
  → Settled：终态项或有固定 dueAt 的后继项
```

所有发生业务效果的步骤均在已有 Monarch Run/Operation 中；`prepare`、`classify`、`complete` 只作纯映射。observer 只能用于诊断/显示。接管、执行成功、父作业排空是不同提交事实。

## 2. 实际 API 与薄接入

公开入口位于 `batch/WorkflowHost.scala`：

- 实现 `WorkflowProgram` 的 `definition`、`capacities`、`resourceEvidence`、`connector(area)`、`operation(manifest)`、`complete(manifest, terminal)`。
- `new WorkflowHost(system, namespace, program)` 创建同一命名空间的有限本地宿主。
- `host.job(jobId)` 返回 `ActorRef[BatchProtocol.Command]`。
- 逐条 `Offer(sequence, itemId, payload, area, compatibilityKey, replyTo)`；只有提交后才回复 `Accepted`。
- `CloseInput` 只关闭外部输入；`Get` 查询已提交状态，使用 `afterSequence` / `limit` 分页。
- `host.reconcile(jobId, batchId)` 是一次显式、有限的人工查证动作，不应放在页面自动轮询中。
- `host.resourceDomain` 提供资源查询。生产连接器不能把示例回执校验当成物理/资金安全认证。

可编译接线而非拟议 DSL：

- `src/test/scala/net/imadz/monarch/persistence/CompleteWorkflowProgram.scala`
- `src/test/scala/net/imadz/monarch/persistence/CompleteWorkflowJvmHost.scala`
- `examples` 通过公开 Maven JAR 编译同一两个入口，不将产品源码/测试断言作为消费者源码。

现有 Bank/Fab 仓库通过新 profile 与 controller 接入；旧 pipeline、projection 和业务代码不被搬空或覆盖。新增 build 开关须显式指定 `monarch.r2.version`，不指定时不启用 R2 源文件或路由。

## 3. 成批、背压与逐项守恒

实际纯配置类型：`AreaDefinition(id, policyVersion, batch, resources, parallelism, maxExecutions)`。

- `BatchPolicy.single`
- `BatchPolicy.countOrWait(maxItems, maxWaitMillis)`
- `BatchPolicy.compatible(maxItems, minItems, maxWaitMillis)`：硬最低装载量，EOF/时间/压力均不能绕过。

兼容键和 area 在接收/路由时冻结；等待窗口从该次入队最早 `acceptedAt` 计算。manifest 固定 job/batch/item 身份、顺序、payload、policyVersion、execution、lineage 和 carried claims。之后重启、重发或配置改变不能重新分组这个已封批的 manifest。

结果必须恰好覆盖 manifest 全部 ItemId，不允许重复、缺失或陌生项。处置为：

- `Completed(result)` / `Failed(reason)`：确证结算；由宿主决定何种外部证据足够。
- `Retry(delayMillis)`：仅适用于明确允许再次执行的项；新 execution，仍保留 ItemId 和历史。
- `Route(area, compatibilityKey, delayMillis)`：一个明确后继，不能任意 fork/join。
- `Hold(reason)`：未结算，继续占信用；只有宿主注册的 query-only verification 路由才可 `VerifyItem`，不能改成任意业务重试。

成功项不随失败子集再执行。返工和技术重传不同：技术查证/幂等重传保持同一 OperationKey；真正重试/返工才增加相应 area 的 execution。回到同一 area 也计本次已完成执行；lineage 为最后一次结算留出空间。

`InputClosed` 后内部 retry/route/child 仍继续。只有没有活跃项和批次才显示 `Drained`；`succeeded` / `failed` 分别报告，所以 Drained 不等于全部成功。输入 EOF 在崩溃前未提交，恢复不能凭空关输入：输入适配器须重放同一不可变源及其 EOF。

## 4. 资源许可与物理占位

一个 ResourceCoordinator 管一个静态、独占控制域；bundle 全获批或全不占用。排队采用 **最早可完整满足的 bundle**，而非会让入站请求挡住占位者续行的严格 FIFO。它不承诺无条件公平或跨独立资源系统的原子事务。

- `Reserve` 使用稳定 reservationId、ownerOperationKey 和不可变需求；同 ID 不同请求拒绝。
- `Release` 核对 token、owner 和宿主证据；回执提交后返回。
- `Transfer` 关闭旧 grant，创建稳定的新占位 claim，保留其中一部分单位并释放其余执行资源。必须取得对应新 `Granted`，不能把 `Released(oldId)` 当作移交成功。
- 重复 release/transfer 使用同一指纹和原回执；已经移交的旧 claim 不能再释放以撤销新占位。
- 超时、重启或 Unknown 不自动释放任何占位；证据校验失败保留原资源。

`BatchCompletion(..., retainCarried=true)` 表示只释放本步执行许可，继续携带输入占位。不能同时再 Carry 一份新 claim。共享一份物理占位的 items 必须路由到相同 area/key/delay，所有 holders 到期且可装入目标批后一起封批，不拆开载具。

有 carried Pending 后继（包括尚未到期的后继）时，调度不会用新的普通入站批占满批槽。已有批采用轮转推进；字节余量不足接纳下一条最大合法项也会触发柔性尾批，以免满队列无法自我排空。

## 5. 查证与生命周期

Operation、Inbox、Batch 各有持久自动/人工预算。不同宿主默认值不能重置已记录预算。

`WorkflowHost` 从配置读取 `monarch.operation`、`monarch.delivery`、`monarch.resource`，不是硬编码忽略配置。人操作页面的示例 profile 给出有限观察窗口，超出后由人工显式请求查证原身份：

1. 查已提交的 job，确认 batch 属于该作业。
2. 按原 key 激活/查询 Operation；只有确实 Blocked 且无未完成原调用时才授予有限查询。
3. 重新挂接不可变 Delivery；不能假设丢 ACK 证明对端已接纳。
4. 必要时恢复 Inbox 的有限查询，再恢复批次。

Inbox 的 `View.reconciling` 表示已提交的查询授权，业务仍是 Blocked；父级可继续等其证据，不能把这份授权误当拒绝，也不能把它显示成业务完成。

本地宿主是**有限、显式激活**的 bootstrap，不是自动 sharding/集群故障转移系统。进程重启后，调用原 jobId 才按稳定 persistenceId 恢复；没有后台枚举全部作业的扫描器。命名空间只能有一个有效宿主。缓存数量是生命周期身份上限，不是无限流淘汰缓存；已停 actor 的任意自动重建/无限重启损坏 journal 不在此入口的保证中，基础设施停止须修复原因后受控重启宿主。

## 6. 有界性与格式

- 单有限 job 默认最多1000个已接受项，最多100个活跃项；连续 sequence 从0开始。满 lifetime 限额须结束该 job，再用新 job 接下一组输入。
- 完成项压缩 payload，保留身份/摘要/结果/历史用于该有限 job 的精确去重；不会偷偷删除 journal。
- Item/result ≤4096B；单 record ≤24KiB；32 areas/transitions/并发批为格式上限。
- manifest/completion ≤1MiB；公开 wire ≤4MiB；有限历史 event/state ≤32MiB。查询每页1..100条。
- Operation request/evidence 另有默认64KiB上限，资源/Inbox也各有聚合上限。合法批数量不保证任意巨大业务 manifest 能直接作为 Operation payload；超限使用不可变 artifact 引用＋digest，或者显式收紧批量/单项配置。不能提高 actor 配置绕过固定 wire ceiling。
- Resource 域默认最多10000条 reservation/tombstone、1000条排队，聚合 bytes 可更早耗尽；不淘汰旧 ID。满域不能凭空创建 transfer 目标，应提前规划容量；既有 release 留有空间。
- WorkflowHost 默认最多32个 job、每类10000个 child identity；消费者更小。ManagedOperations 本地等待也受独立有限许可约束。
- 邮箱入口和任意宿主分配的内存并非全局自动背压；调用方必须按 Offer 接纳结果节流，连接器使用有界 I/O 池。样例 HTTP 每次至多20条/10条串行输入，有限 ask，不自动重试业务。

新 protobuf 与 serializer：Resource `77102140/77102141`（RE1/RS1/RW1）、Delivery `77102150/77102151`（DE1/DS1/DW1）、Batch `77102160/77102161`（BE1/BS1/BW1）。仅绑定具体 Wire，不绑定内部 callback Command。R1/legacy 格式的兼容边界保持不变。

## 7. 两个端到端例子

**银行**：三个条目按数量/时间成充值批；独立外部账本给出逐项 SUCCESS/RETRY，只失败项再充值；成功项逐笔进入基金申购。每项最后 balance=0、holding=100；所有要求的申购都结束后才排空。不是仅 task 数量变动的假账务测试。

**Fab**：本有限样本每 Item 恰为一个 FOUP＋一片 wafer。入站搬运占 AMHS＋光刻端口；光刻只释放设备；搬出后转移至量测端口；明确 REWORK 才返工并再量测；只有 unload 成功才最终结算。业务次序由独立外部账本验证，端口容量由资源域管理。

外部模拟器单独进程，持久 WAL；宿主被杀时任务历史仍在。`COMPLETE` 才触发模拟业务效果，QUERY/页面轮询不制造完成；BUSINESS/ITEMS 提供独立余额、持仓、工艺/卸载核对。它不是真实银行/TA/FTP服务器/设备控制器；不声称已经完成真实文件部分上传、生产连接器或设备安全认证。

## 8. 停止范围

限定 R2 完整例子，不扩张为任意图/循环语言、多资源域事务、无限流历史去重、自动扩缩容或生产 MES。可靠 journal、独占有效 writer、可验证外部身份是前提。JVM kill、测试 journal `force(true)`、独占 LevelDB 和公共 JAR 验证均不等于物理断电、存储损坏、脑裂或生产 SLA 认证。
