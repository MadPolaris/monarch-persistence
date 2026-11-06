# 可恢复外部操作：R1 使用说明

设计背景见 [RECOVERABLE_OPERATIONS_DESIGN.zh.md](RECOVERABLE_OPERATIONS_DESIGN.zh.md)，实际验证结果见 [ACCEPTANCE.zh.md](ACCEPTANCE.zh.md)。本功能保留 `monarch-core` 的顺序 Future 阶段，不引入 Streams 或第二套阶段引擎。

## 1. 两个持久身份，而不是一个长时间 ask

- **Run** 保存冻结计划、已提交阶段前缀和当前 `AwaitingRef`。
- **Operation** 保存冻结请求、发送意图、外部证据、查证预算和确证结果。
- `ManagedStageScope` 是可丢弃的本地等待对象，不是持久事实。恢复后重新挂接同一 Operation。
- `RunId`、`OperationKey` 和 `EffectKey` 都由宿主从稳定业务身份生成，不使用随机 actor generation。`ledgerEpoch` 是外部**耐久账本**的 incarnation，不是进程 PID。

R1 每个 stage 最多一个外部操作。相同请求重复 await 返回同一次等待；同 stage 的另一请求会阻塞 Run。若业务需要多个外部动作，将其明确拆成不同 stepId 的阶段，不在一个 stage 内循环隐藏多个副作用。

## 2. 接入点

可编译的银行/Fab 接线位于：

- `src/test/scala/net/imadz/monarch/persistence/ManagedOperationJvmHost.scala`
- `src/test/scala/net/imadz/monarch/persistence/ManagedRunScopeSpec.scala`

`examples` 子项目也编译同一 host 文件，但仅消费公共候选 JAR，不访问产品源码或私有类。构建/运行方式见 [examples/README.md](../examples/README.md)。

依次实现或配置：

1. **`ExternalOperationPort`**：`submit(request)`、`query(request, lastEvidence)`、`validateEvidence`。请求和结果使用不可变 `ByteString`；可用 `ExternalOperationPort.Typed[A]` 在适配器边界解码领域 payload。
2. **Operation 实体定位**：同一 key 总是定位同一个 `PersistentOperation` / persistenceId。宿主保证有效单 writer，恢复时不能换一套空 journal。
3. **`RunDefinition`**：具名 definition/version/codecVersion 和有限、顺序的 stage 列表；每个 stage 的显式 stepId 唯一。
4. **`ManagedOperations`**：共享的有界本地等待/查询入口，resolver 返回上述稳定 Operation ActorRef。
5. **`PersistentMonarch.managedBehavior`**：解释器在外部操作阶段调用 `scope.operations.awaitTerminal(request)`，只把已确证结果映射成下一状态。
6. **`RunProtocol.StartOrAttach`**：以同 runId、版本、初始 bytes 开始/挂接；重复挂接只查询，不启动第二个 engine。`GetRun` 获取已提交状态。

纯计算/可幂等阶段可使用 `versionedBehavior`，旧 `behavior` / `StartRun` 保留，但旧接口不自动获得可恢复外部操作保证。versioned/managed host 要求使用具体的 `StartOrAttach`，不会接受未提供版本身份的旧 StartRun。

## 3. 状态与外部证据

Operation 的公开 phase 区分：`Empty / Prepared / Dispatching / Ongoing / Reconciling / Blocked / Succeeded / Failed / RecoveryBlocked`。

- `Prepared` 提交后才确认接管请求，`DispatchStarted` 提交后才调用外部。
- `Dispatching` 恢复时先查原请求。接受回复丢失时，不要求已经持有外部 taskId；对端必须能按客户 operationKey 查证，否则只能安全阻塞。
- `NotFound`、设备 `Idle`、网络超时都不是未执行的证明，也不是 `KnownFailed`。
- `validateEvidence` 缺省拒绝。除了框架对 key/digest/ledgerEpoch/taskId 的关联检查，连接器必须验证状态、回执、结果与业务请求的真实关联。
- `SafeToSubmit` 还必须通过独立的 `validateSafeToSubmit`，并受 `maxSubmissions` 限制；默认不允许技术重发。
- 查证预算耗尽进入持久 `Blocked`。`Reconcile` 只给同一操作一份有上限的查证预算，不强制成功、不换 key、不自动忽略未知结果。
- `KnownFailed` 是有证据的业务结果。宿主可显式分类或终止 Run，不能把传输失败伪装成它。

连接器 Future 必须及时返回，阻塞 I/O 使用自己的有界 ExecutionContext；watchdog、查证计时和完成通知使用 Actor 的控制 ExecutionContext，不依赖已被连接器占满的工作池。I/O 超时后，尚未真正结束的调用仍占用该 Operation 的 live-call 槽；不会无限堆积新的不可取消 Future。控制进程/Actor 重建也不能取消已经发出的外部调用，因此恢复仍需查询/幂等证据。

## 4. 提交顺序与生命周期

```text
RunAwaiting 提交
  → Operation Prepared 提交
  → DispatchStarted 提交
  → 外部 submit/query
  → TerminalRecorded 提交
  → 本地 awaitTerminal 完成
  → Run StageDone checkpoint 提交
  → 下一阶段
```

- parent AwaitingRef 只保存 cursor/key/digest，不复制 Operation 全状态。
- interpreter 提前返回、吞掉仍未确证的操作、或恢复时换请求都会阻塞，不提交成功/Resolved checkpoint。
- Run owner 撤销会回收本地 timer/waiter；进行中的有限 ask 结算后释放许可。旧 callback 不推进新 Run。
- 停止 Run 不等于停止外部设备/扣款。Operation 状态和外部事实不会被改成失败。
- `GetRun` 表达已提交 frontier、checkpoint bytes、AwaitingRef 和恢复阻塞；查看外部业务状态还需 `OperationProtocol.Get`。
- Actor 正在等待 journal 时，查询本身也可能超时。超时是不可用，不是业务失败。历史状态超过新 wire 限额时，`GetRun` 返回可序列化的 `Unavailable`，不静默丢弃状态。宿主负责受控重建同一实体，不无上限重启损坏 journal。

## 5. wire 与版本

- `RunProtocol.Wire` 使用具体 bytes DTO、`RW1` / serializer `77102123`。
- Operation 使用独立 protobuf 协议及 serializer `77102130/77102131`。
- 不绑定整个 `PersistentMonarch.Command` 或 `OperationProtocol.Command`；内部 Promise/callback 不作为远程消息。
- 旧 `E1/S1/R1` 仍可读，新的 Run 身份/等待事件使用 `E2`，扩展快照使用 `S2`，不让旧 reader 静默丢掉新语义。
- 旧终态快照若没有结果 bytes，返回缺失，不能重新执行副作用补结果。
- definition、codec、连接器版本或冻结计划不兼容时，保持可诊断的 `RecoveryBlocked`。
- ProtocolLimits 限定 request/state/result/evidence/envelope、字符串、计划和 metadata；R1 可下调，不可超过 wire 上限。大文件另存可靠不可变 artifact，并在业务 bytes 中携带摘要/长度/引用；库不保证一个本地路径在重启后仍存在。

## 6. 本轮不包含的能力

尚未实现：资源 reservation/物理占用转交、durable inbox/跨区交付、BatchCoordinator、逐项 subset retry、业务排空、银行/Fab 页面迁移。它们是 G2–G4，不可因为 R1 可以恢复单次操作便宣称完整银行批业务或 Fab 跨区流程已打通。

运行前提：可靠 journal、同实体单有效 owner、外部幂等或权威查证契约。测试的 JVM kill/`force(true)` 不代表物理断电、生产 journal、脑裂或真实设备认证。observer 仅用于展示/诊断，不承载唯一业务副作用。
