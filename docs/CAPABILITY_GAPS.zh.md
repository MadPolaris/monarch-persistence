# Monarch 能力状态：R1 → R2

设计见 [RECOVERABLE_OPERATIONS_DESIGN.zh.md](RECOVERABLE_OPERATIONS_DESIGN.zh.md)，用法见 [DURABLE_WORKFLOWS.zh.md](DURABLE_WORKFLOWS.zh.md)，实际测试/制品证据以 [ACCEPTANCE.zh.md](ACCEPTANCE.zh.md) 为准。实现完成与消费者验收分开记录。

| 能力 | 实现位置 | 状态与边界 |
|---|---|---|
| 顺序 stage、连续游标、checkpoint ACK | 既有 core + PersistentMonarch | 复用，core 源码未修改 |
| 外部意图、进行中、查证、确证终态 | operation/PersistentOperation | R1 已提交/验收；不把未知改为成功 |
| Unknown 持久阻塞、有限查询/人工预算 | OperationState/OperationSettings | 已实现；普通timeout/NotFound不授权重发 |
| 具名计划、幂等挂接、终态查询 | RunProtocol/PersistentMonarch | R1 已验收；不隐式认领无版本旧run |
| 单stage操作引用/等待与owner清理 | ManagedRunScope | 单stage单操作，共享有限waiter |
| 旧数据读取、显式wire与限额 | Run/Operation protobuf/serializers | 旧E1/S1/R1可读；缺失结果不重做业务补数据 |
| 限定资源域联合准入 | resource/ResourceCoordinator | R2已实现；全bundle获批，oldest-fitting，非跨域事务 |
| 执行许可与持续占位 | Resource Release/Transfer、BatchCompletion.retainCarried | R2已实现；Unknown不释放，移交必须有新claim回执 |
| 耐久接管、固定子Run、查询终态 | delivery/PersistentInbox、RunSubmission | R2已实现；Accepted与完成分开，非observer唯一交付 |
| 成批、冻结manifest、逐项守恒 | workflow/BatchModel、batch/BatchCoordinator | R2已实现；single/countOrWait/硬最低批量 |
| 有限subset retry、返工/路由、EOF排空 | BatchProgress/BatchState | R2已实现；逐项预算/历史，不重跑成功项 |
| 有界人工查证完整接线 | WorkflowHost.reconcile | 原Operation/Inbox/Batch逐层查证；显式调用、有限预算 |
| 公开候选JAR消费样本 | examples | 精确fixture入口，仅解析artifact，不dependsOn产品源码 |
| 银行充值→逐笔申购完整样本 | CompleteWorkflowProgram + 独立外部账本 | 正常/JVM故障矩阵已运行；余额/持仓是独立oracle |
| Fab搬运/加工/量测/返工/卸载 | 同上，固定资源域 | 正常/JVM故障矩阵已运行；限定一Item一FOUP一wafer |
| 现有Bank/Fab薄接入与权威页面 | 各消费者durable profile/controller | 已编译、策略7/7＋10/10、真实HTTP/整Play JVM恢复通过；同一公共JAR/SHA |

## 不把限定实现扩大为承诺

- R2 是有限耐久工作流，不是任意DAG、无限流窗口计算或第二套Graph/Streams引擎。
- 本地WorkflowHost数量有界，按jobId显式激活；不提供全局sharding/自动故障转移/任意停止actor的无限重建。跨JVM必须确认旧writer退出并独占存储。
- ResourceCoordinator不替代真实设备事实，不防止未纳管操作者；释放/移交证据由领域connector核验。
- 外部模拟器不是实际银行、FTP、TA或设备协议实现；文件部分上传、真实结算/物理安全证书另需正式connector与独立验收。
- 固定wire限额、JVM强杀和test journal force并非生产SLA、物理断电、存储损坏或脑裂认证。
- 有限job保留去重摘要与结果，历史journal默认不删除；不宣传长期存储零增长。
- 候选使用新版本/新目录，不覆盖普通0.2.1、P23、R1或任何历史制品。
