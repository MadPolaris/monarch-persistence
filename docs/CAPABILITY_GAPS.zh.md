# Monarch 能力状态：R1 实施批次

2026-11-06；此表只区分已有能力、本轮实现和后续范围。设计见 [RECOVERABLE_OPERATIONS_DESIGN.zh.md](RECOVERABLE_OPERATIONS_DESIGN.zh.md)，逐项测试/制品证据以 [ACCEPTANCE.zh.md](ACCEPTANCE.zh.md) 为准。

| 能力 | 实现位置 | 本轮状态 |
|---|---|---|
| 顺序 stage、连续游标、checkpoint ACK | 既有 core + `PersistentMonarch` | 复用，core 源码未修改 |
| 外部操作意图、进行中、查证、确证终态 | `operation/PersistentOperation.scala` | 已实现；单元及银行/Fab进程故障测试已运行 |
| Unknown 持久阻塞、有限查询/人工查证预算 | `operation/OperationState.scala`、`OperationSettings.scala` | 已实现；不默认重发/失败 |
| 具名版本、Run 幂等挂接、终态查询 | `RunProtocol.scala`、`PersistentMonarch.scala` | 已实现；新接口不隐式认领无版本旧run |
| 单stage操作引用/等待与owner清理 | `ManagedRunScope.scala` | 已实现；单stage单操作，共享有限waiter配置 |
| wire、旧数据读取、尺寸约束 | Run/Operation protobuf、serializers、`ProtocolLimits.scala` | 已实现；新格式 E2/S2/RW1 等显式版本 |
| 公开候选JAR消费示例 | `examples` | 已配置；只解析artifact，不dependsOn产品源码 |
| 全局实体调度/资源域准入 | G2 `ResourceCoordinator` | **未实现**；宿主限制活跃actor/外部调用数量 |
| 跨区耐久交接、接收inbox、子任务结算 | G2 | **未实现**；observer仍不能担任唯一业务交付 |
| 成批/冻结manifest/逐项守恒/重批 | G3 | **未实现** |
| 银行充值→申购端到端、Fab搬运/加工/量测/返工 | G4消费者迁移 | **未实施**；本轮只有单操作样本，不冒称全链路完成 |

## 已实现保证的边界

- R1 是安全恢复**单次外部操作**，不是原生流计算、批调度或任意工作流引擎。
- `ledgerEpoch`、请求digest、taskId 等关联检查不能代替连接器对真实业务证据的核验。没有安全查证条件时提供可靠阻塞，不提供无条件自动继续。
- ManagedOperations 限制本实例的本地waiter和有限ask。它不是跨进程全局资源准入；宿主必须控制可创建的operation/run实体数及其ExecutionContext/外部并发。
- 固定序列化上限和不可变版本是协议边界，不是生产 SLA。零Java序列化、实际公共JAR消费、JVM强杀恢复是测试层次，不等于物理断电/脑裂/真实设备认证。
- 保留并读取旧 E1/S1/R1；无法还原旧终态结果时明确缺失。不替换或回写冻结0.2.1/P23制品与历史journal。
