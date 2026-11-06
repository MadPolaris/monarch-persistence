# Monarch 可恢复操作与耐久工作流：验收记录

## R1 历史范围与状态

2026-11-06 开始实施；设计见 [RECOVERABLE_OPERATIONS_DESIGN.zh.md](RECOVERABLE_OPERATIONS_DESIGN.zh.md)。以下 G0/G1 是已经提交的 R1 历史证据；用户随后授权继续 G2–G4。R2 的新实现与验证不回写为 R1 成果，见本文件后续 R2 节及 [DURABLE_WORKFLOWS.zh.md](DURABLE_WORKFLOWS.zh.md)。

- 设计提交：`ed0b01c`；分支 `feature/monarch-recoverable-operations`。
- 运行时起点：persistence `e6a6031`，core `376b152`。
- 既有修改保留：persistence 的 README.md、README-zh.md、VALIDATION.md；core 的 README.md、README-zh.md、TUTORIAL-zh.md、VALIDATION.md。不计入本次变更。
- 不修改银行/Fab 消费者、生成应用或冻结制品；不推送或外部发布。

## G0 基线：已执行

工具链：AdoptOpenJDK 11.0.11，Scala 2.13.14，sbt 1.4.9，Akka 2.6.20。

各命令在对应子仓执行，JAVA_HOME 指向 JDK 11：

```bash
# monarch-core
sbt -batch test

# monarch-persistence；REPOSITORY 为现有隔离 Maven 目录的 file: URI
sbt -batch -Dmonarch.core.repository="$REPOSITORY" \
  -Dmonarch.validation.java="$JAVA_HOME/bin/java" \
  test mimaReportBinaryIssues 'show Compile / dependencyClasspath'
```

| 检查 | 实际结果 |
|---|---|
| core 回归 | 8 suites，34 tests，通过 34，失败 0 |
| persistence 回归 | 11 suites，33 tests，通过 33，失败 0，另 canceled 1 |
| 跳过项 | `PersistenceCapacitySpec` 的 opt-in capacity run，未启用容量参数 |
| MiMa 对 0.2 | 命令成功，未报告 binary issues |
| core 实际依赖 | `target/demo-candidate-20261021/net/imadz/monarch-core_2.13/0.2.1/monarch-core_2.13-0.2.1.jar` |
| core JAR SHA-256 | `66223d37ab979b59cde557500f3da7aafaf532ddfe3268401c5f786093e0f971` |

现有测试的故障注入会输出 ERROR/WARN（codec、observer、journal 等），不能据日志级别当成新增测试失败。编译有既有未使用 import/target deprecation 和依赖 eviction 警告；基线未顺带清理。

## G1 协议合同

- 同一 OperationKey 固定请求、EffectKey、连接器版本、步骤和外部账本 incarnation；技术重试不换身份。对端持久账本的 incarnation 不等于其进程 PID。
- 发送前提交意图；`Dispatching` 只表示可能发送。恢复查证原身份，不把 NotFound/Idle/timeout 当作未执行。
- 安全重发要求显式连接器证据验证；普通 Unknown 只继续查证或阻塞。
- 已确证结果持久化后才能完成本地等待，之后仍须等待 Run checkpoint 提交才能继续。
- 连接器 Future 超时不能触发无界新调用。旧 owner 撤销不等于外部效果取消。
- 单一有效 owner 和独占 journal 是本轮前提；不认证脑裂、真实断电或金融/设备生产安全。

## G1 已执行证据

### 测试

| 验收 | 实际结果 |
|---|---|
| 首轮 RunQuery/ManagedRunScope/OperationRecovery/OperationProto | 4 suites，27/27 通过 |
| 银行与 Fab 独立外部进程／宿主 JVM 恢复 | 14/14 通过；每领域各7场景 |
| 第一次全量回归（显式启用容量测试） | 17 suites，76/76 通过，0 canceled；MiMa 通过 |
| 后补 ManagedRunScope 等待容量／引用提交超时 | ManagedRunScopeSpec 7/7 通过，其中2项为首次全量之后新增 |
| 第二次全量回归 | 17 suites，78/78 通过，0 canceled；容量改用128阶段快速回归，MiMa通过（10000阶段证据仍以上次为准） |
| 审阅修正后最终全量回归 | 17 suites，83/83 通过，0 canceled；128阶段容量快速回归与MiMa通过 |
| 公共候选 JAR examples 编译 | 3个明确入口/夹具源码成功，仅以公共artifact依赖产品库 |
| artifact-only 多进程复跑 | 15/15 通过：14个业务恢复＋1个外部账本自身恢复 |

14个业务恢复场景：银行/Fab各自接受回执丢失、Ongoing宿主重启、外部完成后operation terminal未提交／已force但ACK丢失、terminal提交后stage checkpoint未提交／已force但ACK丢失、外部历史丢失后的持久Blocked。每个正向场景的外部耐久效果数为1，宿主重启前后只提交过1次；历史丢失场景不重发、不走下一步，第二次恢复仍阻塞。

外部账本独立测试会重启服务进程两次，验证接受/完成重放不产生新效果、FORGET只改变历史epoch且保留业务效果账本。计数中 effect/task 是耐久事实，submit/query次数只是服务进程内诊断，不冒充跨外部进程计数保证。

容量样本：10000阶段×1024B state，maxPending=1；本机观测 elapsed=300479.80ms、checkpoint/s=33.28、journal P95=31.792ms、recovery=87.65ms。使用测试内存存储＋真实序列化＋延迟journal，**不是生产TPS或SLA**。

### 首个候选与公开消费（修正前历史证据）

- 候选版本：`0.3.0-dev.r1-20261106-8e344a28f3a9`。
- 候选目录：`target/r1-candidate-20261106-8e344a28f3a9`；首次创建，未重用/覆盖冻结目录。
- 产品源码＋build/project 文件的长度分帧 SHA-256：`8e344a28f3a965a97ec185f4901a63e3b702e32aeb9368b2d844d187ff5d1c30`。
- persistence JAR SHA-256：`f9a4c4aa80b990e276541116a50c5e90ce3c6edf47865451cdcad635b4ebd99c`。
- core 固定沿用上文0.2.1 JAR/SHA，没有新发布core。
- examples runtime classpath共有60项，没有产品target/classes或test-classes；产品JAR内没有Fixture/ValidationJournal/Spec测试类。
- 30份宿主日志的 `HOST_ORIGIN` 均记录同一候选persistence JAR和固定core JAR及SHA，没有落到classes-directory。

### 最终候选与公开消费（审阅修正后）

- 版本：`0.3.0-dev.r1-20261106-648f4a09bda4`。
- 独占目录：`target/r1-candidate-20261106-648f4a09bda4`；先前候选未覆盖。
- 产品源码＋build/project SHA-256：`648f4a09bda47932ad85698841de992de68f90537798eee0b98fc56b365a6abd`；构建前后重新计算一致。
- persistence JAR SHA-256：`570553d44ce9da7d75fb87ccda8cd94e7b7d521e89c2d23a9909e94c21af3dc4`。
- examples 已重新编译，artifact-only **15/15通过**；30份宿主日志实际加载上述persistence JAR及固定core JAR，SHA一致，无产品classes目录。
- 最终全量：**83/83，0失败、0跳过，MiMa通过**。测试进程已退出，未遗留外部模拟服务。
- 独占空消费者依赖缓存／全新target的examples编译也通过，细节与裸机边界见末节。
- 证据目录：`target/r1-reviewed-regression-20261106/`、`target/r1-final-artifact-evidence-20261106/`。

### 本地过程证据

- 第一轮进程矩阵：`target/r1-evidence-20261106/`。
- 首次全量回归的进程证据：`target/r1-regression-20261106/`。
- 公共JAR消费者证据：`target/r1-artifact-evidence-20261106/`。
- 每场景目录有外部WAL、宿主journal、first/recover日志和processes.log（PID、强杀与waitFor退出结果）。这些是本地测试产物，不发布或混入源码提交。

### 已处理的实现／验收问题

- 初次编译发现测试空Vector类型推断和Runnable.run遮蔽宿主ref两个错误，已修正后重编通过。
- 首次全量中强杀进程可能关闭stdout读取管道，读日志线程抛Stream closed；已将仅“进程已退出时的readLine IOException”视为EOF，写盘错误仍不吞掉。随后artifact复跑通过。
- 工具曾瞬时报ENOSPC；核对后磁盘仍约21GiB可用，两个证据目录合计约1MiB，未删除任何文件。完整全量结果随后可读取，退出码0。

### 独立审阅与已修正事项

两次聚焦只读审阅分别检查 Run 接线与 Operation 协议：

1. Run 拒绝理由在添加诊断前缀后可能超过wire字符串限制。已在查询回复构造处统一按UTF-8上限截断，补充codec超长错误和connector满长拒绝测试；旧状态超过新限额返回`Unavailable`而非无法发送的回复。
2. Operation watchdog原来使用传入的I/O执行线程池，单个阻塞调用能妨碍超时消息送达。已把watchdog、查证计时和完成通知放到Actor控制执行线程池，补“单线程I/O被阻塞且不释放仍能持久Blocked”测试；不会因超时释放未完成调用槽。
3. 额外补充畸形Ongoing快照（缺外部证据）和无已记录查询的重发历史拒绝测试。

较早一次过长的Run审阅被主动停止，不计为完整审阅。Operation编写代理在最终回归前停止；接管时核对其最后的replay校验修改并由主会话补测。所有结论以实际源码及测试为准。

## R1 当时未认证与后续范围（历史记录）

以下描述 R1 验收时的边界，不表示后续 R2 没有实现。

- R1核心实现、独立审阅修正、测试与公共JAR恢复证据已完成，首轮停在G1；完整设计的G2–G4随后获用户授权继续实施。
- 空消费者依赖缓存构建通过：`target/r1-cold-consumer-cjmXHg` 为新建目录，独立Ivy/Coursier缓存和构建target；examples重新编译3个源码成功。60项classpath中57个第三方JAR来自新缓存，0项来自全局`Library/Caches`或`.ivy2`；另外是examples classes及2个明确的本地产品JAR。仍复用本机JDK/sbt launcher与构建插件启动环境，不称为裸机安装认证。
- R1这一轮不含G2资源/交接、G3批调度、G4现有银行/Fab迁移；当时消费者工作树未改。
- 未做真实银行/设备、生产journal、物理断电、存储损坏、脑裂或SLA认证。journal中的历史/终态默认不删除；长期磁盘留存由宿主治理。
- 全局活跃实体/外部连接并发须由宿主限制；共享ManagedOperations只限制该实例本地等待/查询，不冒充全局资源调度器。

## R2 实施与验收（2026-11-06）

用户在R1验收提交后明确授权“都提交，然后继续彻底完成”。R1源码提交为 `d2fd6df`。R2仍在同一短期feature分支演进，消费者使用各自 `feature/monarch-complete-workflow`；不推送、不覆盖历史制品、不混入已有dirty。

### 已执行结果

| 验收 | 结果 |
|---|---|
| 资源与Inbox首轮 | 39/39 |
| 32个完整JVM场景首轮 | 32/32，独立BUSINESS账本及资源关闭验证 |
| 全量（含R1与R2、128×1KiB容量快速回归） | 29 suites，185/185，0失败/0跳过，MiMa通过 |
| 最后受影响的协议/空缓存宿主查证/批活性回归 | 7 suites，43/43，MiMa通过 |
| 新R2候选 + public-artifact examples | 构建成功，5个独立入口/夹具源码编译成功 |
| Bank/Fab构建及HTTP门 | 两域独立target完整编译，策略7/7＋10/10，真实HTTP/整Play JVM恢复均通过 |
| 最终候选artifact-only矩阵 | 4 suites，49/49：R2 32＋R1 14＋外部账本3，0失败/0跳过 |
| 全新消费者缓存 | 5源码编译通过；60项classpath中57个第三方JAR来自独占新cache，0全局cache项 |

185测试的全量运行期间仅继续准备最后的空缓存宿主查证修正；因此185/185不是最后每一字节的全量快照。最后43项回归覆盖受影响代码；最终公共候选49/49矩阵再次验证真实进程行为。此前32场景和全量均保留独立证据。

32场景包含：Bank/Fab正常及Ongoing宿主强杀；两领域分别在Accepted、ChildStartIntent、ChildTerminalRecorded、GrantRecorded、ReleaseRecorded、Settled的提交前与已force未ACK；Fab另有TransferRecorded两边界；Bank封批两边界。每个资源/交接边界场景最后再次重启终态，外部COUNT保持不变。`WORKFLOW_RESOURCES_CLOSED`来源是测试full-capacity sentinel获批，证明此前执行/物理占位已关闭，sentinel留在该退休测试域。

### 候选（不可覆盖）

- 版本：`0.3.0-dev.r2-20261106-dc168f96fa62`。
- 新目录：`target/r2-candidate-20261106-dc168f96fa62/`。
- 产品源码/build/project长度分帧SHA-256：`dc168f96fa62700537644e486e64a44b38bfa77f8e897e54c3bf68d0fa94687d`。
- persistence JAR SHA-256：`2dea09f3c97b4832fd78552f546a1d5fb4f68d5b92c42cb3d4466101c59d21b0`。
- core仍为上文固定0.2.1 / `66223d37...`，没有重新发布core。
- examples runtime classpath 60项；无产品target/classes/test-classes；产品JAR无Fixture/ValidationJournal/Spec类。
- 候选内source-manifest逐文件核验一致；制品与classpath证据：`target/r2-build-evidence-dc168f96fa62/`。
- 最终artifact-only49/49通过；逐份核对118个宿主日志（R1 30、R2 88）的两项CodeSource/路径/SHA，全部是上述persistence和固定core JAR，无产品classes目录；`origins.json`列出全部日志。
- `target/r2-cold-consumer-fekhxoef/`是新建独立消费者build、Ivy/Coursier缓存与target；重新编译5个源码，60项classpath中57个第三方JAR全部来自新cache，0全局`Library/Caches`/`.ivy2`项。仍复用本机JDK/sbt启动环境，不宣称裸机安装。
- 真实进程矩阵证据：`target/r2-final-artifact-evidence-20261106/`；包含源与宿主WAL、被杀进程/恢复/终态日志及waited退出记录。

### Bank/Fab 真实 HTTP 与整应用 JVM 门

- Bank：7/7策略测试、独立target完整编译/stage；Fab：10/10策略测试、完整编译/stage。
- 同一验证脚本 `examples/verify_http_consumers.py`，使用动态独占loopback端口、不同外部WAL/应用LevelDB路径、随机运行时secret。验证JS语法与真实HTTP，不宣称浏览器视觉自动化。
- 每领域三组合同：正常完成＋重复输入/计数冲突；Operation/Inbox预算阻塞后杀整个Play JVM、外部完成任务、全新JVM查证原身份；终态再启且COUNT不变。
- CSRF缺失POST得到403；控制器只允许loopback请求，完成按钮不接受用户伪造成功结果。
- Bank两个作业6项：credit6/purchase6/balance0/holding600；只有指定失败项再次充值，申购各一次；10个外部任务＝10次submit，duplicateEffects=0。
- Fab两个作业4FOUP：全部accepted/unloaded回stocker；指定item-1只返工一次、量测两次；24个外部任务＝24次submit，duplicateEffects=0。
- 两应用实际CodeSource指向stage内上述候选JAR，SHA完全一致；不是仅pom依赖声明。每领域三个不同Play PID与独立external PID已停止并waited=true。
- PASS证据：`target/r2-http-evidence-20261106/r2-http-bank-tj3yjx0p/`、`target/r2-http-evidence-20261106/r2-http-fab-8fhho6pj/`。
- 首次HTTP启动只指定`-Dhttp.port`没覆盖profile内`play.server.http.port`，因此超时；原失败证据保留，新验证器显式设两者并使用独占cwd后通过。未修改运行时制品。
- 消费者构建中的新路由曾覆盖旧`controllers.routes`；改为独立AutoPlugin＋forward-only新增路由，旧源码/配置不动。关闭R2时分别检查两仓的实际sources、routes tasks、dependencyOverrides，确认均无本次新增项；检查日志在`target/r2-build-evidence-dc168f96fa62/*-r2-off.log`。

### 已修正问题和证据诚实边界

- 封批gate期间EOF可能尚未提交；fixture改为重放同一不可变源和EOF，不能把恢复强制关输入算修复。相应Scala裸block和testkit调用语法错误均已修正。
- resource严格FIFO可能挡住占位者续行，改oldest-fitting；批调度增加carried后继优先、共享claim全部holder同目标/延迟、轮转、字节压力封尾批。
- 逐项execution/lineage预算边界、非法Advanced/Settled/Verified历史、未持有新grant却把Transfer当Release等已加反例。
- Inbox查证授权提交后仍是Blocked；新增只读 `View.reconciling` 表达已记录查询授权，父批继续等证据而非误判拒绝。
- 完整宿主的显式人工查证从原operation到inbox/batch接通；新宿主空缓存根据已提交job激活原operation，不依赖旧内存表。自动轮询不会授予新预算。
- 资源/Delivery独立审阅代理因上游422失败，没有可用完整独立审阅结果；由主会话阅读关键实现和验证，并以实际反例与回归收口，不声称多票审阅通过。
- 首轮32场景中进程stdout关闭有一次读取线程race；增加本夹具主动停止标记后，全量复跑无此异常。未吞正常运行中的I/O错误。
- 工具偶发ENOSPC提示；实际df仍有约19GiB，证据只有数MiB；未删除其他目录或缓存。

证据：`target/r2-boundaries-20261106/`、`target/r2-full-regression-20261106/`。运行时保证及非目标见 [DURABLE_WORKFLOWS.zh.md](DURABLE_WORKFLOWS.zh.md)：有限job、单有效writer、可靠journal；无真实银行/FTP/TA/设备、物理断电/损坏/脑裂/SLA认证。
