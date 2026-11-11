# Monarch R2 后续交付与真源工具链升级评估

- 原始评估日期：2026-11-06。
- 性质：历史只读评估与后续状态补注，**本文不新增实施或发布授权**。
- 当前运行时验收：[ACCEPTANCE.zh.md](ACCEPTANCE.zh.md)。
- 当前 R2 API 与保证边界：[DURABLE_WORKFLOWS.zh.md](DURABLE_WORKFLOWS.zh.md)。

## 后续状态补注（2026-11-11）

用户已批准 JS 第七批新非银行家族 `scala-cqrs-saga-monarch-v1`，该范围已完成有限工程验收，消费固定的 `0.3.0-dev.r2-20261106-dc168f96fa62` 与 core `0.2.1`，不等待正式 `0.3.0` 发布。此范围内的新授权已取代下方历史等待。依据为 sibling `metarch-truthsrc` 的[家族合同](../../metarch-truthsrc/docs/artifacts/SAGA-MONARCH-R2.md)和[有限验收记录](../../metarch-truthsrc/docs/artifacts/SAGA-MONARCH-R2-ACCEPTANCE.md)；这些是另一个仓库中已跟踪且干净的文档，链接依赖工作区 sibling 检出，不是只检出本仓即可获得的保证。

有限工程验收不把旧完整 Node `FAIL` 重签为全绿，也不等于架构 owner 最终批准。银行/P23 切换、journal 迁移、runtime 源码修改及重新发布仍在该授权范围之外。

本次四文档检查点独立于上述实施线，只修正文档时态与范围，不重跑测试、编译、生成或服务。历史“保留 dirty / 等待”不是当前 owner 锁，也不是写入其他文件或仓库的许可。

**第 0–8 节均保留为 2026-11-06 的历史快照。** 其中“当前”“本轮”“尚未”“后续”等时间词，以及数量、路径、行号、分支状态和等待决定，均指原始评估时点，不是本次重新核验的现状或新测试声明。第 7 节是当时的候选次序，不要求重复已完成有限验收的第七批家族；尚余事项需按最新基线及独立授权处理。

## 0. 历史用户裁决（2026-11-06）：先等待，再重新计划

用户在评估后明确：

> `metarch-truthsrc` 和 `bank-wealth-demo` 两个仓库仍在为最小交付做最后收尾，当前最小交付还没有包括本会话的升级内容。先把本次结论落盘记住；等那边结束之后，再做一次计划。

据此执行：

1. 当前只保存评估、记录等待边界和提供上下文摘要。
2. **不把 Monarch R2 的语言、语义编译器、代码生成器或银行财富迁移追加到那两个仓库正在进行的最小交付。** 不抢占 owner，不覆盖其 dirty 文件，不重复开展一套平行实现。
3. 暂不依据本文开始新的 README/默认入口迁移、工具链升级或分支合并；这些保留为后续统一计划的候选内容。
4. 等当前最小交付明确完成并交接后，重新核对两仓最新 HEAD、工作树、最终合同、工具/制品版本、实际验收和剩余事项，再形成一次统一计划。下文行号和基线均是当时快照，不可机械套用。
5. 本轮 runtime 的既有实现/提交授权，不自动扩展为工具链仓库实施、P23 历史迁移、共享生成应用写入、外部发布或推送授权。
6. 不设置自动轮询、定时升级或后台接管任务。

## 1. 完成口径：运行时闭环与真源生成闭环分开

### 已经完成并验证

- `monarch-persistence` 在同一 artifact 中提供 R1 可恢复外部操作及 R2 限定资源域、耐久交接、有限批作业、逐项重试/路由/返工与排空。
- 保留原 `PersistentMonarch` 阶段执行与 checkpoint ACK；core 源码不变，没有 Graph/Streams 路线或第二套阶段执行器。
- Bank 小样本：批量充值 → 明确失败子集重试 → 成功项逐笔申购 → 账务收尾。
- Fab 小样本：搬运 → 加工 → 量测 → 必要返工 → 卸载；区分执行许可与持续物理占位。
- 两个现有 demo 仓库增加独立、opt-in 的薄接入和权威查询页面；实际 HTTP、CSRF、输入重放/冲突和整 Play JVM 恢复已经验证。

### 尚未完成的最终交付面

- R2 尚未合入单一 main；README 中英文和可复制上手流程尚未统一。
- 新 R2 页面没有替代旧默认页面和全部旧业务功能。
- 两个 demo 的 R2 提交均没有同步更新 `truth/`；新运行时能力尚未形成对应的完整真源 → 编译 → 生成 → 回锚闭环。
- `metarch-truthsrc`、`bank-wealth-demo` 当前最小交付不是本会话 R2 工具链升级的实施结果。

**不能把“尚未替换全部旧流程”解释成必须删除旧功能。** 小型 R2 样本不覆盖旧 Fab 动态 POR、OCAP、多 wafer、拆分/合并等全部能力。后续先确定推荐入口、新旧功能覆盖矩阵和迁移/legacy 标注，再判断哪些旧实现能退役。

## 2. 分层评估结论

以下“需要/必须升级”以“把 R2 纳入真源驱动开发闭环”为目标，不是现有 R2 手写样本运行的前置条件。

| 层面 | 判断 | 最小合理改动 | 相对量级 |
|---|---|---|---|
| README 中英文及使用指南 | 必须更新 | 对应章节、版本/入口选择、可获得制品、模拟器、复制运行、恢复操作和边界 | S–M |
| Bank/Fab 领域真源 | 需要补齐 | 表达新流程、结果/证据、资源占位、交接与完成条件；不只留在 Scala | M |
| 基础作者语法 | 不必重写 | 复用 author/structure/micro 体系，必要时加受控声明/局部微语法 | S–M |
| 语义编译器/校验器 | 必须适配 | 新受管理执行语义及可选有限工作流合同，严格能力与版本准入 | M–L |
| JS 代码生成器 | 必须适配 | 输出公共 R2 接线、领域适配、codec、配置、查询与测试，不复制恢复内核 | L |
| 本地图/AST/回锚/查询 | 局部增强 | 增加执行内部独立身份、关系和最终代码映射精度，复用存储抽象 | M |
| CLI/验证工具 | 局部增强 | 能力检查、profile/runtime 选择、迁移诊断、新生成结果与恢复验收 | M |
| backkend-codegen | 按需对齐 | 共合同/模板参照；是否维持完整 R2 对等生成另定范围 | 局部 M，全面 L |
| monarch-core/Saga runtime | 不随本轮自动升级 | core 未改；Saga 的跨聚合事务职责不与工作流恢复混为一谈 | 无必要升级 |
| 制品/默认入口/main 收口 | 需要完成 | 干净检出复现、明确可取得的依赖/fixture、分支归并及 legacy 说明 | M |

S/M/L 是相对工作量，不是工期或已批准排期。

## 3. README 与交付问题

### 当前观察

- persistence README 主要介绍旧 checkpoint wrapper；“库不创建子任务”一类表述需要限定为旧接口，不能覆盖新增的 WorkflowHost/Inbox。
- Bank README 的历史候选与启动 BLOCKED 口径需要注明日期/配置，不能简单全部改成“已完成”。
- Fab 中英文 README 的存储/启动口径不一致，应按 legacy 与 R2 profile 分开说明。
- 详细 R2 文档主要是中文，英文不能仅指向未翻译的长协议文档。
- 已验证候选放在本地隔离 Maven 目录；public-artifact 消费验证不等于 Maven Central 已发布。模拟器也不在产品 JAR 中，上手不能依赖一句“使用已准备好的 classpath”。
- 消费者是在保留原 dirty 修改的工作树上构建；本轮没有完成“只 checkout 已提交内容即可完整复现”的交付验证。

### 后续文档结构建议

中英文保持相同章节与相同事实来源：用途 → 分层/保证 → 推荐与legacy入口 → 版本和前置条件 → 获取制品/fixture → 启动/完成/查证/恢复 → 验收及非目标。保留旧正确安全说明，不覆盖/丢弃已有 README 草稿。

core 只补边界说明及 persistence R2 交叉链接，不虚构 core 新批引擎或新版本。

本次检查位置（工作区相对路径）：

- `monarch-persistence/README.md:64`。
- `monarch-persistence/docs/DURABLE_WORKFLOWS.zh.md:28`。
- `bank-batch-transactor-demo/README.md:53`。
- `fab-simulator-demo/README.md:22`、`README-zh.md:22`。

## 4. 领域真源、语言和语义编译器

### 4.1 原 demo 真源没有随 R2 提交更新

机械检查两仓 R2 commit 的文件清单，均未改 `truth/` 或真源项目配置。

- `bank-batch-transactor-demo/truth/pipelines/batch-chain.pipeline.yaml:7` 仍描述六阶段文件链，以已完成 stage 为续跑词汇。
- `fab-simulator-demo/truth/pipelines/fab-pipeline.pipeline.yaml:7` 仍描述原步骤队列、动态 POR、OCAP 及相关业务。

这些事实不等于原真源“没有任何恢复知识”；它们说明本轮代码变更没有完成新语义的源端同步。不能拿旧叙述性扩展的存在冒充严格 R2 可生成支持。

### 4.2 作者语法已经可以复用

`metarch-truthsrc` 当前已实现：

- `truthsrc.author/v1`、`truthsrc.context/v1`、`truthsrc.project/v1`。
- `truthsrc.structure/v1` 的有限结构共享。
- `truthsrc.micro/v1` 的受控语义槽位简写。
- 宏展开 → 微语法降低 → canonical；原作者文件保留与受检查的恢复。
- 单一作者入口、明确技术 bindings、严格语义产品与生成写入边界。

因此不建议换 YAML/CUE/TypeSpec、重写解析器、增加第二棵现役树或要求作者手写所有展开协议字段。

### 4.3 严格执行合同仍非 R2

评估时的注册合同包括 `execution-contract@1`、`child-plan-contract@1`、`effect-host-contract@1/@2`、`saga-host-contract@1/@2` 等。host `@2` 不等于 Monarch R2。

当前 execution payload 为 `truthsrc.execution/v1`，主要步骤为 `port | invokeSaga | awaitChildRun`；schema 字面固定 `monarch-persistence/0.2.1`。已有身份、durable input、confirmed-prefix、外部效果证据/Unknown 验证，但未定义 R2 的完整资源/Inbox/有限批语义。单纯可保存扩展字段不等于可执行语义已受验证。

推荐后续讨论：

1. 受管理执行合同：Run/Operation 身份、不可变请求、connector/epoch/codec、证据和查证语义。
2. 可选有限工作流合同：Item/Manifest、Area/BatchPolicy、资源占位、逐项处置、可靠交接、EOF/Drained 和有限预算。
3. 不把每个 runtime 类型都变成一种作者产品；基础顺序流程不强制携带完整批模型。
4. 作者语法版本、语义能力版本、生成 profile、runtime artifact 版本分别定义。
5. 旧 profile 保持原意义；新能力不支持就拒绝，不能静默降低为旧行为。

关键位置：`metarch-truthsrc/lib/products/registry.mjs:3`、`schema/execution-ir-v1.schema.json:342`、`lib/execution/compile-pure.mjs:492`。

## 5. 生成器、本地图与回锚

### 5.1 生成器不是空骨架，但需要新的原生 API 接线

当前 JS 正式生成包括领域规则/fold、序列化、Saga/effect/child宿主、兼容应用层、local repository、可选JDBC读侧等。不能说“现在只能生成 scaffold”。

但执行/child renderer 仍输出 `PersistentMonarch.behavior`、`StartRun`、observer 和专用 journal/host 机制。正式生成 artifact 准入锁定 P23 三 runtime 的版本与 SHA，不能只改版本常量或仓库 URL。

R2 生成应输出：稳定请求与RunDefinition/RunSubmission、managed waiting或WorkflowProgram、领域connector/证据验证接口、纯分类与资源策略、codec/config/bootstrap/query、测试及外置代码映射。

**不生成第二套 ResourceCoordinator、PersistentInbox 或 BatchCoordinator。** 同时不能简单删掉旧宿主中的领域关联、严格TCC、结果交付/确认职责；先做责任映射，再收敛实现。

关键位置：

- `metarch-truthsrc/generators/scala-cqrs-application/render-execution.mjs:20`。
- `metarch-truthsrc/generators/scala-cqrs-application/render-child-hosts.mjs:29`。
- `metarch-truthsrc/lib/products/generate-cli.mjs:77`。
- `metarch-truthsrc/generators/scala-cqrs-application/render-build.mjs:3`。

### 5.2 复用图/AST/anchors，补执行粒度

当前项目生成确实消费本地图导出的 AST；知识快照与生成锚在同一逻辑 SQLite 存储中分离管理，存在存储接口。宏/微语法支持多展开节点对应一个作者声明，最终字节映射及MANUAL保护也已实现。

差距是部分执行溯源只到整个pipeline，不能自然精确查询step/operation/resource policy的独立身份及生成片段。应增语义记录、关系和renderer映射，继续使用现有一致性/新鲜度校验。

不需要新图服务、另一套anchor系统或恢复新增源码 `@knowledge` 注释。普通 canonical query graphstore 与项目 knowledge snapshot/AST store 的保证不同，不能混用它们的检查结果。

关键位置：`metarch-truthsrc/lib/source-index.mjs:202`、`lib/knowledge-graph.mjs:16`、`lib/project/graph.mjs:21`、`src/core/code-mappings.mjs:3`。

## 6. 银行财富与 BC 的独立边界

### 6.1 bank-wealth-demo 不是 bank-batch-transactor-demo

前者当前最小交付有支付、TA、独立退款、有限定投四条生成执行流；唯一项目入口为根 `truthsrc.project.json`，作者树为 `truth/bank-wealth`，技术选择在 `bindings/implementation/`。

评估快照选择14个产品和8份bindings文档；当前生成ledger为233条（229源码/配置＋4证据）。这些不是本次R2迁移的结果。

其三份 runtime 仍锁 `0.2.1-p23.20261027.1`，准备工具校验实际 JAR SHA：

- Monarch core：source `3109a440`，SHA前缀 `046ce5a54d49`。
- Monarch persistence：source `9facddd1`，SHA前缀 `5cf3dd2784fa`。
- Saga core：source `4352a317`，SHA前缀 `f20ad61eba67`。

完整锁见 `bank-wealth-demo/docs/P23-RUNTIME-LOCK.json:12`；当前生成依赖见 `generated/current/build.sbt:16`。不要与本轮 R2 的固定 core 0.2.1 混为同一制品。

迁移必须保护：订单最后严格TCC；TA技术提交完成与投资成功分离；独立退款不等于补偿已完成支付；有限SIP时钟/暂停/版本/首次终态计数；原identity/PID/codec/journal/receipt；现有冻结测试和制品。

`retry/backoff/deadline` 业务政策债仍OPEN，不能因R2提供通用技术查证预算就宣称它已解决。

若后续计划选该仓为首个迁移消费者，优先候选是单次SIP occurrence → 既有现金流程耐久启动确认的技术边界，不先改SIP调度和四条业务流。此选择仍需在最小交付完成后的新基线中复核，**当前不是已决定实施的切片**。

### 6.2 backkend-codegen 不自动全仓升级

BC的严格semantic产品/模板也仍使用旧Monarch/P23；另有legacy scaffold默认0.2，两者不是同一依赖路径。其现役Metarch输入切流、服务摄入、共享生成应用与本次R2适配应分开。

建议先以BC作共合同和模板参照；是否继续保持BC/JS对等R2生成能力，应在新计划明确，不把全BC迁移、服务端知识摄入或共享应用重生成设为最小前置。

关键位置：`backkend-codegen/src/main/scala/net/imadz/meta/products/SemanticProductSet.scala:82`、`codegen/SemanticProductsGenerationPlan.scala:33`、`codegen/scaffolding/cqrs/template/products/ProductInvocationExecutionTemplate.scala:58`（后二者均位于 `src/main/scala/net/imadz/`）。

本次BC结论基于明确工具/模板/绑定，**未经图验证**；没有作“领域真源不存在某概念”的全局负断言。

## 7. 等待解除后，新计划应如何收敛

下列是候选次序，不是当前执行清单：

A. 核实两仓最小交付已完成/交接，记录最新基线、owner、dirty与剩余事项。
B. 确定双语文档、可获得制品、干净检出构建、推荐/legacy入口和main归并的交付终点。
C. 以已验证Bank/Fab样本固定最小R2作者/语义/技术绑定责任表。
D. 语义编译器、JS renderer、图/AST/外置代码映射同步适配。
E. 由真源生成的小样本通过业务和进程恢复门，再评估银行财富有限切片；BC范围另列。

必要验收：

- 作者原文/语义roundtrip、已有profile回归，新能力未知字段/版本/缺依赖拒绝。
- 身份/请求冲突拒绝；timeout/Unknown/NotFound不得当成功、普通业务失败、安全重发或资源释放证明。
- Queued≠Granted；持有/释放/移交有证据；共享物理占位不得拆分holder。
- 完整逐项覆盖、硬批最低要求、有限execution/lineage、EOF≠Drained、Held不假排空。
- 技术/业务重试分开，人工查证有限，重启/配置变化不能清预算。
- 同图AST生成、N:1作者映射、最终字节锚和MANUAL保护；不得以缓存相似代替完整链路一致。
- 生成代码实际使用公开R2制品，通过业务和JVM故障验证，而非只编译/检查模板文本。
- 老journal的兼容迁移或显式新namespace/交接是独立交付项，不靠换JAR自动完成。

## 8. 保存时的基线、证据和工作树边界

### 本会话已提交成果

| 仓库 | 分支/提交 |
|---|---|
| monarch-core | main `376b152`，本轮源码未改 |
| monarch-persistence | feature/monarch-recoverable-operations；R1 `d2fd6df`，R2 `985aaa5923a3ed05d27849bb01667c100d949b31` |
| bank-batch-transactor-demo | feature/monarch-complete-workflow `bf6223cc15306b1316870126f827e516bb03f0df` |
| fab-simulator-demo | feature/monarch-complete-workflow `8f3447a2afa976d58d030723df8a8ce8b063de6e` |

本地refs检查：persistence main `431c914` 为当前HEAD祖先，落后5commit；Bank main `1917928`、Fab main `d1be967` 各落后1commit；未fetch、未merge、未push。提交谱系可汇回main，不代表允许在dirty工作树中直接切分支/覆盖文件。

### 工具链评估快照（不是未来接续基线）

- metarch-truthsrc：`execution-compiler-v1@7c1b2ed`，有并行onboarding/view/verify/workshop修改。
- bank-wealth-demo：`feature/bank-wealth-truth-preview@8c4cb7e`，有并行workshop/runtime verifier/lease修改。
- backkend-codegen：`p15-p19-accepted-20261026@13c151b8e`，本地ahead2；有Standalone EP/coordination等修改。
- 用户在本记录时再次确认前两仓仍在最小交付收尾；不能凭以上提交名推断已结束。

### 本轮R2不可覆盖候选

- 版本：`0.3.0-dev.r2-20261106-dc168f96fa62`。
- 本地目录：`monarch-persistence/target/r2-candidate-20261106-dc168f96fa62/`。
- 产品源/build/project SHA-256：`dc168f96fa62700537644e486e64a44b38bfa77f8e897e54c3bf68d0fa94687d`。
- persistence JAR SHA-256：`2dea09f3c97b4832fd78552f546a1d5fb4f68d5b92c42cb3d4466101c59d21b0`。
- core固定0.2.1，目录 `target/demo-candidate-20261021/`，SHA-256 `66223d37ab979b59cde557500f3da7aafaf532ddfe3268401c5f786093e0f971`。
- P23、普通0.2.1、R1/R2历史候选及回执均不可覆盖或原版本重发。

### 验证口径

- 185/185为最后小修前的全量；随后43/43受影响回归与MiMa通过，不能把185说成最后每字节全量。
- 最终public-artifact进程矩阵49/49（R2 32＋R1 14＋外部账本3）；118组CodeSource/SHA核验。
- 独占空消费者cache五源码编译；60项classpath、57第三方JAR来自新cache、0全局cache项。
- Bank策略7/7、Fab10/10；两域HTTP/CSRF/输入幂等冲突/Blocked整JVM恢复/终态再启通过。
- Bank两作业6项credit/purchase各6、balance0/holding600；Fab两作业4FOUP卸载，指定项仅返工一次；duplicateEffects均0。
- 证据详见 [ACCEPTANCE.zh.md](ACCEPTANCE.zh.md)，相关路径在其R2章节；本轮验证进程已退出。
- 本次只读评估没有重跑上述测试、编译、生成或服务，没有打开知识图数据库；源码/测试引用不等于新的通过回执。

### 必须保护的现有修改

- MP的README.md、README-zh.md、VALIDATION.md是此前未提交内容；core中英文README/教程等亦是原修改。
- Bank原13文件、Fab原47文件的dirty内容此前已核对保留；不能 `git add .`、stash/reset/checkout 清理。
- 两最小交付仓的并行工作及BC owner修改不接管；生成应用不能直接手改或拷贝覆盖。
- 本次仅新增此评估文档并写入外部长期记忆，没有提交本记录、修改上述仓库或启动后续升级。
