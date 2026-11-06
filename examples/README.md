# R1 public-artifact examples

这些入口以公开 Maven 坐标消费 `monarch-persistence` 候选 JAR 和固定 core JAR，不 `dependsOn(root)`，不把产品 `target/classes` 偷渡到消费者 classpath。

- `net.imadz.monarch.persistence.ExternalTaskServiceFixture`：独立本机外部任务服务，自己的耐久接受／效果账本，仅模拟银行与 Fab。
- `net.imadz.monarch.persistence.ManagedOperationJvmHost`：通过公开 `managedBehavior`、`ManagedOperations`、`PersistentOperation` API 接线。
- `SerializedValidationJournal`：测试专用真实序列化文件 journal，不是产品插件。

上述源码由根构建按精确文件名引用测试夹具，避免复制两份实现；examples 不发布。测试断言和所有 Operation/Run 的产品源码均不作为 examples 源码输入。

## 构建

先把新候选写入一个**此前不存在**的独占本地 Maven 目录，不覆盖 0.2.1、P23 或任何旧候选。设置：

```bash
export JAVA_HOME=/path/to/jdk-11
export PATH="$JAVA_HOME/bin:$PATH"
CORE_REPOSITORY=file:///absolute/path/to/frozen-core-repository
CANDIDATE_REPOSITORY=file:///absolute/path/to/new-candidate-repository
CANDIDATE_VERSION=0.3.0-dev.<unique-source-fingerprint>

# 在 monarch-persistence 根目录；候选 JAR 已准备好之后
sbt -batch \
  -Dmonarch.core.repository="$CORE_REPOSITORY" \
  -Dmonarch.example.repository="$CANDIDATE_REPOSITORY" \
  -Dmonarch.example.version="$CANDIDATE_VERSION" \
  'examples / compile' 'show examples / Runtime / fullClasspath'
```

## 手动运行

使用 examples 的实际 runtime classpath，`JAVA_HOME/bin/java` 启动两个进程。每次新建独占空目录，勿使用现有应用 journal：

```bash
java -cp "$EXAMPLE_CLASSPATH" \
  net.imadz.monarch.persistence.ExternalTaskServiceFixture /absolute/path/to/external-fixture
# 输出 EXTERNAL_READY <port>

java -cp "$EXAMPLE_CLASSPATH" \
  net.imadz.monarch.persistence.ManagedOperationJvmHost \
  /absolute/path/to/host-journal <port> bank r1-bank first
```

将 `bank r1-bank` 换成 `fab r1-fab` 使用同一公共协议处理模拟加工。外部任务由测试客户端 `ExternalTaskServiceFixture.call(port, "COMPLETE", Vector("r1-bank/submit"))` 确认完成；它不因宿主超时自动变成成功。入口会输出实际产品 `CodeSource` 路径／SHA、Run 和 Operation 状态。

自动多进程验收入口为根项目 `OperationJvmRecoverySpec`。为让该套测试启动以上 artifact-only examples classpath，传入：

```bash
sbt -batch \
  -Dmonarch.core.repository="$CORE_REPOSITORY" \
  -Dmonarch.validation.java="$JAVA_HOME/bin/java" \
  -Dmonarch.validation.consumer.classpath="$EXAMPLE_CLASSPATH" \
  -Dmonarch.validation.evidence=/absolute/path/to/new-evidence \
  'testOnly net.imadz.monarch.persistence.OperationJvmRecoverySpec'
```

测试只停止自身创建的进程，先等待旧宿主退出，再用同一 journal 启动新宿主。外部效果账本、过程日志与 PID/退出记录保留在证据目录。

此示例不证明真实银行/设备集成、物理断电或脑裂恢复；也不包含 G2 的资源准入或 G3 的批调度。实际执行结果见 `../docs/ACCEPTANCE.zh.md`。
