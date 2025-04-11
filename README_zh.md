<p align="center">
<picture>
  <source media="(prefers-color-scheme: light)" srcset="docs/images/kafka-logo-readme-light.svg">
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/kafka-logo-readme-dark.svg">
  <img src="docs/images/kafka-logo-readme-light.svg" alt="Kafka Logo" width="50%"> 
</picture>
</p>

[![CI](https://github.com/apache/kafka/actions/workflows/ci.yml/badge.svg?branch=trunk&event=push)](https://github.com/apache/kafka/actions/workflows/ci.yml?query=event%3Apush+branch%3Atrunk)
[![Flaky Test Report](https://github.com/apache/kafka/actions/workflows/generate-reports.yml/badge.svg?branch=trunk&event=schedule)](https://github.com/apache/kafka/actions/workflows/generate-reports.yml?query=event%3Aschedule+branch%3Atrunk)

[**Apache Kafka**](https://kafka.apache.org) 是一个开源的分布式事件流平台，被数千家公司用于高性能数据管道、流分析、数据集成和关键任务应用。

你需要安装 [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html)。

我们使用Java 17和23来构建和测试Apache Kafka。客户端和streams模块的javac `release`参数设置为`11`，其他模块设置为`17`，以确保与各自的最低Java版本兼容。同样，streams模块的scalac `release`参数设置为`11`，其他模块设置为`17`。

Apache Kafka目前仅支持Scala 2.13版本。

### 构建jar包并运行 ###
    ./gradlew jar

按照 https://kafka.apache.org/quickstart 中的说明进行操作

### 构建源代码jar包 ###
    ./gradlew srcJar

### 构建聚合javadoc ###
    ./gradlew aggregatedJavadoc

### 构建javadoc和scaladoc ###
    ./gradlew javadoc
    ./gradlew javadocJar # 为每个模块构建javadoc jar
    ./gradlew scaladoc
    ./gradlew scaladocJar # 为每个模块构建scaladoc jar
    ./gradlew docsJar # 为每个模块构建javadoc和scaladoc jar（如果适用）

### 运行单元/集成测试 ###
    ./gradlew test  # 运行单元和集成测试
    ./gradlew unitTest
    ./gradlew integrationTest
    ./gradlew quarantinedTest  # 运行隔离测试

### 在代码未更改的情况下强制重新运行测试 ###
    ./gradlew test --rerun-tasks
    ./gradlew unitTest --rerun-tasks
    ./gradlew integrationTest --rerun-tasks

### 运行特定的单元/集成测试 ###
    ./gradlew clients:test --tests RequestResponseTest

### 通过设置N来重复运行特定的单元/集成测试 ###
    N=500; I=0; while [ $I -lt $N ] && ./gradlew clients:test --tests RequestResponseTest --rerun --fail-fast; do (( I=$I+1 )); echo "Completed run: $I"; sleep 1; done

### 运行单元/集成测试中的特定测试方法 ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testTimeToNextUpdate

### 运行带有log4j输出的特定单元/集成测试 ###
默认情况下，测试时只会输出少量日志。你可以通过修改模块的`src/test/resources`目录中的`log4j2.yaml`文件来调整它。

例如，如果你想查看更多clients项目测试的日志，可以修改`clients/src/test/resources/log4j2.yaml`中的[这一行](https://github.com/apache/kafka/blob/trunk/clients/src/test/resources/log4j2.yaml#L35)为`level: INFO`，然后运行：
    
    ./gradlew cleanTest clients:test --tests NetworkClientTest   

你应该能在`clients/build/test-results/test`目录下的文件中看到`INFO`级别的日志。

### 指定测试重试 ###
默认情况下重试功能是禁用的，但你可以设置maxTestRetryFailures和maxTestRetries来启用重试。

以下示例声明-PmaxTestRetries=1和-PmaxTestRetryFailures=3，允许失败的测试重试一次，总重试限制为3次。

    ./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=3

quarantinedTest任务默认也没有重试，但你可以设置maxQuarantineTestRetries和maxQuarantineTestRetryFailures来启用重试，类似于test任务。

    ./gradlew quarantinedTest -PmaxQuarantineTestRetries=3 -PmaxQuarantineTestRetryFailures=20

更多详情请参见[Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin)和[build.yml](.github/workflows/build.yml)。

### 生成测试覆盖率报告 ###
为整个项目生成覆盖率报告：

    ./gradlew reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false

为单个模块生成覆盖率报告，例如：

    ./gradlew clients:reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
    
### 构建二进制发布的gzip压缩tar包 ###
    ./gradlew clean releaseTarGz

发布文件可以在`./core/build/distributions/`目录中找到。

### 构建自动生成的消息 ###
有时在分支切换时，只需要重新构建RPC自动生成的消息数据，因为它们可能会因代码更改而失败。你可以直接运行：
 
    ./gradlew processMessages processTestMessages

### 运行Kafka broker

使用编译文件：

    KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
    ./bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
    ./bin/kafka-server-start.sh config/server.properties

使用docker镜像：

    docker run -p 9092:9092 apache/kafka:3.7.0

### 清理构建 ###
    ./gradlew clean

### 为特定项目运行任务 ###
这适用于`core`、`examples`和`clients`

    ./gradlew core:jar
    ./gradlew core:test

Streams有多个子项目，但你可以运行所有测试：

    ./gradlew :streams:testAll

### 列出所有gradle任务 ###
    ./gradlew tasks

### 构建IDE项目 ###
*注意：开发Kafka时请确保使用JDK17。*

IntelliJ原生支持Gradle，它会自动检查每个模块的Java语法和兼容性，即使在`Structure > Project Settings > Modules`中显示的Java版本可能不正确。

对于Eclipse，运行：

    ./gradlew eclipse

`eclipse`任务已配置为使用`${project_dir}/build_eclipse`作为Eclipse的构建目录。Eclipse的默认构建目录（`${project_dir}/bin`）与Kafka的scripts目录冲突，我们不使用Gradle的构建目录是为了避免这种配置的已知问题。

### 将streams quickstart archetype构件发布到maven ###
对于Streams archetype项目，不能使用gradle上传到maven；而是需要在quickstart文件夹中调用`mvn deploy`命令：

    cd streams/quickstart
    mvn deploy

请注意，要使其工作，你应该创建/更新用户maven设置（通常是`${USER_HOME}/.m2/settings.xml`）以分配以下变量

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...

### 将所有项目安装到本地Maven仓库 ###

    ./gradlew -PskipSigning=true publishToMavenLocal

### 将特定项目安装到本地Maven仓库 ###

    ./gradlew -PskipSigning=true :streams:publishToMavenLocal
    
### 构建测试jar包 ###
    ./gradlew testJar

### 运行代码质量检查 ###
我们定期运行两个代码质量分析工具：spotbugs和checkstyle。

#### Checkstyle ####
Checkstyle在Kafka中强制执行一致的编码风格。
你可以使用以下命令运行checkstyle：

    ./gradlew checkstyleMain checkstyleTest spotlessCheck

checkstyle警告将在子项目构建目录的`reports/checkstyle/reports/main.html`和`reports/checkstyle/reports/test.html`文件中找到。它们也会打印到控制台。如果Checkstyle失败，构建将失败。
对于实验（或回归测试目的），添加`-PcheckstyleVersion=X.y.z`开关（覆盖项目定义的checkstyle版本）。

#### Spotless ####
导入顺序是静态检查的一部分。在提交拉取请求之前，请调用`spotlessApply`来优化Java代码的导入。

    ./gradlew spotlessApply

#### Spotbugs ####
Spotbugs使用静态分析来查找代码中的bug。
你可以使用以下命令运行spotbugs：

    ./gradlew spotbugsMain spotbugsTest -x test

spotbugs警告将在子项目构建目录的`reports/spotbugs/main.html`和`reports/spotbugs/test.html`文件中找到。使用-PxmlSpotBugsReport=true可以生成XML报告而不是HTML报告。

### JMH微基准测试 ###
我们使用[JMH](https://openjdk.java.net/projects/code-tools/jmh/)来编写微基准测试，以在JVM中产生可靠的结果。
    
有关如何运行微基准测试的详细信息，请参见[jmh-benchmarks/README.md](https://github.com/apache/kafka/blob/trunk/jmh-benchmarks/README.md)。

### 依赖分析 ###

gradle[依赖调试文档](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html)提到使用`dependencies`或`dependencyInsight`任务来调试根项目或单个子项目的依赖关系。

或者，使用`allDeps`或`allDepInsight`任务递归遍历所有子项目：

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtimeClasspath --dependency com.fasterxml.jackson.core:jackson-databind

这些任务接受与内置变体相同的参数。

### 确定是否可以更新任何依赖项 ###
    ./gradlew dependencyUpdates

### 常用构建选项 ###

以下选项应该使用`-P`开关设置，例如`./gradlew -PmaxParallelForks=1 test`。

* `commitId`：设置构建提交ID，因为.git/HEAD可能不正确（如果添加了用于构建目的的本地提交）。
* `mavenUrl`：设置maven部署仓库的URL（可以使用`file://path/to/repo`指向本地仓库）。
* `maxParallelForks`：并行启动的最大测试进程数。默认为JVM可用的处理器数量。
* `maxScalacThreads`：scalac后端的最大工作线程数。默认为`8`和JVM可用处理器数量中的较小值。该值必须在1到16（含）之间。
* `ignoreFailures`：忽略来自junit的测试失败
* `showStandardStreams`：在控制台上显示测试JVM的标准输出和标准错误。
* `skipSigning`：跳过构件签名。
* `testLoggingEvents`：要记录的单元测试事件，用逗号分隔。例如`./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`。
* `xmlSpotBugsReport`：启用spotBugs的XML报告。这也会禁用HTML报告，因为一次只能启用一种。
* `maxTestRetries`：失败测试用例的最大重试次数。
* `maxTestRetryFailures`：在禁用后续测试重试之前的最大测试失败次数。
* `enableTestCoverage`：启用测试覆盖率插件和任务，包括跟踪覆盖率所需的字节码增强。请注意，这会在运行测试时引入一些开销，这就是为什么默认禁用它（开销因情况而异，但15-20%是一个合理的估计）。
* `keepAliveMode`：配置Gradle编译守护进程的保持活动模式 - 重用可以改善启动时间。值应该是`daemon`或`session`之一（默认是`daemon`）。`daemon`保持守护进程活动直到显式停止，而`session`保持它活动直到构建会话结束。这目前只影响Scala编译器，参见https://github.com/gradle/gradle/pull/21034 了解尝试对Java编译器做同样事情的PR。
* `scalaOptimizerMode`：配置scala编译器的优化行为，值应该是`none`、`method`、`inline-kafka`或`inline-scala`之一（默认是`inline-kafka`）。`none`是scala编译器默认值，只消除不可达代码。`method`还包括方法本地优化。`inline-kafka`添加了kafka包内方法的内联。最后，`inline-scala`还包括scala库中方法的内联（这避免了像`Option.exists`这样的方法的lambda分配）。`inline-scala`只有在编译时和运行时的Scala库版本相同时才是安全的。由于我们不能保证所有情况都是这样（例如，用户可能依赖kafka jar进行集成测试，其中可能包含不同版本的scala库），我们默认不启用它。更多详情请参见https://www.lightbend.com/blog/scala-inliner-optimizer。

### 运行系统测试 ###

参见[tests/README.md](tests/README.md)。

### 在Vagrant中运行 ###

参见[vagrant/README.md](vagrant/README.md)。

### 贡献 ###

Apache Kafka致力于建设社区；我们欢迎任何想法或[补丁](https://issues.apache.org/jira/browse/KAFKA)。你可以通过[Apache邮件列表](http://kafka.apache.org/contact.html)联系我们。

要贡献，请按照以下说明操作：
 * https://kafka.apache.org/contributing.html