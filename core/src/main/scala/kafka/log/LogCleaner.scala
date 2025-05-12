/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.{File, IOException}
import java.nio._
import java.util.Date
import java.util.concurrent.TimeUnit
import kafka.log.LogCleaner.{CleanerRecopyPercentMetricName, DeadThreadCountMetricName, MaxBufferUtilizationPercentMetricName, MaxCleanTimeMetricName, MaxCompactionDelayMetricsName}
import kafka.server.{BrokerReconfigurable, KafkaConfig}
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{BufferSupplier, Time}
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread
import org.apache.kafka.storage.internals.log.{AbortedTxn, CleanerConfig, LastRecord, LogCleaningAbortedException, LogDirFailureChannel, LogSegment, LogSegmentOffsetOverflowException, OffsetMap, SkimpyOffsetMap, ThreadShutdownException, TransactionIndex}
import org.apache.kafka.storage.internals.utils.Throttler

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Iterable, Seq, Set, mutable}
import scala.util.control.ControlThrowable

/**
 * 日志清理器负责从具有"compact"保留策略的日志中删除过时的记录。
 * 如果存在具有相同键K但偏移量O'大于O的消息，则具有键K和偏移量O的消息被视为过时。
 *
 * 每个日志可以被视为分为两个段部分：
 * 1. "已清理"部分 - 之前已经被清理过的部分
 * 2. "脏"部分 - 尚未清理的部分，进一步分为：
 *    - "可清理"部分
 *    - "不可清理"部分（被排除在清理之外）
 * 
 * 活动日志段始终是不可清理的。如果设置了压缩延迟时间，那么最大消息时间戳在清理操作压缩延迟时间内的段也是不可清理的。
 *
 * 清理过程由后台线程池执行。每个线程选择具有"compact"保留策略的最脏的日志进行清理。
 * 日志的脏程度通过日志脏部分的字节数与日志总字节数的比率来估计。
 *
 * 清理日志的过程：
 * 1. 清理器首先为日志的脏部分建立key=>last_offset映射。详细实现请参见{@link OffsetMap}。
 * 2. 映射建立后，通过重新复制每个日志段来清理日志，但会忽略在偏移量映射中出现的、且具有比段中更高偏移量的键
 *    （即出现在日志脏部分的键的消息）。
 *
 * 为避免段在重复清理后变得过小，我们实现了一个规则：
 * 如果在清理开始前，连续段的日志和索引大小小于最大日志和索引大小，则在清理时将这些段合并。
 *
 * 清理后的段会在可用时被交换到日志中。
 *
 * 清理器需要处理的一个细节是日志截断：如果日志在清理过程中被截断，则该日志的清理将被中止。
 *
 * 空负载（null payload）的消息被视为删除标记：
 * 1. 清理器只会在一定时间内保留删除记录，以避免无限期占用空间
 * 2. 这个时间段可以按主题配置，从段进入日志清理部分时开始计算（此时具有该键的任何先前消息已被删除）
 * 3. 清理部分中早于此时间的删除标记在日志段重新复制时不会被保留
 * 4. 这个时间通过在首次清理遇到删除标记的批次时设置其基准时间戳来跟踪
 * 5. 批次中记录的相对时间戳也会根据新的基准时间戳在此清理中进行修改
 *
 * 对于幂等/事务性生产者功能，清理过程更为复杂：
 *
 * 1. 为保持活动生产者的序列号连续性：
 *    - 始终保留每个producerId的最后一个批次，即使批次中的所有记录都已被删除
 *    - 只有当生产者写入新批次或因不活动而过期时，才会删除该批次
 * 
 * 2. 不清理最后稳定偏移量之后的内容：
 *    - 确保清理器观察到的所有记录都已确定（已提交或中止）
 *    - 允许使用事务索引提前收集中止的事务
 * 
 * 3. 来自中止事务的记录会被清理器立即删除，不考虑记录键
 * 
 * 4. 事务标记的保留：
 *    - 保留直到同一事务的所有记录批次都被删除
 *    - 且已经过足够时间，确保活动消费者在达到标记偏移量之前不会消费事务中的任何数据
 *    - 这遵循与墓碑删除相同的逻辑
 *
 * @param initialConfig 清理器的初始配置参数。实际配置可以动态更新
 * @param logDirs 偏移量检查点所在的目录
 * @param logs 日志池
 * @param logDirFailureChannel 用于添加清理日志时可能遇到的离线日志目录的通道
 * @param time 用于控制时间流逝的方式
 */
class LogCleaner(initialConfig: CleanerConfig,
                 val logDirs: Seq[File],
                 val logs: Pool[TopicPartition, UnifiedLog],
                 val logDirFailureChannel: LogDirFailureChannel,
                 time: Time = Time.SYSTEM) extends Logging with BrokerReconfigurable {
  // 用于测试可见
  private[log] val metricsGroup = new KafkaMetricsGroup(this.getClass)

  /* 日志清理器配置，可以动态更新 */
  @volatile private var config = initialConfig

  /* 用于管理正在清理的分区状态。包级私有以允许测试访问 */
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs, logDirFailureChannel)

  /* 用于限制所有清理线程I/O到用户指定的最大速率的限流器 */
  private[log] val throttler = new Throttler(config.maxIoBytesPerSecond, 300, "cleaner-io", "bytes", time)

  /* 清理线程数组 */
  private[log] val cleaners = mutable.ArrayBuffer[CleanerThread]()

  /**
   * 计算所有清理线程中某个指标的最大值
   * 
   * @param f 用于计算结果的函数
   * @return 如果有清理线程则返回最大值，否则返回0
   */
  private[log] def maxOverCleanerThreads(f: CleanerThread => Double): Double =
    cleaners.map(f).maxOption.getOrElse(0.0d)

  /* 用于跟踪上次清理中任何线程的缓冲区最大使用率的指标 */
  metricsGroup.newGauge(MaxBufferUtilizationPercentMetricName,
    () => (maxOverCleanerThreads(_.lastStats.bufferUtilization) * 100).toInt)

  /* 用于跟踪每个线程上次清理的重复复制率的指标 
   * 重复复制率 = 写入字节数 / 读取字节数，表示需要重新写入的数据比例
   * 该值越低说明压缩效果越好 */
  metricsGroup.newGauge(CleanerRecopyPercentMetricName, () => {
    val stats = cleaners.map(_.lastStats)  // 获取所有清理线程的最新统计信息
    val recopyRate = stats.iterator.map(_.bytesWritten).sum.toDouble / math.max(stats.iterator.map(_.bytesRead).sum, 1)  // 计算总写入字节与总读取字节的比率
    (100 * recopyRate).toInt  // 转换为百分比
  })

  /* 用于跟踪每个线程上次清理所需最大时间的指标（以秒为单位）*/
  metricsGroup.newGauge(MaxCleanTimeMetricName, () => maxOverCleanerThreads(_.lastStats.elapsedSecs).toInt)

  /* 用于跟踪日志需要压缩的时间（由最大压缩延迟确定）与上次清理运行时间之间的延迟的指标
   * 该指标反映了清理任务的及时性，值越大表示清理越滞后 */
  metricsGroup.newGauge(MaxCompactionDelayMetricsName,
    () => (maxOverCleanerThreads(_.lastPreCleanStats.maxCompactionDelayMs.toDouble) / 1000).toInt)

  /* 用于跟踪失败的清理线程数量的指标 */
  metricsGroup.newGauge(DeadThreadCountMetricName, () => deadThreadCount)

  /**
   * 获取失败的清理线程数量
   * 通过检查每个清理线程的isThreadFailed标志来统计
   */
  private[log] def deadThreadCount: Int = cleaners.count(_.isThreadFailed)

  /**
   * 启动后台清理线程
   * 根据配置的线程数创建并启动相应数量的CleanerThread
   */
  def startup(): Unit = {
    info("Starting the log cleaner")
    (0 until config.numThreads).foreach { i =>  // 遍历创建配置数量的清理线程
      val cleaner = new CleanerThread(i)  // 创建新的清理线程实例
      cleaners += cleaner  // 将清理线程添加到清理线程池中
      cleaner.start()  // 启动清理线程
    }
  }

  /**
   * 停止后台清理线程
   * 关闭所有运行中的清理线程并清空线程池
   */
  private[this] def shutdownCleaners(): Unit = {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())  // 关闭所有清理线程
    cleaners.clear()  // 清空清理线程池
  }

  /**
   * 停止后台清理线程并清理相关资源
   * 确保在关闭清理线程后移除所有相关的度量指标
   */
  def shutdown(): Unit = {
    try {
      shutdownCleaners()  // 关闭所有清理线程
    } finally {
      removeMetrics()  // 移除所有相关的度量指标
    }
  }

  /**
   * 移除所有与日志清理器相关的度量指标
   * 包括清理器自身的指标和清理管理器的指标
   */
  def removeMetrics(): Unit = {
    LogCleaner.MetricNames.foreach(metricsGroup.removeMetric)  // 移除清理器的度量指标
    cleanerManager.removeMetrics()  // 移除清理管理器的度量指标
  }

  /**
   * 获取LogCleaner中可重新配置的配置项集合
   * @return 可重新配置的配置项名称集合
   */
  override def reconfigurableConfigs: Set[String] = {
    LogCleaner.ReconfigurableConfigs
  }

  /**
   * 验证新的清理线程数量是否合理
   * 确保新的线程数量在合理范围内：
   * 1. 至少有1个线程
   * 2. 不能减少到当前值的一半以下
   * 3. 不能增加到当前值的两倍以上
   *
   * @param newConfig 包含新清理器配置的KafkaConfig实例
   */
  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val numThreads = LogCleaner.cleanerConfig(newConfig).numThreads  // 获取新配置中的线程数
    val currentThreads = config.numThreads  // 获取当前的线程数
    if (numThreads < 1)  // 确保至少有1个清理线程
      throw new ConfigException(s"Log cleaner threads should be at least 1")
    if (numThreads < currentThreads / 2)  // 确保新的线程数不少于当前线程数的一半
      throw new ConfigException(s"Log cleaner threads cannot be reduced to less than half the current value $currentThreads")
    if (numThreads > currentThreads * 2)  // 确保新的线程数不超过当前线程数的两倍
      throw new ConfigException(s"Log cleaner threads cannot be increased to more than double the current value $currentThreads")
  }

  /**
   * 重新配置日志清理配置。这将执行以下操作：
   * 1. 如有必要，使用logCleanerIoMaxBytesPerSecond更新Throttler中的desiredRatePerSec
   * 2. 停止当前的日志清理器并创建新的清理器
   * 这确保了即使某些清理器失败，也会创建新的清理器以匹配新的配置。
   *
   * 应用场景：
   * - 动态调整日志清理的I/O限流参数
   * - 在清理器线程失败后进行恢复
   * - 配置变更后重新初始化清理器
   *
   * @param oldConfig 旧的日志清理配置
   * @param newConfig 重新配置的新日志清理配置
   */
  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    // 使用新配置更新清理器配置
    config = LogCleaner.cleanerConfig(newConfig)

    // 获取新的I/O速率限制
    val maxIoBytesPerSecond = config.maxIoBytesPerSecond
    // 如果I/O速率限制发生变化，更新限流器
    if (maxIoBytesPerSecond != oldConfig.logCleanerIoMaxBytesPerSecond) {
      info(s"Updating logCleanerIoMaxBytesPerSecond: $maxIoBytesPerSecond")
      throttler.updateDesiredRatePerSec(maxIoBytesPerSecond)
    }
    // 调用shutdownCleaners()而不是shutdown以避免不必要的度量指标删除
    shutdownCleaners()
    startup()
  }

  /**
   * 中止正在进行的特定分区的清理操作。此调用会阻塞直到分区的清理操作被中止。
   *
   * 应用场景：
   * - 分区被删除时需要停止清理
   * - 分区发生故障需要紧急停止清理
   * - 手动干预清理过程
   *
   * @param topicPartition 要中止清理的主题分区
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    cleanerManager.abortCleaning(topicPartition)
  }

  /**
   * 更新检查点文件以在必要时删除分区。
   *
   * 应用场景：
   * - 分区被删除时需要清理检查点信息
   * - 日志目录结构变更时更新检查点
   * - 维护检查点文件的一致性
   *
   * @param dataDir 需要更新的数据目录
   * @param partitionToRemove 需要删除的主题分区，默认为None
   */
  def updateCheckpoints(dataDir: File, partitionToRemove: Option[TopicPartition] = None): Unit = {
    cleanerManager.updateCheckpoints(dataDir, partitionToRemove = partitionToRemove)
  }

  /**
   * 修改主题分区的检查点目录，删除sourceLogDir中的数据，并在destLogDir中添加数据。
   * 通常发生在磁盘平衡结束并用未来文件替换先前文件时。
   *
   * 应用场景：
   * - 日志目录迁移
   * - 磁盘间数据平衡
   * - 存储空间重组织
   *
   * @param topicPartition 要修改检查点的主题分区
   * @param sourceLogDir 要删除检查点的源日志目录
   * @param destLogDir 要添加检查点的目标日志目录
   */
  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir)
  }

  /**
   * 在处理日志目录故障时停止清理指定目录中的日志。
   *
   * 应用场景：
   * - 磁盘故障处理
   * - 存储设备维护
   * - 紧急情况下的日志管理
   *
   * @param dir 日志目录的绝对路径
   */
  def handleLogDirFailure(dir: String): Unit = {
    cleanerManager.handleLogDirFailure(dir)
  }

  /**
   * 如果给定分区的检查点偏移量大于给定偏移量，则截断该分区的清理器偏移量检查点。
   *
   * 应用场景：
   * - 日志截断后的检查点调整
   * - 数据一致性维护
   * - 故障恢复过程中的检查点修正
   *
   * @param dataDir 需要截断的数据目录
   * @param topicPartition 要截断检查点偏移量的主题分区
   * @param offset 用于比较的给定偏移量
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long): Unit = {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset)
  }

  /**
   * 中止正在进行的特定分区的清理操作，并暂停该分区的未来清理操作。
   * 此调用会阻塞直到分区的清理操作被中止并暂停。
   *
   * 应用场景：
   * - 分区维护期间暂停清理
   * - 问题诊断时暂停清理
   * - 手动控制清理进度
   *
   * @param topicPartition 要中止和暂停清理的主题分区
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    cleanerManager.abortAndPauseCleaning(topicPartition)
  }

  /**
   * 恢复已暂停分区的清理操作。
   *
   * 应用场景：
   * - 维护操作完成后恢复清理
   * - 问题修复后重新启动清理
   * - 手动恢复清理进程
   *
   * @param topicPartitions 要恢复清理的主题分区集合
   */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]): Unit = {
    cleanerManager.resumeCleaning(topicPartitions)
  }

  /**
   * 用于测试目的，等待日志清理工作完成。此方法会等待直到清理器处理完指定主题分区的给定偏移量。
   * 
   * 应用场景：
   * - 集成测试中验证日志清理完成情况
   * - 调试日志清理过程
   * - 确保清理操作在超时前完成
   *
   * @param topicPartition 需要清理的主题分区
   * @param offset 清理器不需要清理的第一个脏偏移量
   * @param maxWaitMs 等待清理器的最大时间（毫秒）
   *
   * @return 在超时之前工作是否完成
   */
  def awaitCleaned(topicPartition: TopicPartition, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    // 检查给定主题分区的清理检查点是否已经达到或超过目标偏移量
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    // 循环等待直到清理完成或超时
    while (!isCleaned && remainingWaitMs > 0) {
      // 每次最多休眠100ms，避免过长时间无法响应
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }

  /**
   * 为防止日志保留（retention）和压缩（compaction）之间的竞争条件，
   * 保留线程需要调用此方法以获取可以安全工作的日志分区列表。
   * 
   * 应用场景：
   * - 在执行日志保留操作前确保不会与压缩操作冲突
   * - 协调多个线程对同一分区的操作
   * - 避免数据不一致性问题
   *
   * @return 保留线程可以安全操作的日志分区列表
   */
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, UnifiedLog)] = {
    cleanerManager.pauseCleaningForNonCompactedPartitions()
  }

  // 仅用于测试：获取当前的清理器配置
  private[kafka] def currentConfig: CleanerConfig = config

  // 仅用于测试：获取清理线程数量
  private[log] def cleanerCount: Int = cleaners.size

  /**
   * 清理线程类，执行实际的日志清理工作。每个线程重复执行清理过程：
   * 选择最脏的日志进行清理，然后将清理后的段替换到日志中。
   * 
   * 设计考虑：
   * 1. 多线程并行清理提高效率
   * 2. 通过线程ID区分不同清理线程
   * 3. 实现可关闭接口便于管理线程生命周期
   * 4. 使用日志记录功能跟踪清理过程
   */
  private[log] class CleanerThread(threadId: Int)
    extends ShutdownableThread(s"kafka-log-cleaner-thread-$threadId", false) with Logging {
    // 获取日志记录器名称
    protected override def loggerName: String = classOf[LogCleaner].getName

    // 设置日志标识符
    this.logIdent = logPrefix

    // 检查每个清理线程的去重缓冲区大小是否超过2GB限制
    if (config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    // 创建清理器实例，配置必要的参数
    val cleaner = new Cleaner(id = threadId,
                              // 创建偏移量映射，用于记录消息键的最新偏移量
                              offsetMap = new SkimpyOffsetMap(math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt,
                                                              config.hashAlgorithm),
                              // 设置IO缓冲区大小，用于读写日志段
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              // 设置去重缓冲区负载因子
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              // 配置限流器控制IO速率
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)

    // 用于记录最近一次清理的统计信息
    @volatile var lastStats: CleanerStats = new CleanerStats()
    // 用于记录清理前的统计信息
    @volatile var lastPreCleanStats: PreCleanStats = new PreCleanStats()

    /**
     * 检查分区的清理是否被中止。如果被中止，则抛出异常。
     * 
     * 应用场景：
     * - 分区被删除时及时停止清理
     * - 处理清理过程中的异常情况
     * - 响应手动中止清理的请求
     *
     * @param topicPartition 要检查的主题分区
     */
    private def checkDone(topicPartition: TopicPartition): Unit = {
      // 如果线程不在运行状态，抛出线程关闭异常
      if (!isRunning)
        throw new ThreadShutdownException
      // 检查清理是否被中止
      cleanerManager.checkCleaningAborted(topicPartition)
    }

    /**
     * 清理线程的主循环
     * 如果有脏日志可用则进行清理，否则休眠一段时间
     * 
     * 实现细节：
     * 1. 尝试清理最脏的日志
     * 2. 如果没有清理工作，则进行短暂休眠
     * 3. 维护不可清理分区的状态
     */
    override def doWork(): Unit = {
      // 尝试清理最脏的日志
      val cleaned = tryCleanFilthiestLog()
      // 如果没有清理任何日志，则休眠一段时间
      if (!cleaned)
        pause(config.backoffMs, TimeUnit.MILLISECONDS)

      // 维护不可清理分区的状态
      cleanerManager.maintainUncleanablePartitions()
    }

    /**
     * 如果有脏日志可用，则清理该日志
     * 
     * 实现细节：
     * 1. 尝试执行清理操作
     * 2. 捕获并处理清理异常
     * 3. 将发生异常的分区标记为不可清理
     *
     * @return 是否清理了日志
     */
    private def tryCleanFilthiestLog(): Boolean = {
      try {
        // 尝试清理最脏的日志
        cleanFilthiestLog()
      } catch {
        case e: LogCleaningException =>
          // 记录警告日志并将分区标记为不可清理
          warn(s"Unexpected exception thrown when cleaning log ${e.log}. Marking its partition (${e.log.topicPartition}) as uncleanable", e)
          cleanerManager.markPartitionUncleanable(e.log.parentDir, e.log.topicPartition)

          false
      }
    }

    /**
     * 清理最脏的日志分区。这是日志清理器的核心方法，负责选择和清理最需要压缩的日志。
     * 
     * 实现细节：
     * 1. 获取预清理统计信息
     * 2. 选择最脏的压缩日志进行清理
     * 3. 执行日志清理操作
     * 4. 删除可删除的旧日志段
     * 
     * 应用场景：
     * - 定期压缩日志以节省存储空间
     * - 维护日志的整洁度
     * - 删除过期的日志段
     * 
     * @throws LogCleaningException 当清理过程中发生错误时抛出
     * @return 如果成功清理了日志则返回true，否则返回false
     */
    @throws(classOf[LogCleaningException])
    private def cleanFilthiestLog(): Boolean = {
      // 创建预清理统计对象，用于收集清理前的统计信息
      val preCleanStats = new PreCleanStats()
      // 获取最脏的需要压缩的日志
      val ltc = cleanerManager.grabFilthiestCompactedLog(time, preCleanStats)
      val cleaned = ltc match {
        case None =>
          // 没有需要清理的日志
          false
        case Some(cleanable) =>
          // 找到需要清理的日志，执行清理操作
          this.lastPreCleanStats = preCleanStats
          try {
            cleanLog(cleanable)
            true
          } catch {
            // 对于线程关闭和控制流异常直接抛出
            case e @ (_: ThreadShutdownException | _: ControlThrowable) => throw e
            // 其他异常包装为LogCleaningException
            case e: Exception => throw new LogCleaningException(cleanable.log, e.getMessage, e)
          }
      }
      // 获取可以删除的日志列表
      val deletable: Iterable[(TopicPartition, UnifiedLog)] = cleanerManager.deletableLogs()
      try {
        // 遍历删除每个可删除的日志段
        deletable.foreach { case (_, log) =>
          try {
            log.deleteOldSegments()
          } catch {
            case e @ (_: ThreadShutdownException | _: ControlThrowable) => throw e
            case e: Exception => throw new LogCleaningException(log, e.getMessage, e)
          }
        }
      } finally  {
        // 通知清理管理器删除操作已完成
        cleanerManager.doneDeleting(deletable.map(_._1))
      }

      cleaned
    }

    /**
     * 清理单个日志分区。执行实际的日志清理操作，包括压缩和删除过期消息。
     * 
     * 实现细节：
     * 1. 记录起始偏移量
     * 2. 执行日志清理
     * 3. 更新清理统计信息
     * 4. 处理各种异常情况
     * 
     * 应用场景：
     * - 压缩日志数据
     * - 删除重复的消息
     * - 处理存储异常
     * 
     * @param cleanable 要清理的日志对象
     */
    private def cleanLog(cleanable: LogToClean): Unit = {
      // 获取第一个脏偏移量作为起始点
      val startOffset = cleanable.firstDirtyOffset
      var endOffset = startOffset
      try {
        // 执行实际的清理操作，获取下一个脏偏移量和清理统计信息
        val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
        endOffset = nextDirtyOffset
        // 记录清理统计信息
        recordStats(cleaner.id, cleanable.log.name, startOffset, endOffset, cleanerStats)
      } catch {
        case _: LogCleaningAbortedException => // 清理任务被中止，允许继续
        case _: KafkaStorageException => // 分区已离线，允许继续
        case e: IOException =>
          // 处理IO异常，可能导致日志目录离线
          val logDirectory = cleanable.log.parentDir
          val msg = s"Failed to clean up log for ${cleanable.topicPartition} in dir $logDirectory due to IOException"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirectory, msg, e)
      } finally {
        // 通知清理管理器清理操作已完成
        cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.parentDirFile, endOffset)
      }
    }

    /**
     * 记录单次清理操作的统计信息。包括处理的数据量、耗时、压缩率等详细指标。
     * 
     * 实现细节：
     * 1. 更新最新统计信息
     * 2. 计算各项性能指标
     * 3. 格式化并输出统计信息
     * 4. 记录特殊情况（如延迟分区、无效消息）
     * 
     * 应用场景：
     * - 监控清理性能
     * - 评估压缩效果
     * - 诊断清理问题
     * 
     * @param id 清理线程ID
     * @param name 已清理的日志名称
     * @param from 清理的起始偏移量
     * @param to 清理的结束偏移量
     * @param stats 清理统计信息
     */
    private def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats): Unit = {
      // 更新最新的统计信息
      this.lastStats = stats
      // 定义MB转换函数
      def mb(bytes: Double) = bytes / (1024*1024)
      
      // 构建详细的统计信息消息
      val message =
        // 清理的日志范围信息
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) +
        // 处理速度统计
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead.toDouble),
                                                                                stats.elapsedSecs,
                                                                                mb(stats.bytesRead.toDouble / stats.elapsedSecs)) +
        // 索引构建统计
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead.toDouble),
                                                                                           stats.elapsedIndexSecs,
                                                                                           mb(stats.mapBytesRead.toDouble) / stats.elapsedIndexSecs,
                                                                                           100 * stats.elapsedIndexSecs / stats.elapsedSecs) +
        // 缓冲区使用率
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        // 清理速度统计
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead.toDouble),
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs,
                                                                                           mb(stats.bytesRead.toDouble) / (stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs) / stats.elapsedSecs) +
        // 清理前后的大小对比
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead.toDouble), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten.toDouble), stats.messagesWritten) +
        // 压缩率统计
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead),
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      // 输出基本统计信息
      info(message)
      
      // 如果有延迟的分区，输出延迟统计信息
      if (lastPreCleanStats.delayedPartitions > 0) {
        info("\tCleanable partitions: %d, Delayed partitions: %d, max delay: %d".format(lastPreCleanStats.cleanablePartitions, lastPreCleanStats.delayedPartitions, lastPreCleanStats.maxCompactionDelayMs))
      }
      
      // 如果发现无效消息，输出警告信息
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }

  }
}

/**
 * 日志清理器对象，包含清理器的配置项和性能指标定义
 */
object LogCleaner {
  /**
   * 可动态重新配置的配置项集合，包括：
   * - 清理线程数
   * - 去重缓冲区大小
   * - 去重缓冲区负载因子
   * - IO缓冲区大小
   * - 消息最大字节数
   * - 清理器IO最大字节速率
   * - 清理器回退时间
   */
  val ReconfigurableConfigs: Set[String] = Set(
    CleanerConfig.LOG_CLEANER_THREADS_PROP,          // 清理线程数
    CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP,    // 去重缓冲区大小
    CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP,  // 去重缓冲区负载因子
    CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP,    // IO缓冲区大小
    ServerConfigs.MESSAGE_MAX_BYTES_CONFIG,         // 消息最大字节数
    CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP,  // 清理器IO最大字节速率
    CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP      // 清理器回退时间
  )

  /**
   * 根据Kafka配置创建清理器配置
   * @param config Kafka配置对象
   * @return 清理器配置实例
   */
  def cleanerConfig(config: KafkaConfig): CleanerConfig = {
    new CleanerConfig(config.logCleanerThreads,
      config.logCleanerDedupeBufferSize,
      config.logCleanerDedupeBufferLoadFactor,
      config.logCleanerIoBufferSize,
      config.messageMaxBytes,
      config.logCleanerIoMaxBytesPerSecond,
      config.logCleanerBackoffMs,
      config.logCleanerEnable)
  }

  // 性能指标名称定义
  // 用于测试可见
  private[log] val MaxBufferUtilizationPercentMetricName = "max-buffer-utilization-percent"  // 最大缓冲区使用率百分比
  private val CleanerRecopyPercentMetricName = "cleaner-recopy-percent"  // 清理器重复复制百分比
  private[log] val MaxCleanTimeMetricName = "max-clean-time-secs"  // 最大清理时间（秒）
  private[log] val MaxCompactionDelayMetricsName = "max-compaction-delay-secs"  // 最大压缩延迟时间（秒）
  private val DeadThreadCountMetricName = "DeadThreadCount"  // 死亡线程计数
  
  // 包私有，用于测试
  private[log] val MetricNames = Set(
    MaxBufferUtilizationPercentMetricName,
    CleanerRecopyPercentMetricName,
    MaxCleanTimeMetricName,
    MaxCompactionDelayMetricsName,
    DeadThreadCountMetricName)
}

/**
 * 该类包含了实际的日志清理逻辑
 * 设计考虑：
 * 1. 使用偏移量映射进行消息去重
 * 2. 通过读写缓冲区优化IO性能
 * 3. 支持限流控制避免清理影响系统性能
 * 4. 提供清理进度检查机制
 * 
 * @param id 用于日志记录的标识符
 * @param offsetMap 用于消息去重的偏移量映射
 * @param ioBufferSize 缓冲区大小。由于有读写两个缓冲区，实际内存使用量是这个值的2倍
 * @param maxIoBufferSize 日志中单条消息的最大大小
 * @param dupBufferLoadFactor 去重缓冲区的最大填充率
 * @param throttler 用于限制IO速率的限流器实例
 * @param time 时间实例
 * @param checkDone 检查分区清理是否完成或中止的函数
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: TopicPartition => Unit) extends Logging {

  // 获取日志记录器名称
  protected override def loggerName: String = classOf[LogCleaner].getName

  // 设置日志标识符
  this.logIdent = s"Cleaner $id: "

  /* 用于读操作的IO缓冲区 */
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)

  /* 用于写操作的IO缓冲区 */
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  /* 用于解压缩的缓冲区供应器 */
  private val decompressionBufferSupplier = BufferSupplier.create()

  // 确保偏移量映射足够大，能容纳至少一条消息
  require(offsetMap.slots * dupBufferLoadFactor > 1, "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads")

  /**
   * 清理指定的日志
   * 
   * 应用场景：
   * - 定期压缩日志减少存储空间
   * - 删除过期的消息
   * - 合并重复的消息键
   *
   * @param cleanable 需要清理的日志
   * @return (第一个未清理的偏移量, 本轮清理的统计信息)
   */
  private[log] def clean(cleanable: LogToClean): (Long, CleanerStats) = {
    doClean(cleanable, time.milliseconds())
  }

  /**
   * 执行日志清理的具体逻辑
   * 
   * 实现步骤：
   * 1. 确定删除墓碑记录的时间界限
   * 2. 构建偏移量映射表
   * 3. 确定清理的时间范围
   * 4. 按大小分组并清理日志段
   * 5. 记录缓冲区使用率
   *
   * @param cleanable 需要清理的日志
   * @param currentTime 执行清理的当前时间戳
   * @return (第一个未清理的偏移量, 本轮清理的统计信息)
   */
  private[log] def doClean(cleanable: LogToClean, currentTime: Long): (Long, CleanerStats) = {
    // 记录开始清理日志的信息
    info("Beginning cleaning of log %s".format(cleanable.log.name))

    // 计算可以安全删除墓碑记录的时间戳
    // 该时间点定义为最后一个已清理段的最后修改时间减去配置的保留时间
    // 此时间戳仅用于早于MAGIC_VALUE_V2的旧消息格式
    val legacyDeleteHorizonMs =
      cleanable.log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - cleanable.log.config.deleteRetentionMs
      }

    val log = cleanable.log
    val stats = new CleanerStats()

    // 构建偏移量映射表
    info("Building offset map for %s...".format(cleanable.log.name))
    val upperBoundOffset = cleanable.firstUncleanableOffset
    buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats)
    val endOffset = offsetMap.latestOffset + 1
    stats.indexDone()

    // 确定日志清理的时间范围
    // 取最后活动段和压缩延迟时间的较小值
    val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)

    // 按大小分组并清理日志段
    info("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to upper bound deletion horizon %s)...".format(log.name, new Date(cleanableHorizonMs), new Date(legacyDeleteHorizonMs)))
    val transactionMetadata = new CleanedTransactionMetadata

    // 将日志段按大小分组并进行清理
    val groupedSegments = groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize,
      log.config.maxIndexSize, cleanable.firstUncleanableOffset)
    for (group <- groupedSegments)
      cleanSegments(log, group, offsetMap, currentTime, stats, transactionMetadata, legacyDeleteHorizonMs, upperBoundOffset)

    // 记录缓冲区使用率
    stats.bufferUtilization = offsetMap.utilization

    // 完成所有清理工作
    stats.allDone()

    (endOffset, stats)
  }

  /**
   * 将一组日志段清理并合并成一个新的替换段
   * 
   * 设计考虑：
   * 1. 事务完整性：确保事务相关的元数据和标记被正确处理和保留
   * 2. 数据一致性：通过原子性的段替换操作保证日志一致性
   * 3. 错误处理：优雅处理段溢出等异常情况
   * 4. 资源管理：确保在发生异常时正确清理临时资源
   *
   * 应用场景：
   * - 日志压缩：合并具有相同key的多个记录，只保留最新值
   * - 空间回收：删除过期的事务标记和墓碑记录
   * - 日志维护：处理日志段溢出等异常情况
   *
   * @param log 需要清理的日志
   * @param segments 需要清理的日志段组
   * @param map 用于清理段的偏移量映射
   * @param currentTime 当前时间（毫秒）
   * @param stats 清理统计信息收集器
   * @param transactionMetadata 在清理分组段之间传递的正在进行的事务状态
   * @param legacyDeleteHorizonMs 用于版本低于2的墓碑记录的删除时间界限
   * @param upperBoundOffsetOfCleaningRound 本轮清理的上界偏移量
   */
  private[log] def cleanSegments(log: UnifiedLog,
                                 segments: Seq[LogSegment],
                                 map: OffsetMap,
                                 currentTime: Long,
                                 stats: CleanerStats,
                                 transactionMetadata: CleanedTransactionMetadata,
                                 legacyDeleteHorizonMs: Long,
                                 upperBoundOffsetOfCleaningRound: Long): Unit = {
    // 创建一个新的段，其名称和索引都带有后缀
    val cleaned = UnifiedLog.createNewCleanedSegment(log.dir, log.config, segments.head.baseOffset)
    // 设置事务索引，用于跟踪事务状态
    transactionMetadata.cleanedIndex = Some(cleaned.txnIndex)

    try {
      // 将段清理到新的目标段中
      val iter = segments.iterator
      var currentSegmentOpt: Option[LogSegment] = Some(iter.next())
      // 获取活跃生产者的最后偏移量，用于保留生产者状态
      val lastOffsetOfActiveProducers = log.lastRecordsOfActiveProducers

      while (currentSegmentOpt.isDefined) {
        val currentSegment = currentSegmentOpt.get
        val nextSegmentOpt = if (iter.hasNext) Some(iter.next()) else None

        // 收集完整日志段范围内的已中止事务
        // 这对于重建新段的完整事务索引很重要
        val startOffset = currentSegment.baseOffset
        val upperBoundOffset = nextSegmentOpt.map(_.baseOffset).getOrElse(currentSegment.readNextOffset)
        val abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset)
        transactionMetadata.addAbortedTransactions(abortedTransactions)

        // 根据段的最后修改时间决定是否保留旧的删除标记和事务标记
        val retainLegacyDeletesAndTxnMarkers = currentSegment.lastModified > legacyDeleteHorizonMs
        info(s"Cleaning $currentSegment in log ${log.name} into ${cleaned.baseOffset} " +
          s"with an upper bound deletion horizon $legacyDeleteHorizonMs computed from " +
          s"the segment last modified time of ${currentSegment.lastModified}," +
          s"${if(retainLegacyDeletesAndTxnMarkers) "retaining" else "discarding"} deletes.")

        try {
          // 执行实际的清理操作，将当前段的内容清理并写入新段
          cleanInto(log.topicPartition, currentSegment.log, cleaned, map, retainLegacyDeletesAndTxnMarkers, log.config.deleteRetentionMs,
            log.config.maxMessageSize, transactionMetadata, lastOffsetOfActiveProducers,
            upperBoundOffsetOfCleaningRound, stats, currentTime = currentTime)
        } catch {
          case e: LogSegmentOffsetOverflowException =>
            // 如果发生段溢出，拆分当前段并中止清理过程
            // 等待拆分完成后重试整个清理过程
            info(s"Caught segment overflow error during cleaning: ${e.getMessage}")
            log.splitOverflowedSegment(currentSegment)
            throw new LogCleaningAbortedException()
        }
        currentSegmentOpt = nextSegmentOpt
      }

      // 将新段标记为非活动状态
      cleaned.onBecomeInactiveSegment()
      // 在交换之前将新段刷新到磁盘
      cleaned.flush()

      // 更新修改日期以保留原始文件的最后修改日期
      val modified = segments.last.lastModified
      cleaned.setLastModified(modified)

      // 将新段替换到日志中
      info(s"Swapping in cleaned segment $cleaned for segment(s) $segments in log $log")
      log.replaceSegments(List(cleaned), segments)
    } catch {
      case e: LogCleaningAbortedException =>
        // 如果清理过程被中止，删除临时创建的清理段
        try cleaned.deleteIfExists()
        catch {
          case deleteException: Exception =>
            e.addSuppressed(deleteException)
        } finally throw e
    }
  }

  /**
   * 使用提供的key=>offset映射将给定的源日志段清理到目标段中
   * 
   * 设计考虑：
   * 1. 批次保留策略：根据不同场景决定批次的保留方式
   * 2. 记录过滤机制：通过多层过滤确定哪些记录需要保留
   * 3. 生产者状态维护：保留必要的生产者元数据确保幂等性
   * 4. 事务完整性：确保事务标记的正确处理
   *
   * 应用场景：
   * - 日志压缩：保留每个key的最新值
   * - 生产者状态维护：保留活跃生产者的最后记录
   * - 事务处理：处理事务标记和已中止的事务
   * - 墓碑处理：根据保留时间处理删除标记
   *
   * @param topicPartition 要清理的日志段的主题和分区
   * @param sourceRecords 脏日志段
   * @param dest 清理后的日志段
   * @param map key=>offset映射
   * @param retainLegacyDeletesAndTxnMarkers 清理此段时是否应保留墓碑（版本低于2）和标记
   * @param deleteRetentionMs 日志配置中定义的墓碑保留时间
   * @param maxLogMessageSize 对应主题的最大消息大小
   * @param transactionMetadata 在分组段清理之间传递的正在进行的事务状态
   * @param lastRecordsOfActiveProducers 活跃生产者及其最后数据偏移量
   * @param upperBoundOffsetOfCleaningRound 源段中最后一个批次的下一个偏移量
   * @param stats 清理统计信息收集器
   * @param currentTime 清理开始的时间
   */
  private[log] def cleanInto(topicPartition: TopicPartition,
                             sourceRecords: FileRecords,
                             dest: LogSegment,
                             map: OffsetMap,
                             retainLegacyDeletesAndTxnMarkers: Boolean,
                             deleteRetentionMs: Long,
                             maxLogMessageSize: Int,
                             transactionMetadata: CleanedTransactionMetadata,
                             lastRecordsOfActiveProducers: mutable.Map[Long, LastRecord],
                             upperBoundOffsetOfCleaningRound: Long,
                             stats: CleanerStats,
                             currentTime: Long): Unit = {
    val logCleanerFilter: RecordFilter = new RecordFilter(currentTime, deleteRetentionMs) {
      // 标记是否丢弃批次中的所有记录
      var discardBatchRecords: Boolean = _

      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult = {
        // 利用墓碑保留逻辑来延迟删除事务标记
        // 注意：在事务中的所有记录被删除之前，我们不会删除其标记
        val canDiscardBatch = shouldDiscardBatch(batch, transactionMetadata)

        // 根据批次类型和删除时间确定是否丢弃记录
        if (batch.isControlBatch)
          // 对于控制批次，只有在超过删除时间时才丢弃
          discardBatchRecords = canDiscardBatch && batch.deleteHorizonMs().isPresent && batch.deleteHorizonMs().getAsLong <= this.currentTime
        else
          discardBatchRecords = canDiscardBatch

        def isBatchLastRecordOfProducer: Boolean = {
          // 为了保留活跃生产者的状态，我们需要保留批次。有三种情况：
          // 1) 生产者不再活跃，可以删除该生产者的所有记录
          // 2) 生产者仍然活跃且有最后数据偏移量，保留包含此偏移量的批次，因为它包含生产者的最后序列号
          // 3) 日志中的最后一条记录是事务标记，保留此标记因为它包含确保隔离所需的最后生产者纪元
          lastRecordsOfActiveProducers.get(batch.producerId).exists { lastRecord =>
            if (lastRecord.lastDataOffset.isPresent) {
              batch.lastOffset == lastRecord.lastDataOffset.getAsLong
            } else {
              batch.isControlBatch && batch.producerEpoch == lastRecord.producerEpoch
            }
          }
        }

        // 确定批次的保留策略
        val batchRetention: BatchRetention =
          if (batch.hasProducerId && isBatchLastRecordOfProducer)
            // 保留活跃生产者的最后批次，即使它是空的
            BatchRetention.RETAIN_EMPTY
          else if (batch.nextOffset == upperBoundOffsetOfCleaningRound) {
            // 保留清理轮次的最后一个批次，即使它是空的
            // 这样可以确保清理后不会丢失最后偏移量信息
            BatchRetention.RETAIN_EMPTY
          } else if (discardBatchRecords)
            // 完全删除批次
            BatchRetention.DELETE
          else
            // 只删除空批次
            BatchRetention.DELETE_EMPTY
        new RecordFilter.BatchRetentionResult(batchRetention, canDiscardBatch && batch.isControlBatch)
      }

      override def shouldRetainRecord(batch: RecordBatch, record: Record): Boolean = {
        if (discardBatchRecords)
          // 批次仅为保留生产者序列信息而保留，其中的记录可以删除
          false
        else if (batch.isControlBatch)
          // 控制批次中的记录总是保留
          true
        else
          // 使用清理器的记录保留逻辑决定是否保留记录
          Cleaner.this.shouldRetainRecord(map, retainLegacyDeletesAndTxnMarkers, batch, record, stats, currentTime = this.currentTime)
      }
    }

    // 初始化读取位置为0
    var position = 0
    // 循环处理源记录中的所有消息批次，直到处理完所有字节
    while (position < sourceRecords.sizeInBytes) {
      // 检查清理任务是否被中止
      checkDone(topicPartition)
      // 清空读写缓冲区，准备读取新的消息批次
      readBuffer.clear()
      writeBuffer.clear()

      // 从源记录中读取一块数据到读缓冲区
      sourceRecords.readInto(readBuffer, position)
      // 将读缓冲区中的数据转换为可读的内存记录
      val records = MemoryRecords.readableRecords(readBuffer)
      // 根据记录大小进行限流控制
      throttler.maybeThrottle(records.sizeInBytes)
      // 使用日志清理过滤器过滤记录，将需要保留的记录写入写缓冲区
      val result = records.filterTo(logCleanerFilter, writeBuffer, decompressionBufferSupplier)

      // 更新统计信息：已读消息数和字节数
      stats.readMessages(result.messagesRead, result.bytesRead)
      // 更新统计信息：重新复制的消息数和字节数
      stats.recopyMessages(result.messagesRetained, result.bytesRetained)

      // 更新读取位置
      position += result.bytesRead

      // 如果有需要保留的消息，将它们写入目标日志段
      val outputBuffer = result.outputBuffer
      if (outputBuffer.position() > 0) {
        // 准备输出缓冲区用于读取
        outputBuffer.flip()
        val retained = MemoryRecords.readableRecords(outputBuffer)
        // 此处不需要持有日志锁，因为这个段只会在Log.replaceSegments（获取锁）之后被其他线程访问
        dest.append(result.maxOffset, retained)
        // 根据写入量进行限流控制
        throttler.maybeThrottle(outputBuffer.limit())
      }

      // 如果读取了字节但没有获得完整的批次，说明I/O缓冲区太小，需要扩大并重试
      // result.bytesRead包含了已读消息和被丢弃批次的字节数
      if (readBuffer.limit() > 0 && result.bytesRead == 0)
        growBuffersOrFail(sourceRecords, position, maxLogMessageSize, records)
    }
    // 恢复缓冲区到初始状态
    restoreBuffers()
  }


  /**
   * 扩大缓冲区以处理下一批记录。缓冲区大小会翻倍，直到达到maxLogMessageSize。
   * 在某些情况下，记录可能大于日志配置的当前最大大小，例如：
   *   1. 使用压缩的压缩主题可能包含略大于max.message.bytes的消息集
   *   2. 主题的max.message.bytes可能在写入较大消息后被减小
   * 在这些情况下，扩大缓冲区以容纳下一个批次。
   *
   * 设计考虑：
   * - 动态调整缓冲区大小以适应不同大小的消息批次
   * - 处理异常情况下的消息大小变化
   * - 确保数据完整性和处理效率
   *
   * @param sourceRecords 要处理的脏日志段记录
   * @param position 读缓冲区中的当前位置
   * @param maxLogMessageSize 主题的最大记录大小（字节）
   * @param memoryRecords 读缓冲区中的内存记录
   */
  private def growBuffersOrFail(sourceRecords: FileRecords,
                                position: Int,
                                maxLogMessageSize: Int,
                                memoryRecords: MemoryRecords): Unit = {

    // 确定新的缓冲区大小
    val maxSize = if (readBuffer.capacity >= maxLogMessageSize) {
      // 获取下一个批次的大小
      val nextBatchSize = memoryRecords.firstBatchSize
      val logDesc = s"log segment ${sourceRecords.file} at position $position"
      
      // 进行各种有效性检查
      if (nextBatchSize == null)
        throw new IllegalStateException(s"Could not determine next batch size for $logDesc")
      if (nextBatchSize <= 0)
        throw new IllegalStateException(s"Invalid batch size $nextBatchSize for $logDesc")
      if (nextBatchSize <= readBuffer.capacity)
        throw new IllegalStateException(s"Batch size $nextBatchSize < buffer size ${readBuffer.capacity}, but not processed for $logDesc")
      
      // 检查剩余字节数是否足够
      val bytesLeft = sourceRecords.channel.size - position
      if (nextBatchSize > bytesLeft)
        throw new CorruptRecordException(s"Log segment may be corrupt, batch size $nextBatchSize > $bytesLeft bytes left in segment for $logDesc")
      
      nextBatchSize.intValue
    } else
      maxLogMessageSize

    // 使用新的大小扩展缓冲区
    growBuffers(maxSize)
  }

  /**
   * 检查是否应该根据已清理的事务状态丢弃批次
   *
   * 设计考虑：
   * - 维护事务完整性
   * - 区分控制批次和普通批次的处理
   * - 确保事务状态一致性
   *
   * @param batch 要检查的记录批次
   * @param transactionMetadata 关于清理的事务状态元数据
   * @return 如果批次可以被丢弃则返回true
   */
  private def shouldDiscardBatch(batch: RecordBatch,
                                 transactionMetadata: CleanedTransactionMetadata): Boolean = {
    // 如果是控制批次，调用控制批次处理逻辑
    if (batch.isControlBatch)
      transactionMetadata.onControlBatchRead(batch)
    else
      // 否则调用普通批次处理逻辑
      transactionMetadata.onBatchRead(batch)
  }

  /**
   * 检查是否应该保留记录
   *
   * 设计考虑：
   * - 确保每个键只保留最新的记录
   * - 处理删除标记（墓碑）的特殊情况
   * - 兼容不同版本的记录格式
   * - 维护清理统计信息
   *
   * @param map 用于清理段的偏移量映射（键=>偏移量）
   * @param retainDeletesForLegacyRecords 在清理此段时是否保留旧版本（低于版本2）的墓碑和标记
   * @param batch 记录所属的批次
   * @param record 要检查的记录
   * @param stats 清理统计信息收集器
   * @param currentTime 当前时间，用于判断非旧版本记录时与批次的删除时限进行比较
   * @return 如果应该保留记录则返回true
   */
  private def shouldRetainRecord(map: OffsetMap,
                                 retainDeletesForLegacyRecords: Boolean,
                                 batch: RecordBatch,
                                 record: Record,
                                 stats: CleanerStats,
                                 currentTime: Long): Boolean = {
    // 检查记录的偏移量是否超过了映射中的最新偏移量
    val pastLatestOffset = record.offset > map.latestOffset
    if (pastLatestOffset)
      return true

    // 只处理有键的记录
    if (record.hasKey) {
      val key = record.key
      // 获取该键在映射中的偏移量
      val foundOffset = map.get(key)
      /* 保留记录的两个条件：
       * 1. 记录必须是该键的最新偏移量
       * 2. 记录满足以下任一条件：
       *    a) 记录有值
       *    b) 记录没有值（删除标记）但现在还不能删除
       */
      val latestOffsetForKey = record.offset() >= foundOffset
      // 判断是否是旧版本记录
      val legacyRecord = batch.magic() < RecordBatch.MAGIC_VALUE_V2
      // 确定是否应该保留删除标记
      def shouldRetainDeletes = {
        if (!legacyRecord)
          // 对于新版本记录，检查删除时限
          !batch.deleteHorizonMs().isPresent || currentTime < batch.deleteHorizonMs().getAsLong
        else
          // 对于旧版本记录，使用配置的保留策略
          retainDeletesForLegacyRecords
      }
      // 判断记录值是否应该保留
      val isRetainedValue = record.hasValue || shouldRetainDeletes
      // 同时满足最新偏移量和值保留条件
      latestOffsetForKey && isRetainedValue
    } else {
      // 没有键的记录视为无效消息
      stats.invalidMessage()
      false
    }
  }
  }

  /**
   * 将I/O缓冲区容量翻倍
   * 
   * 应用场景：
   * - 处理大消息时动态扩展缓冲区
   * - 优化日志清理性能
   * - 处理突发的大数据量
   * 
   * 设计考虑：
   * 1. 动态调整缓冲区大小以适应不同大小的消息
   * 2. 防止内存溢出，设置最大限制
   * 3. 日志记录缓冲区变化，便于监控和调试
   *
   * @param maxLogMessageSize 允许的最大记录大小（字节）
   */
  private def growBuffers(maxLogMessageSize: Int): Unit = {
    // 计算允许的最大缓冲区大小，取maxLogMessageSize和maxIoBufferSize中的较大值
    val maxBufferSize = math.max(maxLogMessageSize, maxIoBufferSize)
    // 如果当前缓冲区已达到或超过最大允许大小，抛出异常
    if (readBuffer.capacity >= maxBufferSize || writeBuffer.capacity >= maxBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxBufferSize))
    // 计算新的缓冲区大小，取当前大小的2倍和最大允许大小中的较小值
    val newSize = math.min(this.readBuffer.capacity * 2, maxBufferSize)
    // 记录缓冲区大小变化的日志
    info(s"Growing cleaner I/O buffers from ${readBuffer.capacity} bytes to $newSize bytes.")
    // 重新分配读写缓冲区
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }

  /**
   * 将I/O缓冲区容量恢复到原始大小
   * 
   * 应用场景：
   * - 清理完成后释放多余内存
   * - 系统资源紧张时主动收缩缓冲区
   * - 定期维护时重置缓冲区大小
   * 
   * 设计考虑：
   * 1. 内存资源管理
   * 2. 避免长期占用过多内存
   * 3. 只在必要时调整缓冲区大小
   */
  private def restoreBuffers(): Unit = {
    // 如果读缓冲区大于初始大小，则重置为初始大小
    if (this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    // 如果写缓冲区大于初始大小，则重置为初始大小
    if (this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
   * 将日志中的段按大小分组，日志数据和索引数据的大小分别强制执行。
   * 我们将这样的段组合在一起形成单个目标段，这可以防止段大小过度缩小。
   * 
   * 应用场景：
   * - 日志压缩前的段整理
   * - 优化存储空间使用
   * - 提高日志清理效率
   * 
   * 设计考虑：
   * 1. 平衡段大小和清理效率
   * 2. 考虑日志数据和索引数据的大小限制
   * 3. 处理特殊情况（如空段）
   *
   * @param segments 要分组的日志段
   * @param maxSize 一个组中所有日志数据的最大总字节数
   * @param maxIndexSize 一个组中所有索引数据的最大总字节数
   * @param firstUncleanableOffset 要清理到的上限（不包含）偏移量
   * @return 分组后的段列表
   */
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int, firstUncleanableOffset: Long): List[Seq[LogSegment]] = {
    // 初始化分组列表
    var grouped = List[List[LogSegment]]()
    // 将输入段转换为列表
    var segs = segments.toList
    // 当还有段需要处理时继续循环
    while (segs.nonEmpty) {
      // 创建新组，从第一个段开始
      var group = List(segs.head)
      // 初始化组的大小统计
      var logSize = segs.head.size.toLong
      var indexSize = segs.head.offsetIndex.sizeInBytes.toLong
      var timeIndexSize = segs.head.timeIndex.sizeInBytes.toLong
      // 移除已处理的段
      segs = segs.tail
      // 尝试将更多的段添加到当前组
      while (segs.nonEmpty &&
            // 检查是否超过日志数据大小限制
            logSize + segs.head.size <= maxSize &&
            // 检查是否超过偏移量索引大小限制
            indexSize + segs.head.offsetIndex.sizeInBytes <= maxIndexSize &&
            // 检查是否超过时间索引大小限制
            timeIndexSize + segs.head.timeIndex.sizeInBytes <= maxIndexSize &&
            // 如果第一个段大小为0，不需要检查索引偏移量范围
            // 这可以避免每2^31条消息留下空日志
            (segs.head.size == 0 ||
              lastOffsetForFirstSegment(segs, firstUncleanableOffset) - group.last.baseOffset <= Int.MaxValue)) {
        // 将段添加到组中
        group = segs.head :: group
        // 更新组的大小统计
        logSize += segs.head.size
        indexSize += segs.head.offsetIndex.sizeInBytes
        timeIndexSize += segs.head.timeIndex.sizeInBytes
        // 移除已处理的段
        segs = segs.tail
      }
      // 将当前组（反转后）添加到分组列表
      grouped ::= group.reverse
    }
    // 返回最终的分组列表（反转以保持原始顺序）
    grouped.reverse
  }

  /**
   * 获取segs中第一个日志段的最后偏移量。
   * LogSegment.nextOffset()可以给出段中的确切最后偏移量，但这可能很耗时，因为需要从最后一个索引条目开始扫描段。
   * 因此，我们通过使用列表中下一个段的基础偏移量来估算第一个日志段的最后偏移量。
   * 如果下一个段不存在，则使用firstUncleanableOffset。
   * 
   * 应用场景：
   * - 估算段的边界
   * - 优化性能，避免完整扫描
   * - 处理段分组时的偏移量计算
   * 
   * 设计考虑：
   * 1. 性能优化，避免完整扫描段
   * 2. 使用保守估计确保安全性
   * 3. 处理边界情况（最后一个段）
   *
   * @param segs 要分组的剩余段
   * @param firstUncleanableOffset 要清理到的上限（不包含）偏移量
   * @return segs中第一个段的估算最后偏移量
   */
  private def lastOffsetForFirstSegment(segs: List[LogSegment], firstUncleanableOffset: Long): Long = {
    if (segs.size > 1) {
      // 如果存在下一个段，使用其基础偏移量作为边界偏移量
      // 这保证了我们知道最坏情况的偏移量
      segs(1).baseOffset - 1
    } else {
      // 对于列表中的最后一个段，使用第一个不可清理的偏移量
      firstUncleanableOffset - 1
    }
  }

  /**
   * 为日志中可清理的脏部分中的键构建key_hash => offset映射，用于清理。
   * 
   * 应用场景：
   * - 日志压缩前的准备工作
   * - 跟踪消息键的最新位置
   * - 事务消息的处理
   * 
   * 设计考虑：
   * 1. 高效构建偏移量映射
   * 2. 处理事务消息
   * 3. 资源使用监控
   * 
   * @param log 要使用的日志
   * @param start 脏消息开始的偏移量
   * @param end 正在构建的映射的结束偏移量
   * @param map 用于存储映射的映射对象
   * @param stats 清理统计信息收集器
   */
  private[log] def buildOffsetMap(log: UnifiedLog,
                                  start: Long,
                                  end: Long,
                                  map: OffsetMap,
                                  stats: CleanerStats): Unit = {
    // 清除现有映射
    map.clear()
    // 获取指定范围内的脏段
    val dirty = log.logSegments(start, end).toBuffer
    // 创建下一个段起始偏移量的列表
    val nextSegmentStartOffsets = new ListBuffer[Long]
    if (dirty.nonEmpty) {
      // 收集所有段的起始偏移量（除第一个段外）
      for (nextSegment <- dirty.tail) nextSegmentStartOffsets.append(nextSegment.baseOffset)
      // 添加结束偏移量
      nextSegmentStartOffsets.append(end)
    }
    // 记录开始构建偏移量映射的日志
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))

    // 创建事务元数据对象
    val transactionMetadata = new CleanedTransactionMetadata
    // 收集指定范围内的已中止事务
    val abortedTransactions = log.collectAbortedTransactions(start, end)
    // 添加已中止事务到元数据
    transactionMetadata.addAbortedTransactions(abortedTransactions)

    // 添加所有可清理的脏段。我们必须至少取map.slots * load_factor个槽位，
    // 但如果日志的脏部分有大量重复，我们可能可以容纳更多
    var full = false
    // 遍历脏段及其对应的下一个段起始偏移量
    for ((segment, nextSegmentStartOffset) <- dirty.zip(nextSegmentStartOffsets) if !full) {
      // 检查是否需要中止清理
      checkDone(log.topicPartition)

      // 为当前段构建偏移量映射
      full = buildOffsetMapForSegment(log.topicPartition, segment, map, start, nextSegmentStartOffset, log.config.maxMessageSize,
        transactionMetadata, stats)
      // 如果映射已满，记录调试信息
      if (full)
        debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
    }
    // 记录完成构建偏移量映射的日志
    info("Offset map for log %s complete.".format(log.name))
  }

  /**
   * 将给定日志段中的消息添加到偏移量映射中
   * 
   * 设计考虑：
   * 1. 增量构建：通过遍历日志段中的消息批次，逐步构建偏移量映射
   * 2. 事务处理：区分控制批次和普通批次，正确处理已中止的事务
   * 3. 内存管理：通过限制映射大小避免内存溢出
   * 4. 性能优化：使用流式迭代器处理消息，减少内存使用
   * 
   * 应用场景：
   * 1. 日志压缩：构建消息键到最新偏移量的映射
   * 2. 事务处理：跟踪和处理事务状态
   * 3. 性能监控：收集清理过程的统计信息
   *
   * @param topicPartition 要构建偏移量的日志段所属的主题分区
   * @param segment 要索引的日志段
   * @param map 用于存储键=>偏移量映射的映射表
   * @param startOffset 脏消息开始的偏移量
   * @param nextSegmentStartOffset 构建当前段时下一个段的基准偏移量
   * @param maxLogMessageSize 允许的记录最大字节数
   * @param transactionMetadata 要构建的偏移量范围内日志的进行中事务状态
   * @param stats 清理统计信息收集器
   *
   * @return 如果在从此段加载时映射已满则返回true
   */
  private def buildOffsetMapForSegment(topicPartition: TopicPartition,
                                       segment: LogSegment,
                                       map: OffsetMap,
                                       startOffset: Long,
                                       nextSegmentStartOffset: Long,
                                       maxLogMessageSize: Int,
                                       transactionMetadata: CleanedTransactionMetadata,
                                       stats: CleanerStats): Boolean = {
    // 获取startOffset对应的物理位置
    var position = segment.offsetIndex.lookup(startOffset).position
    // 计算期望的最大映射大小，考虑负载因子
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    // 遍历日志段中的所有消息
    while (position < segment.log.sizeInBytes) {
      // 检查清理是否被中止
      checkDone(topicPartition)
      // 清空读缓冲区准备读取新数据
      readBuffer.clear()
      try {
        // 从日志段读取数据到缓冲区
        segment.log.readInto(readBuffer, position)
      } catch {
        case e: Exception =>
          throw new KafkaException(s"Failed to read from segment $segment of partition $topicPartition " +
            "while loading offset map", e)
      }
      // 将缓冲区数据转换为可读记录
      val records = MemoryRecords.readableRecords(readBuffer)
      // 根据记录大小进行限流控制
      throttler.maybeThrottle(records.sizeInBytes)

      val startPosition = position
      // 遍历每个消息批次
      for (batch <- records.batches.asScala) {
        if (batch.isControlBatch) {
          // 处理事务控制批次
          transactionMetadata.onControlBatchRead(batch)
          stats.indexMessagesRead(1)
        } else {
          // 检查批次是否已中止
          val isAborted = transactionMetadata.onBatchRead(batch)
          if (isAborted) {
            // 如果批次已中止，不需要填充偏移量映射
            // 注意：中止标记在v2及以上版本支持，这意味着count是已定义的
            stats.indexMessagesRead(batch.countOrNull)
          } else {
            // 使用流式迭代器处理批次中的记录
            val recordsIterator = batch.streamingIterator(decompressionBufferSupplier)
            try {
              for (record <- recordsIterator.asScala) {
                // 只处理有键且偏移量大于等于起始偏移量的记录
                if (record.hasKey && record.offset >= startOffset) {
                  // 如果映射未满，添加键值对
                  if (map.size < maxDesiredMapSize)
                    map.put(record.key, record.offset)
                  else
                    return true
                }
                stats.indexMessagesRead(1)
              }
            } finally recordsIterator.close()
          }
        }

        // 更新最新偏移量
        if (batch.lastOffset >= startOffset)
          map.updateLatestOffset(batch.lastOffset)
      }
      // 更新读取位置和统计信息
      val bytesRead = records.validBytes
      position += bytesRead
      stats.indexBytesRead(bytesRead)

      // 如果没有读取到完整消息，可能需要增加缓冲区大小
      if (position == startPosition)
        growBuffersOrFail(segment.log, position, maxLogMessageSize, records)
    }

    // 处理偏移量间隙，快速前进到此段中的最新预期偏移量
    map.updateLatestOffset(nextSegmentStartOffset - 1L)

    // 恢复缓冲区状态
    restoreBuffers()
    false
  }
}

/**
  * 清理前统计信息收集器
  * 
  * 设计考虑：
  * 1. 跟踪压缩延迟：记录最大压缩延迟时间
  * 2. 分区状态监控：统计延迟和可清理的分区数量
  * 
  * 应用场景：
  * 1. 性能监控：评估清理任务的及时性
  * 2. 资源规划：根据延迟情况调整清理资源
  */
private class PreCleanStats {
  // 最大压缩延迟时间（毫秒）
  var maxCompactionDelayMs = 0L
  // 存在延迟的分区数量
  var delayedPartitions = 0
  // 可清理的分区总数
  var cleanablePartitions = 0

  /**
   * 更新最大压缩延迟时间
   * @param delayMs 当前检测到的延迟时间
   */
  def updateMaxCompactionDelay(delayMs: Long): Unit = {
    maxCompactionDelayMs = Math.max(maxCompactionDelayMs, delayMs)
    if (delayMs > 0) {
      delayedPartitions += 1
    }
  }

  /**
   * 记录可清理的分区数量
   * @param numOfCleanables 可清理的分区数
   */
  def recordCleanablePartitions(numOfCleanables: Int): Unit = {
    cleanablePartitions = numOfCleanables
  }
}

/**
 * 日志清理统计信息收集器
 * 
 * 设计考虑：
 * 1. 性能指标：跟踪读写字节数和消息数
 * 2. 时间统计：记录清理各阶段的耗时
 * 3. 资源利用：监控缓冲区使用率
 * 
 * 应用场景：
 * 1. 性能优化：分析清理过程的瓶颈
 * 2. 监控告警：检测异常的清理行为
 * 3. 容量规划：评估清理效率和资源需求
 */
private class CleanerStats(time: Time = Time.SYSTEM) {
  // 清理开始时间
  val startTime = time.milliseconds
  // 映射构建完成时间
  var mapCompleteTime: Long = -1L
  // 清理结束时间
  var endTime: Long = -1L
  // 读取的总字节数
  var bytesRead = 0L
  // 写入的总字节数
  var bytesWritten = 0L
  // 映射阶段读取的字节数
  var mapBytesRead = 0L
  // 映射阶段读取的消息数
  var mapMessagesRead = 0L
  // 读取的总消息数
  var messagesRead = 0L
  // 读取的无效消息数
  var invalidMessagesRead = 0L
  // 写入的消息数
  var messagesWritten = 0L
  // 缓冲区利用率
  var bufferUtilization = 0.0d

  /**
   * 记录读取的消息统计信息
   * @param messagesRead 读取的消息数
   * @param bytesRead 读取的字节数
   */
  def readMessages(messagesRead: Int, bytesRead: Int): Unit = {
    this.messagesRead += messagesRead
    this.bytesRead += bytesRead
  }

  /**
   * 记录一条无效消息
   */
  def invalidMessage(): Unit = {
    invalidMessagesRead += 1
  }

  /**
   * 记录重新复制的消息统计信息
   * @param messagesWritten 写入的消息数
   * @param bytesWritten 写入的字节数
   */
  def recopyMessages(messagesWritten: Int, bytesWritten: Int): Unit = {
    this.messagesWritten += messagesWritten
    this.bytesWritten += bytesWritten
  }

  /**
   * 记录映射阶段读取的消息数
   * @param size 读取的消息数
   */
  def indexMessagesRead(size: Int): Unit = {
    mapMessagesRead += size
  }

  /**
   * 记录映射阶段读取的字节数
   * @param size 读取的字节数
   */
  def indexBytesRead(size: Int): Unit = {
    mapBytesRead += size
  }

  /**
   * 标记映射阶段完成
   */
  def indexDone(): Unit = {
    mapCompleteTime = time.milliseconds
  }

  /**
   * 标记清理任务完成
   */
  def allDone(): Unit = {
    endTime = time.milliseconds
  }

  /**
   * 获取清理任务总耗时（秒）
   */
  def elapsedSecs: Double = (endTime - startTime) / 1000.0

  /**
   * 获取映射阶段耗时（秒）
   */
  def elapsedIndexSecs: Double = (mapCompleteTime - startTime) / 1000.0

}

/**
  * 日志清理辅助类，用于跟踪日志的清理状态。
  * 
  * 设计考虑：
  * 1. 封装了日志清理所需的关键信息，包括主题分区、日志对象、脏数据偏移量等
  * 2. 实现了Ordered特质以支持基于清理比率的优先级排序
  * 3. 提供了计算清理字节数和清理比率的功能
  * 
  * 应用场景：
  * - 日志压缩任务的优先级排序
  * - 跟踪日志段的清理状态
  * - 计算日志清理的进度和效率
  * 
  * @param topicPartition 主题分区，标识要清理的日志所属的主题和分区
  * @param log 统一日志对象，包含了要清理的实际日志数据
  * @param firstDirtyOffset 第一个脏数据的偏移量，表示从此位置开始需要清理
  * @param uncleanableOffset 不可清理的偏移量边界，超过此位置的数据暂时不能清理
  * @param needCompactionNow 是否需要立即进行压缩，默认为false
  */
private case class LogToClean(topicPartition: TopicPartition,
                              log: UnifiedLog,
                              firstDirtyOffset: Long,
                              uncleanableOffset: Long,
                              needCompactionNow: Boolean = false) extends Ordered[LogToClean] {
  /**
   * 已清理的字节数，通过计算从日志开始到第一个脏数据偏移量之间所有日志段大小的总和得到
   * 用于评估日志清理的进度和计算清理比率
   */
  val cleanBytes: Long = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum

  /**
   * 计算实际可清理的范围和字节数
   * firstUncleanableOffset: 第一个不可清理的偏移量，可能小于等于uncleanableOffset
   * cleanableBytes: 可以清理的字节数，即从firstDirtyOffset到firstUncleanableOffset之间的数据大小
   */
  val (firstUncleanableOffset, cleanableBytes) = LogCleanerManager.calculateCleanableBytes(log, firstDirtyOffset, uncleanableOffset)

  /**
   * 日志总字节数，包括已清理和可清理的部分
   * 用于计算清理比率，评估清理任务的规模
   */
  val totalBytes: Long = cleanBytes + cleanableBytes

  /**
   * 可清理比率，即可清理字节数占总字节数的比例
   * 该比率用于确定清理任务的优先级，比率越高表示越需要清理
   */
  val cleanableRatio: Double = cleanableBytes / totalBytes.toDouble

  /**
   * 实现Ordered特质的比较方法，用于对清理任务进行优先级排序
   * 根据cleanableRatio进行比较，返回值：
   * - 正数：当前对象优先级更高
   * - 零：优先级相等
   * - 负数：参数对象优先级更高
   *
   * @param that 要比较的另一个LogToClean对象
   * @return 比较结果
   */
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}

/**
 * 事务状态跟踪辅助类，用于在日志清理过程中管理事务状态。
 * 
 * 设计考虑：
 * 1. 维护了正在进行的已中止和已提交事务的集合
 * 2. 使用优先队列管理已中止事务，按照事务的第一个偏移量排序
 * 3. 提供了事务标记删除决策和事务索引更新功能
 * 
 * 应用场景：
 * - 跟踪事务状态变化
 * - 决定何时可以安全地删除事务标记
 * - 维护事务索引的一致性
 * - 处理事务日志的清理和压缩
 * 
 * 实现细节：
 * 1. 使用可变集合存储事务状态，支持高效的状态更新
 * 2. 通过优先队列实现按偏移量顺序处理已中止事务
 * 3. 提供事务批次处理和控制记录处理的接口
 */
private[log] class CleanedTransactionMetadata {
  /**
   * 存储正在进行的已提交事务的生产者ID集合
   * 用于跟踪哪些事务已经提交但还未完全清理
   */
  private val ongoingCommittedTxns = mutable.Set.empty[Long]

  /**
   * 存储正在进行的已中止事务的元数据映射
   * key: 生产者ID
   * value: 已中止事务的元数据，包含事务的详细信息
   */
  private val ongoingAbortedTxns = mutable.Map.empty[Long, AbortedTransactionMetadata]

  /**
   * 已中止事务的最小堆，按事务的第一个偏移量排序
   * 使用优先队列实现，确保按照事务开始的顺序处理
   * 通过reverse操作将默认的最大堆转换为最小堆
   */
  private val abortedTransactions = mutable.PriorityQueue.empty[AbortedTxn](new Ordering[AbortedTxn] {
    override def compare(x: AbortedTxn, y: AbortedTxn): Int = java.lang.Long.compare(x.firstOffset, y.firstOffset)
  }.reverse)

  /**
   * 清理后的事务索引，用于写入保留的已中止事务信息
   * 在日志清理过程中维护，确保事务状态的一致性
   */
  var cleanedIndex: Option[TransactionIndex] = None

  /**
   * 更新已清理的事务状态，添加新发现的已中止事务
   * 在日志清理过程中，当发现新的已中止事务时调用此方法
   *
   * @param abortedTransactions 要添加的新发现的已中止事务列表
   */
  def addAbortedTransactions(abortedTransactions: List[AbortedTxn]): Unit = {
    // 将新发现的已中止事务添加到优先队列中，保持按偏移量排序
    this.abortedTransactions ++= abortedTransactions
  }

  /**
   * 处理清理器遍历到的控制批次，更新已清理的事务状态
   * 根据控制记录类型（ABORT/COMMIT）决定是否可以丢弃该控制批次
   *
   * 实现细节：
   * 1. 首先处理当前偏移量之前的所有已中止事务
   * 2. 根据控制记录类型进行不同处理：
   *    - ABORT：检查事务是否已完全清理
   *    - COMMIT：检查是否遍历过该事务的任何批次
   *
   * @param controlBatch 已遍历的控制批次
   * @return 如果控制批次可以丢弃则返回true
   */
  def onControlBatchRead(controlBatch: RecordBatch): Boolean = {
    // 处理截至当前批次最后偏移量的所有已中止事务
    consumeAbortedTxnsUpTo(controlBatch.lastOffset)

    val controlRecordIterator = controlBatch.iterator
    if (controlRecordIterator.hasNext) {
      val controlRecord = controlRecordIterator.next()
      val controlType = ControlRecordType.parse(controlRecord.key)
      val producerId = controlBatch.producerId
      controlType match {
        case ControlRecordType.ABORT =>
          ongoingAbortedTxns.remove(producerId) match {
            // 只有当事务的所有批次都被移除后，才保留中止标记
            case Some(abortedTxnMetadata) if abortedTxnMetadata.lastObservedBatchOffset.isDefined =>
              cleanedIndex.foreach(_.append(abortedTxnMetadata.abortedTxn))
              false // 不能丢弃，因为还需要保留中止标记
            case _ => true // 可以丢弃，因为没有观察到该事务的任何批次
          }

        case ControlRecordType.COMMIT =>
          // 如果没有遍历到该事务的任何批次，则该标记可以删除
          !ongoingCommittedTxns.remove(producerId)

        case _ => false // 其他类型的控制记录不能丢弃
      }
    } else {
      // 空的控制批次已经被清理，可以安全丢弃
      true
    }
  }

  /**
   * 处理给定偏移量之前的所有已中止事务
   * 将这些事务从优先队列中移出并添加到正在进行的已中止事务映射中
   *
   * @param offset 要处理的最大偏移量
   */
  private def consumeAbortedTxnsUpTo(offset: Long): Unit = {
    // 处理所有第一个偏移量小于等于给定偏移量的已中止事务
    while (abortedTransactions.headOption.exists(_.firstOffset <= offset)) {
      val abortedTxn = abortedTransactions.dequeue()
      // 将已中止事务添加到映射中，如果不存在则创建新的元数据对象
      ongoingAbortedTxns.getOrElseUpdate(abortedTxn.producerId, new AbortedTransactionMetadata(abortedTxn))
    }
  }

  /**
   * 处理非控制批次，更新事务状态
   * 如果批次属于已中止事务，则返回true表示可以安全丢弃
   *
   * 实现细节：
   * 1. 首先处理当前偏移量之前的所有已中止事务
   * 2. 对于事务性批次：
   *    - 如果属于已中止事务，更新最后观察到的批次偏移量
   *    - 如果属于已提交事务，添加到正在进行的已提交事务集合
   *
   * @param batch 要处理的批次
   * @return 如果批次属于已中止事务则返回true
   */
  def onBatchRead(batch: RecordBatch): Boolean = {
    // 处理截至当前批次最后偏移量的所有已中止事务
    consumeAbortedTxnsUpTo(batch.lastOffset)
    if (batch.isTransactional) {
      ongoingAbortedTxns.get(batch.producerId) match {
        case Some(abortedTransactionMetadata) =>
          // 更新已中止事务的最后观察到的批次偏移量
          abortedTransactionMetadata.lastObservedBatchOffset = Some(batch.lastOffset)
          true // 批次属于已中止事务，可以丢弃
        case None =>
          // 批次属于已提交事务，添加到正在进行的已提交事务集合
          ongoingCommittedTxns += batch.producerId
          false // 不能丢弃
      }
    } else {
      false // 非事务性批次不能丢弃
    }
  }

}

/**
 * 已中止事务的元数据类，用于在日志清理过程中跟踪和管理已中止的事务信息。
 *
 * 设计考虑：
 * 1. 封装事务状态：将已中止事务的状态信息封装在一个类中，便于管理和维护
 * 2. 跟踪进度：通过lastObservedBatchOffset记录清理进度，确保事务完整性
 * 3. 内存效率：使用Option类型避免空值，优化内存使用
 *
 * 应用场景：
 * - 日志压缩：在压缩过程中识别和处理已中止的事务
 * - 数据清理：清理已中止事务的相关记录
 * - 事务恢复：在broker重启时重建事务状态
 *
 * @param abortedTxn 已中止事务的基本信息，包含事务ID、生产者ID等
 */
private class AbortedTransactionMetadata(val abortedTxn: AbortedTxn) {
  /**
   * 记录在清理过程中最后观察到的批次偏移量
   * 用于跟踪清理进度，确保不会重复处理同一事务的记录
   * None表示尚未处理任何批次
   */
  var lastObservedBatchOffset: Option[Long] = None

  override def toString: String = s"(txn: $abortedTxn, lastOffset: $lastObservedBatchOffset)"
}
