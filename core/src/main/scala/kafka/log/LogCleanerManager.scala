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

import java.lang.{Long => JLong}
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile
import org.apache.kafka.storage.internals.log.{LogCleaningAbortedException, LogDirFailureChannel}
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import java.util.Comparator
import scala.collection.{Iterable, Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * 日志清理状态的密封特质，定义了所有可能的日志清理状态
 */
private[log] sealed trait LogCleaningState

/**
 * 表示日志清理正在进行中的状态
 */
private[log] case object LogCleaningInProgress extends LogCleaningState

/**
 * 表示日志清理已被中止的状态
 */
private[log] case object LogCleaningAborted extends LogCleaningState

/**
 * 表示日志清理已暂停的状态，pausedCount表示暂停的次数
 */
private[log] case class LogCleaningPaused(pausedCount: Int) extends LogCleaningState

/**
 * 日志清理异常类，用于在日志清理过程中出现错误时抛出
 *
 * @param log 发生异常的统一日志对象
 * @param message 异常信息
 * @param cause 异常原因
 */
private[log] class LogCleaningException(val log: UnifiedLog,
                                        private val message: String,
                                        private val cause: Throwable) extends KafkaException(message, cause)

/**
 * 这个类管理每个正在清理的分区的状态。
 * LogCleaningState定义了一个TopicPartition可能处于的清理状态：
 *
 * 1. None状态：
 *    - 表示TopicPartition没有清理状态
 *    - 可以转换为LogCleaningInProgress或LogCleaningPaused(1)
 *    - 有效的前置状态是LogCleaningInProgress和LogCleaningPaused(1)
 *
 * 2. LogCleaningInProgress状态：
 *    - 表示清理当前正在进行中
 *    - 当日志清理完成时可以转换为None
 *    - 或者转换为LogCleaningAborted
 *    - 有效的前置状态是None
 *
 * 3. LogCleaningAborted状态：
 *    - 表示请求中止清理
 *    - 可以转换为LogCleaningPaused(1)
 *    - 有效的前置状态是LogCleaningInProgress
 *
 * 4-a. LogCleaningPaused(1)状态：
 *    - 表示清理被暂停一次
 *    - 在此状态下不能进行日志清理
 *    - 可以转换为None或LogCleaningPaused(2)
 *    - 有效的前置状态是None、LogCleaningAborted或LogCleaningPaused(2)
 *
 * 4-b. LogCleaningPaused(i)状态：
 *    - 表示清理被暂停i次，其中i >= 2
 *    - 在此状态下不能进行日志清理
 *    - 可以转换为LogCleaningPaused(i-1)或LogCleaningPaused(i+1)
 *    - 有效的前置状态是LogCleaningPaused(i-1)或LogCleaningPaused(i+1)
 */
private[log] class LogCleanerManager(val logDirs: Seq[File], // 日志目录列表
                                     val logs: Pool[TopicPartition, UnifiedLog], // 主题分区到统一日志的映射池
                                     val logDirFailureChannel: LogDirFailureChannel) extends Logging { // 日志目录失败通道
  import LogCleanerManager._

  // 用于收集和报告指标的度量组
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // 重写日志记录器名称，使用LogCleaner类名
  protected override def loggerName: String = classOf[LogCleaner].getName

  // 清理器偏移量检查点文件名，包级私有用于测试
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"

  /* 保存每个日志最后清理点的偏移量检查点映射 */
  @volatile private var checkpoints = logDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, offsetCheckpointFile), logDirFailureChannel))).toMap

  /* 当前正在清理的日志集合 */
  private val inProgress = mutable.HashMap[TopicPartition, LogCleaningState]()

  /* 每个日志目录中不可清理的分区集合（在清理过程中出现意外错误的分区） */
  private val uncleanablePartitions = mutable.HashMap[String, mutable.Set[TopicPartition]]()

  /* 用于控制对进行中集合和偏移量检查点的所有访问的全局锁 */
  private val lock = new ReentrantLock

  /* 用于协调分区的暂停和清理的条件变量 */
  private val pausedCleaningCond = lock.newCondition()

  // 用于测试的可见度量标签映射
  private[log] val gaugeMetricNameWithTag = new java.util.HashMap[String, java.util.List[java.util.Map[String, String]]]()

  /* 跟踪每个日志目录中标记为不可清理的分区数量的度量器 */
  for (dir <- logDirs) {
    // 创建度量标签，包含日志目录路径
    val metricTag = Map("logDirectory" -> dir.getAbsolutePath).asJava
    // 注册新的度量器，用于统计不可清理分区的数量
    metricsGroup.newGauge(UncleanablePartitionsCountMetricName,
      () => inLock(lock) { uncleanablePartitions.get(dir.getAbsolutePath).map(_.size).getOrElse(0) },
      metricTag
    )
    // 将度量标签添加到标签映射中
    gaugeMetricNameWithTag.computeIfAbsent(UncleanablePartitionsCountMetricName, _ => new java.util.ArrayList[java.util.Map[String, String]]())
      .add(metricTag)
  }

  /* 跟踪每个日志目录中不可清理分区的不可清理字节数的度量器 */
  for (dir <- logDirs) {
    // 创建度量标签，包含日志目录路径
    val metricTag = Map("logDirectory" -> dir.getAbsolutePath).asJava
    // 注册新的度量器，用于统计不可清理的字节数
    metricsGroup.newGauge(UncleanableBytesMetricName,
      () => inLock(lock) {
        uncleanablePartitions.get(dir.getAbsolutePath) match {
          case Some(partitions) =>
            // 获取所有清理器检查点
            val lastClean = allCleanerCheckpoints
            val now = Time.SYSTEM.milliseconds
            // 计算所有不可清理分区的不可清理字节数总和
            partitions.iterator.map { tp =>
              Option(logs.get(tp)).map {
                log =>
                  // 获取上次清理的偏移量
                  val lastCleanOffset: Option[Long] = lastClean.get(tp)
                  // 计算需要清理的偏移量范围
                  val offsetsToClean = cleanableOffsets(log, lastCleanOffset, now)
                  // 计算不可清理的字节数
                  val (_, uncleanableBytes) = calculateCleanableBytes(log, offsetsToClean.firstDirtyOffset, offsetsToClean.firstUncleanableDirtyOffset)
                  uncleanableBytes
              }.getOrElse(0L)
            }.sum
          case None => 0
        }
      },
      metricTag
    )
    // 将度量标签添加到标签映射中
    gaugeMetricNameWithTag.computeIfAbsent(UncleanableBytesMetricName, _ => new java.util.ArrayList[java.util.Map[String, String]]())
      .add(metricTag)
  }

  /* 用于跟踪最脏日志的可清理比率的度量器 */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  metricsGroup.newGauge(MaxDirtyPercentMetricName, () => (100 * dirtiestLogCleanableRatio).toInt)

  /* 用于跟踪自上次日志清理器运行以来的时间（毫秒）的度量器 */
  @volatile private var timeOfLastRun: Long = Time.SYSTEM.milliseconds
  metricsGroup.newGauge(TimeSinceLastRunMsMetricName, () => Time.SYSTEM.milliseconds - timeOfLastRun)

  /**
   * 获取所有日志的已处理位置（检查点）
   * 
   * @return 返回一个映射，键为主题分区，值为该分区的检查点偏移量
   */
  def allCleanerCheckpoints: Map[TopicPartition, Long] = {
    inLock(lock) { // 使用锁确保线程安全
      checkpoints.values.flatMap(checkpoint => {
        try {
          // 读取检查点文件并转换格式
          checkpoint.read().asScala.map{ case (tp, offset) => tp -> Long2long(offset) }
        } catch {
          case e: KafkaStorageException =>
            // 如果访问检查点文件失败，记录错误并返回空映射
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
            Map.empty[TopicPartition, Long]
        }
      }).toMap
    }
  }

  /**
    * 获取分区的清理状态（包级私有，用于单元测试）
    * 
    * @param tp 主题分区
    * @return 返回该分区的清理状态，如果不存在则返回None
    */
  private[log] def cleaningState(tp: TopicPartition): Option[LogCleaningState] = {
    inLock(lock) {
      inProgress.get(tp)
    }
  }

  /**
    * 设置分区的清理状态（包级私有，用于单元测试）
    * 
    * @param tp 主题分区
    * @param state 要设置的清理状态
    */
  private[log] def setCleaningState(tp: TopicPartition, state: LogCleaningState): Unit = {
    inLock(lock) {
      inProgress.put(tp, state)
    }
  }

   /**
    * 选择下一个要清理的日志并将其添加到正在进行的集合中。
    * 每次都会从日志管理器维护的完整日志池中重新计算，以允许动态添加日志。
    * 
    * @param time 时间实例，用于获取当前时间戳
    * @param preCleanStats 预清理统计信息，用于记录清理相关的统计数据
    * @return 返回最脏的需要清理的日志，如果没有可清理的日志则返回None
    */
  def grabFilthiestCompactedLog(time: Time, preCleanStats: PreCleanStats = new PreCleanStats()): Option[LogToClean] = {
    inLock(lock) {
      // 获取当前时间戳并更新最后运行时间
      val now = time.milliseconds
      this.timeOfLastRun = now
      // 获取所有日志的清理检查点
      val lastClean = allCleanerCheckpoints

      // 筛选出需要压缩的脏日志
      val dirtyLogs = logs.filter {
        case (_, log) => log.config.compact // 只选择启用了压缩的日志
      }.filterNot {
        case (topicPartition, log) =>
          inProgress.contains(topicPartition) || isUncleanablePartition(log, topicPartition) // 排除正在清理或不可清理的分区
      }.map {
        case (topicPartition, log) => // 为每个日志创建LogToClean实例
          try {
            // 获取上次清理的偏移量
            val lastCleanOffset = lastClean.get(topicPartition)
            // 计算需要清理的偏移量范围
            val offsetsToClean = cleanableOffsets(log, lastCleanOffset, now)
            // 如果检查点偏移量无效，则更新检查点
            if (offsetsToClean.forceUpdateCheckpoint)
              updateCheckpoints(log.parentDirFile, partitionToUpdateOrAdd = Option(topicPartition, offsetsToClean.firstDirtyOffset))
            // 计算压缩延迟时间
            val compactionDelayMs = maxCompactionDelay(log, offsetsToClean.firstDirtyOffset, now)
            preCleanStats.updateMaxCompactionDelay(compactionDelayMs)

            // 创建LogToClean实例
            LogToClean(topicPartition, log, offsetsToClean.firstDirtyOffset, offsetsToClean.firstUncleanableDirtyOffset, compactionDelayMs > 0)
          } catch {
            case e: Throwable => throw new LogCleaningException(log,
              s"Failed to calculate log cleaning stats for partition $topicPartition", e)
          }
      }.filter(ltc => ltc.totalBytes > 0) // 跳过空日志

      // 更新最脏日志的可清理比率
      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
      // 筛选出满足最小清理阈值或需要立即压缩的日志
      val cleanableLogs = dirtyLogs.filter { ltc =>
        (ltc.needCompactionNow && ltc.cleanableBytes > 0) || ltc.cleanableRatio > ltc.log.config.minCleanableRatio
      }

      if (cleanableLogs.isEmpty)
        None
      else {
        // 记录可清理的分区数量
        preCleanStats.recordCleanablePartitions(cleanableLogs.size)
        // 获取最脏的日志并将其标记为正在清理
        val filthiest = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }

  /**
    * 暂停未启用压缩且当前没有其他删除或压缩操作正在进行的日志的清理。
    * 这是为了处理用户在压缩和非压缩主题配置之间切换时，保留线程和清理器线程之间的潜在竞争。
    * 
    * @return 返回已成功暂停日志清理的保留日志集合
    */
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, UnifiedLog)] = {
    inLock(lock) {
      // 筛选出未启用压缩的日志
      val deletableLogs = logs.filter {
        case (_, log) => !log.config.compact // 选择未启用压缩的日志
      }.filterNot {
        case (topicPartition, _) => inProgress.contains(topicPartition) // 跳过已在进行中的日志
      }

      // 将筛选出的日志标记为已暂停
      deletableLogs.foreach {
        case (topicPartition, _) => inProgress.put(topicPartition, LogCleaningPaused(1))
      }
      deletableLogs
    }
  }

  /**
    * 查找所有启用了压缩的日志并将其标记为正在清理中。
    * 包括未启用删除功能的日志，因为它们可能有早于起始偏移量的日志段。
    * 
    * 应用场景：
    * 1. 在日志清理过程中，需要找出所有可以进行压缩清理的日志分区
    * 2. 用于日志清理器的初始化阶段，确定清理任务的范围
    * 
    * 实现细节：
    * 1. 使用锁保护并发访问
    * 2. 过滤条件：
    *   - 分区不在进行中的清理任务中
    *   - 日志配置启用了压缩
    *   - 分区不在不可清理的分区列表中
    * 3. 将筛选出的分区标记为正在清理状态
    */
  def deletableLogs(): Iterable[(TopicPartition, UnifiedLog)] = {
    inLock(lock) { // 使用锁确保线程安全
      val toClean = logs.filter { case (topicPartition, log) =>
        // 过滤出可清理的日志：未在清理中、启用了压缩且不在不可清理列表中
        !inProgress.contains(topicPartition) && log.config.compact &&
          !isUncleanablePartition(log, topicPartition)
      }
      // 将筛选出的日志标记为正在清理状态
      toClean.foreach { case (tp, _) => inProgress.put(tp, LogCleaningInProgress) }
      toClean
    }
  }

  /**
   * 如果某个分区正在进行清理，中止该分区的清理过程。此调用会阻塞直到分区的清理被中止。
   * 实现方式是先调用abortAndPausing中止并暂停清理，然后通过resumeCleaning恢复分区的清理。
   * 
   * 应用场景：
   * 1. 当需要紧急停止某个分区的清理任务时（如分区删除、主题删除等操作）
   * 2. 在进行分区迁移或副本重分配前，需要确保清理任务已停止
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 先中止并暂停清理（设置状态为已中止）
   * 3. 然后立即恢复清理（移除暂停状态）
   * 4. 整个过程是原子的，确保状态转换的一致性
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      // 先中止并暂停清理
      abortAndPauseCleaning(topicPartition)
      // 然后恢复清理，移除暂停状态
      resumeCleaning(Seq(topicPartition))
    }
  }

  /**
   * 如果某个分区正在进行清理，中止该分区的清理过程，并暂停该分区未来的清理操作。
   * 此调用会阻塞直到分区的清理被中止并暂停。
   * 
   * 状态转换流程：
   * 1. 如果分区不在进行中，将其标记为已暂停状态
   * 2. 否则，先将分区状态标记为已中止
   * 3. 清理线程会定期检查状态，如果发现分区状态为已中止，则抛出LogCleaningAbortedException以停止清理任务
   * 4. 当清理任务停止时，会调用doneCleaning()，将分区状态设置为已暂停
   * 5. abortAndPauseCleaning()会等待直到分区状态变为已暂停
   * 6. 如果分区已经处于暂停状态，新的调用会将暂停计数加一
   * 
   * 应用场景：
   * 1. 在进行分区重分配前暂停清理
   * 2. 在删除主题前确保清理任务已停止
   * 3. 处理清理任务异常时的恢复机制
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 使用模式匹配处理不同的清理状态
   * 3. 使用条件变量等待状态转换完成
   * 4. 支持暂停计数，允许多次暂停请求
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      inProgress.get(topicPartition) match {
        case None =>
          // 如果分区不在进行中，直接标记为已暂停，计数为1
          inProgress.put(topicPartition, LogCleaningPaused(1))
        case Some(LogCleaningInProgress) =>
          // 如果分区正在清理中，标记为已中止
          inProgress.put(topicPartition, LogCleaningAborted)
        case Some(LogCleaningPaused(count)) =>
          // 如果分区已经暂停，增加暂停计数
          inProgress.put(topicPartition, LogCleaningPaused(count + 1))
        case Some(s) =>
          // 其他状态下不能中止和暂停，抛出异常
          throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be aborted and paused since it is in $s state.")
      }
      // 等待直到分区状态变为已暂停
      while (!isCleaningInStatePaused(topicPartition))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
  }

  /**
    * 恢复已暂停分区的清理操作。
    * 每次调用此函数将撤销一次暂停操作。
    * 
    * 应用场景：
    * 1. 分区重分配完成后恢复清理
    * 2. 系统从异常状态恢复后重新启动清理
    * 3. 手动恢复之前暂停的清理任务
    * 
    * 实现细节：
    * 1. 使用锁保护并发访问
    * 2. 对每个分区进行状态检查和转换：
    *   - 如果暂停计数为1，完全移除暂停状态
    *   - 如果暂停计数大于1，减少暂停计数
    *   - 如果分区不在暂停状态，抛出异常
    * 3. 支持批量处理多个分区
    * 4. 保持暂停计数的一致性
    */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      topicPartitions.foreach { topicPartition =>
        inProgress.get(topicPartition) match {
          case None =>
            // 如果分区不在暂停状态，抛出异常
            throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is not paused.")
          case Some(state) =>
            state match {
              case LogCleaningPaused(count) if count == 1 =>
                // 如果暂停计数为1，移除暂停状态
                inProgress.remove(topicPartition)
              case LogCleaningPaused(count) if count > 1 =>
                // 如果暂停计数大于1，减少计数
                inProgress.put(topicPartition, LogCleaningPaused(count - 1))
              case s =>
                // 其他状态下不能恢复，抛出异常
                throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is in $s state.")
            }
        }
      }
    }
  }

  /**
   * 检查分区的清理是否处于特定状态。调用者需要在调用此方法时持有锁。
   * 
   * 应用场景：
   * 1. 在执行状态转换前检查当前状态
   * 2. 在清理操作前验证分区状态
   * 3. 用于调试和监控目的
   * 
   * 实现细节：
   * 1. 通过模式匹配检查分区状态
   * 2. 如果分区不存在于进行中的任务中，返回false
   * 3. 将当前状态与期望状态进行比较
   * 4. 返回布尔值表示状态是否匹配
   */
  private def isCleaningInState(topicPartition: TopicPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false // 分区不在进行中的任务中
      case Some(state) =>
        if (state == expectedState) // 比较当前状态与期望状态
          true
        else
          false
    }
  }

  /**
   * 检查分区的清理是否处于暂停状态。调用者需要在调用此方法时持有锁。
   * 
   * 应用场景：
   * 1. 在恢复清理前检查分区是否已暂停
   * 2. 在等待清理暂停完成时进行状态检查
   * 3. 用于确保清理操作的安全性
   * 
   * 实现细节：
   * 1. 使用嵌套的模式匹配检查状态
   * 2. 如果分区不存在于进行中的任务中，返回false
   * 3. 对于LogCleaningPaused状态（不关心具体的暂停计数）返回true
   * 4. 对于其他所有状态返回false
   */
  private def isCleaningInStatePaused(topicPartition: TopicPartition): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false // 分区不在进行中的任务中
      case Some(state) =>
        state match {
          case _: LogCleaningPaused => // 检查是否为暂停状态（任何暂停计数）
            true
          case _ => // 其他所有状态
            false
        }
    }
  }

  /**
   * 检查分区的清理是否已被中止。如果已中止，则抛出异常。
   * 
   * 应用场景：
   * 1. 在清理操作的关键点检查是否需要中止
   * 2. 确保清理任务能够及时响应中止请求
   * 3. 用于实现清理任务的可控性
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 使用isCleaningInState检查是否处于已中止状态
   * 3. 如果确实已中止，抛出LogCleaningAbortedException异常
   * 4. 异常会被上层捕获并处理，确保清理任务能够正确停止
   */
  def checkCleaningAborted(topicPartition: TopicPartition): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      // 检查是否处于已中止状态，如果是则抛出异常
      if (isCleaningInState(topicPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  /**
   * 更新检查点文件，根据需要添加或删除分区。
   * 
   * 应用场景：
   * 1. 在日志清理完成后更新分区的检查点偏移量
   * 2. 在分区迁移过程中更新检查点信息
   * 3. 在删除分区时移除检查点信息
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 读取当前检查点并过滤出有效的分区
   * 3. 根据参数更新检查点信息
   * 4. 将更新后的检查点写入文件
   *
   * @param dataDir 要更新的文件对象
   * @param partitionToUpdateOrAdd 要更新的[TopicPartition, Long]映射数据，如果是删除操作则传入"none"
   * @param partitionToRemove 要删除的TopicPartition
   */
  def updateCheckpoints(dataDir: File,
                        partitionToUpdateOrAdd: Option[(TopicPartition, JLong)] = None,
                        partitionToRemove: Option[TopicPartition] = None): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      val checkpoint = checkpoints(dataDir) // 获取指定目录的检查点
      if (checkpoint != null) {
        try {
          // 读取当前检查点并过滤出仍然存在的分区
          val currentCheckpoint = checkpoint.read().asScala.filter { case (tp, _) => logs.keys.contains(tp) }.toMap
          // 如果有需要删除的分区，从检查点中移除
          var updatedCheckpoint = partitionToRemove match {
            case Some(topicPartition) => currentCheckpoint - topicPartition
            case None => currentCheckpoint
          }
          // 如果有需要更新或添加的分区，更新检查点
          updatedCheckpoint = partitionToUpdateOrAdd match {
            case Some(updatedOffset) => updatedCheckpoint + updatedOffset
            case None => updatedCheckpoint
          }

          // 将更新后的检查点写入文件
          checkpoint.write(updatedCheckpoint.asJava)
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
        }
      }
    }
  }

  /**
   * 修改主题分区的检查点目录，从源日志目录删除数据，并将数据添加到目标日志目录。
   * 
   * 应用场景：
   * 1. 分区迁移时更新检查点位置
   * 2. 日志目录重新平衡时调整检查点
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 读取源目录的检查点信息
   * 3. 在源目录和目标目录更新检查点
   * 4. 处理不可清理分区的迁移
   */
  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      try {
        // 尝试从源目录读取检查点偏移量
        checkpoints.get(sourceLogDir).flatMap(_.read().asScala.get(topicPartition)) match {
          case Some(offset) =>
            // 从源目录移除检查点数据
            debug(s"Removing the partition offset data in checkpoint file for '$topicPartition' " +
              s"from ${sourceLogDir.getAbsoluteFile} directory.")
            updateCheckpoints(sourceLogDir, partitionToRemove = Option(topicPartition))

            // 在目标目录添加检查点数据
            debug(s"Adding the partition offset data in checkpoint file for '$topicPartition' " +
              s"to ${destLogDir.getAbsoluteFile} directory.")
            updateCheckpoints(destLogDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
          case None => // 如果没有找到检查点数据，不进行操作
        }
      } catch {
        case e: KafkaStorageException =>
          error(s"Failed to access checkpoint file in dir ${sourceLogDir.getAbsolutePath}", e)
      }

      // 处理不可清理分区的迁移
      val logUncleanablePartitions = uncleanablePartitions.getOrElse(sourceLogDir.toString, mutable.Set[TopicPartition]())
      if (logUncleanablePartitions.contains(topicPartition)) {
        logUncleanablePartitions.remove(topicPartition)
        markPartitionUncleanable(destLogDir.toString, topicPartition)
      }
    }
  }

  /**
   * 停止指定目录中日志的清理操作。
   * 
   * 应用场景：
   * 1. 日志目录发生故障时停止清理
   * 2. 磁盘空间不足时停止特定目录的清理
   * 
   * 实现细节：
   * 1. 记录警告日志
   * 2. 从检查点映射中移除指定目录
   *
   * @param dir 日志目录的绝对路径
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping cleaning logs in dir $dir")
    inLock(lock) { // 使用锁确保线程安全
      // 从检查点映射中移除失败目录的检查点
      checkpoints = checkpoints.filter { case (k, _) => k.getAbsolutePath != dir }
    }
  }

  /**
   * 如果给定分区的检查点偏移量大于指定偏移量，则截断该检查点偏移量。
   * 
   * 应用场景：
   * 1. 日志截断后更新检查点
   * 2. 恢复到之前的检查点时
   * 
   * 实现细节：
   * 1. 检查分区是否启用了压缩
   * 2. 读取并比较检查点偏移量
   * 3. 必要时更新检查点
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: JLong): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      // 只处理启用了压缩的日志
      if (logs.get(topicPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        if (checkpoint != null) {
          val existing = checkpoint.read()
          // 如果当前检查点偏移量大于给定偏移量，则更新检查点
          if (existing.getOrDefault(topicPartition, 0L) > offset) {
            existing.put(topicPartition, offset)
            checkpoint.write(existing)
          }
        }
      }
    }
  }

  /**
   * 保存结束偏移量并从正在进行的集合中移除给定的日志（如果未被中止）。
   * 
   * 应用场景：
   * 1. 日志清理任务完成时
   * 2. 更新清理进度
   * 
   * 实现细节：
   * 1. 检查清理状态
   * 2. 根据状态执行相应操作
   * 3. 更新检查点或暂停清理
   */
  def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          // 清理成功完成，更新检查点并移除进行中状态
          updateCheckpoints(dataDir, partitionToUpdateOrAdd = Option(topicPartition, endOffset))
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          // 清理被中止，将状态设置为暂停
          inProgress.put(topicPartition, LogCleaningPaused(1))
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }

  /**
   * 完成删除操作，处理多个主题分区的清理状态。
   * 
   * 应用场景：
   * 1. 批量删除分区完成时
   * 2. 主题删除操作完成时
   * 
   * 实现细节：
   * 1. 遍历所有分区
   * 2. 根据清理状态更新进度
   * 3. 处理中止和暂停状态
   */
  def doneDeleting(topicPartitions: Iterable[TopicPartition]): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      topicPartitions.foreach {
        topicPartition =>
          inProgress.get(topicPartition) match {
            case Some(LogCleaningInProgress) =>
              // 删除成功完成，移除进行中状态
              inProgress.remove(topicPartition)
            case Some(LogCleaningAborted) =>
              // 删除被中止，将状态设置为暂停
              inProgress.put(topicPartition, LogCleaningPaused(1))
              pausedCleaningCond.signalAll()
            case None =>
              throw new IllegalStateException(s"State for partition $topicPartition should exist.")
            case s =>
              throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
          }
      }
    }
  }

  /**
   * 返回指定日志目录中不可清理分区的不可变集合。
   * 仅用于测试目的。
   * 
   * 实现细节：
   * 1. 创建空集合
   * 2. 在锁保护下获取不可清理分区
   * 3. 返回不可变集合
   */
  private[log] def uncleanablePartitions(logDir: String): Set[TopicPartition] = {
    var partitions: Set[TopicPartition] = Set()
    inLock(lock) { partitions ++= uncleanablePartitions.getOrElse(logDir, partitions) }
    partitions
  }

  /**
   * 将分区标记为不可清理。
   * 
   * 应用场景：
   * 1. 分区出现错误无法清理时
   * 2. 手动禁用分区清理时
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 获取或创建不可清理分区集合
   * 3. 添加分区到集合中
   */
  def markPartitionUncleanable(logDir: String, partition: TopicPartition): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      uncleanablePartitions.get(logDir) match {
        case Some(partitions) =>
          // 将分区添加到现有的不可清理集合中
          partitions.add(partition)
        case None =>
          // 创建新的不可清理集合并添加分区
          uncleanablePartitions.put(logDir, mutable.Set(partition))
      }
    }
  }

  /**
   * 检查指定的分区是否为不可清理的分区
   * 
   * 应用场景：
   * 1. 在日志清理过程中，需要判断某个分区是否可以进行清理
   * 2. 在选择下一个要清理的分区时，用于过滤不可清理的分区
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 根据日志的父目录获取不可清理分区集合
   * 3. 检查该集合是否包含指定的主题分区
   * 
   * @param log 统一日志对象
   * @param topicPartition 主题分区
   * @return 如果分区不可清理返回true，否则返回false
   */
  private def isUncleanablePartition(log: UnifiedLog, topicPartition: TopicPartition): Boolean = {
    inLock(lock) { // 使用锁确保线程安全
      // 获取日志父目录对应的不可清理分区集合，并检查是否包含指定的主题分区
      uncleanablePartitions.get(log.parentDir).exists(partitions => partitions.contains(topicPartition))
    }
  }

  /**
   * 维护不可清理分区的集合，移除已删除的分区和空的分区集合
   * 
   * 应用场景：
   * 1. 定期清理不可清理分区列表，确保其包含的分区都是有效的
   * 2. 在分区被删除后，需要从不可清理分区列表中移除对应的记录
   * 
   * 实现细节：
   * 1. 使用锁保护并发访问
   * 2. 遍历所有不可清理分区集合，移除已不存在的分区
   * 3. 移除不包含任何分区的目录项
   */
  def maintainUncleanablePartitions(): Unit = {
    inLock(lock) { // 使用锁确保线程安全
      // 遍历每个目录的不可清理分区集合，移除已删除的分区
      uncleanablePartitions.values.foreach { partitions =>
        partitions.filterInPlace(logs.contains)
      }

      // 移除不包含任何分区的目录项
      uncleanablePartitions.filterInPlace {
        case (_, partitions) => partitions.nonEmpty
      }
    }
  }

  /**
   * 移除所有注册的度量指标
   * 
   * 应用场景：
   * 1. 在关闭日志清理管理器时，需要清理所有注册的度量指标
   * 2. 在重新配置度量指标时，需要先移除旧的度量指标
   * 
   * 实现细节：
   * 1. 移除没有标签的度量指标
   * 2. 移除带有标签的度量指标
   * 3. 清空度量指标标签映射
   */
  def removeMetrics(): Unit = {
    // 移除没有标签的度量指标
    GaugeMetricNameNoTag.foreach(metricsGroup.removeMetric)
    // 移除带有标签的度量指标
    gaugeMetricNameWithTag.asScala.foreach { metricNameAndTags =>
      metricNameAndTags._2.asScala.foreach { tag =>
        metricsGroup.removeMetric(metricNameAndTags._1, tag)
      }
    }
    // 清空度量指标标签映射
    gaugeMetricNameWithTag.clear()
  }
}

/**
 * 用于表示日志可清理的脏偏移量范围以及是否需要更新检查点的辅助类
 *
 * 应用场景：
 * 1. 在计算日志清理范围时，用于存储清理的起始和结束偏移量
 * 2. 在需要强制更新检查点时，提供更新标志
 *
 * @param firstDirtyOffset 开始清理的起始偏移量（包含）
 * @param firstUncleanableDirtyOffset 清理的结束偏移量（不包含）
 * @param forceUpdateCheckpoint 是否需要强制更新检查点，如果为true，检查点将被重置为firstDirtyOffset
 */
private case class OffsetsToClean(firstDirtyOffset: Long,
                                  firstUncleanableDirtyOffset: Long,
                                  forceUpdateCheckpoint: Boolean = false) {
}

/**
 * LogCleanerManager的伴生对象，包含了一些常量定义和工具方法
 */
private[log] object LogCleanerManager extends Logging {
  // 不可清理分区数量的度量指标名称
  private val UncleanablePartitionsCountMetricName = "uncleanable-partitions-count"
  // 不可清理字节数的度量指标名称
  private val UncleanableBytesMetricName = "uncleanable-bytes"
  // 最大脏数据百分比的度量指标名称
  private val MaxDirtyPercentMetricName = "max-dirty-percent"
  // 距离上次运行时间的度量指标名称
  private val TimeSinceLastRunMsMetricName = "time-since-last-run-ms"

  // 用于测试的无标签度量指标名称集合
  private[log] val GaugeMetricNameNoTag = Set(
    MaxDirtyPercentMetricName,
    TimeSinceLastRunMsMetricName
  )

  /**
   * 检查日志是否同时启用了压缩和删除策略
   *
   * @param log 要检查的统一日志对象
   * @return 如果同时启用了压缩和删除返回true，否则返回false
   */
  private def isCompactAndDelete(log: UnifiedLog): Boolean = {
    log.config.compact && log.config.delete
  }

  /**
    * 计算日志需要进行压缩的最大延迟时间
    *
    * 应用场景：
    * 1. 用于确定日志段是否需要立即进行压缩
    * 2. 帮助实现基于时间的压缩策略
    * 3. 防止日志段过长时间未被压缩
    *
    * 实现细节：
    * 1. 获取从firstDirtyOffset开始的所有非活动（已满）的日志段
    * 2. 获取这些日志段中第一个批次的时间戳
    * 3. 找出最早的脏段时间戳
    * 4. 根据配置的最大压缩延迟时间计算清理截止时间
    * 5. 如果最早的脏段时间戳早于清理截止时间，返回延迟时间，否则返回0
    *
    * @param log 统一日志对象
    * @param firstDirtyOffset 第一个脏数据的偏移量
    * @param now 当前时间戳（毫秒）
    * @return 返回压缩延迟时间（毫秒），如果不需要压缩则返回0
    */
  private def maxCompactionDelay(log: UnifiedLog, firstDirtyOffset: Long, now: Long) : Long = {
    // 获取从firstDirtyOffset开始的所有非活动日志段
    val dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset)
    // 获取这些日志段中第一个批次的时间戳，并过滤掉无效值（小于等于0的时间戳）
    val firstBatchTimestamps = log.getFirstBatchTimestampForSegments(dirtyNonActiveSegments).stream.filter(_ > 0)

    // 找出最早的脏段时间戳，如果没有有效时间戳则使用Long.MaxValue
    val earliestDirtySegmentTimestamp = firstBatchTimestamps.min(Comparator.naturalOrder()).orElse(Long.MaxValue)

    // 获取配置的最大压缩延迟时间，确保不小于0
    val maxCompactionLagMs = math.max(log.config.maxCompactionLagMs, 0L)
    // 计算清理截止时间：当前时间减去最大压缩延迟时间
    val cleanUntilTime = now - maxCompactionLagMs

    // 如果最早的脏段时间戳早于清理截止时间，返回延迟时间
    if (earliestDirtySegmentTimestamp < cleanUntilTime)
      cleanUntilTime - earliestDirtySegmentTimestamp
    else
      0L // 不需要压缩，返回0
  }

  /**
    * 返回可以被清理的脏数据偏移量范围
    *
    * 应用场景：
    * 1. 在日志压缩过程中确定需要清理的数据范围
    * 2. 处理日志异常截断或损坏的情况
    * 3. 确保不会清理最新的或仍在写入的数据
    * 4. 实现基于时间的数据保留策略
    *
    * 实现细节：
    * 1. 首先处理异常情况：
    *    - 检查检查点偏移量是否有效
    *    - 处理日志截断和数据损坏的情况
    * 2. 确定不可清理的偏移量边界：
    *    - 不清理活动段（正在写入的段）
    *    - 不清理最后稳定偏移量之后的数据
    *    - 不清理在最小压缩延迟时间内的数据
    * 3. 返回可清理的偏移量范围和是否需要强制更新检查点
    *
    * @param log 需要清理的统一日志对象
    * @param lastCleanOffset 上次检查点的偏移量
    * @param now 清理操作的当前时间戳（毫秒）
    * @return 返回包含可清理日志部分的偏移量和是否需要更新日志检查点的信息
    */
  def cleanableOffsets(log: UnifiedLog, lastCleanOffset: Option[Long], now: Long): OffsetsToClean = {
    // 如果日志段异常截断导致检查点偏移量无效，则重置为日志起始偏移量并记录错误
    val (firstDirtyOffset, forceUpdateCheckpoint) = {
      val logStartOffset = log.logStartOffset
      // 获取检查点脏数据偏移量，如果不存在则使用日志起始偏移量
      val checkpointDirtyOffset = lastCleanOffset.getOrElse(logStartOffset)

      if (checkpointDirtyOffset < logStartOffset) {
        // 如果同时启用了压缩和删除，则不显示警告
        if (!isCompactAndDelete(log))
          warn(s"Resetting first dirty offset of ${log.name} to log start offset $logStartOffset " +
            s"since the checkpointed offset $checkpointDirtyOffset is invalid.")
        (logStartOffset, true) // 重置为日志起始偏移量，并标记需要更新检查点
      } else if (checkpointDirtyOffset > log.logEndOffset) {
        // 脏数据偏移量超过了日志结束偏移量，可能是由于日志末尾数据损坏
        // 保守处理：假设整个日志都需要清理
        warn(s"The last checkpoint dirty offset for partition ${log.name} is $checkpointDirtyOffset, " +
          s"which is larger than the log end offset ${log.logEndOffset}. Resetting to the log start offset $logStartOffset.")
        (logStartOffset, true) // 重置为日志起始偏移量，并标记需要更新检查点
      } else {
        (checkpointDirtyOffset, false) // 检查点有效，使用检查点偏移量
      }
    }

    // 获取配置的最小压缩延迟时间，确保不小于0
    val minCompactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // 找出第一个不能被清理的段。我们不能清理以下数据：
    // 1. 活动段（当前正在写入的段）
    // 2. 最后稳定偏移量之后的数据（包括高水位标记）
    // 3. 距离日志头部时间小于最小压缩延迟时间的段
    val firstUncleanableDirtyOffset: Long = Seq(

      // 不清理最后稳定偏移量之后的数据
      Some(log.lastStableOffset),

      // 活动段永远不可清理
      Option(log.activeSegment.baseOffset),

      // 找出第一个最大消息时间戳在最小延迟时间内的段
      if (minCompactionLagMs > 0) {
        // 获取脏数据日志段
        val dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset)
        dirtyNonActiveSegments.asScala.find { s =>
          // 检查段是否在最小压缩延迟时间内
          val isUncleanable = s.largestTimestamp > now - minCompactionLagMs
          debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} " +
            s"segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - minCompactionLagMs}; " +
            s"is uncleanable=$isUncleanable")
          isUncleanable
        }.map(_.baseOffset)
      } else None
    ).flatten.min // 取所有不可清理边界中的最小值

    debug(s"Finding range of cleanable offsets for log=${log.name}. Last clean offset=$lastCleanOffset " +
      s"now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset " +
      s"activeSegment.baseOffset=${log.activeSegment.baseOffset}")

    // 返回可清理的偏移量范围，确保firstDirtyOffset不大于firstUncleanableDirtyOffset
    OffsetsToClean(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableDirtyOffset), forceUpdateCheckpoint)
  }

  /**
   * 根据第一个脏数据偏移量和不可清理偏移量，计算日志中可清理的总字节数
   *
   * 应用场景：
   * 1. 评估日志清理的工作量
   * 2. 确定清理优先级
   * 3. 监控和报告清理进度
   * 4. 资源使用预估
   *
   * 实现细节：
   * 1. 找到第一个不可清理的日志段
   * 2. 获取该段的基准偏移量
   * 3. 计算可清理范围内所有日志段的总大小
   *
   * @param log 统一日志对象
   * @param firstDirtyOffset 第一个脏数据的偏移量
   * @param uncleanableOffset 不可清理的偏移量
   * @return 返回一个元组，包含：
   *         - 第一个不可清理的偏移量
   *         - 可清理的总字节数
   */
  def calculateCleanableBytes(log: UnifiedLog, firstDirtyOffset: Long, uncleanableOffset: Long): (Long, Long) = {
    // 获取第一个不可清理的段，如果没有则使用活动段
    val firstUncleanableSegment = log.nonActiveLogSegmentsFrom(uncleanableOffset).asScala.headOption.getOrElse(log.activeSegment)
    // 获取不可清理段的基准偏移量
    val firstUncleanableOffset = firstUncleanableSegment.baseOffset
    // 计算可清理范围内（从较小的偏移量到不可清理偏移量之间）所有段的总大小
    val cleanableBytes = log.logSegments(math.min(firstDirtyOffset, firstUncleanableOffset), firstUncleanableOffset).map(_.size.toLong).sum

    (firstUncleanableOffset, cleanableBytes) // 返回不可清理偏移量和可清理字节数
  }

}
