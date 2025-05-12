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

import kafka.log.remote.RemoteLogManager
import kafka.utils._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DescribeProducersResponseData
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.utils.{PrimitiveRef, Time, Utils}
import org.apache.kafka.common.{InvalidRecordException, KafkaException, TopicPartition, Uuid}
import org.apache.kafka.server.common.{OffsetAndEpoch, RequestLocal}
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.server.storage.log.{FetchIsolation, UnexpectedAppendOffsetException}
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.checkpoint.{LeaderEpochCheckpointFile, PartitionMetadataFile}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.LocalLog.SplitSegmentResult
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, BatchMetadata, CompletedTxn, FetchDataInfo, LastRecord, LeaderHwChange, LocalLog, LogAppendInfo, LogConfig, LogDirFailureChannel, LogFileUtils, LogLoader, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogSegment, LogSegments, LogStartOffsetIncrementReason, LogValidator, OffsetResultHolder, OffsetsOutOfOrderException, ProducerAppendInfo, ProducerStateManager, ProducerStateManagerConfig, RollParams, SegmentDeletionReason, VerificationGuard, UnifiedLog => JUnifiedLog}
import org.apache.kafka.storage.log.metrics.{BrokerTopicMetrics, BrokerTopicStats}

import java.io.{File, IOException}
import java.lang.{Long => JLong}
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ScheduledFuture}
import java.util.stream.Collectors
import java.util.{Collections, Optional, OptionalInt, OptionalLong}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOption, RichOptional, RichOptionalInt}

/**
 * 提供本地和分层日志段的统一视图的日志实现。
 *
 * 该日志由分层和本地段组成，其中分层部分是可选的。分层和本地段之间可能存在重叠。
 * 活动段始终保证是本地的。如果存在分层段，它们总是出现在日志的开头，后面是一个可选的重叠区域，
 * 然后是包括活动段在内的本地段。
 *
 * 注意：此类处理特定于分层段的状态和行为，以及结合分层和本地段的行为。
 * 特定于本地段的状态和行为由封装的LocalLog实例处理。
 *
 * @param logStartOffset 允许暴露给kafka客户端的最早偏移量。
 *                       logStartOffset可以通过以下方式更新：
 *                       - 用户的DeleteRecordsRequest请求
 *                       - broker的日志保留机制
 *                       - broker的日志截断操作
 *                       - broker的日志恢复过程
 *                       logStartOffset用于决定以下内容：
 *                       - 日志删除：nextOffset <= logStartOffset的LogSegment可以被删除。
 *                         如果活动段被删除，可能会触发日志滚动。
 *                       - 响应ListOffsetRequest时的最早偏移量。为避免用户寻找最早偏移量时出现OffsetOutOfRange异常，
 *                         我们确保logStartOffset <= log的highWatermark
 *                       其他活动（如日志清理）不受logStartOffset影响。
 * @param localLog 包含从磁盘恢复的非空日志段的LocalLog实例
 * @param brokerTopicStats Broker主题Yammer指标的容器
 * @param producerIdExpirationCheckIntervalMs 检查需要过期的生产者ID的频率
 * @param leaderEpochCache 包含与提供的logStartOffset和nextOffsetMetadata相关联的状态的LeaderEpochFileCache实例
 * @param producerStateManager 包含与提供的段相关联的状态的ProducerStateManager实例
 * @param _topicId 可选的主题ID UUID。仅在通过Partition.makeLeader或Partition.makeFollower首次创建日志时指定。
 *                重新加载日志时，此字段将通过读取partition.metadata中的主题ID值来填充。
 * @param remoteStorageSystemEnable 指示系统级远程日志存储是否启用的标志。
 */
@threadsafe
class UnifiedLog(@volatile var logStartOffset: Long,
                 private val localLog: LocalLog,
                 val brokerTopicStats: BrokerTopicStats,
                 val producerIdExpirationCheckIntervalMs: Int,
                 @volatile var leaderEpochCache: LeaderEpochFileCache,
                 val producerStateManager: ProducerStateManager,
                 @volatile private var _topicId: Option[Uuid],
                 val remoteStorageSystemEnable: Boolean = false,
                 @volatile private var logOffsetsListener: LogOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER) extends Logging with AutoCloseable {

  import kafka.log.UnifiedLog._

  // 为了兼容性，指标被定义在`Log`类下
  private val metricsGroup = new KafkaMetricsGroup(getClass.getPackage.getName, "Log")

  // 日志标识符，用于日志输出，包含分区和目录信息
  this.logIdent = s"[UnifiedLog partition=$topicPartition, dir=$parentDir] "

  /* 用于保护日志所有修改操作的锁对象 */
  private val lock = new Object
  // 用于记录验证指标的记录器
  private val validatorMetricsRecorder = newValidatorMetricsRecorder(brokerTopicStats.allTopicsStats)

  /* 不完整事务中的最早偏移量。用于在ReplicaManager中计算最后稳定偏移量(LSO)。
   * 需要注意的是，"真正的"第一个不稳定偏移量可能会从日志中被删除（通过记录或段删除）。
   * 在这种情况下，第一个不稳定偏移量将指向日志起始偏移量，这个偏移量实际上可能是已完成事务的一部分，
   * 或者根本不是事务的一部分。
   * 
   * 然而，由于我们使用LSO仅用于限制read_committed消费者获取已确定的数据（即已提交、已中止或非事务性数据），
   * 这种临时的不准确性是可以接受的，并且避免了在删除后扫描日志以找到每个正在进行的事务的第一个偏移量来计算新的第一个不稳定偏移量。
   * 
   * 但是，这可能会导致副本之间的不一致，这取决于它们开始复制日志的时间。
   * 在最坏的情况下，消费者可能会看到LSO回退。
   */
  @volatile private var firstUnstableOffsetMetadata: Option[LogOffsetMetadata] = None

  /* 跟踪当前的高水位，以确保包含高水位及以上偏移量的段不能被删除。
   * 这意味着只有当高水位等于日志结束偏移量时，活动段才能被删除
   * （对于持续负载的分区，这种情况可能永远不会发生）。
   * 这是为了防止日志起始偏移量（在获取响应中暴露）超过高水位。
   */
  @volatile private var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(logStartOffset)

  // 分区元数据文件，用于存储分区相关的元数据信息
  @volatile var partitionMetadataFile: Option[PartitionMetadataFile] = None

  // 本地日志的起始偏移量
  @volatile private[kafka] var _localLogStartOffset: Long = logStartOffset

  // 获取本地日志起始偏移量的方法
  def localLogStartOffset(): Long = _localLogStartOffset

  // 记录已复制到远程存储的段的最高偏移量（包含）
  @volatile private[kafka] var _highestOffsetInRemoteStorage: Long = -1L

  // 获取远程存储中最高偏移量的方法
  def highestOffsetInRemoteStorage(): Long = _highestOffsetInRemoteStorage

  // 在对象初始化时执行的本地代码块
  locally {
    // 内部函数：更新本地日志起始偏移量
    def updateLocalLogStartOffset(offset: Long): Unit = {
      // 设置新的本地日志起始偏移量
      _localLogStartOffset = offset

      // 如果高水位小于新的偏移量，更新高水位
      if (highWatermark < offset) {
        updateHighWatermark(offset)
      }

      // 如果恢复点小于新的偏移量，更新恢复点
      if (this.recoveryPoint < offset) {
        localLog.updateRecoveryPoint(offset)
      }
    }

    // 初始化分区元数据
    initializePartitionMetadata()
    // 更新日志起始偏移量
    updateLogStartOffset(logStartOffset)
    // 更新本地日志起始偏移量为logStartOffset和第一个段基础偏移量的最大值
    updateLocalLogStartOffset(math.max(logStartOffset, localLog.segments.firstSegmentBaseOffset.orElse(0L)))
    // 如果远程日志未启用，使用本地日志起始偏移量
    if (!remoteLogEnabled())
      logStartOffset = localLogStartOffset()
    // 可能需要增加第一个不稳定偏移量
    maybeIncrementFirstUnstableOffset()
    // 初始化主题ID
    initializeTopicId()

    // 通知监听器高水位已更新
    logOffsetsListener.onHighWatermarkUpdated(highWatermarkMetadata.messageOffset)
  }

  /**
   * 设置日志偏移量监听器
   * @param listener 要设置的监听器实例
   */
  def setLogOffsetsListener(listener: LogOffsetsListener): Unit = {
    logOffsetsListener = listener
  }

  /**
   * 从远程层更新日志起始偏移量
   * @param remoteLogStartOffset 远程日志的起始偏移量
   */
  def updateLogStartOffsetFromRemoteTier(remoteLogStartOffset: Long): Unit = {
    // 如果远程日志存储未启用，忽略此调用
    if (!remoteLogEnabled()) {
      error("Ignoring the call as the remote log storage is disabled")
      return
    }
    // 尝试增加日志起始偏移量，原因是段删除
    maybeIncrementLogStartOffset(remoteLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion)
  }

  /**
   * 检查是否启用了远程日志存储功能
   * 
   * @return 如果启用了远程日志存储则返回true，否则返回false
   */
  def remoteLogEnabled(): Boolean = {
    UnifiedLog.isRemoteLogEnabled(remoteStorageSystemEnable, config, topicPartition.topic())
  }

  /**
   * Initialize topic ID information for the log by maintaining the partition metadata file and setting the in-memory _topicId.
   * Set _topicId based on a few scenarios:
   *   - Recover topic ID if present. Ensure we do not try to assign a provided topicId that is inconsistent
   *     with the ID on file.
   *   - If we were provided a topic ID when creating the log and one does not yet exist
   *     set _topicId and write to the partition metadata file.
   * 
   * 通过维护分区元数据文件并设置内存中的_topicId来初始化日志的主题ID信息。
   * 基于以下场景设置_topicId：
   *   - 如果存在主题ID则恢复它。确保不会尝试分配与文件中的ID不一致的topicId。
   *   - 如果在创建日志时提供了主题ID且当前不存在主题ID，则设置_topicId并写入分区元数据文件。
   */
  private def initializeTopicId(): Unit =  {
    // 获取分区元数据文件，如果未初始化则抛出异常
    val partMetadataFile = partitionMetadataFile.getOrElse(
      throw new KafkaException("The partitionMetadataFile should have been initialized"))

    if (partMetadataFile.exists()) {
      // 如果元数据文件存在，读取文件中的主题ID
      val fileTopicId = partMetadataFile.read().topicId
      // 如果内存中已有主题ID且与文件中的ID不一致，则抛出异常
      if (_topicId.isDefined && !_topicId.contains(fileTopicId))
        throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
          s"but log already contained topic ID $fileTopicId")

      // 使用文件中的主题ID更新内存中的值
      _topicId = Some(fileTopicId)
    } else {
      // 如果元数据文件不存在，将内存中的主题ID（如果有）写入文件
      _topicId.foreach(partMetadataFile.record)
      // 调度一次性任务以刷新元数据文件
      scheduler.scheduleOnce("flush-metadata-file", () => maybeFlushMetadataFile())
    }
  }

  /**
   * 获取主题ID
   * @return 主题ID的Option包装，如果未设置则返回None
   */
  def topicId: Option[Uuid] = _topicId

  /**
   * 获取日志目录
   * @return 日志文件目录
   */
  def dir: File = localLog.dir

  /**
   * 获取父目录路径
   * @return 父目录的字符串表示
   */
  def parentDir: String = localLog.parentDir

  /**
   * 获取父目录文件对象
   * @return 父目录的File对象
   */
  def parentDirFile: File = localLog.parentDirFile

  /**
   * 获取日志名称
   * @return 日志名称字符串
   */
  def name: String = localLog.name

  /**
   * 获取恢复点偏移量
   * @return 恢复点的偏移量值
   */
  def recoveryPoint: Long = localLog.recoveryPoint

  /**
   * 获取主题分区信息
   * @return 主题分区对象
   */
  def topicPartition: TopicPartition = localLog.topicPartition

  /**
   * 获取时间实例
   * @return Time实例
   */
  def time: Time = localLog.time

  /**
   * 获取调度器实例
   * @return Scheduler实例
   */
  def scheduler: Scheduler = localLog.scheduler

  /**
   * 获取日志配置
   * @return 日志配置对象
   */
  def config: LogConfig = localLog.config

  /**
   * 获取日志目录失败通道
   * @return 日志目录失败通道实例
   */
  def logDirFailureChannel: LogDirFailureChannel = localLog.logDirFailureChannel

  /**
   * 更新日志配置
   * @param newConfig 新的日志配置
   * @return 旧的日志配置
   */
  def updateConfig(newConfig: LogConfig): LogConfig = {
    // 保存当前配置
    val oldConfig = localLog.config
    // 更新为新配置
    localLog.updateConfig(newConfig)
    // 返回旧配置
    oldConfig
  }

  /**
   * 获取当前的高水位标记偏移量
   * 高水位标记(High Watermark)是Kafka用来保证数据一致性的重要机制
   * 消费者只能看到高水位标记之前的消息
   */
  def highWatermark: Long = highWatermarkMetadata.messageOffset

  /**
   * 更新高水位标记到新的偏移量。新的高水位标记会被限制在日志起始偏移量和日志结束偏移量之间。
   * 这个方法通常由Leader副本在初始化高水位标记时调用。
   *
   * @param hw 建议的新高水位标记值
   * @return 更新后的高水位标记偏移量
   */
  def updateHighWatermark(hw: Long): Long = {
    // 将简单的偏移量转换为带有元数据的偏移量进行更新
    updateHighWatermark(new LogOffsetMetadata(hw))
  }

  /**
   * 使用偏移量元数据更新高水位标记。新的高水位标记会被限制在日志起始偏移量和日志结束偏移量之间。
   * 
   * @param highWatermarkMetadata 建议的带有元数据的新高水位标记
   * @return 更新后的高水位标记偏移量
   */
  def updateHighWatermark(highWatermarkMetadata: LogOffsetMetadata): Long = {
    // 获取日志的结束偏移量元数据
    val endOffsetMetadata = localLog.logEndOffsetMetadata
    // 确保新的高水位标记在有效范围内
    val newHighWatermarkMetadata = if (highWatermarkMetadata.messageOffset < logStartOffset) {
      // 如果小于日志起始偏移量，则使用日志起始偏移量
      new LogOffsetMetadata(logStartOffset)
    } else if (highWatermarkMetadata.messageOffset >= endOffsetMetadata.messageOffset) {
      // 如果大于等于日志结束偏移量，则使用日志结束偏移量
      endOffsetMetadata
    } else {
      // 在有效范围内，使用提供的值
      highWatermarkMetadata
    }

    // 更新高水位标记元数据并返回新的偏移量
    updateHighWatermarkMetadata(newHighWatermarkMetadata)
    newHighWatermarkMetadata.messageOffset
  }

  /**
   * 仅当新值大于旧值时才更新高水位标记。如果更新值大于日志结束偏移量，则会抛出错误。
   * 
   * 这个方法通常由Leader副本在更新Follower的获取偏移量后调用，用于更新高水位标记。
   * 它确保了高水位标记的单调递增特性，这对于维护数据一致性至关重要。
   *
   * @return 如果更新成功，返回旧的高水位标记
   */
  def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata] = {
    // 检查新的高水位标记是否超过日志结束偏移量
    if (newHighWatermark.messageOffset > logEndOffset)
      throw new IllegalArgumentException(s"High watermark $newHighWatermark update exceeds current " +
        s"log end offset ${localLog.logEndOffsetMetadata}")

    lock.synchronized {
      // 获取当前的高水位标记元数据
      val oldHighWatermark = fetchHighWatermarkMetadata

      // 确保高水位标记单调递增。当新的偏移量元数据在更新的段上时，
      // 我们也会更新高水位标记，这种情况发生在日志滚动到新段时。
      if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
        (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
        updateHighWatermarkMetadata(newHighWatermark)
        Some(oldHighWatermark)
      } else {
        None
      }
    }
  }

  /**
   * 使用新值更新高水位标记。新的高水位标记会被限制在日志起始偏移量和日志结束偏移量之间。
   * 
   * 这个方法通常由Follower副本在从Leader复制数据后调用，用于更新其高水位标记。
   * 它是Follower副本保持与Leader副本数据一致性的关键机制之一。
   *
   * @return 如果高水位标记发生变化，返回新的高水位标记；否则返回None
   */
  def maybeUpdateHighWatermark(hw: Long): Option[Long] = {
    lock.synchronized {
      // 获取当前的高水位标记元数据
      val oldHighWatermark = highWatermarkMetadata
      // 尝试更新高水位标记
      updateHighWatermark(new LogOffsetMetadata(hw)) match {
        case oldHighWatermark.messageOffset =>
          // 如果高水位标记没有变化，返回None
          None
        case newHighWatermark =>
          // 如果高水位标记发生变化，返回新值
          Some(newHighWatermark)
      }
    }
  }

  /**
   * 获取当前高水位标记的偏移量和元数据。如果偏移量元数据未知，
   * 将在索引中查找并缓存结果。
   * 
   * 高水位标记是Kafka用于保证数据一致性的重要机制，它表示所有副本都已成功复制的最大偏移量。
   * 消费者只能看到高水位标记之前的消息，这确保了消费者不会读取到可能不一致的数据。
   */
  private def fetchHighWatermarkMetadata: LogOffsetMetadata = {
    // 检查内存映射缓冲区是否已关闭
    localLog.checkIfMemoryMappedBufferClosed()

    // 获取当前的高水位标记元数据
    val offsetMetadata = highWatermarkMetadata
    // 如果元数据只包含消息偏移量，需要获取完整的元数据信息
    if (offsetMetadata.messageOffsetOnly) {
      lock.synchronized {
        // 转换为完整的偏移量元数据
        val fullOffset = maybeConvertToOffsetMetadata(highWatermark)
        // 更新高水位标记元数据
        updateHighWatermarkMetadata(fullOffset)
        fullOffset
      }
    } else {
      offsetMetadata
    }
  }

  /**
   * 更新高水位标记元数据
   * 
   * 这个方法在以下情况下被调用：
   * 1. Leader副本初始化高水位标记时
   * 2. Follower副本完成消息复制后更新高水位标记时
   * 3. 日志截断操作后重置高水位标记时
   * 
   * @param newHighWatermark 新的高水位标记元数据
   */
  private def updateHighWatermarkMetadata(newHighWatermark: LogOffsetMetadata): Unit = {
    // 检查新的高水位标记是否为负数
    if (newHighWatermark.messageOffset < 0)
      throw new IllegalArgumentException("High watermark offset should be non-negative")

    lock synchronized {
      // 检查高水位标记是否出现非单调递增的情况
      if (newHighWatermark.messageOffset < highWatermarkMetadata.messageOffset) {
        warn(s"Non-monotonic update of high watermark from $highWatermarkMetadata to $newHighWatermark")
      }

      // 更新高水位标记元数据
      highWatermarkMetadata = newHighWatermark
      // 通知生产者状态管理器高水位标记已更新
      producerStateManager.onHighWatermarkUpdated(newHighWatermark.messageOffset)
      // 通知日志偏移量监听器高水位标记已更新
      logOffsetsListener.onHighWatermarkUpdated(newHighWatermark.messageOffset)
      // 可能需要增加第一个不稳定偏移量
      maybeIncrementFirstUnstableOffset()
    }
    trace(s"Setting high watermark $newHighWatermark")
  }

  /**
   * 获取第一个不稳定偏移量。与最后稳定偏移量不同，最后稳定偏移量总是有定义的，
   * 第一个不稳定偏移量只在存在进行中的事务时才存在。
   * 
   * 不稳定偏移量用于：
   * 1. 事务消息的隔离性保证
   * 2. 计算最后稳定偏移量(LSO)
   * 3. 防止消费者读取到未完成的事务消息
   *
   * @return 如果存在，返回第一个不稳定偏移量
   */
  private[log] def firstUnstableOffset: Option[Long] = firstUnstableOffsetMetadata.map(_.messageOffset)

  /**
   * 获取最后稳定偏移量的元数据
   * 
   * 最后稳定偏移量(LSO)是用于事务消息处理的重要概念：
   * 1. 对于非事务消息，消息写入后立即被认为是稳定的
   * 2. 对于事务消息，只有在写入对应的COMMIT或ABORT标记后才被认为是稳定的
   * 3. LSO不能超过高水位标记
   */
  private def fetchLastStableOffsetMetadata: LogOffsetMetadata = {
    // 检查内存映射缓冲区是否已关闭
    localLog.checkIfMemoryMappedBufferClosed()

    // 缓存当前的高水位标记，以避免并发更新导致范围检查失效
    val highWatermarkMetadata = fetchHighWatermarkMetadata

    // 根据第一个不稳定偏移量的情况来确定最后稳定偏移量
    firstUnstableOffsetMetadata match {
      // 如果存在不稳定偏移量，且小于高水位标记
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermarkMetadata.messageOffset =>
        if (offsetMetadata.messageOffsetOnly) {
          lock synchronized {
            // 转换为完整的偏移量元数据
            val fullOffset = maybeConvertToOffsetMetadata(offsetMetadata.messageOffset)
            // 如果第一个不稳定偏移量未变化，更新其元数据
            if (firstUnstableOffsetMetadata.contains(offsetMetadata))
              firstUnstableOffsetMetadata = Some(fullOffset)
            fullOffset
          }
        } else {
          offsetMetadata
        }
      // 如果不存在不稳定偏移量，使用高水位标记作为最后稳定偏移量
      case _ => highWatermarkMetadata
    }
  }




  /**
   * 存储度量指标名称和对应的标签映射
   * 用于跟踪已注册的度量指标，便于后续管理和清理
   */
  private var metricNames: Map[String, java.util.Map[String, String]] = Map.empty

  // 初始化度量指标
  newMetrics()

  /**
   * 创建新的度量指标
   * 为日志添加以下监控指标:
   * - 日志段数量(NumLogSegments)
   * - 日志起始偏移量(LogStartOffset)
   * - 日志结束偏移量(LogEndOffset)
   * - 日志大小(Size)
   * 每个指标都带有主题和分区的标签，用于标识具体的日志分区
   */
  private[log] def newMetrics(): Unit = {
    // 创建基础标签，包含主题和分区信息
    val tags = (Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++
      // 如果是未来日志，添加is-future标签
      (if (isFuture) Map("is-future" -> "true") else Map.empty)).asJava

    // 注册各项度量指标
    metricsGroup.newGauge(LogMetricNames.NumLogSegments, () => numberOfSegments, tags)
    metricsGroup.newGauge(LogMetricNames.LogStartOffset, () => logStartOffset, tags)
    metricsGroup.newGauge(LogMetricNames.LogEndOffset, () => logEndOffset, tags)
    metricsGroup.newGauge(LogMetricNames.Size, () => size, tags)

    // 保存度量指标名称和标签的映射关系
    metricNames = Map(LogMetricNames.NumLogSegments -> tags,
      LogMetricNames.LogStartOffset -> tags,
      LogMetricNames.LogEndOffset -> tags,
      LogMetricNames.Size -> tags)
  }

  /**
   * 定期检查并移除过期的生产者状态的调度任务
   * 该任务按照配置的时间间隔(producerIdExpirationCheckIntervalMs)定期执行
   */
  val producerExpireCheck: ScheduledFuture[_] = scheduler.schedule("PeriodicProducerExpirationCheck", () => removeExpiredProducers(time.milliseconds),
    producerIdExpirationCheckIntervalMs, producerIdExpirationCheckIntervalMs)

  /**
   * 移除过期的生产者状态
   * 该方法在测试中可见，用于手动触发过期生产者的清理
   *
   * @param currentTimeMs 当前时间戳，用于判断生产者是否过期
   */
  def removeExpiredProducers(currentTimeMs: Long): Unit = {
    lock synchronized {
      // 委托给生产者状态管理器执行实际的过期检查和清理
      producerStateManager.removeExpiredProducers(currentTimeMs)
    }
  }

  /**
   * 加载生产者状态
   * 在日志恢复或副本成为Leader时调用，用于重建生产者状态
   *
   * @param lastOffset 需要加载的最后偏移量
   */
  def loadProducerState(lastOffset: Long): Unit = lock synchronized {
    // 重建生产者状态直到指定的偏移量
    rebuildProducerState(lastOffset, producerStateManager)
    // 可能需要增加第一个不稳定偏移量
    maybeIncrementFirstUnstableOffset()
    // 更新高水位到日志的结束偏移量
    updateHighWatermark(localLog.logEndOffsetMetadata)
  }

  /**
   * 初始化分区元数据
   * 创建并初始化分区元数据文件，用于存储分区相关的持久化信息
   */
  private def initializePartitionMetadata(): Unit = lock synchronized {
    // 在日志目录下创建新的分区元数据文件
    val partitionMetadata = PartitionMetadataFile.newFile(dir)
    // 创建分区元数据文件的管理对象
    partitionMetadataFile = Some(new PartitionMetadataFile(partitionMetadata, logDirFailureChannel))
  }

  /**
   * 尝试刷新元数据文件
   * 将内存中的元数据变更持久化到磁盘
   */
  private def maybeFlushMetadataFile(): Unit = {
    // 如果存在分区元数据文件，则尝试刷新
    partitionMetadataFile.foreach(_.maybeFlush())
  }

  /**
   * 为主题分配ID
   * 仅在ZooKeeper集群中，当我们开始在现有主题上使用主题ID时使用
   *
   * @param topicId 要分配的主题ID
   * @throws InconsistentTopicIdException 如果主题已经有一个不同的ID
   */
  def assignTopicId(topicId: Uuid): Unit = {
    _topicId match {
      case Some(currentId) =>
        // 如果主题已有ID且与要分配的ID不同，抛出异常
        if (!currentId.equals(topicId)) {
          throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
            s"but log already contained topic ID $currentId")
        }

      case None =>
        // 如果主题没有ID，进行分配
        _topicId = Some(topicId)
        partitionMetadataFile match {
          case Some(partMetadataFile) =>
            // 如果元数据文件不存在，记录主题ID并安排刷新
            if (!partMetadataFile.exists()) {
              partMetadataFile.record(topicId)
              scheduler.scheduleOnce("flush-metadata-file", () => maybeFlushMetadataFile())
            }
          case _ => 
            // 如果分区已被删除，记录警告信息
            warn(s"The topic id $topicId will not be persisted to the partition metadata file " +
              "since the partition is deleted")
        }
    }
  }

  /**
   * 重新初始化Leader Epoch缓存
   * 在日志重新加载或Leader变更时调用，用于重建Leader Epoch信息
   */
  private def reinitializeLeaderEpochCache(): Unit = lock synchronized {
    // 使用工厂方法创建新的Leader Epoch缓存，保留现有缓存中的数据
    leaderEpochCache = UnifiedLog.createLeaderEpochCache(
      dir, topicPartition, logDirFailureChannel, Option.apply(leaderEpochCache), scheduler)
  }

  /**
   * 使用日志结束偏移量更新高水位
   * 在以下情况下需要更新:
   * 1. 日志截断后，高水位可能超过了日志结束偏移量
   * 2. 新的日志段被创建，需要更新偏移量元数据
   */
  private def updateHighWatermarkWithLogEndOffset(): Unit = {
    // 如果高水位大于等于日志结束偏移量，使用日志结束偏移量的元数据更新高水位
    if (highWatermark >= localLog.logEndOffset) {
      updateHighWatermarkMetadata(localLog.logEndOffsetMetadata)
    }
  }

  /**
   * 更新日志起始偏移量
   * 在日志截断、日志清理或用户请求删除消息时调用
   *
   * @param offset 新的日志起始偏移量
   */
  private def updateLogStartOffset(offset: Long): Unit = {
    // 更新日志起始偏移量
    logStartOffset = offset

    // 如果高水位小于新的起始偏移量，更新高水位
    if (highWatermark < offset) {
      updateHighWatermark(offset)
    }

    // 如果恢复点小于新的起始偏移量，更新恢复点
    if (localLog.recoveryPoint < offset) {
      localLog.updateRecoveryPoint(offset)
    }
  }

  /**
   * 更新远程存储中的最高偏移量
   * 用于跟踪已成功复制到远程存储的最新数据位置
   *
   * @param offset 要更新的远程存储最高偏移量
   */
  def updateHighestOffsetInRemoteStorage(offset: Long): Unit = {
    // 如果远程存储未启用，记录警告信息
    if (!remoteLogEnabled())
      warn(s"Unable to update the highest offset in remote storage with offset $offset since remote storage is not enabled. The existing highest offset is ${highestOffsetInRemoteStorage()}.")
    // 只有当新偏移量大于当前记录的最高偏移量时才更新
    else if (offset > highestOffsetInRemoteStorage()) _highestOffsetInRemoteStorage = offset
  }

  /**
   * 重建生产者状态直到指定的偏移量
   * 该方法可能在恢复过程中被调用，因此必须没有任何副作用，即不能更新任何日志特定的状态
   *
   * @param lastOffset 重建状态的目标偏移量
   * @param producerStateManager 生产者状态管理器实例
   */
  private def rebuildProducerState(lastOffset: Long,
                                   producerStateManager: ProducerStateManager): Unit = lock synchronized {
    // 检查内存映射缓冲区是否已关闭
    localLog.checkIfMemoryMappedBufferClosed()
    // 调用Java版本的UnifiedLog来重建生产者状态
    JUnifiedLog.rebuildProducerState(producerStateManager, localLog.segments, logStartOffset, lastOffset, time, false, logIdent)
  }

  /**
   * 检查是否存在延迟的事务
   * 线程安全方法，用于判断是否有超时未完成的事务
   *
   * @param currentTimeMs 当前时间戳
   * @return 如果存在延迟事务返回true，否则返回false
   */
  @threadsafe
  def hasLateTransaction(currentTimeMs: Long): Boolean = {
    producerStateManager.hasLateTransaction(currentTimeMs)
  }

  /**
   * 获取活跃生产者ID的数量
   * 线程安全方法，用于监控当前活跃的生产者数量
   *
   * @return 活跃生产者的数量
   */
  @threadsafe
  def producerIdCount: Int = producerStateManager.producerIdCount

  /**
   * 获取所有活跃生产者的状态信息
   * 用于响应DescribeProducers请求，提供生产者的详细状态
   *
   * @return 活跃生产者状态的序列
   */
  def activeProducers: Seq[DescribeProducersResponseData.ProducerState] = {
    lock synchronized {
      producerStateManager.activeProducers.asScala.map { case (producerId, state) =>
        // 构建生产者状态响应对象
        new DescribeProducersResponseData.ProducerState()
          .setProducerId(producerId)
          .setProducerEpoch(state.producerEpoch)
          .setLastSequence(state.lastSeq)
          .setLastTimestamp(state.lastTimestamp)
          .setCoordinatorEpoch(state.coordinatorEpoch)
          .setCurrentTxnStartOffset(state.currentTxnFirstOffset.orElse(-1L))
      }
    }.toSeq
  }

  /**
   * 获取活跃生产者及其最后序列号的映射
   * 用于内部日志管理，跟踪生产者的最新序列号
   *
   * @return 生产者ID到最后序列号的映射
   */
  private[log] def activeProducersWithLastSequence: mutable.Map[Long, Int] = lock synchronized {
    val result = mutable.Map[Long, Int]()
    producerStateManager.activeProducers.forEach { case (producerId, producerIdEntry) =>
      result.put(producerId.toLong, producerIdEntry.lastSeq)
    }
    result
  }

  /**
   * 获取活跃生产者的最后记录信息
   * 用于内部日志管理，跟踪生产者的最后写入记录
   *
   * @return 生产者ID到最后记录信息的映射
   */
  private[log] def lastRecordsOfActiveProducers: mutable.Map[Long, LastRecord] = lock synchronized {
    val result = mutable.Map[Long, LastRecord]()
    producerStateManager.activeProducers.forEach { case (producerId, producerIdEntry) =>
      // 获取最后数据偏移量，如果无效则为None
      val lastDataOffset = if (producerIdEntry.lastDataOffset >= 0) Some(producerIdEntry.lastDataOffset) else None
      // 创建最后记录对象
      val lastRecord = new LastRecord(
        if (lastDataOffset.isEmpty) OptionalLong.empty() else OptionalLong.of(lastDataOffset.get),
        producerIdEntry.producerEpoch)
      result.put(producerId.toLong, lastRecord)
    }
    result
  }

  /**
   * 尝试为给定的生产者ID启动事务验证
   * 如果事务尚未进行，则创建并返回VerificationGuard；否则返回哨兵VerificationGuard
   *
   * @param producerId 生产者ID
   * @param sequence 序列号
   * @param epoch 生产者纪元
   * @param supportsEpochBump 是否支持纪元提升
   * @return 验证保护对象
   */
  def maybeStartTransactionVerification(producerId: Long, sequence: Int, epoch: Short, supportsEpochBump: Boolean): VerificationGuard = lock synchronized {
    // 检查是否存在进行中的事务
    if (hasOngoingTransaction(producerId, epoch))
      VerificationGuard.SENTINEL
    else
      // 创建新的验证保护对象
      maybeCreateVerificationGuard(producerId, sequence, epoch, supportsEpochBump)
  }

  /**
   * 为给定的生产者ID创建验证状态条目，并始终返回验证守卫
   * 这个方法用于在生产者首次写入时初始化其验证状态
   * 
   * @param producerId 生产者ID
   * @param sequence 序列号
   * @param epoch 生产者纪元号
   * @param supportsEpochBump 是否支持纪元号递增
   * @return 验证守卫实例
   */
  private def maybeCreateVerificationGuard(producerId: Long,
                                           sequence: Int,
                                           epoch: Short,
                                           supportsEpochBump: Boolean): VerificationGuard = lock synchronized {
    // 调用生产者状态管理器创建验证状态条目并返回其验证守卫
    producerStateManager.maybeCreateVerificationStateEntry(producerId, sequence, epoch, supportsEpochBump).verificationGuard
  }

  /**
   * 获取指定生产者ID的验证守卫
   * 如果存在验证状态条目则返回其验证守卫，否则返回哨兵验证守卫
   * 验证守卫用于确保生产者消息的幂等性和事务完整性
   * 
   * @param producerId 生产者ID
   * @return 验证守卫实例
   */
  def verificationGuard(producerId: Long): VerificationGuard = lock synchronized {
    // 从生产者状态管理器获取验证状态条目
    val entry = producerStateManager.verificationStateEntry(producerId)
    // 如果条目存在则返回其验证守卫，否则返回哨兵值
    if (entry != null) entry.verificationGuard else VerificationGuard.SENTINEL
  }

  /**
   * 检查指定生产者是否有正在进行的事务
   * 注意：如果传入的生产者纪元号比存储的更新，则事务可能已经完成
   * 
   * @param producerId 生产者ID
   * @param producerEpoch 生产者纪元号
   * @return 如果有正在进行的事务则返回true
   */
  def hasOngoingTransaction(producerId: Long, producerEpoch: Short): Boolean = lock synchronized {
    // 获取活跃生产者的状态条目
    val entry = producerStateManager.activeProducers.get(producerId)
    // 检查条目是否存在、是否有事务的第一个偏移量、以及纪元号是否匹配
    entry != null && entry.currentTxnFirstOffset.isPresent && entry.producerEpoch() == producerEpoch
  }

  /**
   * 获取日志中的段数量
   * 注意：这是一个O(n)操作，因为需要遍历所有段
   * 
   * @return 日志段的数量
   */
  def numberOfSegments: Int = localLog.segments.numberOfSegments

  /**
   * 关闭日志
   * 日志的内存映射缓冲区（用于索引文件）会保持打开状态直到日志被删除
   * 这个方法会执行以下操作：
   * 1. 重置日志偏移量监听器
   * 2. 刷新元数据文件
   * 3. 检查内存映射缓冲区状态
   * 4. 取消生产者过期检查
   * 5. 创建生产者状态快照
   * 6. 关闭本地日志
   */
  override def close(): Unit = {
    debug("Closing log")
    lock synchronized {
      // 重置日志偏移量监听器为空操作监听器
      logOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER
      // 刷新分区元数据文件
      maybeFlushMetadataFile()
      // 检查内存映射缓冲区是否已关闭
      localLog.checkIfMemoryMappedBufferClosed()
      // 取消生产者过期检查任务
      producerExpireCheck.cancel(true)
      // 处理可能的IO异常
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // 在最后写入的偏移量处创建快照，以避免重启后需要扫描日志
        // 并确保不会意外触发升级优化（清理关闭文件在所有日志关闭后写入）
        producerStateManager.takeSnapshot()
      }
      // 关闭本地日志
      localLog.close()
    }
  }

  /**
   * 重命名本地日志的目录
   * 如果是由于StopReplica请求而进行的异步删除重命名，则shouldReinitialize参数应设为false，否则应设为true
   * 
   * @param name 日志目录的新名称
   * @param shouldReinitialize 重命名后是否需要重新初始化日志的元数据
   * @throws KafkaStorageException 如果重命名失败
   */
  def renameDir(name: String, shouldReinitialize: Boolean): Unit = {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        // 在重新初始化之前刷新分区元数据文件
        maybeFlushMetadataFile()
        // 尝试重命名目录
        if (localLog.renameDir(name)) {
          // 更新生产者状态管理器中的父目录
          producerStateManager.updateParentDir(dir)
          if (shouldReinitialize) {
            // 重新初始化Leader纪元缓存，以便LeaderEpochCheckpointFile.checkpoint能够正确引用
            // 重命名后的日志目录中的检查点文件
            reinitializeLeaderEpochCache()
            initializePartitionMetadata()
          } else {
            // 如果不需要重新初始化，则清除Leader纪元缓存和分区元数据文件
            leaderEpochCache.clear()
            partitionMetadataFile = None
          }
        }
      }
    }
  }

  /**
   * 关闭日志使用的文件句柄，但不写入磁盘
   * 当日志目录离线时调用此方法
   */
  def closeHandlers(): Unit = {
    debug("Closing handlers")
    lock synchronized {
      localLog.closeHandlers()
    }
  }

  /**
   * 作为Leader将消息集追加到本地日志的活动段，分配偏移量和分区Leader纪元
   * 这是Leader副本用于处理生产者写入请求的主要方法
   * 
   * @param records 要追加的记录
   * @param leaderEpoch Leader纪元号
   * @param origin 追加来源，影响所需的验证
   * @param requestLocal 请求本地实例
   * @param verificationGuard 验证守卫
   * @throws KafkaStorageException 如果由于I/O错误导致追加失败
   * @return 包含已追加消息的第一个和最后一个偏移量的信息
   */
  def appendAsLeader(records: MemoryRecords,
                     leaderEpoch: Int,
                     origin: AppendOrigin = AppendOrigin.CLIENT,
                     requestLocal: RequestLocal = RequestLocal.noCaching,
                     verificationGuard: VerificationGuard = VerificationGuard.SENTINEL): LogAppendInfo = {
    // 如果不是RAFT Leader的追加，则需要验证和分配偏移量
    val validateAndAssignOffsets = origin != AppendOrigin.RAFT_LEADER
    append(records, origin, validateAndAssignOffsets, leaderEpoch, Some(requestLocal), verificationGuard, ignoreRecordSize = false)
  }

  /**
   * 用于测试目的的特殊方法，允许使用旧版本的记录格式追加数据
   * 尽管从Apache Kafka 4.0开始我们总是使用v2版本的记录格式写入磁盘，
   * 但在此之前可能已经使用旧版本格式持久化到磁盘。为了测试这些场景，
   * 我们需要能够使用旧版本格式追加数据
   * 
   * 参见 #appendAsLeader
   */
  private[log] def appendAsLeaderWithRecordVersion(records: MemoryRecords, leaderEpoch: Int, recordVersion: RecordVersion): LogAppendInfo = {
    append(records, AppendOrigin.CLIENT, true, leaderEpoch, Some(RequestLocal.noCaching),
      VerificationGuard.SENTINEL, ignoreRecordSize = false, recordVersion.value)
  }

  /**
   * 作为Follower将消息集追加到本地日志的活动段，不分配偏移量和分区Leader纪元
   * 这是Follower副本用于复制Leader数据的主要方法
   * 
   * @param records 要追加的记录
   * @throws KafkaStorageException 如果由于I/O错误导致追加失败
   * @return 包含已追加消息的第一个和最后一个偏移量的信息
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records,
      origin = AppendOrigin.REPLICATION,  // 标记为复制来源
      validateAndAssignOffsets = false,   // 不需要验证和分配偏移量，因为这些都由Leader完成
      leaderEpoch = -1,                   // Follower不需要设置Leader纪元
      requestLocal = None,
      verificationGuard = VerificationGuard.SENTINEL,
      // 禁用记录大小验证，因为记录已经被Leader接受
      ignoreRecordSize = true)
  }

  /**
   * 将消息集追加到本地日志的活动段中，必要时会滚动到新的段。
   * 
   * 该方法主要负责为消息分配偏移量。但如果设置了assignOffsets=false标志，
   * 我们只会检查现有偏移量是否有效。
   *
   * 应用场景：
   * 1. 生产者发送消息时，需要将消息追加到日志中
   * 2. 副本同步时，Follower需要将从Leader复制的消息追加到本地日志
   * 3. 日志压缩时，需要将压缩后的消息追加到新的日志段
   *
   * 实现细节：
   * 1. 首先验证消息的有效性和格式
   * 2. 根据需要分配偏移量
   * 3. 更新生产者状态和事务状态
   * 4. 处理日志段滚动
   * 5. 更新各种元数据（如高水位、事务索引等）
   *
   * @param records 要追加的日志记录
   * @param origin 追加的来源，影响所需的验证
   * @param validateAndAssignOffsets 是否由日志分配偏移量，而不是直接使用给定的偏移量
   * @param leaderEpoch 分区的Leader Epoch，在Leader上分配偏移量时会应用到消息
   * @param requestLocal 如果validateAndAssignOffsets为true，则需要提供RequestLocal实例
   * @param verificationGuard 验证保护器，用于事务和幂等性验证
   * @param ignoreRecordSize 是否跳过记录大小的验证
   * @param toMagic 目标消息格式版本，默认为当前最新版本
   * @throws KafkaStorageException 如果由于I/O错误导致追加失败
   * @throws OffsetsOutOfOrderException 如果在records中发现乱序的偏移量
   * @throws UnexpectedAppendOffsetException 如果追加的第一个或最后一个偏移量小于下一个偏移量
   * @return 包含已追加消息的第一个和最后一个偏移量的信息
   */
  private def append(records: MemoryRecords,
                     origin: AppendOrigin,
                     validateAndAssignOffsets: Boolean,
                     leaderEpoch: Int,
                     requestLocal: Option[RequestLocal],
                     verificationGuard: VerificationGuard,
                     ignoreRecordSize: Boolean,
                     toMagic: Byte = RecordBatch.CURRENT_MAGIC_VALUE): LogAppendInfo = {
    // 确保在写入任何日志数据到磁盘之前，分区元数据文件已经写入到日志目录
    // 这样可以确保在发生故障时，任何日志数据都能以正确的主题ID恢复
    maybeFlushMetadataFile()

    // 分析和验证记录，检查消息格式、大小等是否合法
    val appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize, !validateAndAssignOffsets, leaderEpoch)

    // 如果没有有效消息或者这是最后一个追加条目的副本，则直接返回
    if (appendInfo.validBytes <= 0) appendInfo
    else {
      // 在追加到磁盘日志之前，修剪任何无效字节或部分消息
      var validRecords = trimInvalidBytes(records, appendInfo)

      // 消息有效，将它们插入到日志中
      // 使用同步锁确保线程安全
      lock synchronized {
        maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
          // 检查内存映射缓冲区是否已关闭
          localLog.checkIfMemoryMappedBufferClosed()
          
          if (validateAndAssignOffsets) {
            // 为消息集分配偏移量
            val offset = PrimitiveRef.ofLong(localLog.logEndOffset)
            appendInfo.setFirstOffset(offset.value)
            val validateAndOffsetAssignResult = try {
              // 根据配置和源压缩类型确定目标压缩类型
              val targetCompression = BrokerCompressionType.targetCompression(config.compression, appendInfo.sourceCompression())
              // 创建日志验证器，用于验证消息并分配偏移量
              val validator = new LogValidator(validRecords,
                topicPartition,
                time,
                appendInfo.sourceCompression,
                targetCompression,
                config.compact,
                toMagic,
                config.messageTimestampType,
                config.messageTimestampBeforeMaxMs,
                config.messageTimestampAfterMaxMs,
                leaderEpoch,
                origin
              )
              // 验证消息并分配偏移量
              validator.validateMessagesAndAssignOffsets(offset,
                validatorMetricsRecorder,
                requestLocal.getOrElse(throw new IllegalArgumentException(
                  "requestLocal should be defined if assignOffsets is true")
                ).bufferSupplier
              )
            } catch {
              case e: IOException =>
                throw new KafkaException(s"Error validating messages while appending to log $name", e)
            }

            // 更新验证后的记录和相关元数据
            validRecords = validateAndOffsetAssignResult.validatedRecords
            appendInfo.setMaxTimestamp(validateAndOffsetAssignResult.maxTimestampMs)
            appendInfo.setLastOffset(offset.value - 1)
            appendInfo.setRecordValidationStats(validateAndOffsetAssignResult.recordValidationStats)
            // 如果配置了使用日志追加时间作为消息时间戳，则设置日志追加时间
            if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
              appendInfo.setLogAppendTime(validateAndOffsetAssignResult.logAppendTimeMs)

            // 如果消息大小可能已更改（由于重新压缩或消息格式转换），则重新验证消息大小
            if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged) {
              validRecords.batches.forEach { batch =>
                if (batch.sizeInBytes > config.maxMessageSize) {
                  // 记录原始消息集大小而不是修剪后的大小
                  // 以保持与压缩前bytesRejectedRate记录的一致性
                  brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                  brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                  throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +
                    s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
                }
              }
            }
          } else {
            // 使用给定的偏移量
            if (appendInfo.firstOrLastOffsetOfFirstBatch < localLog.logEndOffset) {
              // 如果日志为空，我们可能仍然可以恢复
              // 例如：从Leader的日志起始偏移量获取数据，但该偏移量不是批次对齐的，
              // 这种情况可能是由AdminClient#deleteRecords()导致的
              val hasFirstOffset = appendInfo.firstOffset != UnifiedLog.UnknownOffset
              val firstOffset = if (hasFirstOffset) appendInfo.firstOffset else records.batches.iterator().next().baseOffset()

              val firstOrLast = if (hasFirstOffset) "First offset" else "Last offset of the first batch"
              throw new UnexpectedAppendOffsetException(
                s"Unexpected offset in append to $topicPartition. $firstOrLast " +
                  s"${appendInfo.firstOrLastOffsetOfFirstBatch} is less than the next offset ${localLog.logEndOffset}. " +
                  s"First 10 offsets in append: ${records.records.asScala.take(10).map(_.offset)}, last offset in" +
                  s" append: ${appendInfo.lastOffset}. Log start offset = $logStartOffset",
                firstOffset, appendInfo.lastOffset)
            }
          }

          // 使用Leader标记的Epoch更新Epoch缓存
          validRecords.batches.forEach { batch =>
            if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
              // 对于V2及以上版本的消息格式，更新Leader Epoch缓存
              assignEpochStartOffset(batch.partitionLeaderEpoch, batch.baseOffset)
            } else {
              // 在部分升级场景中，消息格式可能会临时回退到旧版本
              // 为了确保Leader选举的安全性，我们清除Epoch缓存
              // 这样在下次Leader选举后会回退到使用高水位进行截断
              if (leaderEpochCache.nonEmpty) {
                warn(s"Clearing leader epoch cache after unexpected append with message format v${batch.magic}")
                leaderEpochCache.clearAndFlush()
              }
            }
          }

          // 检查消息集大小是否超过配置的段大小限制
          if (validRecords.sizeInBytes > config.segmentSize) {
            throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
              s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
          }

          // 如果当前段已满，可能需要滚动到新的日志段
          val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

          // 创建日志偏移量元数据，包含批次的第一个偏移量、段基础偏移量和段大小
          val logOffsetMetadata = new LogOffsetMetadata(
            appendInfo.firstOrLastOffsetOfFirstBatch,
            segment.baseOffset,
            segment.size)

          // 现在我们有了有效的记录、已分配的偏移量和更新的时间戳
          // 需要验证生产者的幂等性/事务状态并收集一些元数据
          val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
            logOffsetMetadata, validRecords, origin, verificationGuard)

          maybeDuplicate match {
            case Some(duplicate) =>
              // 如果是重复的消息，更新追加信息中的偏移量和时间戳
              appendInfo.setFirstOffset(duplicate.firstOffset)
              appendInfo.setLastOffset(duplicate.lastOffset)
              appendInfo.setLogAppendTime(duplicate.timestamp)
              appendInfo.setLogStartOffset(logStartOffset)
            case None =>
              // 追加记录，并在追加后立即增加本地日志结束偏移量
              // 因为写入事务索引可能会失败，我们希望确保未来追加的偏移量仍然单调增长
              // 由此产生的事务索引不一致将在日志目录恢复后清理
              // 注意，如果追加到事务索引失败，ProducerStateManager的结束偏移量不会更新
              // 且最后稳定偏移量不会前进
              localLog.append(appendInfo.lastOffset, validRecords)
              updateHighWatermarkWithLogEndOffset()

              // 更新生产者状态
              updatedProducers.values.forEach(producerAppendInfo => producerStateManager.update(producerAppendInfo))

              // 使用真实的最后稳定偏移量更新事务索引
              // 使用READ_COMMITTED的消费者可见的最后偏移量将受此值和高水位的限制
              completedTxns.foreach { completedTxn =>
                val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
                segment.updateTxnIndex(completedTxn, lastStableOffset)
                producerStateManager.completeTxn(completedTxn)
              }

              // 始终更新最后的生产者ID映射偏移量，以便快照反映当前偏移量
              // 即使没有写入任何幂等数据
              producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

              // 更新第一个不稳定偏移量（用于计算LSO - 最后稳定偏移量）
              maybeIncrementFirstUnstableOffset()

              trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
                s"first offset: ${appendInfo.firstOffset}, " +
                s"next offset: ${localLog.logEndOffset}, " +
                s"and messages: $validRecords")

              // 如果未刷新的消息数量超过配置的刷新间隔，则执行刷新操作
              if (localLog.unflushedMessages >= config.flushInterval) flush(false)
          }
          appendInfo
        }
      }
    }
  }

  /**
   * 为指定的Leader Epoch分配起始偏移量
   * 
   * 应用场景：
   * 1. 当新的Leader被选举出来时，需要记录新的Epoch和对应的起始偏移量
   * 2. 在消息追加过程中，需要更新Epoch缓存
   * 
   * @param leaderEpoch Leader的任期号
   * @param startOffset 该任期的起始偏移量
   */
  def assignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit =
    leaderEpochCache.assign(leaderEpoch, startOffset)

  /**
   * 获取最新的Leader Epoch
   * 
   * @return 如果存在，返回最新的Leader Epoch；否则返回None
   */
  def latestEpoch: Option[Int] = leaderEpochCache.latestEpoch.toScala

  /**
   * 获取指定Leader Epoch对应的结束偏移量
   * 
   * 应用场景：
   * 1. Follower需要确定在Leader发生变更时要截断到哪个位置
   * 2. 用于处理Leader和Follower之间的日志截断和恢复
   * 
   * @param leaderEpoch 要查询的Leader Epoch
   * @return 如果找到，返回包含偏移量和Epoch的OffsetAndEpoch对象；否则返回None
   */
  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
    val entry = leaderEpochCache.endOffsetFor(leaderEpoch, logEndOffset)
    val (foundEpoch, foundOffset) = (entry.getKey, entry.getValue)
    if (foundOffset == UNDEFINED_EPOCH_OFFSET)
      None
    else
      Some(new OffsetAndEpoch(foundOffset, foundEpoch))
  }

  /**
   * 可能增加第一个不稳定偏移量
   * 
   * 应用场景：
   * 1. 用于事务处理，跟踪未完成事务的最早偏移量
   * 2. 帮助计算最后稳定偏移量(LSO)，这影响了READ_COMMITTED消费者可以看到的消息范围
   * 
   * 实现细节：
   * 1. 检查内存映射缓冲区状态
   * 2. 获取生产者状态管理器中的第一个不稳定偏移量
   * 3. 确保偏移量不小于日志起始偏移量
   * 4. 更新并记录变更
   */
  private def maybeIncrementFirstUnstableOffset(): Unit = lock synchronized {
    // 检查内存映射缓冲区是否已关闭
    localLog.checkIfMemoryMappedBufferClosed()

    // 从生产者状态管理器获取更新后的第一个不稳定偏移量
    val updatedFirstUnstableOffset = producerStateManager.firstUnstableOffset.toScala match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        // 确保不稳定偏移量不小于日志起始偏移量
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        Some(maybeConvertToOffsetMetadata(offset))
      case other => other
    }

    // 如果不稳定偏移量发生变化，更新并记录
    if (updatedFirstUnstableOffset != this.firstUnstableOffsetMetadata) {
      debug(s"First unstable offset updated to $updatedFirstUnstableOffset")
      this.firstUnstableOffsetMetadata = updatedFirstUnstableOffset
    }
  }

  /**
   * 可能增加本地日志的起始偏移量
   * 
   * 应用场景：
   * 1. 日志清理后需要更新起始偏移量
   * 2. 根据保留策略删除旧消息后更新起始偏移量
   * 3. 日志截断操作后更新起始偏移量
   * 
   * @param newLocalLogStartOffset 新的本地日志起始偏移量
   * @param reason 增加起始偏移量的原因
   */
  def maybeIncrementLocalLogStartOffset(newLocalLogStartOffset: Long, reason: LogStartOffsetIncrementReason): Unit = {
    lock synchronized {
      // 只有当新的起始偏移量大于当前值时才更新
      if (newLocalLogStartOffset > localLogStartOffset()) {
        _localLogStartOffset = newLocalLogStartOffset
        info(s"Incremented local log start offset to ${localLogStartOffset()} due to reason $reason")
      }
    }
  }

  /**
   * 如果提供的偏移量更大，则增加日志起始偏移量。
   *
   * 如果日志起始偏移量发生变化，此方法还会更新几个关键偏移量，以确保：
   * `logStartOffset <= logStableOffset <= highWatermark`。
   * 同时更新Leader Epoch缓存，确保该组件中引用的所有偏移量都指向此日志中的有效偏移量。
   *
   * 应用场景：
   * 1. 日志清理：当旧的消息被删除时，需要更新日志起始偏移量
   * 2. 日志截断：当日志发生截断操作时，可能需要增加起始偏移量
   * 3. 消费者组位移重置：当消费者组的位移被重置到一个更大的值时
   *
   * @throws OffsetOutOfRangeException 如果新的日志起始偏移量大于高水位标记
   * @return 如果日志起始偏移量被更新则返回true，否则返回false
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long,
                                   reason: LogStartOffsetIncrementReason): Boolean = {
    // 不需要立即将日志起始偏移量写入log-start-offset-checkpoint文件
    // 只有当所有同步副本在log.flush.start.offset.checkpoint.interval.ms时间内
    // 都以不干净的方式关闭时，deleteRecordsOffset才可能丢失。这种情况发生的概率很低。
    var updatedLogStartOffset = false
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        // 如果新的起始偏移量大于高水位标记，抛出异常
        // 这是为了确保日志的一致性，防止数据丢失
        if (newLogStartOffset > highWatermark)
          throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
            s"since it is larger than the high watermark $highWatermark")

        // 如果启用了远程日志存储
        if (remoteLogEnabled()) {
          // 设置本地日志起始偏移量为新偏移量和当前本地起始偏移量的较大值
          _localLogStartOffset = math.max(newLogStartOffset, localLogStartOffset())
        }

        // 检查内存映射缓冲区是否已关闭
        localLog.checkIfMemoryMappedBufferClosed()
        // 只有当新的起始偏移量大于当前起始偏移量时才进行更新
        if (newLogStartOffset > logStartOffset) {
          updatedLogStartOffset = true
          // 更新日志起始偏移量
          updateLogStartOffset(newLogStartOffset)
          // 记录日志
          info(s"Incremented log start offset to $newLogStartOffset due to $reason")
          // 从起始位置异步截断Leader Epoch缓存
          leaderEpochCache.truncateFromStartAsyncFlush(logStartOffset)
          // 通知生产者状态管理器日志起始偏移量已增加
          producerStateManager.onLogStartOffsetIncremented(newLogStartOffset)
          // 可能需要增加第一个不稳定偏移量
          maybeIncrementFirstUnstableOffset()
        }
      }
    }

    updatedLogStartOffset
  }

  /**
   * 分析和验证生产者状态，处理事务性消息和幂等性生产者的状态管理。
   * 
   * 应用场景：
   * 1. 事务消息写入：验证事务状态，确保消息属于有效的事务
   * 2. 幂等性生产：检测重复消息，避免重复写入
   * 3. 生产者状态维护：跟踪和更新生产者的状态信息
   *
   * @param appendOffsetMetadata 追加操作的偏移量元数据
   * @param records 要追加的消息记录
   * @param origin 追加来源（客户端/协调者/复制）
   * @param requestVerificationGuard 事务验证保护器
   * @return 包含更新的生产者信息、已完成事务列表和可能的重复批次元数据的元组
   */
  private def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,
                                              records: MemoryRecords,
                                              origin: AppendOrigin,
                                              requestVerificationGuard: VerificationGuard):
  (util.Map[JLong, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    // 存储需要更新的生产者状态信息
    val updatedProducers = new util.HashMap[JLong, ProducerAppendInfo]
    // 存储已完成的事务列表
    val completedTxns = ListBuffer.empty[CompletedTxn]
    // 记录在段内的相对位置
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    records.batches.forEach { batch =>
      // 只处理包含生产者ID的批次
      if (batch.hasProducerId) {
        // 如果是客户端生产请求，可能存在最多5个可能重复的批次
        // 如果发现重复，返回已追加批次的元数据给客户端
        if (origin == AppendOrigin.CLIENT) {
          val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

          val duplicateBatch = maybeLastEntry.flatMap(_.findDuplicateBatch(batch))
          if (duplicateBatch.isPresent) {
            return (updatedProducers, completedTxns.toList, Some(duplicateBatch.get()))
          }
        }

        // 处理来自客户端或协调者的事务性消息
        if (origin == AppendOrigin.CLIENT || origin == AppendOrigin.COORDINATOR) {
          // 验证事务性记录：确保记录属于正在进行的事务或已验证的事务状态
          // 这保证了事务性记录只能在事务协调者知晓的开放事务中写入日志
          // 如果没有正在进行的事务或正确的保护器，返回错误并不追加
          // 分为两个阶段 - 首次追加和后续追加
          //
          // 1. 首次追加：
          // - 创建VerificationGuard
          // - 向事务协调者发送验证请求
          // - 收到"已验证"响应后继续追加
          // - 使用唯一的VerificationGuard防止事务协调者响应和中止标记写入之间的竞争
          // - 防止ABA问题：事务验证开始->收到验证响应->写入中止标记->开始新事务->收到过期验证
          //
          // 2. 后续追加：
          // - 写入事务后，内存状态currentTxnFirstOffset被填充
          // - 该字段保持到事务完成或中止
          // - 基于第1步可以保证事务协调者知道事务且事务仍在进行
          // - 如果事务预期继续，不设置VerificationGuard
          // - 如果事务已中止，hasOngoingTransaction为false且requestVerificationGuard为哨兵值
          if (batch.isTransactional && !hasOngoingTransaction(batch.producerId, batch.producerEpoch()) && batchMissingRequiredVerification(batch, requestVerificationGuard))
            throw new InvalidTxnStateException("Record was not part of an ongoing transaction")
        }

        // 缓存每个事务开始的偏移量元数据
        // 这允许我们计算最后稳定偏移量，而不需要额外的索引查找
        val firstOffsetMetadata = if (batch.isTransactional)
          Optional.of(new LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          Optional.empty[LogOffsetMetadata]

        // 更新生产者状态并处理可能完成的事务
        val maybeCompletedTxn = JUnifiedLog.updateProducers(producerStateManager, batch, updatedProducers, firstOffsetMetadata, origin)
        maybeCompletedTxn.ifPresent(ct => completedTxns += ct)
      }

      // 更新段内相对位置
      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * 检查批次是否缺少必需的事务验证。
   * 
   * 应用场景：
   * 在事务性消息写入时，确保消息批次已经过事务协调者的验证。
   * 
   * @param batch 要检查的可变记录批次
   * @param requestVerificationGuard 请求验证保护器
   * @return 如果批次缺少必需的验证则返回true
   */
  private def batchMissingRequiredVerification(batch: MutableRecordBatch, requestVerificationGuard: VerificationGuard): Boolean = {
    // 检查是否启用了事务验证且不是控制批次
    // 同时验证生产者的验证保护器是否匹配请求的验证保护器
    producerStateManager.producerStateManagerConfig().transactionVerificationEnabled() && !batch.isControlBatch &&
      !verificationGuard(batch.producerId).verify(requestVerificationGuard)
  }

  /**
   * 分析和验证消息记录，执行以下验证：
   * <ol>
   * <li> 每条消息的CRC校验和是否匹配
   * <li> 每条消息的大小是否有效（如果ignoreRecordSize为false）
   * <li> 传入记录批次的序列号是否与现有状态和彼此一致
   * <li> 偏移量是否单调递增（如果requireOffsetsMonotonic为true）
   * </ol>
   *
   * 同时计算以下数量：
   * <ol>
   * <li> 消息集合中的第一个偏移量
   * <li> 消息集合中的最后一个偏移量
   * <li> 消息数量
   * <li> 有效字节数
   * <li> 偏移量是否单调递增
   * <li> 使用的压缩编解码器（如果使用多个，则使用最后一个）
   * </ol>
   *
   * 应用场景：
   * 1. 消息追加：在将消息写入日志前进行验证
   * 2. 复制验证：确保从Leader复制的消息符合要求
   * 3. 客户端请求验证：验证客户端发送的消息格式是否正确
   *
   * @param records 要验证的内存中的记录
   * @param origin 追加来源（客户端/复制/Raft Leader等）
   * @param ignoreRecordSize 是否忽略记录大小限制
   * @param requireOffsetsMonotonic 是否要求偏移量单调递增
   * @param leaderEpoch Leader的纪元号
   * @return 日志追加信息，包含验证结果和统计数据
   */
  private def analyzeAndValidateRecords(records: MemoryRecords,
                                        origin: AppendOrigin,
                                        ignoreRecordSize: Boolean,
                                        requireOffsetsMonotonic: Boolean,
                                        leaderEpoch: Int): LogAppendInfo = {
    // 初始化统计变量
    var validBytesCount = 0                                     // 有效字节计数
    var firstOffset = UnifiedLog.UnknownOffset                  // 第一个偏移量
    var lastOffset = -1L                                        // 最后一个偏移量
    var lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH // 最后的Leader纪元
    var sourceCompression = CompressionType.NONE                // 源压缩类型
    var monotonic = true                                        // 偏移量是否单调
    var maxTimestamp = RecordBatch.NO_TIMESTAMP                 // 最大时间戳
    var shallowOffsetOfMaxTimestamp = -1L                      // 最大时间戳对应的偏移量
    var readFirstMessage = false                                // 是否已读取第一条消息
    var lastOffsetOfFirstBatch = -1L                           // 第一个批次的最后偏移量

    // 遍历每个消息批次进行验证
    records.batches.forEach { batch =>
      // 验证Raft Leader的批次纪元
      if (origin == AppendOrigin.RAFT_LEADER && batch.partitionLeaderEpoch != leaderEpoch) {
        throw new InvalidRecordException("Append from Raft leader did not set the batch epoch correctly")
      }
      // 仅对V2及以上版本的客户端请求验证基础偏移量
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && origin == AppendOrigin.CLIENT && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch in the append to $topicPartition should " +
          s"be 0, but it is ${batch.baseOffset}")

      // 更新第一个偏移量（如果是第一条消息）
      // 对于魔数版本低于2的，使用最后偏移量以避免解压数据
      // 对于魔数版本2，可以直接从批次头部获取第一个偏移量
      if (!readFirstMessage) {
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
          firstOffset = batch.baseOffset
        lastOffsetOfFirstBatch = batch.lastOffset
        readFirstMessage = true
      }

      // 检查偏移量是否单调递增
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // 更新最后看到的偏移量和Leader纪元
      lastOffset = batch.lastOffset
      lastLeaderEpoch = batch.partitionLeaderEpoch

      // 验证消息大小是否有效
      val batchSize = batch.sizeInBytes
      if (!ignoreRecordSize && batchSize > config.maxMessageSize) {
        // 更新统计信息并抛出异常
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // 通过检查CRC验证消息的有效性
      if (!batch.isValid) {
        brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec.mark()
        throw new CorruptRecordException(s"Record is corrupt (stored crc = ${batch.checksum()}) in topic partition $topicPartition.")
      }

      // 更新最大时间戳信息
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        shallowOffsetOfMaxTimestamp = lastOffset
      }

      // 累计有效字节数
      validBytesCount += batchSize

      // 更新压缩类型信息
      val batchCompression = CompressionType.forId(batch.compressionType.id)
      // sourceCompression仅在Leader路径上使用，如果版本是v2或消息被压缩，则只包含一个批次
      if (batchCompression != CompressionType.NONE)
        sourceCompression = batchCompression
    }

    // 如果要求偏移量单调递增但不满足条件，抛出异常
    if (requireOffsetsMonotonic && !monotonic)
        throw new OffsetsOutOfOrderException(s"Out of order offsets found in append to $topicPartition: " +
          records.records.asScala.map(_.offset))

    // 设置最后的Leader纪元选项
    val lastLeaderEpochOpt: OptionalInt = if (lastLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH)
      OptionalInt.of(lastLeaderEpoch)
    else
      OptionalInt.empty()

    // 创建并返回日志追加信息
    new LogAppendInfo(firstOffset, lastOffset, lastLeaderEpochOpt, maxTimestamp,
      RecordBatch.NO_TIMESTAMP, logStartOffset, RecordValidationStats.EMPTY, sourceCompression,
      validBytesCount, lastOffsetOfFirstBatch, Collections.emptyList[RecordError], LeaderHwChange.NONE)
  }

  /**
   * 修剪消息集末尾的无效字节（如果存在的话）
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records 需要修剪的消息记录
   * @param info 消息集的通用信息
   * @return 修剪后的消息集。可能与传入的消息集相同，也可能不同。
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    // 获取有效字节数
    val validBytes = info.validBytes
    // 如果有效字节数小于0，说明记录批次长度非法，抛出异常
    if (validBytes < 0)
      throw new CorruptRecordException(s"Cannot append record batch with illegal length $validBytes to " +
        s"log for $topicPartition. A possible cause is a corrupted produce request.")
    // 如果有效字节数等于记录的总字节数，说明没有无效字节，直接返回原记录
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // 修剪无效字节：
      // 1. 复制原始缓冲区以避免修改原始数据
      // 2. 将缓冲区的限制设置为有效字节数
      // 3. 创建新的可读记录集
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  /**
   * 检查请求的偏移量是否小于日志的起始偏移量
   * 
   * @param offset 要检查的偏移量
   * @throws OffsetOutOfRangeException 如果请求的偏移量小于日志的起始偏移量
   */
  private def checkLogStartOffset(offset: Long): Unit = {
    // 如果请求的偏移量小于日志的起始偏移量，抛出异常
    if (offset < logStartOffset)
      throw new OffsetOutOfRangeException(s"Received request for offset $offset for partition $topicPartition, " +
        s"but we only have log segments starting from offset: $logStartOffset.")
  }

  /**
   * 从日志中读取消息。
   * Read messages from the log.
   *
   * @param startOffset 开始读取的偏移量
   * @param maxLength 要读取的最大字节数
   * @param isolation 读取隔离级别，控制允许读取的最大偏移量：
   *                 - LOG_END：可以读取到日志末尾
   *                 - HIGH_WATERMARK：只能读取到高水位标记
   *                 - TXN_COMMITTED：只能读取到最后稳定偏移量（已提交的事务）
   * @param minOneMessage 如果为true，即使第一条消息超过maxLength也会返回（如果存在）
   * @throws OffsetOutOfRangeException 如果startOffset超出日志结束偏移量或在日志起始偏移量之前
   * @return 获取数据信息，包括获取起始偏移量元数据和读取的消息
   */
  def read(startOffset: Long,
           maxLength: Int,
           isolation: FetchIsolation,
           minOneMessage: Boolean): FetchDataInfo = {
    // 检查请求的起始偏移量是否有效
    checkLogStartOffset(startOffset)
    // 根据隔离级别确定最大可读取的偏移量元数据
    val maxOffsetMetadata = isolation match {
      case FetchIsolation.LOG_END => localLog.logEndOffsetMetadata          // 可以读取到日志末尾
      case FetchIsolation.HIGH_WATERMARK => fetchHighWatermarkMetadata      // 只能读取到高水位标记
      case FetchIsolation.TXN_COMMITTED => fetchLastStableOffsetMetadata    // 只能读取到最后稳定偏移量
    }
    // 从本地日志中读取数据，并指定是否只读取已提交的事务
    localLog.read(startOffset, maxLength, minOneMessage, maxOffsetMetadata, isolation == FetchIsolation.TXN_COMMITTED)
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    localLog.collectAbortedTransactions(logStartOffset, startOffset, upperBoundOffset).asScala.toList
  }

  /**
   * 根据给定的时间戳获取偏移量
   * Get an offset based on the given timestamp
   * 
   * 返回的偏移量是时间戳大于或等于给定时间戳的第一条消息的偏移量。
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * 如果没有找到这样的消息，则返回日志结束偏移量。
   * If no such message is found, the log end offset is returned.
   *
   * 注意：OffsetRequest V0不使用此方法，OffsetRequest V0的行为保持不变，
   * 即它只根据日志段的最后修改时间返回时间戳。
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp 用于获取偏移量的目标时间戳
   * @param remoteLogManager 可选的RemoteLogManager实例（如果存在）
   * @return 偏移量结果持有者
   *         <ul>
   *           <li>当分区未启用远程存储时，包含时间戳大于或等于给定时间戳的第一条消息的偏移量；
   *               如果没有找到这样的消息，则返回None
   *           <li>当分区启用了远程存储时，包含异步完成的作业/任务future
   *           <li>所有特殊时间戳偏移量结果都会立即返回，不考虑远程存储
   *         </ul>
   */
  def fetchOffsetByTimestamp(targetTimestamp: Long, remoteLogManager: Option[RemoteLogManager] = None): OffsetResultHolder = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      // 对于最早和最新的时间戳，我们不需要返回时间戳
      if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP ||
        (!remoteLogEnabled() && targetTimestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)) {
        // 第一个缓存的epoch通常对应于日志起始偏移量，但我们必须验证这一点
        // 因为在消息格式版本升级后可能不成立，因为在旧格式中写入的日志条目将不可用epoch
        val earliestEpochEntry = leaderEpochCache.earliestEntry()
        val epochOpt = if (earliestEpochEntry.isPresent && earliestEpochEntry.get().startOffset <= logStartOffset) {
          Optional.of[Integer](earliestEpochEntry.get().epoch)
        } else Optional.empty[Integer]()

        // 返回最早的偏移量，不包含时间戳
        new OffsetResultHolder(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt))
      } else if (targetTimestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
        // 获取当前本地日志的起始偏移量
        val curLocalLogStartOffset = localLogStartOffset()

        // 获取对应的epoch信息
        val epochResult: Optional[Integer] = {
          val epochOpt = leaderEpochCache.epochForOffset(curLocalLogStartOffset)
          if (epochOpt.isPresent) Optional.of(epochOpt.getAsInt) else Optional.empty()
        }

        // 返回本地日志的起始偏移量
        new OffsetResultHolder(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, curLocalLogStartOffset, epochResult))
      } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
        // 获取最新的epoch信息
        val latestEpoch = leaderEpochCache.latestEpoch()
        val epoch = if (latestEpoch.isPresent) Optional.of[Integer](latestEpoch.getAsInt) else Optional.empty[Integer]()
        // 返回日志结束偏移量
        new OffsetResultHolder(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset, epoch))
      } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
        // 处理分层存储的最新时间戳请求
        if (remoteLogEnabled()) {
          // 获取远程存储中的最高偏移量
          val curHighestRemoteOffset = highestOffsetInRemoteStorage()
          val epochOpt = leaderEpochCache.epochForOffset(curHighestRemoteOffset)
          val epochResult: Optional[Integer] =
            if (epochOpt.isPresent) Optional.of(epochOpt.getAsInt)
            else if (curHighestRemoteOffset == -1) Optional.of(RecordBatch.NO_PARTITION_LEADER_EPOCH)
            else Optional.empty()
          new OffsetResultHolder(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, curHighestRemoteOffset, epochResult))
        } else {
          // 如果远程存储未启用，返回-1
          new OffsetResultHolder(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, -1L, Optional.of(-1)))
        }
      } else if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
        // 使用缓冲区避免竞态条件。toBuffer比大多数替代方案更快，
        // 提供常量时间访问，同时与并发集合一起使用时比toArray更安全
        val latestTimestampSegment = logSegments.asScala.toBuffer.maxBy[Long](_.maxTimestampSoFar)
        // 缓存时间戳和偏移量
        val maxTimestampSoFar = latestTimestampSegment.readMaxTimestampAndOffsetSoFar
        // 查找批次位置以避免额外的I/O
        val position = latestTimestampSegment.offsetIndex.lookup(maxTimestampSoFar.offset)
        // 查找具有最大时间戳的批次
        val timestampAndOffsetOpt = latestTimestampSegment.log.batchesFrom(position.position).asScala
          .find(_.maxTimestamp() == maxTimestampSoFar.timestamp)
          .flatMap(batch => batch.offsetOfMaxTimestamp().toScala.map(new TimestampAndOffset(batch.maxTimestamp(), _,
            Optional.of[Integer](batch.partitionLeaderEpoch()).filter(_ >= 0))))
        new OffsetResultHolder(timestampAndOffsetOpt.toJava)
      } else {
        // 需要搜索最大时间戳大于等于目标时间戳的第一个段（如果存在）
        if (remoteLogEnabled() && !isEmpty) {
          // 如果启用了远程存储但RemoteLogManager为空，抛出异常
          if (remoteLogManager.isEmpty) {
            throw new KafkaException("RemoteLogManager is empty even though the remote log storage is enabled.")
          }

          // 异步读取远程日志中的偏移量
          val asyncOffsetReadFutureHolder = remoteLogManager.get.asyncOffsetRead(topicPartition, targetTimestamp,
            logStartOffset, leaderEpochCache, () => searchOffsetInLocalLog(targetTimestamp, localLogStartOffset()))
          
          new OffsetResultHolder(Optional.empty(), Optional.of(asyncOffsetReadFutureHolder))
        } else {
          // 在本地日志中搜索偏移量
          new OffsetResultHolder(searchOffsetInLocalLog(targetTimestamp, logStartOffset).toJava)
        }
      }
    }
  }

  /**
   * 检查日志是否为空。
   * @return 当日志为空时返回True，否则返回false。
   */
  private[log] def isEmpty = {
    // 通过比较日志起始偏移量和结束偏移量是否相等来判断日志是否为空
    // 如果相等，说明日志中没有任何消息
    logStartOffset == logEndOffset
  }

  /**
   * 在本地日志中搜索指定时间戳对应的偏移量。
   * 应用场景：当消费者需要从特定时间点开始消费消息时，可以使用此方法找到对应的偏移量。
   *
   * @param targetTimestamp 目标时间戳
   * @param startOffset 搜索的起始偏移量
   * @return 返回找到的时间戳和偏移量的元组，如果未找到则返回None
   */
  private def searchOffsetInLocalLog(targetTimestamp: Long, startOffset: Long): Option[TimestampAndOffset] = {
    // 创建日志段的副本以避免并发条件。使用toBuffer比其他替代方案更快，
    // 提供常量时间访问，同时与并发集合一起使用时比toArray更安全。
    val segmentsCopy = logSegments.asScala.toBuffer
    // 查找第一个最大时间戳大于等于目标时间戳的日志段
    val targetSeg = segmentsCopy.find(_.largestTimestamp >= targetTimestamp)
    // 在找到的日志段中查找具体的时间戳和偏移量
    targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, startOffset).toScala)
  }

  /**
   * 根据消息偏移量查找其在日志中对应的偏移量元数据。
   * 应用场景：在进行日志段管理和消息查找时，需要获取偏移量的详细元数据信息。
   *
   * 处理逻辑：
   * 1. 如果消息偏移量小于日志起始偏移量或本地日志起始偏移量，则返回仅包含消息的元数据。
   * 2. 如果消息偏移量超出日志结束偏移量，则返回仅包含消息的元数据。
   * 3. 对于其他所有情况，返回日志中的完整偏移量元数据。
   *
   * @param offset 要查找的消息偏移量
   * @return 对应的偏移量元数据
   */
  private[log] def maybeConvertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      // 尝试从本地日志中获取偏移量元数据
      localLog.convertToOffsetMetadataOrThrow(offset)
    } catch {
      case _: OffsetOutOfRangeException =>
        // 如果偏移量超出范围，则创建一个只包含偏移量的基本元数据
        new LogOffsetMetadata(offset)
    }
  }

  /**
   * 删除本地日志段，从最旧的段开始向前删除，直到用户提供的谓词返回false或达到当前高水位标记所在的段。
   * 为了确保日志起始偏移量永远不会超过高水位标记，我们不会删除偏移量大于等于高水位标记的段。
   * 如果高水位标记尚未初始化，则没有段符合删除条件。
   *
   * 应用场景：日志清理、日志压缩等需要删除旧日志段的操作。
   *
   * @param predicate 一个函数，接收候选日志段和下一个更高的段（如果存在），当段可删除时返回true
   * @param reason 段删除的原因
   * @return 删除的段数量
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean,
                                reason: SegmentDeletionReason): Int = {
    lock synchronized {
      // 获取可删除的段列表
      val deletable = deletableSegments(predicate)
      // 如果有可删除的段，则执行删除操作
      if (deletable.nonEmpty)
        deleteSegments(deletable, reason)
      else
        0
    }
  }

  /**
   * 检查主题是否启用了分层存储且远程日志复制功能已启用。
   * 
   * @return 如果主题启用了分层存储且remote.log.copy.disable=false，则返回true
   */
  private def remoteLogEnabledAndRemoteCopyEnabled(): Boolean = {
    // 检查是否同时满足远程日志启用和远程日志复制未禁用的条件
    remoteLogEnabled() && !config.remoteLogCopyDisable()
  }

  /**
   * 从最旧的段开始查找可删除的段，直到用户提供的谓词返回false。
   * 空的最终段永远不会被返回。
   *
   * 应用场景：在日志清理和压缩过程中，需要确定哪些段可以安全删除。
   *
   * @param predicate 一个函数，接收候选日志段和下一个更高的段（如果存在），当段可删除时返回true
   * @return 准备删除的段集合
   */
  private[log] def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    // 判断段是否符合删除条件的内部函数
    def isSegmentEligibleForDeletion(nextSegmentOpt: Option[LogSegment], upperBoundOffset: Long): Boolean = {
      // 检查是否因日志起始偏移量增加而允许删除
      val allowDeletionDueToLogStartOffsetIncremented = nextSegmentOpt.isDefined && logStartOffset >= nextSegmentOpt.get.baseOffset
      // 段符合删除条件的情况：
      // 1. 已上传到远程存储
      // 2. 日志起始偏移量已增加到超过候选段中的最大偏移量
      // 注意：当远程日志复制被禁用时，将回退到使用retention.ms/bytes的本地日志检查
      if (remoteLogEnabledAndRemoteCopyEnabled()) {
        (upperBoundOffset > 0 && upperBoundOffset - 1 <= highestOffsetInRemoteStorage()) ||
          allowDeletionDueToLogStartOffsetIncremented
      } else {
        true
      }
    }

    // 如果本地日志段为空，返回空序列
    if (localLog.segments.isEmpty) {
      Seq.empty
    } else {
      // 用于存储可删除的段
      val deletable = ArrayBuffer.empty[LogSegment]
      val segmentsIterator = localLog.segments.values.iterator
      var segmentOpt = nextOption(segmentsIterator)
      var shouldRoll = false
      
      // 遍历所有段，检查每个段是否可删除
      while (segmentOpt.isDefined) {
        val segment = segmentOpt.get
        val nextSegmentOpt = nextOption(segmentsIterator)
        // 检查是否为最后一个空段
        val isLastSegmentAndEmpty = nextSegmentOpt.isEmpty && segment.size == 0
        // 确定上界偏移量
        val upperBoundOffset = if (nextSegmentOpt.nonEmpty) nextSegmentOpt.get.baseOffset() else logEndOffset
        // 确保不会删除偏移量大于等于高水位标记的段
        val predicateResult = highWatermark >= upperBoundOffset && predicate(segment, nextSegmentOpt)

        // 当活动段超过配置的保留策略时进行滚动。
        // 滚动后的段将在下一次迭代中符合删除条件。
        if (predicateResult && remoteLogEnabled() && nextSegmentOpt.isEmpty && segment.size > 0) {
          shouldRoll = true
        }
        // 如果段满足删除条件，则添加到可删除列表中
        if (predicateResult && !isLastSegmentAndEmpty && isSegmentEligibleForDeletion(nextSegmentOpt, upperBoundOffset)) {
          deletable += segment
          segmentOpt = nextSegmentOpt
        } else {
          segmentOpt = Option.empty
        }
      }
      
      // 如果需要滚动活动段，执行滚动操作
      if (shouldRoll) {
        info("Rolling the active segment to make it eligible for deletion")
        roll()
      }
      deletable
    }
  }

  /**
   * 增加日志的起始偏移量。
   * 应用场景：在日志清理或截断操作后，需要更新日志的起始位置。
   *
   * @param startOffset 新的起始偏移量
   * @param reason 增加起始偏移量的原因
   */
  private def incrementStartOffset(startOffset: Long, reason: LogStartOffsetIncrementReason): Unit = {
    // 根据是否启用远程日志和远程复制来选择不同的增加方式
    if (remoteLogEnabledAndRemoteCopyEnabled()) maybeIncrementLocalLogStartOffset(startOffset, reason)
    else maybeIncrementLogStartOffset(startOffset, reason)
  }

  /**
   * 删除指定的日志段集合
   * 
   * 应用场景：
   * 1. 日志清理：删除过期的日志段
   * 2. 日志压缩：删除不再需要的日志段
   * 3. 主题删除：删除整个主题的日志段
   * 
   * @param deletable 要删除的日志段集合
   * @param reason 删除原因，用于日志记录和监控
   * @return 实际删除的日志段数量
   */
  private def deleteSegments(deletable: Iterable[LogSegment], reason: SegmentDeletionReason): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      // 获取要删除的日志段数量
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // Kafka要求每个分区至少保留一个日志段，如果要删除所有日志段，需要先创建一个新的
        var segmentsToDelete = deletable
        if (localLog.segments.numberOfSegments == numToDelete) {
          // 创建新的日志段
          val newSegment = roll()
          // 如果最后一个要删除的日志段的基准偏移量与新创建的日志段相同
          if (deletable.last.baseOffset == newSegment.baseOffset) {
            warn(s"Empty active segment at ${deletable.last.baseOffset} was deleted and recreated due to $reason")
            // 从删除列表中移除最后一个日志段
            segmentsToDelete = deletable.dropRight(1)
          }
        }
        // 检查内存映射缓冲区是否已关闭
        localLog.checkIfMemoryMappedBufferClosed()
        if (segmentsToDelete.nonEmpty) {
          // 在删除日志段之前，更新本地日志起始偏移量
          val newLocalLogStartOffset = localLog.segments.higherSegment(segmentsToDelete.last.baseOffset()).get.baseOffset()
          incrementStartOffset(newLocalLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion)
          // 从日志中移除并删除指定的日志段
          localLog.removeAndDeleteSegments(segmentsToDelete.toList.asJava, true, reason)
        }
        // 异步删除生产者快照文件
        deleteProducerSnapshots(deletable.toList.asJava, asyncDelete = true)
      }
      numToDelete
    }
  }

  /**
   * 删除过期的日志段
   * 
   * 应用场景：
   * 1. 日志清理：定期清理过期的日志段，释放磁盘空间
   * 2. 日志维护：确保日志大小和保留时间符合配置要求
   * 
   * 删除条件：
   * 1. 如果启用了主题删除功能(config.delete=true)：
   *    - 删除超过保留时间的日志段
   *    - 删除导致日志大小超过保留大小的日志段
   * 2. 无论是否启用删除功能：
   *    - 删除位于日志起始偏移量之前的日志段
   * 
   * @return 删除的日志段总数
   */
  def deleteOldSegments(): Int = {
    if (config.delete) {
      // 如果启用了删除功能，执行所有类型的日志段删除
      deleteLogStartOffsetBreachedSegments() +  // 删除起始偏移量之前的日志段
        deleteRetentionSizeBreachedSegments() +  // 删除超过保留大小的日志段
        deleteRetentionMsBreachedSegments()     // 删除超过保留时间的日志段
    } else {
      // 如果未启用删除功能，只删除起始偏移量之前的日志段
      deleteLogStartOffsetBreachedSegments()
    }
  }

  /**
   * 删除超过保留时间的日志段
   * 
   * 应用场景：
   * 1. 时间基础的日志清理：删除超过指定保留时间的旧数据
   * 2. 合规要求：确保数据不会保留超过规定的时间
   * 
   * @return 删除的日志段数量
   */
  private def deleteRetentionMsBreachedSegments(): Int = {
    // 获取本地日志的保留时间配置
    val retentionMs = localRetentionMs(config, remoteLogEnabledAndRemoteCopyEnabled())
    // 如果保留时间小于0，表示永久保留，直接返回0
    if (retentionMs < 0) return 0
    // 获取当前时间戳
    val startMs = time.milliseconds

    // 定义判断日志段是否应该删除的函数
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      // 如果当前时间减去日志段最大时间戳大于保留时间，则应该删除
      val shouldDelete = startMs - segment.largestTimestamp > retentionMs
      debug(s"$segment retentionMs breached: $shouldDelete, startMs=$startMs, retentionMs=$retentionMs")
      shouldDelete
    }

    // 执行日志段删除操作
    deleteOldSegments(shouldDelete, RetentionMsBreach(this, remoteLogEnabledAndRemoteCopyEnabled()))
  }

  /**
   * 删除导致日志大小超过保留大小的日志段
   * 
   * 应用场景：
   * 1. 空间管理：确保日志不会占用过多磁盘空间
   * 2. 资源控制：在有限的存储资源下维护最新的数据
   * 
   * @return 删除的日志段数量
   */
  private def deleteRetentionSizeBreachedSegments(): Int = {
    // 获取本地日志的保留大小配置
    val retentionSize: Long = localRetentionSize(config, remoteLogEnabledAndRemoteCopyEnabled())
    // 如果保留大小小于0或当前大小小于保留大小，不需要删除
    if (retentionSize < 0 || size < retentionSize) return 0
    // 计算需要删除的字节数
    var diff = size - retentionSize

    // 定义判断日志段是否应该删除的函数
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      val segmentSize = segment.size
      // 如果删除当前段后仍超过保留大小，则应该删除
      val shouldDelete = diff - segmentSize >= 0
      debug(s"$segment retentionSize breached: $shouldDelete, log size before delete segment=$diff, after delete segment=${diff - segmentSize}")
      // 如果要删除，更新差值
      if (shouldDelete) {
        diff -= segmentSize
      }
      shouldDelete
    }

    // 执行日志段删除操作
    deleteOldSegments(shouldDelete, RetentionSizeBreach(this, remoteLogEnabledAndRemoteCopyEnabled()))
  }

  /**
   * 删除位于日志起始偏移量之前的日志段
   * 
   * 应用场景：
   * 1. 日志压缩：删除已经被压缩的旧数据
   * 2. 日志清理：删除不再需要的早期数据
   * 3. 远程存储：当启用远程存储时，删除已经复制到远程的本地日志段
   * 
   * @return 删除的日志段数量
   */
  private def deleteLogStartOffsetBreachedSegments(): Int = {
    // 定义判断日志段是否应该删除的函数
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      // 检查是否启用了远程日志存储
      val isRemoteLogEnabled = remoteLogEnabled()
      // 获取本地日志起始偏移量
      val localLSO = localLogStartOffset()
      // 如果下一个日志段的基准偏移量小于等于起始偏移量，则当前段应该删除
      // 当启用远程日志时使用localLSO，否则使用logStartOffset
      val shouldDelete = nextSegmentOpt.exists(_.baseOffset <= (if (isRemoteLogEnabled) localLSO else logStartOffset))
      debug(s"$segment logStartOffset breached: $shouldDelete, nextSegmentOpt=$nextSegmentOpt, " +
        s"${if (isRemoteLogEnabled) s"localLogStartOffset=$localLSO" else s"logStartOffset=$logStartOffset"}")
      shouldDelete
    }

    // 执行日志段删除操作
    deleteOldSegments(shouldDelete, StartOffsetBreach(this, remoteLogEnabled()))
  }

  def isFuture: Boolean = localLog.isFuture

  /**
   * The size of the log in bytes
   */
  def size: Long = localLog.segments.sizeInBytes

  /**
   * The log size in bytes for all segments that are only in local log but not yet in remote log.
   */
  def onlyLocalLogSegmentsSize: Long =
    UnifiedLog.sizeInBytes(logSegments.stream.filter(_.baseOffset >= highestOffsetInRemoteStorage()).collect(Collectors.toList[LogSegment]))

  /**
   * The number of segments that are only in local log but not yet in remote log.
   */
  def onlyLocalLogSegmentsCount: Long =
    logSegments.stream().filter(_.baseOffset >= highestOffsetInRemoteStorage()).count()

  /**
   * The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long =  localLog.logEndOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = localLog.logEndOffsetMetadata

  /**
   * 在必要时将日志滚动到新的空日志段
   * 
   * 应用场景：
   * 1. 日志分段管理：确保单个日志段不会过大，便于管理和清理
   * 2. 性能优化：通过分段提高日志读写性能
   * 3. 资源控制：限制单个日志段的大小和时间跨度
   * 
   * 日志段滚动条件（满足任一即可）：
   * 1. 当前日志段已满（大小达到配置的最大值）
   * 2. 当前日志段中第一条消息的时间戳距今已超过最大时间限制
   * 3. 索引已满（达到最大索引条目数）
   *
   * @param messagesSize 要追加的消息集合的总大小（字节）
   * @param appendInfo 日志追加相关的信息，包含时间戳、偏移量等
   * @return 当前活动的日志段（可能是新创建的）
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = lock synchronized {
    // 获取当前活动的日志段
    val segment = localLog.segments.activeSegment
    // 获取当前时间戳
    val now = time.milliseconds

    // 获取要追加的消息集合中的最大时间戳和最大偏移量
    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset

    // 检查是否需要滚动到新的日志段
    if (segment.shouldRoll(new RollParams(config.maxSegmentMs, config.segmentSize, appendInfo.maxTimestamp, appendInfo.lastOffset, messagesSize, now))) {
      // 记录滚动日志的详细信息
      debug(s"Rolling new log segment (log_size = ${segment.size}/${config.segmentSize}}, " +
        s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
        s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
        s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

      /*
       * 计算新日志段的起始偏移量：
       * - 对于未知的第一个偏移量（pre-V2消息格式），使用启发式方法：
       *   maxOffsetInMessages - Integer.MAX_VALUE
       * - 这样可以确保计算出的偏移量小于等于消息集合中的第一个偏移量
       * - 这种处理主要是为了避免Follower在日志追加时进行消息解压缩
       * - 旧的实现直接使用旧段的logEndOffset作为新段的起始偏移量，这在处理高度压缩的主题恢复时可能出现问题
       * - 特别是当两个连续消息的偏移量差值超过Integer.MAX_VALUE + 2时
       */
      val rollOffset = if (appendInfo.firstOffset == UnifiedLog.UnknownOffset)
        maxOffsetInMessages - Integer.MAX_VALUE
      else
        appendInfo.firstOffset

      // 创建新的日志段
      roll(Some(rollOffset))
    } else {
      // 如果不需要滚动，返回当前日志段
      segment
    }
  }

  /**
   * 将本地日志滚动到一个新的活动段。新段的起始偏移量可以通过expectedNextOffset指定，
   * 如果未指定则使用localLog.logEndOffset。这个操作会将索引裁剪到当前包含的条目的确切大小。
   * 
   * 应用场景：
   * 1. 当前活动段达到配置的大小限制时
   * 2. 强制创建新的日志段时
   * 3. 日志清理后需要重新组织日志段时
   *
   * @param expectedNextOffset 期望的下一个偏移量，可选参数
   * @return 新创建的日志段
   */
  def roll(expectedNextOffset: Option[Long] = None): LogSegment = lock synchronized {
    // 确定新段的起始偏移量
    val nextOffset : JLong = expectedNextOffset match {
      case Some(offset) => offset  // 使用指定的偏移量
      case None => 0L             // 如果未指定，使用0作为默认值
    }
    // 创建新的日志段
    val newSegment = localLog.roll(nextOffset)
    
    // 为了便于恢复，创建生产者状态的快照。
    // 将快照偏移量与新段偏移量对齐很有用，因为这确保我们可以通过从相应的快照文件开始扫描段数据来恢复该段。
    // 由于段基础偏移量可能实际上超前于当前生产者状态结束偏移量（对应于日志结束偏移量），
    // 我们在创建快照之前手动覆盖状态偏移量。
    producerStateManager.updateMapEndOffset(newSegment.baseOffset)
    
    // 避免潜在的昂贵的fsync调用，因为我们在这里获取了UnifiedLog#lock
    // 这可能会阻塞后续的生产操作。
    // flush操作在调度器线程中与段刷新一起完成
    val maybeSnapshot = producerStateManager.takeSnapshot(false)
    
    // 更新高水位到日志结束偏移量
    updateHighWatermarkWithLogEndOffset()
    
    // 调度异步刷新旧段
    scheduler.scheduleOnce("flush-log", () => {
      // 如果存在快照，刷新生产者状态快照
      maybeSnapshot.ifPresent(f => flushProducerStateSnapshot(f.toPath))
      // 刷新直到新段的基础偏移量（不包含）
      flushUptoOffsetExclusive(newSegment.baseOffset)
    })
    
    // 返回新创建的段
    newSegment
  }

  /**
   * 刷新所有本地日志段到磁盘
   *
   * 应用场景：
   * 1. 系统正常关闭时确保数据持久化
   * 2. 定期刷新以防止数据丢失
   * 3. 手动触发刷新操作时
   *
   * @param forceFlushActiveSegment 在干净关闭时应为true，其他情况为false。原因是
   * 我们必须传递logEndOffset + 1给`localLog.flush(offset: Long): Unit`函数来刷新空的
   * 活动段，这对于确保在关闭时持久化活动段文件很重要，特别是当它为空时。
   */
  def flush(forceFlushActiveSegment: Boolean): Unit = flush(logEndOffset, forceFlushActiveSegment)

  /**
   * 刷新本地日志段直到指定偏移量（不包含该偏移量）
   *
   * 应用场景：
   * 1. 日志段滚动时刷新旧段
   * 2. 日志清理后刷新保留的数据
   * 3. 定期检查点时刷新数据
   *
   * @param offset 要刷新到的偏移量（不包含）；这将成为新的恢复点
   */
  def flushUptoOffsetExclusive(offset: Long): Unit = flush(offset, includingOffset = false)

  /**
   * 刷新本地日志段。如果includingOffset=false，刷新到offset-1；
   * 如果includingOffset=true，刷新到offset。恢复点设置为offset。
   *
   * 实现细节：
   * 1. 根据includingOffset参数计算实际的刷新偏移量
   * 2. 只有当刷新偏移量大于当前恢复点时才执行刷新
   * 3. 刷新完成后更新恢复点
   *
   * @param offset 要刷新到的偏移量；这将成为新的恢复点
   * @param includingOffset 是否包含提供的偏移量
   */
  private def flush(offset: Long, includingOffset: Boolean): Unit = {
    // 计算实际的刷新偏移量
    val flushOffset = if (includingOffset) offset + 1  else offset
    val newRecoveryPoint = offset
    val includingOffsetStr =  if (includingOffset) "inclusive" else "exclusive"
    
    // 处理刷新过程中可能出现的IO异常
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset " +
      s"($includingOffsetStr) and recovery point $newRecoveryPoint") {
      // 只有当刷新偏移量大于当前恢复点时才执行刷新
      if (flushOffset > localLog.recoveryPoint) {
        debug(s"Flushing log up to offset $offset ($includingOffsetStr)" +
          s"with recovery point $newRecoveryPoint, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}," +
          s"unflushed: ${localLog.unflushedMessages}")
        
        // 执行实际的刷新操作
        localLog.flush(flushOffset)
        
        // 同步更新恢复点
        lock synchronized {
          localLog.markFlushed(newRecoveryPoint)
        }
      }
    }
  }

  /**
   * 完全删除本地日志目录及其所有内容，不延迟立即执行
   * 
   * 应用场景：
   * 1. 删除主题时清理相关分区数据
   * 2. 重新分配分区时清理旧副本数据
   * 3. 日志目录损坏需要清理时
   * 
   * 实现细节：
   * 1. 检查内存映射缓冲区状态
   * 2. 取消生产者过期检查
   * 3. 清理leader epoch缓存
   * 4. 删除所有日志段
   * 5. 删除生产者快照
   * 6. 删除空目录
   */
  private[log] def delete(): Unit = {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        // 确保内存映射缓冲区已关闭
        localLog.checkIfMemoryMappedBufferClosed()
        // 取消生产者过期检查任务
        producerExpireCheck.cancel(true)
        // 清理leader epoch缓存
        leaderEpochCache.clear()
        // 删除所有日志段并获取被删除的段列表
        val deletedSegments = localLog.deleteAllSegments()
        // 同步删除相关的生产者快照
        deleteProducerSnapshots(deletedSegments, asyncDelete = false)
        // 删除空的日志目录
        localLog.deleteEmptyDir()
      }
    }
  }

  /**
   * 创建生产者状态的快照，用于测试
   * 
   * 应用场景：
   * 1. 测试生产者状态管理功能
   * 2. 验证快照创建和恢复机制
   */
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    localLog.checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  /**
   * 获取最新的生产者快照偏移量，用于测试
   * 
   * @return 最新快照的偏移量，如果不存在则返回空
   */
  private[log] def latestProducerSnapshotOffset: OptionalLong = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  /**
   * 获取最旧的生产者快照偏移量，用于测试
   * 
   * @return 最旧快照的偏移量，如果不存在则返回空
   */
  private[log] def oldestProducerSnapshotOffset: OptionalLong = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  /**
   * 获取生产者状态的最新结束偏移量，用于测试
   * 
   * @return 生产者状态的最新结束偏移量
   */
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * 将生产者状态快照刷新到磁盘
   * 
   * 应用场景：
   * 1. 日志段滚动时保存生产者状态
   * 2. 定期检查点时持久化生产者状态
   * 
   * @param snapshot 快照文件的路径
   */
  private[log] def flushProducerStateSnapshot(snapshot: Path): Unit = {
    maybeHandleIOException(s"Error while deleting producer state snapshot $snapshot for $topicPartition in dir ${dir.getParent}") {
      Utils.flushFileIfExists(snapshot)
    }
  }

  /**
   * 将日志截断到小于目标偏移量的最大偏移量处。
   * 
   * 应用场景：
   * 1. 副本同步时，Follower发现与Leader数据不一致，需要截断日志
   * 2. 日志清理时，需要删除过期的日志段
   * 3. 分区重分配时，需要调整日志大小
   *
   * @param targetOffset 目标截断偏移量，截断后日志中所有偏移量的上限
   * @return 当且仅当targetOffset < logEndOffset时返回true
   */
  private[kafka] def truncateTo(targetOffset: Long): Boolean = {
    // 使用错误处理包装器处理可能的IO异常
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      // 检查目标偏移量是否为负数，如果是则抛出异常
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      
      // 如果目标偏移量大于等于日志结束偏移量，说明不需要截断
      if (targetOffset >= localLog.logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${localLog.logEndOffset - 1}")

        // 始终截断epoch缓存，因为可能存在来自Leader的冲突epoch条目。
        // 这种情况可能发生在：当前broker曾是Leader并插入了第一个起始偏移量条目，
        // 但在追加任何条目之前就失败了，导致另一个Leader被选举。
        lock synchronized {
          leaderEpochCache.truncateFromEndAsyncFlush(logEndOffset)
        }

        false
      } else {
        // 需要执行实际的截断操作
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          // 检查内存映射缓冲区是否已关闭
          localLog.checkIfMemoryMappedBufferClosed()
          
          // 如果第一个段的基础偏移量大于目标偏移量，需要完全截断并从目标偏移量重新开始
          if (localLog.segments.firstSegmentBaseOffset.getAsLong > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            // 执行部分截断操作
            // 删除目标偏移量之后的日志段
            val deletedSegments = localLog.truncateTo(targetOffset)
            // 异步删除生产者快照
            deleteProducerSnapshots(deletedSegments, asyncDelete = true)
            // 截断Leader Epoch缓存
            leaderEpochCache.truncateFromEndAsyncFlush(targetOffset)
            // 更新日志起始偏移量为目标偏移量和当前起始偏移量的较小值
            logStartOffset = math.min(targetOffset, logStartOffset)
            // 重建生产者状态
            rebuildProducerState(targetOffset, producerStateManager)
            // 如果高水位大于等于日志结束偏移量，更新高水位
            if (highWatermark >= localLog.logEndOffset)
              updateHighWatermark(localLog.logEndOffsetMetadata)
          }
          true
        }
      }
    }
  }

  /**
   * 删除日志中的所有数据，并从新的偏移量开始。
   * 
   * 应用场景：
   * 1. 分区重新分配时，需要清空目标broker上的数据
   * 2. 日志损坏时的恢复操作
   * 3. 管理员手动触发的日志重置操作
   *
   * @param newOffset 日志的新起始偏移量
   * @param logStartOffsetOpt 要设置的日志起始偏移量。如果为None，则使用newOffset
   */
  def truncateFullyAndStartAt(newOffset: Long,
                              logStartOffsetOpt: Option[Long] = None): Unit = {
    // 使用错误处理包装器处理可能的IO异常
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset, logStartOffset: ${logStartOffsetOpt.getOrElse(newOffset)}")
      lock synchronized {
        // 完全截断本地日志并设置新的起始偏移量
        localLog.truncateFullyAndStartAt(newOffset)
        // 清空并刷新Leader Epoch缓存
        leaderEpochCache.clearAndFlush()
        // 完全截断生产者状态管理器并设置新的起始偏移量
        producerStateManager.truncateFullyAndStartAt(newOffset)
        // 设置日志起始偏移量
        logStartOffset = logStartOffsetOpt.getOrElse(newOffset)
        // 如果启用了远程日志，更新本地日志起始偏移量
        if (remoteLogEnabled()) _localLogStartOffset = newOffset
        // 重建生产者状态
        rebuildProducerState(newOffset, producerStateManager)
        // 更新高水位标记
        updateHighWatermark(localLog.logEndOffsetMetadata)
      }
    }
  }

  /**
   * 获取日志最后一次完全刷新到磁盘的时间
   * 
   * 应用场景：
   * 1. 监控日志刷新状态
   * 2. 判断是否需要强制刷新
   * 3. 故障恢复时确定数据一致性点
   */
  def lastFlushTime: Long = localLog.lastFlushTime

  /**
   * 获取当前正在接收追加操作的活动段
   * 
   * 应用场景：
   * 1. 写入新消息时定位目标段
   * 2. 日志段滚动时的状态检查
   * 3. 日志清理时的段管理
   */
  def activeSegment: LogSegment = localLog.segments.activeSegment

  /**
   * 获取该日志中所有的日志段，按从旧到新的顺序排列
   * 
   * 应用场景：
   * 1. 日志清理时遍历所有段
   * 2. 日志压缩时处理全部数据
   * 3. 统计和监控目的
   */
  def logSegments: util.Collection[LogSegment] = localLog.segments.values

  /**
   * 获取指定偏移量范围内的所有日志段
   * 返回的段集合从包含"from"偏移量的段开始，到包含"to-1"偏移量的段结束
   * 如果to大于logEndOffset，则返回到日志末尾的所有段
   * 
   * 应用场景：
   * 1. 消费者按偏移量范围读取数据
   * 2. 日志复制时的段选择
   * 3. 按时间范围清理日志时的段筛选
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = lock synchronized {
    localLog.segments.values(from, to).asScala
  }

  /**
   * 获取从指定偏移量开始的所有非活动日志段
   * 
   * 应用场景：
   * 1. 日志清理时选择可清理的段
   * 2. 日志压缩时处理已封闭的段
   * 3. 将旧数据迁移到远程存储
   */
  def nonActiveLogSegmentsFrom(from: Long): util.Collection[LogSegment] = lock synchronized {
    localLog.segments.nonActiveLogSegmentsFrom(from)
  }

  /**
   * 重写toString方法，返回日志的关键信息的字符串表示
   * 包含以下信息：
   * - 日志目录
   * - 主题ID（如果存在）
   * - 主题名称
   * - 分区号
   * - 高水位标记
   * - 最后稳定偏移量
   * - 日志起始偏移量
   * - 日志结束偏移量
   */
  override def toString: String = {
    // 创建一个StringBuilder来构建输出字符串
    val logString = new StringBuilder
    // 添加日志目录信息
    logString.append(s"Log(dir=$dir")
    // 如果存在主题ID，添加主题ID信息
    topicId.foreach(id => logString.append(s", topicId=$id"))
    // 添加主题名称
    logString.append(s", topic=${topicPartition.topic}")
    // 添加分区号
    logString.append(s", partition=${topicPartition.partition}")
    // 添加高水位标记
    logString.append(s", highWatermark=$highWatermark")
    // 添加最后稳定偏移量
    logString.append(s", lastStableOffset=$lastStableOffset")
    // 添加日志起始偏移量
    logString.append(s", logStartOffset=$logStartOffset")
    // 添加日志结束偏移量
    logString.append(s", logEndOffset=$logEndOffset")
    logString.append(")")
    logString.toString
  }

  /**
   * 替换日志中的段
   * 这个方法用于替换日志中的旧段为新段，通常在日志压缩或清理操作后调用
   *
   * @param newSegments 要添加的新日志段序列
   * @param oldSegments 要替换的旧日志段序列
   */
  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment]): Unit = {
    lock synchronized {
      // 检查内存映射缓冲区是否已关闭
      localLog.checkIfMemoryMappedBufferClosed()
      // 调用UnifiedLog对象的replaceSegments方法执行实际的段替换操作
      val deletedSegments = UnifiedLog.replaceSegments(localLog.segments, newSegments, oldSegments, dir, topicPartition,
        config, scheduler, logDirFailureChannel, logIdent)
      // 异步删除被删除段的生产者快照
      deleteProducerSnapshots(deletedSegments.toList.asJava, asyncDelete = true)
    }
  }

  /**
   * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
   * this call, and also protects against calling this function on the same segment in parallel.
   *
   * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
   * to ensure no other logcleaner threads and retention thread can work on the same segment.
   */
  /**
   * 获取指定日志段集合中每个段的第一个批次的时间戳
   * 此方法不获取Log.lock锁。调用者必须确保在调用期间日志段不会被删除，
   * 并且防止在同一段上并行调用此函数。
   *
   * 当前主要由LogCleaner线程在具有LogCleanerManager锁的情况下对非活动的日志压缩段使用，
   * 以确保没有其他logcleaner线程和保留线程可以在同一段上工作。
   *
   * @param segments 要获取时间戳的日志段集合
   * @return 返回每个段第一个批次的时间戳集合
   */
  private[log] def getFirstBatchTimestampForSegments(segments: util.Collection[LogSegment]): util.Collection[JLong] = {
    // 使用Java 8流式处理，获取每个段的第一个批次时间戳并收集为列表
    segments.stream().map[JLong](s => s.getFirstBatchTimestamp).collect(Collectors.toList())
  }

  /**
   * remove deleted log metrics
   */
  /**
   * 移除日志相关的所有度量指标
   * 当日志被删除或关闭时调用此方法，清理相关的监控指标
   */
  private[log] def removeLogMetrics(): Unit = {
    // 遍历所有度量指标名称和标签
    metricNames.foreach {
      case (name, tags) => 
        // 从度量指标组中移除每个指标
        metricsGroup.removeMetric(name, tags)
    }
    // 清空度量指标名称映射
    metricNames = Map.empty
  }

  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  /**
   * 向日志中添加新的日志段
   * 如果新段替换了现有的段，则删除旧段
   *
   * @param segment 要添加的日志段
   * @return 添加的日志段
   * @threadsafe 此方法是线程安全的
   */
  @threadsafe
  private[log] def addSegment(segment: LogSegment): LogSegment = localLog.segments.add(segment)

  /**
   * 处理可能发生的IO异常的辅助方法
   * 将IO异常包装并通过logDirFailureChannel通知上层处理
   *
   * @param msg 发生异常时的错误消息
   * @param fun 可能抛出IO异常的函数
   * @return 函数执行的结果
   * @tparam T 函数返回值类型
   */
  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    // 调用LocalLog的异常处理方法
    LocalLog.maybeHandleIOException(logDirFailureChannel, parentDir, () => msg, () => fun)
  }

  /**
   * 分割超出大小限制的日志段
   * 当日志段大小超过配置的最大限制时，将其分割成多个较小的段
   *
   * @param segment 需要分割的日志段
   * @return 分割后的新日志段列表
   */
  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment] = lock synchronized {
    // 调用UnifiedLog对象的方法执行实际的分割操作
    val result = UnifiedLog.splitOverflowedSegment(segment, localLog.segments, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent)
    // 异步删除被删除段的生产者快照
    deleteProducerSnapshots(result.deletedSegments, asyncDelete = true)
    // 将Java集合转换为Scala列表并返回
    result.newSegments.asScala.toList
  }

  /**
   * 删除指定日志段的生产者状态快照
   * 当日志段被删除时，相关的生产者状态信息也需要清理
   *
   * @param segments 要删除快照的日志段集合
   * @param asyncDelete 是否异步删除，true表示异步删除，false表示同步删除
   */
  private[log] def deleteProducerSnapshots(segments: util.Collection[LogSegment], asyncDelete: Boolean): Unit = {
    // 调用Java版UnifiedLog的方法执行实际的快照删除操作
    JUnifiedLog.deleteProducerSnapshots(segments, producerStateManager, asyncDelete, scheduler, config, logDirFailureChannel, parentDir, topicPartition)
  }
}

/**
 * UnifiedLog的伴生对象，提供了一些常量定义和工具方法
 */
object UnifiedLog extends Logging {
  /** 日志文件的后缀名 */
  val LogFileSuffix: String = LogFileUtils.LOG_FILE_SUFFIX

  /** 索引文件的后缀名 */
  val IndexFileSuffix: String = LogFileUtils.INDEX_FILE_SUFFIX

  /** 时间索引文件的后缀名 */
  val TimeIndexFileSuffix: String = LogFileUtils.TIME_INDEX_FILE_SUFFIX

  /** 事务索引文件的后缀名 */
  val TxnIndexFileSuffix: String = LogFileUtils.TXN_INDEX_FILE_SUFFIX

  /** 清理后的文件后缀名 */
  val CleanedFileSuffix: String = LogFileUtils.CLEANED_FILE_SUFFIX

  /** 交换文件的后缀名 */
  val SwapFileSuffix: String = LogFileUtils.SWAP_FILE_SUFFIX

  /** 删除目录的后缀名 */
  val DeleteDirSuffix: String = LogFileUtils.DELETE_DIR_SUFFIX

  /** 游离目录的后缀名（存储不属于任何主题分区的文件） */
  val StrayDirSuffix: String = LogFileUtils.STRAY_DIR_SUFFIX

  /** 表示未知偏移量的常量值 */
  val UnknownOffset: Long = LocalLog.UNKNOWN_OFFSET

  /**
   * 检查是否启用了远程日志存储功能
   * 远程日志存储只对非压缩和非内部主题启用
   *
   * @param remoteStorageSystemEnable 系统级别的远程存储开关
   * @param config 日志配置
   * @param topic 主题名称
   * @return 如果启用了远程日志存储则返回true，否则返回false
   */
  def isRemoteLogEnabled(remoteStorageSystemEnable: Boolean,
                         config: LogConfig,
                         topic: String): Boolean = {
    // 远程日志存储只对非压缩和非内部主题启用
    remoteStorageSystemEnable &&
      // 检查主题是否满足启用条件：
      // 1. 不是压缩主题
      // 2. 不是内部主题
      // 3. 不是远程日志元数据主题
      // 4. 不是集群元数据主题
      !(config.compact || Topic.isInternal(topic)
        || TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME.equals(topic)
        || Topic.CLUSTER_METADATA_TOPIC_NAME.equals(topic)) &&
      // 检查配置中是否启用了远程存储
      config.remoteStorageEnable()
  }

  /**
   * UnifiedLog对象的工厂方法，用于创建一个新的UnifiedLog实例。
   * 该方法负责初始化日志目录、加载日志段、创建Leader Epoch缓存等关键组件。
   *
   * @param dir 日志目录的File对象，用于存储日志文件
   * @param config 日志配置对象，包含了各种日志相关的配置参数
   * @param logStartOffset 日志的起始偏移量
   * @param recoveryPoint 日志恢复点的偏移量
   * @param scheduler 用于调度后台任务的调度器
   * @param brokerTopicStats broker主题统计信息对象
   * @param time 时间工具类，用于获取系统时间
   * @param maxTransactionTimeoutMs 事务最大超时时间（毫秒）
   * @param producerStateManagerConfig 生产者状态管理器的配置
   * @param producerIdExpirationCheckIntervalMs 生产者ID过期检查间隔（毫秒）
   * @param logDirFailureChannel 日志目录失败通道，用于处理日志目录故障
   * @param lastShutdownClean 上次关闭是否正常，默认为true
   * @param topicId 主题ID，可选参数
   * @param numRemainingSegments 剩余段数的并发Map，默认为新的ConcurrentHashMap
   * @param remoteStorageSystemEnable 是否启用远程存储系统，默认为false
   * @param logOffsetsListener 日志偏移量监听器，默认为NO_OP_OFFSETS_LISTENER
   * @return 新创建的UnifiedLog实例
   */
  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time,
            maxTransactionTimeoutMs: Int,
            producerStateManagerConfig: ProducerStateManagerConfig,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel,
            lastShutdownClean: Boolean = true,
            topicId: Option[Uuid],
            numRemainingSegments: ConcurrentMap[String, Integer] = new ConcurrentHashMap[String, Integer],
            remoteStorageSystemEnable: Boolean = false,
            logOffsetsListener: LogOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER): UnifiedLog = {
    // 如果日志目录不存在，则创建它
    Files.createDirectories(dir.toPath)
    // 从目录名解析主题分区信息
    val topicPartition = UnifiedLog.parseTopicPartitionName(dir)
    // 创建日志段管理器
    val segments = new LogSegments(topicPartition)
    // 创建Leader Epoch缓存
    // 注意：创建的leaderEpochCache如果需要会被LogLoader截断
    // 这样可以保证即使磁盘上的检查点过期（由于LeaderEpochFileCache#truncateFromStart/End的异步特性），
    // epoch条目也会是正确的
    val leaderEpochCache = UnifiedLog.createLeaderEpochCache(
      dir,
      topicPartition,
      logDirFailureChannel,
      None,
      scheduler)
    // 创建生产者状态管理器
    val producerStateManager = new ProducerStateManager(topicPartition, dir,
      maxTransactionTimeoutMs, producerStateManagerConfig, time)
    // 检查是否启用远程日志
    val isRemoteLogEnabled = UnifiedLog.isRemoteLogEnabled(remoteStorageSystemEnable, config, topicPartition.topic)
    // 使用LogLoader加载日志，包括恢复日志段和生产者状态
    val offsets = new LogLoader(
      dir,
      topicPartition,
      config,
      scheduler,
      time,
      logDirFailureChannel,
      lastShutdownClean,
      segments,
      logStartOffset,
      recoveryPoint,
      leaderEpochCache,
      producerStateManager,
      numRemainingSegments,
      isRemoteLogEnabled,
    ).load()
    // 创建本地日志实例
    val localLog = new LocalLog(dir, config, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel)
    // 创建并返回UnifiedLog实例
    new UnifiedLog(offsets.logStartOffset,
      localLog,
      brokerTopicStats,
      producerIdExpirationCheckIntervalMs,
      leaderEpochCache,
      producerStateManager,
      topicId,
      remoteStorageSystemEnable,
      logOffsetsListener)
  }

  /**
   * 获取待删除日志目录的名称
   * @param topicPartition 主题分区
   * @return 待删除日志目录的名称
   */
  def logDeleteDirName(topicPartition: TopicPartition): String = LocalLog.logDeleteDirName(topicPartition)

  /**
   * 获取未来日志目录的名称（用于日志迁移）
   * @param topicPartition 主题分区
   * @return 未来日志目录的名称
   */
  def logFutureDirName(topicPartition: TopicPartition): String = LocalLog.logFutureDirName(topicPartition)

  /**
   * 获取游离日志目录的名称（存储不属于任何主题分区的日志）
   * @param topicPartition 主题分区
   * @return 游离日志目录的名称
   */
  def logStrayDirName(topicPartition: TopicPartition): String = LocalLog.logStrayDirName(topicPartition)

  /**
   * 获取日志目录的名称
   * @param topicPartition 主题分区
   * @return 日志目录的名称
   */
  def logDirName(topicPartition: TopicPartition): String = LocalLog.logDirName(topicPartition)

  /**
   * 获取事务索引文件
   * @param dir 目录
   * @param offset 偏移量
   * @param suffix 文件后缀，默认为空字符串
   * @return 事务索引文件对象
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File = LogFileUtils.transactionIndexFile(dir, offset, suffix)

  /**
   * 从文件名中提取偏移量
   * @param file 文件对象
   * @return 文件名中包含的偏移量
   */
  def offsetFromFile(file: File): Long = LogFileUtils.offsetFromFile(file)

  /**
   * 计算日志段集合的总字节大小
   * @param segments 日志段集合
   * @return 总字节大小
   */
  def sizeInBytes(segments: util.Collection[LogSegment]): Long = LogSegments.sizeInBytes(segments)

  /**
   * 从目录名解析主题分区信息
   * @param dir 目录对象
   * @return 主题分区对象
   */
  def parseTopicPartitionName(dir: File): TopicPartition = LocalLog.parseTopicPartitionName(dir)

  /**
   * 创建一个新的Leader Epoch文件缓存实例，并从检查点文件或当前缓存（如果非空）加载epoch条目。
   * Leader Epoch是Kafka用于处理Leader切换和日志截断的重要机制。
   *
   * @param dir 日志所在的目录
   * @param topicPartition 主题分区
   * @param logDirFailureChannel 用于异步处理日志目录故障的通道
   * @param currentCache 当前的Leader Epoch文件缓存实例（如果有）
   * @param scheduler 用于执行异步任务的调度器
   * @return 新创建的Leader Epoch文件缓存实例
   */
  def createLeaderEpochCache(dir: File,
                             topicPartition: TopicPartition,
                             logDirFailureChannel: LogDirFailureChannel,
                             currentCache: Option[LeaderEpochFileCache],
                             scheduler: Scheduler): LeaderEpochFileCache = {
    // 在指定目录创建新的Leader Epoch检查点文件
    val leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir)
    // 创建检查点文件实例
    val checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel)
    // 如果存在当前缓存，则使用新的检查点文件更新它
    // 否则创建新的Leader Epoch文件缓存实例
    currentCache.map(_.withCheckpoint(checkpointFile)).getOrElse(new LeaderEpochFileCache(topicPartition, checkpointFile, scheduler))
  }

  /**
   * 替换日志段的私有方法。用于在日志清理或压缩后替换旧的日志段。
   * 
   * @param existingSegments 现有的日志段集合
   * @param newSegments 新的日志段序列
   * @param oldSegments 要被替换的旧日志段序列
   * @param dir 日志目录
   * @param topicPartition 主题分区
   * @param config 日志配置
   * @param scheduler 调度器
   * @param logDirFailureChannel 日志目录失败通道
   * @param logPrefix 日志前缀，用于日志输出
   * @param isRecoveredSwapFile 是否是从交换文件恢复，默认为false
   * @return 替换后的日志段集合
   */
  private[log] def replaceSegments(existingSegments: LogSegments,
                                   newSegments: Seq[LogSegment],
                                   oldSegments: Seq[LogSegment],
                                   dir: File,
                                   topicPartition: TopicPartition,
                                   config: LogConfig,
                                   scheduler: Scheduler,
                                   logDirFailureChannel: LogDirFailureChannel,
                                   logPrefix: String,
                                   isRecoveredSwapFile: Boolean = false): Iterable[LogSegment] = {
    // 调用LocalLog的replaceSegments方法执行实际的替换操作
    // 将Scala序列转换为Java集合，然后再将结果转回Scala集合
    LocalLog.replaceSegments(existingSegments,
      newSegments.asJava,
      oldSegments.asJava,
      dir,
      topicPartition,
      config,
      scheduler,
      logDirFailureChannel,
      logPrefix,
      isRecoveredSwapFile).asScala
  }

  /**
   * 分割溢出的日志段。当日志段大小超过配置的最大大小时，将其分割成多个较小的段。
   * 
   * @param segment 需要分割的日志段
   * @param existingSegments 现有的日志段集合
   * @param dir 日志目录
   * @param topicPartition 主题分区
   * @param config 日志配置
   * @param scheduler 调度器
   * @param logDirFailureChannel 日志目录失败通道
   * @param logPrefix 日志前缀
   * @return 分割结果，包含新创建的日志段信息
   */
  private[log] def splitOverflowedSegment(segment: LogSegment,
                                          existingSegments: LogSegments,
                                          dir: File,
                                          topicPartition: TopicPartition,
                                          config: LogConfig,
                                          scheduler: Scheduler,
                                          logDirFailureChannel: LogDirFailureChannel,
                                          logPrefix: String): SplitSegmentResult = {
    // 调用LocalLog的splitOverflowedSegment方法执行实际的分割操作
    LocalLog.splitOverflowedSegment(segment, existingSegments, dir, topicPartition, config, scheduler, logDirFailureChannel, logPrefix)
  }

  /**
   * 创建新的已清理日志段。在日志清理操作后用于创建新的日志段。
   * 
   * @param dir 日志目录
   * @param logConfig 日志配置
   * @param baseOffset 基础偏移量，新日志段的起始偏移量
   * @return 新创建的日志段
   */
  private[log] def createNewCleanedSegment(dir: File, logConfig: LogConfig, baseOffset: Long): LogSegment = {
    // 调用LocalLog的createNewCleanedSegment方法创建新的已清理日志段
    LocalLog.createNewCleanedSegment(dir, logConfig, baseOffset)
  }

  /**
   * 用于基准测试的可见方法
   * 创建一个新的验证指标记录器，用于记录各种无效记录的统计信息
   * 
   * @param allTopicsStats 代理主题指标实例
   * @return 日志验证器的指标记录器实例
   */
  def newValidatorMetricsRecorder(allTopicsStats: BrokerTopicMetrics): LogValidator.MetricsRecorder = {
    new LogValidator.MetricsRecorder {
      // 记录无效魔数的记录数
      def recordInvalidMagic(): Unit =
        allTopicsStats.invalidMagicNumberRecordsPerSec.mark()

      // 记录无效偏移量的记录数
      def recordInvalidOffset(): Unit =
        allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()

      // 记录无效序列号的记录数
      def recordInvalidSequence(): Unit =
        allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()

      // 记录校验和无效的记录数
      def recordInvalidChecksums(): Unit =
        allTopicsStats.invalidMessageCrcRecordsPerSec.mark()

      // 记录压缩主题中无键记录的数量
      def recordNoKeyCompactedTopic(): Unit =
        allTopicsStats.noKeyCompactedTopicRecordsPerSec.mark()
    }
  }

  /**
   * 获取本地保留时间配置
   * 如果启用了远程日志和远程复制，则使用本地保留时间配置，否则使用全局保留时间配置
   * 
   * @param config 日志配置
   * @param remoteLogEnabledAndRemoteCopyEnabled 是否启用远程日志和远程复制
   * @return 保留时间（毫秒）
   */
  private[log] def localRetentionMs(config: LogConfig, remoteLogEnabledAndRemoteCopyEnabled: Boolean): Long = {
    if (remoteLogEnabledAndRemoteCopyEnabled) config.localRetentionMs else config.retentionMs
  }

  /**
   * 获取本地保留大小配置
   * 如果启用了远程日志和远程复制，则使用本地保留大小配置，否则使用全局保留大小配置
   * 
   * @param config 日志配置
   * @param remoteLogEnabledAndRemoteCopyEnabled 是否启用远程日志和远程复制
   * @return 保留大小（字节）
   */
  private[log] def localRetentionSize(config: LogConfig, remoteLogEnabledAndRemoteCopyEnabled: Boolean): Long = {
    if (remoteLogEnabledAndRemoteCopyEnabled) config.localRetentionBytes else config.retentionSize
  }

  /**
   * 将迭代器的next()值包装在Option中。
   * 注意：这个功能从scala v2.13开始就是Iterator类的一部分。
   *
   * @param iterator 迭代器
   * @tparam T 迭代器中持有的对象类型
   * @return 如果存在下一个元素则返回Some(iterator.next)，否则返回None
   */
  private[log] def nextOption[T](iterator: util.Iterator[T]): Option[T] = {
    if (iterator.hasNext)
      Some(iterator.next())
    else
      None
  }

}

/**
 * 日志指标名称常量对象
 * 定义了与日志相关的各种指标名称
 */
object LogMetricNames {
  // 日志段数量指标名称
  val NumLogSegments: String = "NumLogSegments"
  // 日志起始偏移量指标名称
  val LogStartOffset: String = "LogStartOffset"
  // 日志结束偏移量指标名称
  val LogEndOffset: String = "LogEndOffset"
  // 日志大小指标名称
  val Size: String = "Size"

  /**
   * 获取所有指标名称列表
   * @return 包含所有指标名称的列表
   */
  def allMetricNames: List[String] = {
    List(NumLogSegments, LogStartOffset, LogEndOffset, Size)
  }
}

/**
 * 保留时间超限的日志段删除原因
 * 当日志段的保留时间超过配置的限制时，使用此原因进行删除
 * 
 * @param log 统一日志实例
 * @param remoteLogEnabledAndRemoteCopyEnabled 是否启用远程日志和远程复制
 */
case class RetentionMsBreach(log: UnifiedLog, remoteLogEnabledAndRemoteCopyEnabled: Boolean) extends SegmentDeletionReason {
  override def logReason(toDelete: util.List[LogSegment]): Unit = {
    // 获取保留时间配置
    val retentionMs = UnifiedLog.localRetentionMs(log.config, remoteLogEnabledAndRemoteCopyEnabled)
    toDelete.forEach { segment =>
      // 根据段中最大记录时间戳或最后修改时间记录删除原因
      if (segment.largestRecordTimestamp.isPresent)
        if (remoteLogEnabledAndRemoteCopyEnabled)
          log.info(s"Deleting segment $segment due to local log retention time ${retentionMs}ms breach based on the largest " +
            s"record timestamp in the segment")
        else
          log.info(s"Deleting segment $segment due to log retention time ${retentionMs}ms breach based on the largest " +
            s"record timestamp in the segment")
      else {
        if (remoteLogEnabledAndRemoteCopyEnabled)
          log.info(s"Deleting segment $segment due to local log retention time ${retentionMs}ms breach based on the " +
            s"last modified time of the segment")
        else
          log.info(s"Deleting segment $segment due to log retention time ${retentionMs}ms breach based on the " +
            s"last modified time of the segment")
      }
    }
  }
}

/**
 * 保留大小超限的日志段删除原因
 * 当日志段的总大小超过配置的限制时，使用此原因进行删除
 * 
 * @param log 统一日志实例
 * @param remoteLogEnabledAndRemoteCopyEnabled 是否启用远程日志和远程复制
 */
case class RetentionSizeBreach(log: UnifiedLog, remoteLogEnabledAndRemoteCopyEnabled: Boolean) extends SegmentDeletionReason {
  override def logReason(toDelete: util.List[LogSegment]): Unit = {
    // 获取当前日志大小
    var size = log.size
    toDelete.forEach { segment =>
      // 计算删除后的大小
      size -= segment.size
      // 记录删除原因和删除后的大小
      if (remoteLogEnabledAndRemoteCopyEnabled) log.info(s"Deleting segment $segment due to local log retention size ${UnifiedLog.localRetentionSize(log.config, remoteLogEnabledAndRemoteCopyEnabled)} breach. " +
        s"Local log size after deletion will be $size.")
      else log.info(s"Deleting segment $segment due to log retention size ${log.config.retentionSize} breach. Log size " +
        s"after deletion will be $size.")
    }
  }
}

/**
 * 起始偏移量超限的日志段删除原因
 * 当日志段的起始偏移量小于配置的最小偏移量时，使用此原因进行删除
 * 
 * @param log 统一日志实例
 * @param remoteLogEnabled 是否启用远程日志
 */
case class StartOffsetBreach(log: UnifiedLog, remoteLogEnabled: Boolean) extends SegmentDeletionReason {
  override def logReason(toDelete: util.List[LogSegment]): Unit = {
    // 根据是否启用远程日志，使用不同的起始偏移量记录删除原因
    if (remoteLogEnabled)
      log.info(s"Deleting segments due to local log start offset ${log.localLogStartOffset()} breach: ${toDelete.asScala.mkString(",")}")
    else
      log.info(s"Deleting segments due to log start offset ${log.logStartOffset} breach: ${toDelete.asScala.mkString(",")}")
  }
}
