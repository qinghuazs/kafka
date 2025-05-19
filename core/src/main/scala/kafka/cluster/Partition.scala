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
package kafka.cluster

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.Optional
import java.util.concurrent.{CompletableFuture, CopyOnWriteArrayList}
import kafka.controller.StateChangeLogger
import kafka.log._
import kafka.log.remote.RemoteLogManager
import kafka.server._
import kafka.server.share.DelayedShareFetch
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.apache.kafka.common.{DirectoryId, IsolationLevel, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.common.message.{DescribeProducersResponseData, FetchResponseData}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{LeaderAndIsr, LeaderRecoveryState}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, LeaderHwChange, LogAppendInfo, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogReadInfo, LogStartOffsetIncrementReason, OffsetResultHolder, VerificationGuard}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, TopicPartitionOperationKey}
import org.apache.kafka.server.share.fetch.DelayedShareFetchPartitionKey
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, UnexpectedAppendOffsetException}
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints
import org.slf4j.event.Level

import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters

/**
 * 分区监听器，用于接收在线分区的状态变更通知。
 *
 * 监听器只能注册到在线状态的分区。只要分区保持在线状态，监听器就会持续接收通知。
 * 当分区发生故障或被删除时，分别会调用一次 `onFailed` 或 `onDeleted` 方法，之后不会再发送任何通知。
 *
 * 注意：回调方法是在触发变更的线程中执行的，并且执行过程中可能会持有锁。
 * 这些回调方法仅用作通知机制，不应该在回调中执行耗时操作。
 */
trait PartitionListener {
  /**
   * 当分区日志的高水位标记更新时调用此方法。
   * 
   * @param partition 发生高水位更新的主题分区
   * @param offset 新的高水位标记值
   */
  def onHighWatermarkUpdated(partition: TopicPartition, offset: Long): Unit = {}

  /**
   * 当该broker上的分区(或副本)发生故障时调用此方法(例如离线)。
   * 
   * @param partition 发生故障的主题分区
   */
  def onFailed(partition: TopicPartition): Unit = {}

  /**
   * 当该broker上的分区(或副本)被删除时调用此方法。
   * 注意这并不意味着分区被完全删除，而是表示该broker不再托管这个分区的副本。
   * 
   * @param partition 被删除副本的主题分区
   */
  def onDeleted(partition: TopicPartition): Unit = {}

  /**
   * 当该broker上的分区角色转变为follower时调用此方法。
   * 
   * @param partition 角色发生变更的主题分区
   */
  def onBecomingFollower(partition: TopicPartition): Unit = {}
}

/**
 * 分区变更监听器，用于监控ISR(In-Sync Replicas)集合的变更。
 * 当分区的ISR集合发生扩大、收缩或更新失败时，会调用相应的方法进行统计。
 */
trait AlterPartitionListener {
  /**
   * 标记ISR集合扩大事件，用于统计ISR扩容的频率
   */
  def markIsrExpand(): Unit

  /**
   * 标记ISR集合收缩事件，用于统计ISR缩容的频率
   */
  def markIsrShrink(): Unit

  /**
   * 标记ISR更新失败事件，用于统计更新失败的频率
   */
  def markFailed(): Unit
}

/**
 * 延迟操作管理类，负责处理与特定主题分区相关的延迟操作。
 * 包括生产请求、获取请求、删除记录请求和共享获取请求的延迟处理。
 *
 * @param topicId 主题ID，可选
 * @param topicPartition 主题分区
 * @param produce 生产请求的延迟操作管理器
 * @param fetch 获取请求的延迟操作管理器
 * @param deleteRecords 删除记录请求的延迟操作管理器
 * @param shareFetch 共享获取请求的延迟操作管理器
 */
class DelayedOperations(topicId: Option[Uuid],
                        topicPartition: TopicPartition,
                        produce: DelayedOperationPurgatory[DelayedProduce],
                        fetch: DelayedOperationPurgatory[DelayedFetch],
                        deleteRecords: DelayedOperationPurgatory[DelayedDeleteRecords],
                        shareFetch: DelayedOperationPurgatory[DelayedShareFetch]) extends Logging {

  /**
   * 检查并完成所有类型的延迟操作。
   * 该方法会尝试完成与该主题分区相关的所有延迟操作，包括：
   * 1. 获取请求的延迟操作
   * 2. 生产请求的延迟操作
   * 3. 删除记录请求的延迟操作
   * 4. 如果存在主题ID，还会处理共享获取请求的延迟操作
   */
  def checkAndCompleteAll(): Unit = {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    // 使用CoreUtils.swallow包装操作，确保即使发生异常也不会影响其他操作的执行
    CoreUtils.swallow(() -> fetch.checkAndComplete(requestKey), this, Level.ERROR)
    CoreUtils.swallow(() -> produce.checkAndComplete(requestKey), this, Level.ERROR)
    CoreUtils.swallow(() -> deleteRecords.checkAndComplete(requestKey), this, Level.ERROR)
    if (topicId.isDefined) CoreUtils.swallow(() -> shareFetch.checkAndComplete(new DelayedShareFetchPartitionKey(
      topicId.get, topicPartition.partition())), this, Level.ERROR)
  }

  /**
   * 获取当前待处理的删除记录请求数量
   * @return 待处理的删除记录请求数
   */
  def numDelayedDelete: Int = deleteRecords.numDelayed()
}

/**
 * Partition对象的伴生对象，提供工厂方法和度量指标管理
 */
object Partition {
  // 用于管理分区相关的度量指标
  private val metricsGroup = new KafkaMetricsGroup(classOf[Partition])

  /**
   * 根据TopicIdPartition创建Partition实例的工厂方法
   * 
   * @param topicIdPartition 包含主题ID和分区信息的对象
   * @param time 时间服务实例
   * @param replicaManager 副本管理器实例
   * @return 新创建的Partition实例
   */
  def apply(topicIdPartition: TopicIdPartition,
            time: Time,
            replicaManager: ReplicaManager): Partition = {
    // 调用另一个工厂方法创建Partition实例
    Partition(
      topicPartition = topicIdPartition.topicPartition(),
      topicId = Option(topicIdPartition.topicId()),
      time = time,
      replicaManager = replicaManager)
  }

  /**
   * 创建Partition实例的主要工厂方法
   * 
   * @param topicPartition 主题分区信息
   * @param time 时间服务实例
   * @param replicaManager 副本管理器实例
   * @param topicId 可选的主题ID
   * @return 新创建的Partition实例
   */
  def apply(topicPartition: TopicPartition,
            time: Time,
            replicaManager: ReplicaManager,
            topicId: Option[Uuid] = None): Partition = {

    // 创建ISR变更监听器，用于监控ISR集合的变化
    val isrChangeListener = new AlterPartitionListener {
      // 当ISR集合扩大时更新度量指标
      override def markIsrExpand(): Unit = {
        replicaManager.isrExpandRate.mark()
      }

      // 当ISR集合收缩时更新度量指标
      override def markIsrShrink(): Unit = {
        replicaManager.isrShrinkRate.mark()
      }

      // 当ISR更新失败时更新度量指标
      override def markFailed(): Unit = replicaManager.failedIsrUpdatesRate.mark()
    }

    // 创建延迟操作管理器，处理各类延迟请求
    val delayedOperations = new DelayedOperations(
      topicId,
      topicPartition,
      replicaManager.delayedProducePurgatory,  // 生产请求延迟管理器
      replicaManager.delayedFetchPurgatory,    // 获取请求延迟管理器
      replicaManager.delayedDeleteRecordsPurgatory,  // 删除记录请求延迟管理器
      replicaManager.delayedShareFetchPurgatory)    // 共享获取请求延迟管理器

    // 创建并返回新的Partition实例
    new Partition(topicPartition,
      _topicId = topicId,
      replicaLagTimeMaxMs = replicaManager.config.replicaLagTimeMaxMs,  // 副本延迟最大时间
      localBrokerId = replicaManager.config.brokerId,                   // 本地broker ID
      localBrokerEpochSupplier = replicaManager.brokerEpochSupplier,    // broker epoch提供器
      time = time,
      alterPartitionListener = isrChangeListener,                       // ISR变更监听器
      delayedOperations = delayedOperations,                            // 延迟操作管理器
      metadataCache = replicaManager.metadataCache,                     // 元数据缓存
      logManager = replicaManager.logManager,                           // 日志管理器
      alterIsrManager = replicaManager.alterPartitionManager)           // ISR变更管理器
  }

  /**
   * 移除指定主题分区的所有度量指标
   * 
   * @param topicPartition 要移除度量指标的主题分区
   */
  def removeMetrics(topicPartition: TopicPartition): Unit = {
    // 创建度量指标标签
    val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString).asJava
    // 移除各项度量指标
    metricsGroup.removeMetric("UnderReplicated", tags)  // 副本数不足的指标
    metricsGroup.removeMetric("UnderMinIsr", tags)     // ISR数量低于最小值的指标
    metricsGroup.removeMetric("InSyncReplicasCount", tags)  // 同步副本数量指标
    metricsGroup.removeMetric("ReplicasCount", tags)       // 总副本数量指标
    metricsGroup.removeMetric("LastStableOffsetLag", tags)  // 最后稳定偏移量延迟指标
    metricsGroup.removeMetric("AtMinIsr", tags)            // 处于最小ISR数量的指标
  }
}

/**
 * 分区副本分配状态的特质
 * 定义了分区副本分配的基本属性和行为
 */
sealed trait AssignmentState {
  /** 获取分区的所有副本ID列表 */
  def replicas: Seq[Int]
  
  /** 获取分区的复制因子，默认为副本数量 */
  def replicationFactor: Int = replicas.size
  
  /** 判断指定的broker是否为新增加的副本，默认返回false */
  def isAddingReplica(brokerId: Int): Boolean = false
}

/**
 * 表示正在进行副本重分配的状态
 * 
 * @param addingReplicas 正在添加的副本ID列表
 * @param removingReplicas 正在移除的副本ID列表
 * @param replicas 当前的副本ID列表
 */
case class OngoingReassignmentState(addingReplicas: Seq[Int],
                                    removingReplicas: Seq[Int],
                                    replicas: Seq[Int]) extends AssignmentState {

  // 重写复制因子计算方法，保持原始副本数量（不包括正在添加的副本）
  override def replicationFactor: Int = replicas.diff(addingReplicas).size
  
  // 判断指定的副本ID是否在正在添加的副本列表中
  override def isAddingReplica(replicaId: Int): Boolean = addingReplicas.contains(replicaId)
}

/**
 * 表示简单的副本分配状态
 * 
 * @param replicas 分区的副本ID列表
 */
case class SimpleAssignmentState(replicas: Seq[Int]) extends AssignmentState

/**
 * 分区状态特质
 * 定义了分区的同步副本集合(ISR)和领导者恢复状态等核心属性
 */
sealed trait PartitionState {
  /**
   * 获取已提交到ZooKeeper的同步副本集合(ISR)
   * 这个集合只包含已确认提交的同步副本
   */
  def isr: Set[Int]

  /**
   * 获取最大可能的同步副本集合
   * 这个集合可能包含尚未提交的新增ISR成员
   * 用于推进高水位和处理acks=all的生产请求
   * 
   * 注意：仅适用于IBP 2.7-IV2及以上版本
   * 对于较早版本，将返回已提交的ISR集合
   */
  def maximalIsr: Set[Int]

  /**
   * 获取领导者恢复状态
   * 表示分区领导者的恢复进度
   * 具体状态值的含义参见LeaderRecoveryState的说明
   */
  def leaderRecoveryState: LeaderRecoveryState

  /**
   * 检查是否有正在处理的AlterPartition请求
   * 用于判断分区状态是否正在变更中
   */
  def isInflight: Boolean
}

/**
 * 表示分区状态变更的基础特质，定义了分区状态变更过程中的通用属性和行为
 * 
 * 该特质用于处理ISR(In-Sync Replicas)集合变更的中间状态，包括扩容和缩容操作
 */
sealed trait PendingPartitionChange extends PartitionState {
  /** 获取最后一次已提交的分区状态 */
  def lastCommittedState: CommittedPartitionState
  
  /** 获取已发送给控制器的LeaderAndIsr请求 */
  def sentLeaderAndIsr: LeaderAndIsr

  /** 领导者恢复状态，默认为已恢复状态 */
  override val leaderRecoveryState: LeaderRecoveryState = LeaderRecoveryState.RECOVERED

  /** 通知监听器状态变更事件 */
  def notifyListener(alterPartitionListener: AlterPartitionListener): Unit
}

/**
 * 表示ISR集合正在扩容的分区状态
 * 
 * @param newInSyncReplicaId 新加入ISR的副本ID
 * @param sentLeaderAndIsr 已发送给控制器的LeaderAndIsr请求
 * @param lastCommittedState 最后一次已提交的分区状态
 */
case class PendingExpandIsr(
  newInSyncReplicaId: Int,
  sentLeaderAndIsr: LeaderAndIsr,
  lastCommittedState: CommittedPartitionState
) extends PendingPartitionChange {
  // 当前的ISR集合，继承自最后一次提交的状态
  val isr: Set[Int] = lastCommittedState.isr
  // 最大可能的ISR集合，包含新加入的副本
  val maximalIsr: Set[Int] = isr + newInSyncReplicaId
  // 标记该状态为进行中
  val isInflight = true

  /** 
   * 通知监听器ISR扩容事件
   * 用于更新相关指标统计
   */
  def notifyListener(alterPartitionListener: AlterPartitionListener): Unit = {
    alterPartitionListener.markIsrExpand()
  }

  override def toString: String = {
    s"PendingExpandIsr(newInSyncReplicaId=$newInSyncReplicaId" +
    s", sentLeaderAndIsr=$sentLeaderAndIsr" +
    s", leaderRecoveryState=$leaderRecoveryState" +
    s", lastCommittedState=$lastCommittedState" +
    ")"
  }
}

/**
 * 表示ISR集合正在缩容的分区状态
 * 
 * @param outOfSyncReplicaIds 将要从ISR中移除的副本ID集合
 * @param sentLeaderAndIsr 已发送给控制器的LeaderAndIsr请求
 * @param lastCommittedState 最后一次已提交的分区状态
 */
case class PendingShrinkIsr(
  outOfSyncReplicaIds: Set[Int],
  sentLeaderAndIsr: LeaderAndIsr,
  lastCommittedState: CommittedPartitionState
) extends PendingPartitionChange  {
  // 当前的ISR集合，继承自最后一次提交的状态
  val isr: Set[Int] = lastCommittedState.isr
  // 最大可能的ISR集合，在缩容时与当前ISR相同
  val maximalIsr: Set[Int] = isr
  // 标记该状态为进行中
  val isInflight = true

  /** 
   * 通知监听器ISR缩容事件
   * 用于更新相关指标统计
   */
  def notifyListener(alterPartitionListener: AlterPartitionListener): Unit = {
    alterPartitionListener.markIsrShrink()
  }

  override def toString: String = {
    s"PendingShrinkIsr(outOfSyncReplicaIds=$outOfSyncReplicaIds" +
    s", sentLeaderAndIsr=$sentLeaderAndIsr" +
    s", leaderRecoveryState=$leaderRecoveryState" +
    s", lastCommittedState=$lastCommittedState" +
    ")"
  }
}

/**
 * 表示已提交的分区状态
 * 
 * @param isr 当前的ISR(In-Sync Replicas)集合
 * @param leaderRecoveryState 领导者恢复状态
 */
case class CommittedPartitionState(
  isr: Set[Int],
  leaderRecoveryState: LeaderRecoveryState
) extends PartitionState {
  // 最大可能的ISR集合，在已提交状态下与当前ISR相同
  val maximalIsr: Set[Int] = isr
  // 标记该状态为非进行中
  val isInflight = false

  override def toString: String = {
    s"CommittedPartitionState(isr=$isr" +
    s", leaderRecoveryState=$leaderRecoveryState" +
    ")"
  }
}


/**
 * 表示Kafka主题分区的数据结构。分区的领导者维护以下关键信息：
 * - AR(Assigned Replicas): 已分配的所有副本
 * - ISR(In-Sync Replicas): 保持同步的副本集合
 * - CUR(Caught-Up Replicas): 已追赶上领导者的副本集合
 * - RAR(Reassigned Replicas): 正在进行重分配的副本集合
 *
 * 并发控制说明：
 * 1) Partition类是线程安全的。分区操作可能会被不同的请求处理线程并发调用
 * 
 * 2) ISR更新使用读写锁进行同步。使用读锁检查是否需要更新，以避免在副本拉取等常见场景下
 *    获取写锁。在获取写锁后会再次检查ISR更新条件
 * 
 * 3) 领导者变更等其他操作在持有ISR写锁的情况下处理。这可能会导致生产者和副本拉取请求
 *    出现延迟，但这些操作通常较少发生
 * 
 * 4) HW(High Watermark)更新使用ISR读锁同步。更新时按照Partition锁 -> Log锁的顺序获取锁
 * 
 * 5) futureLogLock用于防止在ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()
 *    替换follower副本时更新follower副本
 */
/**
 * Kafka主题分区的核心实现类
 *
 * @param topicPartition 主题分区信息
 * @param replicaLagTimeMaxMs 副本最大延迟时间(毫秒)
 * @param localBrokerId 本地broker ID
 * @param localBrokerEpochSupplier 本地broker epoch提供器
 * @param time 时间服务实例
 * @param alterPartitionListener 分区变更监听器
 * @param delayedOperations 延迟操作管理器
 * @param metadataCache 元数据缓存
 * @param logManager 日志管理器
 * @param alterIsrManager ISR变更管理器
 * @param _topicId 主题ID(可选)
 */
class Partition(val topicPartition: TopicPartition,
                val replicaLagTimeMaxMs: Long,
                localBrokerId: Int,
                localBrokerEpochSupplier: () => Long,
                time: Time,
                alterPartitionListener: AlterPartitionListener,
                delayedOperations: DelayedOperations,
                metadataCache: MetadataCache,
                logManager: LogManager,
                alterIsrManager: AlterPartitionManager,
                @volatile private var _topicId: Option[Uuid] = None // TODO: 待KAFKA-16212完成后，将topicPartition和_topicId合并为TopicIdPartition
               ) extends Logging {

  import Partition.metricsGroup
  // 获取主题名称
  def topic: String = topicPartition.topic
  // 获取分区ID
  def partitionId: Int = topicPartition.partition

  // 状态变更日志记录器
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)
  // 远程副本映射表，保存分区的所有远程副本信息
  private val remoteReplicasMap = new Pool[Int, Replica]
  // 领导者和ISR更新锁，用于保证多线程下的一致性
  // 仅在需要多个读操作保持一致性时才需要获取读锁
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock

  // 日志目录切换锁，用于防止在检查日志目录是否可替换为future日志时更新follower副本日志
  private val futureLogLock = new Object()
  // 分区当前的epoch值，用于KRaft控制器
  @volatile private var partitionEpoch: Int = LeaderAndIsr.INITIAL_PARTITION_EPOCH
  // 领导者epoch值，初始化为初始值减1
  @volatile private var leaderEpoch: Int = LeaderAndIsr.INITIAL_LEADER_EPOCH - 1
  // 当前领导者epoch对应的起始偏移量
  // 仅在该broker为分区领导者时定义
  @volatile private[cluster] var leaderEpochStartOffsetOpt: Option[Long] = None
  // 领导者副本ID，在该broker为领导者或追随者时定义
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  // 分区当前状态，初始化为空ISR集合和已恢复状态
  @volatile private[cluster] var partitionState: PartitionState = CommittedPartitionState(Set.empty, LeaderRecoveryState.RECOVERED)
  // 分区分配状态，初始化为空副本序列
  @volatile var assignmentState: AssignmentState = SimpleAssignmentState(Seq.empty)

  // 分区的日志对象。大多数情况下只有一个日志，但在执行ReplicaAlterLogDirs命令时，
  // 在复制完成并切换到新位置之前可能同时存在两个日志。
  // 使用log和futureLog变量来管理这种情况
  @volatile var log: Option[UnifiedLog] = None
  // 如果正在执行ReplicaAlterLogDir命令，这是日志的未来位置
  @volatile var futureLog: Option[UnifiedLog] = None

  // 分区事件监听器集合
  private val listeners = new CopyOnWriteArrayList[PartitionListener]()

  // 日志偏移量监听器，用于通知高水位标记更新事件
  private val logOffsetsListener = new LogOffsetsListener {
    override def onHighWatermarkUpdated(offset: Long): Unit = {
      // 当高水位标记更新时，通知所有监听器
      listeners.forEach { listener =>
        listener.onHighWatermarkUpdated(topicPartition, offset)
      }
    }
  }

  // 设置日志标识符，用于日志输出
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  // 设置监控指标标签
  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString).asJava

  // 注册各种监控指标
  // 副本数不足指标
  metricsGroup.newGauge("UnderReplicated", () => if (isUnderReplicated) 1 else 0, tags)
  // 同步副本数量指标
  metricsGroup.newGauge("InSyncReplicasCount", () => if (isLeader) partitionState.isr.size else 0, tags)
  // ISR数量低于最小值指标
  metricsGroup.newGauge("UnderMinIsr", () => if (isUnderMinIsr) 1 else 0, tags)
  // ISR数量等于最小值指标
  metricsGroup.newGauge("AtMinIsr", () => if (isAtMinIsr) 1 else 0, tags)
  // 总副本数量指标
  metricsGroup.newGauge("ReplicasCount", () => if (isLeader) assignmentState.replicationFactor else 0, tags)
  // 最后稳定偏移量延迟指标
  metricsGroup.newGauge("LastStableOffsetLag", () => log.map(_.lastStableOffsetLag).getOrElse(0), tags)

  /**
   * 检查分区是否存在延迟事务
   * 
   * @param currentTimeMs 当前时间戳(毫秒)
   * @return 如果存在延迟事务返回true，否则返回false
   * 
   * 应用场景：
   * - 用于事务协调器检查是否有未完成的事务
   * - 帮助判断是否需要等待事务完成
   */
  def hasLateTransaction(currentTimeMs: Long): Boolean = leaderLogIfLocal.exists(_.hasLateTransaction(currentTimeMs))

  /**
   * 检查分区是否处于副本数不足状态
   * 
   * @return 如果副本数不足返回true，否则返回false
   * 
   * 应用场景：
   * - 监控分区健康状态
   * - 触发副本重平衡决策
   * 
   * 实现说明：
   * - 只有分区Leader才能判断副本是否不足
   * - 通过比较配置的复制因子和当前ISR集合大小来判断
   */
  def isUnderReplicated: Boolean = isLeader && (assignmentState.replicationFactor - partitionState.isr.size) > 0

  /**
   * 检查分区的ISR集合大小是否小于最小要求
   * 
   * 在将分区转换为Follower的过程中，会先更改Leader副本ID，然后清空ISR集合。
   * 为了避免在最小ISR检查时出现误报，需要再次检查Leader副本ID。
   * 虽然在Leader->Follower->Leader的ABA问题场景下可能受影响，但对于度量指标来说已经足够好。
   * 
   * @return 如果ISR集合小于最小要求返回true，否则返回false
   * 
   * 应用场景：
   * - 监控分区可用性
   * - 触发告警或自动化运维操作
   */
  def isUnderMinIsr: Boolean = {
    // 检查本地Leader日志是否存在且ISR大小小于有效最小ISR值，同时确保是Leader
    leaderLogIfLocal.exists { partitionState.isr.size < effectiveMinIsr(_) } && isLeader
  }

  /**
   * 计算分区的有效最小ISR值
   * 
   * 设置最小ISR时没有限制条件，即使该值大于复制因子也是允许的。
   * 在这种情况下，返回复制因子和配置的最小ISR值中的较小值作为有效值。
   * 
   * @param leaderLog Leader副本的日志
   * @return 有效的最小ISR值
   * 
   * 实现说明：
   * - 返回配置的最小同步副本数和(远程副本数+1)中的较小值
   * - 远程副本数+1是因为还要算上本地副本
   */
  private def effectiveMinIsr(leaderLog: UnifiedLog): Int = {
      leaderLog.config.minInSyncReplicas.min(remoteReplicasMap.size + 1)
  }

  /**
   * 检查分区的ISR大小是否等于有效最小ISR值
   * 
   * @return 如果ISR大小等于有效最小ISR值返回true，否则返回false
   * 
   * 应用场景：
   * - 监控分区是否处于临界状态
   * - 用于判断是否需要进行预防性维护
   */
  def isAtMinIsr: Boolean = leaderLogIfLocal.exists { partitionState.isr.size == effectiveMinIsr(_) }

  /**
   * 检查分区是否正在进行副本重分配
   * 
   * @return 如果正在重分配返回true，否则返回false
   * 
   * 应用场景：
   * - 控制器进行分区状态管理
   * - 避免在重分配过程中进行其他操作
   */
  def isReassigning: Boolean = assignmentState.isInstanceOf[OngoingReassignmentState]

  /**
   * 检查是否正在向当前broker添加该分区的副本
   * 
   * @return 如果正在添加本地副本返回true，否则返回false
   * 
   * 应用场景：
   * - 副本管理器进行本地副本状态管理
   * - 控制副本数据同步行为
   */
  def isAddingLocalReplica: Boolean = assignmentState.isAddingReplica(localBrokerId)

  /**
   * 检查是否正在向指定broker添加该分区的副本
   * 
   * @param replicaId 要检查的副本所在的broker ID
   * @return 如果正在添加指定副本返回true，否则返回false
   * 
   * 应用场景：
   * - 控制器进行全局副本分配管理
   * - 协调多个broker上的副本操作
   */
  def isAddingReplica(replicaId: Int): Boolean = assignmentState.isAddingReplica(replicaId)

  /**
   * 获取当前活跃的生产者数量
   * 
   * @return 活跃生产者数量
   * 
   * 应用场景：
   * - 监控生产者活动状态
   * - 资源使用统计
   */
  def producerIdCount: Int = log.map(_.producerIdCount).getOrElse(0)

  /**
   * 移除过期的生产者记录
   * 该方法仅用于测试
   * 
   * @param currentTimeMs 当前时间戳(毫秒)
   */
  def removeExpiredProducers(currentTimeMs: Long): Unit = log.foreach(_.removeExpiredProducers(currentTimeMs))

  /**
   * 获取当前的同步副本ID集合
   * 
   * @return 同步副本ID集合
   * 
   * 应用场景：
   * - 副本管理和监控
   * - 选举决策支持
   */
  def inSyncReplicaIds: Set[Int] = partitionState.isr

  /**
   * 尝试添加分区状态监听器
   * 
   * @param listener 要添加的监听器
   * @return 添加成功返回true，分区已失败或被删除时返回false
   * 
   * 应用场景：
   * - 监控分区状态变化
   * - 实现自定义的分区事件处理
   * 
   * 实现说明：
   * - 使用读锁保护并发访问
   * - 只有分区正常时才能添加监听器
   */
  def maybeAddListener(listener: PartitionListener): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      // 当分区失败或被删除时，log被设置为None
      log match {
        case Some(_) =>
          listeners.add(listener)
          true

        case None =>
          false
      }
    }
  }

  /**
   * 移除分区状态监听器
   * 
   * @param listener 要移除的监听器
   * 
   * 应用场景：
   * - 清理不再需要的监听器
   * - 资源释放
   */
  def removeListener(listener: PartitionListener): Unit = {
    listeners.remove(listener)
  }

  /**
    * 如果满足以下条件，则创建未来副本：
    * 1) 当前副本不在指定的日志目录中
    * 2) 未来副本不存在
    * 该方法假设当前副本已经创建完成。
    *
    * 应用场景：
    * - 在副本重分配过程中，需要在新的目录创建副本
    * - 在日志目录迁移过程中，需要在目标目录创建副本
    *
    * 设计考虑：
    * - 使用写锁确保检查和创建过程的原子性
    * - 避免重复创建和目录冲突
    * - 保持与现有副本的一致性
    *
    * @param logDir 日志目录路径
    * @param highWatermarkCheckpoints 用于加载初始高水位的检查点
    * @param topicId 主题ID（可选）
    * @return 如果创建了未来副本则返回true
    */
  def maybeCreateFutureReplica(logDir: String, highWatermarkCheckpoints: OffsetCheckpoints, topicId: Option[Uuid] = topicId): Boolean = {
    // 需要写锁来确保在检查当前副本的日志目录和未来副本是否存在期间，
    // 没有其他线程可以更新当前副本的日志目录或删除未来副本
    inWriteLock(leaderIsrUpdateLock) {
      // 获取当前日志目录
      val currentLogDir = localLogOrException.parentDir
      if (currentLogDir == logDir) {
        // 如果目标目录与当前目录相同，则跳过创建
        info(s"Current log directory $currentLogDir is same as requested log dir $logDir. " +
          s"Skipping future replica creation.")
        false
      } else {
        // 检查未来副本的状态
        futureLog match {
          case Some(partitionFutureLog) =>
            // 如果未来副本已存在，检查其目录是否匹配
            val futureLogDir = partitionFutureLog.parentDir
            if (futureLogDir != logDir)
              // 如果目录不匹配，抛出异常
              throw new IllegalStateException(s"The future log dir $futureLogDir of $topicPartition is " +
                s"different from the requested log dir $logDir")
            false
          case None =>
            // 如果未来副本不存在，创建新的副本
            createLogIfNotExists(isNew = false, isFutureReplica = true, highWatermarkCheckpoints, topicId)
            true
        }
      }
    }
  }

  /**
    * 如果日志不存在则创建日志。该方法支持创建普通日志和未来副本日志。
    *
    * 设计考虑：
    * - 支持动态分配主题ID
    * - 区分普通日志和未来副本日志的处理
    * - 确保日志创建的原子性
    *
    * @param isNew 是否为新建分区
    * @param isFutureReplica 是否为未来副本
    * @param offsetCheckpoints 偏移量检查点
    * @param topicId 主题ID（可选）
    * @param targetLogDirectoryId 目标日志目录ID（可选）
    */
  def createLogIfNotExists(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid],
                           targetLogDirectoryId: Option[Uuid] = None): Unit = {
    // 内部函数：创建或获取已存在的日志
    def maybeCreate(logOpt: Option[UnifiedLog]): UnifiedLog = {
      logOpt match {
        case Some(log) =>
          // 如果日志已存在，记录日志并可能分配主题ID
          trace(s"${if (isFutureReplica) "Future UnifiedLog" else "UnifiedLog"} already exists.")
          if (log.topicId.isEmpty)
            topicId.foreach(log.assignTopicId)
          log
        case None =>
          // 如果日志不存在，创建新的日志
          createLog(isNew, isFutureReplica, offsetCheckpoints, topicId, targetLogDirectoryId)
      }
    }

    // 根据是否为未来副本，更新相应的日志引用
    if (isFutureReplica) {
      this.futureLog = Some(maybeCreate(this.futureLog))
    } else {
      this.log = Some(maybeCreate(this.log))
    }
  }

  /**
    * 创建新的日志实例。该方法在测试中可见。
    *
    * 实现细节：
    * 1. 初始化日志管理器状态
    * 2. 创建或获取日志
    * 3. 设置偏移量监听器
    * 4. 更新高水位标记
    * 5. 确保日志管理器状态正确清理
    *
    * @param isNew 是否为新建分区
    * @param isFutureReplica 是否为未来副本
    * @param offsetCheckpoints 偏移量检查点
    * @param topicId 主题ID（可选）
    * @param targetLogDirectoryId 目标日志目录ID（可选）
    * @return 创建的统一日志实例
    */
  private[cluster] def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints,
                                 topicId: Option[Uuid], targetLogDirectoryId: Option[Uuid]): UnifiedLog = {
    // 内部函数：更新日志的高水位标记
    def updateHighWatermark(log: UnifiedLog): Unit = {
      // 从检查点获取高水位标记，如果不存在则使用0
      val checkpointHighWatermark = offsetCheckpoints.fetch(log.parentDir, topicPartition).orElseGet(() => {
        info(s"No checkpointed highwatermark is found for partition $topicPartition")
        0L
      })
      // 更新日志的高水位标记并记录
      val initialHighWatermark = log.updateHighWatermark(checkpointHighWatermark)
      info(s"Log loaded for partition $topicPartition with initial high watermark $initialHighWatermark")
    }

    // 通知日志管理器开始初始化
    logManager.initializingLog(topicPartition)
    var maybeLog: Option[UnifiedLog] = None
    try {
      // 创建或获取日志
      val log = logManager.getOrCreateLog(topicPartition, isNew, isFutureReplica, topicId, targetLogDirectoryId)
      // 如果不是未来副本，设置偏移量监听器
      if (!isFutureReplica) log.setLogOffsetsListener(logOffsetsListener)
      maybeLog = Some(log)
      // 更新高水位标记
      updateHighWatermark(log)
      log
    } finally {
      // 确保日志管理器状态正确清理
      logManager.finishedInitializingLog(topicPartition, maybeLog)
    }
  }

  /**
    * 获取指定ID的副本。
    *
    * @param replicaId 副本ID
    * @return 如果存在则返回副本实例，否则返回None
    */
  def getReplica(replicaId: Int): Option[Replica] = Option(remoteReplicasMap.get(replicaId))

  /**
    * 检查当前领导者epoch与远程epoch的关系。
    * 
    * 实现细节：
    * - 如果未提供远程epoch，返回NONE
    * - 如果本地epoch大于远程epoch，返回FENCED_LEADER_EPOCH（表示远程请求已过期）
    * - 如果本地epoch小于远程epoch，返回UNKNOWN_LEADER_EPOCH（表示本地状态落后）
    * - 如果相等，返回NONE
    *
    * @param remoteLeaderEpochOpt 远程领导者epoch（可选）
    * @return 错误码，表示epoch检查的结果
    */
  private def checkCurrentLeaderEpoch(remoteLeaderEpochOpt: Optional[Integer]): Errors = {
    if (!remoteLeaderEpochOpt.isPresent) {
      // 如果未提供远程epoch，不进行检查
      Errors.NONE
    } else {
      val remoteLeaderEpoch = remoteLeaderEpochOpt.get
      val localLeaderEpoch = leaderEpoch
      if (localLeaderEpoch > remoteLeaderEpoch)
        // 远程epoch已过期
        Errors.FENCED_LEADER_EPOCH
      else if (localLeaderEpoch < remoteLeaderEpoch)
        // 本地epoch落后
        Errors.UNKNOWN_LEADER_EPOCH
      else
        // epoch匹配
        Errors.NONE
    }
  }

  /**
   * 获取本地日志对象，同时进行领导者epoch和角色的验证
   * 
   * @param currentLeaderEpoch 当前的领导者epoch，用于验证请求的有效性
   * @param requireLeader 是否要求当前节点必须是领导者
   * @return 如果验证通过且日志存在，返回Left(日志对象)；否则返回Right(错误信息)
   */
  private def getLocalLog(currentLeaderEpoch: Optional[Integer],
                          requireLeader: Boolean): Either[UnifiedLog, Errors] = {
    // 首先验证请求中的领导者epoch是否与当前epoch匹配
    checkCurrentLeaderEpoch(currentLeaderEpoch) match {
      case Errors.NONE =>
        // 如果要求领导者角色但当前不是领导者，返回错误
        if (requireLeader && !isLeader) {
          Right(Errors.NOT_LEADER_OR_FOLLOWER)
        } else {
          // 检查日志是否存在
          log match {
            case Some(partitionLog) =>
              Left(partitionLog)  // 日志存在，返回日志对象
            case _ =>
              Right(Errors.NOT_LEADER_OR_FOLLOWER)  // 日志不存在，返回错误
          }
        }
      case error =>
        Right(error)  // epoch验证失败，返回相应错误
    }
  }

  /**
   * 获取本地日志对象，如果不存在则抛出异常
   * 用于必须操作本地日志的场景
   * 
   * @return 本地日志对象
   * @throws NotLeaderOrFollowerException 当日志不存在时抛出
   */
  def localLogOrException: UnifiedLog = log.getOrElse {
    throw new NotLeaderOrFollowerException(s"Log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  /**
   * 获取未来日志对象，如果不存在则抛出异常
   * 用于日志目录迁移等场景
   * 
   * @return 未来日志对象
   * @throws NotLeaderOrFollowerException 当未来日志不存在时抛出
   */
  def futureLocalLogOrException: UnifiedLog = futureLog.getOrElse {
    throw new NotLeaderOrFollowerException(s"Future log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  /**
   * 如果当前节点是领导者，则返回本地日志对象
   * 用于只有领导者才能执行的操作
   * 
   * @return 如果是领导者则返回Some(日志对象)，否则返回None
   */
  def leaderLogIfLocal: Option[UnifiedLog] = {
    log.filter(_ => isLeader)
  }

  /**
   * 判断当前节点是否是该分区的领导者
   * 
   * @return 如果当前节点是领导者返回true，否则返回false
   */
  def isLeader: Boolean = leaderReplicaIdOpt.contains(localBrokerId)

  /**
   * 如果当前节点是领导者，返回领导者ID
   * 
   * @return 如果是领导者则返回Some(领导者ID)，否则返回None
   */
  def leaderIdIfLocal: Option[Int] = {
    leaderReplicaIdOpt.filter(_ == localBrokerId)
  }

  /**
   * 获取本地日志对象，需要验证领导者epoch，如果验证失败则抛出异常
   * 
   * @param currentLeaderEpoch 当前的领导者epoch
   * @param requireLeader 是否要求当前节点必须是领导者
   * @return 本地日志对象
   * @throws KafkaException 当验证失败或日志不存在时抛出相应异常
   */
  def localLogWithEpochOrThrow(
    currentLeaderEpoch: Optional[Integer],
    requireLeader: Boolean
  ): UnifiedLog = {
    // 尝试获取本地日志并处理结果
    getLocalLog(currentLeaderEpoch, requireLeader) match {
      case Left(localLog) => localLog  // 验证通过，返回日志对象
      case Right(error) =>
        // 验证失败，抛出异常并附带详细信息
        throw error.exception(s"Failed to find ${if (requireLeader) "leader" else ""} log for " +
          s"partition $topicPartition with leader epoch $currentLeaderEpoch. The current leader " +
          s"is $leaderReplicaIdOpt and the current epoch $leaderEpoch")
    }
  }

  /**
   * 设置分区的日志对象，用于测试场景
   * 
   * @param log 要设置的日志对象
   * @param isFutureLog 是否是未来日志
   */
  def setLog(log: UnifiedLog, isFutureLog: Boolean): Unit = {
    if (isFutureLog) {
      // 设置为未来日志
      futureLog = Some(log)
    } else {
      // 设置为当前日志，并添加日志偏移量监听器
      log.setLogOffsetsListener(logOffsetsListener)
      this.log = Some(log)
    }
  }

  /**
   * 获取主题ID
   * 如果_topicId为空或为零值，则尝试从日志对象中获取主题ID并更新_topicId
   * 
   * @return 主题ID，如果不存在则返回None
   */
  def topicId: Option[Uuid] = {
    // 如果主题ID为空或为零值，尝试从日志中获取
    if (_topicId.isEmpty || _topicId.contains(Uuid.ZERO_UUID)) {
      _topicId = this.log.orElse(logManager.getLog(topicPartition)).flatMap(_.topicId)
    }
    _topicId
  }

  /**
   * 获取远程副本列表
   * 该方法在热路径上调用，需要保持高效
   * 
   * @return 远程副本的迭代器
   */
  def remoteReplicas: Iterable[Replica] =
    remoteReplicasMap.values

  /**
   * 检查未来副本的目录是否发生变化
   * 
   * @param newDestinationDir 新的目标目录路径
   * @return 如果目录发生变化返回true，否则返回false
   */
  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      // 检查未来日志是否存在且其父目录是否与新目录不同
      futureLog.exists(_.parentDir != newDestinationDir)
    }
  }

  /**
   * 移除未来本地副本
   * 
   * @param deleteFromLogDir 是否从日志目录中删除未来副本的日志文件，默认为true
   */
  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true): Unit = {
    // 获取写锁以确保线程安全，防止并发修改futureLog和日志删除操作
    inWriteLock(leaderIsrUpdateLock) {
      // 将futureLog设置为None，表示不再有未来副本
      futureLog = None
      // 如果需要删除日志文件，则异步删除未来副本的日志
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  /**
   * 尝试开始事务验证过程
   * 该方法用于验证生产者事务的有效性，确保事务完整性
   * 
   * @param producerId 生产者ID
   * @param sequence 序列号
   * @param epoch 生产者epoch
   * @param supportsEpochBump 是否支持epoch递增
   * @return 验证守卫对象，用于后续的验证过程
   * @throws NotLeaderOrFollowerException 如果当前broker不是分区的leader
   */
  def maybeStartTransactionVerification(producerId: Long, sequence: Int, epoch: Short, supportsEpochBump: Boolean): VerificationGuard = {
    // 检查是否存在本地leader日志
    leaderLogIfLocal match {
      // 如果存在，则开始事务验证
      case Some(log) => log.maybeStartTransactionVerification(producerId, sequence, epoch, supportsEpochBump)
      // 如果不存在，说明当前broker不是leader，抛出异常
      case None => throw new NotLeaderOrFollowerException()
    }
  }

  /**
   * 尝试用未来副本替换当前副本
   * 仅当未来副本存在且已经追赶上当前副本时才会进行替换
   * 该方法只能由ReplicaAlterDirThread调用
   * 
   * @return 如果替换成功返回true，否则返回false
   */
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    // 如果未来副本已经追赶上当前副本，则执行替换操作
    runCallbackIfFutureReplicaCaughtUp((futurePartitionLog: UnifiedLog) => {
      // 使用日志管理器替换当前日志
      logManager.replaceCurrentWithFutureLog(topicPartition)
      // 设置日志偏移量监听器
      futurePartitionLog.setLogOffsetsListener(logOffsetsListener)
      // 更新当前日志为未来日志
      log = futureLog
      // 移除未来副本但不删除日志文件
      removeFutureLocalReplica(false)
    })
  }

  /**
   * 如果未来副本已经追赶上当前副本，则执行回调函数
   * 
   * @param callback 当未来副本追赶上时要执行的回调函数
   * @return 如果回调执行成功返回true，否则返回false
   */
  def runCallbackIfFutureReplicaCaughtUp(callback: UnifiedLog => Unit): Boolean = {
    // 使用futureLogLock同步，防止在检查过程中follower追加日志
    futureLogLock.synchronized {
      // 获取本地副本的日志末端偏移量(LEO)
      val localReplicaLEO = localLogOrException.logEndOffset
      // 获取未来副本的日志末端偏移量
      val futureReplicaLEO = futureLog.map(_.logEndOffset)
      // 检查未来副本是否追赶上当前副本
      if (futureReplicaLEO.contains(localReplicaLEO)) {
        // 获取写锁，确保在检查LEO时没有其他线程通过日志截断或追加操作更新LEO
        inWriteLock(leaderIsrUpdateLock) {
          futureLog match {
            case Some(futurePartitionLog) =>
              // 再次检查两个日志的LEO是否相等
              if (log.exists(_.logEndOffset == futurePartitionLog.logEndOffset)) {
                // 执行回调并返回true
                callback(futurePartitionLog)
                true
              } else false
            case None =>
              // 如果未来副本在方法调用前被其他线程移除
              // 此时分区应该已经从ReplicaAlterLogDirsThread的状态中移除
              // 返回false以避免ReplicaAlterLogDirsThread重复移除该分区
              false
          }
        }
      } else false
    }
  }

  /**
   * 获取未来副本所在目录的ID
   * @return 目录ID，如果未来副本不存在则返回None
   */
  def futureReplicaDirectoryId(): Option[Uuid] = futureLog.flatMap(log => logManager.directoryId(log.dir.getParent))

  /**
   * 获取当前日志所在目录的ID
   * @return 目录ID，如果日志不存在则返回None
   */
  def logDirectoryId(): Option[Uuid] = log.flatMap(log => logManager.directoryId(log.dir.getParent))

  /**
   * 删除分区
   * 注意：删除分区不会删除底层的日志文件
   * 日志文件的删除由ReplicaManager在删除分区后进行
   */
  def delete(): Unit = {
    // 获取写锁以防止在删除过程中appendMessagesToLeader()遇到I/O异常
    inWriteLock(leaderIsrUpdateLock) {
      // 清理分区状态
      clear()
      // 通知所有监听器分区已被删除
      listeners.forEach { listener =>
        listener.onDeleted(topicPartition)
      }
      // 清空监听器列表
      listeners.clear()
    }
  }

  /**
   * 将分区标记为离线状态
   * 当分区转换为离线状态时，由ReplicaManager调用此方法
   */
  def markOffline(): Unit = {
    // 获取写锁以确保线程安全
    inWriteLock(leaderIsrUpdateLock) {
      // 清理分区状态
      clear()
      // 通知所有监听器分区已失败
      listeners.forEach { listener =>
        listener.onFailed(topicPartition)
      }
      // 清空监听器列表
      listeners.clear()
    }
  }

  /**
   * 当分区转换为follower角色时调用分区监听器
   * 通知所有注册的监听器分区角色已变更为follower
   */
  def invokeOnBecomingFollowerListeners(): Unit = {
    // 遍历所有监听器并通知分区已成为follower
    listeners.forEach { listener =>
      listener.onBecomingFollower(topicPartition)
    }
  }

  /**
   * 清理分区的所有状态信息。
   * 该方法会重置分区的所有内部状态，包括：
   * 1. 清空远程副本映射表
   * 2. 重置副本分配状态为空
   * 3. 清除本地日志和未来日志引用
   * 4. 重置分区状态为已恢复状态
   * 5. 清除领导者副本ID和起始偏移量
   * 6. 移除相关的度量指标
   */
  private def clear(): Unit = {
    // 清空远程副本映射表，移除所有远程副本信息
    remoteReplicasMap.clear()
    // 重置副本分配状态为空序列
    assignmentState = SimpleAssignmentState(Seq.empty)
    // 清除本地日志引用
    log = None
    // 清除未来日志引用
    futureLog = None
    // 重置分区状态为空ISR集合和已恢复状态
    partitionState = CommittedPartitionState(Set.empty, LeaderRecoveryState.RECOVERED)
    // 清除领导者副本ID
    leaderReplicaIdOpt = None
    // 清除领导者纪元起始偏移量
    leaderEpochStartOffsetOpt = None
    // 移除该主题分区的所有度量指标
    Partition.removeMetrics(topicPartition)
  }

  /**
   * 获取当前的领导者纪元(Leader Epoch)。
   * 领导者纪元用于标识分区领导者的版本，每次领导者变更时都会递增。
   * 这个值用于确保副本之间的一致性，并帮助处理日志截断等场景。
   *
   * @return 当前的领导者纪元值
   */
  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * 获取当前的分区纪元(Partition Epoch)。
   * 分区纪元用于跟踪分区状态的版本，每次分区配置变更时都会递增。
   * 这个值用于确保分区状态更新的顺序性和一致性。
   *
   * @return 当前的分区纪元值
   */
  def getPartitionEpoch: Int = this.partitionEpoch

  /**
   * 将本地副本提升为领导者。
   * 
   * 该方法的主要职责：
   * 1. 重置远程副本的日志末端偏移量(可能存在旧值)
   * 2. 设置新的领导者和ISR(In-Sync Replicas)集合
   * 3. 更新分区状态和高水位标记
   * 
   * 并发控制：
   * - 使用写锁确保状态更新的原子性
   * - 在更新高水位时可能会触发延迟操作的完成
   * 
   * @param partitionState 新的分区状态信息
   * @param highWatermarkCheckpoints 高水位检查点管理器
   * @param topicId 主题ID(可选)
   * @param targetDirectoryId 目标目录ID(可选)
   * @return 如果领导者副本ID发生变化则返回true，否则返回false
   */
  def makeLeader(partitionState: LeaderAndIsrRequest.PartitionState,
                 highWatermarkCheckpoints: OffsetCheckpoints,
                 topicId: Option[Uuid],
                 targetDirectoryId: Option[Uuid] = None): Boolean = {
    // 使用写锁保护状态更新，返回高水位是否增加和是否为新领导者的标志
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 分区状态变更要求新的分区纪元大于或等于当前纪元
      // 允许等于是因为分区纪元可能已经通过AlterPartition响应更新
      // 所以在收到LeaderAndIsr请求或元数据日志更新之前就可能知道新的纪元
      if (partitionState.partitionEpoch < partitionEpoch) {
        // 如果新的分区纪元小于当前纪元，记录日志并返回false
        stateChangeLogger.info(s"Skipped the become-leader state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since the leader is already at a newer partition epoch $partitionEpoch.")
        return false
      }

      // 获取当前时间戳
      val currentTimeMs = time.milliseconds
      // 判断是否为新领导者(之前不是领导者)
      val isNewLeader = !isLeader
      // 判断是否为新的领导者纪元(新纪元大于当前纪元)
      val isNewLeaderEpoch = partitionState.leaderEpoch > leaderEpoch
      // 转换副本列表、ISR集合、正在添加和移除的副本列表
      val replicas = partitionState.replicas.asScala.map(_.toInt)
      val isr = partitionState.isr.asScala.map(_.toInt).toSet
      val addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt)
      val removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)

      // 如果分区处于恢复状态，记录日志并标记为已恢复
      if (partitionState.leaderRecoveryState == LeaderRecoveryState.RECOVERING.value) {
        stateChangeLogger.info(s"The topic partition $topicPartition was marked as RECOVERING. " +
          "Marking the topic partition as RECOVERED.")
      }

      // 更新副本分配状态和ISR集合
      // 当分区纪元大于或等于当前纪元时，这个操作是安全的
      updateAssignmentAndIsr(
        replicas = replicas,
        isLeader = true,
        isr = isr,
        addingReplicas = addingReplicas,
        removingReplicas = removingReplicas,
        LeaderRecoveryState.RECOVERED
      )

      // 在指定目录中创建日志
      createLogInAssignedDirectoryId(partitionState, highWatermarkCheckpoints, topicId, targetDirectoryId)

      // 获取本地日志(如果不存在则抛出异常)
      val leaderLog = localLogOrException

      // 仅在领导者纪元发生变化时更新纪元起始偏移量和副本状态
      if (isNewLeaderEpoch) {
        // 获取领导者日志的末端偏移量作为新纪元的起始偏移量
        val leaderEpochStartOffset = leaderLog.logEndOffset
        // 记录详细的状态变更日志
        stateChangeLogger.info(s"Leader $topicPartition with topic id $topicId starts at " +
          s"leader epoch ${partitionState.leaderEpoch} from offset $leaderEpochStartOffset " +
          s"with partition epoch ${partitionState.partitionEpoch}, high watermark ${leaderLog.highWatermark}, " +
          s"ISR ${isr.mkString("[", ",", "]")}, adding replicas ${addingReplicas.mkString("[", ",", "]")} and " +
          s"removing replicas ${removingReplicas.mkString("[", ",", "]")} ${if (isUnderMinIsr) "(under-min-isr)" else ""}. " +
          s"Previous leader $leaderReplicaIdOpt and previous leader epoch was $leaderEpoch.")

        // 在短时间内连续进行领导者选举时，follower可能拥有比新领导者日志更新的纪元的条目
        // 为了确保这些follower能够截断到正确的偏移量，我们必须缓存新的领导者纪元和起始偏移量
        // 因为它应该大于任何follower可能查询的纪元
        leaderLog.assignEpochStartOffset(partitionState.leaderEpoch, leaderEpochStartOffset)

        // 初始化所有远程副本的最后追赶时间、最后获取时间和最后获取的领导者日志末端偏移量
        remoteReplicas.foreach { replica =>
          replica.resetReplicaState(
            currentTimeMs = currentTimeMs,
            leaderEndOffset = leaderEpochStartOffset,
            isNewLeader = isNewLeader,
            isFollowerInSync = partitionState.isr.contains(replica.brokerId)
          )
        }

        // 仅在领导者纪元发生变化时更新领导者纪元和纪元起始偏移量
        leaderEpoch = partitionState.leaderEpoch
        leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      } else {
        // 如果领导者纪元没有变化，记录跳过状态变更的日志
        stateChangeLogger.info(s"Skipped the become-leader state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since it is already the leader with leader epoch $leaderEpoch. " +
          s"Current high watermark ${leaderLog.highWatermark}, ISR ${isr.mkString("[", ",", "]")}, " +
          s"adding replicas ${addingReplicas.mkString("[", ",", "]")} and " +
          s"removing replicas ${removingReplicas.mkString("[", ",", "]")}.")
      }

      // 更新分区纪元为新的纪元值
      partitionEpoch = partitionState.partitionEpoch
      // 设置领导者副本ID为本地broker ID
      leaderReplicaIdOpt = Some(localBrokerId)

      // 由于ISR集合可能减少到只有1个副本，需要尝试增加高水位标记
      // 返回高水位是否增加的标志和是否为新领导者的标志
      (maybeIncrementLeaderHW(leaderLog, currentTimeMs = currentTimeMs), isNewLeader)
    }

    // 如果高水位增加了，可能会解锁一些延迟的操作请求
    // 比如等待消息被复制到足够多副本的生产请求
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    // 返回是否为新领导者的标志
    isNewLeader
  }

  /**
   * 将本地副本转换为follower角色，通过设置新的leader和清空ISR集合来实现
   * 如果leader副本ID没有变化且新的epoch等于或仅大于当前值1(即没有错过任何更新)，
   * 返回false以告知副本管理器当前状态已经正确，可以跳过become-follower的步骤
   *
   * 该方法的主要职责：
   * 1. 更新分区的leader和epoch信息
   * 2. 更新副本分配状态和ISR集合
   * 3. 创建或验证日志目录
   * 4. 记录状态变更日志
   *
   * 并发控制：
   * - 使用写锁保护整个状态转换过程
   * - 确保leader更新在ISR清空之前完成，避免出现最小ISR数量不足的错误度量
   *
   * @param partitionState 分区状态信息，包含新的leader、epoch等信息
   * @param highWatermarkCheckpoints 高水位检查点管理器
   * @param topicId 主题ID(可选)
   * @param targetLogDirectoryId 目标日志目录ID(可选)
   * @return 如果是新的leader epoch则返回true，否则返回false
   */
  def makeFollower(partitionState: LeaderAndIsrRequest.PartitionState,
                   highWatermarkCheckpoints: OffsetCheckpoints,
                   topicId: Option[Uuid],
                   targetLogDirectoryId: Option[Uuid] = None): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      // 如果新的分区epoch小于当前值，说明这是一个过期的请求，跳过状态转换
      if (partitionState.partitionEpoch < partitionEpoch) {
        stateChangeLogger.info(s"Skipped the become-follower state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since the follower is already at a newer partition epoch $partitionEpoch.")
        return false
      }

      // 检查是否是新的leader epoch
      val isNewLeaderEpoch = partitionState.leaderEpoch > leaderEpoch
      // 在清空ISR之前更新leader信息，避免在makeFollower过程中出现ISR数量不足的错误度量
      leaderReplicaIdOpt = Option(partitionState.leader)
      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      partitionEpoch = partitionState.partitionEpoch

      // 更新副本分配状态和ISR集合
      updateAssignmentAndIsr(
        replicas = partitionState.replicas.asScala.iterator.map(_.toInt).toSeq,
        isLeader = false,
        isr = Set.empty,  // follower的ISR集合初始为空
        addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt),
        removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt),
        LeaderRecoveryState.of(partitionState.leaderRecoveryState)
      )

      // 在指定目录创建或验证日志
      createLogInAssignedDirectoryId(partitionState, highWatermarkCheckpoints, topicId, targetLogDirectoryId)

      // 获取本地日志
      val followerLog = localLogOrException
      if (isNewLeaderEpoch) {
        // 如果是新的leader epoch，记录详细的状态变更信息
        val leaderEpochEndOffset = followerLog.logEndOffset
        stateChangeLogger.info(s"Follower $topicPartition starts at leader epoch ${partitionState.leaderEpoch} from " +
          s"offset $leaderEpochEndOffset with partition epoch ${partitionState.partitionEpoch} and " +
          s"high watermark ${followerLog.highWatermark}. Current leader is ${partitionState.leader}. " +
          s"Previous leader $leaderReplicaIdOpt and previous leader epoch was $leaderEpoch.")
      } else {
        // 如果不是新的leader epoch，记录跳过状态变更的信息
        stateChangeLogger.info(s"Skipped the become-follower state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since it is already a follower with leader epoch $leaderEpoch.")
      }

      // 当leader epoch发生变化时，无论leader是否变化，都需要重启fetcher
      isNewLeaderEpoch
    }
  }

  /**
   * 在指定的目录ID下创建日志
   * 
   * 该方法的主要职责：
   * 1. 检查目标日志目录的状态
   * 2. 在合适的条件下创建日志
   * 
   * 目录选择逻辑：
   * - 如果指定了目录ID，检查该目录是否在线或是否有离线目录
   * - 如果目录ID为None，使用默认目录
   *
   * @param partitionState 分区状态信息
   * @param highWatermarkCheckpoints 高水位检查点管理器
   * @param topicId 主题ID(可选)
   * @param targetLogDirectoryId 目标日志目录ID(可选)
   */
  private def createLogInAssignedDirectoryId(partitionState: LeaderAndIsrRequest.PartitionState, highWatermarkCheckpoints: OffsetCheckpoints, topicId: Option[Uuid], targetLogDirectoryId: Option[Uuid]): Unit = {
    targetLogDirectoryId match {
      case Some(directoryId) =>
        // 检查目录是否可用：在线、无离线目录或未分配
        if (logManager.onlineLogDirId(directoryId) || !logManager.hasOfflineLogDirs() || directoryId == DirectoryId.UNASSIGNED) {
          // 在指定目录创建日志
          createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId, targetLogDirectoryId)
        } else {
          // 如果目录不可用，记录警告日志
          warn(s"Skipping creation of log because there are potentially offline log " +
            s"directories and log may already exist there. directoryId=$directoryId, " +
            s"topicId=$topicId, targetLogDirectoryId=$targetLogDirectoryId")
        }

      case None =>
        // 如果没有指定目录ID，使用默认目录创建日志
        createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId)
    }
  }

  /**
   * 基于最近的fetch请求更新leader上follower的状态
   * 详细信息参见[[Replica.updateFetchStateOrThrow()]]
   *
   * 该方法的主要职责：
   * 1. 更新follower的fetch状态
   * 2. 检查并可能扩展ISR集合
   * 3. 更新分区的高水位和低水位
   * 4. 处理延迟的操作请求
   *
   * 并发控制：
   * - 使用读锁保护ISR更新和fetch状态更新
   * - 避免与重启的follower的fetch请求发生竞争
   *
   * 性能考虑：
   * - 该方法对性能测试可见
   * - 仅在必要时计算低水位
   * - 使用读锁而非写锁以提高并发性
   *
   * @param replica 需要更新状态的副本
   * @param followerFetchOffsetMetadata follower的fetch偏移量元数据
   * @param followerStartOffset follower的起始偏移量
   * @param followerFetchTimeMs follower的fetch时间戳
   * @param leaderEndOffset leader的末尾偏移量
   * @param brokerEpoch broker的epoch值
   */
  def updateFollowerFetchState(
    replica: Replica,
    followerFetchOffsetMetadata: LogOffsetMetadata,
    followerStartOffset: Long,
    followerFetchTimeMs: Long,
    leaderEndOffset: Long,
    brokerEpoch: Long
  ): Unit = {
    // 只有在存在延迟的DeleteRecordsRequest时才需要计算低水位
    val oldLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
    val prevFollowerEndOffset = replica.stateSnapshot.logEndOffset

    // 使用读锁避免与重启的follower的fetch请求发生竞争
    // 这可能会破坏ISR扩展中的broker epoch检查
    inReadLock(leaderIsrUpdateLock) {
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata,
        followerStartOffset,
        followerFetchTimeMs,
        leaderEndOffset,
        brokerEpoch
      )
    }

    // 重新计算新的低水位
    val newLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
    // 检查分区的低水位是否增加(因为副本的logStartOffset可能已增加)
    val leaderLWIncremented = newLeaderLW > oldLeaderLW

    // 检查是否需要将这个同步的副本添加到ISR中
    maybeExpandIsr(replica)

    // 检查是否可以增加分区的高水位
    // 因为该副本可能已经在ISR中且其LEO刚刚增加
    val leaderHWIncremented = if (prevFollowerEndOffset != replica.stateSnapshot.logEndOffset) {
      // leader日志可能被ReplicaAlterLogDirsThread更新
      // 因此下面的方法必须在leaderIsrUpdateLock的读锁保护下执行
      // 以防止向无效的日志添加新的高水位
      inReadLock(leaderIsrUpdateLock) {
        leaderLogIfLocal.exists(leaderLog => maybeIncrementLeaderHW(leaderLog, followerFetchTimeMs))
      }
    } else {
      false
    }

    // 如果高水位或低水位发生变化，可能需要处理一些延迟的操作
    if (leaderLWIncremented || leaderHWIncremented)
      tryCompleteDelayedRequests()

    // 记录副本的日志末尾偏移量位置和起始偏移量
    debug(s"Recorded replica ${replica.brokerId} log end offset (LEO) position " +
      s"${followerFetchOffsetMetadata.messageOffset} and log start offset $followerStartOffset.")
  }

  /**
   * 更新主题分区的副本分配和ISR(In-Sync Replicas)集合。
   * 为每个新的远程broker创建一个新的Replica对象。ISR参数必须是分配参数的子集。
   * 
   * 并发控制说明：
   * 1. 由于remoteReplicasMap可能在无锁的情况下被访问，我们采用先添加新副本再删除旧副本的策略
   * 2. 这样可以确保在任何时刻都不会出现副本丢失的情况
   *
   * 注意：此方法为public可见性，主要用于测试目的
   *
   * @param replicas 分配给该主题分区的所有broker ID的有序序列
   * @param isLeader 当前副本是否为leader
   * @param isr 已知与leader保持同步的broker ID集合
   * @param addingReplicas 将要添加到分配中的所有broker ID的有序序列
   * @param removingReplicas 将要从分配中移除的所有broker ID的有序序列
   * @param leaderRecoveryState leader恢复状态
   */
  def updateAssignmentAndIsr(
    replicas: Seq[Int],
    isLeader: Boolean,
    isr: Set[Int],
    addingReplicas: Seq[Int],
    removingReplicas: Seq[Int],
    leaderRecoveryState: LeaderRecoveryState
  ): Unit = {
    if (isLeader) {
      // 获取所有follower副本（排除本地broker）
      val followers = replicas.filter(_ != localBrokerId)
      // 找出需要移除的副本
      val removedReplicas = remoteReplicasMap.keys.filterNot(followers.contains(_))

      // 由于代码路径可能在无锁的情况下访问remoteReplicasMap，
      // 我们采用先添加新副本再删除旧副本的策略
      followers.foreach(id => remoteReplicasMap.getAndMaybePut(id, new Replica(id, topicPartition, metadataCache)))
      remoteReplicasMap.removeAll(removedReplicas)
    } else {
      // 如果不是leader，清空远程副本映射表
      remoteReplicasMap.clear()
    }

    // 更新分配状态
    assignmentState = if (addingReplicas.nonEmpty || removingReplicas.nonEmpty)
      // 如果有副本正在添加或移除，使用OngoingReassignmentState表示正在进行重分配
      OngoingReassignmentState(addingReplicas, removingReplicas, replicas)
    else
      // 否则使用SimpleAssignmentState表示简单的副本分配状态
      SimpleAssignmentState(replicas)

    // 更新分区状态，包括ISR集合和leader恢复状态
    partitionState = CommittedPartitionState(isr, leaderRecoveryState)
  }

  /**
   * 检查并可能扩展分区的ISR(In-Sync Replicas)集合。
   * 
   * 副本加入ISR的条件：
   * 1. LEO(Log End Offset) >= 当前分区的高水位(HW)
   * 2. 已经追赶上当前leader epoch的偏移量
   * 
   * 副本必须追赶上当前leader epoch才能加入ISR的原因：
   * 如果leader的HW和LEO之间存在已提交的数据，而副本在获取这些数据之前就成为leader，
   * 这些数据就会丢失。
   * 
   * 特别说明：
   * 从技术上讲，如果副本超过replicaLagTimeMaxMs时间没有追赶上，即使其LEO >= HW，
   * 也不应该在ISR中。但为了与follower判断副本是否同步的逻辑保持一致，这里只检查HW。
   * 
   * 并发控制：
   * 1. 使用读锁检查是否需要更新ISR
   * 2. 使用写锁执行实际的ISR更新操作
   * 3. 在锁外提交AlterPartition请求，因为完成逻辑可能会增加高水位并完成延迟操作
   *
   * 触发时机：当副本的LEO增加时，可能触发此函数
   * 
   * @param followerReplica 需要检查的follower副本
   */
  private def maybeExpandIsr(followerReplica: Replica): Unit = {
    // 检查是否需要更新ISR：
    // 1. 当前没有正在进行的ISR更新
    // 2. 该副本可以被添加到ISR中
    // 3. 在读锁保护下确认需要扩展ISR
    val needsIsrUpdate = !partitionState.isInflight && canAddReplicaToIsr(followerReplica.brokerId) && inReadLock(leaderIsrUpdateLock) {
      needsExpandIsr(followerReplica)
    }
    
    if (needsIsrUpdate) {
      val alterIsrUpdateOpt = inWriteLock(leaderIsrUpdateLock) {
        // 在写锁保护下再次检查该副本是否需要被添加到ISR中
        partitionState match {
          case currentState: CommittedPartitionState if needsExpandIsr(followerReplica) =>
            // 准备ISR扩容更新
            Some(prepareIsrExpand(currentState, followerReplica.brokerId))
          case _ =>
            None
        }
      }
      // 在LeaderAndIsr锁外提交AlterPartition请求
      // 因为完成逻辑可能会增加高水位并导致延迟操作完成
      alterIsrUpdateOpt.foreach(submitAlterPartition)
    }
  }

  private def needsExpandIsr(followerReplica: Replica): Boolean = {
    canAddReplicaToIsr(followerReplica.brokerId) && isFollowerInSync(followerReplica)
  }

  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = partitionState
    !current.isInflight &&
      !current.isr.contains(followerReplicaId) &&
      isReplicaIsrEligible(followerReplicaId)
  }

  private def isFollowerInSync(followerReplica: Replica): Boolean = {
    leaderLogIfLocal.exists { leaderLog =>
      val followerEndOffset = followerReplica.stateSnapshot.logEndOffset
      followerEndOffset >= leaderLog.highWatermark && leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)
    }
  }

  private def isReplicaIsrEligible(followerReplicaId: Int): Boolean = {
    // A replica which meets all of the following requirements is allowed to join the ISR.
    // 1. It is not fenced.
    // 2. It is not in controlled shutdown.
    // 3. Its metadata cached broker epoch matches its Fetch request broker epoch. Or the Fetch
    //    request broker epoch is -1 which bypasses the epoch verification.
    val mayBeReplica = getReplica(followerReplicaId)
    // The topic is already deleted and we don't have any replica information. In this case, we can return false
    // so as to avoid NPE
    if (mayBeReplica.isEmpty) {
      warn(s"The replica state of replica ID:[$followerReplicaId] doesn't exist in the leader node. It might because the topic is already deleted.")
      return false
    }
    val storedBrokerEpoch = mayBeReplica.get.stateSnapshot.brokerEpoch
    val cachedBrokerEpoch = metadataCache.getAliveBrokerEpoch(followerReplicaId)
    !metadataCache.isBrokerFenced(followerReplicaId) &&
      !metadataCache.isBrokerShuttingDown(followerReplicaId) &&
      isBrokerEpochIsrEligible(storedBrokerEpoch, cachedBrokerEpoch)
  }

  private def isBrokerEpochIsrEligible(storedBrokerEpoch: Option[Long], cachedBrokerEpoch: Option[Long]): Boolean = {
    storedBrokerEpoch.isDefined && cachedBrokerEpoch.isDefined &&
      (storedBrokerEpoch.get == -1 || storedBrokerEpoch == cachedBrokerEpoch)
  }

  /**
   * 检查是否有足够的副本达到指定的偏移量。
   * 
   * 返回值说明：
   * - 返回一个元组，第一个元素是布尔值，表示是否有足够的副本达到了requiredOffset
   * - 第二个元素是错误码，如果没有错误则为Errors.NONE
   * 
   * 调用时机：
   * 此方法仅在requiredAcks = -1（等待所有ISR确认）时被调用。
   * 在确认生产请求之前，需要等待ISR中的所有副本都完全追赶上leader的对应偏移量。
   * 
   * 并发控制：
   * 此方法在处理生产请求时被调用，通过分区的读写锁保证并发安全。
   * 
   * @param requiredOffset 需要达到的目标偏移量
   * @return (是否有足够的副本达到偏移量, 错误码)
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        // 获取当前最大可能的ISR集合（包含已提交的ISR和正在添加的副本）
        val curMaximalIsr = partitionState.maximalIsr

        if (isTraceEnabled) {
          // 用于生成副本日志末端偏移量的字符串表示
          def logEndOffsetString: ((Int, Long)) => String = {
            case (brokerId, logEndOffset) => s"broker $brokerId: $logEndOffset"
          }

          // 获取当前同步的副本对象（不包括本地副本）
          val curInSyncReplicaObjects = (curMaximalIsr - localBrokerId).flatMap(getReplica)
          // 获取每个副本的末端偏移量信息
          val replicaInfo = curInSyncReplicaObjects.map(replica => (replica.brokerId, replica.stateSnapshot.logEndOffset))
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffset)
          // 将副本分为已确认和等待确认两组
          val (ackedReplicas, awaitingReplicas) = (replicaInfo + localLogInfo).partition { _._2 >= requiredOffset}

          // 记录追踪日志
          trace(s"Progress awaiting ISR acks for offset $requiredOffset: " +
            s"acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        // 获取有效的最小ISR数量
        val minIsr = effectiveMinIsr(leaderLog)
        if (leaderLog.highWatermark >= requiredOffset) {
          // 如果高水位已经超过了要求的偏移量
          // 注意：主题可能配置为在ISR中没有足够的副本时不接受消息
          // 在这种情况下，请求已经在本地追加并添加到延迟操作队列中，然后ISR才收缩
          if (minIsr <= curMaximalIsr.size)
            // ISR数量满足最小要求
            (true, Errors.NONE)
          else
            // ISR数量不足
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          // 高水位未达到要求的偏移量
          (false, Errors.NONE)
      case None =>
        // 不是leader副本
        (false, Errors.NOT_LEADER_OR_FOLLOWER)
    }
  }

  /**
   * 检查并可能增加分区的高水位标记(HW)。
   * 该函数在以下情况下会被触发：
   *
   * 1. 分区的ISR(同步副本集合)发生变更
   * 2. 任何副本的LEO(日志末端偏移量)发生变更
   *
   * 高水位的确定规则：
   * - 取所有同步副本中最小的日志末端偏移量
   * - 或者考虑那些已追赶上且有资格加入ISR的副本
   * 如果一个副本已经追赶上但其LEO小于当前HW，我们会等待该副本追赶到HW再推进HW。
   * 这有助于处理ISR中只有leader副本，而follower正在追赶的情况。如果不等待follower，
   * follower的LEO可能会一直落后于HW(由leader的LEO决定)，从而永远无法加入ISR。
   *
   * 高水位推进的条件：
   * - ISR大小必须大于等于最小ISR配置(min.insync.replicas)
   * - 使用AlterPartition时，我们也会将新加入的副本视为ISR的一部分
   * 这些副本尚未被控制器提交到ISR中，我们可能会回退到之前提交的ISR。
   * 但是将额外的副本加入ISR会使条件更严格，因此是安全的。
   * 我们称这个集合为"最大化"ISR。详见KIP-497。
   *
   * 注意：不需要在这里获取leaderIsrUpdate锁，因为所有调用此私有API的地方都已获取该锁
   *
   * @param leaderLog leader副本的日志
   * @param currentTimeMs 当前时间戳(毫秒)
   * @return 如果高水位被增加则返回true，否则返回false
   */
  private def maybeIncrementLeaderHW(leaderLog: UnifiedLog, currentTimeMs: Long = time.milliseconds): Boolean = {
    // 如果ISR大小小于最小要求，不更新高水位
    if (isUnderMinIsr) {
      trace(s"Not increasing HWM because partition is under min ISR(ISR=${partitionState.isr}")
      return false
    }
    
    // 获取leader日志的末端偏移量，初始化新的高水位
    val leaderLogEndOffset = leaderLog.logEndOffsetMetadata
    var newHighWatermark = leaderLogEndOffset
    
    // 遍历所有远程副本，找出最小的日志末端偏移量
    remoteReplicasMap.values.foreach { replica =>
      val replicaState = replica.stateSnapshot

      // 判断是否需要等待副本加入ISR
      def shouldWaitForReplicaToJoinIsr: Boolean = {
        // 检查副本是否已追赶上且有资格加入ISR
        replicaState.isCaughtUp(leaderLogEndOffset.messageOffset, currentTimeMs, replicaLagTimeMaxMs) &&
        isReplicaIsrEligible(replica.brokerId)
      }

      // 如果副本的LEO小于当前高水位，且副本在最大化ISR中或应该等待其加入ISR
      // 则更新高水位为该副本的LEO
      if (replicaState.logEndOffsetMetadata.messageOffset < newHighWatermark.messageOffset &&
          (partitionState.maximalIsr.contains(replica.brokerId) || shouldWaitForReplicaToJoinIsr)
      ) {
        newHighWatermark = replicaState.logEndOffsetMetadata
      }
    }

    // 尝试更新leader日志的高水位
    leaderLog.maybeIncrementHighWatermark(newHighWatermark) match {
      case Some(oldHighWatermark) =>
        // 高水位成功更新，记录日志
        debug(s"High watermark updated from $oldHighWatermark to $newHighWatermark")
        true

      case None =>
        // 格式化副本日志末端偏移量信息的函数
        def logEndOffsetString: ((Int, LogOffsetMetadata)) => String = {
          case (brokerId, logEndOffsetMetadata) => s"replica $brokerId: $logEndOffsetMetadata"
        }

        // 如果启用了跟踪日志，记录所有副本的LEO信息
        if (isTraceEnabled) {
          val replicaInfo = remoteReplicas.map(replica => (replica.brokerId, replica.stateSnapshot.logEndOffsetMetadata)).toSet
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffsetMetadata)
          trace(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old value. " +
            s"All current LEOs are ${(replicaInfo + localLogInfo).map(logEndOffsetString)}")
        }
        false
    }
  }

  /**
   * 获取分区的低水位值，仅当本地副本是分区leader时才计算
   * 低水位主要用于leader broker判断DeleteRecordsRequest是否可以执行
   * 其值为所有存活副本中最小的logStartOffset
   * 当leader broker收到FetchRequest或DeleteRecordsRequest时，低水位可能会增加
   *
   * @return 分区的低水位值
   * @throws NotLeaderOrFollowerException 如果本地副本不是leader
   */
  def lowWatermarkIfLeader: Long = {
    // 检查本地副本是否是leader
    if (!isLeader)
      throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")

    // 初始化低水位为本地日志的起始偏移量
    var lowWaterMark = localLogOrException.logStartOffset
    
    // 遍历所有远程副本，找出最小的日志起始偏移量
    remoteReplicas.foreach { replica =>
      val logStartOffset = replica.stateSnapshot.logStartOffset
      // 只考虑存活的broker上的副本
      if (metadataCache.hasAliveBroker(replica.brokerId) && logStartOffset < lowWaterMark) {
        lowWaterMark = logStartOffset
      }
    }

    // 如果存在future日志，取当前低水位和future日志起始偏移量的较小值
    futureLog match {
      case Some(partitionFutureLog) =>
        Math.min(lowWaterMark, partitionFutureLog.logStartOffset)
      case None =>
        lowWaterMark
    }
  }

  /**
   * 尝试完成所有待处理的请求
   * 调用此方法时不应持有leaderIsrUpdateLock锁
   */
  def tryCompleteDelayedRequests(): Unit = {
    delayedOperations.checkAndCompleteAll()
  }

  /**
   * 尝试收缩ISR(同步副本集合)
   * 当发现有副本不再同步时，将其从ISR中移除
   */
  def maybeShrinkIsr(): Unit = {
    // 检查是否需要更新ISR
    def needsIsrUpdate: Boolean = {
      // 确保没有正在进行的ISR更新，并在读锁下检查是否需要收缩ISR
      !partitionState.isInflight && inReadLock(leaderIsrUpdateLock) {
        needsShrinkIsr()
      }
    }

    // 如果需要更新ISR
    if (needsIsrUpdate) {
      // 在写锁保护下准备ISR更新
      val alterIsrUpdateOpt = inWriteLock(leaderIsrUpdateLock) {
        leaderLogIfLocal.flatMap { leaderLog =>
          // 获取不同步的副本ID列表
          val outOfSyncReplicaIds = getOutOfSyncReplicas(replicaLagTimeMaxMs)
          partitionState match {
            // 如果当前是已提交状态且存在不同步的副本
            case currentState: CommittedPartitionState if outOfSyncReplicaIds.nonEmpty =>
              // 收集不同步副本的详细信息用于日志记录
              val outOfSyncReplicaLog = outOfSyncReplicaIds.map { replicaId =>
                val replicaStateSnapshot = getReplica(replicaId).map(_.stateSnapshot)
                val logEndOffsetMessage = replicaStateSnapshot
                  .map(_.logEndOffset.toString)
                  .getOrElse("unknown")
                val lastCaughtUpTimeMessage = replicaStateSnapshot
                  .map(_.lastCaughtUpTimeMs.toString)
                  .getOrElse("unknown")
                s"(brokerId: $replicaId, endOffset: $logEndOffsetMessage, lastCaughtUpTimeMs: $lastCaughtUpTimeMessage)"
              }.mkString(" ")
              // 计算新的ISR
              val newIsrLog = (partitionState.isr -- outOfSyncReplicaIds).mkString(",")
              // 记录ISR收缩的详细信息
              info(s"Shrinking ISR from ${partitionState.isr.mkString(",")} to $newIsrLog. " +
                s"Leader: (highWatermark: ${leaderLog.highWatermark}, " +
                s"endOffset: ${leaderLog.logEndOffset}). " +
                s"Out of sync replicas: $outOfSyncReplicaLog.")
              // 准备ISR收缩操作
              Some(prepareIsrShrink(currentState, outOfSyncReplicaIds))
            case _ =>
              None
          }
        }
      }
      // 在释放锁后提交ISR更新请求
      // 因为完成逻辑可能会增加高水位并完成延迟操作
      alterIsrUpdateOpt.foreach(submitAlterPartition)
    }
  }

  /**
   * 检查是否需要收缩ISR
   * 当本地副本是leader且存在不同步的副本时返回true
   */
  private def needsShrinkIsr(): Boolean = {
    leaderLogIfLocal.exists { _ => getOutOfSyncReplicas(replicaLagTimeMaxMs).nonEmpty }
  }

  /**
   * 检查follower副本是否不同步
   *
   * @param replicaId 要检查的副本ID
   * @param leaderEndOffset leader的日志末端偏移量
   * @param currentTimeMs 当前时间戳(毫秒)
   * @param maxLagMs 允许的最大延迟时间(毫秒)
   * @return 如果副本不同步返回true
   */
  private def isFollowerOutOfSync(replicaId: Int,
                                  leaderEndOffset: Long,
                                  currentTimeMs: Long,
                                  maxLagMs: Long): Boolean = {
    // 如果找不到副本则认为不同步
    getReplica(replicaId).fold(true) { followerReplica =>
      // 检查副本是否未追赶上leader
      !followerReplica.stateSnapshot.isCaughtUp(leaderEndOffset, currentTimeMs, maxLagMs)
    }
  }

  /**
   * 获取所有不同步的副本ID集合
   * 如果follower的LEO与leader相同，则不会被视为不同步
   * 否则会处理以下两种情况：
   * 1. 卡住的follower：如果副本的LEO在maxLagMs毫秒内没有更新
   *                   则认为follower卡住了，应该从ISR中移除
   * 2. 慢速的follower：如果副本在最近maxLagMs毫秒内未能读取到LEO
   *                   则认为follower落后了，应该从ISR中移除
   * 这两种情况都通过检查lastCaughtUpTimeMs来判断
   * lastCaughtUpTimeMs表示副本最后一次完全追赶上的时间
   * 如果违反了上述任一条件，该副本就被认为是不同步的
   *
   * 如果有正在进行的ISR更新，则返回空集合
   *
   * @param maxLagMs 允许的最大延迟时间(毫秒)
   * @return 不同步的副本ID集合
   */
  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
    val current = partitionState
    if (!current.isInflight) {
      // 获取除了本地副本外的所有ISR成员
      val candidateReplicaIds = current.isr - localBrokerId
      val currentTimeMs = time.milliseconds()
      val leaderEndOffset = localLogOrException.logEndOffset
      // 过滤出不同步的副本
      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
    } else {
      // 如果有正在进行的ISR更新，返回空集合
      Set.empty
    }
  }

  /**
   * 将记录追加到Follower副本或Future副本的内部实现方法
   * 
   * @param records 要追加的内存记录集合
   * @param isFuture 是否追加到Future副本
   * @return 追加操作的结果信息，如果成功返回Some(LogAppendInfo)，如果副本不存在返回None
   */
  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    if (isFuture) {
      // 需要读锁来处理请求处理线程在接收到AlterReplicaLogDirsRequest后尝试删除future副本的竞态条件
      inReadLock(leaderIsrUpdateLock) {
        // 注意：如果副本在调用此方法之前被非ReplicaAlterLogDirsThread线程移除，则副本可能未定义
        futureLog.map { _.appendAsFollower(records) }
      }
    } else {
      // 需要锁来防止在ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()替换follower副本时更新follower副本
      futureLogLock.synchronized {
        Some(localLogOrException.appendAsFollower(records))
      }
    }
  }

  /**
   * 将记录追加到Follower副本或Future副本
   * 
   * 该方法处理了一种特殊情况：当Leader（或当前副本）的日志起始偏移量因删除记录请求而落在批次中间，
   * 且Follower尝试从Leader获取其第一个偏移量时可能发生的情况。在这种情况下，我们需要：
   * 1. 删除以日志起始偏移量开始的段
   * 2. 创建一个具有较早偏移量（批次的基准偏移量）的新段
   * 3. 在追加之前检查新的恢复点
   * 
   * @param records 要追加的内存记录集合
   * @param isFuture 是否追加到Future副本
   * @return 追加操作的结果信息
   * @throws UnexpectedAppendOffsetException 如果追加的记录偏移量不符合预期且不满足特殊处理条件
   */
  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    try {
      // 尝试追加记录到副本
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        // 获取目标日志（future或本地）
        val log = if (isFuture) futureLocalLogOrException else localLogOrException
        val logEndOffset = log.logEndOffset
        // 检查是否满足特殊处理条件：
        // 1. 日志结束偏移量等于起始偏移量（空日志）
        // 2. 要追加的第一个偏移量小于日志结束偏移量
        // 3. 要追加的最后一个偏移量大于等于日志结束偏移量
        if (logEndOffset == log.logStartOffset &&
            e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${log.logStartOffset}." +
               s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          // 完全截断日志并从新的偏移量开始
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          // 重新尝试追加记录
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          // 如果不满足特殊处理条件，则抛出异常
          throw e
    }
  }

  /**
   * 将记录追加到Leader副本
   * 
   * 该方法在追加记录之前会进行以下检查：
   * 1. 确保本地副本是Leader
   * 2. 确保同步副本数量满足最小ISR要求（当requiredAcks=-1时）
   * 3. 追加记录后可能需要更新高水位标记
   * 
   * @param records 要追加的内存记录集合
   * @param origin 追加来源（客户端/复制/压缩等）
   * @param requiredAcks 所需的确认数（-1表示需要所有ISR确认）
   * @param requestLocal 请求本地信息
   * @param verificationGuard 验证保护器
   * @return 追加操作的结果信息，包含高水位是否增加的标记
   * @throws NotLeaderOrFollowerException 如果本地副本不是Leader
   * @throws NotEnoughReplicasException 如果同步副本数量不足且requiredAcks=-1
   */
  def appendRecordsToLeader(records: MemoryRecords, origin: AppendOrigin, requiredAcks: Int,
                            requestLocal: RequestLocal, verificationGuard: VerificationGuard = VerificationGuard.SENTINEL): LogAppendInfo = {
    // 在读锁保护下执行追加操作
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal match {
        case Some(leaderLog) =>
          // 获取有效的最小ISR数量和当前ISR大小
          val minIsr = effectiveMinIsr(leaderLog)
          val inSyncSize = partitionState.isr.size

          // 如果同步副本数量不足且需要所有ISR确认，则拒绝写入
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR : $inSyncSize " +
              s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition, " +
              s"live replica(s) broker.id are : $inSyncReplicaIds")
          }

          // 以Leader身份追加记录
          val info = leaderLog.appendAsLeader(records, leaderEpoch = this.leaderEpoch, origin,
            requestLocal, verificationGuard)

          // 由于ISR可能只剩下1个，需要检查是否需要增加高水位
          (info, maybeIncrementLeaderHW(leaderLog))

        case None =>
          // 如果本地副本不是Leader，抛出异常
          throw new NotLeaderOrFollowerException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // 返回追加结果，并标记高水位是否发生变化
    info.copy(if (leaderHWIncremented) LeaderHwChange.INCREASED else LeaderHwChange.SAME)
  }

  /**
   * 从分区获取记录
   * 
   * 该方法根据请求来源（Follower或Consumer）采用不同的处理逻辑：
   * 1. Follower请求：需要验证请求的合法性，并可能更新Follower的获取状态
   * 2. Consumer请求：直接从本地日志读取数据
   * 
   * @param fetchParams Fetch请求的参数
   * @param fetchPartitionData 分区级别的Fetch参数（如获取偏移量）
   * @param fetchTimeMs broker上此获取请求的当前时间（毫秒）
   * @param maxBytes 返回的最大字节数
   * @param minOneMessage 是否确保至少返回一条完整消息
   * @param updateFetchState 是否更新副本状态（仅适用于follower获取）
   * @return [[LogReadInfo]] 包含获取的记录或存在的分歧epoch
   * @throws NotLeaderOrFollowerException 当节点不是当前leader且启用了fetchOnlyLeader，或这是较旧请求版本的follower获取且replicaId不在当前有效副本中时
   * @throws FencedLeaderEpochException 当Fetch请求中的leader epoch小于当前leader epoch时
   * @throws UnknownLeaderEpochException 当Fetch请求中的leader epoch大于当前leader epoch，或这是follower获取且replicaId不在当前有效副本中时
   * @throws OffsetOutOfRangeException 当获取偏移量小于日志起始偏移量或大于日志结束偏移量（或根据FetchParams.isolation的高水位），
   *                                   或无法从本地epoch缓存确定FetchRequest.PartitionData中最后获取的epoch的结束偏移量时
   */
  def fetchRecords(
    fetchParams: FetchParams,
    fetchPartitionData: FetchRequest.PartitionData,
    fetchTimeMs: Long,
    maxBytes: Int,
    minOneMessage: Boolean,
    updateFetchState: Boolean
  ): LogReadInfo = {
    // 定义从本地日志读取记录的辅助函数
    def readFromLocalLog(log: UnifiedLog): LogReadInfo = {
      readRecords(
        log,
        fetchPartitionData.lastFetchedEpoch,
        fetchPartitionData.fetchOffset,
        fetchPartitionData.currentLeaderEpoch,
        maxBytes,
        fetchParams.isolation,
        minOneMessage
      )
    }

    if (fetchParams.isFromFollower) {
      // 如果是Follower的请求，在读取之前检查请求是否来自有效的副本
      val (replica, logReadInfo) = inReadLock(leaderIsrUpdateLock) {
        // 获取本地日志，如果epoch不匹配则抛出异常
        val localLog = localLogWithEpochOrThrow(
          fetchPartitionData.currentLeaderEpoch,
          fetchParams.fetchOnlyLeader
        )
        // 获取Follower副本，如果副本无效则抛出异常
        val replica = followerReplicaOrThrow(
          fetchParams.replicaId,
          fetchPartitionData
        )
        // 从本地日志读取数据
        val logReadInfo = readFromLocalLog(localLog)
        (replica, logReadInfo)
      }

      // 如果需要更新获取状态且没有epoch分歧，则更新Follower的获取状态
      if (updateFetchState && !logReadInfo.divergingEpoch.isPresent) {
        updateFollowerFetchState(
          replica,
          followerFetchOffsetMetadata = logReadInfo.fetchedData.fetchOffsetMetadata,
          followerStartOffset = fetchPartitionData.logStartOffset,
          followerFetchTimeMs = fetchTimeMs,
          leaderEndOffset = logReadInfo.logEndOffset,
          fetchParams.replicaEpoch
        )
      }

      logReadInfo
    } else {
      // 如果是Consumer的请求，在读锁保护下直接从本地日志读取数据
      inReadLock(leaderIsrUpdateLock) {
        val localLog = localLogWithEpochOrThrow(
          fetchPartitionData.currentLeaderEpoch,
          fetchParams.fetchOnlyLeader
        )
        readFromLocalLog(localLog)
      }
    }
  }

  /**
   * 获取跟随者副本对象，如果副本不存在则抛出异常
   * 
   * @param replicaId 跟随者副本的ID
   * @param fetchPartitionData 获取请求的分区数据
   * @return 跟随者副本对象
   * @throws KafkaException 当副本不存在或不合法时抛出异常
   */
  private def followerReplicaOrThrow(
    replicaId: Int,
    fetchPartitionData: FetchRequest.PartitionData
  ): Replica = {
    // 尝试获取副本对象，如果不存在则执行异常处理
    getReplica(replicaId).getOrElse {
      // 记录调试日志，说明无法找到对应的副本
      debug(s"Leader $localBrokerId failed to record follower $replicaId's position " +
        s"${fetchPartitionData.fetchOffset}, and last sent high watermark since the replica is " +
        s"not recognized to be one of the assigned replicas ${assignmentState.replicas.mkString(",")} " +
        s"for leader epoch $leaderEpoch with partition epoch $partitionEpoch")

      // 根据请求中是否包含leader epoch来决定返回的错误类型
      val error = if (fetchPartitionData.currentLeaderEpoch.isPresent) {
        // 如果请求中包含leader epoch并且与本地epoch匹配，但副本不在副本集合中
        // 这种情况在KRaft中可能发生，例如在重分配过程中添加新副本时
        // 返回UNKNOWN_LEADER_EPOCH错误，表示(replicaId, leaderEpoch)组合尚未被识别为有效
        // 这会导致follower重试请求
        Errors.UNKNOWN_LEADER_EPOCH
      } else {
        // 如果请求中没有leader epoch，说明是较旧的版本
        // 此时无法判断是follower状态过期还是本地状态有问题
        // 返回NOT_LEADER_OR_FOLLOWER错误，让follower重试请求
        Errors.NOT_LEADER_OR_FOLLOWER
      }

      // 抛出异常，包含详细的错误信息
      throw error.exception(s"Replica $replicaId is not recognized as a " +
        s"valid replica of $topicPartition in leader epoch $leaderEpoch with " +
        s"partition epoch $partitionEpoch")
    }
  }

  /**
   * 从本地日志中读取消息记录
   * 
   * @param localLog 本地日志对象
   * @param lastFetchedEpoch 上次获取的epoch(可选)
   * @param fetchOffset 要获取的起始偏移量
   * @param currentLeaderEpoch 当前leader的epoch(可选)
   * @param maxBytes 最大读取字节数
   * @param fetchIsolation 获取隔离级别
   * @param minOneMessage 是否至少返回一条消息
   * @return 日志读取信息
   */
  private def readRecords(
    localLog: UnifiedLog,
    lastFetchedEpoch: Optional[Integer],
    fetchOffset: Long,
    currentLeaderEpoch: Optional[Integer],
    maxBytes: Int,
    fetchIsolation: FetchIsolation,
    minOneMessage: Boolean
  ): LogReadInfo = {
    // 在读取之前记录日志的结束偏移量，这确保了在获取过程中的新追加不会阻止follower同步
    val initialHighWatermark = localLog.highWatermark
    val initialLogStartOffset = localLog.logStartOffset
    val initialLogEndOffset = localLog.logEndOffset
    val initialLastStableOffset = localLog.lastStableOffset

    // 如果提供了上次获取的epoch，进行epoch验证
    lastFetchedEpoch.ifPresent { fetchEpoch =>
      // 获取指定leader epoch的最后偏移量
      val epochEndOffset = lastOffsetForLeaderEpoch(currentLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
      val error = Errors.forCode(epochEndOffset.errorCode)
      if (error != Errors.NONE) {
        throw error.exception()
      }

      // 检查epoch的结束偏移量是否有效
      if (epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
        throw new OffsetOutOfRangeException("Could not determine the end offset of the last fetched epoch " +
          s"$lastFetchedEpoch from the request")
      }

      // 如果获取的偏移量小于日志起始偏移量，抛出异常
      // 无论epoch是否发生分歧，都会执行此检查
      if (fetchOffset < initialLogStartOffset) {
        throw new OffsetOutOfRangeException(s"Received request for offset $fetchOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $initialLogStartOffset to $initialLogEndOffset.")
      }

      // 检查epoch是否发生分歧
      // 如果leader的epoch小于follower的fetch epoch，或者结束偏移量小于请求的偏移量
      // 说明发生了日志截断，需要返回分歧信息
      if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
        val divergingEpoch = new FetchResponseData.EpochEndOffset()
          .setEpoch(epochEndOffset.leaderEpoch)
          .setEndOffset(epochEndOffset.endOffset)

        return new LogReadInfo(
          FetchDataInfo.empty(fetchOffset),
          Optional.of(divergingEpoch),
          initialHighWatermark,
          initialLogStartOffset,
          initialLogEndOffset,
          initialLastStableOffset)
      }
    }

    // 从本地日志读取数据
    val fetchedData = localLog.read(
      fetchOffset,
      maxBytes,
      fetchIsolation,
      minOneMessage
    )

    // 返回读取结果
    new LogReadInfo(
      fetchedData,
      Optional.empty(),
      initialHighWatermark,
      initialLogStartOffset,
      initialLogEndOffset,
      initialLastStableOffset
    )
  }

  /**
   * 根据时间戳查找对应的偏移量
   * 
   * @param timestamp 要查找的时间戳
   * @param isolationLevel 隔离级别(可选)
   * @param currentLeaderEpoch 当前leader的epoch(可选)
   * @param fetchOnlyFromLeader 是否只从leader获取
   * @param remoteLogManager 远程日志管理器(可选)
   * @return 偏移量查找结果
   */
  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean,
                              remoteLogManager: Option[RemoteLogManager] = None): OffsetResultHolder = inReadLock(leaderIsrUpdateLock) {
    // 获取本地日志对象，如果不满足条件则抛出异常
    val localLog = localLogWithEpochOrThrow(currentLeaderEpoch, fetchOnlyFromLeader)

    // 根据隔离级别确定最后可获取的偏移量
    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localLog.lastStableOffset    // 读已提交
      case Some(IsolationLevel.READ_UNCOMMITTED) => localLog.highWatermark    // 读未提交
      case None => localLog.logEndOffset                                      // 无隔离级别
    }

    // 构建epoch日志字符串，用于错误消息
    val epochLogString = if (currentLeaderEpoch.isPresent) {
      s"epoch ${currentLeaderEpoch.get}"
    } else {
      "unknown epoch"
    }

    // 检查是否需要抛出错误
    // 仅当收到客户端请求(隔离级别已定义)且高水位落后于起始偏移量时才考虑抛出错误
    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localLog.highWatermark)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(s"Failed to fetch offsets for " +
        s"partition $topicPartition with leader $epochLogString as this partition's " +
        s"high watermark (${localLog.highWatermark}) is lagging behind the " +
        s"start offset from the beginning of this epoch ($epochStart)."))

    // 根据时间戳获取偏移量的辅助函数
    def getOffsetByTimestamp: OffsetResultHolder = {
      logManager.getLog(topicPartition)
        .map(log => log.fetchOffsetByTimestamp(timestamp, remoteLogManager))
        .getOrElse(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()))
    }

    // 根据时间戳类型处理不同的查找逻辑
    timestamp match {
      case ListOffsetsRequest.LATEST_TIMESTAMP =>
        // 如果是查找最新偏移量，检查是否需要抛出错误
        maybeOffsetsError.map(e => throw e)
          .getOrElse(new OffsetResultHolder(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetsRequest.EARLIEST_TIMESTAMP | ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP =>
        // 如果是查找最早偏移量，直接调用辅助函数
        getOffsetByTimestamp
      case _ =>
        // 对于其他时间戳，获取偏移量并设置额外信息
        val offsetResultHolder = getOffsetByTimestamp
        offsetResultHolder.maybeOffsetsError(OptionConverters.toJava(maybeOffsetsError))
        offsetResultHolder.lastFetchableOffset(Optional.of(lastFetchableOffset))
        offsetResultHolder
    }
  }

  /**
   * 获取分区上活跃生产者的状态信息
   * 该方法用于获取当前分区上所有活跃生产者的状态，包括事务ID、生产者ID等信息
   * 
   * @return 包含活跃生产者信息的PartitionResponse对象
   */
  def activeProducerState: DescribeProducersResponseData.PartitionResponse = {
    // 创建响应对象并设置分区索引
    val producerState = new DescribeProducersResponseData.PartitionResponse()
      .setPartitionIndex(topicPartition.partition())

    // 尝试获取本地日志中的活跃生产者信息
    log.map(_.activeProducers) match {
      case Some(producers) =>
        // 如果成功获取到生产者信息，设置成功状态码并添加生产者列表
        producerState
          .setErrorCode(Errors.NONE.code)
          .setActiveProducers(producers.asJava)
      case None =>
        // 如果无法获取生产者信息（可能不是leader），设置相应的错误码
        producerState
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
    }

    producerState
  }

  /**
   * 获取分区的偏移量快照信息
   * 在读锁保护下获取本地日志的偏移量快照，包括日志起始偏移量、高水位等信息
   * 
   * @param currentLeaderEpoch 当前leader的epoch值
   * @param fetchOnlyFromLeader 是否只从leader获取数据
   * @return 包含各种偏移量信息的快照对象
   */
  def fetchOffsetSnapshot(currentLeaderEpoch: Optional[Integer],
                          fetchOnlyFromLeader: Boolean): LogOffsetSnapshot = inReadLock(leaderIsrUpdateLock) {
    // 获取本地日志对象，如果epoch不匹配或者需要从leader获取但当前不是leader则抛出异常
    val localLog = localLogWithEpochOrThrow(currentLeaderEpoch, fetchOnlyFromLeader)
    localLog.fetchOffsetSnapshot
  }

  /**
   * 获取分区日志的起始偏移量
   * 在读锁保护下获取本地leader日志的起始偏移量，如果不是leader则返回-1
   * 
   * @return 日志的起始偏移量，如果不是leader则返回-1
   */
  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal.map(_.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * 在leader副本上删除指定偏移量之前的日志记录
   * 该操作会更新日志起始偏移量和低水位，可能触发日志段的删除和日志的滚动
   * 需要满足两个条件：1) 偏移量不超过高水位 2) 当前broker是该分区的leader
   *
   * @param offset 要删除到的目标偏移量
   * @return 包含请求的偏移量和分区低水位的结果对象
   * @throws PolicyViolationException 如果分区配置不允许删除操作
   * @throws OffsetOutOfRangeException 如果指定的偏移量无效
   * @throws NotLeaderOrFollowerException 如果当前broker不是leader
   */
  def deleteRecordsOnLeader(offset: Long): LogDeleteRecordsResult = inReadLock(leaderIsrUpdateLock) {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        // 检查分区是否允许删除操作
        if (!leaderLog.config.delete)
          throw new PolicyViolationException(s"Records of partition $topicPartition can not be deleted due to the configured policy")

        // 如果指定HIGH_WATERMARK，则使用当前的高水位作为目标偏移量
        val convertedOffset = if (offset == DeleteRecordsRequest.HIGH_WATERMARK)
          leaderLog.highWatermark
        else
          offset

        // 验证偏移量的有效性
        if (convertedOffset < 0)
          throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

        // 尝试增加日志的起始偏移量，这可能会触发日志段的删除
        leaderLog.maybeIncrementLogStartOffset(convertedOffset, LogStartOffsetIncrementReason.ClientRecordDeletion)
        LogDeleteRecordsResult(
          requestedOffset = convertedOffset,
          lowWatermark = lowWatermarkIfLeader)
      case None =>
        throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    }
  }

  /**
   * 将分区的本地日志截断到指定的偏移量，并将恢复点检查点设置到该偏移量
   * 在读锁保护下执行截断操作，以防止在ReplicaAlterDirThread执行过程中被截断
   *
   * @param offset 要截断到的目标偏移量
   * @param isFuture 是否在future日志上执行截断操作
   */
  def truncateTo(offset: Long, isFuture: Boolean): Unit = {
    // 获取读锁以防止在ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()时
    // follower副本被截断
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
   * 完全删除分区的本地日志数据，并从新的偏移量开始
   * 在读锁保护下执行操作，确保在日志目录切换过程中的安全性
   *
   * @param newOffset 新的起始偏移量
   * @param isFuture 是否在future日志上执行操作
   * @param logStartOffsetOpt 可选的日志起始偏移量，如果未指定则使用newOffset
   */
  def truncateFullyAndStartAt(newOffset: Long,
                              isFuture: Boolean,
                              logStartOffsetOpt: Option[Long] = None): Unit = {
    // 获取读锁以防止在ReplicaAlterDirThread执行maybeReplaceCurrentWithFutureReplica()时
    // follower副本被截断
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture, logStartOffsetOpt)
    }
  }

  /**
   * 查找小于等于请求的epoch值的最大epoch对应的最后偏移量（不包含）
   * 该方法用于处理日志截断和副本同步等场景，帮助确定日志同步的起始位置
   *
   * @param currentLeaderEpoch 当前leader的epoch值（如果已知）
   * @param leaderEpoch 请求的leader epoch值
   * @param fetchOnlyFromLeader 是否只从leader获取数据
   * @return 包含请求的epoch和对应结束偏移量的响应对象。如果请求的epoch未知，
   *         则返回小于请求epoch的最大epoch及其结束偏移量。epoch的结束偏移量定义为
   *         大于该epoch的第一个epoch的起始偏移量，如果是最新的epoch则为日志的结束偏移量
   */
  def lastOffsetForLeaderEpoch(currentLeaderEpoch: Optional[Integer],
                               leaderEpoch: Int,
                               fetchOnlyFromLeader: Boolean): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      // 获取本地日志或错误信息
      val localLogOrError = getLocalLog(currentLeaderEpoch, fetchOnlyFromLeader)
      localLogOrError match {
        case Left(localLog) =>
          // 查找指定epoch的结束偏移量
          localLog.endOffsetForEpoch(leaderEpoch) match {
            case Some(epochAndOffset) => new EpochEndOffset()
              .setPartition(partitionId)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(epochAndOffset.leaderEpoch)
              .setEndOffset(epochAndOffset.offset)
            case None => new EpochEndOffset()
              .setPartition(partitionId)
              .setErrorCode(Errors.NONE.code)
          }
        case Right(error) => new EpochEndOffset()
          .setPartition(partitionId)
          .setErrorCode(error.code)
      }
    }
  }

  /**
   * 准备ISR集合的扩容操作
   * 
   * 在扩容ISR时，我们假设新的副本会在收到确认之前就进入ISR集合。这样可以确保高水位(HW)已经反映了更新后的ISR状态，
   * 即使在收到确认之前有一定延迟。即使更新失败也不会造成问题，因为扩容后的ISR对高水位的推进有更严格的要求。
   * 
   * @param currentState 当前已提交的分区状态
   * @param newInSyncReplicaId 要加入ISR的新副本ID
   * @return 待处理的ISR扩容状态
   */
  private def prepareIsrExpand(
    currentState: CommittedPartitionState,
    newInSyncReplicaId: Int
  ): PendingExpandIsr = {
    // 将新副本添加到当前ISR集合中
    val isrToSend = partitionState.isr + newInSyncReplicaId
    // 为ISR中的每个副本添加broker epoch信息
    val isrWithBrokerEpoch = addBrokerEpochToIsr(isrToSend.toList).asJava
    // 创建新的LeaderAndIsr请求
    val newLeaderAndIsr = new LeaderAndIsr(
      localBrokerId,  // 本地broker作为leader
      leaderEpoch,    // 当前的leader epoch
      partitionState.leaderRecoveryState,  // leader恢复状态
      isrWithBrokerEpoch,  // 带有broker epoch的ISR集合
      partitionEpoch  // 分区epoch
    )
    // 创建待处理的ISR扩容状态
    val updatedState = PendingExpandIsr(
      newInSyncReplicaId,  // 新加入的副本ID
      newLeaderAndIsr,     // 新的LeaderAndIsr请求
      currentState         // 当前的分区状态
    )
    // 更新分区状态
    partitionState = updatedState
    updatedState
  }

  /**
   * 准备ISR集合的缩容操作
   * 
   * 在缩容ISR时，我们不能假设更新一定会成功，因为如果AlterPartition请求失败，这可能会错误地推进高水位。
   * 因此，对于PendingShrinkIsr状态，其"最大ISR"就是当前的ISR集合。
   * 
   * @param currentState 当前已提交的分区状态
   * @param outOfSyncReplicaIds 要从ISR中移除的副本ID集合
   * @return 待处理的ISR缩容状态
   */
  private[cluster] def prepareIsrShrink(
    currentState: CommittedPartitionState,
    outOfSyncReplicaIds: Set[Int]
  ): PendingShrinkIsr = {
    // 从当前ISR集合中移除不同步的副本
    val isrToSend = partitionState.isr -- outOfSyncReplicaIds
    // 为剩余的ISR副本添加broker epoch信息
    val isrWithBrokerEpoch = addBrokerEpochToIsr(isrToSend.toList).asJava
    // 创建新的LeaderAndIsr请求
    val newLeaderAndIsr = new LeaderAndIsr(
      localBrokerId,  // 本地broker作为leader
      leaderEpoch,    // 当前的leader epoch
      partitionState.leaderRecoveryState,  // leader恢复状态
      isrWithBrokerEpoch,  // 带有broker epoch的ISR集合
      partitionEpoch  // 分区epoch
    )
    // 创建待处理的ISR缩容状态
    val updatedState = PendingShrinkIsr(
      outOfSyncReplicaIds,  // 要移除的副本ID集合
      newLeaderAndIsr,      // 新的LeaderAndIsr请求
      currentState          // 当前的分区状态
    )
    // 更新分区状态
    partitionState = updatedState
    updatedState
  }

  /**
   * 为ISR集合中的每个副本添加broker epoch信息
   * 
   * 该方法处理两种情况：
   * 1. 本地broker：直接使用本地的broker epoch
   * 2. 远程broker：从远程副本的状态快照中获取broker epoch
   * 
   * 对于远程broker，如果broker epoch缺失，有两种情况：
   * 1. ISR扩容时：由于已经持有分区锁并完成了broker epoch检查，新的ISR副本应该有有效的broker epoch。
   *    此时broker epoch缺失只可能发生在现有ISR副本上，这些副本的fetch请求还未被leader接收。
   *    此时将epoch设为-1是安全的，因为即使该副本此时崩溃，controller会将其从ISR中移除，
   *    并增加分区epoch来拒绝这个AlterPartition请求。
   * 2. ISR缩容时：同样，如果现有ISR副本没有broker epoch，将其设置为-1是安全的。
   * 
   * @param isr ISR集合中的broker ID列表
   * @return 带有broker epoch信息的BrokerState列表
   */
  private def addBrokerEpochToIsr(isr: List[Int]): List[BrokerState] = {
    isr.map { brokerId =>
      // 创建新的broker状态对象
      val brokerState = new BrokerState().setBrokerId(brokerId)
      if (brokerId == localBrokerId) {
        // 本地broker：使用本地的broker epoch
        brokerState.setBrokerEpoch(localBrokerEpochSupplier())
      } else {
        // 远程broker：从远程副本映射表中获取副本信息
        val replica = remoteReplicasMap.get(brokerId)
        val brokerEpoch = if (replica == null) Option.empty else replica.stateSnapshot.brokerEpoch
        if (brokerEpoch.isEmpty) {
          // broker epoch缺失时，设置为-1
          brokerState.setBrokerEpoch(-1)
        } else {
          // 使用远程副本的broker epoch
          brokerState.setBrokerEpoch(brokerEpoch.get)
        }
      }
      brokerState
    }
  }

  /**
   * 提交ISR状态变更请求
   * 
   * 该方法负责向controller提交AlterPartition请求，以更新ISR集合。方法使用异步处理方式，
   * 通过CompletableFuture处理请求的响应。在处理响应时，需要考虑并发情况和各种错误场景。
   * 
   * @param proposedIsrState 提议的ISR状态变更
   * @return 包含LeaderAndIsr响应的CompletableFuture
   */
  private def submitAlterPartition(proposedIsrState: PendingPartitionChange): CompletableFuture[LeaderAndIsr] = {
    debug(s"Submitting ISR state change $proposedIsrState")
    // 向AlterIsrManager提交请求
    val future = alterIsrManager.submit(
      new TopicIdPartition(topicId.getOrElse(Uuid.ZERO_UUID), topicPartition),
      proposedIsrState.sentLeaderAndIsr
    )
    
    // 异步处理响应
    future.whenComplete { (leaderAndIsr, e) =>
      var hwIncremented = false  // 标记高水位是否增加
      var shouldRetry = false    // 标记是否需要重试

      // 获取写锁以更新分区状态
      inWriteLock(leaderIsrUpdateLock) {
        if (partitionState != proposedIsrState) {
          // 如果分区状态已经通过其他机制更新（如leader选举），
          // 说明这个响应已经过时，我们忽略它
          debug(s"Ignoring failed ISR update to $proposedIsrState since we have already " +
            s"updated state to $partitionState")
        } else if (leaderAndIsr != null) {
          // 处理成功的响应
          hwIncremented = handleAlterPartitionUpdate(proposedIsrState, leaderAndIsr)
        } else {
          // 处理错误响应
          shouldRetry = handleAlterPartitionError(proposedIsrState, Errors.forException(e))
        }
      }

      // 如果高水位增加了，尝试完成延迟的请求
      if (hwIncremented) {
        tryCompleteDelayedRequests()
      }

      // 在LeaderAndIsr锁外发送AlterPartition请求
      // 因为完成逻辑可能会增加高水位并完成延迟操作
      if (shouldRetry) {
        submitAlterPartition(proposedIsrState)
      }
    }
  }

  /**
   * Handle a failed `AlterPartition` request. For errors which are non-retriable, we simply give up.
   * This leaves [[Partition.partitionState]] in a pending state. Since the error was non-retriable,
   * we are okay staying in this state until we see new metadata from LeaderAndIsr (or an update
   * to the KRaft metadata log).
   *
   * @param proposedIsrState The ISR state change that was requested
   * @param error The error returned from [[AlterPartitionManager]]
   * @return true if the `AlterPartition` request should be retried, false otherwise
   */
  /**
   * 处理AlterPartition请求的错误响应
   * 
   * 对于不可重试的错误，我们会放弃重试。这会使Partition.partitionState保持在待处理状态。
   * 由于错误是不可重试的，我们可以等待从LeaderAndIsr（或KRaft元数据日志）收到新的元数据来更新状态。
   * 
   * @param proposedIsrState 提议的ISR状态变更
   * @param error AlterPartitionManager返回的错误
   * @return 如果需要重试AlterPartition请求则返回true，否则返回false
   */
  private def handleAlterPartitionError(
    proposedIsrState: PendingPartitionChange,
    error: Errors
  ): Boolean = {
    // 标记ISR更新失败
    alterPartitionListener.markFailed()
    error match {
      case Errors.OPERATION_NOT_ATTEMPTED | Errors.INELIGIBLE_REPLICA =>
        // 当重置到最后提交的状态时需要特别小心，因为考虑到重试和controller变更，
        // 我们通常可能不知道请求是否已经被应用。
        // 但是，当controller返回INELIGIBLE_REPLICA（或OPERATION_NOT_ATTEMPTED）时，
        // controller明确告诉我们：
        // 1) 当前的分区epoch是正确的
        // 2) 请求未被应用
        // 即使发送响应的controller已过时，基于controller epoch的单调性，
        // 我们可以保证请求不可能被任何过去或未来的controller应用。
        partitionState = proposedIsrState.lastCommittedState
        info(s"Failed to alter partition to $proposedIsrState since the controller rejected the request with $error. " +
          s"Partition state has been reset to the latest committed state $partitionState.")
        false

      case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
        // controller不知道这个主题或分区，等待新的元数据
        debug(s"Failed to alter partition to $proposedIsrState since the controller doesn't know about " +
          "this topic or partition. Partition state may be out of sync, awaiting new the latest metadata.")
        false

      case Errors.UNKNOWN_TOPIC_ID =>
        // controller不知道这个主题，等待新的元数据
        debug(s"Failed to alter partition to $proposedIsrState since the controller doesn't know about " +
          "this topic. Partition state may be out of sync, awaiting new the latest metadata.")
        false

      case Errors.FENCED_LEADER_EPOCH =>
        // leader epoch已过时，等待新的元数据
        debug(s"Failed to alter partition to $proposedIsrState since the leader epoch is old. " +
          "Partition state may be out of sync, awaiting new the latest metadata.")
        false

      case Errors.INVALID_UPDATE_VERSION =>
        // 分区epoch无效，等待新的元数据
        debug(s"Failed to alter partition to $proposedIsrState because the partition epoch is invalid. " +
          "Partition state may be out of sync, awaiting new the latest metadata.")
        false

      case Errors.INVALID_REQUEST =>
        // 请求无效，等待新的元数据
        debug(s"Failed to alter partition to $proposedIsrState because the request is invalid. " +
          "Partition state may be out of sync, awaiting new the latest metadata.")
        false

      case Errors.NEW_LEADER_ELECTED =>
        // 操作成功完成，但在完成进行中的重分配时，该副本被controller从副本集中移除。
        // 该副本不再是leader但它还不知道。它应该保持在当前的待处理状态，直到元数据更新覆盖它。
        // 这种情况只在KRaft模式下出现。
        debug(s"The alter partition request successfully updated the partition state to $proposedIsrState but " +
          "this replica got removed from the replica set while completing a reassignment. " +
          "Waiting on new metadata to clean up this replica.")
        false

      case _ =>
        // 未预期的错误，需要重试
        warn(s"Failed to update ISR to $proposedIsrState due to unexpected $error. Retrying.")
        true
    }
  }

  /**
   * 处理成功的`AlterPartition`响应。
   * 
   * 该方法在收到控制器对AlterPartition请求的成功响应后被调用，主要完成以下工作：
   * 1. 验证leader epoch和partition epoch的有效性
   * 2. 更新分区的ISR集合和分区版本号
   * 3. 通知监听器ISR变更事件
   * 4. 尝试更新高水位标记
   *
   * 并发控制：
   * - 该方法在持有leaderIsrUpdateLock写锁的情况下调用
   * - 确保ISR更新的原子性，防止与其他状态变更操作产生冲突
   *
   * @param proposedIsrState 请求的ISR状态变更信息
   * @param leaderAndIsr 更新后的LeaderAndIsr状态
   * @return 如果高水位标记成功更新则返回true，否则返回false
   */
  private def handleAlterPartitionUpdate(
    proposedIsrState: PendingPartitionChange,
    leaderAndIsr: LeaderAndIsr
  ): Boolean = {
    // 从控制器收到成功响应后，仍需要进行一些验证
    if (leaderAndIsr.leaderEpoch != leaderEpoch) {
      // 如果leader epoch不匹配，说明本地状态已过期，忽略此次更新
      debug(s"Ignoring new ISR $leaderAndIsr since we have a stale leader epoch $leaderEpoch.")
      alterPartitionListener.markFailed()
      false
    } else if (leaderAndIsr.partitionEpoch < partitionEpoch) {
      // 如果收到的partition epoch小于本地版本，说明这是一个过期的更新，忽略它
      debug(s"Ignoring new ISR $leaderAndIsr since we have a newer version $partitionEpoch.")
      alterPartitionListener.markFailed()
      false
    } else {
      // 这里有两种可能的状态：
      // 1) leaderAndIsr.partitionEpoch > partitionEpoch：控制器使用proposedIsrState更新到了新版本
      // 2) leaderAndIsr.partitionEpoch == partitionEpoch：由于提议的状态和实际状态相同，没有执行更新
      // 在这两种情况下，我们都需要从Pending状态转移到Committed状态，以确保新的更新能够被处理

      // 使用新的ISR集合和领导者恢复状态创建已提交的分区状态
      partitionState = CommittedPartitionState(leaderAndIsr.isr.asScala.map(_.toInt).toSet, leaderAndIsr.leaderRecoveryState)
      // 更新分区版本号
      partitionEpoch = leaderAndIsr.partitionEpoch
      // 记录ISR更新信息，如果ISR数量低于最小值则标记出来
      info(s"ISR updated to ${partitionState.isr.mkString(",")} ${if (isUnderMinIsr) "(under-min-isr)" else ""} " +
        s"and version updated to $partitionEpoch")

      // 通知监听器ISR变更事件
      proposedIsrState.notifyListener(alterPartitionListener)

      // 由于ISR可能减少到只剩1个副本，我们可能需要增加高水位标记
      leaderLogIfLocal.exists(log => maybeIncrementLeaderHW(log))
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; Replicas: " + assignmentState.replicas.mkString(","))
    partitionString.append("; ISR: " + partitionState.isr.mkString(","))
    assignmentState match {
      case OngoingReassignmentState(adding, removing, _) =>
        partitionString.append("; AddingReplicas: " + adding.mkString(","))
        partitionString.append("; RemovingReplicas: " + removing.mkString(","))
      case _ =>
    }
    partitionString.append("; LeaderRecoveryState: " + partitionState.leaderRecoveryState)
    partitionString.toString
  }
}
