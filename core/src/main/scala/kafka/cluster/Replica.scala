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

import kafka.log.UnifiedLog
import kafka.server.MetadataCache
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.NotLeaderOrFollowerException
import org.apache.kafka.storage.internals.log.LogOffsetMetadata

import java.util.concurrent.atomic.AtomicReference

/**
 * 副本状态类，用于维护Kafka分区副本的各种状态信息
 * 包括日志偏移量、同步状态、时间戳等关键信息
 */
case class ReplicaState(
  // 日志起始偏移量值，所有副本都会维护这个值
  // 对于本地副本，这是日志的实际起始偏移量
  // 对于远程副本，这个值仅在follower进行数据拉取时更新
  logStartOffset: Long,

  // 日志末端偏移量值，所有副本都会维护这个值
  // 对于本地副本，这是日志的实际末端偏移量
  // 对于远程副本，这个值仅在follower进行数据拉取时更新
  logEndOffsetMetadata: LogOffsetMetadata,

  // leader收到该follower最后一次FetchRequest时的日志末端偏移量值
  // 用于确定follower的lastCaughtUpTimeMs
  // 当收到LeaderAndIsr请求时会被leader重置
  // 当leader追加记录到日志时也可能被重置
  lastFetchLeaderLogEndOffset: Long,

  // leader收到该follower最后一次FetchRequest的时间戳
  // 用于确定follower的lastCaughtUpTimeMs
  lastFetchTimeMs: Long,

  // 最后一次追赶上leader的时间戳，定义为时间t，满足：
  // 该follower最近一次FetchRequest的偏移量 >= leader在时间t的LEO
  // 用于判断该follower的延迟情况和该分区的ISR集合
  lastCaughtUpTimeMs: Long,

  // broker的epoch值，来自Fetch请求
  // 用于防止处理过期的请求
  brokerEpoch: Option[Long]
) {
  /**
   * 获取副本的当前日志末端偏移量
   * @return 日志末端偏移量值
   */
  def logEndOffset: Long = logEndOffsetMetadata.messageOffset

  /**
   * 判断副本是否已经追赶上leader
   * 当满足以下任一条件时，认为副本已经追赶上：
   * 1. 副本的日志末端偏移量等于leader的日志末端偏移量
   * 2. 当前时间减去最后一次追赶上的时间小于等于允许的最大副本延迟时间
   *
   * @param leaderEndOffset leader的日志末端偏移量
   * @param currentTimeMs 当前时间戳
   * @param replicaMaxLagMs 允许的最大副本延迟时间
   * @return 如果副本已追赶上则返回true，否则返回false
   */
  def isCaughtUp(
    leaderEndOffset: Long,
    currentTimeMs: Long,
    replicaMaxLagMs: Long
  ): Boolean = {
    leaderEndOffset == logEndOffset || currentTimeMs - lastCaughtUpTimeMs <= replicaMaxLagMs
  }
}

/**
 * ReplicaState的伴生对象，提供空状态实例
 */
object ReplicaState {
  // 空的副本状态，用于初始化新的副本
  val Empty: ReplicaState = ReplicaState(
    logEndOffsetMetadata = LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
    logStartOffset = UnifiedLog.UnknownOffset,
    lastFetchLeaderLogEndOffset = 0L,
    lastFetchTimeMs = 0L,
    lastCaughtUpTimeMs = 0L,
    brokerEpoch = None : Option[Long],
  )
}

/**
 * Replica类表示Kafka分区的一个副本
 * 负责维护副本的状态信息，包括日志偏移量、同步状态等
 *
 * @param brokerId broker的唯一标识符
 * @param topicPartition 该副本所属的主题分区
 * @param metadataCache 元数据缓存，用于获取broker的epoch信息
 */
class Replica(val brokerId: Int, val topicPartition: TopicPartition, val metadataCache: MetadataCache) extends Logging {
  // 使用原子引用保存副本状态，确保线程安全
  private val replicaState = new AtomicReference[ReplicaState](ReplicaState.Empty)

  /**
   * 获取副本当前状态的快照
   * @return 副本状态的快照
   */
  def stateSnapshot: ReplicaState = replicaState.get

  /**
   * 更新副本的拉取状态
   * 仅当broker epoch为-1或大于等于当前broker epoch时才更新
   * 否则会抛出NOT_LEADER_OR_FOLLOWER异常，这可以防止处理过期请求
   *
   * lastCaughtUpTimeMs的更新规则：
   * 1. 如果follower的拉取偏移量大于等于当前leader的末端偏移量，
   *    将lastCaughtUpTimeMs设置为当前拉取请求的时间
   * 2. 如果follower的拉取偏移量大于等于上次拉取时leader的末端偏移量，
   *    将lastCaughtUpTimeMs设置为上次拉取请求的时间
   * 3. 其他情况保持lastCaughtUpTimeMs不变
   *
   * 这个机制用于维护ISR（In-Sync Replicas）语义：
   * 只有当副本落后leader的LEO不超过replicaLagTimeMaxMs时，该副本才能在ISR中
   * 这允许即使follower的拉取请求偏移量始终小于leader的LEO（高频小数据量生产场景），
   * 该follower仍然可以被添加到ISR中
   *
   * @param followerFetchOffsetMetadata follower的拉取偏移量元数据
   * @param followerStartOffset follower的起始偏移量
   * @param followerFetchTimeMs follower发起拉取请求的时间戳
   * @param leaderEndOffset leader的末端偏移量
   * @param brokerEpoch broker的epoch值
   */
  def updateFetchStateOrThrow(
    followerFetchOffsetMetadata: LogOffsetMetadata,
    followerStartOffset: Long,
    followerFetchTimeMs: Long,
    leaderEndOffset: Long,
    brokerEpoch: Long
  ): Unit = {
    replicaState.updateAndGet { currentReplicaState =>
      // 获取缓存的broker epoch
      val cachedBrokerEpoch = metadataCache.getAliveBrokerEpoch(brokerId)
      // 如果提供的broker epoch过期，拒绝更新并抛出异常
      if (brokerEpoch != -1 && cachedBrokerEpoch.exists(_ > brokerEpoch)) {
        throw new NotLeaderOrFollowerException(s"Received stale fetch state update. broker epoch=$brokerEpoch " +
          s"vs expected=${currentReplicaState.brokerEpoch.get}")
      }

      // 计算最后一次追赶上leader的时间
      val lastCaughtUpTime = if (followerFetchOffsetMetadata.messageOffset >= leaderEndOffset) {
        // 如果follower已经追上了当前leader的末端偏移量
        math.max(currentReplicaState.lastCaughtUpTimeMs, followerFetchTimeMs)
      } else if (followerFetchOffsetMetadata.messageOffset >= currentReplicaState.lastFetchLeaderLogEndOffset) {
        // 如果follower追上了上次拉取时leader的末端偏移量
        math.max(currentReplicaState.lastCaughtUpTimeMs, currentReplicaState.lastFetchTimeMs)
      } else {
        // follower仍然落后，保持原有的lastCaughtUpTimeMs
        currentReplicaState.lastCaughtUpTimeMs
      }

      // 创建新的副本状态
      ReplicaState(
        logStartOffset = followerStartOffset,
        logEndOffsetMetadata = followerFetchOffsetMetadata,
        lastFetchLeaderLogEndOffset = math.max(leaderEndOffset, currentReplicaState.lastFetchLeaderLogEndOffset),
        lastFetchTimeMs = followerFetchTimeMs,
        lastCaughtUpTimeMs = lastCaughtUpTime,
        brokerEpoch = Option(brokerEpoch)
      )
    }
  }

  /**
   * 当leader被选举或重新选举时，重置follower的状态
   * 这个方法会根据follower是否在ISR中以及是否是新leader来调整状态
   *
   * @param currentTimeMs 当前时间戳
   * @param leaderEndOffset leader的末端偏移量
   * @param isNewLeader 是否是新选举的leader
   * @param isFollowerInSync follower是否在ISR（In-Sync Replicas）中
   */
  def resetReplicaState(
    currentTimeMs: Long,
    leaderEndOffset: Long,
    isNewLeader: Boolean,
    isFollowerInSync: Boolean
  ): Unit = {
    replicaState.updateAndGet { currentReplicaState =>
      // 设置follower的最后追赶时间
      // 如果follower在ISR中，设置为当前时间
      // 如果不在ISR中，设置为0，这样可以确保高水位不会因为
      // 已经不在ISR中的follower而被不必要地延迟
      val lastCaughtUpTimeMs = if (isFollowerInSync) currentTimeMs else 0L

      if (isNewLeader) {
        // 如果是新leader，重置所有状态为初始值
        ReplicaState(
          // 将日志起始偏移量设置为未知
          logStartOffset = UnifiedLog.UnknownOffset,
          // 将日志末端偏移量元数据设置为未知
          logEndOffsetMetadata = LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
          // 将上次拉取时leader的末端偏移量设置为未知
          lastFetchLeaderLogEndOffset = UnifiedLog.UnknownOffset,
          // 将上次拉取时间重置为0
          lastFetchTimeMs = 0L,
          // 设置最后追赶时间
          lastCaughtUpTimeMs = lastCaughtUpTimeMs,
          // 清空broker epoch
          brokerEpoch = Option.empty
        )
      } else {
        // 如果是leader重新选举，保留部分原有状态
        ReplicaState(
          // 保持原有的日志起始偏移量
          logStartOffset = currentReplicaState.logStartOffset,
          // 保持原有的日志末端偏移量元数据
          logEndOffsetMetadata = currentReplicaState.logEndOffsetMetadata,
          // 更新为新leader的末端偏移量
          lastFetchLeaderLogEndOffset = leaderEndOffset,
          // 设置follower的最后拉取时间
          // 如果follower在ISR中，设置为当前时间
          // 如果不在ISR中，设置为0，这样可以确保follower在进行新的拉取之前
          // 不会被重新加入到ISR中
          lastFetchTimeMs = if (isFollowerInSync) currentTimeMs else 0L,
          // 设置最后追赶时间
          lastCaughtUpTimeMs = lastCaughtUpTimeMs,
          // 保持原有的broker epoch
          brokerEpoch = currentReplicaState.brokerEpoch
        )
      }
    }
    // 记录跟踪日志
    trace(s"Reset state of replica to $this")
  }

  /**
   * 重写toString方法，提供副本的详细状态信息
   * 这个方法在以下场景非常有用：
   * 1. 调试和监控：提供副本的完整状态快照，包括偏移量、同步状态等关键信息
   * 2. 日志记录：在系统日志中记录副本状态变化，便于问题排查
   * 3. 监控和告警：监控系统可以解析这些信息来检测副本异常
   *
   * @return 包含副本所有关键状态信息的字符串表示
   */
  override def toString: String = {
    // 获取当前副本状态的快照
    val replicaState = this.replicaState.get
    // 使用StringBuilder构建状态字符串，提高性能
    val replicaString = new StringBuilder
    // 依次添加副本的标识信息：副本ID
    replicaString.append(s"Replica(replicaId=$brokerId")
    // 添加主题和分区信息
    replicaString.append(s", topic=${topicPartition.topic}")
    replicaString.append(s", partition=${topicPartition.partition}")
    // 添加同步状态相关信息
    replicaString.append(s", lastCaughtUpTimeMs=${replicaState.lastCaughtUpTimeMs}")
    // 添加日志偏移量信息
    replicaString.append(s", logStartOffset=${replicaState.logStartOffset}")
    replicaString.append(s", logEndOffset=${replicaState.logEndOffsetMetadata.messageOffset}")
    replicaString.append(s", logEndOffsetMetadata=${replicaState.logEndOffsetMetadata}")
    replicaString.append(s", lastFetchLeaderLogEndOffset=${replicaState.lastFetchLeaderLogEndOffset}")
    // 添加broker epoch和最后拉取时间
    replicaString.append(s", brokerEpoch=${replicaState.brokerEpoch.getOrElse(-2L)}")
    replicaString.append(s", lastFetchTimeMs=${replicaState.lastFetchTimeMs}")
    replicaString.append(")")
    replicaString.toString
  }

  /**
   * 重写equals方法，用于副本对象的相等性比较
   * 两个副本相等的条件：
   * 1. broker ID相同：确保是同一个broker上的副本
   * 2. 主题分区相同：确保是同一个分区的副本
   * 
   * 这个方法在以下场景很重要：
   * 1. 副本集合操作：添加/删除/查找特定副本
   * 2. 副本状态比较：确定是否是同一个副本的不同状态
   * 3. 副本迁移：确保目标位置没有相同的副本
   *
   * @param that 要比较的对象
   * @return 如果两个副本相等返回true，否则返回false
   */
  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  /**
   * 重写hashCode方法，生成副本对象的哈希码
   * 使用质数31和17作为乘数，结合topicPartition和brokerId计算哈希值
   * 
   * 这个方法在以下场景很重要：
   * 1. HashMap/HashSet：当副本对象作为键使用时
   * 2. 缓存：用于副本对象的快速查找
   * 3. 集合去重：识别重复的副本
   *
   * @return 副本对象的哈希码
   */
  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId
}
