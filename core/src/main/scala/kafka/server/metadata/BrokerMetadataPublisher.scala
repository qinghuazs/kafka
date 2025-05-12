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

package kafka.server.metadata

import java.util.OptionalInt
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.image.{MetadataDelta, MetadataImage, TopicDelta}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.fault.FaultHandler

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._


/**
 * 代理元数据发布器
 * 负责处理和发布Kafka集群中的元数据更新
 * 
 * 应用场景：
 * 1. 元数据管理：处理集群元数据的变更和同步
 * 2. 主题管理：处理主题创建、删除和配置变更
 * 3. 协调器管理：维护组、事务和共享协调器的状态
 */
object BrokerMetadataPublisher extends Logging {
  /**
   * 检查给定主题是否发生变更
   * 注意：如果主题X被删除后重新创建，此方法只返回重建信息
   * 删除信息会在deletedTopicIds中体现，需要单独处理
   *
   * @param topicName   主题名称
   * @param newImage    新的元数据镜像
   * @param delta       要搜索的元数据变更
   * @return           主题变更信息，如果没有变更则返回None
   */
  def getTopicDelta(topicName: String,
                    newImage: MetadataImage,
                    delta: MetadataDelta): Option[TopicDelta] = {
    // 首先获取主题信息，然后检查是否有变更
    Option(newImage.topics().getTopic(topicName)).flatMap {
      topicImage => Option(delta.topicsDelta()).flatMap {
        topicDelta => Option(topicDelta.changedTopic(topicImage.id()))
      }
    }
  }
}

/**
 * 代理元数据发布器类
 * 负责管理和发布Kafka代理的元数据更新
 * 
 * 设计考虑：
 * 1. 线程安全：确保多线程环境下的元数据更新安全
 * 2. 错误处理：提供完善的错误处理机制
 * 3. 状态管理：维护发布状态和生命周期
 */
class BrokerMetadataPublisher(
  // Kafka配置
  config: KafkaConfig,
  // 元数据缓存
  metadataCache: KRaftMetadataCache,
  // 日志管理器
  logManager: LogManager,
  // 副本管理器
  replicaManager: ReplicaManager,
  // 消费者组协调器
  groupCoordinator: GroupCoordinator,
  // 事务协调器
  txnCoordinator: TransactionCoordinator,
  // 共享协调器（可选）
  shareCoordinator: Option[ShareCoordinator],
  // 动态配置发布器
  var dynamicConfigPublisher: DynamicConfigPublisher,
  // 动态客户端配额发布器
  dynamicClientQuotaPublisher: DynamicClientQuotaPublisher,
  // SCRAM认证发布器
  scramPublisher: ScramPublisher,
  // 委托令牌发布器
  delegationTokenPublisher: DelegationTokenPublisher,
  // ACL发布器
  aclPublisher: AclPublisher,
  // 致命错误处理器
  fatalFaultHandler: FaultHandler,
  // 元数据发布错误处理器
  metadataPublishingFaultHandler: FaultHandler,
) extends MetadataPublisher with Logging {
  // 设置日志标识符
  logIdent = s"[BrokerMetadataPublisher id=${config.nodeId}] "

  import BrokerMetadataPublisher._

  /**
   * 代理ID
   */
  val brokerId: Int = config.nodeId

  /**
   * 标记是否是首次发布元数据
   */
  var _firstPublish = true

  /**
   * 首次发布完成时完成的Future
   */
  val firstPublishFuture = new CompletableFuture[Void]

  /**
   * 获取发布器名称
   */
  override def name(): String = "BrokerMetadataPublisher"

  /**
   * 处理元数据更新
   * 当收到新的元数据更新时被调用
   *
   * @param delta 元数据变更信息
   * @param newImage 新的元数据镜像
   * @param manifest 加载器清单
   */
  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    // 获取最高偏移量和纪元
    val highestOffsetAndEpoch = newImage.highestOffsetAndEpoch()

    // 构建变更名称，用于日志记录
    val deltaName = if (_firstPublish) {
      s"initial MetadataDelta up to ${highestOffsetAndEpoch.offset}"
    } else {
      s"MetadataDelta up to ${highestOffsetAndEpoch.offset}"
    }
    try {
      // 记录跟踪日志
      if (isTraceEnabled) {
        trace(s"Publishing delta $delta with highest offset $highestOffsetAndEpoch")
      }

      // 将新的元数据镜像发布到元数据缓存
      metadataCache.setImage(newImage)

      // 构建元数据版本日志消息
      val metadataVersionLogMsg = s"metadata.version ${newImage.features().metadataVersion()}"

      // 处理首次发布场景
      if (_firstPublish) {
        info(s"Publishing initial metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")
        // 初始化管理器
        initializeManagers(newImage)
      } else if (isDebugEnabled) {
        debug(s"Publishing metadata at offset $highestOffsetAndEpoch with $metadataVersionLogMsg.")
      }

      // 应用主题变更
      Option(delta.topicsDelta()).foreach { topicsDelta =>
        try {
          // 通知副本管理器关于主题的变更
          replicaManager.applyDelta(topicsDelta, newImage)
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error applying topics " +
            s"delta in $deltaName", t)
        }
        try {
          // 更新消费者组协调器的本地变更
          updateCoordinator(newImage,
            delta,
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupCoordinator.onElection,
            (partitionIndex, leaderEpochOpt) => groupCoordinator.onResignation(partitionIndex, toOptionalInt(leaderEpochOpt))
          )
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
            s"coordinator with local changes in $deltaName", t)
        }
        try {
          // 更新事务协调器的本地变更
          updateCoordinator(newImage,
            delta,
            Topic.TRANSACTION_STATE_TOPIC_NAME,
            txnCoordinator.onElection,
            txnCoordinator.onResignation)
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating txn " +
            s"coordinator with local changes in $deltaName", t)
        }
        // 如果配置了共享协调器，更新其本地变更
        if (shareCoordinator.isDefined) {
          try {
            updateCoordinator(newImage,
              delta,
              Topic.SHARE_GROUP_STATE_TOPIC_NAME,
              shareCoordinator.get.onElection,
              (partitionIndex, leaderEpochOpt) => shareCoordinator.get.onResignation(partitionIndex, toOptionalInt(leaderEpochOpt))
            )
          } catch {
            case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating share " +
              s"coordinator with local changes in $deltaName", t)
          }
        }
        try {
          // 通知消费者组协调器关于已删除的主题
          val deletedTopicPartitions = new mutable.ArrayBuffer[TopicPartition]()
          // 收集已删除的主题分区
          topicsDelta.deletedTopicIds().forEach { id =>
            val topicImage = topicsDelta.image().getTopic(id)
            topicImage.partitions().keySet().forEach {
              id => deletedTopicPartitions += new TopicPartition(topicImage.name(), id)
            }
          }
          // 如果有删除的分区，通知协调器
          if (deletedTopicPartitions.nonEmpty) {
            groupCoordinator.onPartitionsDeleted(deletedTopicPartitions.asJava, RequestLocal.noCaching.bufferSupplier)
          }
        } catch {
          case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
            s"coordinator with deleted partitions in $deltaName", t)
        }
      }

      // 应用配置变更
      dynamicConfigPublisher.onMetadataUpdate(delta, newImage)

      // 应用客户端配额变更
      dynamicClientQuotaPublisher.onMetadataUpdate(delta, newImage)

      // 应用SCRAM（安全认证）变更
      scramPublisher.onMetadataUpdate(delta, newImage)

      // 应用委托令牌变更
      delegationTokenPublisher.onMetadataUpdate(delta, newImage)

      // 应用ACL（访问控制列表）变更
      aclPublisher.onMetadataUpdate(delta, newImage, manifest)

      try {
        // 将新的元数据镜像传播到消费者组协调器
        groupCoordinator.onNewMetadataImage(newImage, delta)
      } catch {
        case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating group " +
          s"coordinator with local changes in $deltaName", t)
      }

      try {
        // 将新的元数据镜像传播到共享协调器
        shareCoordinator.foreach(coordinator => coordinator.onNewMetadataImage(newImage, delta))
      } catch {
        case t: Throwable => metadataPublishingFaultHandler.handleFault("Error updating share " +
          s"coordinator with local changes in $deltaName", t)
      }

      // 如果是首次发布，完成副本管理器的初始化
      if (_firstPublish) {
        finishInitializingReplicaManager()
      }
    } catch {
      // 处理元数据发布过程中的未捕获异常
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Uncaught exception while " +
        s"publishing broker metadata from $deltaName", t)
    } finally {
      // 更新首次发布标志并完成首次发布Future
      _firstPublish = false
      firstPublishFuture.complete(null)
    }
  }

  /**
   * 将Scala的Option[Int]转换为Java的OptionalInt
   * 用于跨语言边界的类型转换
   *
   * @param option Scala的Option[Int]类型
   * @return Java的OptionalInt类型
   */
  private def toOptionalInt(option: Option[Int]): OptionalInt = {
    option match {
      case Some(leaderEpoch) => OptionalInt.of(leaderEpoch)
      case None => OptionalInt.empty
    }
  }

  /**
   * 更新协调器的本地副本变更：选举和退位
   * 
   * 应用场景：
   * 1. 主题删除：处理主题被删除时的协调器状态更新
   * 2. 副本重分配：处理副本被重新分配时的状态变更
   * 3. 领导者变更：处理分区领导者选举和退位
   *
   * @param image 最新的元数据镜像
   * @param delta 前一个镜像和最新镜像之间的元数据变更
   * @param topicName 与协调器关联的主题名称
   * @param election 选举时调用的函数，第一个参数是分区ID，第二个参数是领导者纪元
   * @param resignation 退位时调用的函数，第一个参数是分区ID，第二个参数是领导者纪元
   */
  def updateCoordinator(
    image: MetadataImage,
    delta: MetadataDelta,
    topicName: String,
    election: (Int, Int) => Unit,
    resignation: (Int, Option[Int]) => Unit
  ): Unit = {
    // 处理主题被删除的情况
    Option(delta.topicsDelta()).foreach { topicsDelta =>
      if (topicsDelta.topicWasDeleted(topicName)) {
        // 遍历所有分区，如果当前broker是领导者，则调用退位函数
        topicsDelta.image.getTopic(topicName).partitions.entrySet.forEach { entry =>
          if (entry.getValue.leader == brokerId) {
            resignation(entry.getKey, Some(entry.getValue.leaderEpoch))
          }
        }
      }
    }

    // 处理副本被重新分配、成为领导者或追随者的情况
    getTopicDelta(topicName, image, delta).foreach { topicDelta =>
      // 获取当前broker的本地变更
      val changes = topicDelta.localChanges(brokerId)

      // 处理删除的分区
      changes.deletes.forEach { topicPartition =>
        resignation(topicPartition.partition, None)
      }
      // 处理新选举的领导者
      changes.electedLeaders.forEach { (topicPartition, partitionInfo) =>
        election(topicPartition.partition, partitionInfo.partition.leaderEpoch)
      }
      // 处理新的追随者
      changes.followers.forEach { (topicPartition, partitionInfo) =>
        resignation(topicPartition.partition, Some(partitionInfo.partition.leaderEpoch))
      }
    }
  }

  /**
   * 初始化各种管理器
   * 
   * 应用场景：
   * 1. 系统启动：初始化必要的管理器组件
   * 2. 恢复处理：处理非正常关闭后的恢复
   * 3. 配置更新：使管理器可以进行动态配置
   *
   * @param newImage 最新的元数据镜像
   */
  private def initializeManagers(newImage: MetadataImage): Unit = {
    try {
      // 启动日志管理器，如果需要，将执行非正常关闭后的恢复
      logManager.startup(
        metadataCache.getAllTopics(),
        isStray = log => LogManager.isStrayKraftReplica(brokerId, newImage.topics(), log)
      )

      // 重命名所有与控制器分配的目录相同的未来副本
      // 这种情况只会在磁盘故障和代理关闭后发生，
      // 即在控制器更新目录分配后但在未来副本被提升之前
      logManager.recoverAbandonedFutureLogs(brokerId, newImage.topics())

      // 使LogCleaner可以进行重新配置
      // 在此之前无法执行此操作，因为LogManager#startup会创建LogCleaner对象
      Option(logManager.cleaner).foreach(config.dynamicConfig.addBrokerReconfigurable)
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting LogManager", t)
    }
    try {
      // 启动副本管理器
      replicaManager.startup()
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting ReplicaManager", t)
    }
    try {
      // 启动消费者组协调器
      groupCoordinator.startup(() => metadataCache.numPartitions(Topic.GROUP_METADATA_TOPIC_NAME)
        .getOrElse(config.groupCoordinatorConfig.offsetsTopicPartitions))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting GroupCoordinator", t)
    }
    try {
      // 启动事务协调器
      txnCoordinator.startup(() => metadataCache.numPartitions(
        Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionLogConfig.transactionTopicPartitions))
    } catch {
      case t: Throwable => fatalFaultHandler.handleFault("Error starting TransactionCoordinator", t)
    }
    // 如果启用了共享组功能并且存在共享协调器，则启动它
    if (config.shareGroupConfig.isShareGroupEnabled && shareCoordinator.isDefined) {
      try {
        shareCoordinator.get.startup(() => metadataCache.numPartitions(
          Topic.SHARE_GROUP_STATE_TOPIC_NAME).getOrElse(config.shareCoordinatorConfig.shareCoordinatorStateTopicNumPartitions()))
      } catch {
        case t: Throwable => fatalFaultHandler.handleFault("Error starting Share coordinator", t)
      }
    }
  }

  /**
   * 完成副本管理器的初始化
   * 
   * 应用场景：
   * 1. 启动完成：确保高水位标记检查点线程正在运行
   * 2. 数据一致性：维护副本之间的数据同步状态
   */
  private def finishInitializingReplicaManager(): Unit = {
    try {
      // 确保副本管理器的高水位标记检查点线程正在运行
      replicaManager.startHighWatermarkCheckPointThread()
    } catch {
      case t: Throwable => metadataPublishingFaultHandler.handleFault("Error starting high " +
        "watermark checkpoint thread during startup", t)
    }
  }

  /**
   * 关闭代理元数据发布器
   * 
   * 应用场景：
   * 1. 系统关闭：优雅地关闭发布器
   * 2. 错误处理：处理关闭过程中的异常
   */
  override def close(): Unit = {
    // 使用超时异常完成首次发布Future
    firstPublishFuture.completeExceptionally(new TimeoutException())
  }
}
