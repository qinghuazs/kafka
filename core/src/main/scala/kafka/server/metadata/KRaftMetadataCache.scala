/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.server.MetadataCache
import kafka.utils.Logging
import org.apache.kafka.admin.BrokerMetadata
import org.apache.kafka.common._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.{Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.metadata.{BrokerRegistration, LeaderAndIsr, PartitionRegistration, Replicas}
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion}

import java.util
import java.util.concurrent.ThreadLocalRandom
import java.util.function.Supplier
import java.util.{Collections, Properties}
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional
import scala.util.control.Breaks._


/**
 * KRaft元数据缓存
 * 负责在KRaft模式下管理和缓存Kafka集群的元数据信息
 * 
 * 应用场景：
 * 1. 元数据管理：维护主题、分区、副本等元数据信息
 * 2. 状态同步：确保集群中的元数据一致性
 * 3. 性能优化：通过缓存提高元数据访问性能
 */
class KRaftMetadataCache(
  // broker的唯一标识符
  val brokerId: Int,
  // KRaft版本提供者，用于获取当前KRaft版本信息
  val kraftVersionSupplier: Supplier[KRaftVersion]
) extends MetadataCache with Logging {
  // 设置日志标识符
  this.logIdent = s"[MetadataCache brokerId=$brokerId] "

  // 缓存状态
  // MetadataImage实例是不可变的，更新时会替换整个实例
  // 读取操作需要一次性获取该变量的值，并在整个操作期间保持该副本
  // 多次读取该值可能会得到不同的镜像值
  @volatile private var _currentImage: MetadataImage = MetadataImage.EMPTY

  /**
   * 过滤活跃副本
   * 这是元数据请求性能的主要热点，需要谨慎添加额外逻辑
   * filterUnavailableEndpoints用于支持v0版本的元数据响应
   * 
   * @param image 元数据镜像
   * @param brokers broker数组
   * @param listenerName 监听器名称
   * @param filterUnavailableEndpoints 是否过滤不可用端点
   * @return 活跃副本列表
   */
  private def maybeFilterAliveReplicas(image: MetadataImage,
                                     brokers: Array[Int],
                                     listenerName: ListenerName,
                                     filterUnavailableEndpoints: Boolean): java.util.List[Integer] = {
    // 如果不需要过滤，直接返回所有副本
    if (!filterUnavailableEndpoints) {
      Replicas.toList(brokers)
    } else {
      // 创建结果列表
      val res = new util.ArrayList[Integer](brokers.length)
      // 遍历所有broker
      for (brokerId <- brokers) {
        // 获取broker信息并检查其状态
        Option(image.cluster().broker(brokerId)).foreach { b =>
          // 只添加未被隔离且具有指定监听器的broker
          if (!b.fenced() && b.listeners().containsKey(listenerName.value())) {
            res.add(brokerId)
          }
        }
      }
      res
    }
  }

  /**
   * 获取当前元数据镜像
   * @return 当前元数据镜像
   */
  def currentImage(): MetadataImage = _currentImage

  /**
   * 获取分区元数据
   * errorUnavailableEndpoints用于支持v0版本的元数据响应
   * 当errorUnavailableListeners为true时，如果broker上缺少监听器则返回LISTENER_NOT_FOUND
   * 否则，对于不可用的broker和缺少的监听器返回LEADER_NOT_AVAILABLE（元数据响应v5及以下版本）
   */
  private def getPartitionMetadata(image: MetadataImage, topicName: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                 errorUnavailableListeners: Boolean): Option[Iterator[MetadataResponsePartition]] = {
    // 尝试获取主题信息
    Option(image.topics().getTopic(topicName)) match {
      case None => None
      case Some(topic) => Some(topic.partitions().entrySet().asScala.map { entry =>
        // 获取分区ID和分区信息
        val partitionId = entry.getKey
        val partition = entry.getValue
        
        // 获取经过过滤的副本列表和ISR列表
        val filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas,
          listenerName, errorUnavailableEndpoints)
        val filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName,
          errorUnavailableEndpoints)
        
        // 获取离线副本和leader节点信息
        val offlineReplicas = getOfflineReplicas(image, partition, listenerName)
        val maybeLeader = getAliveEndpoint(image, partition.leader, listenerName)
        
        // 根据leader状态构建响应
        maybeLeader match {
          case None =>
            // 确定错误类型
            val error = if (!image.cluster().brokers.containsKey(partition.leader)) {
              debug(s"Error while fetching metadata for $topicName-$partitionId: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for $topicName-$partitionId: listener $listenerName " +
                s"not found on leader ${partition.leader}")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }
            
            // 构建无leader的分区元数据响应
            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
          case Some(leader) =>
            // 检查副本和ISR状态
            val error = if (filteredReplicas.size < partition.replicas.length) {
              debug(s"Error while fetching metadata for $topicName-$partitionId: replica information not available for " +
                s"following brokers ${partition.replicas.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < partition.isr.length) {
              debug(s"Error while fetching metadata for $topicName-$partitionId: in sync replica information not available for " +
                s"following brokers ${partition.isr.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            // 构建有leader的分区元数据响应
            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partitionId)
              .setLeaderId(leader.id())
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(offlineReplicas)
        }
      }.iterator)
    }
  }

  /**
   * 获取主题分区元数据，用于描述主题响应
   * 返回给定主题、监听器和索引范围的分区元数据，以及下一个未包含在结果中的分区索引
   * 
   * @param image 元数据镜像
   * @param topicName 主题名称
   * @param listenerName 监听器名称
   * @param startIndex 要包含在结果中的最小分区索引
   * @param maxCount 最大返回分区数量
   * @return (分区元数据列表, 下一个分区索引)，-1表示没有下一个分区
   */
  private def getPartitionMetadataForDescribeTopicResponse(
    image: MetadataImage,
    topicName: String,
    listenerName: ListenerName,
    startIndex: Int,
    maxCount: Int
  ): (Option[List[DescribeTopicPartitionsResponsePartition]], Int) = {
    // 尝试获取主题信息
    Option(image.topics().getTopic(topicName)) match {
      case None => (None, -1)
      case Some(topic) => {
        // 初始化结果列表
        val result = new ListBuffer[DescribeTopicPartitionsResponsePartition]()
        val partitions = topic.partitions().keySet()
        
        // 计算上界索引和下一个分区索引
        val upperIndex = topic.partitions().size().min(startIndex + maxCount)
        val nextIndex = if (upperIndex < partitions.size()) upperIndex else -1
        
        // 遍历指定范围内的分区
        for (partitionId <- startIndex until upperIndex) {
          topic.partitions().get(partitionId) match {
            case partition : PartitionRegistration => {
              // 获取经过过滤的副本列表和ISR列表
              val filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas,
                listenerName, filterUnavailableEndpoints = false)
              val filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName, filterUnavailableEndpoints = false)
              val offlineReplicas = getOfflineReplicas(image, partition, listenerName)
              val maybeLeader = getAliveEndpoint(image, partition.leader, listenerName)
              
              // 根据leader状态构建响应
              maybeLeader match {
                case None =>
                  // 构建无leader的分区描述响应
                  result.append(new DescribeTopicPartitionsResponsePartition()
                    .setPartitionIndex(partitionId)
                    .setLeaderId(MetadataResponse.NO_LEADER_ID)
                    .setLeaderEpoch(partition.leaderEpoch)
                    .setReplicaNodes(filteredReplicas)
                    .setIsrNodes(filteredIsr)
                    .setOfflineReplicas(offlineReplicas)
                    .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                    .setLastKnownElr(Replicas.toList(partition.lastKnownElr)))
                case Some(leader) =>
                  // 构建有leader的分区描述响应
                  result.append(new DescribeTopicPartitionsResponsePartition()
                    .setPartitionIndex(partitionId)
                    .setLeaderId(leader.id())
                    .setLeaderEpoch(partition.leaderEpoch)
                    .setReplicaNodes(filteredReplicas)
                    .setIsrNodes(filteredIsr)
                    .setOfflineReplicas(offlineReplicas)
                    .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                    .setLastKnownElr(Replicas.toList(partition.lastKnownElr)))
              }
            }
            case _ => warn(s"The partition $partitionId does not exist for $topicName")
          }
        }
        (Some(result.toList), nextIndex)
      }
    }
  }

  /**
   * 获取离线副本列表
   * 检查分区中的所有副本，确定哪些副本当前处于离线状态
   * 
   * @param image 元数据镜像
   * @param partition 分区注册信息
   * @param listenerName 监听器名称
   * @return 离线副本的ID列表
   */
  private def getOfflineReplicas(image: MetadataImage,
                                 partition: PartitionRegistration,
                                 listenerName: ListenerName): util.List[Integer] = {
    // 创建存储离线副本ID的列表
    val offlineReplicas = new util.ArrayList[Integer](0)
    // 遍历分区的所有副本
    for (brokerId <- partition.replicas) {
      // 检查broker是否存在于集群中
      Option(image.cluster().broker(brokerId)) match {
        case None => offlineReplicas.add(brokerId)  // broker不存在，添加到离线列表
        case Some(broker) => if (isReplicaOffline(partition, listenerName, broker)) {
          offlineReplicas.add(brokerId)  // broker存在但离线，添加到离线列表
        }
      }
    }
    offlineReplicas
  }

  /**
   * 检查副本是否离线
   * 副本在以下情况被认为是离线的：
   * 1. broker被隔离(fenced)
   * 2. broker没有指定的监听器
   * 3. 副本所在目录离线
   */
  private def isReplicaOffline(partition: PartitionRegistration, listenerName: ListenerName, broker: BrokerRegistration) =
    broker.fenced() || !broker.listeners().containsKey(listenerName.value()) || isReplicaInOfflineDir(broker, partition)

  /**
   * 检查副本是否在离线目录中
   * 通过检查broker是否有该分区的在线目录来判断
   */
  private def isReplicaInOfflineDir(broker: BrokerRegistration, partition: PartitionRegistration): Boolean =
    !broker.hasOnlineDir(partition.directory(broker.id()))

  /**
   * 获取活跃broker的端点信息
   * 如果broker存活且具有指定的监听器，返回对应的节点信息
   * 注意：监听器可以动态添加，因此缺少监听器可能是暂时性错误
   * 
   * @param image 元数据镜像
   * @param id broker ID
   * @param listenerName 监听器名称
   * @return 如果broker不存活或没有指定监听器则返回None，否则返回节点信息
   */
  private def getAliveEndpoint(image: MetadataImage, id: Int, listenerName: ListenerName): Option[Node] = {
    Option(image.cluster().broker(id)).flatMap(_.node(listenerName.value()).toScala)
  }

  /**
   * 获取主题元数据
   * 用于支持元数据响应，包括v0版本的向后兼容
   * 
   * @param topics 主题集合
   * @param listenerName 监听器名称
   * @param errorUnavailableEndpoints 是否对不可用端点返回错误（用于v0版本兼容）
   * @param errorUnavailableListeners 是否对不可用监听器返回错误
   * @return 主题元数据响应序列
   */
  override def getTopicMetadata(topics: Set[String],
                                listenerName: ListenerName,
                                errorUnavailableEndpoints: Boolean = false,
                                errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    // 获取当前元数据镜像
    val image = _currentImage
    // 转换主题集合为序列并处理每个主题
    topics.toSeq.flatMap { topic =>
      // 获取分区元数据并构建响应
      getPartitionMetadata(image, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)  // 设置错误码为无错误
          .setName(topic)                  // 设置主题名称
          .setTopicId(Option(image.topics().getTopic(topic).id()).getOrElse(Uuid.ZERO_UUID))  // 设置主题ID
          .setIsInternal(Topic.isInternal(topic))  // 设置是否为内部主题
          .setPartitions(partitionMetadata.toBuffer.asJava)  // 设置分区元数据
      }
    }
  }

  /**
   * 生成主题描述响应
   * 支持分页查询主题分区信息
   * 
   * @param topics 主题迭代器
   * @param listenerName 监听器名称
   * @param topicPartitionStartIndex 获取主题起始分区索引的函数
   * @param maximumNumberOfPartitions 最大返回分区数
   * @param ignoreTopicsWithExceptions 是否忽略异常主题
   * @return 主题描述响应数据
   */
  override def describeTopicResponse(
    topics: Iterator[String],
    listenerName: ListenerName,
    topicPartitionStartIndex: String => Int,
    maximumNumberOfPartitions: Int,
    ignoreTopicsWithExceptions: Boolean
  ): DescribeTopicPartitionsResponseData = {
    val image = _currentImage  // 获取当前元数据镜像
    var remaining = maximumNumberOfPartitions  // 剩余可返回的分区数
    val result = new DescribeTopicPartitionsResponseData()  // 创建响应对象
    
    breakable {
      // 遍历所有主题
      topics.foreach { topicName =>
        if (remaining > 0) {  // 还有剩余配额时继续处理
          // 获取主题分区元数据
          val (partitionResponse, nextPartition) =
            getPartitionMetadataForDescribeTopicResponse(
              image, topicName, listenerName, topicPartitionStartIndex(topicName), remaining
            )
          
          // 处理分区响应
          partitionResponse.map(partitions => {
            // 构建主题响应
            val response = new DescribeTopicPartitionsResponseTopic()
              .setErrorCode(Errors.NONE.code)
              .setName(topicName)
              .setTopicId(Option(image.topics().getTopic(topicName).id()).getOrElse(Uuid.ZERO_UUID))
              .setIsInternal(Topic.isInternal(topicName))
              .setPartitions(partitions.asJava)
            result.topics().add(response)

            // 如果有下一个分区，设置游标并中断
            if (nextPartition != -1) {
              result.setNextCursor(new Cursor()
                .setTopicName(topicName)
                .setPartitionIndex(nextPartition)
              )
              break()
            }
            remaining -= partitions.size  // 更新剩余配额
          })

          // 处理主题异常情况
          if (!ignoreTopicsWithExceptions && partitionResponse.isEmpty) {
            val error = try {
              Topic.validate(topicName)  // 验证主题名称
              Errors.UNKNOWN_TOPIC_OR_PARTITION
            } catch {
              case _: InvalidTopicException =>
                Errors.INVALID_TOPIC_EXCEPTION
            }
            // 添加错误响应
            result.topics().add(new DescribeTopicPartitionsResponseTopic()
              .setErrorCode(error.code())
              .setName(topicName)
              .setTopicId(getTopicId(topicName))
              .setIsInternal(Topic.isInternal(topicName)))
          }
        } else if (remaining == 0) {
          // 配额用尽，设置游标指向当前主题的开始
          result.setNextCursor(new Cursor()
            .setTopicName(topicName)
            .setPartitionIndex(0))
          break()
        }
      }
    }
    result
  }

  /**
   * 获取所有主题名称
   * @return 主题名称集合
   */
  override def getAllTopics(): Set[String] = _currentImage.topics().topicsByName().keySet().asScala

  /**
   * 获取指定主题的所有分区
   * @param topicName 主题名称
   * @return 主题分区集合
   */
  override def getTopicPartitions(topicName: String): Set[TopicPartition] = {
    Option(_currentImage.topics().getTopic(topicName)) match {
      case None => Set.empty  // 主题不存在返回空集合
      case Some(topic) => topic.partitions().keySet().asScala.map(new TopicPartition(topicName, _))
    }
  }

  /**
   * 获取主题ID
   * @param topicName 主题名称
   * @return 主题UUID，不存在则返回ZERO_UUID
   */
  override def getTopicId(topicName: String): Uuid = _currentImage.topics().topicsByName().asScala.get(topicName).map(_.id()).getOrElse(Uuid.ZERO_UUID)

  /**
   * 根据主题ID获取主题名称
   * @param topicId 主题ID
   * @return 主题名称，不存在则返回None
   */
  override def getTopicName(topicId: Uuid): Option[String] = _currentImage.topics().topicsById.asScala.get(topicId).map(_.name())

  /**
   * 检查broker是否存活
   * @param brokerId broker ID
   * @return 如果broker存在且未被隔离返回true
   */
  override def hasAliveBroker(brokerId: Int): Boolean = {
    Option(_currentImage.cluster.broker(brokerId)).count(!_.fenced()) == 1
  }

  /**
   * 检查broker是否被隔离
   * @param brokerId broker ID
   * @return 如果broker被隔离返回true
   */
  override def isBrokerFenced(brokerId: Int): Boolean = {
    Option(_currentImage.cluster.broker(brokerId)).count(_.fenced) == 1
  }

  /**
   * 检查broker是否正在关闭
   * @param brokerId broker ID
   * @return 如果broker处于受控关闭状态返回true
   */
  override def isBrokerShuttingDown(brokerId: Int): Boolean = {
    Option(_currentImage.cluster.broker(brokerId)).count(_.inControlledShutdown) == 1
  }

  /**
   * 获取所有存活的broker元数据
   * @param image 元数据镜像
   * @return 存活broker的元数据集合
   */
  private def getAliveBrokers(image: MetadataImage): Iterable[BrokerMetadata] = {
    // 获取所有未被隔离的broker，并转换为BrokerMetadata对象
    image.cluster().brokers().values().asScala.filterNot(_.fenced()).
      map(b => new BrokerMetadata(b.id, b.rack))
  }

  /**
   * 获取指定ID的存活broker节点
   * @param brokerId broker ID
   * @param listenerName 监听器名称
   * @return 如果broker存活且有指定监听器则返回节点信息
   */
  override def getAliveBrokerNode(brokerId: Int, listenerName: ListenerName): Option[Node] = {
    // 获取未被隔离的broker，并返回其指定监听器的节点信息
    Option(_currentImage.cluster().broker(brokerId)).filterNot(_.fenced()).
      flatMap(_.node(listenerName.value()).toScala)
  }

  /**
   * 获取所有存活broker的节点信息
   * @param listenerName 监听器名称
   * @return 存活broker节点序列
   */
  override def getAliveBrokerNodes(listenerName: ListenerName): Seq[Node] = {
    // 获取所有未被隔离的broker的指定监听器节点信息
    _currentImage.cluster().brokers().values().asScala.filterNot(_.fenced()).
      flatMap(_.node(listenerName.value()).toScala).toSeq
  }

  /**
   * 获取所有broker的节点信息（包括被隔离的）
   * @param listenerName 监听器名称
   * @return 所有broker节点序列
   */
  override def getBrokerNodes(listenerName: ListenerName): Seq[Node] = {
    // 获取所有broker的指定监听器节点信息，不考虑隔离状态
    _currentImage.cluster().brokers().values().asScala.flatMap(_.node(listenerName.value()).toScala).toSeq
  }

  /**
   * 获取主题分区的Leader和ISR信息
   * @param topicName 主题名称
   * @param partitionId 分区ID
   * @return Leader和ISR信息
   */
  override def getLeaderAndIsr(topicName: String, partitionId: Int): Option[LeaderAndIsr] = {
    // 获取主题分区信息并转换为LeaderAndIsr对象
    Option(_currentImage.topics().getTopic(topicName)).
      flatMap(topic => Option(topic.partitions().get(partitionId))).
      flatMap(partition => Some(new LeaderAndIsr(partition.leader, partition.leaderEpoch,
        util.Arrays.asList(partition.isr.map(i => i: java.lang.Integer): _*), partition.leaderRecoveryState, partition.partitionEpoch)))
  }

  /**
   * 获取主题的分区数量
   * @param topicName 主题名称
   * @return 分区数量
   */
  override def numPartitions(topicName: String): Option[Int] = {
    // 获取主题的分区数量
    Option(_currentImage.topics().getTopic(topicName)).
      map(topic => topic.partitions().size())
  }

  /**
   * 获取主题名称到ID的映射视图
   * @return 主题名称到ID的映射
   */
  override def topicNamesToIds(): util.Map[String, Uuid] = _currentImage.topics.topicNameToIdView()

  /**
   * 获取主题ID到名称的映射视图
   * @return 主题ID到名称的映射
   */
  override def topicIdsToNames(): util.Map[Uuid, String] = _currentImage.topics.topicIdToNameView()

  /**
   * 获取主题ID信息（包括双向映射）
   * @return (主题名称到ID的映射, 主题ID到名称的映射)
   */
  override def topicIdInfo(): (util.Map[String, Uuid], util.Map[Uuid, String]) = {
    val image = _currentImage
    (image.topics.topicNameToIdView(), image.topics.topicIdToNameView())
  }

  /**
   * 获取分区Leader节点的端点信息
   * 如果Leader未知，返回None
   * 如果Leader已知且对应节点可用，返回Some(node)
   * 如果Leader已知但对应节点的监听器不可用，返回Some(NO_NODE)
   */
  override def getPartitionLeaderEndpoint(topicName: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val image = _currentImage
    // 通过主题名称、分区ID和监听器名称获取Leader节点信息
    Option(image.topics().getTopic(topicName)) match {
      case None => None
      case Some(topic) => Option(topic.partitions().get(partitionId)) match {
        case None => None
        case Some(partition) => Option(image.cluster().broker(partition.leader)) match {
          case None => Some(Node.noNode)
          case Some(broker) => Some(broker.node(listenerName.value()).orElse(Node.noNode()))
        }
      }
    }
  }

  /**
   * 获取分区副本的端点信息
   * @param tp 主题分区
   * @param listenerName 监听器名称
   * @return 副本ID到节点的映射
   */
  override def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    val image = _currentImage
    val result = new mutable.HashMap[Int, Node]()
    // 获取主题分区的所有副本节点信息
    Option(image.topics().getTopic(tp.topic())).foreach { topic =>
      Option(topic.partitions().get(tp.partition())).foreach { partition =>
        partition.replicas.foreach { replicaId =>
          val broker = image.cluster().broker(replicaId)
          if (broker != null && !broker.fenced()) {
            broker.node(listenerName.value).ifPresent { node =>
              if (!node.isEmpty)
                result.put(replicaId, node)
            }
          }
        }
      }
    }
    result
  }

  /**
   * 获取随机存活broker的ID
   * @return 随机选择的存活broker ID
   */
  override def getRandomAliveBrokerId: Option[Int] = {
    getRandomAliveBroker(_currentImage)
  }

  /**
   * 从元数据镜像中获取随机存活broker
   * @param image 元数据镜像
   * @return 随机选择的存活broker ID
   */
  private def getRandomAliveBroker(image: MetadataImage): Option[Int] = {
    // 获取所有存活broker并随机选择一个
    val aliveBrokers = getAliveBrokers(image).toList
    if (aliveBrokers.isEmpty) {
      None
    } else {
      Some(aliveBrokers(ThreadLocalRandom.current().nextInt(aliveBrokers.size)).id)
    }
  }

  /**
   * 获取存活broker的epoch值
   * @param brokerId broker ID
   * @return broker的epoch值
   */
  override def getAliveBrokerEpoch(brokerId: Int): Option[Long] = {
    // 获取未被隔离的broker的epoch值
    Option(_currentImage.cluster().broker(brokerId)).filterNot(_.fenced()).
      map(brokerRegistration => brokerRegistration.epoch())
  }

  /**
   * 获取集群元数据
   * @param clusterId 集群ID
   * @param listenerName 监听器名称
   * @return 集群元数据对象
   */
  override def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val image = _currentImage
    val nodes = new util.HashMap[Integer, Node]
    // 收集所有未被隔离的broker节点
    image.cluster().brokers().values().forEach { broker =>
      if (!broker.fenced()) {
        broker.node(listenerName.value()).toScala.foreach { node =>
          nodes.put(broker.id(), node)
        }
      }
    }

    // 获取节点信息的辅助函数
    def node(id: Int): Node = {
      Option(nodes.get(id)).getOrElse(Node.noNode())
    }

    val partitionInfos = new util.ArrayList[PartitionInfo]
    val internalTopics = new util.HashSet[String]

    // 收集所有主题的分区信息
    image.topics().topicsByName().values().forEach { topic =>
      topic.partitions().forEach { (key, value) =>
        val partitionId = key
        val partition = value
        partitionInfos.add(new PartitionInfo(topic.name(),
          partitionId,
          node(partition.leader),
          partition.replicas.map(replica => node(replica)),
          partition.isr.map(replica => node(replica)),
          getOfflineReplicas(image, partition, listenerName).asScala.
            map(replica => node(replica)).toArray))
        if (Topic.isInternal(topic.name())) {
          internalTopics.add(topic.name())
        }
      }
    }
    // 随机选择一个控制器节点
    val controllerNode = node(getRandomAliveBroker(image).getOrElse(-1))
    
    // 创建并返回集群元数据对象
    // 注意：Cluster构造函数不允许引用未注册的节点
    // 例如，如果分区foo-0的副本为[1, 2]但broker 2未注册
    // 我们传递的副本为[1, -1]，这看起来不太合理
    // 但为了保持与ZkMetadataCache行为一致，暂时保持这样
    new Cluster(clusterId, nodes.values(),
      partitionInfos, Collections.emptySet(), internalTopics, controllerNode)
  }

  /**
   * 检查主题是否存在
   * @param topicName 主题名称
   * @return 如果主题存在返回true
   */
  override def contains(topicName: String): Boolean =
    // 从当前元数据镜像中查找主题名称
    _currentImage.topics().topicsByName().containsKey(topicName)

  /**
   * 检查主题分区是否存在
   * @param tp 主题分区对象
   * @return 如果主题分区存在返回true
   */
  override def contains(tp: TopicPartition): Boolean = {
    // 首先尝试获取主题信息
    Option(_currentImage.topics().getTopic(tp.topic())) match {
      case None => false  // 主题不存在返回false
      case Some(topic) => topic.partitions().containsKey(tp.partition())  // 检查分区是否存在
    }
  }

  /**
   * 设置新的元数据镜像
   * 用于更新整个元数据缓存的状态
   * @param newImage 新的元数据镜像
   */
  def setImage(newImage: MetadataImage): Unit = {
    // 更新当前元数据镜像
    _currentImage = newImage
  }

  /**
   * 获取当前元数据镜像
   * @return 当前元数据镜像
   */
  def getImage(): MetadataImage = {
    _currentImage
  }

  /**
   * 获取配置资源的属性
   * @param configResource 配置资源对象
   * @return 配置属性
   */
  override def config(configResource: ConfigResource): Properties =
    // 从当前元数据镜像中获取配置属性
    _currentImage.configs().configProperties(configResource)

  /**
   * 描述客户端配额
   * @param request 客户端配额请求数据
   * @return 客户端配额响应数据
   */
  override def describeClientQuotas(request: DescribeClientQuotasRequestData): DescribeClientQuotasResponseData = {
    // 从当前元数据镜像中获取客户端配额信息
    _currentImage.clientQuotas().describe(request)
  }

  /**
   * 描述SCRAM凭证
   * @param request SCRAM凭证请求数据
   * @return SCRAM凭证响应数据
   */
  override def describeScramCredentials(request: DescribeUserScramCredentialsRequestData): DescribeUserScramCredentialsResponseData = {
    // 从当前元数据镜像中获取SCRAM凭证信息
    _currentImage.scram().describe(request)
  }

  /**
   * 获取元数据版本
   * @return 元数据版本
   */
  override def metadataVersion(): MetadataVersion = _currentImage.features().metadataVersion()

  /**
   * 获取已完成的特性集
   * 包括KRaft版本特性
   * @return 已完成的特性集
   */
  override def features(): FinalizedFeatures = {
    // 获取当前元数据镜像
    val image = _currentImage
    // 创建已完成特性的映射
    val finalizedFeatures = new java.util.HashMap[String, java.lang.Short](image.features().finalizedVersions())
    // 获取KRaft版本级别
    val kraftVersionLevel = kraftVersionSupplier.get().featureLevel()
    // 如果KRaft版本级别大于0，添加到特性集中
    if (kraftVersionLevel > 0) {
      finalizedFeatures.put(KRaftVersion.FEATURE_NAME, kraftVersionLevel)
    }
    // 创建并返回已完成的特性集
    new FinalizedFeatures(image.features().metadataVersion(),
      finalizedFeatures,
      image.highestOffsetAndEpoch().offset)
  }
}

