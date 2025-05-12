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

package kafka.server

import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.network.BrokerEndPoint

import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters._

/**
 * 抽象抓取管理器
 * 负责管理和协调多个抓取线程，用于从其他broker获取消息
 * 
 * @param name 管理器名称
 * @param clientId 客户端ID
 * @param numFetchers 每个broker的抓取线程数量
 * 
 * 应用场景：
 * 1. 副本同步：管理从leader副本获取数据的线程
 * 2. 消费者抓取：管理消费者从broker获取消息的线程
 * 3. 性能监控：跟踪抓取延迟和速率
 */
abstract class AbstractFetcherManager[T <: AbstractFetcherThread](val name: String, clientId: String, numFetchers: Int)
  extends Logging {
  // 创建度量指标组，用于监控性能
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // 抓取线程映射表：(源broker_id, 每个源broker的fetcher_id) => fetcher线程
  // 包私有，用于测试
  private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, T]
  
  // 同步锁对象，用于线程安全
  private val lock = new Object
  
  // 每个broker的抓取线程数量
  private var numFetchersPerBroker = numFetchers
  
  // 失败分区集合，记录抓取失败的分区
  val failedPartitions = new FailedPartitions
  
  // 日志标识符
  this.logIdent = "[" + name + "] "

  // 度量标签，包含clientId信息
  private val tags = Map("clientId" -> clientId).asJava

  // 注册最大延迟度量指标
  metricsGroup.newGauge("MaxLag", () => {
    // 计算所有抓取器/主题/分区中的当前最大延迟
    fetcherThreadMap.values.foldLeft(0L) { (curMaxLagAll, fetcherThread) =>
      // 获取每个线程的最大延迟
      val maxLagThread = fetcherThread.fetcherLagStats.stats.values.foldLeft(0L)((curMaxLagThread, lagMetrics) =>
        math.max(curMaxLagThread, lagMetrics.lag))
      // 比较并返回全局最大延迟
      math.max(curMaxLagAll, maxLagThread)
    }
  }, tags)

  // 注册最小抓取速率度量指标
  metricsGroup.newGauge("MinFetchRate", () => {
    // 计算所有抓取器/主题/分区中的当前最小抓取速率
    val headRate = fetcherThreadMap.values.headOption.map(_.fetcherStats.requestRate.oneMinuteRate).getOrElse(0.0)
    fetcherThreadMap.values.foldLeft(headRate)((curMinAll, fetcherThread) =>
      math.min(curMinAll, fetcherThread.fetcherStats.requestRate.oneMinuteRate))
  }, tags)

  // 注册失败分区计数度量指标
  metricsGroup.newGauge("FailedPartitionsCount", () => failedPartitions.size, tags)

  // 注册死亡线程计数度量指标
  metricsGroup.newGauge("DeadThreadCount", () => deadThreadCount, tags)

  /**
   * 获取死亡线程数量
   * @return 当前失败的抓取线程数量
   */
  private[server] def deadThreadCount: Int = lock synchronized { 
    fetcherThreadMap.values.count(_.isThreadFailed) 
  }

  /**
   * 调整线程池大小
   * 重新分配分区到新的线程池大小
   * 
   * @param newSize 新的线程池大小
   */
  def resizeThreadPool(newSize: Int): Unit = {
    // 内部函数：迁移分区到新的线程池
    def migratePartitions(newSize: Int): Unit = {
      // 存储所有需要迁移的分区状态
      val allRemovedPartitionsMap = mutable.Map[TopicPartition, InitialFetchState]()
      // 遍历所有抓取线程
      fetcherThreadMap.foreachEntry { (id, thread) =>
        // 移除线程的所有分区
        val partitionStates = thread.removeAllPartitions()
        // 如果线程ID大于等于新大小，关闭该线程
        if (id.fetcherId >= newSize)
          thread.shutdown()
        // 保存分区状态
        partitionStates.foreachEntry { (topicPartition, currentFetchState) =>
            val initialFetchState = InitialFetchState(currentFetchState.topicId, thread.leader.brokerEndPoint(),
              currentLeaderEpoch = currentFetchState.currentLeaderEpoch,
              initOffset = currentFetchState.fetchOffset)
            allRemovedPartitionsMap += topicPartition -> initialFetchState
        }
      }
      // 将分区重新添加到抓取器
      addFetcherForPartitions(allRemovedPartitionsMap)
    }

    // 同步块：执行线程池调整
    lock synchronized {
      val currentSize = numFetchersPerBroker
      info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
      numFetchersPerBroker = newSize
      if (newSize != currentSize) {
        // 重新分配所有分区以保持基于哈希的分配方式
        migratePartitions(newSize)
      }
      // 关闭空闲的抓取线程
      shutdownIdleFetcherThreads()
    }
  }

  /**
   * 获取指定主题分区的抓取线程
   * 用于测试
   * 
   * @param topicPartition 主题分区
   * @return 处理该分区的抓取线程（如果存在）
   */
  private[server] def getFetcher(topicPartition: TopicPartition): Option[T] = {
    lock synchronized {
      // 查找处理指定分区的抓取线程
      fetcherThreadMap.values.find { fetcherThread =>
        fetcherThread.fetchState(topicPartition).isDefined
      }
    }
  }

  /**
   * 获取主题分区的抓取器ID
   * 用于测试
   * 基于主题分区的哈希值计算抓取器ID
   * 
   * @param topicPartition 主题分区
   * @return 抓取器ID
   */
  private[server] def getFetcherId(topicPartition: TopicPartition): Int = {
    lock synchronized {
      // 使用主题名称和分区ID计算哈希值，确保分区均匀分布到抓取线程
      Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
    }
  }

  /**
   * 标记分区需要截断
   * 仅被ReplicaAlterDirManager使用
   * 用于在副本目录变更时处理分区截断
   * 
   * @param brokerId broker ID
   * @param topicPartition 主题分区
   * @param truncationOffset 截断偏移量
   */
  def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    lock synchronized {
      // 获取分区对应的抓取器ID
      val fetcherId = getFetcherId(topicPartition)
      // 创建broker和抓取器ID的组合标识
      val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
      // 找到对应的抓取线程并标记分区需要截断
      fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
        thread.markPartitionsForTruncation(topicPartition, truncationOffset)
      }
    }
  }

  /**
   * 创建抓取线程
   * 由子类实现以创建特定类型的抓取器
   * 
   * @param fetcherId 抓取器ID
   * @param sourceBroker 源broker端点
   * @return 抓取线程实例
   */
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): T

  /**
   * 为分区添加抓取器
   * 管理分区的抓取线程分配和启动
   * 
   * @param partitionAndOffsets 分区及其初始抓取状态的映射
   */
  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    lock synchronized {
      // 按broker和抓取器ID对分区进行分组
      val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
        BrokerAndFetcherId(brokerAndInitialFetchOffset.leader, getFetcherId(topicPartition))
      }

      // 内部函数：添加并启动抓取线程
      def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId,
                                   brokerIdAndFetcherId: BrokerIdAndFetcherId): T = {
        // 创建新的抓取线程
        val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
        // 将线程添加到映射表
        fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
        // 启动线程
        fetcherThread.start()
        fetcherThread
      }

      // 处理每个抓取器的分区
      for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        // 获取或创建抓取线程
        val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
          // 如果存在相同leader的线程，重用它
          case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == brokerAndFetcherId.broker =>
            currentFetcherThread
          // 如果存在但leader不同，关闭旧线程并创建新线程
          case Some(f) =>
            f.shutdown()
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          // 如果不存在，创建新线程
          case None =>
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }
        // 将分区添加到抓取线程
        addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
      }
    }
  }

  /**
   * 添加失败的分区
   * @param topicPartition 失败的主题分区
   */
  def addFailedPartition(topicPartition: TopicPartition): Unit = {
    lock synchronized {
      failedPartitions.add(topicPartition)
    }
  }

  /**
   * 向抓取线程添加分区
   * @param fetcherThread 抓取线程
   * @param initialOffsetAndEpochs 初始偏移量和epoch信息
   */
  protected def addPartitionsToFetcherThread(fetcherThread: T,
                                             initialOffsetAndEpochs: collection.Map[TopicPartition, InitialFetchState]): Unit = {
    // 添加分区到线程
    fetcherThread.addPartitions(initialOffsetAndEpochs)
    // 记录日志
    info(s"Added fetcher to broker ${fetcherThread.leader.brokerEndPoint().id} for partitions $initialOffsetAndEpochs")
  }

  /**
   * 更新主题ID
   * 如果抓取器和分区状态存在，更新所有分区以包含主题ID
   * 
   * @param partitionsToUpdate 需要更新的分区到其leader ID的映射
   * @param topicIds 主题名称到ID的映射函数
   */
  def maybeUpdateTopicIds(partitionsToUpdate: Map[TopicPartition, Int], topicIds: String => Option[Uuid]): Unit = {
    lock synchronized {
      // 按broker和抓取器ID对分区进行分组
      val partitionsPerFetcher = partitionsToUpdate.groupBy { case (topicPartition, leaderId) =>
        BrokerIdAndFetcherId(leaderId, getFetcherId(topicPartition))
      }.map { case (brokerAndFetcherId, partitionsToUpdate) =>
        (brokerAndFetcherId, partitionsToUpdate.keySet)
      }

      // 更新每个抓取器的主题ID
      for ((brokerIdAndFetcherId, partitions) <- partitionsPerFetcher) {
        fetcherThreadMap.get(brokerIdAndFetcherId).foreach(_.maybeUpdateTopicIds(partitions, topicIds))
      }
    }
  }

  /**
   * 移除分区的抓取器
   * @param partitions 要移除的分区集合
   * @return 移除的分区状态映射
   */
  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    lock synchronized {
      // 从所有抓取器中移除指定分区
      for (fetcher <- fetcherThreadMap.values)
        fetchStates ++= fetcher.removePartitions(partitions)
      // 从失败分区集合中移除
      failedPartitions.removeAll(partitions)
    }
    // 记录日志
    if (partitions.nonEmpty)
      info(s"Removed fetcher for partitions $partitions")
    fetchStates
  }

  /**
   * 关闭空闲的抓取线程
   * 清理没有分区的抓取线程
   */
  def shutdownIdleFetcherThreads(): Unit = {
    lock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerIdAndFetcherId]
      // 遍历所有抓取线程
      for ((key, fetcher) <- fetcherThreadMap) {
        // 如果线程没有分区，关闭它
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      // 从映射表中移除关闭的线程
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  /**
   * 关闭所有抓取器
   * 用于系统关闭时清理资源
   */
  def closeAllFetchers(): Unit = {
    lock synchronized {
      // 首先初始化所有抓取器的关闭
      for ((_, fetcher) <- fetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      // 然后完全关闭所有抓取器
      for ((_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      // 清空抓取器映射表
      fetcherThreadMap.clear()
    }
  }
}

/**
 * FailedPartitions类用于跟踪在截断或追加过程中标记为失败的分区
 * 失败可能由以下错误导致：
 * 1. 存储异常：当写入或读取数据时发生存储相关错误
 * 2. 隔离的epoch：当分区的leader epoch不匹配时
 * 3. 意外错误：其他未预期的系统错误
 * 
 * 注意：由存储错误导致的失败分区会在日志目录离线后从该集合中移除
 * 
 * 应用场景：
 * 1. 故障追踪：记录和管理发生故障的分区
 * 2. 错误恢复：支持系统从故障中恢复
 * 3. 状态监控：提供分区健康状态的监控
 */
class FailedPartitions {
  // 使用HashSet存储失败的分区，支持快速查找和更新
  private val failedPartitionsSet = new mutable.HashSet[TopicPartition]

  /**
   * 获取失败分区的数量
   * @return 失败分区集合的大小
   */
  def size: Int = synchronized {
    // 同步访问以确保线程安全
    failedPartitionsSet.size
  }

  /**
   * 添加失败的分区
   * @param topicPartition 要添加的主题分区
   */
  def add(topicPartition: TopicPartition): Unit = synchronized {
    // 同步添加分区到失败集合
    failedPartitionsSet += topicPartition
  }

  /**
   * 批量移除失败分区
   * @param topicPartitions 要移除的分区集合
   */
  def removeAll(topicPartitions: Set[TopicPartition]): Unit = synchronized {
    // 同步从失败集合中移除指定的分区
    failedPartitionsSet --= topicPartitions
  }

  /**
   * 检查分区是否在失败集合中
   * @param topicPartition 要检查的主题分区
   * @return 如果分区在失败集合中返回true
   */
  def contains(topicPartition: TopicPartition): Boolean = synchronized {
    // 同步检查分区是否存在于失败集合
    failedPartitionsSet.contains(topicPartition)
  }

  /**
   * 获取所有失败分区的副本
   * @return 失败分区集合的不可变副本
   */
  def partitions(): Set[TopicPartition] = synchronized {
    // 同步获取失败分区集合的副本，转换为不可变集合
    failedPartitionsSet.toSet
  }
}

/**
 * 表示broker和抓取器ID的组合
 * 用于标识特定broker上的特定抓取器
 * 
 * @param broker broker端点信息
 * @param fetcherId 抓取器ID
 */
case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

/**
 * 表示分区初始抓取状态
 * 用于初始化分区的抓取操作
 * 
 * @param topicId 主题ID（可选）
 * @param leader leader broker端点
 * @param currentLeaderEpoch 当前leader的epoch值
 * @param initOffset 初始抓取偏移量
 */
case class InitialFetchState(topicId: Option[Uuid], leader: BrokerEndPoint, currentLeaderEpoch: Int, initOffset: Long)

/**
 * 表示broker ID和抓取器ID的组合
 * 用于在抓取管理器中唯一标识一个抓取线程
 * 
 * @param brokerId broker ID
 * @param fetcherId 抓取器ID
 */
case class BrokerIdAndFetcherId(brokerId: Int, fetcherId: Int)
