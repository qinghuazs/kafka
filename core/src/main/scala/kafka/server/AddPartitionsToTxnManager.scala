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

import kafka.server.AddPartitionsToTxnManager.{VerificationFailureRateMetricName, VerificationTimeMsMetricName}
import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, MetadataResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * 事务分区管理器伴生对象
 * 提供事务操作相关的回调函数类型定义和版本兼容性处理
 */
object AddPartitionsToTxnManager {
  // 定义回调函数类型：接收分区到错误的映射作为参数
  type AppendCallback = Map[TopicPartition, Errors] => Unit

  // 验证失败率度量指标名称
  val VerificationFailureRateMetricName = "VerificationFailureRate"
  // 验证时间度量指标名称
  val VerificationTimeMsMetricName = "VerificationTimeMs"

  /**
   * 根据生产请求版本获取对应的事务支持操作类型
   * @param version 请求版本号
   * @return 事务支持操作类型
   */
  def produceRequestVersionToTransactionSupportedOperation(version: Short): TransactionSupportedOperation = {
    // 根据版本号返回对应的操作类型
    if (version > 11) {
      addPartition  // 版本11以上支持添加分区
    } else if (version > 10) {
      genericErrorSupported  // 版本10以上支持通用错误
    } else {
      defaultError  // 低版本使用默认错误处理
    }
  }

  /**
   * 根据事务偏移提交请求版本获取对应的事务支持操作类型
   * @param version 请求版本号
   * @return 事务支持操作类型
   */
  def txnOffsetCommitRequestVersionToTransactionSupportedOperation(version: Short): TransactionSupportedOperation = {
    // 根据版本号返回对应的操作类型
    if (version > 4) {
      addPartition  // 版本4以上支持添加分区
    } else if (version > 3) {
      genericErrorSupported  // 版本3以上支持通用错误
    } else {
      defaultError  // 低版本使用默认错误处理
    }
  }
}

/**
 * 事务支持操作特质
 * 基于请求版本和具体操作处理分区响应的枚举类型
 * - defaultError: 默认工作流，用于处理低版本的生产请求或事务偏移提交请求
 * - genericErrorSupported: 用于处理支持TransactionAbortableException的客户端
 * - addPartition: 允许通过生产和事务偏移提交请求将分区添加到进行中的事务
 */
sealed trait TransactionSupportedOperation {
  // 默认不支持epoch递增
  val supportsEpochBump = false;
}

// 默认错误处理操作类型
case object defaultError extends TransactionSupportedOperation

// 支持通用错误的操作类型
case object genericErrorSupported extends TransactionSupportedOperation

// 支持添加分区的操作类型
case object addPartition extends TransactionSupportedOperation {
  // 支持epoch递增
  override val supportsEpochBump = true
}

/**
 * 事务数据和回调类
 * 用于存储要发送到节点的事务数据
 * 注意：同一时间在映射中每个事务ID最多只能存在一个请求
 * 如果映射中已存在给定事务ID，新请求会根据epoch返回响应
 *
 * @param transactionData 事务数据集合
 * @param callbacks 事务ID到回调函数的映射
 * @param startTimeMs 事务ID到开始时间的映射
 * @param transactionSupportedOperation 事务支持操作类型
 */
class TransactionDataAndCallbacks(
  val transactionData: AddPartitionsToTxnTransactionCollection,
  val callbacks: mutable.Map[String, AddPartitionsToTxnManager.AppendCallback],
  val startTimeMs: mutable.Map[String, Long],
  val transactionSupportedOperation: TransactionSupportedOperation
)

/**
 * 事务分区管理器类
 * 负责管理Kafka事务中分区的添加和验证操作
 *
 * @param config Kafka配置
 * @param client 网络客户端
 * @param metadataCache 元数据缓存
 * @param partitionFor 获取分区ID的函数
 * @param time 时间工具
 */
class AddPartitionsToTxnManager(
  config: KafkaConfig,
  client: NetworkClient,
  metadataCache: MetadataCache,
  partitionFor: String => Int,
  time: Time
) extends InterBrokerSendThread(
  "AddPartitionsToTxnSenderThread-" + config.brokerId,
  client,
  config.requestTimeoutMs,
  time
) with Logging {

  // 设置日志标识符
  this.logIdent = logPrefix

  // broker间通信监听器名称
  private val interBrokerListenerName = config.interBrokerListenerName
  // 正在处理请求的节点集合
  private val inflightNodes = mutable.HashSet[Node]()
  // 节点到事务数据的映射
  private val nodesToTransactions = mutable.Map[Node, TransactionDataAndCallbacks]()

  // 度量指标组
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  // 验证失败率度量器
  private val verificationFailureRate = metricsGroup.newMeter(VerificationFailureRateMetricName, "failures", TimeUnit.SECONDS)
  // 验证时间度量直方图
  private val verificationTimeMs = metricsGroup.newHistogram(VerificationTimeMsMetricName)

  /**
   * 添加或验证事务
   * 将分区添加到事务中或验证分区是否可以添加
   *
   * @param transactionalId 事务ID
   * @param producerId 生产者ID
   * @param producerEpoch 生产者epoch
   * @param topicPartitions 要添加的主题分区列表
   * @param callback 完成回调函数
   * @param transactionSupportedOperation 事务支持操作类型
   */
  def addOrVerifyTransaction(
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    topicPartitions: Seq[TopicPartition],
    callback: AddPartitionsToTxnManager.AppendCallback,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    // 获取事务协调器节点
    val coordinatorNode = getTransactionCoordinator(partitionFor(transactionalId))
    if (coordinatorNode.isEmpty) {
      // 如果找不到协调器，返回协调器不可用错误
      callback(topicPartitions.map(tp => tp -> Errors.COORDINATOR_NOT_AVAILABLE).toMap)
    } else {
      // 创建主题分区集合
      val topicCollection = new AddPartitionsToTxnTopicCollection()
      // 按主题分组处理分区
      topicPartitions.groupBy(_.topic).foreachEntry { (topic, tps) =>
        topicCollection.add(new AddPartitionsToTxnTopic()
          .setName(topic)
          .setPartitions(tps.map(tp => Int.box(tp.partition)).toList.asJava))
      }

      // 创建事务数据对象
      val transactionData = new AddPartitionsToTxnTransaction()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setVerifyOnly(!transactionSupportedOperation.supportsEpochBump)
        .setTopics(topicCollection)

      // 添加事务数据到管理器
      addTxnData(coordinatorNode.get, transactionData, callback, transactionSupportedOperation)
    }
  }

  /**
   * 添加事务数据到指定节点
   * 处理事务数据的添加、验证和冲突处理
   * 
   * @param node 目标节点
   * @param transactionData 事务数据
   * @param callback 完成回调函数
   * @param transactionSupportedOperation 事务支持操作类型
   */
  private def addTxnData(
    node: Node,
    transactionData: AddPartitionsToTxnTransaction,
    callback: AddPartitionsToTxnManager.AppendCallback,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    // 使用同步块确保线程安全
    nodesToTransactions.synchronized {
      // 获取当前时间戳
      val curTime = time.milliseconds()
      
      // 获取或创建节点的事务数据，如果节点不存在则添加新节点
      val existingNodeAndTransactionData = nodesToTransactions.getOrElseUpdate(node,
        new TransactionDataAndCallbacks(
          new AddPartitionsToTxnTransactionCollection(1), // 初始容量为1的事务集合
          mutable.Map[String, AddPartitionsToTxnManager.AppendCallback](), // 回调函数映射
          mutable.Map[String, Long](), // 开始时间映射
          transactionSupportedOperation)) // 事务支持操作类型

      // 查找是否存在相同事务ID的数据
      val existingTransactionData = existingNodeAndTransactionData.transactionData.find(transactionData.transactionalId)

      // 处理已存在的事务数据的三种情况
      if (existingTransactionData != null) {
        if (existingTransactionData.producerEpoch <= transactionData.producerEpoch) {
          // 根据epoch比较确定错误类型
          val error = if (existingTransactionData.producerEpoch < transactionData.producerEpoch)
            Errors.INVALID_PRODUCER_EPOCH // 新数据epoch更高，返回无效producer epoch错误
          else
            Errors.NETWORK_EXCEPTION // 相同epoch，返回网络异常以允许重试
          
          // 获取旧的回调函数并移除旧数据
          val oldCallback = existingNodeAndTransactionData.callbacks(transactionData.transactionalId)
          existingNodeAndTransactionData.transactionData.remove(transactionData)
          // 发送错误回调
          sendCallback(oldCallback, topicPartitionsToError(existingTransactionData, error), existingNodeAndTransactionData.startTimeMs(transactionData.transactionalId))
        } else {
          // 新数据epoch更低，直接返回无效producer epoch错误
          sendCallback(callback, topicPartitionsToError(transactionData, Errors.INVALID_PRODUCER_EPOCH), curTime)
          return
        }
      }

      // 添加新的事务数据
      existingNodeAndTransactionData.transactionData.add(transactionData)
      existingNodeAndTransactionData.callbacks.put(transactionData.transactionalId, callback)
      existingNodeAndTransactionData.startTimeMs.put(transactionData.transactionalId, curTime)
      // 唤醒处理线程
      wakeup()
    }
  }

  /**
   * 获取事务协调器节点
   * 根据分区ID查找对应的事务协调器节点
   * 
   * @param partition 分区ID
   * @return 协调器节点选项
   */
  private def getTransactionCoordinator(partition: Int): Option[Node] = {
    // 从元数据缓存中获取事务状态主题的leader和ISR信息
    metadataCache.getLeaderAndIsr(Topic.TRANSACTION_STATE_TOPIC_NAME, partition)
      // 过滤掉没有leader的分区
      .filter(_.leader != MetadataResponse.NO_LEADER_ID)
      // 获取活跃的broker节点
      .flatMap(metadata => metadataCache.getAliveBrokerNode(metadata.leader, interBrokerListenerName))
  }

  /**
   * 将事务数据中的分区映射到指定错误
   * 用于批量处理分区错误
   * 
   * @param transactionData 事务数据
   * @param error 错误类型
   * @return 分区到错误的映射
   */
  private def topicPartitionsToError(transactionData: AddPartitionsToTxnTransaction, error: Errors): Map[TopicPartition, Errors] = {
    // 创建可变映射存储分区错误
    val topicPartitionsToError = mutable.Map[TopicPartition, Errors]()
    // 遍历所有主题和分区
    transactionData.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        topicPartitionsToError.put(new TopicPartition(topic.name, partition), error)
      }
    }
    // 更新验证失败率指标
    verificationFailureRate.mark(topicPartitionsToError.size)
    // 转换为不可变映射返回
    topicPartitionsToError.toMap
  }

  /**
   * 发送回调函数
   * 更新验证时间指标并执行回调
   * 
   * @param callback 回调函数
   * @param errorMap 错误映射
   * @param startTimeMs 开始时间戳
   */
  private def sendCallback(callback: AddPartitionsToTxnManager.AppendCallback, errorMap: Map[TopicPartition, Errors], startTimeMs: Long): Unit = {
    // 更新验证时间指标
    verificationTimeMs.update(time.milliseconds() - startTimeMs)
    // 执行回调函数
    callback(errorMap)
  }

  /**
   * 添加分区到事务请求处理器
   * 处理请求完成后的回调逻辑
   * 
   * @param node 目标节点
   * @param transactionDataAndCallbacks 事务数据和回调
   */
  private class AddPartitionsToTxnHandler(node: Node, transactionDataAndCallbacks: TransactionDataAndCallbacks) extends RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      // 从处理中节点集合移除当前节点
      inflightNodes.remove(node)
      
      // 处理认证异常
      if (response.authenticationException != null) {
        error(s"AddPartitionsToTxnRequest failed for node ${response.destination} with an " +
          "authentication exception.", response.authenticationException)
        sendCallbacksToAll(Errors.forException(response.authenticationException).code)
      } 
      // 处理版本不匹配异常
      else if (response.versionMismatch != null) {
        warn(s"AddPartitionsToTxnRequest failed for node ${response.destination} with invalid version exception. This suggests verification is not supported." +
          s"Continuing handling the produce request.")
        // 跳过验证，返回空错误映射
        transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
          sendCallback(callback, Map.empty, transactionDataAndCallbacks.startTimeMs(txnId))
        }
      } 
      // 处理网络异常
      else if (response.wasDisconnected || response.wasTimedOut) {
        warn(s"AddPartitionsToTxnRequest failed for node ${response.destination} with a network exception.")
        sendCallbacksToAll(Errors.NETWORK_EXCEPTION.code)
      } 
      // 处理正常响应
      else {
        val addPartitionsToTxnResponseData = response.responseBody.asInstanceOf[AddPartitionsToTxnResponse].data
        // 处理响应级别错误
        if (addPartitionsToTxnResponseData.errorCode != 0) {
          error(s"AddPartitionsToTxnRequest for node ${response.destination} returned with error ${Errors.forCode(addPartitionsToTxnResponseData.errorCode)}.")
          // 将集群授权失败错误转换为无效事务状态错误
          val finalError = if (addPartitionsToTxnResponseData.errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code)
            Errors.INVALID_TXN_STATE.code
          else
            addPartitionsToTxnResponseData.errorCode

          sendCallbacksToAll(finalError)
        } 
        // 处理成功响应
        else {
          // 处理每个事务的结果
          addPartitionsToTxnResponseData.resultsByTransaction.forEach { transactionResult =>
            val unverified = mutable.Map[TopicPartition, Errors]()
            // 处理每个主题的结果
            transactionResult.topicResults.forEach { topicResult =>
              // 处理每个分区的结果
              topicResult.resultsByPartition.forEach { partitionResult =>
                val tp = new TopicPartition(topicResult.name, partitionResult.partitionIndex)
                // 处理分区级别错误
                if (partitionResult.partitionErrorCode != Errors.NONE.code) {
                  // 根据错误类型转换错误码
                  val code =
                    if (partitionResult.partitionErrorCode == Errors.PRODUCER_FENCED.code)
                      Errors.INVALID_PRODUCER_EPOCH.code
                    else if (partitionResult.partitionErrorCode() == Errors.TRANSACTION_ABORTABLE.code
                      && transactionDataAndCallbacks.transactionSupportedOperation == defaultError)
                      Errors.INVALID_TXN_STATE.code
                    else
                      partitionResult.partitionErrorCode
                  unverified.put(tp, Errors.forCode(code))
                }
              }
            }
            // 更新验证失败率指标
            verificationFailureRate.mark(unverified.size)
            // 执行回调
            val callback = transactionDataAndCallbacks.callbacks(transactionResult.transactionalId)
            sendCallback(callback, unverified.toMap, transactionDataAndCallbacks.startTimeMs(transactionResult.transactionalId))
          }
        }
      }
      // 唤醒处理线程
      wakeup()
    }

    /**
     * 构建错误映射
     * 根据事务ID和错误码创建分区到错误的映射
     * 
     * @param transactionalId 事务ID
     * @param errorCode 错误码
     * @return 分区到错误的映射
     */
    private def buildErrorMap(transactionalId: String, errorCode: Short): Map[TopicPartition, Errors] = {
      // 根据事务ID查找事务数据
      val transactionData = transactionDataAndCallbacks.transactionData.find(transactionalId)
      // 将事务数据中的分区映射到指定错误
      topicPartitionsToError(transactionData, Errors.forCode(errorCode))
    }

    /**
     * 向所有回调发送错误
     * 将指定错误码发送给所有注册的回调函数
     * 
     * @param errorCode 错误码
     */
    private def sendCallbacksToAll(errorCode: Short): Unit = {
      // 遍历所有回调函数，为每个事务构建错误映射并发送
      transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
        sendCallback(callback, buildErrorMap(txnId, errorCode), transactionDataAndCallbacks.startTimeMs(txnId))
      }
    }
  }

  /**
   * 生成请求集合
   * 为每个未处理的节点创建添加分区到事务的请求
   * 
   * @return 请求和完成处理器的集合
   */
  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    // 创建请求列表
    val list = new util.ArrayList[RequestAndCompletionHandler]()
    // 获取当前时间戳
    val currentTimeMs = time.milliseconds()
    // 创建已移除节点的集合
    val removedNodes = mutable.Set[Node]()
    
    // 同步处理节点到事务的映射
    nodesToTransactions.synchronized {
      // 遍历所有节点和事务数据
      nodesToTransactions.foreach { case (node, transactionDataAndCallbacks) =>
        // 检查节点是否正在处理请求
        if (!inflightNodes.contains(node)) {
          // 创建新的请求和处理器
          list.add(new RequestAndCompletionHandler(
            currentTimeMs,
            node,
            AddPartitionsToTxnRequest.Builder.forBroker(transactionDataAndCallbacks.transactionData),
            new AddPartitionsToTxnHandler(node, transactionDataAndCallbacks)
          ))
          // 将节点添加到已移除集合
          removedNodes.add(node)
        }
      }
      // 处理已移除的节点
      removedNodes.foreach { node =>
        // 将节点添加到处理中集合
        inflightNodes.add(node)
        // 从节点到事务映射中移除节点
        nodesToTransactions.remove(node)
      }
    }
    // 返回请求列表
    list
  }

  /**
   * 关闭管理器
   * 清理资源并移除度量指标
   */
  override def shutdown(): Unit = {
    // 调用父类的关闭方法
    super.shutdown()
    // 移除验证失败率度量指标
    metricsGroup.removeMetric(VerificationFailureRateMetricName)
    // 移除验证时间度量指标
    metricsGroup.removeMetric(VerificationTimeMsMetricName)
  }

}
