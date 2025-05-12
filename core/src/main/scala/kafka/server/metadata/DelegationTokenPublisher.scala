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

import kafka.server.DelegationTokenManager
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.fault.FaultHandler


/**
 * 委托令牌发布器
 * 负责处理和发布Kafka集群中的委托令牌元数据更新
 * 
 * 应用场景：
 * 1. 安全认证：管理客户端访问权限的令牌
 * 2. 元数据同步：确保令牌信息在集群中的一致性
 * 3. 动态更新：支持运行时的令牌更新和删除
 */
class DelegationTokenPublisher(
  // Kafka配置
  conf: KafkaConfig,
  // 故障处理器，用于处理错误情况
  faultHandler: FaultHandler,
  // 节点类型（如broker、controller等）
  nodeType: String,
  // 委托令牌管理器，负责具体的令牌操作
  tokenManager: DelegationTokenManager,
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  // 设置日志标识符
  logIdent = s"[${name()}] "

  // 标记是否是首次发布元数据
  var _firstPublish = true

  /**
   * 获取发布器名称
   * @return 包含节点类型和ID的发布器名称
   */
  override def name(): String = s"DelegationTokenPublisher $nodeType id=${conf.nodeId}"

  /**
   * 处理元数据更新（带有加载器清单的版本）
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
    // 调用不带清单参数的版本处理更新
    onMetadataUpdate(delta, newImage)
  }

  /**
   * 处理元数据更新的主要方法
   * 负责初始化和更新委托令牌
   * 
   * @param delta 元数据变更信息
   * @param newImage 新的元数据镜像
   */
  def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
  ): Unit = {
    // 构建变更名称，用于日志记录
    val deltaName = if (_firstPublish) {
      s"initial MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    } else {
      s"update MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    }
    try {
      // 首次发布时初始化令牌缓存
      if (_firstPublish) {
        // 从新镜像中获取委托令牌并初始化
        Option(newImage.delegationTokens()).foreach { delegationTokenImage =>
          // 遍历所有令牌并更新到令牌管理器
          delegationTokenImage.tokens().forEach { (_, delegationTokenData) =>
            tokenManager.updateToken(tokenManager.getDelegationToken(delegationTokenData.tokenInformation()))
          }
        }
        // 更新首次发布标志
        _firstPublish = false
      }
      
      // 应用委托令牌的变更
      Option(delta.delegationTokenDelta()).foreach { delegationTokenDelta =>
        // 遍历所有令牌变更
        delegationTokenDelta.changes().forEach { 
          case (tokenId, delegationTokenData) => 
            if (delegationTokenData.isPresent) {
              // 如果令牌存在，更新令牌
              tokenManager.updateToken(tokenManager.getDelegationToken(delegationTokenData.get().tokenInformation()))
            } else {
              // 如果令牌不存在，移除令牌
              tokenManager.removeToken(tokenId)
            }
        }
      }
    } catch {
      // 处理元数据发布过程中的未捕获异常
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing DelegationToken changes from $deltaName", t)
    }
  }
}
