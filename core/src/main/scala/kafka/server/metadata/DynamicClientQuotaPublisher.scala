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

import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.fault.FaultHandler


/**
 * 动态客户端配额发布器
 * 负责处理和发布Kafka集群中的客户端配额元数据更新
 * 
 * 应用场景：
 * 1. 资源限制：动态调整客户端的资源使用限制
 * 2. 负载均衡：确保系统资源的合理分配
 * 3. 服务质量：维护集群的服务质量标准
 */
class DynamicClientQuotaPublisher(
  // Kafka配置
  conf: KafkaConfig,
  // 故障处理器，用于处理错误情况
  faultHandler: FaultHandler,
  // 节点类型（如broker、controller等）
  nodeType: String,
  // 客户端配额元数据管理器，负责具体的配额操作
  clientQuotaMetadataManager: ClientQuotaMetadataManager,
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  // 设置日志标识符，用于日志输出时的前缀
  logIdent = s"[${name()}] "

  /**
   * 获取发布器名称
   * @return 包含节点类型和ID的发布器名称
   */
  override def name(): String = s"DynamicClientQuotaPublisher $nodeType id=${conf.nodeId}"

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
   * 负责更新客户端配额
   * 
   * @param delta 元数据变更信息
   * @param newImage 新的元数据镜像
   */
  def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
  ): Unit = {
    // 构建变更名称，用于日志记录和错误追踪
    val deltaName = s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    try {
        // 获取客户端配额变更并应用更新
        Option(delta.clientQuotasDelta()).foreach { clientQuotasDelta =>
          // 通过配额元数据管理器更新配额
          clientQuotaMetadataManager.update(clientQuotasDelta)
        }
    } catch {
      // 处理更新过程中的任何异常
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing dynamic client quota changes from $deltaName", t)
    }
  }
}
