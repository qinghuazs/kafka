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
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.fault.FaultHandler


/**
 * SCRAM凭证发布器
 * 负责管理和发布Kafka集群中的SCRAM认证凭证更新
 * 
 * 应用场景：
 * 1. 安全认证：管理用户的SCRAM认证凭证
 * 2. 凭证同步：确保集群中的认证信息一致性
 * 3. 动态更新：支持运行时的凭证更新和删除
 */
class ScramPublisher(
  // Kafka配置对象
  conf: KafkaConfig,
  // 故障处理器，用于处理错误情况
  faultHandler: FaultHandler,
  // 节点类型（如broker、controller等）
  nodeType: String,
  // 凭证提供者，负责具体的凭证操作
  credentialProvider: CredentialProvider,
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  // 设置日志标识符，用于日志输出时的前缀
  logIdent = s"[${name()}] "

  /**
   * 获取发布器名称
   * @return 包含节点类型和ID的发布器名称
   */
  override def name(): String = s"ScramPublisher $nodeType id=${conf.nodeId}"

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
   * 负责更新SCRAM凭证
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
      // 获取SCRAM凭证变更并应用更新
      Option(delta.scramDelta()).foreach { scramDelta =>
        // 遍历所有变更的凭证
        scramDelta.changes().forEach {
          case (mechanism, userChanges) =>
            // 对每个认证机制的用户变更进行处理
            userChanges.forEach {
              case (userName, change) =>
                if (change.isPresent) {
                  // 如果存在新的凭证，更新用户凭证
                  credentialProvider.updateCredential(mechanism, userName, change.get().toCredential(mechanism))
                } else {
                  // 如果凭证被删除，移除用户凭证
                  credentialProvider.removeCredentials(mechanism, userName)
                }
            }
        }
      }
    } catch {
      // 处理更新过程中的任何异常
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing SCRAM changes from $deltaName", t)
    }
  }
}
