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

import kafka.utils.Logging
import org.apache.kafka.image.loader.{LoaderManifest, LoaderManifestType}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.fault.FaultHandler

import scala.concurrent.TimeoutException


/**
 * ACL发布器类
 * 负责处理和发布Kafka集群中的ACL（访问控制列表）元数据更新
 * 
 * 应用场景：
 * 1. 权限管理：处理ACL变更和更新
 * 2. 元数据同步：确保ACL元数据在集群中的一致性
 * 3. 安全控制：维护授权状态的正确性
 * 
 * 设计考虑：
 * 1. 线程安全：确保多线程环境下的ACL更新安全
 * 2. 顺序保证：保持ACL更新的顺序性
 * 3. 状态一致：避免出现无效的授权状态
 */
class AclPublisher(
  // 节点ID
  nodeId: Int,
  // 故障处理器，用于处理错误情况
  faultHandler: FaultHandler,
  // 节点类型（如broker、controller等）
  nodeType: String,
  // 可选的授权器实例
  authorizer: Option[Authorizer],
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  // 设置日志标识符
  logIdent = s"[${name()}] "

  /**
   * 获取发布器名称
   * @return 包含节点类型和ID的发布器名称
   */
  override def name(): String = s"AclPublisher $nodeType id=$nodeId"

  // 标记是否完成初始加载
  private var completedInitialLoad = false

  /**
   * 处理元数据更新
   * 当收到新的元数据更新时被调用
   * 
   * 实现说明：
   * 1. 处理ACL变更
   * 2. 确保更新顺序
   * 3. 维护授权状态
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
    // 创建变更名称，用于日志记录
    val deltaName = s"MetadataDelta up to ${newImage.offset()}"

    // 获取ACL变更并处理
    Option(delta.aclsDelta()).foreach { aclsDelta =>
      // 匹配授权器类型
      authorizer match {
        // 如果是集群元数据授权器
        case Some(authorizer: ClusterMetadataAuthorizer) => 
          // 检查是否是快照类型的更新
          if (manifest.`type`().equals(LoaderManifestType.SNAPSHOT)) {
            try {
              // 记录快照加载日志
              info(s"Loading authorizer snapshot at offset ${newImage.offset()}")
              // 加载ACL快照
              authorizer.loadSnapshot(newImage.acls().acls())
            } catch {
              // 处理快照加载错误
              case t: Throwable => faultHandler.handleFault("Error loading " +
                s"authorizer snapshot in $deltaName", t)
            }
          } else {
            try {
              // 处理增量ACL变更
              aclsDelta.changes().forEach((key, value) =>
                if (value.isPresent) {
                  // 添加新的ACL
                  authorizer.addAcl(key, value.get())
                } else {
                  // 移除已存在的ACL
                  authorizer.removeAcl(key)
                })
            } catch {
              // 处理增量更新错误
              case t: Throwable => faultHandler.handleFault("Error loading " +
                s"authorizer changes in $deltaName", t)
            }
          }
          // 检查是否需要完成初始加载
          if (!completedInitialLoad) {
            // 标记初始加载完成并启用授权器
            completedInitialLoad = true
            authorizer.completeInitialLoad()
          }
        // 如果没有配置集群元数据授权器，不做任何处理
        case _ => 
      }
    }
  }

  /**
   * 关闭发布器
   * 处理资源清理和状态更新
   */
  override def close(): Unit = {
    // 匹配授权器类型
    authorizer match {
      // 如果是集群元数据授权器，使用超时异常完成初始加载
      case Some(authorizer: ClusterMetadataAuthorizer) => 
        authorizer.completeInitialLoad(new TimeoutException)
      case _ =>
    }
  }
}
