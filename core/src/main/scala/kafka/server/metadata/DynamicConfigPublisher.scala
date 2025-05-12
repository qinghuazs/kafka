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

import java.util.Properties
import kafka.server.ConfigAdminManager.toLoggableProps
import kafka.server.{ConfigHandler, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, CLIENT_METRICS, GROUP, TOPIC}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.config.{ConfigType, ZooKeeperInternals}
import org.apache.kafka.server.fault.FaultHandler


/**
 * 动态配置发布器
 * 负责处理和发布Kafka集群中的动态配置更新
 * 
 * 应用场景：
 * 1. 配置管理：动态更新主题、代理、客户端指标和消费者组的配置
 * 2. 集群管理：维护集群级别的默认配置
 * 3. 节点配置：处理单个节点的特定配置
 * 4. 安全管理：处理SSL等安全相关配置的动态更新
 */
class DynamicConfigPublisher(
  // Kafka配置对象
  conf: KafkaConfig,
  // 故障处理器，用于处理错误情况
  faultHandler: FaultHandler,
  // 动态配置处理器映射，根据配置类型选择对应的处理器
  dynamicConfigHandlers: Map[String, ConfigHandler],
  // 节点类型（如broker、controller等）
  nodeType: String,
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  // 设置日志标识符
  logIdent = s"[${name()}] "

  /**
   * 获取发布器名称
   * @return 包含节点类型和ID的发布器名称
   */
  override def name(): String = s"DynamicConfigPublisher $nodeType id=${conf.nodeId}"

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
   * 负责处理各种类型的配置更新
   * 
   * @param delta 元数据变更信息
   * @param newImage 新的元数据镜像
   */
  def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
  ): Unit = {
    // 构建变更名称，用于日志记录
    val deltaName = s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    try {
      // 获取配置变更并处理
      Option(delta.configsDelta()).foreach { configsDelta =>
        // 遍历所有变更的资源
        configsDelta.changes().keySet().forEach { resource =>
          // 获取资源的配置属性
          val props = newImage.configs().configProperties(resource)
          // 根据资源类型进行不同的处理
          resource.`type`() match {
            case TOPIC =>
              // 处理主题配置变更
              dynamicConfigHandlers.get(ConfigType.TOPIC).foreach(topicConfigHandler =>
                try {
                  // 记录主题配置更新日志
                  info(s"Updating topic ${resource.name()} with new configuration : " +
                    toLoggableProps(resource, props).mkString(","))
                  // 应用主题配置变更
                  topicConfigHandler.processConfigChanges(resource.name(), props)
                } catch {
                  case t: Throwable => faultHandler.handleFault("Error updating topic " +
                    s"${resource.name()} with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                    s"in $deltaName", t)
                }
              )
            case BROKER =>
              // 处理代理配置变更
              dynamicConfigHandlers.get(ConfigType.BROKER).foreach(nodeConfigHandler =>
                if (resource.name().isEmpty) {
                  try {
                    // 处理集群级别的默认代理配置
                    info("Updating cluster configuration : " +
                      toLoggableProps(resource, props).mkString(","))
                    nodeConfigHandler.processConfigChanges(ZooKeeperInternals.DEFAULT_STRING, props)
                  } catch {
                    case t: Throwable => faultHandler.handleFault("Error updating " +
                      s"cluster with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                      s"in $deltaName", t)
                  }
                } else if (resource.name() == conf.nodeId.toString) {
                  try {
                    // 处理当前节点的特定配置
                    info(s"Updating node ${conf.nodeId} with new configuration : " +
                      toLoggableProps(resource, props).mkString(","))
                    nodeConfigHandler.processConfigChanges(resource.name(), props)
                    // 重新加载节点相关的配置文件
                    reloadUpdatedFilesWithoutConfigChange(props)
                  } catch {
                    case t: Throwable => faultHandler.handleFault("Error updating " +
                      s"node with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                      s"in $deltaName", t)
                  }
                }
              )
            case CLIENT_METRICS =>
              // 处理客户端指标配置变更
              dynamicConfigHandlers.get(ConfigType.CLIENT_METRICS).foreach(metricsConfigHandler =>
                try {
                  info(s"Updating client metrics ${resource.name()} with new configuration : " +
                    toLoggableProps(resource, props).mkString(","))
                  metricsConfigHandler.processConfigChanges(resource.name(), props)
                } catch {
                  case t: Throwable => faultHandler.handleFault("Error updating client metrics" +
                    s"${resource.name()} with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                    s"in $deltaName", t)
                })
            case GROUP =>
              // 处理消费者组配置变更
              dynamicConfigHandlers.get(ConfigType.GROUP).foreach(groupConfigHandler =>
                try {
                  info(s"Updating group ${resource.name()} with new configuration : " +
                    toLoggableProps(resource, props).mkString(","))
                  groupConfigHandler.processConfigChanges(resource.name(), props)
                } catch {
                  case t: Throwable => faultHandler.handleFault("Error updating group " +
                    s"${resource.name()} with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                    s"in $deltaName", t)
                })
            case _ => // 忽略其他类型的配置
          }
        }
      }
    } catch {
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing dynamic configuration changes from $deltaName", t)
    }
  }

  /**
   * 重新加载更新的配置文件
   * 用于处理节点特定的配置文件更新，如SSL证书等
   * 
   * @param props 配置属性
   */
  def reloadUpdatedFilesWithoutConfigChange(props: Properties): Unit = {
    conf.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(props)
  }
}
