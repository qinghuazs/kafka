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

import kafka.network.ConnectionQuotas
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.utils.Sanitizer

import java.net.{InetAddress, UnknownHostException}
import org.apache.kafka.image.{ClientQuotaDelta, ClientQuotasDelta}
import org.apache.kafka.server.config.{QuotaConfig, ZooKeeperInternals}

import scala.jdk.OptionConverters.RichOptionalDouble



// 支持的配额实体的严格层次结构
sealed trait QuotaEntity

/**
 * IP配额实体
 * 用于管理特定IP地址的配额
 * @param ip IP地址字符串
 */
case class IpEntity(ip: String) extends QuotaEntity

/**
 * 默认IP配额实体
 * 用于管理所有未指定IP地址的默认配额
 */
case object DefaultIpEntity extends QuotaEntity

/**
 * 用户配额实体
 * 用于管理特定用户的配额
 * @param user 用户名
 */
case class UserEntity(user: String) extends QuotaEntity

/**
 * 默认用户配额实体
 * 用于管理所有未指定用户的默认配额
 */
case object DefaultUserEntity extends QuotaEntity

/**
 * 客户端ID配额实体
 * 用于管理特定客户端的配额
 * @param clientId 客户端ID
 */
case class ClientIdEntity(clientId: String) extends QuotaEntity

/**
 * 默认客户端ID配额实体
 * 用于管理所有未指定客户端ID的默认配额
 */
case object DefaultClientIdEntity extends QuotaEntity

/**
 * 显式用户和显式客户端ID配额实体
 * 用于管理特定用户和特定客户端ID组合的配额
 * @param user 用户名
 * @param clientId 客户端ID
 */
case class ExplicitUserExplicitClientIdEntity(user: String, clientId: String) extends QuotaEntity

/**
 * 显式用户和默认客户端ID配额实体
 * 用于管理特定用户的所有未指定客户端ID的配额
 * @param user 用户名
 */
case class ExplicitUserDefaultClientIdEntity(user: String) extends QuotaEntity

/**
 * 默认用户和显式客户端ID配额实体
 * 用于管理所有未指定用户的特定客户端ID的配额
 * @param clientId 客户端ID
 */
case class DefaultUserExplicitClientIdEntity(clientId: String) extends QuotaEntity

/**
 * 默认用户和默认客户端ID配额实体
 * 用于管理所有未指定用户和未指定客户端ID的配额
 */
case object DefaultUserDefaultClientIdEntity extends QuotaEntity

/**
 * 客户端配额元数据管理器
 * 负责处理元数据日志中出现的配额元数据记录，并根据需要更新配额管理器和缓存
 * 
 * 应用场景：
 * 1. 配额管理：处理客户端请求的速率限制
 * 2. 资源控制：防止单个客户端占用过多资源
 * 3. 公平调度：确保系统资源的合理分配
 * 
 * @param quotaManagers 配额管理器集合，用于管理不同类型的配额
 * @param connectionQuotas 连接配额管理器，用于管理连接相关的配额
 */
class ClientQuotaMetadataManager(private[metadata] val quotaManagers: QuotaManagers,
                                 private[metadata] val connectionQuotas: ConnectionQuotas) extends Logging {

  /**
   * 更新配额变更
   * 处理配额变更集合中的每个变更
   * 
   * @param quotasDelta 配额变更集合
   */
  def update(quotasDelta: ClientQuotasDelta): Unit = {
    // 遍历所有变更并逐个处理
    quotasDelta.changes().forEach { (key, value) =>
      update(key, value)
    }
  }

  /**
   * 处理单个配额实体的变更
   * 根据实体类型分别处理IP配额和用户客户端配额
   * 
   * @param entity 配额实体
   * @param quotaDelta 配额变更信息
   */
  private def update(entity: ClientQuotaEntity, quotaDelta: ClientQuotaDelta): Unit = {
    if (entity.entries().containsKey(ClientQuotaEntity.IP)) {
      // 处理IP配额，在IP配额管理器中，None用于表示默认实体
      val ipEntity = Option(entity.entries().get(ClientQuotaEntity.IP)) match {
        case Some(ip) => IpEntity(ip)  // 创建特定IP的配额实体
        case None => DefaultIpEntity    // 创建默认IP配额实体
      }
      handleIpQuota(ipEntity, quotaDelta)
    } else if (entity.entries().containsKey(ClientQuotaEntity.USER) ||
        entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
      // 获取用户值和客户端ID值，这些值可能为null，所以需要使用containsKey
      val userVal = entity.entries().get(ClientQuotaEntity.USER)
      val clientIdVal = entity.entries().get(ClientQuotaEntity.CLIENT_ID)

      // 在用户+客户端配额管理器中，"<default>"用于默认实体
      // 需要处理所有可能的值、默认值和缺失实体的组合
      val userClientEntity = if (entity.entries().containsKey(ClientQuotaEntity.USER) &&
          entity.entries().containsKey(ClientQuotaEntity.CLIENT_ID)) {
        if (userVal == null && clientIdVal == null) {
          DefaultUserDefaultClientIdEntity  // 默认用户和默认客户端ID
        } else if (userVal == null) {
          DefaultUserExplicitClientIdEntity(clientIdVal)  // 默认用户和特定客户端ID
        } else if (clientIdVal == null) {
          ExplicitUserDefaultClientIdEntity(userVal)  // 特定用户和默认客户端ID
        } else {
          ExplicitUserExplicitClientIdEntity(userVal, clientIdVal)  // 特定用户和特定客户端ID
        }
      } else if (entity.entries().containsKey(ClientQuotaEntity.USER)) {
        if (userVal == null) {
          DefaultUserEntity  // 默认用户
        } else {
          UserEntity(userVal)  // 特定用户
        }
      } else {
        if (clientIdVal == null) {
          DefaultClientIdEntity  // 默认客户端ID
        } else {
          ClientIdEntity(clientIdVal)  // 特定客户端ID
        }
      }
      // 处理每个配额变更
      quotaDelta.changes().forEach { (key, value) =>
        handleUserClientQuotaChange(userClientEntity, key, value.toScala)
      }
    } else {
      // 记录不支持的配额实体类型
      warn(s"Ignoring unsupported quota entity $entity.")
    }
  }

  /**
   * 处理IP配额变更
   * 更新IP连接速率限制
   * 
   * @param ipEntity IP配额实体
   * @param quotaDelta 配额变更信息
   */
  private[metadata] def handleIpQuota(ipEntity: QuotaEntity, quotaDelta: ClientQuotaDelta): Unit = {
    // 解析IP地址
    val inetAddress = ipEntity match {
      case IpEntity(ip) =>
        try {
          Some(InetAddress.getByName(ip))  // 尝试解析IP地址
        } catch {
          case _: UnknownHostException => throw new IllegalArgumentException(s"Unable to resolve address $ip")
        }
      case DefaultIpEntity => None  // 默认IP实体返回None
      case _ => throw new IllegalStateException("Should only handle IP quota entities here")
    }

    // 处理配额变更
    quotaDelta.changes().forEach { (key, value) =>
      // 连接配额只处理连接速率限制
      val quotaName = key
      val quotaValue = value
      if (!quotaName.equals(QuotaConfig.IP_CONNECTION_RATE_OVERRIDE_CONFIG)) {
        // 忽略意外的配额键
        warn(s"Ignoring unexpected quota key $quotaName for entity $ipEntity")
      } else {
        try {
          // 更新IP连接速率配额
          connectionQuotas.updateIpConnectionRateQuota(inetAddress, quotaValue.toScala.map(_.toInt))
        } catch {
          case t: Throwable => error(s"Failed to update IP quota $ipEntity", t)
        }
      }
    }
  }

  /**
   * 处理用户和客户端配额变更
   * 根据配额类型更新相应的配额管理器
   * 
   * 应用场景：
   * 1. 消费者限流：控制消费者的字节速率
   * 2. 生产者限流：控制生产者的字节速率
   * 3. 请求限制：控制请求的百分比
   * 4. 控制器变更：控制控制器的变更速率
   * 
   * @param quotaEntity 配额实体（用户/客户端）
   * @param key 配额类型键
   * @param newValue 新的配额值
   */
  private def handleUserClientQuotaChange(quotaEntity: QuotaEntity, key: String, newValue: Option[Double]): Unit = {
    // 根据配额类型选择对应的配额管理器
    val manager = key match {
      case QuotaConfig.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG => quotaManagers.fetch      // 消费者字节速率配额
      case QuotaConfig.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG => quotaManagers.produce    // 生产者字节速率配额
      case QuotaConfig.REQUEST_PERCENTAGE_OVERRIDE_CONFIG => quotaManagers.request    // 请求百分比配额
      case QuotaConfig.CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG => quotaManagers.controllerMutation  // 控制器变更速率配额
      case _ =>
        // 记录未知配额类型的警告并返回
        warn(s"Ignoring unexpected quota key $key for entity $quotaEntity")
        return
    }

    // 将配额实体转换为带有清理值的Options，供配额管理器使用
    val (sanitizedUser, sanitizedClientId) = quotaEntity match {
      // 处理单独的用户实体
      case UserEntity(user) => (Some(Sanitizer.sanitize(user)), None)
      // 处理默认用户实体
      case DefaultUserEntity => (Some(ZooKeeperInternals.DEFAULT_STRING), None)
      // 处理单独的客户端ID实体
      case ClientIdEntity(clientId) => (None, Some(Sanitizer.sanitize(clientId)))
      // 处理默认客户端ID实体
      case DefaultClientIdEntity => (None, Some(ZooKeeperInternals.DEFAULT_STRING))
      // 处理显式用户和显式客户端ID组合
      case ExplicitUserExplicitClientIdEntity(user, clientId) => 
        (Some(Sanitizer.sanitize(user)), Some(Sanitizer.sanitize(clientId)))
      // 处理显式用户和默认客户端ID组合
      case ExplicitUserDefaultClientIdEntity(user) => 
        (Some(Sanitizer.sanitize(user)), Some(ZooKeeperInternals.DEFAULT_STRING))
      // 处理默认用户和显式客户端ID组合
      case DefaultUserExplicitClientIdEntity(clientId) => 
        (Some(ZooKeeperInternals.DEFAULT_STRING), Some(Sanitizer.sanitize(clientId)))
      // 处理默认用户和默认客户端ID组合
      case DefaultUserDefaultClientIdEntity => 
        (Some(ZooKeeperInternals.DEFAULT_STRING), Some(ZooKeeperInternals.DEFAULT_STRING))
      // 处理意外的IP配额实体
      case IpEntity(_) | DefaultIpEntity => 
        throw new IllegalStateException("Should not see IP quota entities here")
    }

    // 创建新的配额值对象
    val quotaValue = newValue.map(new Quota(_, true))
    try {
      // 更新配额管理器中的配额值
      manager.updateQuota(
        sanitizedUser = sanitizedUser,                                    // 清理后的用户名
        clientId = sanitizedClientId.map(Sanitizer.desanitize),          // 原始客户端ID
        sanitizedClientId = sanitizedClientId,                           // 清理后的客户端ID
        quota = quotaValue)                                              // 新的配额值
    } catch {
      case t: Throwable => error(s"Failed to update user-client quota $quotaEntity", t)  // 记录更新失败的错误
    }
  }
}
