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

package kafka.cluster

import kafka.common.BrokerEndPointNotAvailableException
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.feature.Features._
import org.apache.kafka.common.Node
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.network.BrokerEndPoint

import scala.collection.Seq

/**
 * Broker伴生对象，提供创建Broker实例的工厂方法
 */
object Broker {

  /**
   * 创建一个新的Broker实例
   * 
   * @param id        broker的唯一标识符
   * @param endPoints 该broker的所有网络端点列表
   * @param rack      broker所在的机架信息（可选）
   * @return         新创建的Broker实例
   */
  def apply(id: Int, endPoints: Seq[EndPoint], rack: Option[String]): Broker = {
    // 使用空的特性支持创建新的Broker实例
    new Broker(id, endPoints, rack, emptySupportedFeatures)
  }

  /**
   * 创建一个只有单个网络端点的Broker实例
   * 
   * @param id       broker的唯一标识符
   * @param endPoint 该broker的单个网络端点
   * @param rack     broker所在的机架信息（可选）
   * @return        新创建的Broker实例
   */
  def apply(id: Int, endPoint: EndPoint, rack: Option[String]): Broker = {
    // 将单个端点转换为序列后创建Broker实例
    new Broker(id, Seq(endPoint), rack, emptySupportedFeatures)
  }
}

/**
 * Kafka broker类，代表Kafka集群中的一个节点
 *
 * @param id          broker的唯一标识符
 * @param endPoints   网络端点集合，每个端点包含(host, port, listener name, security protocol)信息
 * @param rack        broker所在的机架信息（可选），用于机架感知的副本分配
 * @param features    该broker支持的特性及其版本范围
 */
case class Broker(id: Int, endPoints: Seq[EndPoint], rack: Option[String], features: Features[SupportedVersionRange]) {

  // 将端点列表转换为以监听器名称为键的映射，方便快速查找
  private val endPointsMap = endPoints.map { endPoint =>
    endPoint.listenerName -> endPoint
  }.toMap

  // 验证是否存在重复的监听器名称
  if (endPointsMap.size != endPoints.size)
    throw new IllegalArgumentException(s"There is more than one end point with the same listener name: ${endPoints.mkString(",")}")

  override def toString: String =
    s"$id : ${endPointsMap.values.mkString("(",",",")")} : ${rack.orNull} : $features"

  /**
   * 使用基本网络信息创建Broker实例的构造函数
   *
   * @param id           broker的唯一标识符
   * @param host         broker的主机名或IP地址
   * @param port         broker监听的端口号
   * @param listenerName 监听器名称
   * @param protocol     安全协议类型
   */
  def this(id: Int, host: String, port: Int, listenerName: ListenerName, protocol: SecurityProtocol) = {
    this(id, Seq(EndPoint(host, port, listenerName, protocol)), None, emptySupportedFeatures)
  }

  /**
   * 从BrokerEndPoint创建Broker实例的构造函数
   *
   * @param bep          broker端点信息
   * @param listenerName 监听器名称
   * @param protocol     安全协议类型
   */
  def this(bep: BrokerEndPoint, listenerName: ListenerName, protocol: SecurityProtocol) = {
    this(bep.id, bep.host, bep.port, listenerName, protocol)
  }

  /**
   * 获取指定监听器对应的Node对象
   * 
   * @param listenerName 监听器名称
   * @return            对应的Node对象
   * @throws BrokerEndPointNotAvailableException 如果找不到指定监听器的端点
   */
  def node(listenerName: ListenerName): Node =
    getNode(listenerName).getOrElse {
      throw new BrokerEndPointNotAvailableException(s"End point with listener name ${listenerName.value} not found " +
        s"for broker $id")
    }

  /**
   * 尝试获取指定监听器对应的Node对象
   * 
   * @param listenerName 监听器名称
   * @return            Option[Node]，如果找到则返回Some(Node)，否则返回None
   */
  def getNode(listenerName: ListenerName): Option[Node] =
    endPointsMap.get(listenerName).map(endpoint => new Node(id, endpoint.host, endpoint.port, rack.orNull))

  /**
   * 获取指定监听器对应的BrokerEndPoint对象
   * 
   * @param listenerName 监听器名称
   * @return            对应的BrokerEndPoint对象
   */
  def brokerEndPoint(listenerName: ListenerName): BrokerEndPoint = {
    val endpoint = endPoint(listenerName)
    new BrokerEndPoint(id, endpoint.host, endpoint.port)
  }

  /**
   * 获取指定监听器对应的EndPoint对象
   * 
   * @param listenerName 监听器名称
   * @return            对应的EndPoint对象
   * @throws BrokerEndPointNotAvailableException 如果找不到指定监听器的端点
   */
  def endPoint(listenerName: ListenerName): EndPoint = {
    endPointsMap.getOrElse(listenerName,
      throw new BrokerEndPointNotAvailableException(s"End point with listener name ${listenerName.value} not found for broker $id"))
  }
}
