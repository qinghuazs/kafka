/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.cluster

import org.apache.kafka.common.{KafkaException, Endpoint => JEndpoint}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import java.util.Locale

/**
 * EndPoint对象提供了用于处理Kafka broker网络端点相关的工具方法
 */
object EndPoint {
  /**
   * 从连接字符串中解析监听器名称
   * 
   * @param connectionString 格式为"listenerName://host:port"的连接字符串
   * @return 提取并转换为大写的监听器名称
   * @throws KafkaException 当无法从连接字符串中解析出监听器名称时抛出异常
   */
  def parseListenerName(connectionString: String): String = {
    // 查找第一个冒号的位置，用于分隔监听器名称和主机端口信息
    val firstColon = connectionString.indexOf(':')
    if (firstColon < 0) {
      throw new KafkaException(s"Unable to parse a listener name from $connectionString")
    }
    // 提取并转换为大写的监听器名称
    connectionString.substring(0, firstColon).toUpperCase(Locale.ROOT)
  }

  /**
   * 将Java版本的Endpoint转换为Scala版本的EndPoint
   * 
   * @param endpoint Java版本的端点对象
   * @return 转换后的Scala版本EndPoint对象
   */
  def fromJava(endpoint: JEndpoint): EndPoint =
    new EndPoint(endpoint.host(),
      endpoint.port(),
      new ListenerName(endpoint.listenerName().get()),
      endpoint.securityProtocol())
}

/**
 * Kafka broker的网络端点定义，用于表示broker的监听地址
 * 
 * 每个EndPoint包含了以下核心信息：
 * - host：broker监听的主机名或IP地址
 * - port：监听的端口号
 * - listenerName：监听器名称，用于标识不同的监听配置
 * - securityProtocol：安全协议类型，如PLAINTEXT、SSL等
 */
case class EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol) {
  /**
   * 生成标准格式的连接字符串
   * 格式为：listenerName://host:port
   * 如果host为null，则只显示端口号
   * 
   * @return 格式化的连接字符串
   */
  def connectionString: String = {
    // 根据host是否为null构造主机端口部分
    val hostport =
      if (host == null)
        ":"+port
      else
        Utils.formatAddress(host, port)
    // 组合成最终的连接字符串
    listenerName.value + "://" + hostport
  }

  /**
   * 将Scala版本的EndPoint转换为Java版本的Endpoint
   * 用于在Scala和Java代码之间进行互操作
   * 
   * @return Java版本的Endpoint对象
   */
  def toJava: JEndpoint = {
    new JEndpoint(listenerName.value, securityProtocol, host, port)
  }
}
