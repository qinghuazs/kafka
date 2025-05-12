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

package kafka.controller

import com.typesafe.scalalogging.Logger
import kafka.utils.Logging

/**
 * 状态变更日志记录器的单例对象
 * 用于创建和管理全局共享的日志记录器实例
 */
object StateChangeLogger {
  // 创建一个名为"state.change.logger"的日志记录器实例
  private val logger = Logger("state.change.logger")
}

/**
 * 状态变更日志记录器类
 * 
 * 该类负责根据运行上下文（是否在Kafka控制器中）设置适当的日志标识符(logIdent)。
 * 在Kafka集群中，无论broker是否为控制器，都会使用此日志记录器记录状态变更信息
 * （例如：ReplicaManager和MetadataCache都会使用此日志记录器）。
 *
 * @param brokerId broker的唯一标识符
 * @param inControllerContext 是否在控制器上下文中运行
 * @param controllerEpoch 可选的控制器纪元号，仅在控制器上下文中有效
 */
class StateChangeLogger(brokerId: Int, inControllerContext: Boolean, controllerEpoch: Option[Int]) extends Logging {

  // 验证控制器纪元的合法性：只有在控制器上下文中才能定义控制器纪元
  if (controllerEpoch.isDefined && !inControllerContext)
    throw new IllegalArgumentException("Controller epoch should only be defined if inControllerContext is true")

  // 使用单例对象中的共享日志记录器实例
  override lazy val logger: Logger = StateChangeLogger.logger

  // 在本地代码块中初始化日志标识符
  locally {
    // 根据运行上下文确定前缀：控制器上下文使用"Controller"，否则使用"Broker"
    val prefix = if (inControllerContext) "Controller" else "Broker"
    // 处理控制器纪元信息：如果存在则添加到标识符中，否则为空字符串
    val epochEntry = controllerEpoch.fold("")(epoch => s" epoch=$epoch")
    // 组装最终的日志标识符，格式：[前缀 id=brokerId epoch=epochNumber]
    logIdent = s"[$prefix id=$brokerId$epochEntry] "
  }

}
