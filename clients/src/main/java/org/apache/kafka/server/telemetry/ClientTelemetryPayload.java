/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.server.telemetry;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.nio.ByteBuffer;

/**
 * 客户端遥测数据负载接口
 * 用于客户端向遥测接收器发送遥测数据。该负载由broker的ClientTelemetryReceiver实现接收和处理。
 * 
 * 应用场景：
 * 1. 客户端监控：收集和传输客户端运行时的遥测数据
 * 2. 性能分析：传输客户端性能指标数据
 * 3. 问题诊断：提供客户端运行状态数据
 * 4. 资源管理：监控客户端资源使用情况
 * 
 * 设计考虑：
 * 1. 可扩展性：使用接口设计支持多种实现
 * 2. 序列化：支持不同格式的数据序列化
 * 3. 生命周期管理：支持客户端终止状态的标识
 * 4. 唯一标识：通过实例ID确保数据源的唯一性
 */
@InterfaceStability.Evolving  // 标记接口为演进中，API可能在未来版本中变化
public interface ClientTelemetryPayload {

    /**
     * 获取客户端实例ID
     * 用于唯一标识遥测数据的来源客户端实例
     * 
     * 实现要求：
     * 1. 返回值必须是全局唯一的UUID
     * 2. 同一客户端实例在其生命周期内应保持ID不变
     * 3. 重启后应生成新的实例ID
     * 
     * @return 客户端实例ID
     */
    Uuid clientInstanceId();

    /**
     * 判断客户端是否正在终止
     * 用于标识这是否是客户端的最后一次指标推送
     * 
     * 实现要求：
     * 1. 客户端正常关闭时应返回true
     * 2. 用于通知服务器清理相关资源
     * 3. 帮助服务器区分客户端正常终止和异常断开
     * 
     * @return 如果客户端正在终止返回true，否则返回false
     */
    boolean isTerminating();

    /**
     * 获取指标数据的内容类型格式
     * 返回客户端发送的指标数据的序列化格式
     * 
     * 实现要求：
     * 1. 返回标准的MIME类型
     * 2. 用于服务器端正确解析数据
     * 3. 支持多种序列化格式（如JSON、Protobuf等）
     * 
     * @return 指标数据的内容类型/序列化格式
     */
    String contentType();

    /**
     * 获取序列化后的指标数据
     * 返回从客户端接收到的序列化指标数据
     * 
     * 实现要求：
     * 1. 返回二进制格式的指标数据
     * 2. 数据格式应与contentType()返回的类型匹配
     * 3. 确保数据的完整性和一致性
     * 
     * @return 序列化的指标数据
     */
    ByteBuffer data();
}
