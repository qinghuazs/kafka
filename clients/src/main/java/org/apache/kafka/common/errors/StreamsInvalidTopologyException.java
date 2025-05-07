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
package org.apache.kafka.common.errors;

/**
 * 当Kafka Streams应用程序的处理拓扑结构无效时抛出此异常。
 * 
 * 在Kafka Streams中，拓扑结构定义了：
 * 1. 数据流的处理逻辑和顺序
 * 2. 处理器节点之间的连接关系
 * 3. 状态存储的配置和使用
 * 
 * 此异常通常出现在以下场景：
 * - 拓扑中存在循环依赖
 * - 处理器节点的连接配置错误
 * - 状态存储的配置无效
 * - 缺少必要的源处理器或接收处理器
 * 
 * 处理建议：
 * - 检查拓扑结构的设计是否合理
 * - 确保所有处理器节点正确连接
 * - 验证状态存储的配置
 * - 确保拓扑包含必要的源和接收节点
 */
public class StreamsInvalidTopologyException extends ApiException {
    /**
     * 使用指定的错误消息构造异常
     * @param message 描述拓扑结构无效原因的错误消息
     */
    public StreamsInvalidTopologyException(String message) {
        super(message);
    }
}
