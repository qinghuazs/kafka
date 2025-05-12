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
package org.apache.kafka.server.quota;

/**
 * broker上可配置的客户端请求配额类型
 * 
 * 应用场景：
 * 1. 资源限制：控制客户端对broker资源的使用
 * 2. 流量控制：限制不同类型请求的处理速率
 * 3. 负载均衡：通过配额确保公平的资源分配
 * 4. 服务质量：防止单个客户端占用过多资源
 * 
 * 设计考虑：
 * 1. 类型分离：将不同类型的请求配额分开管理
 * 2. 扩展性：支持添加新的配额类型
 * 3. 简单性：使用枚举确保类型安全
 * 4. 可维护性：清晰定义支持的配额类型
 */
public enum ClientQuotaType {
    /**
     * 生产请求配额类型
     * 用于限制客户端向broker发送消息的速率
     * 应用：控制生产者写入速率，防止单个生产者占用过多资源
     */
    PRODUCE,

    /**
     * 获取请求配额类型
     * 用于限制客户端从broker获取消息的速率
     * 应用：控制消费者读取速率，确保公平的消息消费
     */
    FETCH,

    /**
     * 通用请求配额类型
     * 用于限制客户端发送的所有类型请求的总速率
     * 应用：控制客户端的总体请求频率，包括元数据请求等
     */
    REQUEST,

    /**
     * 控制器变更请求配额类型
     * 用于限制控制器相关的变更操作请求速率
     * 应用：控制管理操作的频率，如主题创建、分区调整等
     */
    CONTROLLER_MUTATION
}
