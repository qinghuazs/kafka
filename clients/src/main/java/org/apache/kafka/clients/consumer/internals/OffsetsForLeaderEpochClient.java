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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.LogContext;

import java.util.Map;

/**
 * 领导者纪元偏移量客户端
 * 这是一个便利类，用于向OffsetsForLeaderEpoch API发送异步请求
 * 
 * 应用场景：
 * 1. 获取特定领导者纪元的偏移量信息
 * 2. 处理分区领导者变更
 * 3. 确保消费者位置的一致性
 * 4. 支持日志截断检测
 */
public class OffsetsForLeaderEpochClient extends AsyncClient<
        Map<TopicPartition, SubscriptionState.FetchPosition>, // 请求数据类型：主题分区到获取位置的映射
        OffsetsForLeaderEpochRequest,                        // 请求类型：领导者纪元偏移量请求
        OffsetsForLeaderEpochResponse,                       // 响应类型：领导者纪元偏移量响应
        OffsetsForLeaderEpochUtils.OffsetForEpochResult> {   // 结果类型：处理后的偏移量结果

    /**
     * 构造函数
     * 创建一个新的领导者纪元偏移量客户端实例
     *
     * @param client 消费者网络客户端，用于发送网络请求
     * @param logContext 日志上下文，用于日志记录
     */
    OffsetsForLeaderEpochClient(ConsumerNetworkClient client, LogContext logContext) {
        // 调用父类AsyncClient的构造函数，初始化网络客户端和日志上下文
        super(client, logContext);
    }

    /**
     * 准备领导者纪元偏移量请求
     * 重写父类方法，将请求数据转换为具体的请求构建器
     *
     * @param node 目标节点，请求将发送到该节点
     * @param requestData 请求数据，包含主题分区和获取位置的映射
     * @return 领导者纪元偏移量请求的构建器
     */
    @Override
    protected AbstractRequest.Builder<OffsetsForLeaderEpochRequest> prepareRequest(
            Node node, Map<TopicPartition, SubscriptionState.FetchPosition> requestData) {
        // 委托给OffsetsForLeaderEpochUtils工具类准备请求
        return OffsetsForLeaderEpochUtils.prepareRequest(requestData);
    }

    /**
     * 处理领导者纪元偏移量响应
     * 重写父类方法，将响应转换为处理结果
     *
     * @param node 响应来源的节点
     * @param requestData 原始请求数据
     * @param response 服务器的响应
     * @return 处理后的偏移量结果
     */
    @Override
    protected OffsetsForLeaderEpochUtils.OffsetForEpochResult handleResponse(
            Node node,
            Map<TopicPartition, SubscriptionState.FetchPosition> requestData,
            OffsetsForLeaderEpochResponse response) {
        // 委托给OffsetsForLeaderEpochUtils工具类处理响应
        return OffsetsForLeaderEpochUtils.handleResponse(requestData, response);
    }
}