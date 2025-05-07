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
 * Leader Epoch隔离异常。当请求中包含的Leader Epoch值小于接收该请求的Broker上的Leader Epoch值时抛出此异常。
 * 
 * 应用场景：
 * 1. 在Kafka集群中，每个分区的Leader都有一个单调递增的Epoch编号，用于防止脑裂问题
 * 2. 当Leader发生变更时，新Leader的Epoch会增加，确保旧Leader无法继续处理请求
 * 
 * 触发条件：
 * 1. 客户端使用过期的元数据信息发送请求
 * 2. 在元数据更新完成之前尝试执行操作
 * 
 * 处理机制：
 * 1. 客户端收到此异常后，通常会刷新元数据信息
 * 2. 使用新的元数据重试操作，确保与当前集群状态一致
 * 
 * 设计考虑：
 * 1. 保证集群数据一致性，避免脑裂场景下的数据写入
 * 2. 作为Kafka故障转移机制的重要组成部分
 */
public class FencedLeaderEpochException extends InvalidMetadataException {
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个新的Leader Epoch隔离异常
     * @param message 异常描述信息
     */
    public FencedLeaderEpochException(String message) {
        super(message);
    }

    /**
     * 创建一个新的Leader Epoch隔离异常
     * @param message 异常描述信息
     * @param cause 导致此异常的原始异常
     */
    public FencedLeaderEpochException(String message, Throwable cause) {
        super(message, cause);
    }

}
