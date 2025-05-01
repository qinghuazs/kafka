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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

/**
 * The result of {@link Admin#describeMetadataQuorum(DescribeMetadataQuorumOptions)}
 */
/**
 * {@link Admin#describeMetadataQuorum(DescribeMetadataQuorumOptions)} 调用的结果类。
 * 该类用于获取Kafka集群中元数据仲裁组的信息。
 */
public class DescribeMetadataQuorumResult {

    // 存储仲裁组信息的Future对象
    // 使用KafkaFuture而不是CompletableFuture是为了提供更好的异常处理和类型安全
    private final KafkaFuture<QuorumInfo> quorumInfo;

    /**
     * 构造函数，初始化元数据仲裁组描述结果
     * 
     * @param quorumInfo 包含仲裁组信息的Future对象
     */
    DescribeMetadataQuorumResult(KafkaFuture<QuorumInfo> quorumInfo) {
        // 初始化quorumInfo字段，存储异步获取的仲裁组信息
        this.quorumInfo = quorumInfo;
    }

    /**
     * 获取包含仲裁组信息的Future对象
     * 
     * @return 返回一个KafkaFuture，当完成时将产生仲裁组信息
     */
    public KafkaFuture<QuorumInfo> quorumInfo() {
        // 返回存储的Future对象，允许调用者异步获取仲裁组信息
        return quorumInfo;
    }
}
