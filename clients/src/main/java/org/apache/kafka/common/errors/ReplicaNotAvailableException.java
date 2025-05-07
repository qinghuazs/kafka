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
 * 副本不可用异常
 * 
 * 当请求的主题分区副本不可用时抛出此异常。这种情况通常在以下场景发生：
 * 1. 副本重分配过程中（临时性异常）
 * 2. 副本所在的broker离线
 * 3. 副本同步延迟过大
 * 
 * 重要说明：
 * 从Kafka 2.6版本开始，如果broker不是分区的副本，对于仅面向leader或follower的请求
 * （如Fetch请求等），将返回{@link NotLeaderOrFollowerException}异常而不是本异常。
 * 
 * 处理建议：
 * - 检查broker状态和网络连接
 * - 等待副本重分配完成
 * - 确认副本因子配置是否合理
 * - 监控副本同步状态
 */
public class ReplicaNotAvailableException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param message 异常消息，描述副本不可用的具体原因
     */
    public ReplicaNotAvailableException(String message) {
        super(message);
    }

    /**
     * 构造函数
     *
     * @param message 异常消息，描述副本不可用的具体原因
     * @param cause 导致此异常的原始异常
     */
    public ReplicaNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 构造函数
     *
     * @param cause 导致此异常的原始异常
     */
    public ReplicaNotAvailableException(Throwable cause) {
        super(cause);
    }

}
