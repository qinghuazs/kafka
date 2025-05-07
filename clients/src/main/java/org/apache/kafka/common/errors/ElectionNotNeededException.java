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
 * 不需要选举异常
 * 
 * 该异常在Kafka集群中，当请求进行Leader选举但实际并不需要时抛出。
 * 主要出现在以下场景：
 * 1. 当前Leader节点运行正常，不需要重新选举
 * 2. 分区已经有了合适的Leader，无需进行选举
 * 3. 集群状态稳定，强制选举请求被拒绝
 * 
 * 该异常继承自InvalidMetadataException，表明这是一个元数据相关的问题，
 * 通常不需要立即的错误恢复操作，而是需要：
 * - 验证当前集群状态
 * - 检查Leader存活状态
 * - 更新本地元数据缓存
 */
public class ElectionNotNeededException extends InvalidMetadataException {

    /**
     * 使用指定的错误消息构造不需要选举异常
     * 
     * @param message 描述为什么不需要选举的错误消息
     */
    public ElectionNotNeededException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造不需要选举异常
     * 
     * @param message 描述为什么不需要选举的错误消息
     * @param cause 导致该异常的原始异常
     */
    public ElectionNotNeededException(String message, Throwable cause) {
        super(message, cause);
    }
}
