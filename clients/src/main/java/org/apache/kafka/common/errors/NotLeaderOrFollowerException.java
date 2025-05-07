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
 * 当Broker无法处理请求因为它不是目标分区的Leader或Follower时，会抛出此异常。
 * 这种情况通常在Leader选举或分区重分配期间暂时出现。
 * 
 * 异常场景：
 * 1. 生产者请求（Produce）：
 *    - 当请求发送到非Leader的Broker时
 *    - 因为只有Leader才能处理写入请求
 * 
 * 2. 消费者请求（Fetch）：
 *    - 当请求发送到既不是Leader也不是Follower的Broker时
 *    - 因为Fetch请求可以由Leader或Follower处理
 * 
 * 设计考虑：
 * 1. 继承自InvalidMetadataException表明这是一个元数据过期相关的异常
 * 2. 客户端需要刷新元数据来获取最新的Leader信息
 * 3. 这是一个常见的临时性异常，特别是在以下情况：
 *    - Leader选举进行时
 *    - 分区重分配时
 *    - Broker离线或重启时
 * 
 * 最佳实践：
 * 1. 实现合适的重试机制
 * 2. 在收到此异常后刷新元数据缓存
 * 3. 监控此类异常的频率，过高可能表示集群不稳定
 */
public class NotLeaderOrFollowerException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    /**
     * 创建一个无参数的NotLeaderOrFollowerException异常实例
     */
    public NotLeaderOrFollowerException() {
        super();
    }

    /**
     * 使用指定的错误消息创建异常实例
     * @param message 描述异常的详细信息，通常包含Broker不是Leader或Follower的说明
     */
    public NotLeaderOrFollowerException(String message) {
        super(message);
    }

    /**
     * 使用导致此异常的原始异常创建异常实例
     * @param cause 导致此异常的原始异常
     */
    public NotLeaderOrFollowerException(Throwable cause) {
        super(cause);
    }

    /**
     * 使用错误消息和原始异常创建异常实例
     * @param message 描述异常的详细信息
     * @param cause 导致此异常的原始异常
     */
    public NotLeaderOrFollowerException(String message, Throwable cause) {
        super(message, cause);
    }

}
