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
 * 当分区的同步副本（ISR）数量低于配置的最小同步副本数（min.insync.replicas）时抛出此异常。
 * 这个异常在消息写入之前就会检查并抛出，可以避免消息重复的问题。
 * 
 * 应用场景：
 * 1. 写入请求预检查：在消息写入之前验证ISR数量是否满足要求
 * 2. 数据可靠性保证：确保有足够的副本可以同步数据
 * 3. 集群健康检查：反映集群中副本同步状态的问题
 * 
 * 设计考虑：
 * 1. 继承自RetriableException表明这是一个可重试的异常
 * 2. 与NotEnoughReplicasAfterAppendException的区别是检查时机在写入之前
 * 3. 有助于及早发现副本同步问题，避免数据一致性风险
 * 
 * 最佳实践：
 * 1. 合理配置min.insync.replicas值，平衡可用性和可靠性
 * 2. 监控ISR列表变化，及时处理副本失效问题
 * 3. 在应用程序中实现合适的重试策略
 */
public class NotEnoughReplicasException extends RetriableException {
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个无参数的NotEnoughReplicasException异常实例
     */
    public NotEnoughReplicasException() {
        super();
    }

    /**
     * 使用错误消息和原始异常创建异常实例
     * @param message 描述异常的详细信息
     * @param cause 导致此异常的原始异常
     */
    public NotEnoughReplicasException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的错误消息创建异常实例
     * @param message 描述异常的详细信息，通常包含当前ISR数量和所需最小ISR数量
     */
    public NotEnoughReplicasException(String message) {
        super(message);
    }

    /**
     * 使用导致此异常的原始异常创建异常实例
     * @param cause 导致此异常的原始异常
     */
    public NotEnoughReplicasException(Throwable cause) {
        super(cause);
    }
}
