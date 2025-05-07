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
 * 消费者组重平衡进行中异常
 * 
 * 当消费者组正在进行分区重平衡时抛出此异常。重平衡是Kafka消费者组的核心机制，用于在消费者加入或离开组时重新分配分区。
 * 
 * 触发重平衡的场景：
 * 1. 消费者加入或离开消费者组
 * 2. 会话超时导致消费者被踢出组
 * 3. 订阅的主题分区数发生变化
 * 4. 消费者手动调用unsubscribe()
 * 
 * 重平衡过程：
 * - 暂停所有消费者的消费操作
 * - 等待所有活跃消费者提交偏移量
 * - 重新分配分区给剩余的消费者
 * - 消费者从新分配的分区位置继续消费
 * 
 * 影响：
 * - 短暂的消息消费延迟
 * - 可能导致消息重复消费
 * - 消费者处理中的消息可能被中断
 * 
 * 处理建议：
 * - 合理配置session.timeout.ms和heartbeat.interval.ms
 * - 实现幂等的消息处理逻辑
 * - 在消费者关闭时优雅退出
 * - 考虑使用静态成员ID减少不必要的重平衡
 */
public class RebalanceInProgressException extends ApiException {
    private static final long serialVersionUID = 1L;

    public RebalanceInProgressException() {
        super();
    }

    public RebalanceInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

    public RebalanceInProgressException(String message) {
        super(message);
    }

    public RebalanceInProgressException(Throwable cause) {
        super(cause);
    }
}
