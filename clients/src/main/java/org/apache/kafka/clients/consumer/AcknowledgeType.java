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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Locale;

/**
 * 消息确认类型枚举
 * 定义了消费者确认消息处理结果的不同状态
 * 
 * 设计原理：
 * 1. 提供明确的消息处理结果标识
 * 2. 支持不同的错误处理策略
 * 3. 实现可靠的消息投递
 */
@InterfaceStability.Evolving
public enum AcknowledgeType {
    /** 
     * 消息已成功消费
     * 表示消费者已经正确处理了消息，可以提交偏移量
     * 
     * 使用场景：
     * 1. 消息处理完全成功
     * 2. 消息内容符合预期
     * 3. 业务逻辑执行无异常
     */
    ACCEPT((byte) 1),

    /** 
     * 消息消费失败，释放以便重新投递
     * 表示当前消费失败，但允许重新投递给其他消费者或稍后重试
     * 
     * 使用场景：
     * 1. 临时性的处理失败
     * 2. 需要重试的业务异常
     * 3. 消费者负载过高需要转移消息
     */
    RELEASE((byte) 2),

    /** 
     * 消息消费失败，拒绝并且不再重试
     * 表示消息无法处理，直接丢弃而不是重新投递
     * 
     * 使用场景：
     * 1. 消息格式错误
     * 2. 业务规则校验失败
     * 3. 不可重试的严重错误
     */
    REJECT((byte) 3);

    public final byte id;

    AcknowledgeType(byte id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }


    public static AcknowledgeType forId(byte id) {
        switch (id) {
            case 1:
                return ACCEPT;
            case 2:
                return RELEASE;
            case 3:
                return REJECT;
            default:
                throw new IllegalArgumentException("Unknown acknowledge type id: " + id);
        }
    }
}
