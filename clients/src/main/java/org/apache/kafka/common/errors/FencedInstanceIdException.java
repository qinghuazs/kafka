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
 * 实例ID隔离异常
 * 
 * 该异常在Kafka中用于处理实例级别的隔离问题，主要在以下场景中抛出：
 * 1. 当一个消费者实例被新的实例替代时
 * 2. 当实例ID与现有活动实例冲突时
 * 3. 当实例被管理员手动隔离时
 * 
 * 这种隔离机制的主要目的是：
 * - 防止脑裂情况的发生
 * - 确保同一个消费者组内不会出现重复的实例
 * - 保护数据一致性和处理的顺序性
 * 
 * 当遇到此异常时，通常表明：
 * - 可能存在配置错误
 * - 实例重启但旧实例未正确关闭
 * - 网络分区导致的多实例并存
 */
public class FencedInstanceIdException extends ApiException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造实例ID隔离异常
     * 
     * @param message 描述实例被隔离原因的错误消息
     */
    public FencedInstanceIdException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造实例ID隔离异常
     * 
     * @param message 描述实例被隔离原因的错误消息
     * @param cause 导致该异常的原始异常
     */
    public FencedInstanceIdException(String message, Throwable cause) {
        super(message, cause);
    }
}
