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

import org.apache.kafka.common.KafkaException;

/**
 * 偏移量提交失败异常
 * 当使用{@link KafkaConsumer#commitSync()}提交偏移量时遇到不可恢复的错误时抛出此异常
 * 
 * 主要发生场景：
 * 1. 消费者组在提交完成前发生了rebalance
 * 2. 分区已经被重新分配给组内其他成员
 * 3. 消费者处理消息的时间超过了max.poll.interval.ms配置
 * 
 * 设计原理：
 * 1. 标识提交失败且无法重试的情况
 * 2. 提供清晰的错误信息和解决方案
 * 3. 帮助诊断消费者性能问题
 * 
 * 解决方案：
 * 1. 增加max.poll.interval.ms的值
 * 2. 减少max.poll.records返回的批次大小
 * 3. 优化消息处理逻辑，提高处理速度
 */
public class CommitFailedException extends KafkaException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 详细的错误信息
     */
    public CommitFailedException(final String message) {
        super(message);
    }

    /**
     * 使用默认错误消息构造异常
     * 默认消息说明了异常原因和可能的解决方案
     */
    public CommitFailedException() {
        super("提交无法完成，因为消费者组已经发生rebalance并将分区分配给其他成员。" +
                "这表示两次poll()调用之间的时间间隔超过了配置的max.poll.interval.ms，" +
                "通常意味着poll循环在消息处理上花费了太多时间。" +
                "你可以通过增加max.poll.interval.ms的值，" +
                "或者通过max.poll.records减少poll()返回的批次大小来解决这个问题。");
    }
}
