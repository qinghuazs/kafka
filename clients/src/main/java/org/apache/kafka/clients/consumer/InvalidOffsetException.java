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
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * 当分区的偏移量无效（未定义或超出范围）且未配置重置策略时抛出此异常
 * 此类是一个抽象基类，有两个具体的实现：
 * @see NoOffsetForPartitionException - 当分区没有初始偏移量时抛出
 * @see OffsetOutOfRangeException - 当偏移量超出有效范围时抛出
 */
public abstract class InvalidOffsetException extends KafkaException {

    /**
     * 构造函数
     * @param message 异常信息
     */
    public InvalidOffsetException(String message) {
        super(message); // 调用父类KafkaException的构造函数，传入异常信息
    }

    /**
     * 获取发生偏移量无效的分区集合
     * 此方法由子类实现，用于返回所有出现偏移量问题的分区
     * @return 包含所有偏移量无效的主题分区集合
     */
    public abstract Set<TopicPartition> partitions();

}
