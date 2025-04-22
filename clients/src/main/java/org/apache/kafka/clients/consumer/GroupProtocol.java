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

import java.util.Locale;

/**
 * Kafka消费者组协议类型的枚举。
 * 定义了Kafka支持的两种不同的消费者组协议类型，用于控制消费者组的行为模式。
 */
public enum GroupProtocol {
    /** 
     * 经典消费者组协议。
     * 这是Kafka最初的消费者组实现方式，主要特点是：
     * 1. 一个分区只能被消费者组内的一个消费者消费
     * 2. 消费者组成员之间严格分区所有权
     * 3. 提供强有序性保证
     */
    CLASSIC("CLASSIC"),

    /** 
     * 新版消费者组协议
     * 这是Kafka后来引入的改进版消费者组协议，主要特点是：
     * 1. 支持更灵活的消费模式
     * 2. 提供更好的扩展性
     * 3. 可以实现更细粒度的消费控制
     */
    CONSUMER("CONSUMER");

    /**
     * 协议类型的字符串表示。
     * 用于在配置和API中标识具体的协议类型。
     */
    public final String name;

    /**
     * 构造函数
     * @param name 协议类型的字符串标识符
     */
    GroupProtocol(final String name) {
        this.name = name;
    }

    /**
     * 根据字符串名称查找对应的协议类型（大小写不敏感）。
     * 
     * @param name 要查找的协议类型名称
     * @return 对应的GroupProtocol枚举值
     * @throws IllegalArgumentException 如果找不到对应的协议类型
     */
    public static GroupProtocol of(final String name) {
        return GroupProtocol.valueOf(name.toUpperCase(Locale.ROOT));
    }
}
