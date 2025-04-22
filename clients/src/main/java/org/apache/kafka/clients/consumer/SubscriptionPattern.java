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

import java.util.Objects;

/**
 * 表示一个与Google RE2/J兼容的正则表达式，用于订阅Kafka主题。
 * 该类仅保存正则表达式的字符串表示形式，所有确保其与RE2/J兼容的验证工作都委托给broker执行。
 * 
 * 应用场景：
 * 1. 当消费者需要动态订阅符合特定命名模式的主题时使用
 * 2. 支持使用正则表达式匹配多个主题名称，例如："test.*"可以匹配所有以"test"开头的主题
 * 3. 作为TopicRe2JPatternSubscriptionChangeEvent的参数，用于更新消费者的订阅模式
 * 
 * 设计考虑：
 * 1. 将正则表达式的验证工作委托给broker，避免客户端和服务器端使用不同的正则表达式引擎导致的不一致
 * 2. 使用不可变的字符串字段存储模式，确保线程安全
 */
public class SubscriptionPattern {

    /**
     * 正则表达式的字符串表示，必须与Google RE2/J兼容。
     * RE2/J是Google开发的正则表达式引擎，具有以下特点：
     * 1. 性能高效：保证线性时间复杂度，避免回溯
     * 2. 内存使用可预测：不会出现灾难性的内存使用情况
     * 3. 线程安全：可以被多个线程同时使用
     */
    private final String pattern;

    /**
     * 创建一个新的订阅模式实例
     * 
     * @param pattern 正则表达式字符串，将用于匹配主题名称
     *               例如："test.*"将匹配所有以"test"开头的主题
     */
    public SubscriptionPattern(String pattern) {
        this.pattern = pattern;
    }

    /**
     * 获取正则表达式模式字符串
     * 
     * @return 返回与RE2/J兼容的正则表达式模式字符串
     */
    public String pattern() {
        return this.pattern;
    }

    @Override
    public String toString() {
        return pattern;
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SubscriptionPattern &&
            Objects.equals(pattern, ((SubscriptionPattern) obj).pattern);
    }
}
