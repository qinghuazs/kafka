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
package org.apache.kafka.clients.consumer.internals.events;

import java.util.Objects;

/**
 * 主题元数据事件类
 * 该类用于处理Kafka主题的元数据请求和更新
 * 继承自AbstractTopicMetadataEvent，提供主题特定的元数据处理功能
 */
public class TopicMetadataEvent extends AbstractTopicMetadataEvent {

    /**
     * 存储主题名称
     * 使用final修饰确保不可变性
     */
    private final String topic;

    /**
     * 构造函数，初始化主题元数据事件
     *
     * @param topic 主题名称，不能为null
     * @param deadlineMs 事件处理的截止时间（毫秒）
     * @throws NullPointerException 如果topic参数为null
     */
    public TopicMetadataEvent(final String topic, final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为TOPIC_METADATA和截止时间
        super(Type.TOPIC_METADATA, deadlineMs);
        // 使用Objects.requireNonNull确保topic参数不为null
        this.topic = Objects.requireNonNull(topic);
    }

    /**
     * 获取主题名称
     *
     * @return 返回主题名称
     */
    public String topic() {
        // 返回主题名称
        return topic;
    }

    /**
     * 重写toString方法的基础实现，添加topic信息
     */
    @Override
    public String toStringBase() {
        // 调用父类的toStringBase方法，并附加topic信息
        return super.toStringBase() + ", topic=" + topic;
    }
}
