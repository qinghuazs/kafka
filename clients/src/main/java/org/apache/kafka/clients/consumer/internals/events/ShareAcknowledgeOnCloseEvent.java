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

import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.common.TopicIdPartition;

import java.util.Map;

/**
 * 共享确认关闭事件类
 * 该类用于在Kafka消费者关闭时处理分区消息的确认操作
 * 继承自CompletableApplicationEvent<Void>，表示这是一个可完成的应用层级事件
 */
public class ShareAcknowledgeOnCloseEvent extends CompletableApplicationEvent<Void> {

    /**
     * 存储主题分区ID到其对应确认信息的映射
     * 使用TopicIdPartition作为键，确保能够精确定位到具体的分区
     * 使用Acknowledgements作为值，包含该分区的确认详情
     */
    private final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap;

    /**
     * 构造函数，初始化共享确认关闭事件
     *
     * @param acknowledgementsMap 包含主题分区ID到确认信息的映射关系
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public ShareAcknowledgeOnCloseEvent(final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap, final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为SHARE_ACKNOWLEDGE_ON_CLOSE和截止时间
        super(Type.SHARE_ACKNOWLEDGE_ON_CLOSE, deadlineMs);
        // 初始化确认信息映射
        this.acknowledgementsMap = acknowledgementsMap;
    }

    /**
     * 获取确认信息映射
     *
     * @return 返回主题分区ID到确认信息的映射
     */
    public Map<TopicIdPartition, Acknowledgements> acknowledgementsMap() {
        // 返回确认信息映射
        return acknowledgementsMap;
    }

    /**
     * 重写toString方法的基础实现，添加acknowledgementsMap信息
     */
    @Override
    protected String toStringBase() {
        // 调用父类的toStringBase方法，并附加acknowledgementsMap信息
        return super.toStringBase() + ", acknowledgementsMap=" + acknowledgementsMap;
    }
}
