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
 * 共享确认异步事件类
 * 该类用于在Kafka消费者内部处理分区消息确认的异步共享操作
 * 继承自ApplicationEvent基类，表示这是一个应用层级的事件
 */
public class ShareAcknowledgeAsyncEvent extends ApplicationEvent {

    /**
     * 存储主题分区ID到其对应确认信息的映射
     * 使用TopicIdPartition作为键，确保能够精确定位到具体的分区
     * 使用Acknowledgements作为值，包含该分区的确认详情
     */
    private final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap;

    /**
     * 构造函数，初始化共享确认异步事件
     *
     * @param acknowledgementsMap 包含主题分区ID到确认信息的映射关系
     */
    public ShareAcknowledgeAsyncEvent(final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        // 调用父类构造函数，指定事件类型为SHARE_ACKNOWLEDGE_ASYNC
        super(Type.SHARE_ACKNOWLEDGE_ASYNC);
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
}
