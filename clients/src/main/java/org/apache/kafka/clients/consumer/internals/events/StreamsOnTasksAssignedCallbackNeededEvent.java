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

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;

import java.util.Objects;

/**
 * Streams任务分配回调需求事件类
 * 该类用于在Kafka Streams中任务分配时，标识需要执行回调处理
 * 继承自CompletableBackgroundEvent<Void>，表示这是一个可完成的后台事件，不需要返回值
 */
public class StreamsOnTasksAssignedCallbackNeededEvent extends CompletableBackgroundEvent<Void> {

    /**
     * 存储任务分配信息的字段
     * 使用StreamsRebalanceData.Assignment类型来保存分配给消费者的任务信息
     * 该字段为final，确保分配信息的不可变性
     */
    private final StreamsRebalanceData.Assignment assignment;

    /**
     * 构造函数，初始化Streams任务分配回调需求事件
     * 使用Long.MAX_VALUE作为截止时间，表示这个事件不会超时
     *
     * @param assignment 任务分配信息对象
     */
    public StreamsOnTasksAssignedCallbackNeededEvent(StreamsRebalanceData.Assignment assignment) {
        // 调用父类构造函数，指定事件类型为STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED
        // 使用Long.MAX_VALUE作为截止时间，确保事件不会因超时而失败
        super(Type.STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED, Long.MAX_VALUE);
        // 使用Objects.requireNonNull确保assignment参数不为null
        this.assignment = Objects.requireNonNull(assignment);
    }

    /**
     * 获取任务分配信息
     *
     * @return 返回任务分配信息对象
     */
    public StreamsRebalanceData.Assignment assignment() {
        // 返回任务分配信息
        return assignment;
    }

    /**
     * 重写toString方法的基础实现，添加assignment信息
     */
    @Override
    protected String toStringBase() {
        // 调用父类的toStringBase方法，并附加assignment信息
        return super.toStringBase() +
            ", assignment=" + assignment;
    }
}
