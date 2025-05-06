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
import java.util.Set;

/**
 * Streams任务撤销回调需求事件类
 * 该类用于在Kafka Streams中需要撤销活跃任务时，标识需要执行回调处理
 * 继承自CompletableBackgroundEvent<Void>，表示这是一个可完成的后台事件，不需要返回值
 */
public class StreamsOnTasksRevokedCallbackNeededEvent extends CompletableBackgroundEvent<Void> {

    /**
     * 存储需要撤销的活跃任务ID集合
     * 使用StreamsRebalanceData.TaskId类型来标识每个需要撤销的任务
     * 该字段为final，确保任务集合的不可变性
     */
    private final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke;

    /**
     * 构造函数，初始化Streams任务撤销回调需求事件
     * 使用Long.MAX_VALUE作为截止时间，表示这个事件不会超时
     *
     * @param activeTasksToRevoke 需要撤销的活跃任务ID集合
     * @throws NullPointerException 如果activeTasksToRevoke为null
     */
    public StreamsOnTasksRevokedCallbackNeededEvent(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        // 调用父类构造函数，指定事件类型为STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED
        // 使用Long.MAX_VALUE作为截止时间，确保事件不会因超时而失败
        super(Type.STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED, Long.MAX_VALUE);
        // 使用Objects.requireNonNull确保activeTasksToRevoke参数不为null
        this.activeTasksToRevoke = Objects.requireNonNull(activeTasksToRevoke);
    }

    /**
     * 获取需要撤销的活跃任务ID集合
     *
     * @return 返回需要撤销的活跃任务ID集合
     */
    public Set<StreamsRebalanceData.TaskId> activeTasksToRevoke() {
        // 返回需要撤销的活跃任务集合
        return activeTasksToRevoke;
    }

    /**
     * 重写toString方法的基础实现，添加待撤销任务信息
     */
    @Override
    protected String toStringBase() {
        // 调用父类的toStringBase方法，并附加待撤销任务信息
        return super.toStringBase() +
            ", active tasks to revoke=" + activeTasksToRevoke;
    }
}
