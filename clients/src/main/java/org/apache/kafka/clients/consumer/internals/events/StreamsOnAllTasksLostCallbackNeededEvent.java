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

/**
 * Streams所有任务丢失回调需求事件类
 * 该类用于在Kafka Streams中所有任务丢失时，标识需要执行回调处理
 * 继承自CompletableBackgroundEvent<Void>，表示这是一个可完成的后台事件，不需要返回值
 */
public class StreamsOnAllTasksLostCallbackNeededEvent extends CompletableBackgroundEvent<Void> {

    /**
     * 构造函数，初始化Streams所有任务丢失回调需求事件
     * 使用Long.MAX_VALUE作为截止时间，表示这个事件不会超时
     */
    public StreamsOnAllTasksLostCallbackNeededEvent() {
        // 调用父类构造函数，指定事件类型为STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED
        // 使用Long.MAX_VALUE作为截止时间，确保事件不会因超时而失败
        super(Type.STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED, Long.MAX_VALUE);
    }

    /**
     * 重写toString方法的基础实现
     * 直接使用父类的toStringBase实现，因为当前类没有额外的字段需要输出
     */
    @Override
    protected String toStringBase() {
        // 调用父类的toStringBase方法，返回基础的字符串表示
        return super.toStringBase();
    }
}
