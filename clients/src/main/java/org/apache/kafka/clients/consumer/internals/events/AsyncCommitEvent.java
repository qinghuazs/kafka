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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;

/**
 * 异步提交偏移量的事件类。
 * 
 * <p>该事件用于执行异步的偏移量提交操作，具有以下特点：
 * 1. 不等待提交响应：发送提交请求后立即返回，不阻塞等待服务器响应
 * 2. 无重试机制：如果提交失败，不会进行重试操作
 * 3. 最大超时时间：使用Long.MAX_VALUE作为超时时间，实际上不会超时
 * 
 * <p>使用场景：
 * - 当应用程序对提交的实时性要求不高，更注重性能时
 * - 可以接受偶尔的提交失败，不需要保证提交的可靠性时
 * 
 * <p>注意事项：
 * - 如果不提供具体的偏移量(offsets为空)，将提交所有已消费的偏移量
 * - 由于没有重试机制，在网络不稳定的环境下可能会导致提交失败
 */
public class AsyncCommitEvent extends CommitEvent {

    /**
     * 创建一个异步提交事件实例
     *
     * @param offsets 需要提交的分区偏移量映射，key为主题分区，value为对应的偏移量和元数据
     *                如果为空，则提交所有已消费的偏移量
     */
    public AsyncCommitEvent(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        // 调用父类构造器，指定事件类型为COMMIT_ASYNC，传入偏移量映射，并设置超时时间为Long.MAX_VALUE
        // 使用Long.MAX_VALUE作为超时时间意味着实际上不会因为超时而中断提交操作
        super(Type.COMMIT_ASYNC, offsets, Long.MAX_VALUE);
    }
}
