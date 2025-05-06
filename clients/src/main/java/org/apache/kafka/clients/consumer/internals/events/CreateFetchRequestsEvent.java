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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;

/**
 * {@code CreateFetchRequestsEvent} 表示 {@link Consumer} 需要向节点发送拉取请求，
 * 用于获取当前消费者已订阅分区的消息。
 * 
 * <p>该事件在消费者消息拉取流程中扮演重要角色：
 * <ul>
 * <li>触发时机：当消费者需要从Kafka集群拉取新消息时</li>
 * <li>处理过程：{@link FetchRequestManager} 会为每个已订阅的分区创建对应的拉取请求</li>
 * <li>完成条件：当 {@link FetchRequestManager} 完成拉取请求的<em>创建</em>后（注意：不包括请求的排队、发送或接收过程）</li>
 * </ul>
 * 
 * <p>事件处理流程：
 * <ol>
 * <li>消费者触发创建拉取请求事件</li>
 * <li>FetchRequestManager接收并处理该事件</li>
 * <li>根据订阅分区信息创建对应的拉取请求</li>
 * <li>请求创建完成后事件结束</li>
 * </ol>
 * 
 * <p>注意：该事件仅负责创建拉取请求，实际的请求发送和响应处理由其他组件完成
 */
public class CreateFetchRequestsEvent extends CompletableApplicationEvent<Void> {

    /**
     * 创建一个新的拉取请求事件
     *
     * @param deadlineMs 事件的截止时间（以毫秒为单位的时间戳）
     *                   用于控制事件的最大执行时间，防止事件处理时间过长
     */
    public CreateFetchRequestsEvent(final long deadlineMs) {
        super(Type.CREATE_FETCH_REQUESTS, deadlineMs);
    }
}
