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
 * PollEvent类表示Kafka消费者执行消息轮询操作的事件
 * 该事件在消费者调用poll()方法时触发，用于从Kafka服务器拉取消息
 * 继承自ApplicationEvent，是消费者事件处理机制中的一个重要组件
 */
public class PollEvent extends ApplicationEvent {

    /**
     * 轮询操作的超时时间（以毫秒为单位）
     * 这个值决定了消费者在没有新消息可用时等待的最长时间
     * 如果在超时时间内没有新消息到达，poll操作将返回空结果
     */
    private final long pollTimeMs;

    /**
     * 创建一个新的PollEvent实例
     * @param pollTimeMs 轮询超时时间（毫秒），用于控制消费者的阻塞时间
     */
    public PollEvent(final long pollTimeMs) {
        // 调用父类构造函数，设置事件类型为POLL
        super(Type.POLL);
        // 初始化轮询超时时间
        this.pollTimeMs = pollTimeMs;
    }

    /**
     * 获取轮询操作的超时时间
     * @return 返回配置的轮询超时时间（毫秒）
     */
    public long pollTimeMs() {
        return pollTimeMs;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", pollTimeMs=" + pollTimeMs;
    }
}