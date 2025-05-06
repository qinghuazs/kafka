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

import java.util.concurrent.BlockingQueue;

/**
 * 事件处理器接口，用于处理Kafka消费者客户端中的各类事件。该接口采用泛型设计，支持处理不同类型的事件。
 * 
 * <p>设计特点：</p>
 * <ul>
 * <li>松耦合设计：事件处理的具体含义被刻意保持松散，这使得{@code EventProcessor}能够专注于事件处理的核心逻辑，
 *     而不与周围应用程序产生过多的耦合。</li>
 * <li>无状态服务：作为一个无状态的服务，事件处理器充当管道的角色，负责接收事件并将其分发给相应的代码块进行处理。</li>
 * <li>状态管理：由于不同事件具有不同的语义含义，事件处理器需要与系统的其他部分进行交互来维护状态。这种设计
 *     使得状态管理更加清晰和可控。</li>
 * </ul>
 * 
 * <p>实现要求：</p>
 * <ul>
 * <li>事件处理器的实现不应关心事件是如何到达的。尽管事件在消费者子系统中通过{@link BlockingQueue 共享队列}进行传递，
 *     但了解事件的到达机制或处理后的结果被认为是一种反模式。</li>
 * <li>实现类应该保持简单和专注，只需要关注事件的处理逻辑本身。</li>
 * </ul>
 * 
 * <p>应用场景：</p>
 * <ul>
 * <li>消息消费事件处理：处理从Kafka服务器接收到的消息</li>
 * <li>消费者组事件处理：处理消费者组的成员变更、分区分配等事件</li>
 * <li>异常事件处理：处理消费过程中出现的各类异常情况</li>
 * </ul>
 */
public interface EventProcessor<T> {

    /**
     * 处理接收到的事件
     * 
     * @param event 要处理的事件对象，类型由泛型T指定
     * 
     * 实现说明：
     * 1. 实现类应该根据事件类型提供相应的处理逻辑
     * 2. 处理过程应该是同步的，确保事件按照接收顺序被处理
     * 3. 如果处理过程中出现异常，应该有适当的异常处理机制
     */
    void process(T event);
}
