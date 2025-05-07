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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * 请求管理器接口
 * 如果有请求需要发送，{@code PollResult}将包含{@code UnsentRequest}；
 * 否则，返回到下一次轮询事件的时间间隔。
 * 
 * 应用场景：
 * 1. 管理消费者的网络请求
 * 2. 处理心跳和关闭请求
 * 3. 控制请求的发送时机
 * 4. 管理消费者的生命周期
 */
public interface RequestManager {

    /**
     * 轮询方法，用于在消费者正常运行期间发送网络请求
     * 
     * 实现细节：
     * - 在消费者的网络I/O线程的单线程上下文中调用
     * - 不需要同步保护
     * - 不执行实际的网络I/O操作
     * - 方法本身不应该阻塞
     * - 需要快速执行以确保及时发送心跳
     * 
     * 应用场景：
     * - 检查是否有待发送的请求
     * - 准备网络I/O操作
     * - 管理请求队列
     * - 控制请求发送时机
     *
     * @param currentTimeMs 调用方法时的当前系统时间（毫秒），用于确定是否执行时间敏感的操作
     * @return PollResult 包含未发送的请求或下次轮询的时间
     */
    PollResult poll(long currentTimeMs);

    /**
     * 关闭时的轮询方法，用于在消费者关闭时发送网络请求
     * 
     * 实现细节：
     * - 在消费者的网络I/O线程的单线程上下文中调用
     * - 不需要同步保护
     * - 不执行实际的网络I/O操作
     * - 方法本身不应该阻塞
     * - 需要快速执行以确保在用户提供的超时时间内完成关闭任务
     *
     * @param currentTimeMs 调用方法时的当前系统时间（毫秒）
     * @return PollResult 包含关闭请求，默认返回空结果
     */
    default PollResult pollOnClose(long currentTimeMs) {
        // 返回空的轮询结果
        return EMPTY;
    }

    /**
     * 获取应用线程可以安全等待的延迟时间
     * 
     * 实现细节：
     * - 计算安全的等待时间
     * - 考虑心跳间隔等因素
     * - 确保不会错过重要的状态变化
     * 
     * 应用场景：
     * - 当发送心跳时，订阅状态可能发生变化
     * - 阻塞时间超过心跳间隔可能导致应用线程无法及时响应变化
     * - 控制应用线程的响应性
     *
     * @param currentTimeMs 调用方法时的当前系统时间（毫秒）
     * @return 最大等待时间（毫秒），默认返回Long.MAX_VALUE
     */
    default long maximumTimeToWait(long currentTimeMs) {
        // 返回最大可能的等待时间
        return Long.MAX_VALUE;
    }

    /**
     * 通知请求管理器消费者正在关闭
     * 用于准备执行适当的关闭操作
     * 
     * 实现细节：
     * - 标记关闭状态
     * - 准备清理资源
     * - 取消待处理的请求
     * - 默认实现为空
     */
    default void signalClose() { }
}
