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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 偏移量提交回调调用器
 * 这个工具类帮助应用线程调用用户注册的{@link OffsetCommitCallback}和
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor}。
 * 实现方式是让后台线程在未来完成时向调用器注册{@link OffsetCommitCallbackTask}，
 * 并在用户轮询/提交/关闭消费者时执行回调。
 *
 * 应用场景：
 * 1. 异步提交偏移量后的回调处理
 * 2. 消费者拦截器的执行管理
 * 3. 用户自定义回调的处理
 * 4. 确保回调在正确的线程上下文中执行
 */
public class OffsetCommitCallbackInvoker {
    /**
     * 消费者拦截器集合
     * 用于在偏移量提交时执行拦截器逻辑
     */
    private final ConsumerInterceptors<?, ?> interceptors;

    /**
     * 构造函数
     * 初始化回调调用器
     *
     * @param interceptors 消费者拦截器集合
     */
    OffsetCommitCallbackInvoker(ConsumerInterceptors<?, ?> interceptors) {
        // 初始化拦截器集合
        this.interceptors = interceptors;
    }

    /**
     * 线程安全的队列，用于存储待执行的用户定义回调和拦截器
     * 使用LinkedBlockingQueue确保线程安全性
     */
    private final BlockingQueue<OffsetCommitCallbackTask> callbackQueue = new LinkedBlockingQueue<>();

    /**
     * 将拦截器调用任务加入队列
     * 当拦截器集合非空时，创建并添加拦截器执行任务
     *
     * @param offsets 主题分区与偏移量元数据的映射
     */
    public void enqueueInterceptorInvocation(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 检查拦截器集合是否为空
        if (!interceptors.isEmpty()) {
            // 创建新的回调任务并添加到队列
            // 使用lambda表达式创建回调，忽略异常参数，只调用拦截器的onCommit方法
            callbackQueue.add(new OffsetCommitCallbackTask(
                (offsetsParam, exception) -> interceptors.onCommit(offsetsParam),
                offsets,
                null
            ));
        }
    }

    /**
     * 将用户回调调用任务加入队列
     * 创建并添加用户定义的回调执行任务
     *
     * @param callback 用户定义的回调函数
     * @param offsets 主题分区与偏移量元数据的映射
     * @param exception 执行过程中的异常，可能为null
     */
    public void enqueueUserCallbackInvocation(final OffsetCommitCallback callback,
                                              final Map<TopicPartition, OffsetAndMetadata> offsets,
                                              final Exception exception) {
        // 创建新的回调任务并添加到队列
        callbackQueue.add(new OffsetCommitCallbackTask(callback, offsets, exception));
    }

    /**
     * 执行所有队列中的回调任务
     * 按照FIFO顺序执行队列中的所有回调
     */
    public void executeCallbacks() {
        // 循环处理队列中的所有任务
        while (!callbackQueue.isEmpty()) {
            // 从队列中获取任务
            OffsetCommitCallbackTask task = callbackQueue.poll();
            // 如果任务不为null，执行回调
            if (task != null) {
                // 调用回调的onComplete方法，传入偏移量和异常
                task.callback.onComplete(task.offsets, task.exception);
            }
        }
    }

    /**
     * 偏移量提交回调任务
     * 封装单个回调任务的所有必要信息
     */
    private static class OffsetCommitCallbackTask {
        /**
         * 主题分区与偏移量元数据的映射
         */
        public final Map<TopicPartition, OffsetAndMetadata> offsets;
        
        /**
         * 执行过程中的异常
         */
        public final Exception exception;
        
        /**
         * 要执行的回调函数
         */
        public final OffsetCommitCallback callback;

        /**
         * 构造函数
         * 创建新的回调任务
         *
         * @param callback 回调函数
         * @param offsets 偏移量映射
         * @param exception 异常信息
         */
        public OffsetCommitCallbackTask(final OffsetCommitCallback callback,
                                        final Map<TopicPartition, OffsetAndMetadata> offsets,
                                        final Exception exception) {
            // 初始化任务的所有字段
            this.offsets = offsets;
            this.exception = exception;
            this.callback = callback;
        }
    }
}