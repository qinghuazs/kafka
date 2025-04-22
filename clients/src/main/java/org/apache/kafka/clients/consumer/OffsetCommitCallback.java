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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * 偏移量提交回调接口，用户可以实现此接口来处理偏移量提交请求完成时的自定义操作。
 * 注意：此回调可能在任何执行 {@link Consumer#poll(java.time.Duration) poll()} 的线程中被调用，
 * 因此实现时需要考虑线程安全性。
 */
public interface OffsetCommitCallback {

    /**
     * 用户实现的回调方法，用于异步处理偏移量提交请求的完成事件。
     * 当服务器确认提交请求后，此方法将被调用。
     * 
     * @param offsets 包含此次回调涉及的主题分区偏移量和相关元数据的映射
     *               Map的键为TopicPartition（主题分区），值为OffsetAndMetadata（偏移量和元数据）
     * @param exception 请求处理过程中抛出的异常，如果提交成功完成则为null
     *                 通过检查此参数可以判断提交是否成功
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 
     *         当提交失败且无法重试时抛出。这种情况通常发生在：
     *         1. 使用 {@link KafkaConsumer#subscribe(Collection)} 进行自动组管理时
     *         2. 存在使用相同groupId的活跃消费组且正在使用组管理时
     * 
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException 
     *         当提交失败是由于正在进行重平衡时抛出。
     *         在这种情况下，可以等待重平衡完成后通过 {@link KafkaConsumer#poll(Duration)} 调用重试提交
     * 
     * @throws org.apache.kafka.common.errors.WakeupException 
     *         当在此方法调用之前或期间调用了 {@link KafkaConsumer#wakeup()} 时抛出
     * 
     * @throws org.apache.kafka.common.errors.InterruptException 
     *         当调用线程在此方法调用之前或期间被中断时抛出
     * 
     * @throws org.apache.kafka.common.errors.AuthorizationException 
     *         当没有主题或配置的groupId的授权时抛出
     *         详细信息请查看异常描述
     * 
     * @throws org.apache.kafka.common.KafkaException 
     *         对于其他不可恢复的错误时抛出，例如：
     *         1. 偏移量元数据太大
     *         2. 提交的偏移量无效
     */
    void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception);
}
