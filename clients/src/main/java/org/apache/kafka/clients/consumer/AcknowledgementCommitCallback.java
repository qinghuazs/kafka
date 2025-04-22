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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;
import java.util.Set;

/**
 * 消息确认完成回调接口
 * 用户可以实现此接口来处理消息确认完成时的自定义操作
 * 回调方法可能在任何调用{@link ShareConsumer#poll(java.time.Duration)}的线程中执行
 * 
 * 设计原理：
 * 1. 提供异步确认机制，避免同步确认带来的阻塞
 * 2. 允许用户在确认完成时执行自定义逻辑
 * 3. 支持批量确认，提高性能
 * 
 * 使用场景：
 * 1. 监控消息确认状态
 * 2. 记录确认失败的消息
 * 3. 实现重试或补偿逻辑
 */
@InterfaceStability.Evolving
public interface AcknowledgementCommitCallback {

    /**
     * 消息确认完成的回调方法
     * 当服务器完成确认请求处理后会调用此方法
     * 
     * 实现建议：
     * 1. 保持实现逻辑轻量级，避免阻塞
     * 2. 注意线程安全，因为可能在不同线程调用
     * 3. 合理处理异常情况
     *
     * @param offsets 本次确认涉及的偏移量映射，key为主题分区，value为偏移量集合
     *
     * @param exception 请求处理过程中抛出的异常，如果确认成功则为null
     * <p><ul>
     * <li> {@link InvalidRecordStateException} - 记录状态无效，可能是重复确认或已过期
     * <li> {@link AuthorizationException} - 没有主题或消费者组的操作权限
     * <li> {@link WakeupException} - 在方法执行前或执行期间调用了{@link KafkaShareConsumer#wakeup()}
     * <li> {@link InterruptException} - 执行线程在方法执行前或执行期间被中断
     * <li> {@link KafkaException} - 其他不可恢复的错误
     * </ul>
     */
    void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception);
}
