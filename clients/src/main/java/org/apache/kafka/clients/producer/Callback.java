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
package org.apache.kafka.clients.producer;

/**
 * 用户可以实现的回调接口，用于在消息发送请求完成时执行代码。
 * 该回调通常在后台I/O线程中执行，因此实现时应保持代码简洁高效。
 * 
 * 重要说明：
 * 1. 线程安全性：由于回调在I/O线程中执行，实现时需要注意线程安全问题
 * 2. 性能考虑：回调逻辑应该快速完成，避免复杂的计算或阻塞操作
 * 3. 错误处理：建议在回调中妥善处理异常，避免影响I/O线程的正常运行
 * 4. 使用场景：适用于异步获取发送结果、记录日志、触发后续操作等场景
 */
public interface Callback {

    /**
     * 用户实现的回调方法，用于异步处理消息发送完成事件。当消息被服务器确认接收后，该方法将被调用。
     * 
     * 执行时机：
     * 1. 消息发送成功：服务器确认接收后调用，exception为null，metadata包含实际的分区和偏移量信息
     * 2. 消息发送失败：发生异常时调用，exception包含具体错误信息，metadata所有字段将被设置为-1
     * 
     * 元数据说明：
     * - 正常情况：metadata包含消息的实际分区(partition)和偏移量(offset)信息
     * - 异常情况：所有字段值为-1
     * - 分区分配失败：topicPartition将被设置为-1
     *
     * @param metadata 已发送消息的元数据（包含分区和偏移量信息）。
     *                 如果发生错误，将返回所有字段值均为-1的空元数据。
     * @param exception 消息处理过程中抛出的异常。如果发送成功则为null。
     *                 可能抛出的异常包括：
     *                 <p>
     *                 不可重试异常（致命错误，消息永远不会被发送）：
     *                 <ul>
     *                     <li>{@link org.apache.kafka.common.errors.InvalidTopicException InvalidTopicException} - 主题名称无效
     *                     <li>{@link org.apache.kafka.common.errors.OffsetMetadataTooLarge OffsetMetadataTooLarge} - 偏移量元数据过大
     *                     <li>{@link org.apache.kafka.common.errors.RecordBatchTooLargeException RecordBatchTooLargeException} - 消息批次过大
     *                     <li>{@link org.apache.kafka.common.errors.RecordTooLargeException RecordTooLargeException} - 单条消息过大
     *                     <li>{@link org.apache.kafka.common.errors.UnknownServerException UnknownServerException} - 未知服务器错误
     *                     <li>{@link org.apache.kafka.common.errors.UnknownProducerIdException UnknownProducerIdException} - 生产者ID未知
     *                     <li>{@link org.apache.kafka.common.errors.InvalidProducerEpochException InvalidProducerEpochException} - 生产者Epoch无效
     *                     <li>{@link org.apache.kafka.common.errors.AuthenticationException AuthenticationException} - 认证失败
     *                     <li>{@link org.apache.kafka.common.errors.AuthorizationException AuthorizationException} - 授权失败
     *                 </ul>
     *                 可重试异常（临时性错误，可通过增加重试次数解决）：
     *                 <ul>
     *                     <li>{@link org.apache.kafka.common.errors.CorruptRecordException CorruptRecordException} - 消息损坏
     *                     <li>{@link org.apache.kafka.common.errors.InvalidMetadataException InvalidMetadataException} - 元数据无效
     *                     <li>{@link org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException NotEnoughReplicasAfterAppendException} - 写入后副本数不足
     *                     <li>{@link org.apache.kafka.common.errors.NotEnoughReplicasException NotEnoughReplicasException} - 副本数不足
     *                     <li>{@link org.apache.kafka.common.errors.OffsetOutOfRangeException OffsetOutOfRangeException} - 偏移量超出范围
     *                     <li>{@link org.apache.kafka.common.errors.TimeoutException TimeoutException} - 操作超时
     *                     <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException UnknownTopicOrPartitionException} - 未知的主题或分区
     *                     <li>{@link org.apache.kafka.clients.producer.BufferExhaustedException BufferExhaustedException} - 缓冲区耗尽
     *                 </ul>
     */
    void onCompletion(RecordMetadata metadata, Exception exception);
}
