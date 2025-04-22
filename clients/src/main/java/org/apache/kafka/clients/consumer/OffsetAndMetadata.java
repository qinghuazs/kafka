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

import org.apache.kafka.common.requests.OffsetFetchResponse;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Kafka偏移量提交API允许用户在提交偏移量时提供额外的元数据信息（以字符串形式）。
 * 这些元数据可以用来存储有用的信息，例如：
 * - 记录是哪个节点提交的偏移量
 * - 提交发生的具体时间
 * - 其他自定义的跟踪信息
 * 
 * 该类实现了Serializable接口，支持序列化，便于在网络传输和持久化存储。
 */
public class OffsetAndMetadata implements Serializable {
    // 序列化版本号，用于版本兼容性
    private static final long serialVersionUID = 2019555404968089681L;

    // 消费者提交的偏移量值，表示消费到的位置
    private final long offset;
    
    // 与偏移量关联的元数据信息，用户可以存储自定义的描述信息
    private final String metadata;

    // 领导者纪元(Leader Epoch)，用于检测日志截断
    // 使用null表示没有领导者纪元信息，这样可以简化序列化过程
    // 对于旧版本的序列化对象，该字段会自动初始化为null
    private final Integer leaderEpoch;

    /**
     * 构造一个新的OffsetAndMetadata对象，用于通过{@link KafkaConsumer}提交偏移量。
     * 这是最完整的构造函数，允许指定所有相关属性。
     *
     * @param offset 要提交的偏移量值，必须是非负数
     * @param leaderEpoch 上一条已消费记录的领导者纪元（可选）
     * @param metadata 与偏移量关联的元数据信息，可以为null
     */
    public OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch, String metadata) {
        // 检查偏移量是否为负数，如果是则抛出异常
        if (offset < 0)
            throw new IllegalArgumentException("Invalid negative offset");

        // 设置偏移量
        this.offset = offset;
        // 如果没有提供leaderEpoch，则使用null
        this.leaderEpoch = leaderEpoch.orElse(null);

        // 服务器会将null元数据转换为空字符串
        // 为了保持一致性，客户端这里也将null转换为空字符串
        if (metadata == null)
            this.metadata = OffsetFetchResponse.NO_METADATA;
        else
            this.metadata = metadata;
    }

    /**
     * 构造一个新的OffsetAndMetadata对象，用于通过{@link KafkaConsumer}提交偏移量。
     * 这个构造函数不包含领导者纪元信息。
     * 
     * @param offset 要提交的偏移量值
     * @param metadata 与偏移量关联的元数据信息
     */
    public OffsetAndMetadata(long offset, String metadata) {
        // 调用完整的构造函数，领导者纪元设置为空
        this(offset, Optional.empty(), metadata);
    }

    /**
     * 构造一个新的OffsetAndMetadata对象，用于通过{@link KafkaConsumer}提交偏移量。
     * 这是最简单的构造函数，只需要提供偏移量值，元数据将为空字符串。
     * 
     * @param offset 要提交的偏移量值
     */
    public OffsetAndMetadata(long offset) {
        // 调用两参数构造函数，元数据设置为空字符串
        this(offset, "");
    }

    /**
     * 获取已提交的偏移量值
     * @return 偏移量值
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取与偏移量关联的元数据信息
     * @return 元数据字符串
     */
    public String metadata() {
        return metadata;
    }

    /**
     * 获取上一条已消费记录的领导者纪元（如果已知）。
     * 领导者纪元用于检测日志截断：
     * 如果存在一个比当前纪元更大的领导者纪元，并且它的起始偏移量早于已提交的偏移量，
     * 就说明发生了日志截断。
     *
     * @return 如果已知则返回领导者纪元，否则返回空
     */
    public Optional<Integer> leaderEpoch() {
        // 如果leaderEpoch为null或负数，表示没有有效的领导者纪元信息
        if (leaderEpoch == null || leaderEpoch < 0)
            return Optional.empty();
        return Optional.of(leaderEpoch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetAndMetadata that = (OffsetAndMetadata) o;
        return offset == that.offset &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, metadata, leaderEpoch);
    }

    @Override
    public String toString() {
        return "OffsetAndMetadata{" +
                "offset=" + offset +
                ", leaderEpoch=" + leaderEpoch +
                ", metadata='" + metadata + '\'' +
                '}';
    }

}
