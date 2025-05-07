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

import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.MessageUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 确认批次类
 * 用于管理Kafka消费者的消息确认信息，包括偏移量范围和确认类型
 */
public class AcknowledgementBatch {
    /**
     * 批次中第一条消息的偏移量
     */
    private long firstOffset;
    
    /**
     * 批次中最后一条消息的偏移量
     */
    private long lastOffset;
    
    /**
     * 确认类型列表
     * 存储每条消息的确认类型
     */
    private List<Byte> acknowledgeTypes;

    /**
     * 默认构造函数
     * 初始化一个空的确认批次
     */
    public AcknowledgementBatch() {
        // 初始化第一个偏移量为0
        this.firstOffset = 0L;
        // 初始化最后一个偏移量为0
        this.lastOffset = 0L;
        // 初始化一个空的确认类型列表
        this.acknowledgeTypes = new ArrayList<>(0);
    }

    /**
     * 获取批次中第一条消息的偏移量
     *
     * @return 第一条消息的偏移量
     */
    public long firstOffset() {
        // 返回第一个偏移量
        return this.firstOffset;
    }

    /**
     * 获取批次中最后一条消息的偏移量
     *
     * @return 最后一条消息的偏移量
     */
    public long lastOffset() {
        // 返回最后一个偏移量
        return this.lastOffset;
    }

    /**
     * 获取确认类型列表
     *
     * @return 确认类型列表
     */
    public List<Byte> acknowledgeTypes() {
        // 返回确认类型列表
        return this.acknowledgeTypes;
    }

    /**
     * 设置批次中第一条消息的偏移量
     *
     * @param v 要设置的偏移量值
     * @return 当前对象，支持链式调用
     */
    public AcknowledgementBatch setFirstOffset(long v) {
        // 设置第一个偏移量
        this.firstOffset = v;
        // 返回当前对象以支持链式调用
        return this;
    }

    /**
     * 设置批次中最后一条消息的偏移量
     *
     * @param v 要设置的偏移量值
     * @return 当前对象，支持链式调用
     */
    public AcknowledgementBatch setLastOffset(long v) {
        // 设置最后一个偏移量
        this.lastOffset = v;
        // 返回当前对象以支持链式调用
        return this;
    }

    /**
     * 设置确认类型列表
     *
     * @param v 要设置的确认类型列表
     * @return 当前对象，支持链式调用
     */
    public AcknowledgementBatch setAcknowledgeTypes(List<Byte> v) {
        // 设置确认类型列表
        this.acknowledgeTypes = v;
        // 返回当前对象以支持链式调用
        return this;
    }

    /**
     * 转换为共享确认请求数据格式
     *
     * @return ShareAcknowledgeRequestData.AcknowledgementBatch对象
     */
    public ShareAcknowledgeRequestData.AcknowledgementBatch toShareAcknowledgeRequest() {
        // 创建新的共享确认请求批次对象并设置所有字段
        return new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(firstOffset)
                .setLastOffset(lastOffset)
                .setAcknowledgeTypes(acknowledgeTypes);
    }

    /**
     * 转换为共享获取请求数据格式
     *
     * @return ShareFetchRequestData.AcknowledgementBatch对象
     */
    public ShareFetchRequestData.AcknowledgementBatch toShareFetchRequest() {
        // 创建新的共享获取请求批次对象并设置所有字段
        return new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(firstOffset)
                .setLastOffset(lastOffset)
                .setAcknowledgeTypes(acknowledgeTypes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AcknowledgementBatch)) return false;
        AcknowledgementBatch other = (AcknowledgementBatch) obj;
        if (firstOffset != other.firstOffset) return false;
        if (lastOffset != other.lastOffset) return false;
        if (this.acknowledgeTypes == null) {
            return other.acknowledgeTypes == null;
        } else {
            return this.acknowledgeTypes.equals(other.acknowledgeTypes);
        }
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + ((int) (firstOffset >> 32) ^ (int) firstOffset);
        hashCode = 31 * hashCode + ((int) (lastOffset >> 32) ^ (int) lastOffset);
        hashCode = 31 * hashCode + (acknowledgeTypes == null ? 0 : acknowledgeTypes.hashCode());
        return hashCode;
    }

    @Override
    public String toString() {
        return "AcknowledgementBatch("
                + "firstOffset=" + firstOffset
                + ", lastOffset=" + lastOffset
                + ", acknowledgeTypes=" + MessageUtil.deepToString(acknowledgeTypes.iterator())
                + ")";
    }
}
