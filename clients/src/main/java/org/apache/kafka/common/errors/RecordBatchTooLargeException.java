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
package org.apache.kafka.common.errors;

/**
 * 记录批次过大异常
 * 
 * 当一个记录批次（RecordBatch）的大小超过了Kafka允许的最大限制时抛出此异常。
 * 记录批次大小限制的主要原因：
 * 1. 确保消息能够高效地在磁盘上存储和传输
 * 2. 防止单个批次消耗过多的内存资源
 * 3. 维护集群的稳定性和性能
 * 
 * 可能的触发场景：
 * - 生产者尝试发送过大的消息批次
 * - 压缩后的消息批次超出大小限制
 * - 消息批次中包含过多的记录
 * 
 * 处理建议：
 * - 减小单个消息的大小
 * - 调整批次大小配置（batch.size）
 * - 考虑将大消息拆分成多个小消息
 */
public class RecordBatchTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 默认构造函数
     */
    public RecordBatchTooLargeException() {
        super();
    }

    /**
     * 构造函数
     *
     * @param message 异常消息，描述记录批次过大的具体原因
     * @param cause 导致此异常的原始异常
     */
    public RecordBatchTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 构造函数
     *
     * @param message 异常消息，描述记录批次过大的具体原因
     */
    public RecordBatchTooLargeException(String message) {
        super(message);
    }

    /**
     * 构造函数
     *
     * @param cause 导致此异常的原始异常
     */
    public RecordBatchTooLargeException(Throwable cause) {
        super(cause);
    }

}
