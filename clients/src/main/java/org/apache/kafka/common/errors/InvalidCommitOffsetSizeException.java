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
 * 表示提交的偏移量大小无效的异常
 *
 * 此异常继承自ApiException，当消费者提交的偏移量数据大小超出限制时抛出。
 * 在Kafka中，消费者需要定期提交已消费消息的偏移量，以记录消费进度。
 *
 * 触发场景：
 * 1. 单次提交的偏移量数据量过大
 * 2. 批量提交的分区数超出限制
 * 3. 提交请求的总大小超过broker配置的限制
 * 4. 元数据（如自定义元数据）大小超出限制
 *
 * 影响：
 * 1. 偏移量提交失败
 * 2. 可能导致消息重复消费
 * 3. 影响消费者组的进度管理
 *
 * 最佳实践：
 * 1. 合理控制单次提交的偏移量数据量
 * 2. 适当调整提交频率
 * 3. 必要时分批提交大量偏移量
 * 4. 控制自定义元数据的大小
 */
public class InvalidCommitOffsetSizeException extends ApiException {
    
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息和原因创建InvalidCommitOffsetSizeException实例
     *
     * @param message 详细描述异常原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public InvalidCommitOffsetSizeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的错误消息创建InvalidCommitOffsetSizeException实例
     *
     * @param message 详细描述异常原因的错误消息
     */
    public InvalidCommitOffsetSizeException(String message) {
        super(message);
    }
}
