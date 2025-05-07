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

import org.apache.kafka.common.KafkaException;

/**
 * 序列化异常。当生产者进行消息序列化过程中发生错误时抛出此异常。
 * 
 * 应用场景：
 * 1. 消息格式与序列化器不匹配
 * 2. 自定义序列化器实现错误
 * 3. 消息内容不符合序列化规则
 * 4. 序列化过程中出现数据转换错误
 * 
 * 异常特点：
 * 1. 在消息生产过程中的序列化阶段发生
 * 2. 继承自KafkaException，表示Kafka特定的错误
 * 3. 通常表示数据处理层面的问题
 * 
 * 处理机制：
 * 1. 检查消息格式是否正确
 * 2. 验证序列化器配置
 * 3. 确保数据类型转换的正确性
 * 
 * 设计考虑：
 * 1. 提供多个构造方法支持不同的异常信息
 * 2. 允许包含原始异常作为cause
 * 3. 有助于快速定位序列化问题
 */
public class SerializationException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Throwable cause) {
        super(cause);
    }

    public SerializationException() {
        super();
    }

}
