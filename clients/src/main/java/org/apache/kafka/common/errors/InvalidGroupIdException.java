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
 * 表示消费者组ID无效的异常
 *
 * 此异常继承自ApiException，当消费者组ID不符合Kafka的要求时抛出。
 * 在Kafka中，消费者组ID是标识消费者组的唯一标识符，必须满足特定的格式要求。
 *
 * 触发场景：
 * 1. 组ID为空或null
 * 2. 组ID包含非法字符（如特殊字符）
 * 3. 组ID长度超过允许的最大长度
 * 4. 组ID不符合Kafka的命名规范
 *
 * 影响：
 * 1. 消费者无法加入消费者组
 * 2. 组协调器无法正确管理消费者组
 * 3. 可能导致消费者组操作（如提交偏移量）失败
 */
public class InvalidGroupIdException extends ApiException {
    
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息和原因创建InvalidGroupIdException实例
     *
     * @param message 详细描述异常原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public InvalidGroupIdException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的错误消息创建InvalidGroupIdException实例
     *
     * @param message 详细描述异常原因的错误消息
     */
    public InvalidGroupIdException(String message) {
        super(message);
    }
}
