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
 * 当消费者的代际（Generation）信息与当前消费者组的代际不匹配时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者重平衡后，旧代际的消费者尝试提交偏移量
 * 2. 消费者组成员变更导致代际更新后的冲突处理
 * 3. 检测过期的消费者会话
 * 
 * 设计考虑：
 * 1. 维护消费者组的一致性，确保所有消费者属于同一代际
 * 2. 通过代际机制实现消费者组的成员管理
 * 3. 防止旧代际的消费者干扰当前消费者组的操作
 * 4. 支持消费者组的动态伸缩和故障恢复
 */
public class IllegalGenerationException extends ApiException {
    /**
     * 序列化版本ID
     */
    private static final long serialVersionUID = 1L;

    /**
     * 默认构造函数
     */
    public IllegalGenerationException() {
        super();
    }

    /**
     * 带有错误信息和原因的构造函数
     * @param message 异常描述信息，通常包含期望的代际和实际的代际信息
     * @param cause 导致此异常的原始异常
     */
    public IllegalGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 带有错误信息的构造函数
     * @param message 异常描述信息，说明代际不匹配的具体原因
     */
    public IllegalGenerationException(String message) {
        super(message);
    }

    /**
     * 带有原因的构造函数
     * @param cause 导致此异常的原始异常
     */
    public IllegalGenerationException(Throwable cause) {
        super(cause);
    }
}
