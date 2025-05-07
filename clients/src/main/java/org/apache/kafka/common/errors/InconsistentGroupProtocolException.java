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
 * 消费者组协议不一致异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当消费者组内的成员使用不同的分区分配策略时
 * 2. 当新加入的消费者配置了与组内现有成员不兼容的协议时
 * 3. 当组协调器无法为所有成员选择一个共同的协议版本时
 * 
 * 消费者组协议的作用：
 * - 确保组内所有消费者使用相同的分区分配策略
 * - 维护消费者组的一致性和稳定性
 * - 支持消费者组的动态伸缩和再平衡
 */
public class InconsistentGroupProtocolException extends ApiException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述组协议不一致问题的详细信息
     * @param cause 导致此异常的原始异常
     */
    public InconsistentGroupProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述组协议不一致问题的详细信息
     */
    public InconsistentGroupProtocolException(String message) {
        super(message);
    }
}
