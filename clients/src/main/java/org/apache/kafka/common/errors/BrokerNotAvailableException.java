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
 * Broker不可用异常类
 * 当尝试与不可用的Broker通信时抛出此异常。
 * 
 * 应用场景：
 * 1. 网络问题：Broker网络连接失败
 * 2. 服务状态：Broker服务不可用
 * 3. 故障转移：需要进行Broker故障转移
 * 4. 健康检查：检测Broker的可用性
 *
 * 设计考虑：
 * 1. 继承性：继承自ApiException以统一异常处理
 * 2. 序列化：支持异常的序列化
 * 3. 错误信息：提供详细的错误描述
 * 4. 异常链：支持异常原因的传递
 */
public class BrokerNotAvailableException extends ApiException {

    /**
     * 序列化版本ID
     * 用于确保序列化的兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用错误消息构造Broker不可用异常
     *
     * @param message 描述Broker不可用原因的消息
     */
    public BrokerNotAvailableException(String message) {
        // 调用父类构造函数，传递错误消息
        super(message);
    }

    /**
     * 使用错误消息和原因构造Broker不可用异常
     *
     * @param message 描述Broker不可用原因的消息
     * @param cause 导致Broker不可用的原始异常
     */
    public BrokerNotAvailableException(String message, Throwable cause) {
        // 调用父类构造函数，传递错误消息和原因
        super(message, cause);
    }
}
