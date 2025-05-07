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
 * 当消费者组成员尝试加入组但未提供必需的成员ID时抛出此异常
 *
 * 应用场景：
 * 1. 消费者组成员首次加入组时未能生成或提供有效的成员ID
 * 2. 在消费者组重平衡过程中，成员的ID信息丢失或无效
 * 3. 当使用静态成员机制时，消费者未正确配置group.instance.id
 *
 * 设计考虑：
 * 1. 继承自ApiException，用于处理消费者组成员身份识别相关的错误
 * 2. 提供序列化支持，确保异常信息可以在网络中传递
 * 3. 包含详细的错误信息和原因，便于诊断和处理成员ID相关问题
 */
public class MemberIdRequiredException extends ApiException {

    private static final long serialVersionUID = 1L;

    public MemberIdRequiredException(String message) {
        super(message);
    }

    public MemberIdRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
