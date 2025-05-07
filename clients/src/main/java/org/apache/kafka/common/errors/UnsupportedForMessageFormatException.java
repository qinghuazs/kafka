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
 * 当请求的功能不被当前消息格式版本支持时抛出此异常。
 * 
 * 应用场景：
 * 1. 幂等性生产者：当主题使用的消息格式版本低于0.11.0.0时，不支持幂等性特性
 * 2. 事务性生产者：需要较新的消息格式版本支持
 * 3. 新特性与旧版本格式不兼容：某些新增的消息属性或功能可能需要特定的最低消息格式版本
 * 
 * 消息格式版本是Kafka中的重要概念，它决定了消息的存储格式和支持的特性。
 * 升级消息格式版本可能需要考虑兼容性和性能影响。
 */
public class UnsupportedForMessageFormatException extends ApiException {
    
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述不支持的消息格式的错误消息
     */
    public UnsupportedForMessageFormatException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述不支持的消息格式的错误消息
     * @param cause 导致此异常的原始异常
     */
    public UnsupportedForMessageFormatException(String message, Throwable cause) {
        super(message, cause);
    }

}
