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
 * 当客户端需要的请求API或版本不被broker支持时抛出此异常。
 * 
 * 这通常是一个致命错误，因为Kafka客户端会根据需要自动降级请求版本，
 * 除非在旧版本中某些必需的特性不可用。
 * 
 * 应用场景：
 * 1. 幂等性生产者：当启用幂等性时，由于不支持降级到弱语义，此错误是致命的
 * 2. 消费者API：例如在使用KafkaConsumer.offsetsForTimes()时，如果broker不支持此API，
 *    可以回退到替代逻辑来设置消费者位置
 * 3. 新特性依赖：当使用的新特性在旧版本broker中不可用时
 * 
 * 处理方式：
 * 1. 致命错误通常只能通过关闭客户端实例来处理
 * 2. 某些情况下，可以在不依赖底层特性的情况下继续运行
 * 3. 建议确保客户端和broker版本的兼容性
 */
public class UnsupportedVersionException extends ApiException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述版本不支持的错误消息
     * @param cause 导致此异常的原始异常
     */
    public UnsupportedVersionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述版本不支持的错误消息
     */
    public UnsupportedVersionException(String message) {
        super(message);
    }
}
