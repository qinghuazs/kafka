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
 * 表示消费者请求获取消息的大小无效的异常
 *
 * 此异常继承自ApiException，当消费者在fetch请求中指定的获取大小参数不合法时抛出。
 * 在Kafka中，fetch请求用于消费者从broker获取消息，请求中需要指定最大获取字节数等参数。
 *
 * 触发场景：
 * 1. 请求的获取大小小于系统允许的最小值
 * 2. 请求的获取大小超过系统允许的最大值
 * 3. 请求的获取大小超过消费者客户端的内存限制
 * 4. 获取大小参数设置为负数或其他非法值
 *
 * 影响：
 * 1. 消费者无法从broker获取消息
 * 2. 可能导致消费延迟
 * 3. 需要调整消费者配置后重试
 *
 * 最佳实践：
 * 1. 根据消息大小和消费能力合理设置fetch.max.bytes参数
 * 2. 考虑broker端message.max.bytes等相关配置的限制
 * 3. 确保设置的获取大小不会导致内存压力
 */
public class InvalidFetchSizeException extends ApiException {

    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息创建InvalidFetchSizeException实例
     *
     * @param message 详细描述异常原因的错误消息
     */
    public InvalidFetchSizeException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因创建InvalidFetchSizeException实例
     *
     * @param message 详细描述异常原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public InvalidFetchSizeException(String message, Throwable cause) {
        super(message, cause);
    }

}
