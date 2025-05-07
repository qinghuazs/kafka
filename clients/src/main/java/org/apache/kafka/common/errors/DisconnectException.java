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
 * 连接断开异常
 * 
 * 该异常在服务器在请求完成之前断开连接时抛出。这种情况可能发生在：
 * 1. 网络连接不稳定或超时
 * 2. 服务器进行维护或重启
 * 3. 服务器负载过高，主动断开连接
 * 4. 客户端与服务器之间的网络问题
 * 
 * 由于继承自RetriableException，表示这是一个可重试的异常。
 * 当发生此异常时，客户端通常会：
 * - 自动重新建立连接
 * - 重试之前失败的请求
 * - 如果多次重试失败，才会向上层报告错误
 */
public class DisconnectException extends RetriableException {
    /**
     * 预定义的DisconnectException实例
     * 用于避免频繁创建新的异常对象，提高性能
     */
    public static final DisconnectException INSTANCE = new DisconnectException();

    private static final long serialVersionUID = 1L;

    /**
     * 无参构造函数
     * 创建一个没有错误消息和原因的连接断开异常
     */
    public DisconnectException() {
        super();
    }

    /**
     * 构造函数
     * 
     * @param message 异常消息，描述连接断开的具体原因
     * @param cause 导致连接断开的原始异常
     */
    public DisconnectException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 构造函数
     * 
     * @param message 异常消息，描述连接断开的具体原因
     */
    public DisconnectException(String message) {
        super(message);
    }

    /**
     * 构造函数
     * 
     * @param cause 导致连接断开的原始异常
     */
    public DisconnectException(Throwable cause) {
        super(cause);
    }

}
