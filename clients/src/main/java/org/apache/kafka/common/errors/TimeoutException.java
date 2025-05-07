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
 * 请求超时异常
 * 
 * 该异常表示Kafka客户端的请求操作超过了预设的超时时间。这是一个可重试的异常，
 * 通常是由于网络延迟、服务端负载过高或客户端配置不当导致。
 * 
 * 常见场景：
 * 1. 生产者发送消息超时
 * 2. 消费者拉取消息超时
 * 3. 管理操作（如创建主题）超时
 * 4. 元数据请求超时
 * 
 * 处理建议：
 * - 检查网络连接状况
 * - 适当调整超时时间配置
 * - 评估服务端负载情况
 * - 考虑是否需要增加重试次数
 * - 在必要时添加熔断机制
 */
public class TimeoutException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public TimeoutException() {
        super();
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(Throwable cause) {
        super(cause);
    }

}
