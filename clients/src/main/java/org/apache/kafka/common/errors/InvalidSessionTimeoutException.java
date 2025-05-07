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
 * 表示会话超时设置无效的异常。
 * 
 * 应用场景：
 * 1. 当客户端配置的会话超时时间小于服务器允许的最小值
 * 2. 当客户端配置的会话超时时间大于服务器允许的最大值
 * 3. 在消费者组协调过程中，如果设置了不合理的会话超时参数
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表示这是一个不可重试的异常
 * 2. 提供两种构造方法，支持带有原因链的异常创建
 * 3. 用于帮助用户快速识别和修正会话超时配置问题
 */
public class InvalidSessionTimeoutException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidSessionTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSessionTimeoutException(String message) {
        super(message);
    }
}
