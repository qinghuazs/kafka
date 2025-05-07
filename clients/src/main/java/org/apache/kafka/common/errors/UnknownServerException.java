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
 * 未知服务器异常
 * 
 * 当服务器端发生了客户端无法识别的错误时抛出此异常。这通常是一个意外的错误情况。
 * 
 * 应用场景：
 * 1. 服务器端发生了未预期的内部错误
 * 2. 新版本服务器返回了旧版本客户端无法识别的错误码
 * 3. 服务器端的错误无法映射到具体的客户端异常类型
 * 
 * 错误处理：
 * 1. 检查服务器日志以确定具体错误原因
 * 2. 考虑是否存在客户端和服务器版本不兼容的问题
 * 3. 可能需要升级客户端以支持新的错误类型
 * 4. 作为通用的错误处理机制，捕获未知的服务器端错误
 */
public class UnknownServerException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 创建一个未知服务器异常
     */
    public UnknownServerException() {
    }

    /**
     * 创建一个未知服务器异常
     * 
     * @param message 异常描述信息
     */
    public UnknownServerException(String message) {
        super(message);
    }

    /**
     * 创建一个未知服务器异常
     * 
     * @param cause 导致此异常的原始异常
     */
    public UnknownServerException(Throwable cause) {
        super(cause);
    }

    /**
     * 创建一个未知服务器异常
     * 
     * @param message 异常描述信息
     * @param cause 导致此异常的原始异常
     */
    public UnknownServerException(String message, Throwable cause) {
        super(message, cause);
    }

}
