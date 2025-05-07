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
 * 当请求访问一个在Broker上不存在的日志目录时抛出此异常
 *
 * 应用场景：
 * 1. 当客户端或管理工具请求访问特定的日志目录，但该目录在Broker上已被删除或从未存在时
 * 2. 当Broker配置的日志目录路径无效或不可访问时
 * 3. 在执行日志目录相关的管理操作（如清理、迁移）时，目标目录不存在的情况
 *
 * 设计考虑：
 * 1. 继承自ApiException，表明这是一个API层面的异常，通常需要客户端进行处理
 * 2. 提供了序列化支持，便于在网络传输时的异常传递
 * 3. 支持带有详细错误信息的构造，有助于问题诊断
 */
public class LogDirNotFoundException extends ApiException {

    private static final long serialVersionUID = 1L;

    public LogDirNotFoundException(String message) {
        super(message);
    }

    public LogDirNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogDirNotFoundException(Throwable cause) {
        super(cause);
    }
}
