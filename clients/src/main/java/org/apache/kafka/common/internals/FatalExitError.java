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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.utils.Exit;

/**
 * 表示需要退出JVM进程的致命错误。此错误类仅应由服务器或命令行工具使用，客户端永远不应关闭JVM进程。
 * 
 * 此异常应在线程的最高层级被捕获，以确保在调用{@link Exit#exit(int)}时线程不持有任何共享锁。
 * 这样的设计是为了避免在进程退出时可能出现的死锁情况。
 */
public class FatalExitError extends Error {

    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 进程退出状态码
     * 状态码用于表示进程退出的原因，0表示正常退出，非0表示异常退出
     * 在Unix/Linux系统中，状态码范围通常是0-255
     */
    private final int statusCode;

    /**
     * 使用指定的状态码构造FatalExitError
     * 
     * @param statusCode 进程退出状态码，必须是非0值
     * @throws IllegalArgumentException 如果状态码为0则抛出此异常，因为0表示正常退出
     */
    public FatalExitError(int statusCode) {
        // 状态码0通常表示正常退出，而这是一个错误类，所以不允许使用0作为状态码
        if (statusCode == 0)
            throw new IllegalArgumentException("statusCode must not be 0");
        this.statusCode = statusCode;
    }

    /**
     * 使用默认状态码1构造FatalExitError
     * 状态码1通常表示一般性错误导致的异常退出
     */
    public FatalExitError() {
        this(1);
    }

    /**
     * 获取进程退出状态码
     * 
     * @return 非0的进程退出状态码
     */
    public int statusCode() {
        return statusCode;
    }
}
