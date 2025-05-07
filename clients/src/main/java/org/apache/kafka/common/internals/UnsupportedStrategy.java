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

import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/**
 * 这是一个后备策略类，当其他所有安全管理器策略都不可用时使用。
 * <p>该策略的主要目的是改善控制流程，并在异常情况下提供详细的错误信息。
 * 
 * 应用场景：
 * 1. 作为SecurityManagerCompatibility接口实现的最后防线
 * 2. 在无法找到合适的安全管理器实现时提供明确的错误提示
 * 3. 帮助诊断安全管理器相关的配置问题
 * 
 * 设计考虑：
 * 1. 通过组合异常提供完整的错误上下文
 * 2. 为每个不支持的操作提供具体的错误信息
 * 3. 确保在安全管理器功能完全不可用时的优雅降级
 */
class UnsupportedStrategy implements SecurityManagerCompatibility {

    /**
     * 存储第一个导致策略不可用的异常
     * 通常是尝试加载传统安全管理器API时的异常
     */
    private final Throwable e1;

    /**
     * 存储第二个导致策略不可用的异常
     * 通常是尝试加载现代安全管理器API时的异常
     */
    private final Throwable e2;

    /**
     * 构造函数，初始化不支持策略
     * 
     * @param e1 第一个导致策略不可用的异常（通常来自传统API）
     * @param e2 第二个导致策略不可用的异常（通常来自现代API）
     */
    UnsupportedStrategy(Throwable e1, Throwable e2) {
        this.e1 = e1;
        this.e2 = e2;
    }

    /**
     * 创建一个包含详细错误信息的UnsupportedOperationException
     * 该方法将两个初始化异常作为被抑制的异常添加到新创建的异常中
     * 
     * @param message 描述不支持操作的具体原因
     * @return 返回一个包含完整错误上下文的UnsupportedOperationException
     */
    private UnsupportedOperationException createException(String message) {
        // 创建主异常对象
        UnsupportedOperationException e = new UnsupportedOperationException(message);
        // 添加导致策略不可用的两个异常作为被抑制的异常
        e.addSuppressed(e1);
        e.addSuppressed(e2);
        return e;
    }

    /**
     * 实现接口的doPrivileged方法
     * 该实现总是抛出异常，表示无法找到合适的特权操作实现
     * 
     * @param action 要执行的特权操作（在此实现中不会被执行）
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        throw createException("Unable to find suitable AccessController#doPrivileged implementation");
    }

    /**
     * 实现接口的current方法
     * 该实现总是抛出异常，表示无法找到合适的获取当前Subject的实现
     * 
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public Subject current() {
        throw createException("Unable to find suitable Subject#getCurrent or Subject#current implementation");
    }

    /**
     * 实现接口的callAs方法
     * 该实现总是抛出异常，表示无法找到合适的Subject执行上下文切换实现
     * 
     * @param subject 要执行操作的Subject身份（在此实现中不会被使用）
     * @param action 要执行的操作（在此实现中不会被执行）
     * @return 永远不会返回，总是抛出异常
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
        throw createException("Unable to find suitable Subject#doAs or Subject#callAs implementation");
    }
}
