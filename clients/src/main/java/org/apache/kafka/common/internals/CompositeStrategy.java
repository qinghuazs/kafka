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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.security.auth.Subject;

/**
 * 这个策略类结合了{@link LegacyStrategy}（传统策略）、{@link ModernStrategy}（现代策略）和
 * {@link UnsupportedStrategy}（不支持策略）的功能，以提供向后兼容的API支持。
 * 该策略的主要目的是在传统API可用且未降级的情况下使用传统API，
 * 当传统API缺失或降级时，自动切换到现代API。
 * 
 * 设计考虑：
 * 1. 优先使用传统API以保持最大兼容性
 * 2. 通过降级机制平滑过渡到现代API
 * 3. 提供完整的错误处理和日志记录
 * 4. 支持在运行时动态切换策略
 */
class CompositeStrategy implements SecurityManagerCompatibility {

    // 用于记录日志的Logger实例
    private static final Logger log = LoggerFactory.getLogger(CompositeStrategy.class);
    // 单例模式，创建CompositeStrategy的全局实例
    static final CompositeStrategy INSTANCE = new CompositeStrategy(ReflectiveStrategy.Loader.forName());

    // 备用策略，用于在主策略不可用时进行降级
    private final SecurityManagerCompatibility fallbackStrategy;
    // 当前活动的策略，使用AtomicReference保证线程安全
    private final AtomicReference<SecurityManagerCompatibility> activeStrategy;

    // 构造函数，用于测试
    CompositeStrategy(ReflectiveStrategy.Loader loader) {
        SecurityManagerCompatibility initial;
        SecurityManagerCompatibility fallback = null;
        try {
            initial = new LegacyStrategy(loader);
            try {
                fallback = new ModernStrategy(loader);
                // This is expected for JRE 18+
                log.debug("Loaded legacy SecurityManager methods, will fall back to modern methods after UnsupportedOperationException");
            } catch (NoSuchMethodException | ClassNotFoundException ex) {
                // This is expected for JRE <= 17
                log.debug("Unable to load modern Subject methods, relying only on legacy methods", ex);
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            try {
                initial = new ModernStrategy(loader);
                // This is expected for JREs after the removal takes place.
                log.debug("Unable to load legacy SecurityManager methods, relying only on modern methods", e);
            } catch (NoSuchMethodException | ClassNotFoundException ex) {
                initial = new UnsupportedStrategy(e, ex);
                // This is not expected in normal use, only in test environments.
                log.error("Unable to load legacy SecurityManager methods", e);
                log.error("Unable to load modern Subject methods", ex);
            }
        }
        Objects.requireNonNull(initial, "initial strategy must be defined");
        activeStrategy = new AtomicReference<>(initial);
        fallbackStrategy = fallback;
    }

    /**
     * 执行安全管理器操作的核心方法
     * 
     * @param action 要执行的安全操作，封装在Function接口中
     * @param <T> 操作返回值的类型参数
     * @return 返回安全操作的执行结果
     * @throws UnsupportedOperationException 当所有可用策略都无法执行操作时抛出
     */
    private <T> T performAction(Function<SecurityManagerCompatibility, T> action) {
        // 获取当前活动的策略
        SecurityManagerCompatibility active = activeStrategy.get();
        try {
            // 尝试使用当前活动策略执行操作
            return action.apply(active);
        } catch (UnsupportedOperationException e) {
            // 如果当前策略执行失败，且存在可用的备用策略，则尝试切换到备用策略
            if (active != fallbackStrategy && fallbackStrategy != null) {
                // 使用CAS操作安全地切换到备用策略
                if (activeStrategy.compareAndSet(active, fallbackStrategy)) {
                    log.debug("检测到传统方法降级，切换到备用策略", e);
                }
                // 使用备用策略重试操作
                return action.apply(fallbackStrategy);
            }
            // 如果已经在使用备用策略，或者没有可用的备用策略，则抛出异常
            throw e;
        }
    }

    /**
     * 执行特权操作
     * 
     * @param action 需要以特权方式执行的操作
     * @param <T> 操作返回值的类型参数
     * @return 返回特权操作的执行结果
     */
    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        // 将特权操作委托给当前活动的策略执行
        return performAction(compatibility -> compatibility.doPrivileged(action));
    }

    /**
     * 获取当前的Subject对象
     * 
     * @return 返回当前的Subject对象
     */
    @Override
    public Subject current() {
        // 委托给当前活动的策略获取Subject
        return performAction(SecurityManagerCompatibility::current);
    }

    /**
     * 以指定的Subject身份执行操作
     * 
     * @param subject 要使用的Subject身份
     * @param action 要执行的操作
     * @param <T> 操作返回值的类型参数
     * @return 返回操作的执行结果
     * @throws CompletionException 当操作执行失败时抛出
     */
    @Override
    public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
        // 将操作委托给当前活动的策略执行
        return performAction(compatibility -> compatibility.callAs(subject, action));
    }
}
