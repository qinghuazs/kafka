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
 * 这是一个兼容性接口，用于同时支持有SecurityManager和无SecurityManager的JRE环境。
 * <p>用户应该通过调用{@link #get()}获取单例实例，然后使用实例方法
 * {@link #doPrivileged(PrivilegedAction)}, {@link #current()}, 和 {@link #callAs(Subject, Callable)}。
 * <p>这个类的设计动机和预期行为在以下KIP中定义：
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1006%3A+Remove+SecurityManager+Support">KIP-1006</a>
 * 
 * 主要应用场景：
 * 1. 在需要执行特权操作时，提供统一的安全检查机制
 * 2. 在不同JRE版本间提供一致的安全管理接口
 * 3. 支持在移除SecurityManager后的平滑迁移
 */
public interface SecurityManagerCompatibility {

    /**
     * 获取当前JRE环境下可用的接口实现
     * 
     * @return 返回符合当前JRE功能的接口实现实例
     */
    static SecurityManagerCompatibility get() {
        return CompositeStrategy.INSTANCE;
    }

    /**
     * 使用启用的特权执行指定的{@code PrivilegedAction}操作。
     * 该操作将使用调用者保护域拥有的<i>所有</i>权限来执行。
     *
     * <p> 如果action的{@code run}方法抛出（未检查的）异常，
     * 该异常将通过此方法传播。
     *
     * <p> 注意，在执行操作时，与当前AccessControlContext关联的
     * 任何DomainCombiner都将被忽略。
     *
     * @param <T> PrivilegedAction的{@code run}方法返回值的类型
     *
     * @param action 要执行的特权操作
     *
     * @return action的{@code run}方法的返回值
     *
     * @exception NullPointerException 如果action为{@code null}
     * @see java.security.AccessController#doPrivileged(PrivilegedAction)
     */
    <T> T doPrivileged(PrivilegedAction<T> action);

    /**
     * 返回当前的Subject对象。
     * <p>
     * 当前Subject是通过{@link #callAs}方法安装的。
     * 当调用{@code callAs(subject, action)}时，{@code action}将以
     * {@code subject}作为其当前Subject执行，可以通过此方法获取。
     * 在{@code action}执行完成后，当前Subject会重置为之前的值。
     * 在第一次调用{@code callAs()}之前，当前Subject为{@code null}。
     *
     * @return 当前Subject，如果当前Subject未安装或被设置为{@code null}，
     *         则返回{@code null}
     * @see #callAs(Subject, Callable)
     * @see Subject#current()
     * @see Subject#callAs(Subject, Callable)
     */
    Subject current();

    /**
     * 以指定的{@code subject}作为当前Subject来执行{@code Callable}。
     * 这个方法主要用于在特定安全上下文中执行代码。
     *
     * @param subject 指定的{@code Subject}，代码将以此身份运行。
     *                此参数可以为{@code null}。
     * @param action 要执行的代码，将以{@code subject}作为其当前Subject。
     *               不能为{@code null}。
     * @param <T> {@code action}的{@code call}方法返回值的类型
     * @return {@code action}的{@code call}方法的返回值
     * @throws NullPointerException 如果{@code action}为{@code null}
     * @throws CompletionException 如果{@code action.call()}抛出异常。
     *      {@code CompletionException}的cause将被设置为
     *      {@code action.call()}抛出的异常。
     * @see #current()
     * @see Subject#current()
     * @see Subject#callAs(Subject, Callable)
     */
    <T> T callAs(Subject subject, Callable<T> action) throws CompletionException;
}
