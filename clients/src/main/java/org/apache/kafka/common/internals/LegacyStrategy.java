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

import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/**
 * 该类通过反射实现对已标记为弃用的AccessController和Subject方法的访问。
 * <p>在以下情况下，类的实例化可能会失败：
 * 1. 找不到所需的类或方法
 * 2. 方法存在但无法被调用时会抛出{@link UnsupportedOperationException}
 * <p>该类预计在JRE 8及更高版本中可用，直到这些弃用方法最终被移除。
 * <p>应用场景：
 * 1. 在需要向后兼容的情况下访问安全管理器功能
 * 2. 处理特权操作和Subject上下文切换
 * 3. 在安全敏感的环境中进行权限控制
 */
@SuppressWarnings("unchecked")
class LegacyStrategy implements SecurityManagerCompatibility {

    /**
     * 用于执行特权操作的反射Method对象
     * 对应AccessController.doPrivileged方法
     */
    private final Method doPrivileged;

    /**
     * 用于获取当前访问控制上下文的反射Method对象
     * 对应AccessController.getContext方法
     */
    private final Method getContext;

    /**
     * 用于从访问控制上下文中获取Subject的反射Method对象
     * 对应Subject.getSubject方法
     */
    private final Method getSubject;

    /**
     * 用于以指定Subject身份执行操作的反射Method对象
     * 对应Subject.doAs方法
     */
    private final Method doAs;

    /**
     * 构造函数，通过反射加载所需的安全相关方法
     * 该构造函数可见性为包级私有，主要用于测试
     *
     * @param loader 用于加载类的加载器
     * @throws ClassNotFoundException 当所需的类无法找到时抛出
     * @throws NoSuchMethodException 当所需的方法无法找到时抛出
     */
    LegacyStrategy(ReflectiveStrategy.Loader loader) throws ClassNotFoundException, NoSuchMethodException {
        // 加载AccessController类并获取其方法
        Class<?> accessController = loader.loadClass("java.security.AccessController");
        doPrivileged = accessController.getDeclaredMethod("doPrivileged", PrivilegedAction.class);
        getContext = accessController.getDeclaredMethod("getContext");

        // 加载AccessControlContext类
        Class<?> accessControlContext = loader.loadClass("java.security.AccessControlContext");
        
        // 加载Subject类并获取其方法
        Class<?> subject = loader.loadClass(Subject.class.getName());
        getSubject = subject.getDeclaredMethod("getSubject", accessControlContext);
        
        // Subject类本身未被弃用或移除，因此可以直接作为参数类型使用
        // 这允许在保持接受Subject实例作为参数的同时模拟方法实现
        doAs = subject.getDeclaredMethod("doAs", Subject.class, PrivilegedExceptionAction.class);
    }

    /**
     * 执行特权操作，使用所有调用者保护域拥有的权限
     * 
     * @param <T> 特权操作返回值的类型
     * @param action 要执行的特权操作
     * @return 特权操作执行的结果
     * @throws NullPointerException 如果action为null
     */
    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        // 通过反射调用AccessController.doPrivileged方法
        return (T) ReflectiveStrategy.invoke(doPrivileged, null, action);
    }

    /**
     * 获取当前的访问控制上下文
     * 
     * @return AccessController.getContext()的结果，类型为AccessControlContext
     */
    private Object getContext() {
        // 通过反射调用AccessController.getContext方法
        return ReflectiveStrategy.invoke(getContext, null);
    }

    /**
     * 从指定的访问控制上下文中获取Subject
     * 
     * @param context 当前的访问控制上下文
     * @return Subject.getSubject(AccessControlContext)的结果
     */
    private Subject getSubject(Object context) {
        // 通过反射调用Subject.getSubject方法
        return (Subject) ReflectiveStrategy.invoke(getSubject, null, context);
    }

    /**
     * 获取当前的Subject
     * 该方法通过获取当前的访问控制上下文，然后从中提取Subject来实现
     * 
     * @return 当前的Subject，如果没有设置则返回null
     */
    @Override
    public Subject current() {
        // 先获取当前的访问控制上下文，然后从中获取Subject
        return getSubject(getContext());
    }

    /**
     * 以指定的Subject身份执行特权异常操作
     * 
     * @param <T> 操作返回值的类型
     * @param subject 要执行操作的Subject身份
     * @param action 要执行的特权异常操作
     * @return Subject.doAs的执行结果
     * @throws PrivilegedActionException 如果特权操作执行失败
     */
    private <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        // 通过反射调用Subject.doAs方法，并处理可能的异常
        return (T) ReflectiveStrategy.invokeChecked(doAs, PrivilegedActionException.class, null, subject, action);
    }

    /**
     * 以指定的Subject身份执行可调用任务
     * 该方法将Callable包装为PrivilegedExceptionAction并执行
     * 
     * @param <T> 任务返回值的类型
     * @param subject 要执行任务的Subject身份
     * @param callable 要执行的任务
     * @return 任务的执行结果
     * @throws CompletionException 如果任务执行失败
     */
    @Override
    public <T> T callAs(Subject subject, Callable<T> callable) throws CompletionException {
        try {
            // 将Callable转换为PrivilegedExceptionAction并执行
            return doAs(subject, callable::call);
        } catch (PrivilegedActionException e) {
            // 将异常包装为CompletionException并重新抛出
            throw new CompletionException(e.getCause());
        }
    }
}
