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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/**
 * 该类通过反射实现对Subject类新增方法的访问，用于替代已弃用的方法。
 * <p>应用场景：
 * 1. 在JRE 18及更高版本中使用新的安全管理器API
 * 2. 需要在移除SecurityManager后仍能正常工作的场景
 * 3. 处理Subject相关的权限和身份验证操作
 * 
 * <p>注意事项：
 * 1. 如果找不到所需的类或方法，类的实例化可能会失败
 * 2. 即使找到所有方法，如果操作不被允许执行，方法调用可能会抛出{@link UnsupportedOperationException}
 * 3. 该类设计用于JRE 18及以上版本，这些方法目前没有终止日期
 */
@SuppressWarnings("unchecked")
class ModernStrategy implements SecurityManagerCompatibility {

    /**
     * 用于获取当前Subject的反射Method对象
     * 对应Subject.current()方法
     */
    private final Method current;

    /**
     * 用于以指定Subject身份执行操作的反射Method对象
     * 对应Subject.callAs()方法
     */
    private final Method callAs;

    /**
     * 构造函数，通过反射加载Subject类的新方法
     * 该构造函数可见性为包级私有，主要用于测试
     *
     * @param loader 用于加载类的加载器
     * @throws NoSuchMethodException 当所需的方法无法找到时抛出
     * @throws ClassNotFoundException 当所需的类无法找到时抛出
     */
    ModernStrategy(ReflectiveStrategy.Loader loader) throws NoSuchMethodException, ClassNotFoundException {
        // 加载Subject类
        Class<?> subject = loader.loadClass(Subject.class.getName());
        // 获取current方法的反射对象
        current = subject.getDeclaredMethod("current");
        // Subject类未被弃用或移除，因此可以直接作为参数类型使用
        // 这允许在保持接受Subject实例作为参数的同时模拟方法实现
        callAs = subject.getDeclaredMethod("callAs", Subject.class, Callable.class);
    }

    /**
     * 执行特权操作
     * 在现代实现中，这是一个直接传递操作，不进行额外的权限检查
     * 
     * @param <T> 特权操作返回值的类型
     * @param action 要执行的特权操作
     * @return 特权操作执行的结果
     */
    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        // 这是有意的直接传递，不进行额外的权限检查
        return action.run();
    }

    /**
     * 获取当前的Subject
     * 通过反射调用Subject.current()方法实现
     * 
     * @return 当前的Subject，如果没有设置则返回null
     */
    @Override
    public Subject current() {
        // 通过反射调用Subject.current()方法
        return (Subject) ReflectiveStrategy.invoke(current, null);
    }

    /**
     * 以指定的Subject身份执行可调用任务
     * 通过反射调用Subject.callAs()方法实现
     * 
     * @param <T> 任务返回值的类型
     * @param subject 要执行任务的Subject身份
     * @param action 要执行的任务
     * @return 任务的执行结果
     * @throws CompletionException 如果任务执行失败
     */
    @Override
    public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
        // 通过反射调用Subject.callAs()方法，并处理异常
        return (T) ReflectiveStrategy.invokeChecked(callAs, CompletionException.class, null, subject, action);
    }
}
