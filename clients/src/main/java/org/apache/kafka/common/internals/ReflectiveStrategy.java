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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 用于在运行时通过反射访问方法的工具类，无需在编译时就确定要访问的方法。
 * 这个类主要用于以下场景：
 * 1. 需要动态调用方法时，比如插件系统或扩展机制
 * 2. 处理可选依赖，在运行时根据依赖是否存在来决定调用方法
 * 3. 跨版本兼容性处理，处理不同版本API的调用
 */
class ReflectiveStrategy {

    /**
     * 通过反射调用指定对象的方法
     * 
     * @param method 要调用的方法对象
     * @param obj 调用方法的目标对象
     * @param args 方法的参数
     * @return 方法调用的返回值
     * @throws UnsupportedOperationException 如果方法访问出错
     * @throws RuntimeException 如果方法调用过程中发生异常
     */
    static Object invoke(Method method, Object obj, Object... args) {
        try {
            // 通过反射调用方法
            return method.invoke(obj, args);
        } catch (IllegalAccessException e) {
            // 如果方法访问权限不正确，抛出不支持操作异常
            throw new UnsupportedOperationException(e);
        } catch (InvocationTargetException e) {
            // 获取目标方法抛出的实际异常
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // 如果是运行时异常，直接抛出
                throw (RuntimeException) cause;
            } else {
                // 其他异常包装成运行时异常抛出
                throw new RuntimeException(cause);
            }
        }
    }

    /**
     * 通过反射调用指定对象的方法，并对异常进行类型检查
     * 这个方法主要用于需要处理特定类型检查异常的场景
     * 
     * @param method 要调用的方法对象
     * @param ex 期望捕获的异常类型
     * @param obj 调用方法的目标对象
     * @param args 方法的参数
     * @return 方法调用的返回值
     * @throws T 如果方法抛出了指定类型的异常
     * @throws UnsupportedOperationException 如果方法访问出错
     * @throws RuntimeException 如果方法调用过程中发生其他异常
     */
    static <T extends Exception> Object invokeChecked(Method method, Class<T> ex, Object obj, Object... args) throws T {
        try {
            // 通过反射调用方法
            return method.invoke(obj, args);
        } catch (IllegalAccessException e) {
            // 如果方法访问权限不正确，抛出不支持操作异常
            throw new UnsupportedOperationException(e);
        } catch (InvocationTargetException e) {
            // 获取目标方法抛出的实际异常
            Throwable cause = e.getCause();
            if (ex.isInstance(cause)) {
                // 如果异常类型匹配期望的类型，则转换后抛出
                throw ex.cast(cause);
            } else if (cause instanceof RuntimeException) {
                // 如果是运行时异常，直接抛出
                throw (RuntimeException) cause;
            } else {
                // 其他异常包装成运行时异常抛出
                throw new RuntimeException(cause);
            }
        }
    }

    /**
     * 类加载器接口，用于模拟类加载基础设施
     * 主要用于测试反射操作时，可以通过mock这个接口来控制类加载行为
     */
    interface Loader {
        /**
         * 加载指定名称的类
         * 
         * @param className 要加载的类的全限定名
         * @return 加载的类对象
         * @throws ClassNotFoundException 如果类不存在
         */
        Class<?> loadClass(String className) throws ClassNotFoundException;

        /**
         * 创建一个默认的类加载器实现
         * 使用Class.forName进行类加载，并确保类被初始化
         * 
         * @return 默认的类加载器实现
         */
        static Loader forName() {
            return className -> Class.forName(className, true, Loader.class.getClassLoader());
        }
    }
}
