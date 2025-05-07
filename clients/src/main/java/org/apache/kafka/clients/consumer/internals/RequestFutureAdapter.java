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
package org.apache.kafka.clients.consumer.internals;

/**
 * 请求Future适配器
 * 用于将一种类型的请求Future适配到另一种类型
 * 
 * 应用场景：
 * 1. 在异步请求链中转换结果类型
 * 2. 处理不同类型的请求响应
 * 3. 构建请求处理管道
 * 4. 支持请求结果的类型转换
 *
 * 设计考虑：
 * 1. 使用泛型支持灵活的类型转换
 * 2. 提供成功和失败的回调处理
 * 3. 抽象类设计允许自定义成功处理逻辑
 * 4. 默认的失败处理机制
 *
 * @param <F> 源类型，需要从这个类型转换
 * @param <T> 目标类型，需要转换到这个类型
 */
public abstract class RequestFutureAdapter<F, T> {

    /**
     * 处理请求成功的回调方法
     * 当请求成功完成时，将源类型的结果转换为目标类型
     * 
     * 应用场景：
     * - 数据类型转换
     * - 结果过滤和处理
     * - 响应格式转换
     * - 数据聚合和转换
     *
     * @param value 源类型的结果值
     * @param future 用于设置转换后结果的RequestFuture
     */
    public abstract void onSuccess(F value, RequestFuture<T> future);

    /**
     * 处理请求失败的回调方法
     * 当请求失败时，将异常传播到目标Future
     * 
     * 实现细节：
     * - 直接将异常传递给目标Future
     * - 使用raise方法传播异常
     * - 保持异常类型不变
     * - 维护错误处理链
     *
     * @param e 发生的运行时异常
     * @param future 用于传播异常的RequestFuture
     */
    public void onFailure(RuntimeException e, RequestFuture<T> future) {
        // 将异常传播到目标Future
        future.raise(e);
    }
}
