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
 * 请求Future监听器接口
 * 用于挂钩到RequestFuture的完成状态
 * 
 * 应用场景：
 * 1. 异步请求完成通知
 * 2. 请求结果的回调处理
 * 3. 错误处理和恢复
 * 4. 请求链的构建
 * 
 * 设计考虑：
 * 1. 使用泛型支持不同类型的结果处理
 * 2. 分离成功和失败的处理逻辑
 * 3. 简单的接口设计提高可用性
 * 4. 支持异步编程模型
 *
 * @param <T> 请求结果的类型
 */
public interface RequestFutureListener<T> {

    /**
     * 请求成功完成时的回调方法
     * 当Future成功完成时被调用
     * 
     * 应用场景：
     * - 处理成功返回的结果
     * - 触发后续的操作
     * - 更新状态或缓存
     * - 通知其他组件
     *
     * @param value 请求成功返回的结果值
     */
    void onSuccess(T value);

    /**
     * 请求失败时的回调方法
     * 当Future因异常而失败时被调用
     * 
     * 应用场景：
     * - 错误处理和日志记录
     * - 重试策略的实现
     * - 资源清理
     * - 错误通知
     *
     * @param e 导致请求失败的运行时异常
     */
    void onFailure(RuntimeException e);
}
