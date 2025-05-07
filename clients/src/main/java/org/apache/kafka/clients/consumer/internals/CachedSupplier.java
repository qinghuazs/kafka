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

import java.util.function.Supplier;

/**
 * 简单的缓存供应器类
 * 缓存对象的初始创建并将其存储以供后续的{@link #get()}调用使用
 *
 * <p/>
 *
 * <em>注意</em>：此类不是线程安全的！只能在设计为/保证是单线程的上下文中使用。
 */
public abstract class CachedSupplier<T> implements Supplier<T> {

    /**
     * 缓存的结果对象
     * 用于存储create()方法创建的对象实例
     */
    private T result;

    /**
     * 创建缓存对象的抽象方法
     * 子类必须实现此方法来提供实际的对象创建逻辑
     *
     * @return 创建的对象实例
     */
    protected abstract T create();

    /**
     * 获取缓存的对象实例
     * 如果对象尚未创建，则调用create()方法创建并缓存
     * 如果对象已存在，则直接返回缓存的实例
     *
     * @return 缓存的对象实例
     */
    @Override
    public T get() {
        // 如果结果为空，调用create()方法创建新实例
        if (result == null)
            result = create();

        // 返回缓存的结果
        return result;
    }
}
