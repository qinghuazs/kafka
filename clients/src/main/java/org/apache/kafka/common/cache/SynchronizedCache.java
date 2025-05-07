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
package org.apache.kafka.common.cache;

/**
 * 同步缓存包装器
 * 为缓存添加简单的同步机制以提供线程安全的缓存实现。
 * 注意：这个实现仅仅是在底层非同步缓存的每个方法上添加同步。
 * 它不提供原子性的检查条目存在性、计算并插入缺失值的支持。
 * 
 * 应用场景：
 * 1. 多线程环境下的缓存访问
 * 2. 需要线程安全保证的缓存操作
 * 3. 简单的同步需求场景
 * 4. 包装现有非线程安全的缓存实现
 *
 * 设计考虑：
 * 1. 装饰器模式：包装现有缓存实现
 * 2. 方法级同步：保证单个操作的原子性
 * 3. 最小化同步范围：每个方法独立同步
 * 4. 简单性优先：不支持复杂的原子操作
 *
 * @param <K> 缓存键的类型
 * @param <V> 缓存值的类型
 */
public class SynchronizedCache<K, V> implements Cache<K, V> {
    /**
     * 底层缓存实现
     * 被包装的非线程安全缓存实例
     */
    private final Cache<K, V> underlying;

    /**
     * 构造函数
     * 创建一个新的同步缓存包装器
     *
     * @param underlying 要包装的底层缓存实现
     */
    public SynchronizedCache(Cache<K, V> underlying) {
        // 保存底层缓存引用
        this.underlying = underlying;
    }

    /**
     * 同步获取缓存值
     * 在同步块中执行底层缓存的get操作
     *
     * @param key 要获取的键
     * @return 对应的值，如果不存在则返回null
     */
    @Override
    public synchronized V get(K key) {
        // 在同步块中调用底层缓存的get方法
        return underlying.get(key);
    }

    /**
     * 同步存储键值对
     * 在同步块中执行底层缓存的put操作
     *
     * @param key 要存储的键
     * @param value 要存储的值
     */
    @Override
    public synchronized void put(K key, V value) {
        // 在同步块中调用底层缓存的put方法
        underlying.put(key, value);
    }

    /**
     * 同步移除缓存条目
     * 在同步块中执行底层缓存的remove操作
     *
     * @param key 要移除的键
     * @return 如果键存在并被移除则返回true，否则返回false
     */
    @Override
    public synchronized boolean remove(K key) {
        // 在同步块中调用底层缓存的remove方法
        return underlying.remove(key);
    }

    /**
     * 同步获取缓存大小
     * 在同步块中执行底层缓存的size操作
     *
     * @return 缓存中的条目数
     */
    @Override
    public synchronized long size() {
        // 在同步块中调用底层缓存的size方法
        return underlying.size();
    }
}
